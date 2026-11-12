"""Bronze AIS -> silver. Every rule below comes from profiling the real bronze data (2026-10-22),
not from guesses.

silver\\ais_positions                  typed, cleaned positions; one row per ship per 5-minute window
silver\\ais_positions_rejected.parquet rows that cannot be used at all, with the reason (never silently dropped)
silver\\ais_ships.parquet              one row per MMSI: latest name/type/size, IMO check, name-change count
silver\\ais_voyage_reports.parquet     what each ship said about where it is going and when (destination + ETA)

Positions are processed ONE DAY AT A TIME (~2M rows) so the laptop never holds all 19M in RAM.
The only rule that looks across days - "compare with this ship's previous fix" - gets a small
carry-over table: the last fix of every ship seen so far (~130k rows), see flag_jumps().
"""
import polars as pl

from skysea.config import REFERENCE_DIR

# --- AIS "not available" codes (ITU-R M.1371): these are sentinels, not measurements ---
SOG_NOT_AVAILABLE = 102.3      # speed over ground 1023 = n/a (1/10 knot units)
COG_NOT_AVAILABLE = 360.0      # course over ground 3600 = n/a
NAV_STATUS_UNDEFINED = 15
MAX_PLAUSIBLE_SHIP_KNOTS = 50  # fastest ferries/HSC ~45 kn; above that a "ship" speed is suspect
JUMP_KNOTS = 60                # implied speed between two consecutive fixes above this = position jump
JUMP_MIN_KM = 2                # ...but ignore tiny jumps (GPS noise over short gaps)
EARTH_KM = 6371.0
TS_FORMAT = "%Y-%m-%dT%H:%M:%S%z"  # 2026-09-26T00:00:00+00:00 -> Datetime in UTC


def mmsi_kind(mmsi: pl.Expr) -> pl.Expr:
    """The first digits of an MMSI say what kind of transmitter it is (ITU-R M.585)."""
    m = mmsi.fill_null("")
    return (
        pl.when(~m.str.contains(r"^[0-9]{9}$")).then(pl.lit("invalid"))
        .when(m.str.slice(0, 3) == "111").then(pl.lit("sar_aircraft"))
        .when(m.str.slice(0, 3).is_in(["970", "972", "974"])).then(pl.lit("emergency_beacon"))
        .when(m.str.slice(0, 2) == "99").then(pl.lit("aid_to_navigation"))
        .when(m.str.slice(0, 2) == "00").then(pl.lit("base_station"))
        .when(m.str.slice(0, 1).is_in(list("234567"))).then(pl.lit("ship"))
        .otherwise(pl.lit("other"))
    )


def haversine_km(lat1: pl.Expr, lon1: pl.Expr, lat2: pl.Expr, lon2: pl.Expr) -> pl.Expr:
    dlat = (lat2 - lat1).radians()
    dlon = (lon2 - lon1).radians()
    a = (dlat / 2).sin() ** 2 + lat1.radians().cos() * lat2.radians().cos() * (dlon / 2).sin() ** 2
    return 2 * EARTH_KM * a.sqrt().arcsin()


def blank_to_null(c: pl.Expr) -> pl.Expr:
    c = c.str.strip_chars()
    return pl.when(c == "").then(None).otherwise(c)


def to_ts(name: str) -> pl.Expr:
    # strict=False = Spark's try_cast: an unparseable value becomes NULL instead of failing the job.
    return pl.col(name).str.to_datetime(TS_FORMAT, strict=False, time_unit="us", time_zone="UTC")


def num(name: str, dtype=pl.Float64) -> pl.Expr:
    return pl.col(name).cast(dtype, strict=False)


def read_reference(name: str) -> pl.DataFrame:
    return pl.read_csv(REFERENCE_DIR / name)


# ---------------------------------------------------------------------------------------------
# positions
# ---------------------------------------------------------------------------------------------
def type_positions(bronze: pl.LazyFrame) -> pl.LazyFrame:
    """Strings -> real types."""
    sog, cog, nav = num("sog_knots"), num("cog_deg"), num("nav_status", pl.Int16)
    return bronze.select(
        to_ts("window_start").alias("window_start"),
        to_ts("received_at").alias("received_at"),
        "mmsi",
        mmsi_kind(pl.col("mmsi")).alias("mmsi_kind"),
        blank_to_null(pl.col("ship_name")).alias("ship_name"),
        "ais_class",
        num("latitude").alias("latitude"),
        num("longitude").alias("longitude"),
        pl.when(sog < SOG_NOT_AVAILABLE).then(sog).alias("sog_knots"),     # 102.3 -> NULL
        pl.when(cog < COG_NOT_AVAILABLE).then(cog).alias("cog_deg"),       # 360   -> NULL (not "north")
        num("true_heading", pl.Int16).alias("true_heading"),
        pl.when(nav != NAV_STATUS_UNDEFINED).then(nav).alias("nav_status"),  # 15 -> NULL; class B sends none
        num("reports_in_window", pl.Int32).alias("reports_in_window"),
        "file_date",
        "source_file",
    )


def reject_reason() -> pl.Expr:
    lat, lon = pl.col("latitude"), pl.col("longitude")
    return (
        pl.when(pl.col("mmsi_kind") == "invalid").then(pl.lit("mmsi not 9 digits"))
        .when(pl.col("window_start").is_null()).then(pl.lit("unparseable window_start"))
        .when(lat.is_null() | lon.is_null()).then(pl.lit("missing position"))
        .when((lat.abs() > 90) | (lon.abs() > 180)).then(pl.lit("position out of range"))
        .when((lat == 0) & (lon == 0)).then(pl.lit("null island (0,0 placeholder)"))
    )


CARRY_COLS = ["mmsi", "window_start", "latitude", "longitude"]


def flag_jumps(day: pl.DataFrame, carry: pl.DataFrame | None) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Compare each fix with the same transmitter's previous fix.

    shift(1).over("mmsi") on data sorted by (mmsi, window_start) = SQL LAG(...) OVER (PARTITION BY
    mmsi ORDER BY window_start). Because we go day by day, the first fix of a day would have no
    previous row. So the carry-over rows (each ship's last fix from earlier days) are stacked on
    top, used for the comparison, and dropped again. Returns (flagged day, new carry-over).
    """
    stack = day.with_columns(pl.lit(False).alias("_carry"))
    if carry is not None:
        stack = pl.concat([carry.with_columns(pl.lit(True).alias("_carry")), stack], how="diagonal_relaxed")
    stack = stack.sort("mmsi", "window_start")
    prev = lambda c: pl.col(c).shift(1).over("mmsi")  # noqa: E731
    km = haversine_km(prev("latitude"), prev("longitude"), pl.col("latitude"), pl.col("longitude"))
    hours = (pl.col("window_start") - prev("window_start")).dt.total_seconds() / 3600.0
    not_sar = pl.col("mmsi_kind") != "sar_aircraft"
    out = (
        stack.with_columns(
            km.round(3).alias("km_from_prev"),
            (hours * 60).round(1).alias("minutes_from_prev"),
            pl.when(hours > 0).then(km / hours / 1.852).round(1).alias("implied_knots"),
        )
        .with_columns(
            ((pl.col("implied_knots") > JUMP_KNOTS) & (pl.col("km_from_prev") > JUMP_MIN_KM) & not_sar)
            .fill_null(False).alias("is_position_jump"),
            ((pl.col("sog_knots") > MAX_PLAUSIBLE_SHIP_KNOTS) & not_sar).fill_null(False).alias("is_speed_suspect"),
        )
        .filter(~pl.col("_carry"))
        .drop("_carry")
    )
    last = day.select(CARRY_COLS) if carry is None else pl.concat([carry, day.select(CARRY_COLS)])
    new_carry = last.sort("window_start").unique("mmsi", keep="last")
    return out, new_carry


def build_positions_day(bronze_day: pl.LazyFrame, carry: pl.DataFrame | None, nav: pl.DataFrame):
    """One day: type, reject, dedupe, label, flag. Returns (good, rejected, duplicates, carry)."""
    typed = type_positions(bronze_day).with_columns(reject_reason().alias("reject_reason")).collect()
    rejected = typed.filter(pl.col("reject_reason").is_not_null())
    good = typed.filter(pl.col("reject_reason").is_null()).drop("reject_reason")

    # Bronze had 0 duplicates, but a re-run of a collector could add some: keep this guard.
    n = good.height
    good = good.unique(["mmsi", "window_start"], keep="first", maintain_order=True)
    dups = n - good.height

    # 16-row lookup table: a join this small costs nothing (Spark needed broadcast() for this).
    good = good.join(nav, on="nav_status", how="left")
    good, carry = flag_jumps(good, carry)
    return good, rejected, dups, carry


# ---------------------------------------------------------------------------------------------
# ships + voyage reports (from the static/voyage messages)
# ---------------------------------------------------------------------------------------------
def imo_is_valid(imo: pl.Expr) -> pl.Expr:
    """IMO numbers have a check digit: sum(first 6 digits x weights 7..2) mod 10 = digit 7."""
    d = [imo.str.slice(i, 1).cast(pl.Int32, strict=False) for i in range(7)]
    checksum = sum(d[i] * (7 - i) for i in range(6)) % 10
    return (imo.str.contains(r"^[0-9]{7}$") & (checksum == d[6])).fill_null(False)


def type_static(bronze: pl.LazyFrame) -> pl.LazyFrame:
    dim = lambda c: pl.when(num(c) > 0).then(num(c)).alias(c)  # noqa: E731  0 = not available
    dest = pl.col("destination").str.to_uppercase().str.replace_all(r"\s+", " ")
    return bronze.filter(pl.col("mmsi").str.contains(r"^[0-9]{9}$")).select(
        to_ts("received_at").alias("received_at"),
        "mmsi",
        blank_to_null(pl.col("imo")).alias("imo"),
        blank_to_null(pl.col("ship_name")).alias("ship_name"),
        blank_to_null(pl.col("call_sign")).alias("call_sign"),
        num("ship_type", pl.Int32).alias("ship_type"),
        # Crews type destinations by hand: upper-case, collapse spaces, blank -> NULL.
        blank_to_null(dest).alias("destination"),
        *[num(c, pl.Int32).alias(c) for c in ("eta_month", "eta_day", "eta_hour", "eta_minute")],
        dim("draught_m"), dim("length_m"), dim("width_m"),
    )


def build_ships(static: pl.LazyFrame) -> pl.LazyFrame:
    """One row per MMSI with the most recent value of each field.

    After sort("received_at"), group_by keeps that order inside every group, so
    drop_nulls().last() = the latest NON-NULL value: a later message without a name
    does not erase the name (Spark: max_by(x, received_at))."""
    latest = lambda c: pl.col(c).drop_nulls().last().alias(c)  # noqa: E731
    types = read_reference("ais_ship_types.csv").with_columns(pl.col("ship_type").cast(pl.Int32))
    return (
        static.sort("received_at")
        .group_by("mmsi")
        .agg(
            latest("ship_name"), latest("imo"), latest("call_sign"), latest("ship_type"),
            latest("length_m"), latest("width_m"),
            pl.col("ship_name").drop_nulls().n_unique().alias("n_names"),
            pl.col("received_at").min().alias("first_seen"),
            pl.col("received_at").max().alias("last_seen"),
            pl.len().alias("n_reports"),
        )
        .join(types.lazy(), on="ship_type", how="left")
        .with_columns(
            imo_is_valid(pl.col("imo")).alias("imo_valid"),
            mmsi_kind(pl.col("mmsi")).alias("mmsi_kind"),
            # Largest ships ever built are ~458 m; anything longer is a typo or a default value.
            (pl.col("length_m") > 460).fill_null(False).alias("is_size_suspect"),
        )
    )


def eta_timestamp() -> pl.Expr:
    """AIS ETA has no year. Take the year of the report; if that puts the ETA more than ~6 months
    in the past, the ship means next year (e.g. reported 30 Dec, ETA 2 Jan).
    0 month/day, hour 24, minute 60 = 'not available' -> NULL."""
    ok = (pl.col("eta_month").is_between(1, 12) & pl.col("eta_day").is_between(1, 31)
          & pl.col("eta_hour").is_between(0, 23) & pl.col("eta_minute").is_between(0, 59))
    two = lambda c: pl.col(c).cast(pl.String).str.zfill(2)  # noqa: E731

    def at_year(offset: int) -> pl.Expr:
        # Built as text and parsed with strict=False, so 31 Feb becomes NULL instead of an error.
        txt = pl.format("{}-{}-{} {}:{}", pl.col("received_at").dt.year() + offset,
                        two("eta_month"), two("eta_day"), two("eta_hour"), two("eta_minute"))
        return txt.str.to_datetime("%Y-%m-%d %H:%M", strict=False, time_unit="us", time_zone="UTC")

    this_year = at_year(0)
    return pl.when(ok).then(
        pl.when(this_year < pl.col("received_at") - pl.duration(days=180)).then(at_year(1)).otherwise(this_year)
    )


def build_voyage_reports(static: pl.LazyFrame) -> pl.LazyFrame:
    """One row per (ship, destination, ETA) the crew announced, with when it was first/last said.
    Gold compares this with when the ship actually arrived (ETA accuracy)."""
    return (
        static.with_columns(eta_timestamp().alias("eta"))
        .filter(pl.col("destination").is_not_null() | pl.col("eta").is_not_null())
        .sort("received_at")
        .group_by("mmsi", "destination", "eta")
        .agg(
            pl.col("received_at").min().alias("first_reported"),
            pl.col("received_at").max().alias("last_reported"),
            pl.len().alias("n_reports"),
            pl.col("draught_m").last().alias("draught_m"),
        )
        .with_columns(
            # Placeholders crews leave in the field: not a port.
            pl.col("destination").str.contains(r"^(LAST UPDATE TIME|FOR ORDERS|TBA|TBN|N/?A|NONE|\W+)$")
            .fill_null(False).alias("destination_is_placeholder"),
        )
    )
