"""Bronze OpenSky -> silver. Rules come from scripts/profile_bronze_opensky.py (2026-10-22).

silver\\opensky_states                   typed state vectors, one row per aircraft per snapshot, flagged
silver\\opensky_states_rejected.parquet  rows without a usable position, with the reason
silver\\opensky_aircraft.parquet         one row per transponder (icao24): callsigns, country, first/last seen

What the profile found (34.7M rows, 166k transponders, 0 duplicates, 0 bad icao24):
  - 232k rows have no position at all                         -> rejected
  - sensors and category are ALWAYS empty (API not asked)     -> dropped
  - position age (snapshot - time_position): median 1 s, p99 318 s, max 44 h
                                                              -> is_stale_position (> 120 s)
  - 43k rows faster than 340 m/s (above Mach 1): almost all Turkish/Egyptian airliners at
    cruise showing 1,000-2,000 m/s = GPS spoofing/jamming     -> is_speed_suspect (kept: a story)
  - baro 38 km while GPS says 1 km; baro -900 m               -> is_altitude_suspect
  - real stratospheric balloons at ~21 km fly at 4 m/s        -> NOT suspect, GPS agrees
  - found in gold: N6278R jumps 500 m <-> 18 km every 5 min with vertical rate 1 m/s = altitude
    encoder bit errors (GPS altitude copies them)            -> is_altitude_jump
"""
import polars as pl

from skysea.silver.ais import blank_to_null, haversine_km, num, to_ts

STALE_POSITION_S = 120       # position older than this at snapshot time = re-broadcast, not new
MAX_PLAUSIBLE_MS = 340       # ~Mach 1 at cruise; no civil supersonic aircraft in service
JUMP_MS = 400                # implied speed between two fixes above this = position jump
JUMP_MIN_KM = 20             # ...ignore small jumps (a 5-min snapshot covers ~70 km at cruise)
ALT_DIFF_M = 1500            # |GPS altitude - pressure altitude| above this: one of them is wrong
                             #   (normal difference: median +282 m, 99.9% within 1,494 m)
ALT_JUMP_M = 1500            # altitude change bigger than this between two snapshots...
ALT_JUMP_MS = 40             # ...averaging > 40 m/s (8,000 ft/min held for a whole 5-min gap; nothing civil
                             #   does that, not even an emergency descent) = encoder glitch. 75 missed N6278R.
MIN_BARO_M = -500            # lowest airfield on earth is ~ -400 m (Dead Sea region)
CEILING_M = 15600            # FL512: highest certified business-jet ceiling (~FL510)
EMERGENCY = {"7500": "hijack", "7600": "radio failure", "7700": "general emergency"}
UNRELIABLE_ALT_SHARE = 0.05  # transponder with > 5% altitude-suspect states: do not trust ANY of its altitudes
UNRELIABLE_ALT_MIN = 5       # ...and at least 5 of them: NASA806 had 2 of 36 (5.6%) - a share needs a floor
TEST_ICAO = ["000000", "aaaaaa", "ffffff"]


def type_states(bronze: pl.LazyFrame) -> pl.LazyFrame:
    epoch = lambda c: pl.from_epoch(pl.col(c).cast(pl.Int64, strict=False), time_unit="s").dt.replace_time_zone("UTC")  # noqa: E731
    return bronze.select(
        to_ts("snapshot_time").alias("snapshot_time"),
        pl.col("icao24").str.to_lowercase().alias("icao24"),
        blank_to_null(pl.col("callsign")).alias("callsign"),   # 'TVF42VD ' -> 'TVF42VD'
        "origin_country",
        epoch("time_position").alias("time_position"),
        epoch("last_contact").alias("last_contact"),
        num("latitude").alias("latitude"),
        num("longitude").alias("longitude"),
        num("baro_altitude").alias("baro_altitude"),
        num("geo_altitude").alias("geo_altitude"),
        (pl.col("on_ground") == "True").alias("on_ground"),
        num("velocity").alias("velocity"),
        num("true_track").alias("true_track"),
        num("vertical_rate").alias("vertical_rate"),
        "squawk",
        (pl.col("spi") == "True").alias("spi"),
        num("position_source", pl.Int8).alias("position_source"),
        "file_date",
        "source_file",
    ).with_columns(
        (pl.col("snapshot_time") - pl.col("time_position")).dt.total_seconds().alias("position_age_s"),
    )


def reject_reason() -> pl.Expr:
    lat, lon = pl.col("latitude"), pl.col("longitude")
    return (
        pl.when(~pl.col("icao24").fill_null("").str.contains(r"^[0-9a-f]{6}$")).then(pl.lit("icao24 not 6 hex digits"))
        .when(pl.col("snapshot_time").is_null()).then(pl.lit("unparseable snapshot_time"))
        .when(lat.is_null() | lon.is_null() | pl.col("time_position").is_null()).then(pl.lit("no position"))
        .when((lat.abs() > 90) | (lon.abs() > 180)).then(pl.lit("position out of range"))
    )


def quality_flags() -> list[pl.Expr]:
    baro, geo = pl.col("baro_altitude"), pl.col("geo_altitude")
    return [
        (pl.col("position_age_s") > STALE_POSITION_S).fill_null(False).alias("is_stale_position"),
        (pl.col("velocity") > MAX_PLAUSIBLE_MS).fill_null(False).alias("is_speed_suspect"),
        (((geo - baro).abs() > ALT_DIFF_M) | (baro < MIN_BARO_M) | ((baro > CEILING_M) & geo.is_null()))
        .fill_null(False).alias("is_altitude_suspect"),
        pl.col("squawk").is_in(list(EMERGENCY)).alias("is_emergency_squawk"),
        pl.col("icao24").is_in(TEST_ICAO).alias("is_test_transponder"),
    ]


CARRY_COLS = ["icao24", "snapshot_time", "time_position", "latitude", "longitude", "baro_altitude"]


def flag_jumps(day: pl.DataFrame, carry: pl.DataFrame | None) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Same carry-over idea as AIS (silver/ais.py), keyed by icao24. Time = time_position (when the
    position was measured), not snapshot_time: a stale re-broadcast has the same time_position as
    the row before, gives 0 hours, and so never counts as a jump."""
    stack = day.with_columns(pl.lit(False).alias("_carry"))
    if carry is not None:
        stack = pl.concat([carry.with_columns(pl.lit(True).alias("_carry")), stack], how="diagonal_relaxed")
    stack = stack.sort("icao24", "snapshot_time", "time_position", nulls_last=False)
    prev = lambda c: pl.col(c).shift(1).over("icao24")  # noqa: E731
    km = haversine_km(prev("latitude"), prev("longitude"), pl.col("latitude"), pl.col("longitude"))
    secs = (pl.col("time_position") - prev("time_position")).dt.total_seconds()
    # Altitude comes with every message, not only with positions: compare snapshot to snapshot.
    d_alt = (pl.col("baro_altitude") - prev("baro_altitude")).abs()
    alt_secs = (pl.col("snapshot_time") - prev("snapshot_time")).dt.total_seconds()
    out = (
        stack.with_columns(
            km.round(2).alias("km_from_prev"),
            pl.when(secs > 0).then(km * 1000 / secs).round(1).alias("implied_ms"),
        )
        .with_columns(((pl.col("implied_ms") > JUMP_MS) & (pl.col("km_from_prev") > JUMP_MIN_KM))
                      .fill_null(False).alias("is_position_jump"),
                      ((d_alt > ALT_JUMP_M) & (d_alt / alt_secs > ALT_JUMP_MS))
                      .fill_null(False).alias("is_altitude_jump"))
        .filter(~pl.col("_carry"))
        .drop("_carry")
    )
    last = day.select(CARRY_COLS) if carry is None else pl.concat([carry, day.select(CARRY_COLS)])
    return out, last.sort("snapshot_time").unique("icao24", keep="last")


def build_states_day(bronze_day: pl.LazyFrame, carry: pl.DataFrame | None):
    """One day: type, reject, flag. Returns (good, rejected, carry)."""
    typed = type_states(bronze_day).with_columns(reject_reason().alias("reject_reason")).collect()
    rejected = typed.filter(pl.col("reject_reason").is_not_null())
    good = typed.filter(pl.col("reject_reason").is_null()).drop("reject_reason").with_columns(quality_flags())
    good = good.with_columns(pl.col("squawk").replace_strict(EMERGENCY, default=None).alias("emergency_type"))
    good, carry = flag_jumps(good, carry)
    return good, rejected, carry


def build_aircraft(states: pl.LazyFrame) -> pl.LazyFrame:
    """One row per transponder. icao24 is the airframe's address (fixed); the callsign is the
    flight (changes every flight), so one aircraft has many callsigns."""
    return (
        states.select("icao24", "callsign", "origin_country", "snapshot_time", "on_ground", "is_test_transponder",
                      "is_altitude_suspect", "is_altitude_jump")
        .sort("snapshot_time")
        .group_by("icao24")
        .agg(
            pl.col("callsign").drop_nulls().last().alias("last_callsign"),
            pl.col("callsign").drop_nulls().n_unique().alias("n_callsigns"),
            pl.col("origin_country").drop_nulls().last(),
            pl.col("snapshot_time").min().alias("first_seen"),
            pl.col("snapshot_time").max().alias("last_seen"),
            pl.len().alias("n_states"),
            (~pl.col("on_ground")).mean().round(3).alias("share_airborne"),
            pl.col("is_test_transponder").first(),
            pl.col("is_altitude_suspect").mean().round(4).alias("share_altitude_suspect"),
            pl.col("is_altitude_suspect").sum().alias("n_altitude_suspect"),
            pl.col("is_altitude_jump").sum().alias("n_altitude_jumps"),
        )
        # Found in gold: N290TW's altitude was STUCK at 35,966 m for a whole flight (no jump inside it),
        # but 21% of all its states were altitude-suspect. Judge the transponder, not just the row.
        .with_columns((((pl.col("share_altitude_suspect") > UNRELIABLE_ALT_SHARE)
                        & (pl.col("n_altitude_suspect") >= UNRELIABLE_ALT_MIN)) | (pl.col("n_altitude_jumps") > 0))
                      .alias("is_altitude_unreliable"))
    )
