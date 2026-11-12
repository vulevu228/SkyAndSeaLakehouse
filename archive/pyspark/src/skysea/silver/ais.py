"""Bronze AIS -> silver. Every rule below comes from profiling the real bronze data (2026-10-22,
scripts/profile_bronze_ais.py), not from guesses.

silver.ais_positions           typed, cleaned positions; one row per ship per 5-minute window
silver.ais_positions_rejected  rows that cannot be used at all, with the reason (never silently dropped)
silver.ais_ships               one row per MMSI: latest name/type/size, IMO check, name-change count
silver.ais_voyage_reports      what each ship said about where it is going and when (destination + ETA)

Silver is rebuilt from bronze each run (overwrite). At ~20M rows that takes a minute or two, and
it means a changed rule is applied to all history at once - no half-old, half-new table.
"""
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from skysea.config import REFERENCE_DIR

# --- AIS "not available" codes (ITU-R M.1371): these are sentinels, not measurements ---
SOG_NOT_AVAILABLE = 102.3      # speed over ground 1023 = n/a (1/10 knot units)
COG_NOT_AVAILABLE = 360.0      # course over ground 3600 = n/a
NAV_STATUS_UNDEFINED = 15
MAX_PLAUSIBLE_SHIP_KNOTS = 50  # fastest ferries/HSC ~45 kn; above that a "ship" speed is suspect
JUMP_KNOTS = 60                # implied speed between two consecutive fixes above this = position jump
JUMP_MIN_KM = 2                # ...but ignore tiny jumps (GPS noise over short gaps)
EARTH_KM = 6371.0


def mmsi_kind(mmsi: Column) -> Column:
    """The first digits of an MMSI say what kind of transmitter it is (ITU-R M.585)."""
    return (
        F.when(~mmsi.rlike(r"^[0-9]{9}$"), "invalid")
        .when(mmsi.substr(1, 3) == "111", "sar_aircraft")
        .when(mmsi.substr(1, 3).isin("970", "972", "974"), "emergency_beacon")
        .when(mmsi.substr(1, 2) == "99", "aid_to_navigation")
        .when(mmsi.substr(1, 2) == "00", "base_station")
        .when(mmsi.substr(1, 1).between("2", "7"), "ship")
        .otherwise("other")
    )


def haversine_km(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = F.sin(dlat / 2) ** 2 + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2
    return 2 * EARTH_KM * F.asin(F.sqrt(a))


def read_reference(spark: SparkSession, name: str) -> DataFrame:
    return spark.read.option("header", True).option("inferSchema", True).csv(str(REFERENCE_DIR / name))


# ---------------------------------------------------------------------------------------------
# positions
# ---------------------------------------------------------------------------------------------
def type_positions(bronze: DataFrame) -> DataFrame:
    """Strings -> real types. try_cast returns NULL instead of failing, so one bad value
    never stops the job; the reject step below counts what did not parse."""
    lat = F.expr("try_cast(latitude AS double)")
    lon = F.expr("try_cast(longitude AS double)")
    sog = F.expr("try_cast(sog_knots AS double)")
    cog = F.expr("try_cast(cog_deg AS double)")
    nav = F.expr("try_cast(nav_status AS int)")
    return bronze.select(
        F.expr("try_cast(window_start AS timestamp)").alias("window_start"),
        F.expr("try_cast(received_at AS timestamp)").alias("received_at"),
        "mmsi",
        mmsi_kind(F.col("mmsi")).alias("mmsi_kind"),
        F.nullif(F.trim("ship_name"), F.lit("")).alias("ship_name"),
        "ais_class",
        lat.alias("latitude"),
        lon.alias("longitude"),
        F.when(sog < SOG_NOT_AVAILABLE, sog).alias("sog_knots"),      # 102.3 -> NULL
        F.when(cog < COG_NOT_AVAILABLE, cog).alias("cog_deg"),        # 360   -> NULL (not "north")
        F.expr("try_cast(true_heading AS int)").alias("true_heading"),
        F.when(nav != NAV_STATUS_UNDEFINED, nav).alias("nav_status"),  # 15 -> NULL; class B sends none
        F.expr("try_cast(reports_in_window AS int)").alias("reports_in_window"),
        "file_date",
        "source_file",
    )


def reject_reason(df: DataFrame) -> Column:
    return (
        F.when(df.mmsi_kind == "invalid", "mmsi not 9 digits")
        .when(df.window_start.isNull(), "unparseable window_start")
        .when(df.latitude.isNull() | df.longitude.isNull(), "missing position")
        .when((F.abs(df.latitude) > 90) | (F.abs(df.longitude) > 180), "position out of range")
        .when((df.latitude == 0) & (df.longitude == 0), "null island (0,0 placeholder)")
    )


def flag_jumps(df: DataFrame) -> DataFrame:
    """Compare each fix with the same transmitter's previous fix.

    Window.partitionBy("mmsi") is a SHUFFLE: Spark moves every row of one MMSI to the same task
    so the rows can be sorted by time. That is the expensive part of this job (see Spark UI).
    """
    w = Window.partitionBy("mmsi").orderBy("window_start")
    prev_lat, prev_lon, prev_t = F.lag("latitude").over(w), F.lag("longitude").over(w), F.lag("window_start").over(w)
    km = haversine_km(prev_lat, prev_lon, F.col("latitude"), F.col("longitude"))
    hours = (F.unix_timestamp("window_start") - F.unix_timestamp(prev_t)) / 3600.0
    return (
        df.withColumn("km_from_prev", F.round(km, 3))
        .withColumn("minutes_from_prev", F.round(hours * 60, 1))
        .withColumn("implied_knots", F.round(F.when(hours > 0, km / hours / 1.852), 1))
        .withColumn(
            "is_position_jump",
            F.coalesce((F.col("implied_knots") > JUMP_KNOTS) & (F.col("km_from_prev") > JUMP_MIN_KM)
                       & (F.col("mmsi_kind") != "sar_aircraft"), F.lit(False)),
        )
        .withColumn(
            "is_speed_suspect",
            F.coalesce((F.col("sog_knots") > MAX_PLAUSIBLE_SHIP_KNOTS) & (F.col("mmsi_kind") != "sar_aircraft"),
                       F.lit(False)),
        )
    )


def build_positions(spark: SparkSession, bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    typed = type_positions(bronze)
    typed = typed.withColumn("reject_reason", reject_reason(typed))
    rejected = typed.where(F.col("reject_reason").isNotNull())
    good = typed.where(F.col("reject_reason").isNull()).drop("reject_reason")

    # Bronze had 0 duplicates, but a re-run of a collector could add some: keep this guard.
    good = good.dropDuplicates(["mmsi", "window_start"])

    # The lookup table has 16 rows. F.broadcast() ships a copy to every task, so the 19M-row side
    # is NOT shuffled for this join (a "broadcast hash join" in the plan).
    nav = read_reference(spark, "ais_nav_status.csv")
    good = good.join(F.broadcast(nav), "nav_status", "left")

    return flag_jumps(good), rejected


# ---------------------------------------------------------------------------------------------
# ships + voyage reports (from the static/voyage messages)
# ---------------------------------------------------------------------------------------------
def imo_is_valid(imo: Column) -> Column:
    """IMO numbers have a check digit: sum(first 6 digits x weights 7..2) mod 10 = digit 7."""
    d = [F.substring(imo, i, 1).cast("int") for i in range(1, 8)]
    checksum = sum(d[i] * (7 - i) for i in range(6)) % 10
    return imo.rlike(r"^[0-9]{7}$") & (checksum == d[6])


def type_static(bronze: DataFrame) -> DataFrame:
    def num(c, t="double"):
        return F.expr(f"try_cast({c} AS {t})")

    return bronze.where(F.col("mmsi").rlike(r"^[0-9]{9}$")).select(
        F.expr("try_cast(received_at AS timestamp)").alias("received_at"),
        "mmsi",
        F.nullif(F.trim("imo"), F.lit("")).alias("imo"),
        F.nullif(F.trim("ship_name"), F.lit("")).alias("ship_name"),
        F.nullif(F.trim("call_sign"), F.lit("")).alias("call_sign"),
        num("ship_type", "int").alias("ship_type"),
        # Crews type destinations by hand: upper-case, collapse spaces, blank -> NULL.
        F.nullif(F.regexp_replace(F.upper(F.trim("destination")), r"\s+", " "), F.lit("")).alias("destination"),
        num("eta_month", "int").alias("eta_month"),
        num("eta_day", "int").alias("eta_day"),
        num("eta_hour", "int").alias("eta_hour"),
        num("eta_minute", "int").alias("eta_minute"),
        F.when(num("draught_m") > 0, num("draught_m")).alias("draught_m"),
        F.when(num("length_m") > 0, num("length_m")).alias("length_m"),
        F.when(num("width_m") > 0, num("width_m")).alias("width_m"),
    )


def build_ships(spark: SparkSession, static: DataFrame) -> DataFrame:
    """One row per MMSI. max_by(x, received_at) = the value from the most recent message;
    max_by ignores rows where x is NULL, so a later message without a name does not erase it."""
    latest = lambda c: F.max_by(c, F.when(F.col(c).isNotNull(), F.col("received_at"))).alias(c)  # noqa: E731
    ships = static.groupBy("mmsi").agg(
        latest("ship_name"), latest("imo"), latest("call_sign"), latest("ship_type"),
        latest("length_m"), latest("width_m"),
        F.countDistinct("ship_name").alias("n_names"),
        F.min("received_at").alias("first_seen"),
        F.max("received_at").alias("last_seen"),
        F.count("*").alias("n_reports"),
    )
    types = read_reference(spark, "ais_ship_types.csv")
    return (
        ships.join(F.broadcast(types), "ship_type", "left")
        .withColumn("imo_valid", F.coalesce(imo_is_valid(F.col("imo")), F.lit(False)))
        .withColumn("mmsi_kind", mmsi_kind(F.col("mmsi")))
        # Largest ships ever built are ~458 m; anything longer is a typo or a default value.
        .withColumn("is_size_suspect", F.coalesce(F.col("length_m") > 460, F.lit(False)))
    )


def eta_timestamp(df: DataFrame) -> Column:
    """AIS ETA has no year. Take the year of the report; if that puts the ETA more than ~6 months
    in the past, the ship means next year (e.g. reported 30 Dec, ETA 2 Jan).
    0 month/day, hour 24, minute 60 = 'not available' -> NULL."""
    ok = (F.col("eta_month").between(1, 12) & F.col("eta_day").between(1, 31)
          & F.col("eta_hour").between(0, 23) & F.col("eta_minute").between(0, 59))
    # try_* variants: day 31 in a 30-day month gives NULL instead of an error.
    this_year = F.expr("try_make_timestamp(year(received_at), eta_month, eta_day, eta_hour, eta_minute, 0)")
    next_year = F.expr("try_make_timestamp(year(received_at) + 1, eta_month, eta_day, eta_hour, eta_minute, 0)")
    return F.when(ok, F.when(this_year < F.col("received_at") - F.expr("INTERVAL 180 DAYS"), next_year)
                  .otherwise(this_year))


def build_voyage_reports(static: DataFrame) -> DataFrame:
    """One row per (ship, destination, ETA) the crew announced, with when it was first/last said.
    Gold compares this with when the ship actually arrived (ETA accuracy)."""
    reports = static.withColumn("eta", eta_timestamp(static)).where(
        F.col("destination").isNotNull() | F.col("eta").isNotNull()
    )
    return reports.groupBy("mmsi", "destination", "eta").agg(
        F.min("received_at").alias("first_reported"),
        F.max("received_at").alias("last_reported"),
        F.count("*").alias("n_reports"),
        F.max_by("draught_m", "received_at").alias("draught_m"),
    ).withColumn(
        # Placeholders crews leave in the field: not a port.
        "destination_is_placeholder",
        F.coalesce(F.col("destination").rlike(r"^(LAST UPDATE TIME|FOR ORDERS|TBA|TBN|N/?A|NONE|\W+)$"), F.lit(False)),
    )
