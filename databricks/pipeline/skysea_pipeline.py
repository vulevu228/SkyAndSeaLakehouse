"""Sky & Sea on Databricks: one Lakeflow Declarative Pipeline, Pi collector files -> star schema.

Pipeline settings: default catalog `workspace`, default schema `skysea`. All files sit flat in the
volume /Volumes/workspace/skysea/landing/ (as uploaded); each bronze stream picks its own by name.

  states_YYYY-MM-DD.csv.gz     (OpenSky, every aircraft every 10 min)  -> bronze_opensky_states
  positions_YYYY-MM-DD.csv.gz  (AIS, every ship per 5-min window)      -> bronze_ais_positions
  static_YYYY-MM-DD.csv.gz     (AIS, name/type/size/destination)       -> bronze_ais_static
  ais_nav_status.csv, ais_ship_types.csv                               -> ref_* lookups
  aircraft-database-complete-2025-08.csv  (OpenSky: icao24 -> registration, type, operator)
  doc8643AircraftTypes.csv                (ICAO type designators: engine type/count, wake category)
  faa_aircraft_characteristics.csv        (FAA ACD, sheet ACD_Data saved as CSV: wingspan, length, MTOW)
  unlocode_code_list.csv                  (UN/LOCODE; Function '1...' = seaport)  -> ref_* lookups

  bronze (streaming, all strings) -> silver (typed + quality flags, rules from src/skysea/silver)
  -> gold star schema:
       dims : dim_date, dim_aircraft, dim_aircraft_type, dim_ship, dim_ship_type, dim_nav_status, dim_port
       facts: fact_aircraft_daily (date x aircraft), fact_ship_daily (date x ship),
              fact_traffic_hourly (hour: aircraft + ships seen), fact_port_calls (one row per call),
              fact_quality_daily (per source-file day: bronze -> silver rows, dropped, flagged)
Port calls = gaps-and-islands, same rules as src/skysea/gold/ais.py (stops -> nearest port -> merge).
Not ported yet: position-jump / altitude-jump flags, flight reconstruction.
"""
from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

LANDING = "/Volumes/workspace/skysea/landing"

# thresholds - same values as src/skysea/silver/opensky.py + ais.py (profiled 2026-10-22)
STALE_POSITION_S = 120
MAX_PLAUSIBLE_MS = 340      # ~Mach 1
ALT_DIFF_M = 1500
MIN_BARO_M = -500
CEILING_M = 15600
EMERGENCY = {"7500": "hijack", "7600": "radio failure", "7700": "general emergency"}
TEST_ICAO = ["000000", "aaaaaa", "ffffff"]
SOG_NOT_AVAILABLE = 102.3
COG_NOT_AVAILABLE = 360.0
NAV_STATUS_UNDEFINED = 15
MAX_PLAUSIBLE_SHIP_KNOTS = 50
# port calls - same values as src/skysea/gold/ais.py
STOP_KNOTS = 0.5            # slower than this = not moving
MOORED_DRIFT_KNOTS = 1.0    # reporting moored / at anchor (nav 1, 5) and < 1 kn = still stopped (GPS drift)
MAX_GAP_MINUTES = 30        # a stop continues across up to 30 min of missing fixes
MERGE_GAP_HOURS = 2         # two stops at the same port < 2 h apart = one call (berth shift)
MIN_STOP_MINUTES = 60       # shorter = traffic, locks, pilot pick-up: not a port call
PORT_RADIUS_KM = 20         # stop centre must be this close to the port's LOCODE point
PORT_GRID_DEG = 0.5         # grid cell for the spatial join (0.5 deg ~ 55 km > PORT_RADIUS_KM)
EARTH_KM = 6371.0
FT, LB = 0.3048, 0.45359237
DEST_PLACEHOLDER = r"^(LAST UPDATE TIME|FOR ORDERS|TBA|TBN|N/?A|NONE|\W+)$"


def landing_stream(pattern: str):
    """Auto Loader over the flat landing folder, only files matching `pattern`. Header row -> column
    names, every value stays a string (bronze never alters data). .gz is unpacked automatically."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("pathGlobFilter", pattern)
        .load(LANDING)
        .withColumn("source_file", F.col("_metadata.file_name"))
        .withColumn("file_date", F.to_date(F.regexp_extract("source_file", r"(\d{4}-\d{2}-\d{2})", 1)))
        .withColumn("ingested_at", F.current_timestamp())
    )


def read_ref(name: str, quote: str = '"'):
    return spark.read.option("header", True).option("quote", quote).csv(f"{LANDING}/{name}")


ts = lambda c: F.to_timestamp(F.col(c))          # 2026-10-29T00:00:02+00:00 -> timestamp (UTC)  # noqa: E731
num = lambda c, t="double": F.col(c).try_cast(t)  # bad value -> NULL instead of failing the run  # noqa: E731
blank = lambda c: F.nullif(F.trim(F.col(c)), F.lit(""))  # noqa: E731


def haversine_km(lat1, lon1, lat2, lon2):
    a = (F.sin(F.radians(lat2 - lat1) / 2) ** 2
         + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(F.radians(lon2 - lon1) / 2) ** 2)
    return 2 * EARTH_KM * F.asin(F.sqrt(a))


def imo_is_valid(imo):
    """IMO numbers have a check digit: sum(first 6 digits x weights 7..2) mod 10 = digit 7."""
    d = [F.substring(imo, i + 1, 1).try_cast("int") for i in range(7)]
    checksum = sum(d[i] * (7 - i) for i in range(6)) % 10
    return F.coalesce(imo.rlike(r"^[0-9]{7}$") & (checksum == d[6]), F.lit(False))


# ===================================== BRONZE ==========================================================
@dp.table(name="bronze_opensky_states", comment="OpenSky state vectors as collected by the Pi, all strings.")
def bronze_opensky_states():
    return landing_stream("states_*.csv*")


@dp.table(name="bronze_ais_positions", comment="AIS positions (one row per ship per 5-min window), all strings.")
def bronze_ais_positions():
    return landing_stream("positions_*.csv*")


@dp.table(name="bronze_ais_static", comment="AIS static/voyage reports (name, type, size, destination), all strings.")
def bronze_ais_static():
    return landing_stream("static_*.csv*")


# ===================================== REFERENCE =======================================================
@dp.materialized_view(name="ref_ais_nav_status", comment="AIS navigational status code -> label.")
def ref_ais_nav_status():
    return read_ref("ais_nav_status.csv").select(F.col("nav_status").cast("int").alias("nav_status"), "nav_status_label")


@dp.materialized_view(name="ref_ais_ship_types", comment="AIS ship type code (0-99) -> category.")
def ref_ais_ship_types():
    return read_ref("ais_ship_types.csv").select(F.col("ship_type").cast("int").alias("ship_type"), "ship_category")


@dp.materialized_view(name="ref_aircraft_registry",
                      comment="OpenSky aircraft database 2025-08: one row per transponder (icao24).")
@dp.expect_or_drop("valid_icao24", "icao24 RLIKE '^[0-9a-f]{6}$'")
def ref_aircraft_registry():
    # This file quotes with single quotes ('A320'), not double quotes.
    r = read_ref("aircraft-database-complete-2025-08.csv", quote="'")
    w = Window.partitionBy("icao24").orderBy(F.col("timestamp").desc())
    return (
        r.select(
            F.lower(F.trim("icao24")).alias("icao24"),
            blank("registration").alias("registration"),
            F.upper(blank("typecode")).alias("typecode"),
            blank("manufacturerName").alias("manufacturer"),
            blank("model").alias("model"),
            blank("operator").alias("operator"),
            blank("operatorIcao").alias("operator_icao"),
            F.regexp_extract(F.coalesce(F.col("built"), F.lit("")), r"^(\d{4})", 1).try_cast("int").alias("built_year"),
            "timestamp",
        )
        .withColumn("rn", F.row_number().over(w)).where("rn = 1").drop("rn", "timestamp")
    )


@dp.materialized_view(name="ref_aircraft_types", comment="ICAO Doc 8643: type designator -> engines, wake category.")
def ref_aircraft_types():
    t = read_ref("doc8643AircraftTypes.csv")
    return t.groupBy(F.upper(F.trim("Designator")).alias("typecode")).agg(
        F.min("ModelFullName").alias("icao_model_name"),
        F.min("ManufacturerCode").alias("icao_manufacturer"),
        F.min("Description").alias("icao_description"),     # L2J = landplane, 2 engines, jet
        F.min("EngineType").alias("engine_type"),
        F.min(F.col("EngineCount").try_cast("int")).alias("engines"),
        F.max("WTC").alias("wtc"),                            # L / M / H / J (super)
    )


@dp.materialized_view(name="ref_aircraft_dimensions", comment="FAA Aircraft Characteristics DB, converted to metres / kg.")
def ref_aircraft_dimensions():
    f = read_ref("faa_aircraft_characteristics.csv")
    m = lambda c: F.round(num(c) * FT, 1)  # noqa: E731
    return f.select(
        F.upper(F.trim("ICAO_Code")).alias("typecode"),
        blank("Manufacturer").alias("faa_manufacturer"),
        blank("Model_FAA").alias("faa_model_name"),
        # with winglets/sharklets when the type has them, else the plain span
        F.round(F.coalesce(num("Wingspan_ft_with_winglets_sharklets"), num("Wingspan_ft_without_winglets_sharklets")) * FT, 1)
        .alias("wingspan_m"),
        m("Length_ft").alias("length_m"),
        m("Tail_Height_at_OEW_ft").alias("tail_height_m"),
        F.round(num("MTOW_lb") * LB, 0).alias("mtow_kg"),
        num("Approach_Speed_knot").alias("approach_speed_kt"),
        blank("ADG").alias("design_group"),
    ).dropDuplicates(["typecode"])


@dp.materialized_view(name="ref_ports", comment="UN/LOCODE seaports (Function starts with 1) with decimal coordinates.")
def ref_ports():
    rx = r"^(\d{2})(\d{2})([NS]) (\d{3})(\d{2})([EW])$"
    c = F.trim("Coordinates")
    part = lambda i: F.regexp_extract(c, rx, i)  # noqa: E731
    deg = lambda d, mi, hemi, neg: F.when(part(hemi) == neg, -1).otherwise(1) * (  # noqa: E731
        part(d).try_cast("double") + part(mi).try_cast("double") / 60)
    w = Window.partitionBy("locode").orderBy("row_order")
    return (
        read_ref("unlocode_code_list.csv")
        .withColumn("row_order", F.monotonically_increasing_id())   # first row in file order wins
        .where(F.col("Function").startswith("1") & c.rlike(rx) & F.col("Location").isNotNull())
        .select(
            F.concat("Country", "Location").alias("locode"),
            F.col("Country").alias("country"),
            F.col("NameWoDiacritics").alias("port_name"),
            deg(1, 2, 3, "S").alias("port_lat"),
            deg(4, 5, 6, "W").alias("port_lon"),
            "row_order",
        )
        .withColumn("rn", F.row_number().over(w)).where("rn = 1").drop("rn", "row_order")
    )


# ===================================== SILVER ==========================================================
@dp.materialized_view(
    name="silver_opensky_states",
    comment="Typed aircraft states with a usable position, plus quality flags (kept, not dropped).",
    cluster_by=["snapshot_date"],
)
@dp.expect_or_drop("valid_icao24", "icao24 RLIKE '^[0-9a-f]{6}$'")
@dp.expect_or_drop("has_position", "latitude IS NOT NULL AND longitude IS NOT NULL AND time_position IS NOT NULL")
@dp.expect_or_drop("position_in_range", "abs(latitude) <= 90 AND abs(longitude) <= 180")
@dp.expect("not_stale", "NOT is_stale_position")
@dp.expect("plausible_speed", "NOT is_speed_suspect")
def silver_opensky_states():
    baro, geo = F.col("baro_altitude"), F.col("geo_altitude")
    emergency = F.create_map(*[F.lit(x) for kv in EMERGENCY.items() for x in kv])
    return (
        spark.read.table("bronze_opensky_states")
        .select(
            ts("snapshot_time").alias("snapshot_time"),
            F.lower("icao24").alias("icao24"),
            blank("callsign").alias("callsign"),
            "origin_country",
            F.timestamp_seconds(num("time_position", "bigint")).alias("time_position"),
            num("latitude").alias("latitude"),
            num("longitude").alias("longitude"),
            num("baro_altitude").alias("baro_altitude"),
            num("geo_altitude").alias("geo_altitude"),
            (F.col("on_ground") == "True").alias("on_ground"),
            num("velocity").alias("velocity"),
            num("true_track").alias("true_track"),
            num("vertical_rate").alias("vertical_rate"),
            blank("squawk").alias("squawk"),
            "source_file",
        )
        .withColumn("snapshot_date", F.to_date("snapshot_time"))
        .withColumn("position_age_s", F.col("snapshot_time").cast("long") - F.col("time_position").cast("long"))
        .withColumn("is_stale_position", F.coalesce(F.col("position_age_s") > STALE_POSITION_S, F.lit(False)))
        .withColumn("is_speed_suspect", F.coalesce(F.col("velocity") > MAX_PLAUSIBLE_MS, F.lit(False)))
        .withColumn("is_altitude_suspect", F.coalesce(
            (F.abs(geo - baro) > ALT_DIFF_M) | (baro < MIN_BARO_M) | ((baro > CEILING_M) & geo.isNull()), F.lit(False)))
        .withColumn("emergency_type", emergency[F.col("squawk")])
        .withColumn("is_test_transponder", F.col("icao24").isin(TEST_ICAO))
    )


@dp.materialized_view(
    name="silver_ais_positions",
    comment="Typed AIS positions; 'not available' sentinels -> NULL, unusable rows dropped, suspect speed flagged.",
    cluster_by=["window_date"],
)
@dp.expect_or_drop("valid_mmsi", "mmsi RLIKE '^[0-9]{9}$'")
@dp.expect_or_drop("has_position", "latitude IS NOT NULL AND longitude IS NOT NULL")
@dp.expect_or_drop("position_in_range", "abs(latitude) <= 90 AND abs(longitude) <= 180")
@dp.expect_or_drop("not_null_island", "NOT (latitude = 0 AND longitude = 0)")
@dp.expect("plausible_speed", "NOT is_speed_suspect")
def silver_ais_positions():
    sog, cog, nav = num("sog_knots"), num("cog_deg"), num("nav_status", "int")
    m = F.col("mmsi")
    mmsi_kind = (
        F.when(m.startswith("111"), "sar_aircraft")
        .when(F.substring(m, 1, 3).isin("970", "972", "974"), "emergency_beacon")
        .when(m.startswith("99"), "aid_to_navigation")
        .when(m.startswith("00"), "base_station")
        .when(F.substring(m, 1, 1).isin(*list("234567")), "ship")
        .otherwise("other")
    )
    return (
        spark.read.table("bronze_ais_positions")
        .select(
            ts("window_start").alias("window_start"),
            "mmsi",
            mmsi_kind.alias("mmsi_kind"),
            blank("ship_name").alias("ship_name"),
            "ais_class",
            num("latitude").alias("latitude"),
            num("longitude").alias("longitude"),
            F.when(sog < SOG_NOT_AVAILABLE, sog).alias("sog_knots"),
            F.when(cog < COG_NOT_AVAILABLE, cog).alias("cog_deg"),
            F.when(nav != NAV_STATUS_UNDEFINED, nav).alias("nav_status"),
            num("reports_in_window", "int").alias("reports_in_window"),
            "source_file",
        )
        .withColumn("window_date", F.to_date("window_start"))
        .withColumn("is_speed_suspect", F.coalesce(
            (F.col("sog_knots") > MAX_PLAUSIBLE_SHIP_KNOTS) & (F.col("mmsi_kind") != "sar_aircraft"), F.lit(False)))
    )


@dp.materialized_view(name="silver_ais_ships", comment="One row per MMSI: the LATEST name/IMO/type/size it reported.")
@dp.expect_or_drop("valid_mmsi", "mmsi RLIKE '^[0-9]{9}$'")
def silver_ais_ships():
    s = spark.read.table("bronze_ais_static").select(
        ts("window_start").alias("window_start"), "mmsi",
        blank("ship_name").alias("ship_name"), blank("imo").alias("imo"), blank("call_sign").alias("call_sign"),
        num("ship_type", "int").alias("ship_type"), blank("destination").alias("destination"),
        num("length_m").alias("length_m"), num("width_m").alias("width_m"), num("draught_m").alias("draught_m"),
    )
    latest = lambda c: F.max_by(c, F.when(F.col(c).isNotNull(), F.col("window_start"))).alias(c)  # noqa: E731
    return (
        s.groupBy("mmsi").agg(
            *[latest(c) for c in ["ship_name", "imo", "call_sign", "ship_type", "destination", "length_m", "width_m", "draught_m"]],
            F.countDistinct("ship_name").alias("names_used"),
            F.min("window_start").alias("first_report"),
            F.max("window_start").alias("last_report"),
        )
        # valid IMO = seagoing ship; inland barges mostly have none (they would flood the port ranking)
        .withColumn("imo_valid", imo_is_valid(F.col("imo")))
    )


@dp.materialized_view(name="silver_ship_stops",
                      comment="Gaps-and-islands step 1: one row per continuous stop of a ship (not yet a port call).")
def silver_ship_stops():
    """1. is_stopped per row. 2. new_island = state changed OR gap > MAX_GAP_MINUTES (LAG over the
    ship's rows). 3. island_id = running SUM of new_island. 4. group by (mmsi, island_id)."""
    pos = spark.read.table("silver_ais_positions")
    w = Window.partitionBy("mmsi").orderBy("window_start")
    sog = F.col("sog_knots")
    p = (
        pos.where(F.col("mmsi_kind") == "ship")
        .select("mmsi", "window_start", "sog_knots", "nav_status", "latitude", "longitude")
        .withColumn("is_stopped", F.coalesce(
            (sog < STOP_KNOTS) | ((sog < MOORED_DRIFT_KNOTS) & F.col("nav_status").isin(1, 5)), F.lit(False)))
        .withColumn("prev_stopped", F.lag("is_stopped").over(w))
        .withColumn("gap_min", (F.col("window_start").cast("long") - F.lag("window_start").over(w).cast("long")) / 60)
        .withColumn("new_island", F.coalesce(
            F.col("prev_stopped").isNull() | (F.col("is_stopped") != F.col("prev_stopped"))
            | (F.col("gap_min") > MAX_GAP_MINUTES), F.lit(True)).cast("int"))
        .withColumn("island_id", F.sum("new_island").over(w.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
    )
    # A stop touching the first/last window of the WHOLE dataset may be longer than we can see.
    bounds = pos.agg(F.min("window_start").alias("data_start"), F.max("window_start").alias("data_end"))
    margin = F.expr("INTERVAL 30 MINUTES")
    return (
        p.where("is_stopped")
        .groupBy("mmsi", "island_id")
        .agg(
            F.min("window_start").alias("arrival"),
            F.max("window_start").alias("departure"),
            F.avg("latitude").alias("lat"),
            F.avg("longitude").alias("lon"),
            F.count("*").alias("n_fixes"),
        )
        .crossJoin(bounds)
        .withColumn("is_truncated", (F.col("arrival") <= F.col("data_start") + margin)
                    | (F.col("departure") >= F.col("data_end") - margin))
        .drop("data_start", "data_end")
    )


# ===================================== GOLD: DIMENSIONS ================================================
@dp.materialized_view(name="dim_date", comment="One row per day covered by either source.")
def dim_date():
    days = (spark.read.table("silver_opensky_states").select(F.col("snapshot_date").alias("d"))
            .union(spark.read.table("silver_ais_positions").select(F.col("window_date").alias("d"))))
    bounds = days.agg(F.min("d").alias("a"), F.max("d").alias("b"))
    return (
        bounds.select(F.explode(F.sequence("a", "b")).alias("date"))
        .select(
            F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
            "date",
            F.year("date").alias("year"),
            F.month("date").alias("month"),
            F.date_format("date", "MMM").alias("month_name"),
            F.weekofyear("date").alias("iso_week"),
            F.date_format("date", "EEEE").alias("weekday"),
            F.dayofweek("date").isin(1, 7).alias("is_weekend"),
        )
    )


@dp.materialized_view(name="dim_aircraft", comment="One row per transponder (icao24), enriched with the OpenSky registry.")
def dim_aircraft():
    s = spark.read.table("silver_opensky_states")
    seen = s.groupBy("icao24").agg(
        F.max_by("callsign", F.when(F.col("callsign").isNotNull(), F.col("snapshot_time"))).alias("last_callsign"),
        F.countDistinct("callsign").alias("callsigns_used"),
        F.max_by("origin_country", "snapshot_time").alias("origin_country"),
        F.min("snapshot_time").alias("first_seen"),
        F.max("snapshot_time").alias("last_seen"),
        F.max("is_test_transponder").alias("is_test_transponder"),
        # > 5% altitude-suspect states and at least 5 of them: don't trust any altitude of this transponder
        ((F.avg(F.col("is_altitude_suspect").cast("int")) > 0.05) & (F.sum(F.col("is_altitude_suspect").cast("int")) >= 5))
        .alias("is_altitude_unreliable"),
    )
    reg = spark.read.table("ref_aircraft_registry")
    return (
        seen.join(reg, "icao24", "left")
        # no type in the registry -> 'UNKN', so every aircraft finds a row in dim_aircraft_type
        .withColumn("typecode", F.coalesce("typecode", F.lit("UNKN")))
    )


@dp.materialized_view(name="dim_aircraft_type",
                      comment="One row per aircraft type seen: class, engines, wake category, FAA dimensions.")
def dim_aircraft_type():
    seen = spark.read.table("dim_aircraft").groupBy("typecode").agg(F.count("*").alias("aircraft_seen"))
    t = spark.read.table("ref_aircraft_types")
    d = spark.read.table("ref_aircraft_dimensions")
    desc, eng, wtc = F.col("icao_description"), F.col("engine_type"), F.col("wtc")
    # Class from ICAO Doc 8643: first letter of the description = H helicopter / T tiltrotor,
    # jets split by wake category (L light = business jets, M medium = A320/737 family, H/J heavy).
    aircraft_class = (
        F.when(F.col("typecode") == "UNKN", "Unknown type")
        .when(desc.startswith("H") | desc.startswith("T"), "Helicopter")
        .when((eng == "Jet") & wtc.isin("H", "J"), "Heavy jet")
        .when((eng == "Jet") & (wtc == "M"), "Medium jet")
        .when(eng == "Jet", "Light jet")
        .when(eng.isin("Turboprop/Turboshaft", "Turboprop"), "Turboprop")
        .when(eng == "Piston", "Piston")
        .otherwise("Other")
    )
    return (
        seen.join(t, "typecode", "left").join(d, "typecode", "left")
        .select(
            "typecode",
            F.coalesce("faa_model_name", "icao_model_name", F.when(F.col("typecode") == "UNKN", F.lit("Unknown type")),
                       F.col("typecode")).alias("model_name"),
            F.coalesce("faa_manufacturer", "icao_manufacturer").alias("manufacturer"),
            aircraft_class.alias("aircraft_class"),
            "engine_type", "engines", "wtc",
            "wingspan_m", "length_m", "tail_height_m", "mtow_kg", "approach_speed_kt", "design_group",
            F.col("wingspan_m").isNotNull().alias("has_dimensions"),
            "aircraft_seen",
        )
    )


@dp.materialized_view(name="dim_ship_type", comment="AIS ship type code -> category.")
def dim_ship_type():
    return spark.read.table("ref_ais_ship_types")


@dp.materialized_view(name="dim_nav_status", comment="AIS navigational status code -> label.")
def dim_nav_status():
    return spark.read.table("ref_ais_nav_status")


@dp.materialized_view(name="dim_ship", comment="One row per MMSI seen in positions, with the latest static details.")
def dim_ship():
    pos = spark.read.table("silver_ais_positions").groupBy("mmsi").agg(
        F.first("mmsi_kind").alias("mmsi_kind"),
        F.max_by("ship_name", F.when(F.col("ship_name").isNotNull(), F.col("window_start"))).alias("name_from_positions"),
        F.first("ais_class").alias("ais_class"),
        F.min("window_start").alias("first_seen"),
        F.max("window_start").alias("last_seen"),
    )
    ships = spark.read.table("silver_ais_ships")
    types = spark.read.table("ref_ais_ship_types")
    return (
        pos.join(ships, "mmsi", "left")
        .join(types, "ship_type", "left")
        .withColumn("ship_name", F.coalesce("ship_name", "name_from_positions"))
        .withColumn("ship_category", F.coalesce("ship_category", F.lit("unknown")))
        .withColumn("is_seagoing", F.coalesce("imo_valid", F.lit(False)))
        .drop("name_from_positions", "imo_valid")
        .withColumn("flag_mid", F.substring("mmsi", 1, 3))  # Maritime Identification Digits = flag state
    )


@dp.materialized_view(name="dim_port",
                      comment="UN/LOCODE seaports + how many ships named each one as destination (ranks main ports).")
def dim_port():
    """Crews write 'NLRTM' / 'ROTTERDAM', not 'Botlek': ships naming a port = its weight. Matched by
    code ('NL RTM' -> 'NLRTM', 'DEBRV>NLRTM' -> last part) and by name (all countries with that name)."""
    ports = spark.read.table("ref_ports")
    dest = F.trim(F.element_at(F.split(F.col("destination"), ">"), -1))
    d = (
        spark.read.table("bronze_ais_static")
        .select("mmsi", blank("destination").alias("destination"))
        .where(F.col("destination").isNotNull() & ~F.col("destination").rlike(DEST_PLACEHOLDER))
        .select("mmsi", F.regexp_replace(dest, r"^([A-Z]{2})\s+([A-Z0-9]{3})$", "$1$2").alias("dest_clean"))
        .distinct()
    )
    by_code = d.join(ports.select(F.col("locode").alias("dest_clean")), "dest_clean").select("mmsi", F.col("dest_clean").alias("locode"))
    by_name = d.join(ports.select(F.upper("port_name").alias("dest_clean"), "locode"), "dest_clean").select("mmsi", "locode")
    weights = by_code.union(by_name).groupBy("locode").agg(F.countDistinct("mmsi").alias("dest_ships"))
    return ports.join(weights, "locode", "left").withColumn("dest_ships", F.coalesce("dest_ships", F.lit(0)))


# ===================================== GOLD: FACTS =====================================================
@dp.materialized_view(name="fact_aircraft_daily", comment="One row per aircraft per day. Grain: date_key x icao24.")
def fact_aircraft_daily():
    s = spark.read.table("silver_opensky_states").where(~F.col("is_test_transponder"))
    good_alt = F.when(~F.col("is_altitude_suspect"), F.col("baro_altitude"))
    good_speed = F.when(~F.col("is_speed_suspect"), F.col("velocity"))
    return s.groupBy("snapshot_date", "icao24").agg(
        F.count("*").alias("snapshots"),
        F.sum((~F.col("on_ground")).cast("int")).alias("airborne_snapshots"),
        F.round(F.max(good_alt), 0).alias("max_baro_altitude_m"),
        F.round(F.avg(F.when(~F.col("on_ground"), good_speed)) * 3.6, 1).alias("avg_airborne_speed_kmh"),
        F.sum(F.col("emergency_type").isNotNull().cast("int")).alias("emergency_snapshots"),
        F.max("emergency_type").alias("emergency_type"),
        F.sum(F.col("is_speed_suspect").cast("int")).alias("speed_suspect_snapshots"),
        F.min("snapshot_time").alias("first_seen"),
        F.max("snapshot_time").alias("last_seen"),
    ).select(F.date_format("snapshot_date", "yyyyMMdd").cast("int").alias("date_key"), "*").drop("snapshot_date")


@dp.materialized_view(name="fact_ship_daily", comment="One row per ship per day. Grain: date_key x mmsi.")
def fact_ship_daily():
    p = spark.read.table("silver_ais_positions")
    good_sog = F.when(~F.col("is_speed_suspect"), F.col("sog_knots"))
    return p.groupBy("window_date", "mmsi").agg(
        F.count("*").alias("position_windows"),
        F.round(F.avg(good_sog), 2).alias("avg_sog_knots"),
        F.round(F.max(good_sog), 1).alias("max_sog_knots"),
        F.sum((F.col("sog_knots") < 0.5).cast("int")).alias("stationary_windows"),
        F.max_by("nav_status", "window_start").alias("last_nav_status"),
        F.round(F.avg("latitude"), 4).alias("avg_latitude"),
        F.round(F.avg("longitude"), 4).alias("avg_longitude"),
        F.sum(F.col("is_speed_suspect").cast("int")).alias("speed_suspect_windows"),
    ).select(F.date_format("window_date", "yyyyMMdd").cast("int").alias("date_key"), "*").drop("window_date")


@dp.materialized_view(name="fact_traffic_hourly", comment="Per UTC hour: distinct aircraft (all / airborne) and ships seen.")
def fact_traffic_hourly():
    air = (spark.read.table("silver_opensky_states").where(~F.col("is_test_transponder"))
           .groupBy(F.date_trunc("hour", "snapshot_time").alias("hour_utc"))
           .agg(F.countDistinct("icao24").alias("aircraft"),
                F.countDistinct(F.when(~F.col("on_ground"), F.col("icao24"))).alias("aircraft_airborne")))
    sea = (spark.read.table("silver_ais_positions").where(F.col("mmsi_kind") == "ship")
           .groupBy(F.date_trunc("hour", "window_start").alias("hour_utc"))
           .agg(F.countDistinct("mmsi").alias("ships"),
                F.countDistinct(F.when(F.col("sog_knots") >= 0.5, F.col("mmsi"))).alias("ships_moving")))
    return (
        air.join(sea, "hour_utc", "full")
        .select(
            F.date_format("hour_utc", "yyyyMMdd").cast("int").alias("date_key"),
            "hour_utc",
            F.hour("hour_utc").alias("hour_of_day"),
            "aircraft", "aircraft_airborne", "ships", "ships_moving",
        )
    )


@dp.materialized_view(name="fact_port_calls",
                      comment="One row per port call (stop >= 60 min within 20 km of a seaport). Grain: call_id.")
def fact_port_calls():
    stops = spark.read.table("silver_ship_stops")
    ports = spark.read.table("dim_port")
    # Spatial join without a cross join: each port copied into its grid cell and the 8 around it,
    # then a normal equi-join on (cy, cx). Cells (55 km) are bigger than the 20 km radius.
    cell = lambda c: F.floor(F.col(c) / PORT_GRID_DEG).cast("int")  # noqa: E731
    cells = (
        ports.withColumn("dy", F.explode(F.array(F.lit(-1), F.lit(0), F.lit(1))))
        .withColumn("dx", F.explode(F.array(F.lit(-1), F.lit(0), F.lit(1))))
        .select("locode", "country", "port_name", "port_lat", "port_lon", "dest_ships",
                (cell("port_lat") + F.col("dy")).alias("cy"), (cell("port_lon") + F.col("dx")).alias("cx"))
    )
    best = Window.partitionBy("mmsi", "island_id").orderBy(F.col("dest_ships").desc(), "port_km", "locode")
    matched = (
        stops.select("mmsi", "island_id", "lat", "lon", cell("lat").alias("cy"), cell("lon").alias("cx"))
        .join(cells, ["cy", "cx"])
        .withColumn("port_km", haversine_km(F.col("lat"), F.col("lon"), F.col("port_lat"), F.col("port_lon")))
        .where(F.col("port_km") <= PORT_RADIUS_KM)
        # most-named port in range wins (a stop in Botlek counts for Rotterdam), then distance, then code
        .withColumn("rn", F.row_number().over(best)).where("rn = 1")
        .select("mmsi", "island_id", "locode", "port_km")
    )
    s = stops.join(matched, ["mmsi", "island_id"], "left")

    # Gaps-and-islands step 2: consecutive stops at the SAME port < 2 h apart = one call.
    w = Window.partitionBy("mmsi").orderBy("arrival")
    new_call = F.coalesce(
        F.col("locode").isNull() | (F.col("locode") != F.lag("locode").over(w))
        | ((F.col("arrival").cast("long") - F.lag("departure").over(w).cast("long")) / 3600 > MERGE_GAP_HOURS),
        F.lit(True))
    calls = (
        s.withColumn("new_call", new_call.cast("int"))
        .withColumn("call_no", F.sum("new_call").over(w.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
        .groupBy("mmsi", "call_no")
        .agg(
            F.min("arrival").alias("arrival"), F.max("departure").alias("departure"),
            F.max("locode").alias("locode"), F.min("port_km").alias("port_km"),
            F.avg("lat").alias("lat"), F.avg("lon").alias("lon"),
            F.sum("n_fixes").alias("n_fixes"), F.count("*").alias("n_stops_merged"),
            F.max(F.col("is_truncated").cast("int")).cast("boolean").alias("is_truncated"),
        )
        .withColumn("duration_h", F.round((F.col("departure").cast("long") - F.col("arrival").cast("long")) / 3600, 2))
        .where(F.col("duration_h") * 60 >= MIN_STOP_MINUTES)
    )
    ships = spark.read.table("dim_ship").select("mmsi", "ship_category", "is_seagoing")
    return (
        calls.join(ships, "mmsi", "left")
        .select(
            F.concat_ws("-", "mmsi", "call_no").alias("call_id"),
            "mmsi", "locode",
            F.date_format("arrival", "yyyyMMdd").cast("int").alias("date_key"),   # day of ARRIVAL
            "arrival", "departure", "duration_h", "is_truncated",
            F.coalesce("is_seagoing", F.lit(False)).alias("is_seagoing"),
            "n_stops_merged", "n_fixes", F.round("port_km", 2).alias("port_km"),
            F.round("lat", 4).alias("lat"), F.round("lon", 4).alias("lon"),
        )
    )


@dp.materialized_view(name="fact_quality_daily",
                      comment="Per source dataset and file day: rows in bronze, kept in silver, dropped, flagged.")
def fact_quality_daily():
    day = lambda: F.to_date(F.regexp_extract("source_file", r"(\d{4}-\d{2}-\d{2})", 1))  # noqa: E731

    def one(dataset, bronze, silver, flags):
        b = spark.read.table(bronze).groupBy(day().alias("d")).agg(F.count("*").alias("bronze_rows"))
        s = spark.read.table(silver).groupBy(day().alias("d")).agg(
            F.count("*").alias("silver_rows"),
            *[F.sum(F.col(c).cast("int")).alias(name) for name, c in flags.items()])
        for name in ("flag_stale", "flag_speed", "flag_altitude"):
            if name not in flags:
                s = s.withColumn(name, F.lit(0).cast("bigint"))
        return (b.join(s, "d", "left")
                .select(F.lit(dataset).alias("dataset"),
                        F.date_format("d", "yyyyMMdd").cast("int").alias("date_key"),
                        "bronze_rows", F.coalesce("silver_rows", F.lit(0)).alias("silver_rows"),
                        (F.col("bronze_rows") - F.coalesce("silver_rows", F.lit(0))).alias("dropped_rows"),
                        "flag_stale", "flag_speed", "flag_altitude"))

    air = one("Aircraft states", "bronze_opensky_states", "silver_opensky_states",
              {"flag_stale": "is_stale_position", "flag_speed": "is_speed_suspect", "flag_altitude": "is_altitude_suspect"})
    sea = one("Ship positions", "bronze_ais_positions", "silver_ais_positions", {"flag_speed": "is_speed_suspect"})
    return air.unionByName(sea)
