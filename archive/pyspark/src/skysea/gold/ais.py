"""Silver AIS -> gold: tables that answer questions.

gold.ports             UN/LOCODE seaports with decimal coordinates (reference)
gold.ais_port_calls    one row per stop of a ship near a port (gaps-and-islands over positions)
gold.ais_port_traffic  calls and distinct ships per port and ship category
gold.ais_eta_accuracy  announced ETA vs the actual arrival found in the port calls
gold.ais_fleet_hourly  ships seen / moving per hour and category (the series we forecast over gaps)
"""
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from skysea.silver.ais import haversine_km

STOP_KNOTS = 0.5           # slower than this = not moving
MOORED_DRIFT_KNOTS = 1.0   # a ship reporting moored/at anchor drifts up to ~1 kn on GPS noise: still stopped
MERGE_GAP_HOURS = 2        # two stops at the same port less than 2 h apart = one port call (shifting berth, noise)
MAX_GAP_MINUTES = 30       # a stop continues across up to 30 min of missing fixes (coverage holes)
MIN_STOP_MINUTES = 60      # shorter calls (traffic, locks, pilot pick-up) are not port calls
PORT_RADIUS_KM = 20        # stop centroid must be this close to a port's LOCODE point
GRID_DEG = 0.5             # grid cell size for the spatial join (0.5 deg lat ~ 55 km > PORT_RADIUS_KM)


# ---------------------------------------------------------------------------------------------
# ports reference
# ---------------------------------------------------------------------------------------------
def _dm_to_deg(deg: Column, minutes: Column, hemi: Column, negative: str) -> Column:
    v = deg.cast("double") + minutes.cast("double") / 60
    return F.when(hemi == negative, -v).otherwise(v)


def build_ports(spark: SparkSession, csv_path: str) -> DataFrame:
    """UN/LOCODE rows whose Function starts with '1' are seaports. Coordinates look like
    '5331N 00956E' (degrees + minutes) -> 53.517, 9.933."""
    raw = spark.read.option("header", True).csv(csv_path)
    rx = r"^(\d{2})(\d{2})([NS]) (\d{3})(\d{2})([EW])$"
    c = F.trim("Coordinates")
    return (
        raw.where(F.col("Function").startswith("1") & c.rlike(rx) & F.col("Location").isNotNull())
        .select(
            F.concat("Country", "Location").alias("locode"),
            F.col("Country").alias("country"),
            F.col("NameWoDiacritics").alias("port_name"),
            _dm_to_deg(F.regexp_extract(c, rx, 1), F.regexp_extract(c, rx, 2), F.regexp_extract(c, rx, 3), "S").alias("port_lat"),
            _dm_to_deg(F.regexp_extract(c, rx, 4), F.regexp_extract(c, rx, 5), F.regexp_extract(c, rx, 6), "W").alias("port_lon"),
        )
        .dropDuplicates(["locode"])
    )


# ---------------------------------------------------------------------------------------------
# port calls: gaps-and-islands
# ---------------------------------------------------------------------------------------------
def find_stops(positions: DataFrame) -> DataFrame:
    """Gaps-and-islands: turn 19M position rows into one row per continuous stop.

    1. is_stopped per row.
    2. new_island = 1 when this row starts a new run: state changed, or the gap since the
       previous fix is > MAX_GAP_MINUTES (lag over a window partitioned by mmsi).
    3. island_id = running sum of new_island -> every row of one run shares the same id.
    4. groupBy(mmsi, island_id) -> start, end, centroid of the stop.
    """
    w = Window.partitionBy("mmsi").orderBy("window_start")
    p = (
        positions.where((F.col("mmsi_kind") == "ship") & ~F.col("is_position_jump"))
        .withColumn("is_stopped", F.coalesce(
            (F.col("sog_knots") < STOP_KNOTS)
            | ((F.col("sog_knots") < MOORED_DRIFT_KNOTS) & F.col("nav_status").isin(1, 5)), F.lit(False)))
        .withColumn("prev_stopped", F.lag("is_stopped").over(w))
        .withColumn("gap_min", (F.unix_timestamp("window_start") - F.unix_timestamp(F.lag("window_start").over(w))) / 60)
        .withColumn(
            "new_island",
            (F.col("prev_stopped").isNull() | (F.col("is_stopped") != F.col("prev_stopped"))
             | (F.col("gap_min") > MAX_GAP_MINUTES)).cast("int"),
        )
        .withColumn("island_id", F.sum("new_island").over(w.rowsBetween(Window.unboundedPreceding, 0)))
    )
    data_start, data_end = positions.agg(F.min("window_start"), F.max("window_start")).first()
    stops = (
        p.where("is_stopped")
        .groupBy("mmsi", "island_id")
        .agg(
            F.min("window_start").alias("arrival"),
            F.max("window_start").alias("departure"),
            F.avg("latitude").alias("lat"),
            F.avg("longitude").alias("lon"),
            F.count("*").alias("n_fixes"),
            F.mode("nav_status_label").alias("nav_status_label"),
        )
        # A stop that touches the first/last window may have started earlier or ended later
        # than we can see: keep it, but say so (its duration is a lower bound).
        .withColumn("is_truncated", (F.col("arrival") <= F.lit(data_start) + F.expr("INTERVAL 30 MINUTES"))
                    | (F.col("departure") >= F.lit(data_end) - F.expr("INTERVAL 30 MINUTES")))
    )
    return stops


def grid_cell(lat: Column, lon: Column) -> Column:
    return F.concat_ws(":", F.floor(lat / GRID_DEG), F.floor(lon / GRID_DEG))


def port_weights(reports: DataFrame, ports: DataFrame) -> DataFrame:
    """How many ships named each LOCODE as their destination. Crews write 'NLRTM' / 'ROTTERDAM',
    not 'Botlek' or 'Pernis', so this ranks the main port of an area above its sub-areas."""
    return (resolve_destinations(reports, ports, require_eta=False)
            .groupBy(F.col("dest_locode").alias("locode")).agg(F.countDistinct("mmsi").alias("dest_ships")))


def nearest_port(stops: DataFrame, ports: DataFrame) -> DataFrame:
    """Spatial join without a cross join.

    stops x ports would be ~10^5 x 10^4 = a billion distance calculations. Instead each port is
    copied into its own grid cell and the 8 cells around it (cells are bigger than PORT_RADIUS_KM,
    so every port within range of a stop is in the stop's cell). The join on cell id is a normal
    equi-join, and the 100k-row port side is small enough to broadcast.

    Among ports in range, the one most ships name as destination wins (port AREA: a stop in
    Botlek counts for Rotterdam); distance only breaks ties. ports needs a dest_ships column.
    """
    offsets = F.array(*[F.struct(F.lit(a).alias("dy"), F.lit(b).alias("dx")) for a in (-1, 0, 1) for b in (-1, 0, 1)])
    port_cells = (
        ports.withColumn("o", F.explode(offsets))
        .withColumn("cell", F.concat_ws(":", F.floor(F.col("port_lat") / GRID_DEG) + F.col("o.dy"),
                                        F.floor(F.col("port_lon") / GRID_DEG) + F.col("o.dx")))
        .drop("o")
    )
    cand = (
        stops.withColumn("cell", grid_cell(F.col("lat"), F.col("lon")))
        .join(F.broadcast(port_cells), "cell")
        .withColumn("port_km", haversine_km(F.col("lat"), F.col("lon"), F.col("port_lat"), F.col("port_lon")))
        .where(F.col("port_km") <= PORT_RADIUS_KM)
    )
    best = Window.partitionBy("mmsi", "island_id").orderBy(F.desc("dest_ships"), "port_km")
    matched = cand.withColumn("rn", F.row_number().over(best)).where("rn = 1").drop("rn", "cell")
    # Stops with no port in range (anchorages far out, offshore platforms, open sea) are kept with NULL port.
    return stops.join(matched.select("mmsi", "island_id", "locode", "country", "port_name", "port_km"),
                      ["mmsi", "island_id"], "left")


def merge_stops(stops: DataFrame) -> DataFrame:
    """Second gaps-and-islands, one level up: consecutive stops of a ship at the SAME port less
    than MERGE_GAP_HOURS apart become one call (berth shift, speed noise above the threshold)."""
    w = Window.partitionBy("mmsi").orderBy("arrival")
    prev_port, prev_dep = F.lag("locode").over(w), F.lag("departure").over(w)
    new_call = F.coalesce(
        F.col("locode").isNull() | (F.col("locode") != prev_port)
        | ((F.unix_timestamp("arrival") - F.unix_timestamp(prev_dep)) / 3600 > MERGE_GAP_HOURS),
        F.lit(True),  # first stop of a ship (prev is NULL)
    ).cast("int")
    s = stops.withColumn("call_no", F.sum(new_call).over(w.rowsBetween(Window.unboundedPreceding, 0)))
    return (
        s.groupBy("mmsi", "call_no")
        .agg(
            F.min("arrival").alias("arrival"), F.max("departure").alias("departure"),
            F.first("locode", ignorenulls=True).alias("locode"), F.first("port_name", ignorenulls=True).alias("port_name"),
            F.first("country", ignorenulls=True).alias("country"), F.min("port_km").alias("port_km"),
            F.avg("lat").alias("lat"), F.avg("lon").alias("lon"),
            F.sum("n_fixes").alias("n_fixes"), F.count("*").alias("n_stops_merged"),
            F.mode("nav_status_label").alias("nav_status_label"), F.max("is_truncated").alias("is_truncated"),
        )
        .withColumn("duration_h", F.round((F.unix_timestamp("departure") - F.unix_timestamp("arrival")) / 3600, 2))
        .where(F.col("duration_h") * 60 >= MIN_STOP_MINUTES)
    )


def build_port_calls(positions: DataFrame, ports: DataFrame, ships: DataFrame) -> DataFrame:
    calls = merge_stops(nearest_port(find_stops(positions), ports))
    return calls.join(
        ships.select("mmsi", "ship_name", "ship_category", "length_m", F.col("imo_valid").alias("is_seagoing")),
        "mmsi", "left",
    ).withColumn("is_seagoing", F.coalesce("is_seagoing", F.lit(False))).withColumn(
        "call_id", F.concat_ws("-", "mmsi", "call_no"))


def build_port_traffic(calls: DataFrame) -> DataFrame:
    return (
        calls.where(F.col("locode").isNotNull())
        # is_seagoing = ship has a valid IMO number. Inland barges (huge on the Rhine/Dutch rivers)
        # mostly do not, and would otherwise make every Dutch village a "top port".
        .groupBy("locode", "port_name", "country", F.coalesce("ship_category", F.lit("unknown")).alias("ship_category"),
                 "is_seagoing")
        .agg(
            F.count("*").alias("calls"),
            F.countDistinct("mmsi").alias("ships"),
            F.round(F.percentile_approx("duration_h", 0.5), 1).alias("median_stay_h"),
        )
    )


# ---------------------------------------------------------------------------------------------
# ETA accuracy
# ---------------------------------------------------------------------------------------------
def resolve_destinations(reports: DataFrame, ports: DataFrame, require_eta: bool = True) -> DataFrame:
    """Hand-typed destination -> candidate LOCODEs.
      'NLRTM' / 'NL RTM'          -> code match
      'DEBRV>NLRTM' / 'X > Y'     -> the part after the last '>' (route notation)
      'ROTTERDAM'                 -> name match; a name used in several countries gives several
                                     candidates, and the ship's actual port call decides.
    """
    dest = F.trim(F.element_at(F.split(F.col("destination"), ">"), -1))
    code = F.regexp_replace(dest, r"^([A-Z]{2})\s+([A-Z0-9]{3})$", "$1$2")
    r = reports.where(~F.col("destination_is_placeholder") & F.col("destination").isNotNull())
    if require_eta:
        r = r.where(F.col("eta").isNotNull())
    r = r.withColumn("dest_clean", code)
    by_code = r.join(F.broadcast(ports.select("locode")), r.dest_clean == ports.locode)
    names = ports.select(F.upper("port_name").alias("pname"), F.col("locode").alias("locode"))
    by_name = r.join(F.broadcast(names), r.dest_clean == names.pname).drop("pname")
    return by_code.unionByName(by_name).withColumnRenamed("locode", "dest_locode").dropDuplicates(
        ["mmsi", "destination", "eta", "dest_locode"])


def build_eta_accuracy(reports: DataFrame, calls: DataFrame, ports: DataFrame) -> DataFrame:
    """For each announced (destination, ETA): the first port call at that destination that STARTS
    after the ship first announced it. error_h > 0 = arrived later than announced.

    lead_h = hours between FIRST announcing this ETA and arriving (how far ahead the forecast was).
    is_stale_eta = the ETA was already > 1 h in the past when first announced: the crew never
    updated the field. Kept as its own finding, excluded from the accuracy numbers."""
    cand = resolve_destinations(reports, ports)
    c = calls.where(F.col("locode").isNotNull() & ~F.col("is_truncated")).select(
        "mmsi", F.col("locode").alias("dest_locode"), "arrival", "call_id", "is_seagoing", "ship_category")
    j = cand.join(c, ["mmsi", "dest_locode"]).where(F.col("arrival") >= F.col("first_reported"))
    first = Window.partitionBy("mmsi", "destination", "eta").orderBy("arrival")
    return (
        j.withColumn("rn", F.row_number().over(first)).where("rn = 1").drop("rn")
        .withColumn("error_h", F.round((F.unix_timestamp("arrival") - F.unix_timestamp("eta")) / 3600, 2))
        .withColumn("lead_h", F.round((F.unix_timestamp("arrival") - F.unix_timestamp("first_reported")) / 3600, 2))
        .withColumn("is_stale_eta", F.col("eta") < F.col("first_reported") - F.expr("INTERVAL 1 HOUR"))
        .select("mmsi", "destination", "dest_locode", "eta", "first_reported", "last_reported",
                "arrival", "call_id", "error_h", "lead_h", "is_stale_eta", "is_seagoing", "ship_category", "n_reports")
    )


# ---------------------------------------------------------------------------------------------
# fleet activity
# ---------------------------------------------------------------------------------------------
def build_fleet_hourly(positions: DataFrame, ships: DataFrame) -> DataFrame:
    """Distinct ships per hour and category. Counting DISTINCT mmsi (not rows) because one hour
    holds up to 12 windows per ship."""
    p = positions.where(F.col("mmsi_kind") == "ship").join(
        F.broadcast(ships.select("mmsi", "ship_category")), "mmsi", "left")
    return (
        p.groupBy(F.date_trunc("hour", "window_start").alias("hour"),
                  F.coalesce("ship_category", F.lit("unknown")).alias("ship_category"))
        .agg(
            F.countDistinct("mmsi").alias("ships"),
            F.countDistinct(F.when(F.col("sog_knots") >= STOP_KNOTS, F.col("mmsi"))).alias("moving_ships"),
            F.countDistinct("window_start").alias("windows"),
        )
        .withColumn("is_forecast", F.lit(False))  # gap rows added later by the forecasting step
    )
