"""Silver OpenSky -> gold: tables that answer questions.

gold\\airports.parquet                 OurAirports large + medium airports (reference)
gold\\opensky_flights.parquet          one row per flight, rebuilt from 5/10-minute snapshots (gaps-and-islands)
gold\\opensky_airport_traffic.parquet  departures / arrivals / aircraft per airport per day
gold\\opensky_altitude_top.parquet     highest flights, confirmed by 2 fixes, classified (balloon / aircraft)
gold\\opensky_emergencies.parquet      squawk 7500/7600/7700 episodes + the flight they belong to
gold\\opensky_gnss_anomalies.parquet   1-degree cells per day where aircraft report impossible speeds/jumps
gold\\opensky_air_hourly.parquet       aircraft airborne per hour (the series we forecast over gaps)
"""
import polars as pl

from skysea.silver.ais import haversine_km

MAX_GAP_MINUTES = 30      # no snapshot of an aircraft for longer than this = a new island (new flight)
LINK_MINUTES = 30         # a ground island this close before/after a flight = where it took off / landed
LOW_ALT_M = 1200          # first/last airborne fix below ~4,000 ft = climbing out / on approach
GROUND_RADIUS_KM = 5      # on-ground fix -> airport within 5 km
APPROACH_RADIUS_KM = 25   # low-altitude fix -> airport within 25 km (5 min at 250 km/h on approach)
GRID_DEG = 1.0            # airport grid cells; 1 deg keeps 3x3 cells wider than 25 km up to ~75 N
N_BUCKETS = 8
BALLOON_MS = 100          # above 15 km nothing with wings flies slower than this: it is a balloon
JET_MS = 100              # median ground speed >= 100 m/s (~195 kn) = airliner/jet; slower = light aircraft,
                          #   helicopters, flight schools (they made Mesa<->Phoenix the "busiest route")
ANOMALY_MIN_AIRCRAFT = 3

FLIGHT_COLS = ["icao24", "snapshot_time", "latitude", "longitude", "baro_altitude", "on_ground", "velocity",
               "callsign", "origin_country", "is_stale_position", "is_position_jump", "is_speed_suspect",
               "is_altitude_suspect", "is_altitude_jump", "is_test_transponder"]


def bucket(i: int) -> pl.Expr:
    """Same trick as AIS port calls: a flight only depends on its own aircraft's rows."""
    return pl.col("icao24").hash(seed=42) % N_BUCKETS == i


# ---------------------------------------------------------------------------------------------
# airports reference
# ---------------------------------------------------------------------------------------------
def build_airports(csv_path: str) -> pl.DataFrame:
    rank = {"large_airport": 1, "medium_airport": 2}
    return (
        pl.read_csv(csv_path, infer_schema=False)
        .filter(pl.col("type").is_in(list(rank)))
        .select(
            pl.col("ident").alias("airport"),            # ICAO-style code, e.g. EDDH
            pl.col("iata_code").alias("iata"),           # HAM
            "name", pl.col("iso_country").alias("country"), "municipality", "type",
            pl.col("type").replace_strict(rank, return_dtype=pl.Int8).alias("type_rank"),
            pl.col("latitude_deg").cast(pl.Float64).alias("ap_lat"),
            pl.col("longitude_deg").cast(pl.Float64).alias("ap_lon"),
            pl.col("elevation_ft").cast(pl.Int32, strict=False),
        )
    )


def airport_cells(airports: pl.DataFrame) -> pl.DataFrame:
    offsets = pl.DataFrame({"dy": [a for a in (-1, 0, 1) for _ in range(3)], "dx": [-1, 0, 1] * 3})
    return (
        airports.with_columns((pl.col("ap_lat") / GRID_DEG).floor().cast(pl.Int32).alias("cy"),
                              (pl.col("ap_lon") / GRID_DEG).floor().cast(pl.Int32).alias("cx"))
        .join(offsets, how="cross")
        .with_columns((pl.col("cy") + pl.col("dy")).alias("cy"), (pl.col("cx") + pl.col("dx")).alias("cx"))
        .drop("dy", "dx")
    )


def match_airport(points: pl.DataFrame, cells: pl.DataFrame) -> pl.DataFrame:
    """points: key columns + lat, lon, radius_km -> + airport, km. Large airports beat medium ones
    in range (an approach fix 20 km out is over small fields too), then distance, then code."""
    return (
        points.with_columns((pl.col("lat") / GRID_DEG).floor().cast(pl.Int32).alias("cy"),
                            (pl.col("lon") / GRID_DEG).floor().cast(pl.Int32).alias("cx"))
        .join(cells.select("airport", "type_rank", "ap_lat", "ap_lon", "cy", "cx"), on=["cy", "cx"])
        .with_columns(haversine_km(pl.col("lat"), pl.col("lon"), pl.col("ap_lat"), pl.col("ap_lon")).alias("km"))
        .filter(pl.col("km") <= pl.col("radius_km"))
        .sort(["type_rank", "km", "airport"])
        .unique("flight_id", keep="first", maintain_order=True)
        .select("flight_id", "airport", pl.col("km").round(1))
    )


# ---------------------------------------------------------------------------------------------
# flights: gaps-and-islands over snapshots
# ---------------------------------------------------------------------------------------------
def find_islands(states: pl.DataFrame, max_gap_minutes: float = MAX_GAP_MINUTES) -> pl.DataFrame:
    """One row per continuous run of an aircraft in ONE state (on ground / airborne).

    A new island starts when: on_ground changes, OR no snapshot for > max_gap_minutes, OR the
    callsign changes (next flight). Same LAG + running SUM as the AIS port calls (lesson 2).
    Positions that are stale, jumped or spoofed are left out: they would draw fake flight paths.
    """
    fixes = (
        states.filter(~pl.col("is_stale_position") & ~pl.col("is_position_jump") & ~pl.col("is_speed_suspect")
                      & ~pl.col("is_test_transponder"))
        .sort("icao24", "snapshot_time")
        .with_columns(pl.when(~pl.col("is_altitude_suspect")).then(pl.col("baro_altitude")).alias("alt_ok"))
    )
    prev = lambda c: pl.col(c).shift(1).over("icao24")  # noqa: E731
    new_island = (
        prev("on_ground").is_null()
        | (pl.col("on_ground") != prev("on_ground"))
        | ((pl.col("snapshot_time") - prev("snapshot_time")).dt.total_seconds() / 60 > max_gap_minutes)
        | (pl.col("callsign").is_not_null() & prev("callsign").is_not_null() & (pl.col("callsign") != prev("callsign")))
    ).fill_null(True)
    return (
        fixes.with_columns(new_island.cast(pl.Int32).cum_sum().over("icao24").alias("island_id"))
        .group_by("icao24", "island_id")
        .agg(
            pl.col("on_ground").first(),
            pl.col("snapshot_time").min().alias("start"), pl.col("snapshot_time").max().alias("end"),
            pl.len().alias("n_fixes"),
            pl.col("latitude").first().alias("first_lat"), pl.col("longitude").first().alias("first_lon"),
            pl.col("latitude").last().alias("last_lat"), pl.col("longitude").last().alias("last_lon"),
            pl.col("alt_ok").first().alias("first_alt"), pl.col("alt_ok").last().alias("last_alt"),
            pl.col("alt_ok").max().alias("max_alt"),
            # 2nd-highest altitude: a record needs TWO fixes up there, one glitch is not enough
            pl.when(pl.col("alt_ok").drop_nulls().len() >= 2)
            .then(pl.col("alt_ok").drop_nulls().top_k(2).min()).alias("max_alt_confirmed"),
            pl.col("velocity").median().alias("median_velocity"),
            pl.col("is_altitude_jump").sum().alias("n_altitude_jumps"),
            pl.col("callsign").drop_nulls().mode().first().alias("callsign"),
            pl.col("origin_country").first(),
        )
        .sort("icao24", "start")
    )


def build_flights(states: pl.DataFrame, cells: pl.DataFrame) -> pl.DataFrame:
    """Airborne islands = flights. Where did each one start and end?
      1. a ground island right before/after it (<= LINK_MINUTES): its position, airport within 5 km
      2. else the first/last airborne fix if it is low (< LOW_ALT_M): airport within 25 km
      3. else unknown: the aircraft was already cruising when first seen (outside receiver range)
    """
    isl = find_islands(states)
    nb = lambda c, n: pl.col(c).shift(n).over("icao24")  # noqa: E731  n=1 previous island, -1 next
    linked_before = nb("on_ground", 1).fill_null(False) & (
        (pl.col("start") - nb("end", 1)).dt.total_seconds() / 60 <= LINK_MINUTES)
    linked_after = nb("on_ground", -1).fill_null(False) & (
        (nb("start", -1) - pl.col("end")).dt.total_seconds() / 60 <= LINK_MINUTES)
    f = (
        isl.with_columns(
            pl.when(linked_before).then(nb("last_lat", 1)).when(pl.col("first_alt") < LOW_ALT_M).then(pl.col("first_lat")).alias("dep_lat"),
            pl.when(linked_before).then(nb("last_lon", 1)).when(pl.col("first_alt") < LOW_ALT_M).then(pl.col("first_lon")).alias("dep_lon"),
            pl.when(linked_before).then(pl.lit(GROUND_RADIUS_KM)).otherwise(pl.lit(APPROACH_RADIUS_KM)).alias("dep_radius"),
            pl.when(linked_before).then(pl.lit("ground")).when(pl.col("first_alt") < LOW_ALT_M).then(pl.lit("low_altitude"))
            .otherwise(pl.lit("unknown")).alias("dep_method"),
            pl.when(linked_after).then(nb("first_lat", -1)).when(pl.col("last_alt") < LOW_ALT_M).then(pl.col("last_lat")).alias("arr_lat"),
            pl.when(linked_after).then(nb("first_lon", -1)).when(pl.col("last_alt") < LOW_ALT_M).then(pl.col("last_lon")).alias("arr_lon"),
            pl.when(linked_after).then(pl.lit(GROUND_RADIUS_KM)).otherwise(pl.lit(APPROACH_RADIUS_KM)).alias("arr_radius"),
            pl.when(linked_after).then(pl.lit("ground")).when(pl.col("last_alt") < LOW_ALT_M).then(pl.lit("low_altitude"))
            .otherwise(pl.lit("unknown")).alias("arr_method"),
        )
        .filter(~pl.col("on_ground") & (pl.col("n_fixes") >= 2))
        .with_columns(pl.concat_str("icao24", "island_id", separator="-").alias("flight_id"))
    )
    end = lambda side: match_airport(  # noqa: E731
        f.filter(pl.col(f"{side}_lat").is_not_null()).select(
            "flight_id", pl.col(f"{side}_lat").alias("lat"), pl.col(f"{side}_lon").alias("lon"),
            pl.col(f"{side}_radius").alias("radius_km")), cells)
    return (
        f.join(end("dep").rename({"airport": "dep_airport", "km": "dep_km"}), on="flight_id", how="left")
        .join(end("arr").rename({"airport": "arr_airport", "km": "arr_km"}), on="flight_id", how="left")
        .with_columns(((pl.col("end") - pl.col("start")).dt.total_seconds() / 60).alias("observed_minutes"),
                      (pl.col("median_velocity") >= JET_MS).fill_null(False).alias("is_jet_speed"))
        .select("flight_id", "icao24", "callsign", "origin_country", "is_jet_speed", "start", "end",
                "observed_minutes", "n_fixes", "n_altitude_jumps",
                "dep_airport", "dep_method", "dep_km", "arr_airport", "arr_method", "arr_km",
                "max_alt", "max_alt_confirmed", "median_velocity", "first_lat", "first_lon", "last_lat", "last_lon")
    )


def build_airport_traffic(flights: pl.DataFrame, airports: pl.DataFrame) -> pl.DataFrame:
    """Arrivals are the reliable count: a jet passes 1,200 m about 2 minutes after take-off, so a
    5-minute snapshot often misses the climb-out, while the last ~5 minutes of an approach are
    below 1,200 m and usually caught. Departures are undercounted - compare airports by arrivals."""
    dep = flights.filter(pl.col("dep_airport").is_not_null()).select(
        pl.col("dep_airport").alias("airport"), pl.col("start").dt.date().alias("day"), "icao24", "is_jet_speed",
        pl.lit("dep").alias("kind"))
    arr = flights.filter(pl.col("arr_airport").is_not_null()).select(
        pl.col("arr_airport").alias("airport"), pl.col("end").dt.date().alias("day"), "icao24", "is_jet_speed",
        pl.lit("arr").alias("kind"))
    return (
        pl.concat([dep, arr])
        .group_by("airport", "day", "is_jet_speed")
        .agg((pl.col("kind") == "dep").sum().alias("departures"), (pl.col("kind") == "arr").sum().alias("arrivals"),
             pl.col("icao24").n_unique().alias("aircraft"))
        .join(airports.select("airport", "iata", "name", "country", "municipality"), on="airport", how="left")
        .sort("airport", "day", "is_jet_speed")
    )


def build_altitude_top(flights: pl.DataFrame, aircraft: pl.DataFrame, n: int = 50) -> pl.DataFrame:
    """Records only from transponders whose altitudes can be trusted at all (silver aircraft:
    no altitude jumps, < 5% suspect states). Above 15 km a median ground speed under 100 m/s
    means a balloon: nothing with wings flies that slowly up there."""
    return (
        flights.join(aircraft.select("icao24", "is_altitude_unreliable"), on="icao24", how="left")
        .filter(pl.col("max_alt_confirmed").is_not_null() & ~pl.col("is_altitude_unreliable").fill_null(True))
        .with_columns(
            pl.when((pl.col("max_alt_confirmed") > 15000) & (pl.col("median_velocity") < BALLOON_MS))
            .then(pl.lit("balloon")).otherwise(pl.lit("aircraft")).alias("kind"))
        .sort("max_alt_confirmed", descending=True)
        .head(n)
        .with_columns((pl.col("max_alt_confirmed") * 3.28084).round(0).alias("max_alt_confirmed_ft"))
        .select("kind", "icao24", "callsign", "origin_country", "max_alt_confirmed", "max_alt_confirmed_ft",
                "max_alt", "median_velocity", "start", "end", "dep_airport", "arr_airport")
    )


# ---------------------------------------------------------------------------------------------
# emergencies, GNSS anomalies, hourly activity (straight from silver, small)
# ---------------------------------------------------------------------------------------------
def build_emergencies(states: pl.LazyFrame, flights: pl.DataFrame) -> pl.DataFrame:
    """Episodes of an emergency squawk: consecutive snapshots with the same code (<= 20 min apart).
    A single snapshot is often a pilot dialling past 7xxx or a decoding error: is_sustained needs 2+."""
    e = (
        states.filter("is_emergency_squawk")
        .select("icao24", "snapshot_time", "squawk", "emergency_type", "callsign", "origin_country",
                "latitude", "longitude", "baro_altitude")
        .collect()
        .sort("icao24", "snapshot_time")
    )
    prev = lambda c: pl.col(c).shift(1).over("icao24")  # noqa: E731
    new_ep = (prev("squawk").is_null() | (pl.col("squawk") != prev("squawk"))
              | ((pl.col("snapshot_time") - prev("snapshot_time")).dt.total_seconds() / 60 > 20)).fill_null(True)
    ep = (
        e.with_columns(new_ep.cast(pl.Int32).cum_sum().over("icao24").alias("episode"))
        .group_by("icao24", "episode")
        .agg(pl.col("squawk").first(), pl.col("emergency_type").first(),
             pl.col("callsign").drop_nulls().first(), pl.col("origin_country").first(),
             pl.col("snapshot_time").min().alias("start"), pl.col("snapshot_time").max().alias("end"),
             pl.len().alias("n_snapshots"), pl.col("latitude").first().alias("lat"),
             pl.col("longitude").first().alias("lon"), pl.col("baro_altitude").max().alias("max_alt"),
             pl.col("baro_altitude").min().alias("min_alt"))
        .with_columns((pl.col("n_snapshots") >= 2).alias("is_sustained"))
    )
    # Which flight was it? The flight of the same aircraft whose time span contains the episode start.
    fl = flights.select("icao24", "flight_id", pl.col("start").alias("f_start"), pl.col("end").alias("f_end"),
                        "dep_airport", "arr_airport")
    hit = (ep.join(fl, on="icao24").filter((pl.col("start") >= pl.col("f_start") - pl.duration(minutes=30))
                                           & (pl.col("start") <= pl.col("f_end") + pl.duration(minutes=30)))
           .unique(["icao24", "episode"], keep="first").select("icao24", "episode", "flight_id", "dep_airport", "arr_airport"))
    return ep.join(hit, on=["icao24", "episode"], how="left").sort("start")


def build_gnss_anomalies(states: pl.LazyFrame) -> pl.DataFrame:
    """Where do aircraft report physically impossible speeds or position jumps? Clusters of many
    DIFFERENT aircraft in one area on one day = GNSS jamming/spoofing, not one broken transponder.
    Caveat: a spoofed aircraft reports a FAKE position, so a cell can be the spoofer's target point."""
    bad = pl.col("is_speed_suspect") | pl.col("is_position_jump")
    return (
        states.filter(~pl.col("on_ground"))
        .select(pl.col("snapshot_time").dt.date().alias("day"),
                pl.col("latitude").floor().cast(pl.Int16).alias("lat_cell"),
                pl.col("longitude").floor().cast(pl.Int16).alias("lon_cell"), "icao24", bad.alias("bad"))
        .group_by("day", "lat_cell", "lon_cell")
        .agg(pl.col("icao24").n_unique().alias("aircraft"), pl.col("icao24").filter("bad").n_unique().alias("anomalous_aircraft"))
        .filter(pl.col("anomalous_aircraft") >= ANOMALY_MIN_AIRCRAFT)
        .with_columns((pl.col("anomalous_aircraft") / pl.col("aircraft")).round(3).alias("share"))
        .collect(engine="streaming")
        .sort("day", "anomalous_aircraft", descending=[False, True])
    )


def build_air_hourly(states: pl.LazyFrame) -> pl.DataFrame:
    """Per hour: how many snapshots we have, and aircraft airborne PER SNAPSHOT.
    Distinct aircraft per hour depends on how often we looked (every 5 min until 5 Oct, every
    10 min after - the Azure budget cut); the per-snapshot average does not, so that is the series
    to compare and forecast. coverage = snapshots / 12 (12 = one every 5 minutes)."""
    per_snap = (
        states.filter(~pl.col("is_test_transponder"))
        .group_by("snapshot_time")
        .agg((~pl.col("on_ground")).sum().alias("airborne"), pl.len().alias("aircraft"))
    )
    return (
        per_snap.group_by(pl.col("snapshot_time").dt.truncate("1h").alias("hour"))
        .agg(pl.len().alias("snapshots"), pl.col("airborne").mean().round(0).alias("avg_airborne"),
             pl.col("aircraft").mean().round(0).alias("avg_aircraft"))
        .with_columns((pl.col("snapshots") / 12).round(2).alias("coverage"), pl.lit(False).alias("is_forecast"))
        .sort("hour")
        .collect(engine="streaming")
    )
