"""Silver AIS -> gold: tables that answer questions.

gold\\ports.parquet             UN/LOCODE seaports with decimal coordinates (reference)
gold\\ais_port_calls.parquet    one row per call of a ship at a port (gaps-and-islands over positions)
gold\\ais_port_traffic.parquet  calls and distinct ships per port and ship category
gold\\ais_eta_accuracy.parquet  announced ETA vs the actual arrival found in the port calls
gold\\ais_fleet_hourly.parquet  ships seen / moving per hour and category (the series we forecast over gaps)
"""
import polars as pl

from skysea.silver.ais import haversine_km

STOP_KNOTS = 0.5           # slower than this = not moving
MOORED_DRIFT_KNOTS = 1.0   # a ship reporting moored/at anchor drifts up to ~1 kn on GPS noise: still stopped
MERGE_GAP_HOURS = 2        # two stops at the same port less than 2 h apart = one port call (shifting berth, noise)
MAX_GAP_MINUTES = 30       # a stop continues across up to 30 min of missing fixes (coverage holes)
MIN_STOP_MINUTES = 60      # shorter calls (traffic, locks, pilot pick-up) are not port calls
PORT_RADIUS_KM = 20        # stop centroid must be this close to a port's LOCODE point
GRID_DEG = 0.5             # grid cell size for the spatial join (0.5 deg lat ~ 55 km > PORT_RADIUS_KM)
N_BUCKETS = 8              # port calls are built for 1/8 of the ships at a time (see bucket())

STOP_COLS = ["mmsi", "mmsi_kind", "is_position_jump", "window_start", "sog_knots",
             "nav_status", "nav_status_label", "latitude", "longitude"]


def bucket(i: int) -> pl.Expr:
    """Ships whose MMSI hashes to bucket i. Everything in a port call depends on ONE ship's own
    rows, so the ships can be split into independent groups and done one group at a time:
    ~2.3M rows in memory instead of 19M. This is what Spark's shuffle did for us (all rows of an
    MMSI to the same task) - here we do it by hand, and it is the same idea."""
    return pl.col("mmsi").hash(seed=42) % N_BUCKETS == i


# ---------------------------------------------------------------------------------------------
# ports reference
# ---------------------------------------------------------------------------------------------
def _dm_to_deg(deg: pl.Expr, minutes: pl.Expr, hemi: pl.Expr, negative: str) -> pl.Expr:
    v = deg.cast(pl.Float64) + minutes.cast(pl.Float64) / 60
    return pl.when(hemi == negative).then(-v).otherwise(v)


def build_ports(csv_path: str) -> pl.DataFrame:
    """UN/LOCODE rows whose Function starts with '1' are seaports. Coordinates look like
    '5331N 00956E' (degrees + minutes) -> 53.517, 9.933."""
    rx = r"^(\d{2})(\d{2})([NS]) (\d{3})(\d{2})([EW])$"
    c = pl.col("Coordinates").str.strip_chars()
    part = lambda i: c.str.extract(rx, i)  # noqa: E731
    return (
        pl.read_csv(csv_path, infer_schema=False)
        .filter(pl.col("Function").str.starts_with("1") & c.str.contains(rx) & pl.col("Location").is_not_null())
        .select(
            (pl.col("Country") + pl.col("Location")).alias("locode"),
            pl.col("Country").alias("country"),
            pl.col("NameWoDiacritics").alias("port_name"),
            _dm_to_deg(part(1), part(2), part(3), "S").alias("port_lat"),
            _dm_to_deg(part(4), part(5), part(6), "W").alias("port_lon"),
        )
        .unique("locode", keep="first", maintain_order=True)
    )


# ---------------------------------------------------------------------------------------------
# port calls: gaps-and-islands
# ---------------------------------------------------------------------------------------------
def find_stops(positions: pl.DataFrame, data_start=None, data_end=None) -> pl.DataFrame:
    """Gaps-and-islands: turn position rows into one row per continuous stop.

    1. is_stopped per row.
    2. new_island = 1 when this row starts a new run: state changed, or the gap since the
       previous fix is > MAX_GAP_MINUTES (shift(1).over("mmsi") = LAG over the ship's rows).
    3. island_id = running sum of new_island (cum_sum().over("mmsi")) -> one id per run.
    4. group_by(mmsi, island_id) -> start, end, centroid of the stop.
    data_start/data_end = first/last window of the WHOLE dataset (for is_truncated); default:
    taken from these positions.
    """
    if data_start is None:
        data_start, data_end = positions["window_start"].min(), positions["window_start"].max()
    sog = pl.col("sog_knots")
    p = (
        positions.filter((pl.col("mmsi_kind") == "ship") & ~pl.col("is_position_jump"))
        .sort("mmsi", "window_start")
        .with_columns(
            ((sog < STOP_KNOTS) | ((sog < MOORED_DRIFT_KNOTS) & pl.col("nav_status").is_in([1, 5])))
            .fill_null(False).alias("is_stopped"))
        .with_columns(
            pl.col("is_stopped").shift(1).over("mmsi").alias("prev_stopped"),
            # total_seconds()/60, not dt.total_minutes(): that one truncates (30 min 59 s -> 30),
            # and a 30.98-minute hole would then NOT break the stop.
            ((pl.col("window_start") - pl.col("window_start").shift(1).over("mmsi"))
             .dt.total_seconds() / 60).alias("gap_min"),
        )
        .with_columns(
            (pl.col("prev_stopped").is_null() | (pl.col("is_stopped") != pl.col("prev_stopped"))
             | (pl.col("gap_min") > MAX_GAP_MINUTES)).fill_null(True).cast(pl.Int32).alias("new_island"))
        .with_columns(pl.col("new_island").cum_sum().over("mmsi").alias("island_id"))
    )
    margin = pl.duration(minutes=30)
    return (
        p.filter("is_stopped")
        .group_by("mmsi", "island_id")
        .agg(
            pl.col("window_start").min().alias("arrival"),
            pl.col("window_start").max().alias("departure"),
            pl.col("latitude").mean().alias("lat"),
            pl.col("longitude").mean().alias("lon"),
            pl.len().alias("n_fixes"),
            pl.col("nav_status_label").drop_nulls().mode().first().alias("nav_status_label"),
        )
        # A stop that touches the first/last window may have started earlier or ended later
        # than we can see: keep it, but say so (its duration is a lower bound).
        .with_columns(((pl.col("arrival") <= pl.lit(data_start) + margin)
                       | (pl.col("departure") >= pl.lit(data_end) - margin)).alias("is_truncated"))
    )


def grid_cell(lat: pl.Expr, lon: pl.Expr) -> list[pl.Expr]:
    return [(lat / GRID_DEG).floor().cast(pl.Int32).alias("cy"), (lon / GRID_DEG).floor().cast(pl.Int32).alias("cx")]


def port_weights(reports: pl.DataFrame, ports: pl.DataFrame) -> pl.DataFrame:
    """How many ships named each LOCODE as their destination. Crews write 'NLRTM' / 'ROTTERDAM',
    not 'Botlek' or 'Pernis', so this ranks the main port of an area above its sub-areas."""
    return (resolve_destinations(reports, ports, require_eta=False)
            .group_by(pl.col("dest_locode").alias("locode")).agg(pl.col("mmsi").n_unique().alias("dest_ships")))


def port_cells(ports: pl.DataFrame) -> pl.DataFrame:
    """Each port copied into its own grid cell and the 8 around it (cross join with 9 offsets)."""
    offsets = pl.DataFrame({"dy": [a for a in (-1, 0, 1) for _ in range(3)], "dx": [-1, 0, 1] * 3})
    return (
        ports.with_columns(grid_cell(pl.col("port_lat"), pl.col("port_lon")))
        .join(offsets, how="cross")
        .with_columns((pl.col("cy") + pl.col("dy")).alias("cy"), (pl.col("cx") + pl.col("dx")).alias("cx"))
        .drop("dy", "dx")
    )


def nearest_port(stops: pl.DataFrame, cells: pl.DataFrame) -> pl.DataFrame:
    """Spatial join without a cross join.

    stops x ports would be ~10^5 x 10^4 = a billion distance calculations. Instead each port sits
    in its grid cell and the 8 around it (cells are bigger than PORT_RADIUS_KM, so every port
    within range of a stop is in the stop's cell), and the join on (cy, cx) is a normal equi-join.

    Among ports in range, the one most ships name as destination wins (port AREA: a stop in
    Botlek counts for Rotterdam); distance only breaks ties. cells = port_cells(ports with dest_ships).
    """
    matched = (
        stops.select("mmsi", "island_id", "lat", "lon", *grid_cell(pl.col("lat"), pl.col("lon")))
        .join(cells, on=["cy", "cx"])
        .with_columns(haversine_km(pl.col("lat"), pl.col("lon"), pl.col("port_lat"), pl.col("port_lon")).alias("port_km"))
        .filter(pl.col("port_km") <= PORT_RADIUS_KM)
        # "pick the best match per stop": sort by preference, keep the first row per key
        # (SQL: ROW_NUMBER() OVER (PARTITION BY stop ORDER BY dest_ships DESC, port_km, locode) = 1)
        # locode last = deterministic tie-break: some LOCODEs share one point (PLKRM / PLKMR), and an
        # arbitrary pick per stop would split one call into two (the Spark version left this to chance).
        .sort(["dest_ships", "port_km", "locode"], descending=[True, False, False])
        .unique(["mmsi", "island_id"], keep="first", maintain_order=True)
        .select("mmsi", "island_id", "locode", "country", "port_name", "port_km")
    )
    # Stops with no port in range (anchorages far out, offshore platforms, open sea) are kept with NULL port.
    return stops.join(matched, on=["mmsi", "island_id"], how="left")


def merge_stops(stops: pl.DataFrame) -> pl.DataFrame:
    """Second gaps-and-islands, one level up: consecutive stops of a ship at the SAME port less
    than MERGE_GAP_HOURS apart become one call (berth shift, speed noise above the threshold)."""
    s = stops.sort("mmsi", "arrival")
    prev_port = pl.col("locode").shift(1).over("mmsi")
    prev_dep = pl.col("departure").shift(1).over("mmsi")
    new_call = (
        pl.col("locode").is_null() | (pl.col("locode") != prev_port)
        | ((pl.col("arrival") - prev_dep).dt.total_seconds() / 3600 > MERGE_GAP_HOURS)
    ).fill_null(True)  # first stop of a ship (prev is NULL)
    return (
        s.with_columns(new_call.cast(pl.Int32).cum_sum().over("mmsi").alias("call_no"))
        .group_by("mmsi", "call_no")
        .agg(
            pl.col("arrival").min(), pl.col("departure").max(),
            pl.col("locode").drop_nulls().first(), pl.col("port_name").drop_nulls().first(),
            pl.col("country").drop_nulls().first(), pl.col("port_km").min(),
            pl.col("lat").mean(), pl.col("lon").mean(),
            pl.col("n_fixes").sum(), pl.len().alias("n_stops_merged"),
            pl.col("nav_status_label").drop_nulls().mode().first(), pl.col("is_truncated").any(),
        )
        .with_columns(((pl.col("departure") - pl.col("arrival")).dt.total_seconds() / 3600).round(2).alias("duration_h"))
        .filter(pl.col("duration_h") * 60 >= MIN_STOP_MINUTES)
    )


def build_port_calls(positions: pl.DataFrame, cells: pl.DataFrame, ships: pl.DataFrame,
                     data_start=None, data_end=None) -> pl.DataFrame:
    calls = merge_stops(nearest_port(find_stops(positions, data_start, data_end), cells))
    return (
        calls.join(ships.select("mmsi", "ship_name", "ship_category", "length_m",
                                pl.col("imo_valid").alias("is_seagoing")), on="mmsi", how="left")
        .with_columns(pl.col("is_seagoing").fill_null(False),
                      pl.concat_str("mmsi", "call_no", separator="-").alias("call_id"))
    )


def build_port_traffic(calls: pl.DataFrame) -> pl.DataFrame:
    return (
        calls.filter(pl.col("locode").is_not_null())
        # is_seagoing = ship has a valid IMO number. Inland barges (huge on the Rhine/Dutch rivers)
        # mostly do not, and would otherwise make every Dutch village a "top port".
        .group_by("locode", "port_name", "country", pl.col("ship_category").fill_null("unknown"), "is_seagoing")
        .agg(
            pl.len().alias("calls"),
            pl.col("mmsi").n_unique().alias("ships"),
            pl.col("duration_h").median().round(1).alias("median_stay_h"),
        )
    )


# ---------------------------------------------------------------------------------------------
# ETA accuracy
# ---------------------------------------------------------------------------------------------
def resolve_destinations(reports: pl.DataFrame, ports: pl.DataFrame, require_eta: bool = True) -> pl.DataFrame:
    """Hand-typed destination -> candidate LOCODEs.
      'NLRTM' / 'NL RTM'          -> code match
      'DEBRV>NLRTM' / 'X > Y'     -> the part after the last '>' (route notation)
      'ROTTERDAM'                 -> name match; a name used in several countries gives several
                                     candidates, and the ship's actual port call decides.
    """
    dest = pl.col("destination").str.split(">").list.last().str.strip_chars()
    code = dest.str.replace(r"^([A-Z]{2})\s+([A-Z0-9]{3})$", "${1}${2}")
    r = reports.filter(~pl.col("destination_is_placeholder") & pl.col("destination").is_not_null())
    if require_eta:
        r = r.filter(pl.col("eta").is_not_null())
    r = r.with_columns(code.alias("dest_clean"))
    by_code = r.join(ports.select(pl.col("locode").alias("dest_locode")), left_on="dest_clean", right_on="dest_locode")
    by_code = by_code.with_columns(pl.col("dest_clean").alias("dest_locode"))
    names = ports.select(pl.col("port_name").str.to_uppercase().alias("pname"), pl.col("locode").alias("dest_locode"))
    by_name = r.join(names, left_on="dest_clean", right_on="pname")
    return pl.concat([by_code, by_name], how="diagonal_relaxed").unique(
        ["mmsi", "destination", "eta", "dest_locode"], keep="first", maintain_order=True)


def build_eta_accuracy(reports: pl.DataFrame, calls: pl.DataFrame, ports: pl.DataFrame) -> pl.DataFrame:
    """For each announced (destination, ETA): the first port call at that destination that STARTS
    after the ship first announced it. error_h > 0 = arrived later than announced.

    lead_h = hours between FIRST announcing this ETA and arriving (how far ahead the forecast was).
    is_stale_eta = the ETA was already > 1 h in the past when first announced: the crew never
    updated the field. Kept as its own finding, excluded from the accuracy numbers."""
    hours = lambda a, b: ((pl.col(a) - pl.col(b)).dt.total_seconds() / 3600).round(2)  # noqa: E731
    c = calls.filter(pl.col("locode").is_not_null() & ~pl.col("is_truncated")).select(
        "mmsi", pl.col("locode").alias("dest_locode"), "arrival", "call_id", "is_seagoing", "ship_category")
    return (
        resolve_destinations(reports, ports)
        .join(c, on=["mmsi", "dest_locode"])
        .filter(pl.col("arrival") >= pl.col("first_reported"))
        .sort("arrival")
        .unique(["mmsi", "destination", "eta"], keep="first", maintain_order=True)
        .with_columns(
            hours("arrival", "eta").alias("error_h"),
            hours("arrival", "first_reported").alias("lead_h"),
            (pl.col("eta") < pl.col("first_reported") - pl.duration(hours=1)).alias("is_stale_eta"),
        )
        .select("mmsi", "destination", "dest_locode", "eta", "first_reported", "last_reported",
                "arrival", "call_id", "error_h", "lead_h", "is_stale_eta", "is_seagoing", "ship_category", "n_reports")
    )


# ---------------------------------------------------------------------------------------------
# fleet activity
# ---------------------------------------------------------------------------------------------
def build_fleet_hourly(positions: pl.LazyFrame, ships: pl.DataFrame) -> pl.LazyFrame:
    """Distinct ships per hour and category. Counting DISTINCT mmsi (not rows) because one hour
    holds up to 12 windows per ship."""
    moving = pl.col("mmsi").filter(pl.col("sog_knots") >= STOP_KNOTS)
    return (
        positions.filter(pl.col("mmsi_kind") == "ship")
        .select("mmsi", "window_start", "sog_knots")
        .join(ships.lazy().select("mmsi", "ship_category"), on="mmsi", how="left")
        .group_by(pl.col("window_start").dt.truncate("1h").alias("hour"),
                  pl.col("ship_category").fill_null("unknown"))
        .agg(
            pl.col("mmsi").n_unique().alias("ships"),
            moving.n_unique().alias("moving_ships"),
            pl.col("window_start").n_unique().alias("windows"),
        )
        .with_columns(pl.lit(False).alias("is_forecast"))  # gap rows added later by the forecasting step
        .sort("hour", "ship_category")
    )
