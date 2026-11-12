"""Gold Parquet -> PostgreSQL (database skysea, schema gold), for SQL practice and Power BI.

Parquet stays the source of truth: Postgres is a serving copy that is rebuilt from gold on
every load, so it can be dropped at any time (DROP DATABASE skysea) and re-made in a minute.

Password: %APPDATA%\\postgresql\\pgpass.conf, same as the other trackers (libpq reads it).
"""
import io
import time
from pathlib import Path

import polars as pl
import psycopg

CONN = {"host": "localhost", "port": 5432, "user": "postgres"}
DBNAME = "skysea"
SCHEMA = "gold"
CHUNK_ROWS = 250_000  # COPY in slices so a 2.6M-row table never sits in memory as one CSV string

# Polars -> Postgres. Unsigned ints get the next bigger signed type (Postgres has no unsigned).
PG_TYPES = {
    pl.String: "text", pl.Boolean: "boolean", pl.Date: "date",
    pl.Int8: "smallint", pl.Int16: "smallint", pl.UInt8: "smallint",
    pl.Int32: "integer", pl.UInt16: "integer",
    pl.Int64: "bigint", pl.UInt32: "bigint",
    pl.Float32: "real", pl.Float64: "double precision",
}

# Keys + indexes per table: what the views and Power BI relationships join or filter on.
KEYS = {
    "airports": {"pk": ["airport"]},
    "ports": {"pk": ["locode"]},
    "opensky_flights": {"pk": ["flight_id"], "idx": [["start"], ["dep_airport"], ["arr_airport"]]},
    "opensky_airport_traffic": {"pk": ["airport", "day", "is_jet_speed"], "idx": [["day"]]},
    "opensky_emergencies": {"pk": ["icao24", "episode"]},
    "opensky_gnss_anomalies": {"pk": ["day", "lat_cell", "lon_cell"]},
    "opensky_air_hourly": {"pk": ["hour"]},
    "ais_port_calls": {"pk": ["call_id"], "idx": [["arrival"], ["locode"], ["mmsi"]]},
    "ais_port_traffic": {"idx": [["locode"]]},
    "ais_eta_accuracy": {"idx": [["call_id"]]},
    "ais_fleet_hourly": {"pk": ["hour", "ship_category"]},
}


def pg_type(dtype) -> str:
    if isinstance(dtype, pl.Datetime):
        return "timestamptz" if dtype.time_zone else "timestamp"
    return PG_TYPES[dtype.base_type()]


def q(name: str) -> str:
    """Quote an identifier: some gold columns are SQL keywords (start, end, hour, day)."""
    return '"' + name.replace('"', '""') + '"'


def ensure_database() -> None:
    with psycopg.connect(**CONN, dbname="postgres", autocommit=True) as conn:
        if not conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", [DBNAME]).fetchone():
            conn.execute(f"CREATE DATABASE {DBNAME}")
        # The server's default zone is Europe/Istanbul: without this, ts::date in the views (and in
        # Power BI's session) cuts days at Istanbul midnight. Every client of skysea sees UTC.
        conn.execute(f"ALTER DATABASE {DBNAME} SET timezone TO 'UTC'")


def connect() -> psycopg.Connection:
    conn = psycopg.connect(**CONN, dbname=DBNAME)
    conn.execute("SET TIME ZONE 'UTC'")  # naive UTC text in COPY -> correct timestamptz
    return conn


def for_copy(df: pl.DataFrame) -> pl.DataFrame:
    """Timestamps as naive UTC (session is UTC), so the CSV text needs no offset parsing."""
    return df.with_columns(
        pl.col(c).dt.convert_time_zone("UTC").dt.replace_time_zone(None)
        for c, t in df.schema.items() if isinstance(t, pl.Datetime) and t.time_zone)


def load_table(conn: psycopg.Connection, path: Path) -> int:
    """Rebuild one table inside the caller's transaction: readers keep the old rows until commit."""
    name = path.stem
    df = pl.read_parquet(path)
    cols = ", ".join(f"{q(c)} {pg_type(t)}" for c, t in df.schema.items())
    keys = KEYS.get(name, {})
    table = f"{SCHEMA}.{name}"
    conn.execute(f"DROP TABLE IF EXISTS {table} CASCADE")  # CASCADE drops views; views.sql re-creates
    conn.execute(f"CREATE TABLE {table} ({cols})")
    out = for_copy(df)
    column_list = ", ".join(q(c) for c in df.columns)
    with conn.cursor().copy(f"COPY {table} ({column_list}) FROM STDIN (FORMAT csv)") as copy:
        for chunk in out.iter_slices(CHUNK_ROWS):
            buf = io.BytesIO()
            chunk.write_csv(buf, include_header=False, null_value="")
            copy.write(buf.getvalue())
    # Keys after the data: building an index once is much faster than updating it per row.
    if "pk" in keys:
        conn.execute(f"ALTER TABLE {table} ADD PRIMARY KEY ({', '.join(q(c) for c in keys['pk'])})")
    for i, idx_cols in enumerate(keys.get("idx", [])):
        conn.execute(f"CREATE INDEX {name}_ix{i} ON {table} ({', '.join(q(c) for c in idx_cols)})")
    return df.height


def load_all(gold_dir: Path, views_sql: Path) -> list[tuple[str, int, int, float]]:
    """Load every gold table, re-create the views, analyze, and check counts against Parquet."""
    ensure_database()
    report = []
    with connect() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
        conn.execute(f"""CREATE TABLE IF NOT EXISTS {SCHEMA}.load_runs (
            loaded_at timestamptz DEFAULT now(), table_name text, parquet_rows bigint,
            pg_rows bigint, seconds real)""")
        for path in sorted(gold_dir.glob("*.parquet")):
            t0 = time.time()
            n = load_table(conn, path)
            pg_rows = conn.execute(f"SELECT count(*) FROM {SCHEMA}.{path.stem}").fetchone()[0]
            secs = time.time() - t0
            conn.execute(f"INSERT INTO {SCHEMA}.load_runs (table_name, parquet_rows, pg_rows, seconds)"
                         " VALUES (%s, %s, %s, %s)", [path.stem, n, pg_rows, secs])
            report.append((path.stem, n, pg_rows, secs))
        conn.execute(views_sql.read_text(encoding="utf-8"))
        # Column descriptions (data dictionary) live next to views.sql; tables are new, so re-apply.
        conn.execute((views_sql.parent / "comments.sql").read_text(encoding="utf-8"))
    with psycopg.connect(**CONN, dbname=DBNAME, autocommit=True) as conn:
        conn.execute("ANALYZE")  # fresh planner statistics after a full reload
    return report
