r"""Profile bronze OpenSky before writing silver rules: what is actually broken in the raw data?
Plain SQL on Parquet, run by Polars.  .venv\Scripts\python scripts\profile_bronze_opensky.py"""
import polars as pl

from skysea.console import setup
from skysea.config import BRONZE_OPENSKY_STATES
from skysea.io import scan

setup()
ctx = pl.SQLContext(s=scan(BRONZE_OPENSKY_STATES))


def q(title, sql):
    print(f"\n== {title}")
    print(ctx.execute(sql).collect(engine="streaming"))


q("totals", """
SELECT count(*) AS rows, count(DISTINCT icao24) AS aircraft, count(DISTINCT snapshot_time) AS snapshots,
       min(snapshot_time) AS first, max(snapshot_time) AS last FROM s""")
q("per day: snapshots (288 = every 5 min, 144 = every 10 min) and aircraft per snapshot", """
SELECT file_date, count(DISTINCT snapshot_time) AS snapshots, count(*) AS rows,
       count(*) / count(DISTINCT snapshot_time) AS rows_per_snapshot FROM s GROUP BY file_date ORDER BY file_date""")
q("duplicates (icao24, snapshot_time)", """
SELECT count(*) AS dup_keys FROM (SELECT icao24, snapshot_time FROM s GROUP BY icao24, snapshot_time HAVING count(*) > 1) d""")
q("missing / odd values", """
SELECT sum(CASE WHEN NOT regexp_like(icao24, '^[0-9a-f]{6}$') THEN 1 ELSE 0 END) AS bad_icao24,
       sum(CASE WHEN latitude IS NULL OR longitude IS NULL THEN 1 ELSE 0 END) AS no_position,
       sum(CASE WHEN baro_altitude IS NULL THEN 1 ELSE 0 END) AS no_baro,
       sum(CASE WHEN on_ground = 'True' THEN 1 ELSE 0 END) AS on_ground,
       sum(CASE WHEN callsign IS NULL OR trim(callsign) = '' THEN 1 ELSE 0 END) AS no_callsign,
       sum(CASE WHEN sensors IS NOT NULL THEN 1 ELSE 0 END) AS sensors_set,
       sum(CASE WHEN category IS NOT NULL THEN 1 ELSE 0 END) AS category_set,
       sum(CASE WHEN squawk IS NULL THEN 1 ELSE 0 END) AS no_squawk
FROM s""")
q("altitude / speed extremes (metres, m/s)", """
SELECT max(TRY_CAST(baro_altitude AS DOUBLE)) AS max_baro, min(TRY_CAST(baro_altitude AS DOUBLE)) AS min_baro,
       max(TRY_CAST(geo_altitude AS DOUBLE)) AS max_geo,
       sum(CASE WHEN TRY_CAST(baro_altitude AS DOUBLE) > 13800 THEN 1 ELSE 0 END) AS baro_over_13800,
       sum(CASE WHEN TRY_CAST(baro_altitude AS DOUBLE) > 15600 THEN 1 ELSE 0 END) AS baro_over_15600,
       sum(CASE WHEN TRY_CAST(baro_altitude AS DOUBLE) < -500 THEN 1 ELSE 0 END) AS baro_below_m500,
       max(TRY_CAST(velocity AS DOUBLE)) AS max_velocity,
       sum(CASE WHEN TRY_CAST(velocity AS DOUBLE) > 340 THEN 1 ELSE 0 END) AS velocity_over_340,
       sum(CASE WHEN abs(TRY_CAST(vertical_rate AS DOUBLE)) > 50 THEN 1 ELSE 0 END) AS vrate_over_50
FROM s""")
q("emergency squawks: rows / aircraft", """
SELECT squawk, count(*) AS rows, count(DISTINCT icao24) AS aircraft FROM s
WHERE squawk IN ('7500', '7600', '7700') GROUP BY squawk ORDER BY squawk""")
q("squawk shape", """
SELECT CASE WHEN squawk IS NULL THEN 'null' WHEN regexp_like(squawk, '^[0-7]{4}$') THEN '4 octal digits'
            ELSE 'other' END AS kind, count(*) AS n FROM s GROUP BY kind""")
q("position_source (0 ADS-B, 1 ASTERIX, 2 MLAT, 3 FLARM)", "SELECT position_source, count(*) AS n FROM s GROUP BY position_source ORDER BY n DESC")
q("top origin countries (registration country, not where they fly)", """
SELECT origin_country, count(DISTINCT icao24) AS aircraft FROM s GROUP BY origin_country ORDER BY aircraft DESC LIMIT 10""")

# Position age: how old is the position compared with the snapshot? (not easy in SQL -> Polars)
age = (scan(BRONZE_OPENSKY_STATES)
       .select((pl.col("snapshot_time").str.to_datetime("%Y-%m-%dT%H:%M:%S%z", strict=False).dt.epoch("s")
                - pl.col("time_position").cast(pl.Int64, strict=False)).alias("age_s"))
       .collect(engine="streaming")["age_s"])
print("\n== position age = snapshot_time - time_position (seconds)")
print(age.describe(percentiles=[0.5, 0.9, 0.99, 0.999]))
print("rows with age > 60 s:", (age > 60).sum(), "  > 300 s:", (age > 300).sum(), "  null:", age.null_count())
