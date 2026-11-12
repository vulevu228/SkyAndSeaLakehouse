r"""Profile bronze AIS before writing silver rules: what is actually broken in the raw data?
Plain SQL on Parquet, run by Polars (pl.SQLContext) - no database server, no Spark.
Run:  .venv\Scripts\python scripts\profile_bronze_ais.py"""
import polars as pl

from skysea.console import setup
from skysea.config import BRONZE_AIS_POSITIONS, BRONZE_AIS_STATIC
from skysea.io import scan

setup()
ctx = pl.SQLContext(pos=scan(BRONZE_AIS_POSITIONS), st=scan(BRONZE_AIS_STATIC))


def q(title, sql):
    print(f"\n== {title}")
    print(ctx.execute(sql).collect(engine="streaming"))


q("positions: totals", """
SELECT count(*) AS rows, count(DISTINCT mmsi) AS ships, count(DISTINCT window_start) AS windows,
       min(window_start) AS first, max(window_start) AS last FROM pos""")
q("bad casts / missing", """
SELECT sum(CASE WHEN TRY_CAST(latitude AS DOUBLE) IS NULL THEN 1 ELSE 0 END) AS lat_null,
       sum(CASE WHEN TRY_CAST(latitude AS DOUBLE) = 0 AND TRY_CAST(longitude AS DOUBLE) = 0 THEN 1 ELSE 0 END) AS null_island,
       sum(CASE WHEN TRY_CAST(sog_knots AS DOUBLE) >= 102.2 THEN 1 ELSE 0 END) AS sog_na,
       sum(CASE WHEN TRY_CAST(cog_deg AS DOUBLE) >= 360 THEN 1 ELSE 0 END) AS cog_na
FROM pos""")
q("mmsi shape", """
SELECT CASE WHEN NOT regexp_like(mmsi, '^[0-9]{9}$') THEN 'not 9 digits'
            WHEN substr(mmsi, 1, 3) = '111' THEN 'SAR aircraft'
            WHEN substr(mmsi, 1, 3) IN ('970', '972', '974') THEN 'SART/MOB/EPIRB'
            WHEN substr(mmsi, 1, 2) = '99' THEN 'aid to navigation'
            WHEN substr(mmsi, 1, 2) = '00' THEN 'base station'
            WHEN substr(mmsi, 1, 1) IN ('2', '3', '4', '5', '6', '7') THEN 'ship (MID 2-7)'
            ELSE 'other' END AS kind, count(*) AS rows, count(DISTINCT mmsi) AS ids
FROM pos GROUP BY kind ORDER BY rows DESC""")
q("nav_status", "SELECT nav_status, ais_class, count(*) AS n FROM pos GROUP BY nav_status, ais_class ORDER BY n DESC LIMIT 15")
q("rows per day (file_date)", """
SELECT file_date, count(*) AS n, count(DISTINCT window_start) AS windows FROM pos GROUP BY file_date ORDER BY file_date""")
q("static: totals", """
SELECT count(*) AS rows, count(DISTINCT mmsi) AS ships,
       sum(CASE WHEN imo IS NULL OR imo IN ('', '0') THEN 1 ELSE 0 END) AS no_imo,
       sum(CASE WHEN destination IS NULL OR destination = '' THEN 1 ELSE 0 END) AS no_dest FROM st""")
q("top destinations (raw text)", """
SELECT destination, count(DISTINCT mmsi) AS ships FROM st GROUP BY destination ORDER BY ships DESC LIMIT 15""")
