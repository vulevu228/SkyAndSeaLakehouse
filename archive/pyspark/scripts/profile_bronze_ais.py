"""Profile bronze AIS before writing silver rules: what is actually broken in the raw data?"""
from skysea.config import BRONZE_AIS_POSITIONS, BRONZE_AIS_STATIC
from skysea.spark import get_spark

spark = get_spark("skysea-profile")
spark.read.format("delta").load(str(BRONZE_AIS_POSITIONS)).createOrReplaceTempView("pos")
spark.read.format("delta").load(str(BRONZE_AIS_STATIC)).createOrReplaceTempView("st")

def q(title, sql):
    print(f"\n== {title}")
    spark.sql(sql).show(30, truncate=False)

q("positions: totals", """
SELECT count(*) rows, count(DISTINCT mmsi) ships, count(DISTINCT window_start) windows,
       min(window_start) first, max(window_start) last FROM pos""")
q("duplicates (mmsi, window_start)", """
SELECT count(*) - count(DISTINCT mmsi, window_start) dup_rows FROM pos""")
q("bad casts / missing", """
SELECT sum(int(try_cast(latitude AS double) IS NULL)) lat_null, sum(int(try_cast(longitude AS double) IS NULL)) lon_null,
       sum(int(abs(try_cast(latitude AS double)) > 90)) lat_out, sum(int(abs(try_cast(longitude AS double)) > 180)) lon_out,
       sum(int(try_cast(latitude AS double) = 91)) lat_91, sum(int(try_cast(longitude AS double) = 181)) lon_181,
       sum(int(try_cast(latitude AS double) = 0 AND try_cast(longitude AS double) = 0)) null_island,
       sum(int(try_cast(sog_knots AS double) >= 102.2)) sog_na, sum(int(try_cast(sog_knots AS double) BETWEEN 50 AND 102.1)) sog_50_102,
       sum(int(try_cast(cog_deg AS double) >= 360)) cog_na, sum(int(try_cast(window_start AS timestamp) IS NULL)) ts_bad
FROM pos""")
q("mmsi shape", """
SELECT CASE WHEN mmsi NOT RLIKE '^[0-9]{9}$' THEN 'not 9 digits'
            WHEN substr(mmsi,1,1) BETWEEN '2' AND '7' THEN 'ship (MID 2-7)'
            WHEN substr(mmsi,1,3) = '111' THEN 'SAR aircraft'
            WHEN substr(mmsi,1,2) = '99' THEN 'aid to navigation'
            WHEN substr(mmsi,1,2) = '00' THEN 'base station'
            WHEN substr(mmsi,1,3) = '970' OR substr(mmsi,1,3) IN ('972','974') THEN 'SART/MOB/EPIRB'
            ELSE 'other' END kind, count(*) rows, count(DISTINCT mmsi) ids
FROM pos GROUP BY 1 ORDER BY 2 DESC""")
q("nav_status", "SELECT nav_status, ais_class, count(*) n FROM pos GROUP BY 1,2 ORDER BY 3 DESC")
q("rows per day (file_date)", "SELECT file_date, count(*) n, count(DISTINCT window_start) windows FROM pos GROUP BY 1 ORDER BY 1")
q("received_at lag vs window_start (seconds)", """
SELECT percentile_approx(unix_timestamp(try_cast(window_start AS timestamp)) - unix_timestamp(try_cast(received_at AS timestamp)), array(0.01,0.5,0.99)) p, 
       sum(int(try_cast(received_at AS timestamp) IS NULL)) recv_null FROM pos""")
q("static: totals + ship_type sample", """
SELECT count(*) rows, count(DISTINCT mmsi) ships, sum(int(imo IS NULL OR imo IN ('','0'))) no_imo,
       sum(int(destination IS NULL OR destination = '')) no_dest, sum(int(eta_month IN ('0','') OR eta_month IS NULL)) eta_na,
       count(DISTINCT ship_type) types FROM st""")
q("ships with several names", """
SELECT n_names, count(*) ships FROM (SELECT mmsi, count(DISTINCT ship_name) n_names FROM st WHERE ship_name <> '' GROUP BY mmsi) GROUP BY 1 ORDER BY 1 LIMIT 8""")
q("top destinations (raw text)", "SELECT destination, count(DISTINCT mmsi) ships FROM st GROUP BY 1 ORDER BY 2 DESC LIMIT 15")
