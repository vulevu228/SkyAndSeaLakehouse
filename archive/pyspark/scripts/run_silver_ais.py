"""Rebuild the four silver AIS tables from bronze and print a data-quality report.
Run in WSL:  bash scripts/wsl_run.sh scripts/run_silver_ais.py"""
import time

from pyspark.sql import functions as F

from skysea.config import (BRONZE_AIS_POSITIONS, BRONZE_AIS_STATIC, SILVER_AIS_POSITIONS,
                           SILVER_AIS_REJECTED, SILVER_AIS_SHIPS, SILVER_AIS_VOYAGES)
from skysea.silver.ais import build_positions, build_ships, build_voyage_reports, type_static
from skysea.spark import get_spark

spark = get_spark("skysea-silver-ais")


def write(df, path, partition=None):
    t0 = time.time()
    w = df.write.format("delta").mode("overwrite").option("overwriteSchema", True)
    if partition:
        w = w.partitionBy(partition)
    w.save(str(path))
    n = spark.read.format("delta").load(str(path)).count()
    print(f"  {path.name:<26} {n:>12,} rows  ({time.time() - t0:.0f}s)")
    return n


bronze_pos = spark.read.format("delta").load(str(BRONZE_AIS_POSITIONS))
bronze_static = spark.read.format("delta").load(str(BRONZE_AIS_STATIC))

print("writing silver:")
positions, rejected = build_positions(spark, bronze_pos)
n_bronze = bronze_pos.count()
n_good = write(positions, SILVER_AIS_POSITIONS, "file_date")
n_bad = write(rejected, SILVER_AIS_REJECTED)
static = type_static(bronze_static)
write(build_ships(spark, static), SILVER_AIS_SHIPS)
write(build_voyage_reports(static), SILVER_AIS_VOYAGES)

# Every bronze row must end up in exactly one of the two tables - the core silver guarantee.
print(f"\nbronze {n_bronze:,} = silver {n_good:,} + rejected {n_bad:,} ->",
      "OK" if n_bronze == n_good + n_bad else f"MISMATCH ({n_bronze - n_good - n_bad:+,})")

pos = spark.read.format("delta").load(str(SILVER_AIS_POSITIONS))
print("\nrejected by reason:")
spark.read.format("delta").load(str(SILVER_AIS_REJECTED)).groupBy("reject_reason").count().orderBy(F.desc("count")).show(truncate=False)
print("positions by transmitter kind + flags:")
pos.groupBy("mmsi_kind").agg(
    F.count("*").alias("rows"), F.countDistinct("mmsi").alias("ids"),
    F.sum(F.col("is_position_jump").cast("int")).alias("jumps"),
    F.sum(F.col("is_speed_suspect").cast("int")).alias("speed_suspect"),
    F.sum(F.col("sog_knots").isNull().cast("int")).alias("speed_null"),
    F.sum(F.col("cog_deg").isNull().cast("int")).alias("course_null"),
).orderBy(F.desc("rows")).show(truncate=False)
ships = spark.read.format("delta").load(str(SILVER_AIS_SHIPS))
print("ships by category:")
ships.groupBy("ship_category").agg(F.count("*").alias("ships"), F.sum(F.col("imo_valid").cast("int")).alias("valid_imo"),
                                   F.sum(F.col("is_size_suspect").cast("int")).alias("size_suspect")).orderBy(F.desc("ships")).show(25, truncate=False)
vr = spark.read.format("delta").load(str(SILVER_AIS_VOYAGES))
print("voyage reports:")
vr.agg(F.count("*").alias("reports"), F.countDistinct("mmsi").alias("ships"), F.sum(F.col("eta").isNotNull().cast("int")).alias("with_eta"),
       F.sum(F.col("destination_is_placeholder").cast("int")).alias("placeholder_dest")).show()
