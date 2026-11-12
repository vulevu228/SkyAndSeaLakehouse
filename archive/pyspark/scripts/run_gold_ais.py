"""Build the gold AIS tables from silver and print the headline numbers.
Run in WSL:  bash scripts/wsl_run.sh scripts/run_gold_ais.py"""
import time

from pyspark.sql import functions as F

from skysea.config import (GOLD_AIS_ETA_ACCURACY, GOLD_AIS_FLEET_HOURLY, GOLD_AIS_PORT_CALLS,
                           GOLD_AIS_PORT_TRAFFIC, GOLD_PORTS, LANDING_UNLOCODE, SILVER_AIS_POSITIONS,
                           SILVER_AIS_SHIPS, SILVER_AIS_VOYAGES)
from skysea.gold.ais import (build_eta_accuracy, build_fleet_hourly, build_port_calls, build_port_traffic,
                             build_ports, port_weights)
from skysea.spark import get_spark

spark = get_spark("skysea-gold-ais")
load = lambda p: spark.read.format("delta").load(str(p))  # noqa: E731


def write(df, path):
    t0 = time.time()
    df.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(str(path))
    out = load(path)
    print(f"  {path.name:<20} {out.count():>10,} rows  ({time.time() - t0:.0f}s)")
    return out


positions, ships, reports = load(SILVER_AIS_POSITIONS), load(SILVER_AIS_SHIPS), load(SILVER_AIS_VOYAGES)
print("writing gold:")
ports = build_ports(spark, str(LANDING_UNLOCODE))
ports = write(ports.join(port_weights(reports, ports), "locode", "left").fillna(0, ["dest_ships"]), GOLD_PORTS)
calls = write(build_port_calls(positions, ports, ships), GOLD_AIS_PORT_CALLS)
traffic = write(build_port_traffic(calls), GOLD_AIS_PORT_TRAFFIC)
eta = write(build_eta_accuracy(reports, calls, ports), GOLD_AIS_ETA_ACCURACY)
fleet = write(build_fleet_hourly(positions, ships), GOLD_AIS_FLEET_HOURLY)

print("\nport calls: at a port or not / seagoing (valid IMO) or not")
calls.groupBy(F.col("locode").isNotNull().alias("at_port"), "is_seagoing").agg(
    F.count("*").alias("calls"), F.round(F.percentile_approx("duration_h", 0.5), 1).alias("median_h"),
    F.round(F.avg("n_stops_merged"), 2).alias("avg_stops_merged")).show()

print("busiest ports: SEAGOING cargo + tanker + passenger ships, by distinct ships")
(traffic.where(F.col("is_seagoing") & F.col("ship_category").isin("cargo", "tanker", "passenger"))
 .groupBy("locode", "port_name").agg(F.sum("calls").alias("calls"), F.sum("ships").alias("ships"),
                                     F.round(F.avg("median_stay_h"), 1).alias("median_stay_h"))
 .orderBy(F.desc("ships")).show(20, truncate=False))

print("ETA: stale (already in the past when broadcast) vs usable, seagoing ships")
eta.where("is_seagoing").groupBy("is_stale_eta").count().show()

print("ETA accuracy, seagoing, not stale (hours, + = arrived late) by how far ahead the ETA was announced")
(eta.where(F.col("is_seagoing") & ~F.col("is_stale_eta"))
 .withColumn("lead", F.when(F.col("lead_h") < 6, "a <6h").when(F.col("lead_h") < 24, "b 6-24h")
             .when(F.col("lead_h") < 72, "c 1-3d").otherwise("d >3d"))
 .groupBy("lead").agg(F.count("*").alias("n"),
                      F.round(F.percentile_approx("error_h", 0.5), 1).alias("median_err_h"),
                      F.round(F.percentile_approx(F.abs("error_h"), 0.5), 1).alias("median_abs_err_h"),
                      F.round(F.avg((F.abs("error_h") <= 2).cast("int")) * 100, 0).alias("pct_within_2h"))
 .orderBy("lead").show())

print("fleet: distinct ships seen per hour (all categories), min / median / max")
(fleet.groupBy("hour").agg(F.sum("ships").alias("ships"))
 .agg(F.min("ships"), F.percentile_approx("ships", 0.5), F.max("ships")).show())
