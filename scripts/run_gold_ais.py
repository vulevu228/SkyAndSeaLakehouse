r"""Build the gold AIS tables from silver and print the headline numbers.
Run:  .venv\Scripts\python scripts\run_gold_ais.py"""
import time

import polars as pl

from skysea.console import setup
from skysea.config import (GOLD_AIS_ETA_ACCURACY, GOLD_AIS_FLEET_HOURLY, GOLD_AIS_PORT_CALLS,
                           GOLD_AIS_PORT_TRAFFIC, GOLD_PORTS, LANDING_UNLOCODE, SILVER_AIS_POSITIONS,
                           SILVER_AIS_SHIPS, SILVER_AIS_VOYAGES)
from skysea.gold.ais import (N_BUCKETS, STOP_COLS, bucket, build_eta_accuracy, build_fleet_hourly,
                             build_port_calls, build_port_traffic, build_ports, port_cells, port_weights)
from skysea.io import scan, write_file

setup()


def write(df, path):
    t0 = time.time()
    n = write_file(df, path)
    print(f"  {path.name:<28} {n:>10,} rows  ({time.time() - t0:.0f}s)")
    return df


positions = scan(SILVER_AIS_POSITIONS)
ships, reports = pl.read_parquet(SILVER_AIS_SHIPS), pl.read_parquet(SILVER_AIS_VOYAGES)

print("writing gold:")
ports = build_ports(str(LANDING_UNLOCODE))
ports = write(ports.join(port_weights(reports, ports), on="locode", how="left")
              .with_columns(pl.col("dest_ships").fill_null(0)), GOLD_PORTS)

# Port calls, 1/N of the ships at a time (a ship's calls only depend on its own rows).
t0 = time.time()
cells = port_cells(ports)
data_start, data_end = positions.select(pl.col("window_start").min().alias("first"),
                                       pl.col("window_start").max().alias("last")).collect().row(0)
parts = []
for i in range(N_BUCKETS):
    part = positions.select(STOP_COLS).filter(bucket(i)).collect(engine="streaming")
    parts.append(build_port_calls(part, cells, ships, data_start, data_end))
    print(f"    bucket {i + 1}/{N_BUCKETS}: {part.height:>10,} positions -> {parts[-1].height:>7,} calls")
calls = write(pl.concat(parts).sort("mmsi", "arrival"), GOLD_AIS_PORT_CALLS)
print(f"  (port calls took {time.time() - t0:.0f}s)")
traffic = write(build_port_traffic(calls), GOLD_AIS_PORT_TRAFFIC)
eta = write(build_eta_accuracy(reports, calls, ports), GOLD_AIS_ETA_ACCURACY)
fleet = write(build_fleet_hourly(positions, ships).collect(engine="streaming"), GOLD_AIS_FLEET_HOURLY)

print("\nport calls: at a port or not / seagoing (valid IMO) or not")
print(calls.group_by(pl.col("locode").is_not_null().alias("at_port"), "is_seagoing").agg(
    pl.len().alias("calls"), pl.col("duration_h").median().round(1).alias("median_h"),
    pl.col("n_stops_merged").mean().round(2).alias("avg_stops_merged")).sort("at_port", "is_seagoing"))

print("busiest ports: SEAGOING cargo + tanker + passenger ships, by distinct ships")
print(traffic.filter(pl.col("is_seagoing") & pl.col("ship_category").is_in(["cargo", "tanker", "passenger"]))
      .group_by("locode", "port_name").agg(pl.col("calls").sum(), pl.col("ships").sum(),
                                           pl.col("median_stay_h").mean().round(1))
      .sort("ships", descending=True).head(20))

print("ETA: stale (already in the past when broadcast) vs usable, seagoing ships")
print(eta.filter("is_seagoing").group_by("is_stale_eta").len().sort("is_stale_eta"))

print("ETA accuracy, seagoing, not stale (hours, + = arrived late) by how far ahead the ETA was announced")
lead = pl.col("lead_h")
print(eta.filter(pl.col("is_seagoing") & ~pl.col("is_stale_eta"))
      .with_columns(pl.when(lead < 6).then(pl.lit("a <6h")).when(lead < 24).then(pl.lit("b 6-24h"))
                    .when(lead < 72).then(pl.lit("c 1-3d")).otherwise(pl.lit("d >3d")).alias("lead"))
      .group_by("lead").agg(pl.len().alias("n"), pl.col("error_h").median().round(1).alias("median_err_h"),
                            pl.col("error_h").abs().median().round(1).alias("median_abs_err_h"),
                            ((pl.col("error_h").abs() <= 2).mean() * 100).round(0).alias("pct_within_2h"))
      .sort("lead"))

print("fleet: distinct ships seen per hour (all categories), min / median / max")
print(fleet.group_by("hour").agg(pl.col("ships").sum()).select(
    pl.col("ships").min().alias("min"), pl.col("ships").median().alias("median"), pl.col("ships").max().alias("max")))
