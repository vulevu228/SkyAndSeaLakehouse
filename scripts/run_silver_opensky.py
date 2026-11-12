r"""Rebuild silver OpenSky from bronze and print a data-quality report.
Run:  .venv\Scripts\python scripts\run_silver_opensky.py"""
import time

import polars as pl

from skysea.console import setup
from skysea.config import (BRONZE_OPENSKY_STATES, SILVER_OPENSKY_AIRCRAFT, SILVER_OPENSKY_REJECTED,
                           SILVER_OPENSKY_STATES)
from skysea.io import loaded_days, replace_table, scan, write_day, write_file
from skysea.silver.opensky import build_aircraft, build_states_day

setup()
counts = {"good": 0, "rejected": 0}
rejected_days = []


def build(staging):
    carry = None  # each transponder's last position so far, handed from one day to the next
    for day in sorted(loaded_days(BRONZE_OPENSKY_STATES)):
        t0 = time.time()
        bronze_day = pl.scan_parquet(BRONZE_OPENSKY_STATES / f"file_date={day}" / "data.parquet").with_columns(
            pl.lit(day).str.to_date().alias("file_date"))
        good, rejected, carry = build_states_day(bronze_day, carry)
        counts["good"] += write_day(good.sort("icao24", "snapshot_time"), staging, day)
        counts["rejected"] += rejected.height
        rejected_days.append(rejected)
        print(f"  {day}  {good.height:>10,} good  {rejected.height:>7,} rejected  ({time.time() - t0:.1f}s)")


print("writing silver opensky_states, one day at a time:")
t0 = time.time()
replace_table(SILVER_OPENSKY_STATES, build)
write_file(pl.concat(rejected_days), SILVER_OPENSKY_REJECTED)
states = scan(SILVER_OPENSKY_STATES)
n_aircraft = write_file(build_aircraft(states).collect(engine="streaming"), SILVER_OPENSKY_AIRCRAFT)
print(f"  opensky_aircraft {n_aircraft:,}  (all done in {time.time() - t0:.0f}s)")

n_bronze = scan(BRONZE_OPENSKY_STATES).select(pl.len()).collect().item()
ok = n_bronze == counts["good"] + counts["rejected"]
print(f"\nbronze {n_bronze:,} = silver {counts['good']:,} + rejected {counts['rejected']:,} -> {'OK' if ok else 'MISMATCH'}")

print("\nrejected by reason:")
print(pl.read_parquet(SILVER_OPENSKY_REJECTED).group_by("reject_reason").len().sort("len", descending=True))
print("quality flags (rows / transponders):")
flags = ["is_stale_position", "is_speed_suspect", "is_altitude_suspect", "is_position_jump", "is_altitude_jump",
         "is_emergency_squawk", "is_test_transponder"]
print(pl.concat([states.filter(f).select(pl.lit(f).alias("flag"), pl.len().alias("rows"),
                                         pl.col("icao24").n_unique().alias("transponders")).collect() for f in flags]))
print("transponders with unreliable altitude (> 5% suspect states or any altitude jump):")
print(pl.read_parquet(SILVER_OPENSKY_AIRCRAFT).select(pl.len().alias("transponders"),
      pl.col("is_altitude_unreliable").sum().alias("unreliable")))
