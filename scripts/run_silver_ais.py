r"""Rebuild the four silver AIS tables from bronze and print a data-quality report.
Run:  .venv\Scripts\python scripts\run_silver_ais.py   (~1-2 min)"""
import time

import polars as pl

from skysea.console import setup
from skysea.config import (BRONZE_AIS_POSITIONS, BRONZE_AIS_STATIC, SILVER_AIS_POSITIONS,
                           SILVER_AIS_REJECTED, SILVER_AIS_SHIPS, SILVER_AIS_VOYAGES)
from skysea.io import loaded_days, replace_table, scan, write_day, write_file
from skysea.silver.ais import (build_positions_day, build_ships, build_voyage_reports, read_reference,
                               type_static)

setup()
nav = read_reference("ais_nav_status.csv").with_columns(pl.col("nav_status").cast(pl.Int16))
counts = {"bronze": 0, "good": 0, "rejected": 0, "dups": 0}
rejected_days = []


def build(staging):
    carry = None  # each ship's last fix so far, handed from one day to the next
    for day in sorted(loaded_days(BRONZE_AIS_POSITIONS)):
        t0 = time.time()
        bronze_day = pl.scan_parquet(BRONZE_AIS_POSITIONS / f"file_date={day}" / "data.parquet").with_columns(
            pl.lit(day).str.to_date().alias("file_date"))
        good, rejected, dups, carry = build_positions_day(bronze_day, carry, nav)
        counts["bronze"] += good.height + rejected.height + dups
        counts["good"] += write_day(good.sort("mmsi", "window_start"), staging, day)
        counts["rejected"] += rejected.height
        counts["dups"] += dups
        rejected_days.append(rejected)
        print(f"  {day}  {good.height:>10,} good  {rejected.height:>7,} rejected  ({time.time() - t0:.1f}s)")


print("writing silver positions, one day at a time:")
t0 = time.time()
replace_table(SILVER_AIS_POSITIONS, build)
write_file(pl.concat(rejected_days), SILVER_AIS_REJECTED)
print(f"  positions done in {time.time() - t0:.0f}s")

t0 = time.time()
static = type_static(scan(BRONZE_AIS_STATIC)).collect()
ships = write_file(build_ships(static.lazy()).collect(), SILVER_AIS_SHIPS)
voyages = write_file(build_voyage_reports(static.lazy()).collect(), SILVER_AIS_VOYAGES)
print(f"  ais_ships {ships:,}  ais_voyage_reports {voyages:,}  ({time.time() - t0:.0f}s)")

# Every bronze row must end up in exactly one place - the core silver guarantee.
n_bronze = scan(BRONZE_AIS_POSITIONS).select(pl.len()).collect().item()
ok = n_bronze == counts["good"] + counts["rejected"] + counts["dups"]
print(f"\nbronze {n_bronze:,} = silver {counts['good']:,} + rejected {counts['rejected']:,}"
      f" + duplicates {counts['dups']:,} -> {'OK' if ok else 'MISMATCH'}")

pos = scan(SILVER_AIS_POSITIONS)
print("\nrejected by reason:")
print(pl.read_parquet(SILVER_AIS_REJECTED).group_by("reject_reason").len().sort("len", descending=True))
print("positions by transmitter kind + flags:")
print(pos.group_by("mmsi_kind").agg(
    pl.len().alias("rows"), pl.col("mmsi").n_unique().alias("ids"),
    pl.col("is_position_jump").sum().alias("jumps"), pl.col("is_speed_suspect").sum().alias("speed_suspect"),
    pl.col("sog_knots").null_count().alias("speed_null"), pl.col("cog_deg").null_count().alias("course_null"),
).sort("rows", descending=True).collect(engine="streaming"))
print("ships by category:")
print(pl.read_parquet(SILVER_AIS_SHIPS).group_by("ship_category").agg(
    pl.len().alias("ships"), pl.col("imo_valid").sum().alias("valid_imo"),
    pl.col("is_size_suspect").sum().alias("size_suspect")).sort("ships", descending=True))
print("voyage reports:")
print(pl.read_parquet(SILVER_AIS_VOYAGES).select(
    pl.len().alias("reports"), pl.col("mmsi").n_unique().alias("ships"),
    pl.col("eta").is_not_null().sum().alias("with_eta"),
    pl.col("destination_is_placeholder").sum().alias("placeholder_dest")))
