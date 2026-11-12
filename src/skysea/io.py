"""Reading and writing Parquet safely, without Delta.

Delta gave us two things for free: a crashed write never leaves a half table, and reruns of
bronze skip days already loaded. Plain Parquet gets the same with two habits:
  1. ATOMIC WRITE: write to <name>.tmp first, then rename. A rename on the same drive is a single
     filesystem operation, so readers see either the old file or the new one, never half of it.
  2. ONE FOLDER PER DAY (file_date=YYYY-MM-DD): "is this day loaded?" = "does the folder exist?".
     The folder name is a "hive partition": scan_parquet(hive_partitioning=True) turns it back
     into a file_date column, and a filter on file_date skips the other folders entirely.
"""
import os
import shutil
from pathlib import Path

import polars as pl

COMPRESSION = "zstd"  # better ratio than snappy at similar speed; what most new lakes default to


def write_file(df: pl.DataFrame, path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    df.write_parquet(tmp, compression=COMPRESSION, statistics=True)
    os.replace(tmp, path)  # atomic: replaces the old file in one step
    return df.height


def write_day(df: pl.DataFrame, table: Path, day: str) -> int:
    """One partition: <table>\\file_date=<day>\\data.parquet (file_date itself is not stored inside)."""
    return write_file(df.drop("file_date", strict=False), table / f"file_date={day}" / "data.parquet")


def replace_table(table: Path, build) -> None:
    """Rebuild a whole partitioned table: build(staging_dir) writes into a staging folder, then the
    old table is swapped out. A crash in build() leaves the old table untouched."""
    staging, old = table.with_name(table.name + "._staging"), table.with_name(table.name + "._old")
    for p in (staging, old):
        if p.exists():
            shutil.rmtree(p)
    build(staging)
    if table.exists():
        table.rename(old)
    staging.rename(table)
    if old.exists():
        shutil.rmtree(old)


def scan(path: Path) -> pl.LazyFrame:
    """A table as a LazyFrame (nothing is read yet). Folders are read with their file_date partitions."""
    if path.is_dir():
        return pl.scan_parquet(path / "**" / "*.parquet", hive_partitioning=True)
    return pl.scan_parquet(path)


def loaded_days(table: Path) -> set[str]:
    if not table.exists():
        return set()
    return {p.name.split("=", 1)[1] for p in table.glob("file_date=*") if (p / "data.parquet").exists()}
