"""Landing CSV.gz -> bronze Parquet, one folder per day. Raw, all strings. Used for AIS (positions,
static) and OpenSky (states): bronze does not care what is inside the file.

Only finished days (*.csv.gz) are loaded; today's plain .csv is still being written by the
collector. Audit columns:
  source_file  landing file the row came from (lineage / replay)
  file_date    UTC day of that file (the partition folder)
  ingested_at  when this pipeline loaded it

Reruns are safe: days that already have a bronze folder are skipped.

Several landing folders can feed one table (laptop landing\ + Raspberry Pi landing_pi\). Normally
each day lives in exactly one of them; if a day turns up in two (e.g. a test run on the other
machine), both files are read and exact duplicate rows are dropped, so nothing is counted twice.
"""
import re
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from skysea.io import loaded_days, write_day

DAY = re.compile(r"(\d{4}-\d{2}-\d{2})")


def read_landing(path: Path) -> pl.DataFrame:
    # infer_schema=False = every column is a String, exactly as the collector wrote it.
    # Typing happens in silver, where every failed cast is counted - a bad value can never break the load.
    # Polars decompresses .gz itself; quoted fields ("SHIP, NAME") are handled by the CSV parser.
    return pl.read_csv(path, infer_schema=False).with_columns(
        pl.lit(path.name).alias("source_file"),
        pl.lit(datetime.now(timezone.utc)).alias("ingested_at"),
    )


def read_day(paths: list[Path]) -> pl.DataFrame:
    """One UTC day from one or more landing files. Duplicates = same values in every collector column."""
    if len(paths) == 1:
        return read_landing(paths[0])
    df = pl.concat([read_landing(p) for p in paths], how="diagonal_relaxed")
    data_cols = [c for c in df.columns if c not in ("source_file", "ingested_at")]
    return df.unique(subset=data_cols, keep="first", maintain_order=True)


def ingest(landing_dirs: Path | list[Path], pattern: str, table: Path) -> dict:
    dirs = [landing_dirs] if isinstance(landing_dirs, Path) else landing_dirs
    by_day: dict[str, list[Path]] = {}
    for d in dirs:
        for f in sorted(d.glob(pattern)):
            by_day.setdefault(DAY.search(f.name).group(1), []).append(f)
    done = loaded_days(table)
    new = sorted(day for day in by_day if day not in done)
    rows = 0
    for day in new:
        rows += write_day(read_day(by_day[day]), table, day)
    return {"files_new": sum(len(by_day[d]) for d in new),
            "files_skipped": sum(len(v) for d, v in by_day.items() if d in done),
            "days_from_2_folders": sum(len(by_day[d]) > 1 for d in new), "rows_written": rows}
