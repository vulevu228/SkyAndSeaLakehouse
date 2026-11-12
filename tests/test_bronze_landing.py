"""Bronze over several landing folders (laptop landing\ + Raspberry Pi landing_pi\)."""
import gzip

import polars as pl

from skysea.bronze.landing import ingest


def _gz(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as f:
        f.write(text)


def test_ingest_two_folders_merges_overlap_without_duplicates(tmp_path):
    laptop, pi_dir, table = tmp_path / "landing", tmp_path / "landing_pi", tmp_path / "bronze"
    _gz(laptop / "states_2026-10-28.csv.gz", "icao24,t\na,1\nb,1\n")
    _gz(pi_dir / "states_2026-10-29.csv.gz", "icao24,t\nc,2\n")
    _gz(pi_dir / "states_2026-10-28.csv.gz", "icao24,t\nb,1\nd,3\n")   # overlap: b is a duplicate
    res = ingest([laptop, pi_dir], "states_*.csv.gz", table)
    assert res == {"files_new": 3, "files_skipped": 0, "days_from_2_folders": 1, "rows_written": 4}
    day = pl.read_parquet(table / "file_date=2026-10-28" / "data.parquet")
    assert sorted(day["icao24"].to_list()) == ["a", "b", "d"]
    assert ingest([laptop, pi_dir], "states_*.csv.gz", table)["files_skipped"] == 3   # rerun = no-op
