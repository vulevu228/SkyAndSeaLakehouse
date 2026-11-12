r"""Load all gold tables into PostgreSQL (database skysea) and re-create the bi views.
Run:  .venv\Scripts\python scripts\run_load_postgres.py   (after run_gold_*.py)"""
import sys
import time

from skysea.config import GOLD, REPO_ROOT
from skysea.console import setup
from skysea.load.postgres import load_all

setup()
t0 = time.time()
report = load_all(GOLD, REPO_ROOT / "sql" / "views.sql")
ok = True
for name, parquet_rows, pg_rows, secs in report:
    flag = "OK" if parquet_rows == pg_rows else "MISMATCH"
    ok &= parquet_rows == pg_rows
    print(f"  gold.{name:<26} {pg_rows:>10,} rows  {flag}  ({secs:.1f}s)")
print(f"\n{len(report)} tables + bi views in {time.time() - t0:.0f}s -> {'OK' if ok else 'MISMATCH'}")
sys.exit(0 if ok else 1)
