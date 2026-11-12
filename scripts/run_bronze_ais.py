r"""Load all finished AIS landing days into bronze.  .venv\Scripts\python scripts\run_bronze_ais.py"""
import time

from skysea.bronze.landing import ingest
from skysea.config import BRONZE_AIS_POSITIONS, BRONZE_AIS_STATIC, LANDING_AIS_DIRS

for sub, table in [("positions", BRONZE_AIS_POSITIONS), ("static", BRONZE_AIS_STATIC)]:
    t0 = time.time()
    res = ingest([d / sub for d in LANDING_AIS_DIRS], f"{sub}_*.csv.gz", table)
    print(f"bronze ais_{sub}: {res}  ({time.time() - t0:.0f}s)")
