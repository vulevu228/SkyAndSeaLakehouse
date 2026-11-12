r"""Load all finished OpenSky landing days into bronze.  .venv\Scripts\python scripts\run_bronze_opensky.py"""
import time

from skysea.bronze.landing import ingest
from skysea.config import BRONZE_OPENSKY_STATES, LANDING_OPENSKY_DIRS

t0 = time.time()
res = ingest([d / "raw" for d in LANDING_OPENSKY_DIRS], "states_*.csv.gz", BRONZE_OPENSKY_STATES)
print(f"bronze opensky_states: {res}  ({time.time() - t0:.0f}s)")
