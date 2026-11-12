"""Load all finished AIS landing days into bronze. Run in WSL:  python scripts/run_bronze_ais.py"""
import time

from skysea.bronze.ais import ingest
from skysea.config import BRONZE_AIS_POSITIONS, BRONZE_AIS_STATIC, LANDING_AIS
from skysea.schemas import AIS_POSITION_SCHEMA, AIS_STATIC_SCHEMA
from skysea.spark import get_spark

spark = get_spark("skysea-bronze-ais")
for name, sub, schema, path in [
    ("positions", "positions", AIS_POSITION_SCHEMA, BRONZE_AIS_POSITIONS),
    ("static", "static", AIS_STATIC_SCHEMA, BRONZE_AIS_STATIC),
]:
    t0 = time.time()
    res = ingest(spark, LANDING_AIS / sub, f"{sub}_*.csv.gz", schema, str(path))
    print(f"bronze ais_{name}: {res}  ({time.time() - t0:.0f}s)")
