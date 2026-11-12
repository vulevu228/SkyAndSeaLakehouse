"""Paths, in one place.

LANDING = the raw files the collectors write on the Windows drive (read-only for Spark).
Delta tables (bronze/silver/gold) live under SKYSEA_DATA_ROOT, default ~/skysea-data in WSL:
the Linux filesystem is far faster for Spark's many small writes than /mnt/c.
"""
import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
REFERENCE_DIR = REPO_ROOT / "reference"

LANDING_ROOT = Path(os.environ.get("SKYSEA_LANDING", "/mnt/c/Users/<you>/Data/SkyAndSea/landing"))
DATA_ROOT = Path(os.environ.get("SKYSEA_DATA_ROOT", Path.home() / "skysea-data"))

LANDING_AIS = LANDING_ROOT / "ais"
LANDING_OPENSKY = LANDING_ROOT / "opensky"

BRONZE_AIS_POSITIONS = DATA_ROOT / "bronze" / "ais_positions"
BRONZE_AIS_STATIC = DATA_ROOT / "bronze" / "ais_static"

SILVER_AIS_POSITIONS = DATA_ROOT / "silver" / "ais_positions"
SILVER_AIS_REJECTED = DATA_ROOT / "silver" / "ais_positions_rejected"
SILVER_AIS_SHIPS = DATA_ROOT / "silver" / "ais_ships"
SILVER_AIS_VOYAGES = DATA_ROOT / "silver" / "ais_voyage_reports"

LANDING_UNLOCODE = LANDING_ROOT / "reference" / "unlocode_code_list.csv"

GOLD_PORTS = DATA_ROOT / "gold" / "ports"
GOLD_AIS_PORT_CALLS = DATA_ROOT / "gold" / "ais_port_calls"
GOLD_AIS_PORT_TRAFFIC = DATA_ROOT / "gold" / "ais_port_traffic"
GOLD_AIS_ETA_ACCURACY = DATA_ROOT / "gold" / "ais_eta_accuracy"
GOLD_AIS_FLEET_HOURLY = DATA_ROOT / "gold" / "ais_fleet_hourly"
