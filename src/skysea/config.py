"""Paths, in one place. Everything lives on the Windows drive:

%USERPROFILE%\\Data\\SkyAndSea\\
    landing\\   raw .csv.gz the collectors write (never edited by the pipeline)
    bronze\\    same rows as Parquet, all strings + audit columns, one folder per day
    silver\\    typed, cleaned, flagged
    gold\\      tables that answer questions

Override the root with the env var SKYSEA_ROOT (e.g. an external SSD later).
"""
import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
REFERENCE_DIR = REPO_ROOT / "reference"

ROOT = Path(os.environ.get("SKYSEA_ROOT", Path.home() / "Data" / "SkyAndSea"))
LANDING_ROOT = ROOT / "landing"
BRONZE = ROOT / "bronze"
SILVER = ROOT / "silver"
GOLD = ROOT / "gold"

LANDING_PI_ROOT = ROOT / "landing_pi"

LANDING_AIS = LANDING_ROOT / "ais"
LANDING_OPENSKY = LANDING_ROOT / "opensky"
# Bronze reads every landing folder; a day found in more than one is merged + de-duplicated.
LANDING_AIS_DIRS = [LANDING_AIS, LANDING_PI_ROOT / "ais"]
LANDING_OPENSKY_DIRS = [LANDING_OPENSKY, LANDING_PI_ROOT / "opensky"]
LANDING_UNLOCODE = LANDING_ROOT / "reference" / "unlocode_code_list.csv"

# Partitioned tables are folders (file_date=YYYY-MM-DD\data.parquet); small tables are one file.
BRONZE_AIS_POSITIONS = BRONZE / "ais_positions"
BRONZE_AIS_STATIC = BRONZE / "ais_static"

SILVER_AIS_POSITIONS = SILVER / "ais_positions"
SILVER_AIS_REJECTED = SILVER / "ais_positions_rejected.parquet"
SILVER_AIS_SHIPS = SILVER / "ais_ships.parquet"
SILVER_AIS_VOYAGES = SILVER / "ais_voyage_reports.parquet"

GOLD_PORTS = GOLD / "ports.parquet"
GOLD_AIS_PORT_CALLS = GOLD / "ais_port_calls.parquet"
GOLD_AIS_PORT_TRAFFIC = GOLD / "ais_port_traffic.parquet"
GOLD_AIS_ETA_ACCURACY = GOLD / "ais_eta_accuracy.parquet"
GOLD_AIS_FLEET_HOURLY = GOLD / "ais_fleet_hourly.parquet"

# --- OpenSky (lesson 3) ---
LANDING_AIRPORTS = LANDING_ROOT / "reference" / "airports.csv"   # OurAirports, public domain

BRONZE_OPENSKY_STATES = BRONZE / "opensky_states"

SILVER_OPENSKY_STATES = SILVER / "opensky_states"
SILVER_OPENSKY_REJECTED = SILVER / "opensky_states_rejected.parquet"
SILVER_OPENSKY_AIRCRAFT = SILVER / "opensky_aircraft.parquet"

GOLD_AIRPORTS = GOLD / "airports.parquet"
GOLD_OPENSKY_FLIGHTS = GOLD / "opensky_flights.parquet"
GOLD_OPENSKY_AIRPORT_TRAFFIC = GOLD / "opensky_airport_traffic.parquet"
GOLD_OPENSKY_ALTITUDE_TOP = GOLD / "opensky_altitude_top.parquet"
GOLD_OPENSKY_EMERGENCIES = GOLD / "opensky_emergencies.parquet"
GOLD_OPENSKY_GNSS_ANOMALIES = GOLD / "opensky_gnss_anomalies.parquet"
GOLD_OPENSKY_AIR_HOURLY = GOLD / "opensky_air_hourly.parquet"
