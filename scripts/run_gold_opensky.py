r"""Build the gold OpenSky tables from silver and print the headline numbers.
Run:  .venv\Scripts\python scripts\run_gold_opensky.py"""
import time

import polars as pl

from skysea.console import setup
from skysea.config import (GOLD_AIRPORTS, GOLD_OPENSKY_AIR_HOURLY, GOLD_OPENSKY_AIRPORT_TRAFFIC,
                           GOLD_OPENSKY_ALTITUDE_TOP, GOLD_OPENSKY_EMERGENCIES, GOLD_OPENSKY_FLIGHTS,
                           GOLD_OPENSKY_GNSS_ANOMALIES, LANDING_AIRPORTS, SILVER_OPENSKY_AIRCRAFT, SILVER_OPENSKY_STATES)
from skysea.gold.opensky import (FLIGHT_COLS, N_BUCKETS, airport_cells, bucket, build_air_hourly,
                                 build_airport_traffic, build_airports, build_altitude_top, build_emergencies,
                                 build_flights, build_gnss_anomalies)
from skysea.io import scan, write_file

setup()


def write(df, path):
    print(f"  {path.name:<34} {write_file(df, path):>10,} rows")
    return df


states = scan(SILVER_OPENSKY_STATES)
print("writing gold:")
airports = write(build_airports(str(LANDING_AIRPORTS)), GOLD_AIRPORTS)
cells = airport_cells(airports)

t0 = time.time()
parts = []
for i in range(N_BUCKETS):
    part = states.select(FLIGHT_COLS).filter(bucket(i)).collect(engine="streaming")
    parts.append(build_flights(part, cells))
    print(f"    bucket {i + 1}/{N_BUCKETS}: {part.height:>10,} states -> {parts[-1].height:>7,} flights")
flights = write(pl.concat(parts).sort("start"), GOLD_OPENSKY_FLIGHTS)
print(f"  (flights took {time.time() - t0:.0f}s)")
traffic = write(build_airport_traffic(flights, airports), GOLD_OPENSKY_AIRPORT_TRAFFIC)
top = write(build_altitude_top(flights, pl.read_parquet(SILVER_OPENSKY_AIRCRAFT)), GOLD_OPENSKY_ALTITUDE_TOP)
emerg = write(build_emergencies(states, flights), GOLD_OPENSKY_EMERGENCIES)
gnss = write(build_gnss_anomalies(states), GOLD_OPENSKY_GNSS_ANOMALIES)
hourly = write(build_air_hourly(states), GOLD_OPENSKY_AIR_HOURLY)

print("\nflights: how did we find where they started / ended?")
print(flights.group_by("dep_method", "arr_method").agg(
    pl.len().alias("flights"), pl.col("dep_airport").is_not_null().sum().alias("dep_found"),
    pl.col("arr_airport").is_not_null().sum().alias("arr_found")).sort("flights", descending=True))
both = flights.filter(pl.col("dep_airport").is_not_null() & pl.col("arr_airport").is_not_null())
print(f"flights with both airports: {both.height:,} of {flights.height:,}")

print("\nbusiest airports by JET-SPEED ARRIVALS (receiver coverage bias: US/Europe/Japan/Australia dense)")
print(traffic.filter("is_jet_speed").group_by("airport", "iata", "name", "country").agg(
    pl.col("arrivals").sum(), pl.col("departures").sum())
    .with_columns((pl.col("arrivals") / pl.col("departures")).round(2).alias("arr_per_dep"))
    .sort("arrivals", descending=True).head(20))

print("top jet routes (dep -> arr, both found)")
print(both.filter((pl.col("dep_airport") != pl.col("arr_airport")) & pl.col("is_jet_speed")).group_by("dep_airport", "arr_airport")
      .len().sort("len", descending=True).head(10))

print("highest flights (2nd-highest fix, so one glitch cannot set a record)")
print(top.select("kind", "icao24", "callsign", "origin_country", "max_alt_confirmed", "max_alt_confirmed_ft",
                 pl.col("median_velocity").round(1), "start").head(12))
print("highest AIRCRAFT (not balloons)")
print(top.filter(pl.col("kind") == "aircraft").select("icao24", "callsign", "origin_country", "max_alt_confirmed",
                                                      "max_alt_confirmed_ft", pl.col("median_velocity").round(1)).head(8))

print("emergency squawks: episodes (sustained = 2+ snapshots in a row)")
print(emerg.group_by("squawk", "emergency_type").agg(
    pl.len().alias("episodes"), pl.col("is_sustained").sum().alias("sustained"),
    pl.col("icao24").n_unique().alias("aircraft")).sort("squawk"))
print("longest sustained 7700 episodes")
print(emerg.filter(pl.col("is_sustained") & (pl.col("squawk") == "7700")).sort("n_snapshots", descending=True)
      .select("callsign", "origin_country", "start", "n_snapshots", "max_alt", "min_alt", "dep_airport", "arr_airport").head(10))

print("GNSS anomaly hotspots: 1-degree cells, summed over all days")
print(gnss.group_by("lat_cell", "lon_cell").agg(pl.col("anomalous_aircraft").sum(), pl.col("aircraft").sum(),
                                                 pl.len().alias("days")).sort("anomalous_aircraft", descending=True).head(12))

print("hourly: coverage and aircraft airborne per snapshot")
print(hourly.with_columns(pl.col("hour").dt.date().alias("day")).group_by("day").agg(
    pl.col("snapshots").sum(), pl.col("coverage").mean().round(2), pl.col("avg_airborne").mean().round(0),
    pl.col("avg_airborne").min().alias("min_hour"), pl.col("avg_airborne").max().alias("max_hour")).sort("day"))
