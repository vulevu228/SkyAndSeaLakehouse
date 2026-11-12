-- Sky & Sea: data dictionary. A comment on every gold table + column (DBeaver shows them in the
-- column list and as tooltips; psql: \d+ gold.<table>). Re-applied by run_load_postgres.py after
-- every load, because the tables are re-created each time.
-- Conventions: times = timestamptz UTC; days = UTC days; lat/lon = degrees (WGS84);
-- altitudes in METRES (pressure/baro altitude) unless the name ends in _ft; speeds in m/s
-- (aircraft) or knots (ships). Read everything next to bi.coverage_daily.

-- ---------------------------------------------------------------- reference tables
COMMENT ON TABLE gold.airports IS 'Reference: large + medium airports from OurAirports (public domain). One row per airport. Used to tie flights to airports.';
COMMENT ON COLUMN gold.airports.airport      IS 'Airport code (ICAO style ident, e.g. EDDH = Hamburg). Primary key, joins to *_airport columns.';
COMMENT ON COLUMN gold.airports.iata         IS '3-letter IATA code as on tickets (HAM). NULL for ~700 airports that have none.';
COMMENT ON COLUMN gold.airports.name         IS 'Airport name.';
COMMENT ON COLUMN gold.airports.country      IS '2-letter ISO country code (DE, US).';
COMMENT ON COLUMN gold.airports.municipality IS 'City the airport serves. Can be NULL.';
COMMENT ON COLUMN gold.airports.type         IS 'large_airport or medium_airport.';
COMMENT ON COLUMN gold.airports.type_rank    IS '1 = large, 2 = medium. Tie-break: a flight near two airports goes to the larger one.';
COMMENT ON COLUMN gold.airports.ap_lat       IS 'Airport latitude.';
COMMENT ON COLUMN gold.airports.ap_lon       IS 'Airport longitude.';
COMMENT ON COLUMN gold.airports.elevation_ft IS 'Airport elevation in feet above sea level.';

COMMENT ON TABLE gold.ports IS 'Reference: UN/LOCODE port points. One row per port. Used to tie ship stops to ports.';
COMMENT ON COLUMN gold.ports.locode     IS '5-letter UN/LOCODE: country + place (DEHAM = Hamburg, NLRTM = Rotterdam). Primary key.';
COMMENT ON COLUMN gold.ports.country    IS '2-letter ISO country code.';
COMMENT ON COLUMN gold.ports.port_name  IS 'Port name.';
COMMENT ON COLUMN gold.ports.port_lat   IS 'Port latitude (LOCODE point, can be off by a few km).';
COMMENT ON COLUMN gold.ports.port_lon   IS 'Port longitude.';
COMMENT ON COLUMN gold.ports.dest_ships IS 'Distinct ships that typed this port as their AIS destination. Weight: busy ports win ties.';

-- ---------------------------------------------------------------- OpenSky (aircraft)
COMMENT ON TABLE gold.opensky_flights IS 'One row per reconstructed flight: consecutive airborne snapshots of one aircraft with no gap > 30 min. Snapshots every 5 min (10 min 5-10 Oct).';
COMMENT ON COLUMN gold.opensky_flights.flight_id         IS 'icao24 + running number (a6e7f5-3 = 3rd flight of that aircraft). Primary key.';
COMMENT ON COLUMN gold.opensky_flights.icao24            IS 'Transponder address (hex) = the physical aircraft. Stays the same across flights.';
COMMENT ON COLUMN gold.opensky_flights.callsign          IS 'Flight callsign (DLH4AB, or a registration for private planes). Changes per flight. Can be NULL.';
COMMENT ON COLUMN gold.opensky_flights.origin_country    IS 'Country the transponder is registered in (NOT where the flight went). Full name, e.g. Germany.';
COMMENT ON COLUMN gold.opensky_flights.is_jet_speed      IS 'TRUE = median ground speed >= 100 m/s (~195 kn): airliners/jets. FALSE = light aircraft, helicopters, slow.';
COMMENT ON COLUMN gold.opensky_flights.start             IS 'First airborne snapshot of the flight (UTC). Real take-off is up to 5 min earlier.';
COMMENT ON COLUMN gold.opensky_flights."end"             IS 'Last airborne snapshot (UTC). Quote it: "end" is an SQL keyword.';
COMMENT ON COLUMN gold.opensky_flights.observed_minutes  IS 'end - start in minutes = how long WE saw it, not the real flight time (coverage holes).';
COMMENT ON COLUMN gold.opensky_flights.n_fixes           IS 'Number of snapshots (positions) in this flight.';
COMMENT ON COLUMN gold.opensky_flights.n_altitude_jumps  IS 'Altitude jumps > 1,500 m between snapshots (transponder errors). > 0 = do not trust max_alt.';
COMMENT ON COLUMN gold.opensky_flights.dep_airport       IS 'Departure airport (joins gold.airports.airport). NULL = could not tell (~59%: flight was already airborne when first seen).';
COMMENT ON COLUMN gold.opensky_flights.dep_method        IS 'How dep_airport was found: ground (seen on the ground there within 30 min), low_altitude (first fix < 1,200 m within 25 km), unknown.';
COMMENT ON COLUMN gold.opensky_flights.dep_km            IS 'Distance in km from the first fix to dep_airport (0-25). NULL when dep_airport is NULL.';
COMMENT ON COLUMN gold.opensky_flights.arr_airport       IS 'Arrival airport. NULL = could not tell (~44%).';
COMMENT ON COLUMN gold.opensky_flights.arr_method        IS 'Same as dep_method, for the end of the flight.';
COMMENT ON COLUMN gold.opensky_flights.arr_km            IS 'Distance in km from the last fix to arr_airport.';
COMMENT ON COLUMN gold.opensky_flights.max_alt           IS 'Highest pressure altitude in metres (raw, can be an error).';
COMMENT ON COLUMN gold.opensky_flights.max_alt_confirmed IS 'Highest altitude in metres reached by at least 2 snapshots (one wild value cannot set it). Use this one.';
COMMENT ON COLUMN gold.opensky_flights.median_velocity   IS 'Median ground speed in m/s (x 1.944 = knots, x 3.6 = km/h).';
COMMENT ON COLUMN gold.opensky_flights.first_lat         IS 'Latitude of the first fix.';
COMMENT ON COLUMN gold.opensky_flights.first_lon         IS 'Longitude of the first fix.';
COMMENT ON COLUMN gold.opensky_flights.last_lat          IS 'Latitude of the last fix.';
COMMENT ON COLUMN gold.opensky_flights.last_lon          IS 'Longitude of the last fix.';

COMMENT ON TABLE gold.opensky_airport_traffic IS 'One row per airport + UTC day + jet/non-jet: departures and arrivals counted from gold.opensky_flights. Departures are undercounted (snapshots miss take-offs); rank by arrivals.';
COMMENT ON COLUMN gold.opensky_airport_traffic.airport      IS 'Airport code (joins gold.airports).';
COMMENT ON COLUMN gold.opensky_airport_traffic.day          IS 'UTC day.';
COMMENT ON COLUMN gold.opensky_airport_traffic.is_jet_speed IS 'TRUE = jet-speed flights only, FALSE = slow traffic. Filter on it, or you mix both.';
COMMENT ON COLUMN gold.opensky_airport_traffic.departures   IS 'Flights with dep_airport = this airport that day (a COUNT, not a flag).';
COMMENT ON COLUMN gold.opensky_airport_traffic.arrivals     IS 'Flights with arr_airport = this airport that day.';
COMMENT ON COLUMN gold.opensky_airport_traffic.aircraft     IS 'Distinct aircraft (icao24) that departed or arrived that day. A NUMBER, not a type.';
COMMENT ON COLUMN gold.opensky_airport_traffic.iata         IS 'Copied from gold.airports.';
COMMENT ON COLUMN gold.opensky_airport_traffic.name         IS 'Copied from gold.airports.';
COMMENT ON COLUMN gold.opensky_airport_traffic.country      IS 'Copied from gold.airports (ISO code, DE).';
COMMENT ON COLUMN gold.opensky_airport_traffic.municipality IS 'Copied from gold.airports.';

COMMENT ON TABLE gold.opensky_emergencies IS 'Emergency squawk episodes: an aircraft transmitting 7500/7600/7700. NOT crashes - most end with a normal landing.';
COMMENT ON COLUMN gold.opensky_emergencies.icao24         IS 'Transponder address.';
COMMENT ON COLUMN gold.opensky_emergencies.episode        IS 'Running number of episodes for this aircraft (1, 2, 3). PK = icao24 + episode.';
COMMENT ON COLUMN gold.opensky_emergencies.squawk         IS '7700 general emergency, 7600 radio failure, 7500 hijack (text, not a number).';
COMMENT ON COLUMN gold.opensky_emergencies.emergency_type IS 'Readable form of squawk.';
COMMENT ON COLUMN gold.opensky_emergencies.callsign       IS 'Callsign during the episode.';
COMMENT ON COLUMN gold.opensky_emergencies.origin_country IS 'Registration country of the transponder.';
COMMENT ON COLUMN gold.opensky_emergencies.start          IS 'First snapshot with the squawk (UTC).';
COMMENT ON COLUMN gold.opensky_emergencies."end"          IS 'Last snapshot with the squawk (UTC).';
COMMENT ON COLUMN gold.opensky_emergencies.n_snapshots    IS 'Snapshots showing the squawk.';
COMMENT ON COLUMN gold.opensky_emergencies.lat            IS 'Latitude at the first snapshot.';
COMMENT ON COLUMN gold.opensky_emergencies.lon            IS 'Longitude at the first snapshot.';
COMMENT ON COLUMN gold.opensky_emergencies.max_alt        IS 'Highest altitude during the episode, metres. NULL = no altitude reported.';
COMMENT ON COLUMN gold.opensky_emergencies.min_alt        IS 'Lowest altitude during the episode, metres.';
COMMENT ON COLUMN gold.opensky_emergencies.is_sustained   IS 'TRUE = seen in 2+ snapshots. FALSE = one snapshot, often a pilot dialling past 7xxx or a decode error.';
COMMENT ON COLUMN gold.opensky_emergencies.flight_id      IS 'The flight it happened on (joins gold.opensky_flights). Can be NULL.';
COMMENT ON COLUMN gold.opensky_emergencies.dep_airport    IS 'Departure airport of that flight, if known.';
COMMENT ON COLUMN gold.opensky_emergencies.arr_airport    IS 'Arrival airport of that flight, if known (often a diversion).';

COMMENT ON TABLE gold.opensky_gnss_anomalies IS 'GPS interference per 1x1 degree cell per UTC day: aircraft with impossible speeds or position jumps. Only cells with 3+ such aircraft.';
COMMENT ON COLUMN gold.opensky_gnss_anomalies.day                IS 'UTC day.';
COMMENT ON COLUMN gold.opensky_gnss_anomalies.lat_cell           IS 'Cell = floor(latitude): 54 means 54.0-55.0 N. Centre = lat_cell + 0.5.';
COMMENT ON COLUMN gold.opensky_gnss_anomalies.lon_cell           IS 'Cell = floor(longitude): 21 means 21.0-22.0 E. Negative = west.';
COMMENT ON COLUMN gold.opensky_gnss_anomalies.aircraft           IS 'Distinct airborne aircraft seen in the cell that day.';
COMMENT ON COLUMN gold.opensky_gnss_anomalies.anomalous_aircraft IS 'Of those, how many reported a suspect speed or a position jump.';
COMMENT ON COLUMN gold.opensky_gnss_anomalies.share              IS 'anomalous_aircraft / aircraft, 0-1. Compare areas by share, not by count.';

COMMENT ON TABLE gold.opensky_air_hourly IS 'One row per UTC hour: how many aircraft were in the air on average, and how many snapshots we took.';
COMMENT ON COLUMN gold.opensky_air_hourly."hour"        IS 'Start of the hour (UTC).';
COMMENT ON COLUMN gold.opensky_air_hourly.snapshots     IS 'Snapshots taken in this hour (12 = every 5 min; 6 = every 10 min).';
COMMENT ON COLUMN gold.opensky_air_hourly.avg_airborne  IS 'Average number of aircraft in the air per snapshot.';
COMMENT ON COLUMN gold.opensky_air_hourly.avg_aircraft  IS 'Average aircraft per snapshot incl. those on the ground.';
COMMENT ON COLUMN gold.opensky_air_hourly.coverage      IS 'snapshots / 12. 1.0 = full hour; can be 1.08 when a 13th snapshot slips in.';
COMMENT ON COLUMN gold.opensky_air_hourly.is_forecast   IS 'TRUE = filled by a forecast, not measured (none yet).';

COMMENT ON TABLE gold.opensky_altitude_top IS 'Top 50 highest flights after cleaning (confirmed altitude, reliable transponders only), split into aircraft and balloons.';
COMMENT ON COLUMN gold.opensky_altitude_top.kind                 IS 'aircraft, or balloon (above 15 km and slower than 100 m/s).';
COMMENT ON COLUMN gold.opensky_altitude_top.icao24               IS 'Transponder address.';
COMMENT ON COLUMN gold.opensky_altitude_top.callsign             IS 'Callsign.';
COMMENT ON COLUMN gold.opensky_altitude_top.origin_country       IS 'Registration country.';
COMMENT ON COLUMN gold.opensky_altitude_top.max_alt_confirmed    IS 'Confirmed max altitude, metres.';
COMMENT ON COLUMN gold.opensky_altitude_top.max_alt_confirmed_ft IS 'Same in feet.';
COMMENT ON COLUMN gold.opensky_altitude_top.max_alt              IS 'Raw max altitude, metres.';
COMMENT ON COLUMN gold.opensky_altitude_top.median_velocity      IS 'Median ground speed, m/s.';
COMMENT ON COLUMN gold.opensky_altitude_top.start                IS 'Flight start (UTC).';
COMMENT ON COLUMN gold.opensky_altitude_top."end"                IS 'Flight end (UTC).';
COMMENT ON COLUMN gold.opensky_altitude_top.dep_airport          IS 'Departure airport if known.';
COMMENT ON COLUMN gold.opensky_altitude_top.arr_airport          IS 'Arrival airport if known.';

-- ---------------------------------------------------------------- AIS (ships)
COMMENT ON TABLE gold.ais_port_calls IS 'One row per port call: a ship stopped (< 0.5 kn, or moored drift < 1 kn) for 60+ min, stops < 2 h apart merged. Ships listened to 60 s every 5 min.';
COMMENT ON COLUMN gold.ais_port_calls.mmsi             IS 'Ship radio ID (9 digits, as text). Joins all AIS tables.';
COMMENT ON COLUMN gold.ais_port_calls.call_no          IS 'Running number of calls for this ship.';
COMMENT ON COLUMN gold.ais_port_calls.arrival          IS 'First stopped fix (UTC).';
COMMENT ON COLUMN gold.ais_port_calls.departure        IS 'Last stopped fix (UTC).';
COMMENT ON COLUMN gold.ais_port_calls.locode           IS 'Port (joins gold.ports). NULL (~12%) = stopped at sea / anchorage, no port within 20 km.';
COMMENT ON COLUMN gold.ais_port_calls.port_name        IS 'Port name.';
COMMENT ON COLUMN gold.ais_port_calls.country          IS 'Port country, ISO code.';
COMMENT ON COLUMN gold.ais_port_calls.port_km          IS 'Distance from the stop to the port point, km (0-20).';
COMMENT ON COLUMN gold.ais_port_calls.lat              IS 'Stop position, latitude.';
COMMENT ON COLUMN gold.ais_port_calls.lon              IS 'Stop position, longitude.';
COMMENT ON COLUMN gold.ais_port_calls.n_fixes          IS 'Position reports during the stay.';
COMMENT ON COLUMN gold.ais_port_calls.n_stops_merged   IS 'How many short stops were merged into this call (shifting berth, GPS noise).';
COMMENT ON COLUMN gold.ais_port_calls.nav_status_label IS 'Most common navigation status the crew set (moored, at anchor ...). Often NULL.';
COMMENT ON COLUMN gold.ais_port_calls.is_truncated     IS 'TRUE = the stay touches the start/end of our data: real duration unknown. Exclude for stay lengths.';
COMMENT ON COLUMN gold.ais_port_calls.duration_h       IS 'departure - arrival in hours (min 1).';
COMMENT ON COLUMN gold.ais_port_calls.ship_name        IS 'Ship name from AIS static data.';
COMMENT ON COLUMN gold.ais_port_calls.ship_category    IS 'cargo, tanker, passenger, fishing, tug, pleasure craft ... NULL = never sent static data.';
COMMENT ON COLUMN gold.ais_port_calls.length_m         IS 'Ship length in metres (from AIS, can be wrong).';
COMMENT ON COLUMN gold.ais_port_calls.is_seagoing      IS 'TRUE = ship has a valid IMO number (sea-going). FALSE = inland barges, small craft. Filter on it for port rankings.';
COMMENT ON COLUMN gold.ais_port_calls.call_id          IS 'mmsi + call_no. Primary key.';

COMMENT ON TABLE gold.ais_port_traffic IS 'Per port + ship category + seagoing flag: calls, distinct ships, median stay. Pre-aggregated from ais_port_calls.';
COMMENT ON COLUMN gold.ais_port_traffic.locode        IS 'Port (joins gold.ports).';
COMMENT ON COLUMN gold.ais_port_traffic.port_name     IS 'Port name.';
COMMENT ON COLUMN gold.ais_port_traffic.country       IS 'ISO country code.';
COMMENT ON COLUMN gold.ais_port_traffic.ship_category IS 'Ship category (unknown when not sent).';
COMMENT ON COLUMN gold.ais_port_traffic.is_seagoing   IS 'Valid IMO = sea-going.';
COMMENT ON COLUMN gold.ais_port_traffic.calls         IS 'Number of port calls.';
COMMENT ON COLUMN gold.ais_port_traffic.ships         IS 'Distinct ships. Do NOT sum across categories for a port total if a ship changed category - use ais_port_calls.';
COMMENT ON COLUMN gold.ais_port_traffic.median_stay_h IS 'Median stay in hours.';

COMMENT ON TABLE gold.ais_eta_accuracy IS 'One row per announced ETA that we could match to a real arrival: how wrong was it?';
COMMENT ON COLUMN gold.ais_eta_accuracy.mmsi           IS 'Ship radio ID.';
COMMENT ON COLUMN gold.ais_eta_accuracy.destination    IS 'Destination as the crew typed it (free text: HAMBURG, DE HAM ...).';
COMMENT ON COLUMN gold.ais_eta_accuracy.dest_locode    IS 'That destination cleaned to a UN/LOCODE.';
COMMENT ON COLUMN gold.ais_eta_accuracy.eta            IS 'Announced arrival time (UTC).';
COMMENT ON COLUMN gold.ais_eta_accuracy.first_reported IS 'When the ship first broadcast this ETA.';
COMMENT ON COLUMN gold.ais_eta_accuracy.last_reported  IS 'When it last broadcast it.';
COMMENT ON COLUMN gold.ais_eta_accuracy.arrival        IS 'Real arrival = start of the matched port call.';
COMMENT ON COLUMN gold.ais_eta_accuracy.call_id        IS 'Matched port call (joins gold.ais_port_calls).';
COMMENT ON COLUMN gold.ais_eta_accuracy.error_h        IS 'arrival - eta in hours. > 0 = arrived LATER than announced. Use abs() for size of error.';
COMMENT ON COLUMN gold.ais_eta_accuracy.lead_h         IS 'arrival - first_reported in hours = how far ahead the ETA was given.';
COMMENT ON COLUMN gold.ais_eta_accuracy.is_stale_eta   IS 'TRUE = the ETA was already > 1 h in the past when first sent (crew never updated it). Exclude for accuracy.';
COMMENT ON COLUMN gold.ais_eta_accuracy.is_seagoing    IS 'Valid IMO = sea-going.';
COMMENT ON COLUMN gold.ais_eta_accuracy.ship_category  IS 'Ship category.';
COMMENT ON COLUMN gold.ais_eta_accuracy.n_reports      IS 'How many times this ETA was broadcast.';

COMMENT ON TABLE gold.ais_fleet_hourly IS 'One row per UTC hour + ship category: ships seen and ships moving.';
COMMENT ON COLUMN gold.ais_fleet_hourly."hour"        IS 'Start of the hour (UTC).';
COMMENT ON COLUMN gold.ais_fleet_hourly.ship_category IS 'Ship category.';
COMMENT ON COLUMN gold.ais_fleet_hourly.ships         IS 'Distinct ships of this category seen in the hour.';
COMMENT ON COLUMN gold.ais_fleet_hourly.moving_ships  IS 'Of those, ships doing >= 0.5 kn.';
COMMENT ON COLUMN gold.ais_fleet_hourly.windows       IS 'Listening windows in the hour (12 = every 5 min) = coverage.';
COMMENT ON COLUMN gold.ais_fleet_hourly.is_forecast   IS 'TRUE = forecast, not measured (none yet).';

COMMENT ON TABLE gold.load_runs IS 'Load log: one row per table per run_load_postgres.py run. parquet_rows must equal pg_rows.';
COMMENT ON COLUMN gold.load_runs.loaded_at    IS 'now() = start of the load TRANSACTION: identical for all tables of one run.';
COMMENT ON COLUMN gold.load_runs.table_name   IS 'Gold table that was loaded.';
COMMENT ON COLUMN gold.load_runs.parquet_rows IS 'Rows in the Parquet file.';
COMMENT ON COLUMN gold.load_runs.pg_rows      IS 'Rows in Postgres after the COPY.';
COMMENT ON COLUMN gold.load_runs.seconds      IS 'Time to load this table.';

-- ---------------------------------------------------------------- the dictionary itself
CREATE SCHEMA IF NOT EXISTS meta;
CREATE OR REPLACE VIEW meta.data_dictionary AS
SELECT c.table_schema AS schema_name, c.table_name, c.ordinal_position AS col_no, c.column_name,
       c.data_type, c.is_nullable = 'YES' AS nullable,
       obj_description(format('%I.%I', c.table_schema, c.table_name)::regclass, 'pg_class') AS table_description,
       col_description(format('%I.%I', c.table_schema, c.table_name)::regclass, c.ordinal_position) AS description
FROM information_schema.columns c
WHERE c.table_schema IN ('gold', 'bi')
ORDER BY c.table_schema, c.table_name, c.ordinal_position;
