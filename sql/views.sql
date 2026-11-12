-- Sky & Sea: the bi schema = views Power BI reads. Re-created by run_load_postgres.py after
-- every load (the gold tables are dropped + rebuilt, which drops these views with them).
-- All times are UTC; days are UTC days (the skysea database's timezone is set to UTC, so
-- ts::date cuts at UTC midnight for every client).
CREATE SCHEMA IF NOT EXISTS bi;

-- Calendar covering every day we have data for. Power BI marks it as the date table.
CREATE OR REPLACE VIEW bi.dim_date AS
WITH bounds AS (
    SELECT least(min_f, min_s)::date AS d0, greatest(max_f, max_s)::date AS d1
    FROM (SELECT min("start") AS min_f, max("start") AS max_f FROM gold.opensky_flights) f,
         (SELECT min("hour") AS min_s, max("hour") AS max_s FROM gold.ais_fleet_hourly) s
)
SELECT d::date                         AS day,
       extract(year FROM d)::int       AS year,
       extract(month FROM d)::int      AS month,
       to_char(d, 'YYYY-MM')           AS year_month,
       extract(isodow FROM d)::int     AS weekday_no,   -- 1 = Monday
       to_char(d, 'Dy')                AS weekday,
       extract(isodow FROM d) >= 6     AS is_weekend
FROM bounds, generate_series(bounds.d0, bounds.d1, interval '1 day') AS d;

-- How much of each day we actually saw. Every chart should be read next to this one:
-- a "quiet" day is often just a day the laptop was off.
CREATE OR REPLACE VIEW bi.coverage_daily AS
WITH sky AS (
    -- / 288 = one snapshot every 5 minutes for a whole day, so a half-collected day shows 0.5
    SELECT "hour"::date AS day, sum(snapshots) AS sky_snapshots,
           round(sum(snapshots) / 288.0, 2) AS sky_coverage,
           bool_or(is_forecast) AS sky_has_forecast
    FROM gold.opensky_air_hourly GROUP BY 1
), sea AS (
    SELECT day, sum(w) AS sea_windows, round(sum(w) / 288.0, 2) AS sea_coverage,
           bool_or(f) AS sea_has_forecast
    FROM (SELECT "hour"::date AS day, "hour", max(windows) AS w, bool_or(is_forecast) AS f
          FROM gold.ais_fleet_hourly GROUP BY 1, 2) h
    GROUP BY day
)
SELECT d.day, coalesce(sky.sky_snapshots, 0) AS sky_snapshots, coalesce(sky.sky_coverage, 0) AS sky_coverage,
       coalesce(sea.sea_windows, 0) AS sea_windows, coalesce(sea.sea_coverage, 0) AS sea_coverage,
       coalesce(sky.sky_has_forecast, false) OR coalesce(sea.sea_has_forecast, false) AS has_forecast
FROM bi.dim_date d
LEFT JOIN sky USING (day)
LEFT JOIN sea USING (day);

-- One row per day: reconstructed flights and how many we could tie to airports.
CREATE OR REPLACE VIEW bi.flights_daily AS
SELECT "start"::date                                                   AS day,
       count(*)                                                        AS flights,
       count(*) FILTER (WHERE is_jet_speed)                            AS jet_flights,
       count(*) FILTER (WHERE dep_airport IS NOT NULL AND arr_airport IS NOT NULL) AS flights_both_airports,
       round(percentile_cont(0.5) WITHIN GROUP (ORDER BY observed_minutes)::numeric, 1) AS median_observed_min
FROM gold.opensky_flights
GROUP BY 1;

-- Airport ranking over the whole period (jet-speed traffic only = no gliders/helicopters).
-- Ranked by ARRIVALS: 5-minute snapshots miss many take-offs, landings are seen more reliably.
CREATE OR REPLACE VIEW bi.airport_ranking AS
WITH t AS (
    SELECT airport, sum(arrivals) AS arrivals, sum(departures) AS departures,
           count(DISTINCT day) AS days_seen
    FROM gold.opensky_airport_traffic WHERE is_jet_speed
    GROUP BY airport
)
SELECT t.*, a.iata, a.name, a.country, a.municipality, a.ap_lat, a.ap_lon,
       round(t.arrivals::numeric / t.days_seen, 1)                          AS arrivals_per_day,
       rank() OVER (ORDER BY t.arrivals DESC)                               AS world_rank,
       rank() OVER (PARTITION BY a.country ORDER BY t.arrivals DESC)        AS country_rank
FROM t JOIN gold.airports a USING (airport);

-- Busiest airport pairs (both ends known, not a round trip to the same airport).
CREATE OR REPLACE VIEW bi.routes_top AS
SELECT f.dep_airport, f.arr_airport, count(*) AS flights,
       d.name AS dep_name, d.country AS dep_country, d.ap_lat AS dep_lat, d.ap_lon AS dep_lon,
       a.name AS arr_name, a.country AS arr_country, a.ap_lat AS arr_lat, a.ap_lon AS arr_lon
FROM gold.opensky_flights f
JOIN gold.airports d ON d.airport = f.dep_airport
JOIN gold.airports a ON a.airport = f.arr_airport
WHERE f.is_jet_speed AND f.dep_airport <> f.arr_airport
GROUP BY f.dep_airport, f.arr_airport, d.name, d.country, d.ap_lat, d.ap_lon,
         a.name, a.country, a.ap_lat, a.ap_lon
HAVING count(*) >= 10;

-- Emergency squawks with readable airports. Sustained = seen in 2+ snapshots (not a typo).
CREATE OR REPLACE VIEW bi.emergencies AS
SELECT e.icao24, e.episode, e.squawk, e.emergency_type, e.callsign, e.origin_country,
       e."start", e."end", e."start"::date AS day, e.n_snapshots, e.lat, e.lon, e.max_alt, e.min_alt,
       e.is_sustained, e.dep_airport, d.name AS dep_name, e.arr_airport, a.name AS arr_name
FROM gold.opensky_emergencies e
LEFT JOIN gold.airports d ON d.airport = e.dep_airport
LEFT JOIN gold.airports a ON a.airport = e.arr_airport;

-- GPS interference map: 1x1 degree cells per day; cell centre for the Power BI map.
-- Only cells with enough aircraft to mean something (one confused aircraft is not a zone).
CREATE OR REPLACE VIEW bi.gnss_cells AS
SELECT day, lat_cell + 0.5 AS lat, lon_cell + 0.5 AS lon, aircraft, anomalous_aircraft,
       round(share::numeric, 3) AS share
FROM gold.opensky_gnss_anomalies
WHERE aircraft >= 10;

-- Seaport ranking: seagoing ships only (valid IMO), so inland barges don't win.
CREATE OR REPLACE VIEW bi.port_ranking AS
WITH t AS (
    SELECT c.locode, count(*) AS calls, count(DISTINCT c.mmsi) AS ships,
           round(percentile_cont(0.5) WITHIN GROUP (ORDER BY c.duration_h)
                 FILTER (WHERE NOT c.is_truncated)::numeric, 1) AS median_stay_h
    FROM gold.ais_port_calls c WHERE c.is_seagoing
    GROUP BY c.locode
)
SELECT t.*, p.port_name, p.country, p.port_lat, p.port_lon,
       rank() OVER (ORDER BY t.ships DESC)                          AS world_rank,
       rank() OVER (PARTITION BY p.country ORDER BY t.ships DESC)   AS country_rank
FROM t JOIN gold.ports p USING (locode);

-- Daily arrivals per port and ship category (seagoing).
CREATE OR REPLACE VIEW bi.port_calls_daily AS
SELECT arrival::date AS day, locode, ship_category, count(*) AS arrivals,
       count(DISTINCT mmsi) AS ships
FROM gold.ais_port_calls WHERE is_seagoing
GROUP BY 1, 2, 3;

-- How good are ships' ETAs, by how far ahead they were announced? Stale ETAs counted apart.
CREATE OR REPLACE VIEW bi.eta_accuracy AS
WITH b AS (
    SELECT *,
           CASE WHEN lead_h < 6 THEN 1 WHEN lead_h < 24 THEN 2 WHEN lead_h < 72 THEN 3 ELSE 4 END AS bucket_no
    FROM gold.ais_eta_accuracy
)
SELECT bucket_no,
       (ARRAY['< 6 h', '6-24 h', '1-3 days', '> 3 days'])[bucket_no]          AS announced_ahead,
       count(*)                                                             AS etas,
       count(*) FILTER (WHERE is_stale_eta)                                 AS stale_etas,
       round(percentile_cont(0.5) WITHIN GROUP (ORDER BY abs(error_h))
             FILTER (WHERE NOT is_stale_eta)::numeric, 1)                   AS median_abs_error_h,
       round(avg((abs(error_h) <= 2)::int) FILTER (WHERE NOT is_stale_eta)::numeric, 2) AS share_within_2h
FROM b GROUP BY bucket_no;

-- Hamburg, from the air and the water: EDDH movements + DEHAM seagoing arrivals per day.
CREATE OR REPLACE VIEW bi.hamburg_daily AS
SELECT d.day,
       coalesce(air.arrivals, 0)   AS eddh_arrivals,
       coalesce(air.departures, 0) AS eddh_departures,
       coalesce(sea.arrivals, 0)   AS deham_ship_arrivals,
       coalesce(sea.ships, 0)      AS deham_ships,
       c.sky_coverage, c.sea_coverage
FROM bi.dim_date d
JOIN bi.coverage_daily c USING (day)
LEFT JOIN (SELECT day, arrivals, departures FROM gold.opensky_airport_traffic
           WHERE airport = 'EDDH' AND is_jet_speed) air USING (day)
LEFT JOIN (SELECT day, sum(arrivals) AS arrivals, sum(ships) AS ships FROM bi.port_calls_daily
           WHERE locode = 'DEHAM' GROUP BY day) sea USING (day);
