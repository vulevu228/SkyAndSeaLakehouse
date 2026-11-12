"""Gaps-and-islands on one hand-made ship: does a noisy stay become ONE port call?"""
from datetime import datetime, timedelta, timezone

import polars as pl

from skysea.gold.ais import find_stops, merge_stops


def positions(speeds, nav=5):
    t0 = datetime(2026, 9, 28, tzinfo=timezone.utc)
    n = len(speeds)
    return pl.DataFrame({
        "mmsi": ["211000001"] * n, "mmsi_kind": ["ship"] * n, "is_position_jump": [False] * n,
        "window_start": [t0 + timedelta(minutes=5 * i) for i in range(n)], "sog_knots": speeds,
        "nav_status": [nav] * n, "nav_status_label": ["moored"] * n, "latitude": [53.54] * n, "longitude": [9.97] * n,
    })


def test_moving_stopped_moving_gives_one_stop():
    # 1 h sailing, 2 h stopped, 1 h sailing (5-minute windows)
    stops = find_stops(positions([12.0] * 12 + [0.1] * 24 + [11.0] * 12))
    assert stops.height == 1
    assert stops["n_fixes"][0] == 24


def test_gps_drift_while_moored_does_not_split_the_stay():
    # moored ship drifting 0.7 kn now and then: under MOORED_DRIFT_KNOTS, still stopped
    assert find_stops(positions([12.0] * 6 + [0.1, 0.7, 0.2, 0.7] * 6 + [12.0] * 6)).height == 1


def test_short_bursts_merge_into_one_call():
    # stopped 1 h, 15 min of 3 kn (berth shift), stopped 1 h -> two stops, merged into one call
    stops = find_stops(positions([0.1] * 12 + [3.0] * 3 + [0.1] * 12, nav=0)).with_columns(
        locode=pl.lit("DEHAM"), port_name=pl.lit("Hamburg"), country=pl.lit("DE"), port_km=pl.lit(1.0))
    assert stops.height == 2
    calls = merge_stops(stops)
    assert calls.height == 1 and calls["n_stops_merged"][0] == 2


def test_gap_of_31_minutes_breaks_a_stop():
    # 1 h stopped, 31 min of NO data, 1 h stopped -> two stops (dt.total_minutes() would truncate to 30)
    p = positions([0.1] * 24)
    p = p.with_columns(pl.when(pl.int_range(pl.len()) >= 12).then(pl.col("window_start") + timedelta(minutes=26))
                       .otherwise(pl.col("window_start")).alias("window_start"))
    assert find_stops(p).height == 2
