"""OpenSky rules on hand-made state vectors."""
from datetime import datetime, timedelta, timezone

import polars as pl

from skysea.gold.opensky import find_islands
from skysea.silver.opensky import flag_jumps

T0 = datetime(2026, 9, 28, 12, 0, tzinfo=timezone.utc)


def states(alts, lats=None, ground=None, callsigns=None, step_min=5, stale_at=()):
    n = len(alts)
    t = [T0 + timedelta(minutes=step_min * i) for i in range(n)]
    return pl.DataFrame({
        "icao24": ["3c6444"] * n, "snapshot_time": t,
        # a stale row repeats the previous position time: the aircraft sent no new position
        "time_position": [t[i - 1] if i in stale_at else t[i] for i in range(n)],
        "latitude": lats or [53.6 + 0.2 * i for i in range(n)], "longitude": [10.0] * n,
        "baro_altitude": alts, "on_ground": ground or [False] * n, "velocity": [230.0] * n,
        "callsign": callsigns or ["DLH4AB"] * n, "origin_country": ["Germany"] * n,
        "is_stale_position": [i in stale_at for i in range(n)], "is_position_jump": [False] * n,
        "is_speed_suspect": [False] * n, "is_altitude_suspect": [False] * n, "is_test_transponder": [False] * n,
        "is_altitude_jump": [False] * n,
    })


def test_altitude_bit_error_is_a_jump_but_a_climb_is_not():
    # climb 2,000 -> 5,000 m in 5 min (10 m/s) is normal; 5,000 -> 18,000 m in 5 min is not
    out, _ = flag_jumps(states([2000.0, 5000.0, 18000.0, 5100.0]), None)
    assert out["is_altitude_jump"].to_list() == [False, False, True, True]


def test_stale_position_is_not_a_position_jump():
    # the stale row has an old time_position -> 0 s since the previous fix -> no implied speed at all
    out, _ = flag_jumps(states([10000.0] * 3, stale_at={2}), None)
    assert out["implied_ms"][2] is None and not out["is_position_jump"].any()


def test_teleport_is_a_position_jump():
    out, _ = flag_jumps(states([10000.0] * 3, lats=[53.6, 53.8, 60.0]), None)  # +690 km in 5 min
    assert out["is_position_jump"].to_list() == [False, False, True]


def test_ground_air_ground_and_new_callsign_split_islands():
    ground = [True, True, False, False, False, True, True, False, False]
    calls = ["DLH1"] * 7 + ["DLH2"] * 2   # same aircraft, next flight under a new callsign
    isl = find_islands(states([0.0] * 9, ground=ground, callsigns=calls))
    assert isl["on_ground"].to_list() == [True, False, True, False]


def test_gap_longer_than_30_minutes_splits_a_flight():
    s = states([10000.0] * 4, step_min=20)   # 20, 20, 20 min apart: one island
    assert find_islands(s).height == 1
    s = states([10000.0] * 4, step_min=40)   # 40 min apart: every fix its own island
    assert find_islands(s).height == 4
