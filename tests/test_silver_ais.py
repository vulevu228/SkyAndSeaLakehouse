"""Rule tests on tiny hand-made DataFrames - milliseconds to run, no real data needed."""
from datetime import datetime, timezone

import polars as pl

from skysea.silver.ais import eta_timestamp, flag_jumps, imo_is_valid, mmsi_kind


def test_mmsi_kind():
    df = pl.DataFrame({"mmsi": ["211234567", "111232506", "970123456", "992111111", "12345", None]})
    got = df.select(mmsi_kind(pl.col("mmsi")).alias("k"))["k"].to_list()
    assert got == ["ship", "sar_aircraft", "emergency_beacon", "aid_to_navigation", "invalid", "invalid"]


def test_imo_check_digit():
    # 9074729: 9*7 + 0*6 + 7*5 + 4*4 + 7*3 + 2*2 = 139 -> 9 = last digit
    df = pl.DataFrame({"imo": ["9074729", "9074728", "123", None]})
    assert df.select(imo_is_valid(pl.col("imo")))["imo"].to_list() == [True, False, False, False]


def test_eta_year_rollover_and_not_available():
    utc = lambda *a: datetime(*a, tzinfo=timezone.utc)  # noqa: E731
    df = pl.DataFrame({
        "received_at": [utc(2026, 12, 30, 10), utc(2026, 10, 1, 10), utc(2026, 10, 1, 10), utc(2026, 10, 1, 10)],
        "eta_month": [1, 10, 0, 2], "eta_day": [2, 3, 0, 31], "eta_hour": [8, 14, 24, 12], "eta_minute": [0, 30, 60, 0],
    })  # rows: end of Dec -> next year / normal / "not available" / 31 Feb
    got = df.select(eta_timestamp().dt.strftime("%Y-%m-%d %H:%M").alias("eta"))["eta"].to_list()
    assert got == ["2027-01-02 08:00", "2026-10-03 14:30", None, None]


def test_jump_check_sees_previous_day():
    # Ship at Hamburg on day 1, "in New York" 5 minutes into day 2: only visible through the carry-over.
    t = lambda d, m: datetime(2026, 9, d, 23 if d == 28 else 0, m, tzinfo=timezone.utc)  # noqa: E731
    row = lambda d, m, lat, lon: {"mmsi": "211000001", "mmsi_kind": "ship", "window_start": t(d, m),  # noqa: E731
                                  "latitude": lat, "longitude": lon, "sog_knots": 0.1}
    day1 = pl.DataFrame([row(28, 55, 53.54, 9.97)])
    day2 = pl.DataFrame([row(29, 0, 40.70, -74.0), row(29, 5, 40.70, -74.0)])
    _, carry = flag_jumps(day1, None)
    out, _ = flag_jumps(day2, carry)
    assert out["is_position_jump"].to_list() == [True, False]
