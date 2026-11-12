"""Rule tests on tiny hand-made DataFrames - seconds to run, no real data needed."""
import pytest
from pyspark.sql import functions as F

from skysea.silver.ais import eta_timestamp, imo_is_valid, mmsi_kind
from skysea.spark import get_spark


@pytest.fixture(scope="session")
def spark():
    return get_spark("skysea-tests")


def test_mmsi_kind(spark):
    df = spark.createDataFrame([("211234567",), ("111232506",), ("970123456",), ("992111111",), ("12345",)], ["mmsi"])
    got = [r.k for r in df.select(mmsi_kind(F.col("mmsi")).alias("k")).collect()]
    assert got == ["ship", "sar_aircraft", "emergency_beacon", "aid_to_navigation", "invalid"]


def test_imo_check_digit(spark):
    # 9074729: 9*7 + 0*6 + 7*5 + 4*4 + 7*3 + 2*2 = 139 -> 9 = last digit
    df = spark.createDataFrame([("9074729",), ("9074728",), ("123",)], ["imo"])
    assert [r.ok for r in df.select(imo_is_valid(F.col("imo")).alias("ok")).collect()] == [True, False, False]


def test_eta_year_rollover_and_not_available(spark):
    df = spark.createDataFrame(
        [("2026-12-30 10:00:00", 1, 2, 8, 0),     # reported end of Dec, ETA 2 Jan -> next year
         ("2026-10-01 10:00:00", 10, 3, 14, 30),  # normal
         ("2026-10-01 10:00:00", 0, 0, 24, 60),   # "not available"
         ("2026-10-01 10:00:00", 2, 31, 12, 0)],  # 31 Feb -> NULL, not a crash
        ["received_at", "eta_month", "eta_day", "eta_hour", "eta_minute"],
    ).withColumn("received_at", F.to_timestamp("received_at"))
    # Format inside Spark (UTC session): collect() would convert timestamps to the laptop's local time.
    got = [r.eta for r in df.select(F.date_format(eta_timestamp(df), "yyyy-MM-dd HH:mm").alias("eta")).collect()]
    assert got[0] == "2027-01-02 08:00"
    assert got[1] == "2026-10-03 14:30"
    assert got[2] is None and got[3] is None
