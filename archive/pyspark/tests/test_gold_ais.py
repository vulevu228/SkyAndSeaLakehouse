"""Gaps-and-islands on one hand-made ship: does a noisy stay become ONE port call?"""
from datetime import datetime, timedelta

import pytest
from pyspark.sql import functions as F

from skysea.gold.ais import find_stops, merge_stops
from skysea.spark import get_spark


@pytest.fixture(scope="session")
def spark():
    return get_spark("skysea-tests")


def positions(spark, speeds, nav=5):
    t0 = datetime(2026, 9, 28, 0, 0)
    rows = [("211000001", "ship", False, t0 + timedelta(minutes=5 * i), s, nav, "moored", 53.54, 9.97)
            for i, s in enumerate(speeds)]
    return spark.createDataFrame(rows, ["mmsi", "mmsi_kind", "is_position_jump", "window_start", "sog_knots",
                                        "nav_status", "nav_status_label", "latitude", "longitude"])


def test_moving_stopped_moving_gives_one_stop(spark):
    # 1 h sailing, 2 h stopped, 1 h sailing (5-minute windows)
    df = positions(spark, [12.0] * 12 + [0.1] * 24 + [11.0] * 12)
    stops = find_stops(df).collect()
    assert len(stops) == 1
    assert stops[0].n_fixes == 24


def test_gps_drift_while_moored_does_not_split_the_stay(spark):
    # moored ship drifting 0.7 kn now and then: under MOORED_DRIFT_KNOTS, still stopped
    df = positions(spark, [12.0] * 6 + [0.1, 0.7, 0.2, 0.7] * 6 + [12.0] * 6)
    assert len(find_stops(df).collect()) == 1


def test_short_bursts_merge_into_one_call(spark):
    # stopped 1 h, 15 min of 3 kn (berth shift), stopped 1 h -> two stops, merged into one call
    df = positions(spark, [0.1] * 12 + [3.0] * 3 + [0.1] * 12, nav=0)
    stops = find_stops(df).withColumn("locode", F.lit("DEHAM")).withColumn("port_name", F.lit("Hamburg")) \
        .withColumn("country", F.lit("DE")).withColumn("port_km", F.lit(1.0))
    assert stops.count() == 2
    calls = merge_stops(stops).collect()
    assert len(calls) == 1 and calls[0].n_stops_merged == 2
