from datetime import datetime, timezone

import polars as pl

from skysea.load.postgres import for_copy, pg_type, q


def test_types_map_unsigned_to_bigger_signed_and_keep_time_zones():
    assert pg_type(pl.UInt32) == "bigint"          # UInt32 max 4.29e9 does not fit integer
    assert pg_type(pl.UInt16) == "integer"
    assert pg_type(pl.Datetime("us", "UTC")) == "timestamptz"
    assert pg_type(pl.Datetime("us")) == "timestamp"


def test_keyword_columns_are_quoted():
    assert q("end") == '"end"'


def test_copy_text_is_naive_utc():
    berlin = pl.DataFrame({"t": [datetime(2026, 10, 23, 12, tzinfo=timezone.utc)]}).with_columns(
        pl.col("t").dt.convert_time_zone("Europe/Berlin"))
    out = for_copy(berlin)
    assert out.schema["t"] == pl.Datetime("us")
    assert out["t"][0] == datetime(2026, 10, 23, 12)  # 14:00 Berlin -> 12:00 UTC, no shift lost
