"""Bronze schemas: every column a STRING, exactly as the collectors wrote it.

Giving Spark the schema up front (instead of inferSchema) means it does not read the files
twice to guess types, and a bad value can never break the load - typing happens in silver,
where every failed cast is counted.
"""
from pyspark.sql.types import StringType, StructField, StructType


def _strings(*names: str) -> StructType:
    return StructType([StructField(n, StringType(), True) for n in names])


AIS_POSITION_SCHEMA = _strings(
    "window_start", "received_at", "mmsi", "ship_name", "ais_class", "latitude", "longitude",
    "sog_knots", "cog_deg", "true_heading", "nav_status", "reports_in_window",
)

AIS_STATIC_SCHEMA = _strings(
    "window_start", "received_at", "mmsi", "imo", "ship_name", "call_sign", "ship_type",
    "destination", "eta_month", "eta_day", "eta_hour", "eta_minute", "draught_m",
    "length_m", "width_m",
)
