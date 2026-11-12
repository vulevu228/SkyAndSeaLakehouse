"""Landing CSV.gz -> bronze Delta tables (ais_positions, ais_static). Raw, append-only, all strings.

Only finished days (*.csv.gz) are loaded; today's plain .csv is still being written by the
collector. Audit columns:
  source_file  landing file the row came from (lineage / replay)
  file_date    UTC day of that file (the partition column)
  ingested_at  when this pipeline loaded it

Reruns are safe: files already in bronze are skipped.
"""
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


def read_landing(spark: SparkSession, paths: list[str], schema: StructType) -> DataFrame:
    return (
        spark.read.schema(schema)
        .option("header", True)
        # Ship names can contain commas; the collector's csv writer quoted those fields.
        .option("quote", '"').option("escape", '"')
        .csv(paths)
        # _metadata is a hidden column every file-based source exposes (path, name, size, mtime).
        .withColumn("source_file", F.col("_metadata.file_name"))
        # positions_2026-09-28.csv.gz -> 2026-09-28
        .withColumn("file_date", F.to_date(F.regexp_extract("source_file", r"(\d{4}-\d{2}-\d{2})", 1)))
        .withColumn("ingested_at", F.current_timestamp())
    )


def already_ingested(spark: SparkSession, path: str) -> set[str]:
    if not DeltaTable.isDeltaTable(spark, path):
        return set()
    # Small result (one name per day), so collect() to the driver is fine.
    return {r.source_file for r in spark.read.format("delta").load(path).select("source_file").distinct().collect()}


def ingest(spark: SparkSession, landing_dir: Path, pattern: str, schema: StructType, path: str) -> dict:
    files = sorted(landing_dir.glob(pattern))
    done = already_ingested(spark, path)
    new = [str(f) for f in files if f.name not in done]
    if not new:
        return {"files_new": 0, "files_skipped": len(files), "rows_written": 0}
    (
        read_landing(spark, new, schema)
        .write.format("delta").mode("append")
        .partitionBy("file_date")
        .save(path)
    )
    # Every Delta write records metrics in its transaction log - cheaper than re-counting.
    metrics = DeltaTable.forPath(spark, path).history(1).first()["operationMetrics"]
    return {"files_new": len(new), "files_skipped": len(done), "rows_written": int(metrics["numOutputRows"])}
