"""One way to get a SparkSession, locally or on Databricks."""
from pyspark.sql import SparkSession


def get_spark(app_name: str = "skysea") -> SparkSession:
    # On Databricks a session already exists (with Delta built in) - reuse it, never build one.
    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        # 8 parallel tasks, not 16: all tasks share one 4 GB heap, and with 16 a big count(DISTINCT)
        # over 19M rows ran out of memory (OutOfMemoryError, 2026-10-22). Fewer tasks = more memory each.
        .master("local[8]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Default is 200 partitions after every shuffle. 32 = 2 per core suits ~20M rows on one laptop.
        .config("spark.sql.shuffle.partitions", "32")
        # WSL has ~7 GB; the driver IS the executor in local mode, so this is all Spark gets.
        .config("spark.driver.memory", "4g")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
    )
    # Adds the Delta Lake jars (downloaded once into ~/.ivy2) to the session.
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
