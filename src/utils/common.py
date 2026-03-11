"""Common utilities shared across the ColibriLocal pipeline.

Provides SparkSession creation, logging, DataFrame transformations (deduplication,
null imputation, MD5 hashing), Delta Lake watermark read/write helpers, and
file system utilities for incremental ingestion.
"""

import glob
import logging
import os
from datetime import datetime, timezone

from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import col, concat_ws, first, last, md5, row_number
from pyspark.sql.types import StringType, StructField, StructType, TimestampType


def create_spark_session(app_name: str = "colibri_pipeline") -> SparkSession:
    """Create and return a Delta-enabled SparkSession for local runs.

    Applies the standard configuration shared across all pipeline layers:
    - Delta Lake SQL extensions and catalog
    - UTC session timezone
    - Raw local filesystem (disables Hadoop CRC files)

    Args:
        app_name: Value for the Spark application name. Defaults to "colibri_pipeline".

    Returns:
        Configured SparkSession.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")  # Use all available CPU cores for local execution
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def get_logger(name: str) -> logging.Logger:
    """Return a named logger with a consistent format.

    If the logger has already been configured (e.g. by a previous call with
    the same name), the existing instance is returned without adding duplicate
    handlers.

    Args:
        name: Logger name, typically __name__ of the calling module.

    Returns:
        Configured Logger instance.
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
        )
        logger.addHandler(handler)

    return logger


def add_md5_hash(df: DataFrame, columns: list[str], alias: str = "row_hash") -> DataFrame:
    """Add an MD5 hash column derived from the concatenation of the specified columns.

    Null values are cast to the string 'null' before hashing so they contribute
    a deterministic value rather than propagating null to the entire hash.

    Args:
        df: Input DataFrame.
        columns: Column names whose values are concatenated before hashing.
        alias: Name of the resulting hash column. Defaults to 'row_hash'.

    Returns:
        DataFrame with the new hash column appended.
    """
    return df.withColumn(alias, md5(concat_ws("|", *[col(c).cast("string") for c in columns])))


def deduplicate(
    df: DataFrame,
    deduplicate_columns: list[str],
    order_by_column: str,
    ascending: bool = False,
) -> DataFrame:
    """Remove duplicates by retaining one row per partition based on an ordering column.

    Args:
        df: Input DataFrame.
        deduplicate_columns: Columns that define a duplicate group.
        order_by_column: Column used to rank rows within each group.
        ascending: If True, keep the row with the smallest value; if False (default),
            keep the row with the largest value. Nulls are always ranked last.

    Returns:
        DataFrame with duplicates removed.
    """
    order_expr = (
        col(order_by_column).asc_nulls_last()
        if ascending
        else col(order_by_column).desc_nulls_last()
    )
    window = (
        Window.partitionBy(deduplicate_columns)
        .orderBy(order_expr)
    )

    return (
        df
        .withColumn("_rn", row_number().over(window))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )


def impute_nulls(
    df: DataFrame,
    partition_columns: list[str],
    order_by_column: str,
    measurement_columns: list[str],
    strategy: str = "both",
) -> DataFrame:
    """Fill null measurement values per partition ordered by a given column.

    Fill strategies:
        - "forward": inherit the last known non-null value before each null.
          Covers gaps in the middle and tail of a series.
        - "backward": inherit the next known non-null value after each null.
          Covers nulls at the start of a series with no prior value.
        - "both" (default): forward fill first, then backward fill any remaining
          nulls. A value stays null only if the entire partition is null.

    Args:
        df: Input DataFrame.
        partition_column: Column to partition by (e.g. "turbine_id").
        order_by_column: Column to order within each partition (e.g. "timestamp").
        measurement_columns: Columns to impute.
        strategy: One of "forward", "backward", or "both". Defaults to "both".

    Returns:
        DataFrame with nulls imputed in measurement columns.
    """
    _VALID_IMPUTE_STRATEGIES = ["forward", "backward", "both"]
    if strategy not in _VALID_IMPUTE_STRATEGIES:
        raise ValueError(f"Invalid strategy '{strategy}'. Must be one of {_VALID_IMPUTE_STRATEGIES}.")

    forward_window = (
        Window.partitionBy(partition_columns)
        .orderBy(order_by_column)
        .rowsBetween(Window.unboundedPreceding, 0)
    )
    backward_window = (
        Window.partitionBy(partition_columns)
        .orderBy(order_by_column)
        .rowsBetween(0, Window.unboundedFollowing)
    )

    for column in measurement_columns:
        if strategy in ("forward", "both"):
            df = df.withColumn(column, last(col(column), ignorenulls=True).over(forward_window))
        if strategy in ("backward", "both"):
            df = df.withColumn(column, first(col(column), ignorenulls=True).over(backward_window))

    return df


def read_watermark(spark: SparkSession, control_path: str, table: str) -> datetime:
    """Read the last processed watermark for a specific table from the control Delta table.

    Returns the Unix epoch (1970-01-01 UTC) if the control table does not yet
    exist or has no entry for the given table, causing the first run to process
    all available files.

    Args:
        spark: Active SparkSession.
        control_path: Path to the shared Delta control table.
        table: Logical table name to look up (e.g. "bronze_wind_turbines").

    Returns:
        Watermark as a timezone-aware UTC datetime.
    """
    # Deferred import — avoids loading DeltaTable in contexts where only other
    # common utilities are used (e.g. during unit tests that do not touch Delta I/O).
    from delta.tables import DeltaTable

    if DeltaTable.isDeltaTable(spark, control_path):
        rows = (
            spark.read.format("delta").load(control_path)
            .filter(col("table") == table)
            .collect()
        )
        if rows:
            ts = rows[0]["watermark"]
            return ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts
    return datetime(1970, 1, 1, tzinfo=timezone.utc)


def get_modified_files(raw_path: str, since: datetime) -> list[str]:
    """Return file paths that have been modified after the given watermark.

    Args:
        raw_path: Glob pattern matching the source files.
        since: Only return files modified after this datetime (UTC).

    Returns:
        List of file paths modified since the watermark.
    """
    all_files = glob.glob(raw_path)
    return [
        f for f in all_files
        if datetime.fromtimestamp(os.path.getmtime(f), tz=timezone.utc) > since
    ]


def write_watermark(spark: SparkSession, control_path: str, table: str, watermark: datetime) -> None:
    """Persist the watermark to a Delta control table, overwriting any previous value.

    Args:
        spark: Active SparkSession.
        control_path: Path to the Delta control table storing the watermark.
        watermark: New watermark value to persist.
    """
    # NOTE: mode("overwrite") replaces the entire control table on each write.
    # Safe while only one watermark key (BRONZE_WATERMARK_KEY) exists — if additional
    # layer watermarks are introduced, replace with a MERGE on the "table" column.
    schema = StructType([
        StructField("table", StringType(), False),
        StructField("watermark", TimestampType(), False)])
    # Strip microseconds and tzinfo — PySpark TimestampType expects naive datetime at second precision
    watermark_clean = watermark.replace(microsecond=0, tzinfo=None)

    df = spark.createDataFrame([(table, watermark_clean)], schema)
    df.coalesce(1).write.format("delta").mode("overwrite").save(control_path)