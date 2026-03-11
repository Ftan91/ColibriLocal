"""Silver layer: data quality checks and null imputation for wind turbine readings.

Reads from Bronze, deduplicates, imputes nulls, then splits rows into a clean
table and a quarantine table based on a set of DQ rules.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, when

from src.utils.common import create_spark_session, deduplicate, get_logger, impute_nulls
from src.utils.constants import DELTA_BASE

logger = get_logger(__name__)


def _apply_dq(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Apply data quality rules and split into clean and quarantine DataFrames.

    DQ rules applied:
        - Hard failures (row cannot be used at all):
            1. turbine_id is null
            2. timestamp is null
        - Range violations:
            3. wind_speed is negative
            4. wind_direction is outside 0-359
            5. power_output is negative
        - Cross-column consistency:
            6. wind_speed is 0 but power_output is greater than 0

    Rows that fail any rule are routed to the quarantine table with a
    dq_failure_reason column describing the first violation found.

    Args:
        df: DataFrame after null imputation and deduplication.

    Returns:
        Tuple of (clean_df, quarantine_df).
    """
    df = df.withColumn("dq_failure_reason",
                       when(col("turbine_id").isNull(), lit("turbine_id is null"))
                       .when(col("timestamp").isNull(), lit("timestamp is null"))
                       .when(col("wind_speed") < 0, lit("wind_speed is negative"))
                       .when(
                           (col("wind_direction") < 0) | (col("wind_direction") > 359),
                           lit("wind_direction out of range 0-359"),
                           )
                        .when(col("power_output") < 0, lit("power_output is negative"))
                        .when((col("wind_speed") == 0) & (col("power_output") > 0), lit("power_output > 0 when wind_speed is 0"))
                        .otherwise(None)
                        )

    clean_df = df.filter(col("dq_failure_reason").isNull()).drop("dq_failure_reason")
    quarantine_df = df.filter(col("dq_failure_reason").isNotNull())

    return clean_df, quarantine_df


def transform_wind_turbines(spark: SparkSession, output_path: str = DELTA_BASE) -> None:
    """Read the Bronze Delta table, clean and transform data, write to the Silver Delta table.

    Processing steps applied in order:
        1. Deduplicate on (turbine_id, timestamp), keeping the latest ingested row
        2. Forward then backward fill nulls in measurement columns per turbine
        3. Apply data quality rules — split rows into clean and quarantine

    Args:
        spark: Active SparkSession to use for reading and writing.
        output_path: Base directory for all Delta output tables. Defaults to DELTA_BASE.
    """
    bronze_table = f"{output_path}/01_bronze/wind_turbines"
    silver_clean_table = f"{output_path}/02_silver/wind_turbine_clean"
    silver_quarantine_table = f"{output_path}/02_silver/wind_turbine_quarantine"

    logger.info("Reading Bronze table from %s", bronze_table)
    df = spark.read.format("delta").load(bronze_table)

    # Deduplicate — keep the most recently ingested row per (turbine_id, timestamp)
    # Covering edge cases where there may be more than 1 reading for a single turbine at a time
    df = deduplicate(df, ["turbine_id", "timestamp"], "ingested_at", ascending=False)
    logger.info("Deduplication complete")

    # Forward fill nulls before DQ evaluation so that rows recoverable by
    # imputation are not unnecessarily quarantined
    df = impute_nulls(df, "turbine_id", "timestamp", ["wind_speed", "wind_direction", "power_output"])
    logger.info("Null imputation complete")

    # Split into clean and quarantine based on DQ rules
    clean_df, quarantine_df = _apply_dq(df)
    logger.info("DQ check complete")

    clean_df.coalesce(1).write.format("delta").mode("overwrite").save(silver_clean_table)
    logger.info("Silver clean table written to %s", silver_clean_table)

    quarantine_df.coalesce(1).write.format("delta").mode("overwrite").save(silver_quarantine_table)
    logger.info("Silver quarantine table written to %s", silver_quarantine_table)


if __name__ == "__main__":
    # Standalone entry point — creates its own SparkSession for local runs
    # When called via the orchestrator (src/pipelines/pl_wt_power.py), the shared session is passed in via transform_wind_turbines()
    spark = create_spark_session()
    transform_wind_turbines(spark)