"""Gold layer: turbine power aggregation and fleet anomaly detection.

Reads from Silver clean, aggregates per turbine per time window, computes fleet
benchmarks and z-score anomaly flags, then MERGEs into the Gold fact table.
"""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import abs, avg, col, date_trunc, lit, max, mean, min, round, stddev

from src.utils.common import create_spark_session, get_logger
from src.utils.constants import DELTA_BASE

logger = get_logger(__name__)

# Fixed calendar granularities supported by date_trunc.
# For custom rolling windows, a window function approach would be required instead.
_VALID_WINDOWS = ["hour", "day", "week", "month", "year"]


def _build_summary(df: DataFrame, window: str = "day") -> DataFrame:
    """Aggregate min, max, and avg power output per turbine and fleet over a configurable time window.

    The timestamp is truncated to the requested granularity so all readings
    within the same bucket are grouped together. Fleet benchmarks are computed
    as the average of each per-turbine metric across all turbines in the same period.

    Args:
        df: Cleaned Silver DataFrame.
        window: date_trunc granularity — one of "hour", "day", "week", "month", "year".
            Defaults to "day".

    Returns:
        DataFrame with columns: turbine_id, period, min_power, max_power, avg_power,
        fleet_avg_min_power, fleet_avg_max_power, fleet_avg_power, window_type.
    """
    if window not in _VALID_WINDOWS:
        raise ValueError(f"Invalid window '{window}'. Must be one of {_VALID_WINDOWS}.")

    fleet_window = Window.partitionBy("period")

    return (
        df
        .withColumn("period", date_trunc(window, col("timestamp")))
        .groupBy("turbine_id", "period")
        .agg(
            round(min("power_output"), 2).alias("min_power"),
            round(max("power_output"), 2).alias("max_power"),
            round(avg("power_output"), 2).alias("avg_power"),
        )
        # Fleet benchmarks: average of each per-turbine metric across all turbines in the period
        .withColumn("fleet_avg_min_power", round(mean("min_power").over(fleet_window), 2))
        .withColumn("fleet_avg_max_power", round(mean("max_power").over(fleet_window), 2))
        .withColumn("fleet_avg_power", round(mean("avg_power").over(fleet_window), 2))
        .withColumn("window_type", lit(window))
    )


def _enrich_with_anomaly_flags(summary_df: DataFrame) -> DataFrame:
    """Flag turbines deviating more than 2 standard deviations from the fleet or their own historical mean.

    Two independent anomaly signals are computed:
        - Fleet anomaly: turbine's avg_power vs the fleet mean for that period.
          Catches turbines that are significantly under- or over-performing
          relative to their peers at the same point in time.
        - Self anomaly: turbine's avg_power vs its own historical mean across all periods.
          Catches turbines that are behaving differently from their own baseline,
          even if the rest of the fleet is also affected.

    Args:
        summary_df: Output of _build_summary (includes fleet_avg_power column).

    Returns:
        summary_df with added columns: fleet_stddev, fleet_deviation, fleet_sigmas,
        is_fleet_anomaly, turbine_avg_power, turbine_stddev, turbine_deviation,
        turbine_sigmas, is_self_anomaly.
    """
    fleet_window = Window.partitionBy("period")
    turbine_window = Window.partitionBy("turbine_id")

    return (
        summary_df
        # Fleet anomaly — compare against peers in the same period
        .withColumn("fleet_stddev", round(stddev("avg_power").over(fleet_window), 2))
        .withColumn("fleet_deviation", round(col("avg_power") - col("fleet_avg_power"), 2))
        .withColumn("fleet_sigmas", round(col("fleet_deviation") / col("fleet_stddev"), 2))
        .withColumn("is_fleet_anomaly", abs(col("fleet_sigmas")) > 2)
        # Self anomaly — compare against the turbine's own historical mean across all periods
        .withColumn("turbine_avg_power", round(mean("avg_power").over(turbine_window), 2))
        .withColumn("turbine_stddev", round(stddev("avg_power").over(turbine_window), 2))
        .withColumn("turbine_deviation", round(col("avg_power") - col("turbine_avg_power"), 2))
        .withColumn("turbine_sigmas", round(col("turbine_deviation") / col("turbine_stddev"), 2))
        .withColumn("is_self_anomaly", abs(col("turbine_sigmas")) > 2)
    )


def aggregate_wind_turbines(
    spark: SparkSession,
    window: str = "day",
    output_path: str = DELTA_BASE,
) -> None:
    """Read the Silver clean table, aggregate per turbine per time window, and write to Gold.

    Writes to a single Delta fact table:
        - gold/fct_turbine_power_snapshot: min, max, avg power per turbine per period,
          fleet benchmarks, z-score anomaly flags, and window_type for granularity isolation.
          MERGEd on (turbine_id, period, window_type) so reruns are idempotent and
          multiple granularities coexist without collision via partitioning on window_type.

    Args:
        spark: Active SparkSession to use for reading and writing.
        window: Aggregation granularity passed to date_trunc — e.g. "hour", "day",
            "week", "month". Defaults to "day".
        output_path: Base directory for all Delta output tables. Defaults to DELTA_BASE.
    """
    silver_clean_table = f"{output_path}/02_silver/wind_turbine_clean"
    gold_table = f"{output_path}/03_gold/fct_turbine_power_snapshot"

    logger.info("Reading Silver table from %s", silver_clean_table)
    df = spark.read.format("delta").load(silver_clean_table)

    summary_df = _build_summary(df, window=window)
    enriched_df = _enrich_with_anomaly_flags(summary_df)

    # Canonical column order for the fact table
    enriched_df = enriched_df.select("turbine_id", "window_type", "period",
                                     "min_power", "max_power", "avg_power",
                                     "fleet_avg_min_power", "fleet_avg_max_power", "fleet_avg_power",
                                     "fleet_stddev", "fleet_deviation", "fleet_sigmas", "is_fleet_anomaly",
                                     "turbine_avg_power", "turbine_stddev", "turbine_deviation", "turbine_sigmas", "is_self_anomaly")

    if not DeltaTable.isDeltaTable(spark, gold_table):
        logger.info("Gold table does not exist — performing initial write")
        enriched_df.coalesce(1).write.format("delta").partitionBy("window_type").save(gold_table)
    else:
        # MERGE on (turbine_id, period, window_type) — the composite natural key.
        # window_type in the predicate prevents daily and weekly rows from colliding.
        delta_table = DeltaTable.forPath(spark, gold_table)
        (
            delta_table.alias("target")
            .merge(
                enriched_df.alias("source"),
                "target.turbine_id = source.turbine_id "
                "AND target.period = source.period "
                "AND target.window_type = source.window_type",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    logger.info("Gold table written to %s", gold_table)


if __name__ == "__main__":
    # Standalone entry point — creates its own SparkSession for local runs
    # When called via the orchestrator (src/pipelines/pl_wt_power.py), the shared session is passed in via aggregate_wind_turbines()    
    spark = create_spark_session("gold_wt_power")
    aggregate_wind_turbines(spark, "week")
