"""Pipeline orchestration for the wind turbine medallion pipeline.

Wires Bronze → Silver → Gold into a single run function with a shared SparkSession.
Exposes a main() CLI entry point registered as the 'colibri-pipeline' console script.
"""

import argparse

from src.core.bronze.bronze_wt_power import ingest_raw_wind_turbines
from src.core.silver.silver_wt_power import transform_wind_turbines
from src.core.gold.gold_wt_power import aggregate_wind_turbines
from src.utils.common import create_spark_session, get_logger
from src.utils.constants import DELTA_BASE, RAW_PATH

logger = get_logger(__name__)


def run(
    windows: list[str] = None,
    raw_path: str = RAW_PATH,
    output_path: str = DELTA_BASE,
) -> None:
    """Run the full wind turbine medallion pipeline: Bronze → Silver → Gold.

    A single SparkSession is created and shared across all layers to avoid
    the overhead of initialising multiple sessions in the same process.

    Args:
        windows: One or more Gold aggregation granularities — any of
            "hour", "day", "week", "month", "year". Defaults to ["day"].
        raw_path: Glob pattern for the raw CSV source files. Defaults to RAW_PATH.
        output_path: Base directory for all Delta output tables. Defaults to DELTA_BASE.
    """
    if windows is None:
        windows = ["day"]

    spark = create_spark_session()
    try:
        logger.info("Pipeline started (windows=%s, raw_path=%s, output_path=%s)", windows, raw_path, output_path)
        ingest_raw_wind_turbines(spark, raw_path=raw_path, output_path=output_path)
        transform_wind_turbines(spark, output_path=output_path)
        for window in windows:
            logger.info("Aggregating Gold layer for window=%s", window)
            aggregate_wind_turbines(spark, window=window, output_path=output_path)
        logger.info("Pipeline complete")
    finally:
        spark.stop()


def main() -> None:
    """CLI entry point registered as the 'colibri-pipeline' console script."""
    parser = argparse.ArgumentParser(
        description="Wind turbine medallion pipeline (Bronze → Silver → Gold).",
    )
    parser.add_argument(
        "--raw-path",
        default=RAW_PATH,
        dest="raw_path",
        metavar="PATH",
        help=f"Glob pattern for raw CSV source files (default: {RAW_PATH})",
    )
    parser.add_argument(
        "--output-path",
        default=DELTA_BASE,
        dest="output_path",
        metavar="PATH",
        help=f"Base directory for all Delta output tables (default: {DELTA_BASE})",
    )
    parser.add_argument(
        "--window",
        nargs="+",
        default=["day"],
        dest="windows",
        choices=["hour", "day", "week", "month", "year"],
        metavar="WINDOW",
        help="One or more Gold aggregation windows: hour, day, week, month, year (default: day)",
    )
    args = parser.parse_args()
    run(windows=args.windows, raw_path=args.raw_path, output_path=args.output_path)


if __name__ == "__main__":
    main()