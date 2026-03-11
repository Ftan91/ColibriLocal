"""Bronze layer: incremental ingestion of raw wind turbine CSV files into Delta Lake.

Reads only files modified since the last watermark, attaches audit metadata,
and MERGEs into the Bronze Delta table on an MD5 surrogate key.
"""

from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, element_at, input_file_name, split

from src.utils.common import add_md5_hash, create_spark_session, get_logger, get_modified_files, read_watermark, write_watermark
from src.utils.constants import BRONZE_WATERMARK_KEY, DELTA_BASE, RAW_PATH

logger = get_logger(__name__)



def ingest_raw_wind_turbines(
    spark: SparkSession,
    raw_path: str = RAW_PATH,
    output_path: str = DELTA_BASE,
) -> None:
    """Incrementally ingest raw wind turbine CSV files into the Bronze Delta table.

    On each run:
        1. Read the last processed watermark (file modification time) from the
           control table. Defaults to epoch on first run so all files are picked up.
        2. Identify CSV files whose modification time is newer than the watermark.
           This captures both new rows appended (daily readings) to existing files 
           and brand-new files (onboarding of new turbines).
        3. Read those files and attach audit metadata columns (ingested_at, source_file)
           to introduce lineage.
        4. Add an MD5 surrogate key on (timestamp, turbine_id) used as the MERGE key.
        5. MERGE or UPSERT into the Bronze Delta table on the surrogate key:
             - New key → INSERT the row.
             - Existing key with a newer ingested_at → UPDATE (handles corrected data).
               This ensures that any previously modified readings or older entries are 
               captured accordingly.
        6. Advance the watermark in the control table to the current run time so the
           next run only processes files modified after this point.

    Args:
        spark: Active SparkSession to use for reading and writing.
        raw_path: Glob pattern for the raw CSV files. Defaults to RAW_PATH from constants.
        output_path: Base directory for all Delta output tables. Defaults to DELTA_BASE from constants.
    """
    watermark_path = f"{output_path}/00_control/watermarks"
    bronze_table = f"{output_path}/01_bronze/wind_turbines"

    watermark = read_watermark(spark, watermark_path, BRONZE_WATERMARK_KEY)
    logger.info("Watermark read: %s — scanning for files modified after this time", watermark)

    modified_files = get_modified_files(raw_path, since=watermark)
    if not modified_files:
        logger.info("No new or modified files found since watermark — nothing to ingest")
        return

    logger.info("Found %d modified file(s): %s", len(modified_files), modified_files)

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(modified_files)
    )
    logger.info("Read %d rows from modified CSV files", df.count())

    # Audit columns: ingested_at captures pipeline run time, source_file records
    # which CSV each row came from
    df = (
        df
        .withColumn("ingested_at", current_timestamp())
        .withColumn("source_file", element_at(split(input_file_name(), "/"), -1))
    )

    # MD5 surrogate key on (timestamp, turbine_id) — used as the MERGE predicate
    # so late-arriving rows with duplicate (turbine_id, timestamp) update in place
    df = add_md5_hash(df, ["timestamp", "turbine_id"], "pk")

    if not DeltaTable.isDeltaTable(spark, bronze_table):
        # First run — no existing table, write directly
        logger.info("Bronze table does not exist — performing initial full load")
        df.coalesce(1).write.format("delta").mode("overwrite").save(bronze_table)
    else:
        # Subsequent runs — MERGE incremental rows into the existing table:
        #   - Matching pk with a newer ingested_at → update (corrected / re-delivered row)
        #   - No matching pk → insert as a new reading
        delta_table = DeltaTable.forPath(spark, bronze_table)
        (
            delta_table.alias("target")
            .merge(df.alias("source"), "target.pk = source.pk")
            .whenMatchedUpdateAll(condition="source.ingested_at > target.ingested_at")
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info("MERGE complete into %s", bronze_table)

    # Advance the watermark so the next run skips files already processed
    run_time = datetime.now(tz=timezone.utc)
    write_watermark(spark, watermark_path, BRONZE_WATERMARK_KEY, run_time)
    logger.info("Watermark updated to %s", run_time)


if __name__ == "__main__":
    # Standalone entry point — creates its own SparkSession for local runs
    # When called via the orchestrator (src/pipelines/pl_wt_power.py), the shared session is passed in via ingest_raw_wind_turbines()
    spark = create_spark_session()
    ingest_raw_wind_turbines(spark)