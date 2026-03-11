"""Unit tests for shared utility functions in src/utils/common.py."""

from datetime import datetime

import pytest
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from src.utils.common import deduplicate, impute_nulls


_SCHEMA = StructType([
    StructField("turbine_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("wind_direction", DoubleType(), True),
    StructField("power_output", DoubleType(), True),
])

_DEDUP_SCHEMA = StructType([
    *_SCHEMA.fields,
    StructField("ingested_at", TimestampType(), True),
])


# ---------------------------------------------------------------------------
# deduplicate
# ---------------------------------------------------------------------------

class TestDeduplicate:
    """Tests for deduplication on composite key ordered by ingested_at."""

    def test_duplicate_rows_reduced_to_one(self, spark):
        t = datetime(2022, 3, 1)
        rows = [
            ("T1", t, 10.0, 180.0, 2.0, datetime(2022, 3, 1, 0, 0, 1)),
            ("T1", t, 10.0, 180.0, 2.0, datetime(2022, 3, 1, 0, 0, 2)),
        ]
        df = spark.createDataFrame(rows, _DEDUP_SCHEMA)
        assert deduplicate(df, ["turbine_id", "timestamp"], "ingested_at").count() == 1

    def test_keeps_latest_ingested_row(self, spark):
        t = datetime(2022, 3, 1)
        rows = [
            ("T1", t, 10.0, 180.0, 2.0, datetime(2022, 3, 1, 0, 0, 1)),  # older
            ("T1", t, 10.0, 180.0, 3.0, datetime(2022, 3, 1, 0, 0, 2)),  # newer — should win
        ]
        df = spark.createDataFrame(rows, _DEDUP_SCHEMA)
        result = deduplicate(df, ["turbine_id", "timestamp"], "ingested_at")
        assert result.collect()[0]["power_output"] == 3.0

    def test_unique_rows_all_retained(self, spark):
        rows = [
            ("T1", datetime(2022, 3, 1, 0), 10.0, 180.0, 2.0, datetime(2022, 3, 1, 1)),
            ("T1", datetime(2022, 3, 1, 1), 10.0, 180.0, 2.1, datetime(2022, 3, 1, 1)),
            ("T2", datetime(2022, 3, 1, 0), 11.0, 190.0, 2.5, datetime(2022, 3, 1, 1)),
        ]
        df = spark.createDataFrame(rows, _DEDUP_SCHEMA)
        assert deduplicate(df, ["turbine_id", "timestamp"], "ingested_at").count() == 3

    def test_same_timestamp_different_turbines_kept(self, spark):
        t = datetime(2022, 3, 1)
        rows = [
            ("T1", t, 10.0, 180.0, 2.0, datetime(2022, 3, 1, 1)),
            ("T2", t, 11.0, 190.0, 2.5, datetime(2022, 3, 1, 1)),
        ]
        df = spark.createDataFrame(rows, _DEDUP_SCHEMA)
        assert deduplicate(df, ["turbine_id", "timestamp"], "ingested_at").count() == 2


# ---------------------------------------------------------------------------
# impute_nulls
# ---------------------------------------------------------------------------

class TestImputeNulls:
    """Tests for forward/backward fill null imputation per turbine."""

    def test_forward_fill_fills_trailing_nulls(self, spark):
        rows = [
            ("T1", datetime(2022, 3, 1, 0), 10.0, 180.0, 2.0),
            ("T1", datetime(2022, 3, 1, 1), None, None, None),
            ("T1", datetime(2022, 3, 1, 2), None, None, None),
        ]
        df = spark.createDataFrame(rows, _SCHEMA)
        result = impute_nulls(df, ["turbine_id"], "timestamp", {"wind_speed": "forward", "power_output": "forward"})
        rows_out = result.orderBy("timestamp").collect()
        assert rows_out[1]["wind_speed"] == 10.0
        assert rows_out[2]["power_output"] == 2.0

    def test_backward_fill_fills_leading_nulls(self, spark):
        rows = [
            ("T1", datetime(2022, 3, 1, 0), None, None, None),
            ("T1", datetime(2022, 3, 1, 1), None, None, None),
            ("T1", datetime(2022, 3, 1, 2), 10.0, 180.0, 2.0),
        ]
        df = spark.createDataFrame(rows, _SCHEMA)
        result = impute_nulls(df, ["turbine_id"], "timestamp", {"wind_speed": "backward", "power_output": "backward"})
        rows_out = result.orderBy("timestamp").collect()
        assert rows_out[0]["wind_speed"] == 10.0
        assert rows_out[1]["power_output"] == 2.0

    def test_both_fills_leading_and_trailing_nulls(self, spark):
        rows = [
            ("T1", datetime(2022, 3, 1, 0), None, None, None),   # leading null — backward fills
            ("T1", datetime(2022, 3, 1, 1), 10.0, 180.0, 2.0),
            ("T1", datetime(2022, 3, 1, 2), None, None, None),   # trailing null — forward fills
        ]
        df = spark.createDataFrame(rows, _SCHEMA)
        result = impute_nulls(df, ["turbine_id"], "timestamp", {"wind_speed": "both", "power_output": "both"})
        rows_out = result.orderBy("timestamp").collect()
        assert rows_out[0]["wind_speed"] == 10.0  # backward filled
        assert rows_out[2]["power_output"] == 2.0  # forward filled

    def test_fill_does_not_bleed_across_turbines(self, spark):
        rows = [
            ("T1", datetime(2022, 3, 1, 0), 10.0, 180.0, 2.0),
            ("T2", datetime(2022, 3, 1, 0), None, None, None),
        ]
        df = spark.createDataFrame(rows, _SCHEMA)
        result = impute_nulls(df, ["turbine_id"], "timestamp", {"wind_speed": "both", "power_output": "both"})
        t2 = result.filter("turbine_id = 'T2'").collect()[0]
        assert t2["wind_speed"] is None
        assert t2["power_output"] is None

    def test_all_null_partition_remains_null(self, spark):
        rows = [
            ("T1", datetime(2022, 3, 1, 0), None, None, None),
            ("T1", datetime(2022, 3, 1, 1), None, None, None),
        ]
        df = spark.createDataFrame(rows, _SCHEMA)
        result = impute_nulls(df, ["turbine_id"], "timestamp", {"wind_speed": "both", "power_output": "both"})
        for row in result.collect():
            assert row["wind_speed"] is None
            assert row["power_output"] is None

    def test_no_nulls_unchanged(self, spark):
        rows = [
            ("T1", datetime(2022, 3, 1, 0), 10.0, 180.0, 2.0),
            ("T1", datetime(2022, 3, 1, 1), 11.0, 185.0, 2.5),
        ]
        df = spark.createDataFrame(rows, _SCHEMA)
        result = impute_nulls(df, ["turbine_id"], "timestamp", {"wind_speed": "both", "power_output": "both"})
        rows_out = result.orderBy("timestamp").collect()
        assert rows_out[0]["wind_speed"] == 10.0
        assert rows_out[1]["wind_speed"] == 11.0

    def test_invalid_strategy_raises(self, spark):
        rows = [("T1", datetime(2022, 3, 1), 10.0, 180.0, 2.0)]
        df = spark.createDataFrame(rows, _SCHEMA)
        with pytest.raises(ValueError, match="Invalid strategy"):
            impute_nulls(df, ["turbine_id"], "timestamp", {"wind_speed": "sideways"})