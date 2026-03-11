"""Unit tests for silver_wt_power DQ rules."""

from datetime import datetime

from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from src.core.silver.silver_wt_power import _apply_dq


_SCHEMA = StructType([
    StructField("turbine_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("wind_direction", DoubleType(), True),
    StructField("power_output", DoubleType(), True),
])

_T = datetime(2022, 3, 1)


def _make_df(spark, rows):
    return spark.createDataFrame(rows, _SCHEMA)


def _clean(spark, rows):
    clean_df, _ = _apply_dq(_make_df(spark, rows))
    return clean_df


def _quarantine(spark, rows):
    _, quarantine_df = _apply_dq(_make_df(spark, rows))
    return quarantine_df


# ---------------------------------------------------------------------------
# Hard failures
# ---------------------------------------------------------------------------

class TestHardFailures:
    """Rows missing mandatory identity columns must be quarantined."""

    def test_null_turbine_id_quarantined(self, spark):
        rows = [(None, _T, 10.0, 180.0, 2.0)]
        assert _quarantine(spark, rows).count() == 1
        assert _clean(spark, rows).count() == 0

    def test_null_timestamp_quarantined(self, spark):
        rows = [("T1", None, 10.0, 180.0, 2.0)]
        assert _quarantine(spark, rows).count() == 1
        assert _clean(spark, rows).count() == 0

    def test_quarantine_row_has_failure_reason(self, spark):
        rows = [(None, _T, 10.0, 180.0, 2.0)]
        result = _quarantine(spark, rows).collect()
        assert result[0]["dq_failure_reason"] == "turbine_id is null"


# ---------------------------------------------------------------------------
# Range violations
# ---------------------------------------------------------------------------

class TestRangeViolations:
    """Sensor readings outside valid physical ranges must be quarantined."""

    def test_negative_wind_speed_quarantined(self, spark):
        rows = [("T1", _T, -1.0, 180.0, 2.0)]
        assert _quarantine(spark, rows).count() == 1

    def test_wind_direction_below_range_quarantined(self, spark):
        rows = [("T1", _T, 10.0, -1.0, 2.0)]
        assert _quarantine(spark, rows).count() == 1

    def test_wind_direction_above_range_quarantined(self, spark):
        rows = [("T1", _T, 10.0, 360.0, 2.0)]
        assert _quarantine(spark, rows).count() == 1

    def test_wind_direction_boundary_values_clean(self, spark):
        rows = [
            ("T1", _T, 10.0, 0.0, 2.0),
            ("T2", _T, 10.0, 359.0, 2.0),
        ]
        assert _clean(spark, rows).count() == 2

    def test_negative_power_output_quarantined(self, spark):
        rows = [("T1", _T, 10.0, 180.0, -1.0)]
        assert _quarantine(spark, rows).count() == 1


# ---------------------------------------------------------------------------
# Cross-column consistency
# ---------------------------------------------------------------------------

class TestCrossColumnConsistency:
    """Rows where related columns contradict each other must be quarantined."""

    def test_power_output_positive_when_wind_speed_zero_quarantined(self, spark):
        rows = [("T1", _T, 0.0, 180.0, 1.0)]
        assert _quarantine(spark, rows).count() == 1

    def test_zero_wind_speed_with_zero_power_is_clean(self, spark):
        rows = [("T1", _T, 0.0, 180.0, 0.0)]
        assert _clean(spark, rows).count() == 1


# ---------------------------------------------------------------------------
# Clean rows
# ---------------------------------------------------------------------------

class TestCleanRows:
    """Valid rows must pass through to clean and be absent from quarantine."""

    def test_valid_row_passes_all_rules(self, spark):
        rows = [("T1", _T, 10.0, 180.0, 2.0)]
        assert _clean(spark, rows).count() == 1
        assert _quarantine(spark, rows).count() == 0

    def test_clean_rows_have_no_dq_failure_reason_column(self, spark):
        rows = [("T1", _T, 10.0, 180.0, 2.0)]
        clean_df, _ = _apply_dq(_make_df(spark, rows))
        assert "dq_failure_reason" not in clean_df.columns

    def test_mixed_rows_split_correctly(self, spark):
        rows = [
            ("T1", _T, 10.0, 180.0, 2.0),   # clean
            ("T2", _T, -1.0, 180.0, 2.0),   # quarantine — negative wind_speed
            (None, _T, 10.0, 180.0, 2.0),   # quarantine — null turbine_id
        ]
        assert _clean(spark, rows).count() == 1
        assert _quarantine(spark, rows).count() == 2