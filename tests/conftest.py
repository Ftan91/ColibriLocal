"""Shared pytest fixtures for all test modules."""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Create a local SparkSession shared across all tests in this session."""
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder
        .appName("test_colibrilocal")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
