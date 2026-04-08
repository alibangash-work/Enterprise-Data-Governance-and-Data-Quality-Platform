"""
Test configuration and fixtures
"""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    """Create a Spark session for the entire test session"""
    spark = SparkSession.builder \
        .appName("pytest-spark") \
        .master("local[1]") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .getOrCreate()
    
    yield spark
    
    spark.stop()


@pytest.fixture
def spark(spark_session):
    """Provide Spark session for individual tests"""
    return spark_session


def pytest_configure(config):
    """Configure pytest"""
    config.addinivalue_line(
        "markers", "integration: mark test as an integration test"
    )
    config.addinivalue_line(
        "markers", "unit: mark test as a unit test"
    )
