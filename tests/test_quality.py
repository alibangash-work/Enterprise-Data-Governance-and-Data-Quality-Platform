"""
Unit tests for data quality framework
"""

import pytest
from pyspark.sql import SparkSession
from data_quality.validators import DataQualityValidator, DataQualityProfiler


@pytest.fixture
def spark():
    """Create a Spark session for testing"""
    return SparkSession.builder \
        .appName("test-quality") \
        .master("local[1]") \
        .getOrCreate()


@pytest.fixture
def sample_dataframe(spark):
    """Create sample DataFrame for testing"""
    data = [
        {"id": 1, "name": "Alice", "amount": 100.0},
        {"id": 2, "name": "Bob", "amount": 250.0},
        {"id": 3, "name": "Charlie", "amount": 75.5},
    ]
    return spark.createDataFrame(data)


class TestDataQualityProfiler:
    """Tests for data quality profiler"""
    
    def test_profile_generation(self, sample_dataframe):
        """Test data profiling"""
        profiler = DataQualityProfiler(sample_dataframe, "test_dataset")
        profile = profiler.profile_data()
        
        assert profile["dataset_name"] == "test_dataset"
        assert profile["total_rows"] == 3
        assert profile["total_columns"] == 3
        assert "columns" in profile
    
    def test_column_profiling(self, sample_dataframe):
        """Test individual column profiling"""
        profiler = DataQualityProfiler(sample_dataframe, "test_dataset")
        profile = profiler.profile_data()
        
        # Check id column
        id_profile = profile["columns"].get("id")
        assert id_profile is not None
        assert id_profile["non_null_count"] == 3
        assert id_profile["null_count"] == 0


class TestDataQualityValidator:
    """Tests for data quality validation"""
    
    def test_schema_validation(self, sample_dataframe):
        """Test schema validation"""
        validator = DataQualityValidator(sample_dataframe, "test_dataset")
        result = validator.validate_schema(["id", "name", "amount"])
        
        assert result["passed"] == True
        assert len(result["missing_columns"]) == 0
    
    def test_schema_validation_missing_column(self, sample_dataframe):
        """Test schema validation with missing column"""
        validator = DataQualityValidator(sample_dataframe, "test_dataset")
        result = validator.validate_schema(["id", "name", "missing_column"])
        
        assert result["passed"] == False
        assert "missing_column" in result["missing_columns"]
    
    def test_no_nulls_validation(self, sample_dataframe):
        """Test null value validation"""
        validator = DataQualityValidator(sample_dataframe, "test_dataset")
        result = validator.validate_no_nulls(["id", "name"])
        
        assert result["passed"] == True
        assert len(result["null_violations"]) == 0
    
    def test_duplicates_validation(self, sample_dataframe):
        """Test duplicate detection"""
        validator = DataQualityValidator(sample_dataframe, "test_dataset")
        result = validator.validate_no_duplicates(["id"])
        
        assert result["passed"] == True
        assert result["duplicate_count"] == 0
    
    def test_value_range_validation(self, sample_dataframe):
        """Test value range validation"""
        validator = DataQualityValidator(sample_dataframe, "test_dataset")
        result = validator.validate_value_range("amount", 0, 1000)
        
        assert result["passed"] == True
        assert result["violations"] == 0
    
    def test_value_range_validation_fail(self, sample_dataframe):
        """Test value range validation failure"""
        validator = DataQualityValidator(sample_dataframe, "test_dataset")
        # Range that will be violated
        result = validator.validate_value_range("amount", 200, 300)
        
        # Some values will be outside this range
        assert result["violations"] >= 2
    
    def test_run_all_validations(self, sample_dataframe):
        """Test running all validations together"""
        validator = DataQualityValidator(sample_dataframe, "test_dataset")
        results = validator.run_all_validations()
        
        assert "dataset_name" in results
        assert "validations" in results
        assert "quality_score" in results
        assert results["quality_score"] >= 0
        assert results["quality_score"] <= 100
