"""
Data quality framework using Great Expectations.
Implements validation, profiling, and anomaly detection.
"""

from typing import Dict, Any, Optional, List
from pyspark.sql import DataFrame, SparkSession
from datetime import datetime
import json
from config.logger import setup_logger

logger = setup_logger(__name__)


class DataQualityProfiler:
    """Profile data for quality metrics"""
    
    def __init__(self, dataframe: DataFrame, dataset_name: str):
        """
        Initialize profiler.
        
        Args:
            dataframe: DataFrame to profile
            dataset_name: Name of the dataset
        """
        self.dataframe = dataframe
        self.dataset_name = dataset_name
        self.profiling_time = datetime.utcnow()
        self.profile = {}
    
    def profile_data(self) -> Dict[str, Any]:
        """Generate comprehensive data quality profile"""
        try:
            logger.info(f"Profiling data for: {self.dataset_name}")
            
            profile = {
                "dataset_name": self.dataset_name,
                "profiling_time": self.profiling_time.isoformat(),
                "total_rows": self.dataframe.count(),
                "total_columns": len(self.dataframe.columns),
                "columns": {}
            }
            
            # Profile each column
            for col_name in self.dataframe.columns:
                profile["columns"][col_name] = self._profile_column(col_name)
            
            self.profile = profile
            return profile
            
        except Exception as e:
            logger.error(f"Profiling failed for {self.dataset_name}: {str(e)}")
            return {"status": "error", "error": str(e)}
    
    def _profile_column(self, col_name: str) -> Dict[str, Any]:
        """Profile individual column"""
        from pyspark.sql.functions import col, count, when, min as spark_min, max as spark_max
        
        col_data = self.dataframe.select(col_name)
        
        stats = col_data.select(
            count(col_name).alias("count"),
            count(when(col(col_name).isNull(), 1)).alias("nulls"),
            count(when(col(col_name).isNotNull(), 1)).alias("non_nulls")
        ).collect()[0]
        
        return {
            "data_type": col_data.schema.fields[0].dataType.typeName(),
            "total_count": stats["count"],
            "null_count": stats["nulls"],
            "non_null_count": stats["non_nulls"],
            "null_percentage": round((stats["nulls"] / stats["count"] * 100), 2) if stats["count"] > 0 else 0,
            "distinct_count": col_data.distinct().count() if stats["count"] < 100000 else "N/A"
        }


class DataQualityValidator:
    """Validate data against quality rules"""
    
    def __init__(self, dataframe: DataFrame, dataset_name: str):
        """
        Initialize validator.
        
        Args:
            dataframe: DataFrame to validate
            dataset_name: Name of the dataset
        """
        self.dataframe = dataframe
        self.dataset_name = dataset_name
        self.validation_results = {}
    
    def validate_schema(self, expected_columns: List[str]) -> Dict[str, Any]:
        """Validate data schema"""
        actual_columns = self.dataframe.columns
        missing_columns = [col for col in expected_columns if col not in actual_columns]
        
        result = {
            "validation_type": "schema_validation",
            "dataset_name": self.dataset_name,
            "expected_columns": expected_columns,
            "actual_columns": actual_columns,
            "missing_columns": missing_columns,
            "passed": len(missing_columns) == 0
        }
        
        logger.info(f"Schema validation {'passed' if result['passed'] else 'failed'} for {self.dataset_name}")
        return result
    
    def validate_no_nulls(self, columns: List[str]) -> Dict[str, Any]:
        """Validate no null values in specified columns"""
        from pyspark.sql.functions import col, count, when
        
        null_counts = self.dataframe.select(
            *[count(when(col(c).isNull(), 1)).alias(c) for c in columns]
        ).collect()[0]
        
        violations = {col: null_counts[col] for col in columns if null_counts[col] > 0}
        
        result = {
            "validation_type": "null_check",
            "dataset_name": self.dataset_name,
            "columns": columns,
            "null_violations": violations,
            "passed": len(violations) == 0
        }
        
        if violations:
            logger.warning(f"Null violations found in {self.dataset_name}: {violations}")
        
        return result
    
    def validate_no_duplicates(self, key_columns: List[str]) -> Dict[str, Any]:
        """Validate no duplicate records"""
        total_records = self.dataframe.count()
        unique_records = self.dataframe.dropDuplicates(key_columns).count()
        
        duplicates = total_records - unique_records
        
        result = {
            "validation_type": "duplicates_check",
            "dataset_name": self.dataset_name,
            "key_columns": key_columns,
            "total_records": total_records,
            "unique_records": unique_records,
            "duplicate_count": duplicates,
            "passed": duplicates == 0
        }
        
        if duplicates > 0:
            logger.warning(f"Duplicates found in {self.dataset_name}: {duplicates} duplicates")
        
        return result
    
    def validate_value_range(self, column: str, min_val: float, max_val: float) -> Dict[str, Any]:
        """Validate numeric values are within range"""
        from pyspark.sql.functions import col, count, when
        
        violations = self.dataframe.select(
            count(when((col(column) < min_val) | (col(column) > max_val), 1)).alias("violations")
        ).collect()[0]["violations"]
        
        result = {
            "validation_type": "range_check",
            "dataset_name": self.dataset_name,
            "column": column,
            "min_value": min_val,
            "max_value": max_val,
            "violations": violations,
            "passed": violations == 0
        }
        
        if violations > 0:
            logger.warning(f"Range violations in {self.dataset_name}.{column}: {violations}")
        
        return result
    
    def validate_value_pattern(self, column: str, pattern: str) -> Dict[str, Any]:
        """Validate values match pattern (regex)"""
        from pyspark.sql.functions import col, count, when, regexp_replace
        
        violations = self.dataframe.select(
            count(when(col(column).rlike(pattern), 0)).alias("violations")
        ).collect()[0]["violations"]
        
        result = {
            "validation_type": "pattern_check",
            "dataset_name": self.dataset_name,
            "column": column,
            "pattern": pattern,
            "violations": violations,
            "passed": violations == 0
        }
        
        return result
    
    def run_all_validations(self) -> Dict[str, Any]:
        """Run all standard validations"""
        from pyspark.sql.functions import col
        
        results = {
            "dataset_name": self.dataset_name,
            "validation_timestamp": datetime.utcnow().isoformat(),
            "validations": []
        }
        
        # Schema validation
        expected_cols = self.dataframe.columns
        schema_result = self.validate_schema(expected_cols)
        results["validations"].append(schema_result)
        
        # No nulls in key columns (assuming first column is key)
        if len(self.dataframe.columns) > 0:
            null_result = self.validate_no_nulls([self.dataframe.columns[0]])
            results["validations"].append(null_result)
        
        # No duplicates
        dup_result = self.validate_no_duplicates(self.dataframe.columns[:1])
        results["validations"].append(dup_result)
        
        # Overall quality score
        passed = sum(1 for v in results["validations"] if v.get("passed", False))
        results["quality_score"] = round((passed / len(results["validations"])) * 100, 2)
        results["passed"] = results["quality_score"] >= 80
        
        self.validation_results = results
        return results


class DriftDetector:
    """Detect statistical drift in data"""
    
    def __init__(self, baseline_stats: Dict[str, Any], current_data: DataFrame):
        """
        Initialize drift detector.
        
        Args:
            baseline_stats: Baseline statistics
            current_data: Current data to compare
        """
        self.baseline_stats = baseline_stats
        self.current_data = current_data
        self.drift_results = {}
    
    def detect_volume_drift(self, threshold: float = 0.2) -> Dict[str, Any]:
        """Detect dataset volume drift"""
        current_count = self.current_data.count()
        baseline_count = self.baseline_stats.get("total_rows", 0)
        
        if baseline_count == 0:
            return {"status": "skipped", "reason": "No baseline available"}
        
        drift_percentage = abs((current_count - baseline_count) / baseline_count)
        
        return {
            "drift_type": "volume_drift",
            "baseline_count": baseline_count,
            "current_count": current_count,
            "drift_percentage": round(drift_percentage * 100, 2),
            "threshold_percentage": threshold * 100,
            "drift_detected": drift_percentage > threshold
        }
    
    def detect_column_drift(self, column: str, threshold: float = 0.3) -> Dict[str, Any]:
        """Detect column-level statistical drift"""
        from pyspark.sql.functions import col, count, when
        
        baseline_nulls = self.baseline_stats.get("columns", {}).get(column, {}).get("null_percentage", 0)
        
        current_stats = self.current_data.select(
            count(when(col(column).isNull(), 1)).alias("nulls"),
            count(col(column)).alias("total")
        ).collect()[0]
        
        current_nulls = (current_stats["nulls"] / current_stats["total"] * 100) if current_stats["total"] > 0 else 0
        
        drift_percentage = abs(current_nulls - baseline_nulls) / max(baseline_nulls, 1)
        
        return {
            "drift_type": "column_drift",
            "column": column,
            "baseline_null_percentage": baseline_nulls,
            "current_null_percentage": round(current_nulls, 2),
            "drift_percentage": round(drift_percentage * 100, 2),
            "threshold_percentage": threshold * 100,
            "drift_detected": drift_percentage > threshold
        }
