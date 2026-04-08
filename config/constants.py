"""
Application-wide constants.
"""

# Data layers
BRONZE_LAYER = "bronze"
SILVER_LAYER = "silver"
GOLD_LAYER = "gold"

LAYERS = [BRONZE_LAYER, SILVER_LAYER, GOLD_LAYER]

# Data formats
PARQUET = "parquet"
DELTA = "delta"
CSV = "csv"
JSON = "json"

# Quality check types
QUALITY_CHECK_SCHEMA = "schema_validation"
QUALITY_CHECK_NULL = "null_check"
QUALITY_CHECK_DUPLICATES = "duplicates_check"
QUALITY_CHECK_RANGE = "range_check"
QUALITY_CHECK_PATTERN = "pattern_check"
QUALITY_CHECK_REF_INTEGRITY = "referential_integrity"

# Lineage types
LINEAGE_TYPE_DATASET = "dataset"
LINEAGE_TYPE_PROCESS = "process"
LINEAGE_TYPE_TASK = "task"

# Status codes
STATUS_SUCCESS = "success"
STATUS_FAILURE = "failure"
STATUS_WARNING = "warning"
STATUS_PENDING = "pending"
STATUS_RUNNING = "running"

# Metrics
METRIC_RECORDS_PROCESSED = "records_processed"
METRIC_RECORDS_FAILED = "records_failed"
METRIC_DATA_QUALITY_SCORE = "data_quality_score"
METRIC_PIPELINE_DURATION = "pipeline_duration"

# Default batch size
DEFAULT_BATCH_SIZE = 10000

# Default timeout (seconds)
DEFAULT_TIMEOUT = 3600  # 1 hour
