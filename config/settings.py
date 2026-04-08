"""
Configuration settings for the Data Governance Platform.
Handles environment-based configuration for local, staging, and production.
"""

import os
from typing import Optional
from dataclasses import dataclass
from enum import Enum


class Environment(Enum):
    """Environment types"""
    LOCAL = "local"
    STAGING = "staging"
    PRODUCTION = "production"


@dataclass
class S3Config:
    """AWS S3 Configuration"""
    bucket_name: str = os.getenv("S3_BUCKET", "data-governance-bucket")
    region: str = os.getenv("AWS_REGION", "us-east-1")
    access_key_id: Optional[str] = os.getenv("AWS_ACCESS_KEY_ID")
    secret_access_key: Optional[str] = os.getenv("AWS_SECRET_ACCESS_KEY")


@dataclass
class SparkConfig:
    """Apache Spark Configuration"""
    app_name: str = "DataGovernancePlatform"
    master: str = os.getenv("SPARK_MASTER", "local[4]")
    executor_memory: str = "2g"
    driver_memory: str = "2g"
    shuffle_partitions: int = 200


@dataclass
class MetastoreConfig:
    """Metastore Configuration for Delta Lake"""
    warehouse_path: str = os.getenv("WAREHOUSE_PATH", "/tmp/delta-warehouse")
    catalog_type: str = "hive"  # "hive" or "unity"


@dataclass
class APIConfig:
    """FastAPI Configuration"""
    host: str = os.getenv("API_HOST", "0.0.0.0")
    port: int = int(os.getenv("API_PORT", "8000"))
    debug: bool = os.getenv("API_DEBUG", "false").lower() == "true"
    workers: int = int(os.getenv("API_WORKERS", "4"))


@dataclass
class MonitoringConfig:
    """Prometheus/Grafana Configuration"""
    prometheus_endpoint: str = os.getenv("PROMETHEUS_ENDPOINT", "http://prometheus:9090")
    grafana_endpoint: str = os.getenv("GRAFANA_ENDPOINT", "http://grafana:3000")
    metrics_port: int = int(os.getenv("METRICS_PORT", "8001"))


@dataclass
class MetadataConfig:
    """OpenMetadata Configuration"""
    host: str = os.getenv("METADATA_HOST", "localhost")
    port: int = int(os.getenv("METADATA_PORT", "8585"))
    api_endpoint: str = f"http://{os.getenv('METADATA_HOST', 'localhost')}:{os.getenv('METADATA_PORT', '8585')}"


@dataclass
class DataQualityConfig:
    """Great Expectations Configuration"""
    ge_dir: str = os.getenv("GE_DIR", "/opt/ge")
    expectations_store_path: str = os.getenv("GE_STORE_PATH", "/opt/ge/expectations")
    validations_store_path: str = os.getenv("GE_VALIDATIONS_PATH", "/opt/ge/validations")
    suppress_ge_logging: bool = False


@dataclass
class PlatformConfig:
    """Main Platform Configuration"""
    environment: Environment = Environment(os.getenv("ENVIRONMENT", "local"))
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    
    # Sub-configs
    spark: SparkConfig = SparkConfig()
    s3: S3Config = S3Config()
    metastore: MetastoreConfig = MetastoreConfig()
    api: APIConfig = APIConfig()
    monitoring: MonitoringConfig = MonitoringConfig()
    metadata: MetadataConfig = MetadataConfig()
    data_quality: DataQualityConfig = DataQualityConfig()
    
    # Data paths
    bronze_path: str = os.getenv("BRONZE_PATH", "/data/bronze")
    silver_path: str = os.getenv("SILVER_PATH", "/data/silver")
    gold_path: str = os.getenv("GOLD_PATH", "/data/gold")
    
    # Feature flags
    enable_lineage: bool = os.getenv("ENABLE_LINEAGE", "true").lower() == "true"
    enable_quality_checks: bool = os.getenv("ENABLE_QUALITY_CHECKS", "true").lower() == "true"
    enable_monitoring: bool = os.getenv("ENABLE_MONITORING", "true").lower() == "true"


# Global platform config instance
platform_config = PlatformConfig()


def get_config() -> PlatformConfig:
    """Get the global platform configuration."""
    return platform_config
