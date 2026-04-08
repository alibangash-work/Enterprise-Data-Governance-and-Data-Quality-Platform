"""
Base ingestion module for loading data into Bronze layer.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, Any, Optional
from pyspark.sql import DataFrame, SparkSession
import json
from config.logger import setup_logger
from config.constants import BRONZE_LAYER

logger = setup_logger(__name__)


class BaseIngestionSource(ABC):
    """
    Abstract base class for all data sources.
    Defines the interface for data ingestion.
    """
    
    def __init__(
        self,
        source_name: str,
        spark: SparkSession,
        target_path: str,
        incremental: bool = False
    ):
        """
        Initialize the ingestion source.
        
        Args:
            source_name: Name of the data source
            spark: SparkSession instance
            target_path: Path to write ingested data
            incremental: Whether to use incremental loading
        """
        self.source_name = source_name
        self.spark = spark
        self.target_path = target_path
        self.incremental = incremental
        self.ingestion_time = datetime.utcnow()
        self.metadata = {}
    
    @abstractmethod
    def read_source(self) -> DataFrame:
        """
        Implement source-specific reading logic.
        
        Returns:
            DataFrame with source data
        """
        pass
    
    def add_governance_columns(self, df: DataFrame) -> DataFrame:
        """
        Add governance metadata columns to the data.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with governance columns
        """
        from pyspark.sql.functions import lit, current_timestamp
        
        df = df.withColumn("_ingestion_time", current_timestamp())
        df = df.withColumn("_source_name", lit(self.source_name))
        df = df.withColumn("_ingestion_layer", lit(BRONZE_LAYER))
        df = df.withColumn("_ingestion_id", lit(self.ingestion_time.isoformat()))
        
        return df
    
    def ingest(self) -> Dict[str, Any]:
        """
        Orchestrate the ingestion process.
        
        Returns:
            Ingestion result metadata
        """
        try:
            logger.info(f"Starting ingestion from source: {self.source_name}")
            
            # Read source data
            df = self.read_source()
            record_count_raw = df.count()
            logger.info(f"Read {record_count_raw} records from {self.source_name}")
            
            # Add governance columns
            df = self.add_governance_columns(df)
            
            # Write to Bronze layer
            self._write_to_bronze(df)
            logger.info(f"Successfully ingested {record_count_raw} records to Bronze layer")
            
            # Capture metadata
            result = {
                "source": self.source_name,
                "status": "success",
                "records_ingested": record_count_raw,
                "ingestion_time": self.ingestion_time.isoformat(),
                "target_path": self.target_path,
                "columns": df.columns,
                "schema": df.schema.simpleString()
            }
            
            self.metadata = result
            return result
            
        except Exception as e:
            logger.error(f"Ingestion failed for {self.source_name}: {str(e)}")
            return {
                "source": self.source_name,
                "status": "failure",
                "error": str(e),
                "ingestion_time": self.ingestion_time.isoformat()
            }
    
    def _write_to_bronze(self, df: DataFrame) -> None:
        """
        Write DataFrame to Bronze layer in Delta format.
        
        Args:
            df: DataFrame to write
        """
        write_mode = "append" if self.incremental else "overwrite"
        
        df.write \
            .format("delta") \
            .mode(write_mode) \
            .option("mergeSchema", "true") \
            .save(self.target_path)


class CSVIngestionSource(BaseIngestionSource):
    """CSV file data source ingestion"""
    
    def __init__(
        self,
        source_name: str,
        spark: SparkSession,
        target_path: str,
        file_path: str,
        **csv_options
    ):
        """
        Initialize CSV ingestion source.
        
        Args:
            source_name: Name of the data source
            spark: SparkSession instance
            target_path: Path to write ingested data
            file_path: Path to CSV file
            **csv_options: Additional Spark CSV read options
        """
        super().__init__(source_name, spark, target_path)
        self.file_path = file_path
        self.csv_options = {
            "header": "true",
            "inferSchema": "true",
            **csv_options
        }
    
    def read_source(self) -> DataFrame:
        """Read CSV file"""
        logger.info(f"Reading CSV from: {self.file_path}")
        return self.spark.read.csv(self.file_path, **self.csv_options)


class JSONIngestionSource(BaseIngestionSource):
    """JSON file data source ingestion"""
    
    def __init__(
        self,
        source_name: str,
        spark: SparkSession,
        target_path: str,
        file_path: str,
        **json_options
    ):
        super().__init__(source_name, spark, target_path)
        self.file_path = file_path
        self.json_options = json_options
    
    def read_source(self) -> DataFrame:
        """Read JSON file"""
        logger.info(f"Reading JSON from: {self.file_path}")
        return self.spark.read.json(self.file_path, **self.json_options)


class ParquetIngestionSource(BaseIngestionSource):
    """Parquet file data source ingestion"""
    
    def __init__(
        self,
        source_name: str,
        spark: SparkSession,
        target_path: str,
        file_path: str
    ):
        super().__init__(source_name, spark, target_path)
        self.file_path = file_path
    
    def read_source(self) -> DataFrame:
        """Read Parquet file"""
        logger.info(f"Reading Parquet from: {self.file_path}")
        return self.spark.read.parquet(self.file_path)


class InMemoryIngestionSource(BaseIngestionSource):
    """In-memory data source ingestion (for testing/simulation)"""
    
    def __init__(
        self,
        source_name: str,
        spark: SparkSession,
        target_path: str,
        data: list,
        schema: Optional[str] = None
    ):
        """
        Initialize in-memory ingestion source.
        
        Args:
            source_name: Name of the data source
            spark: SparkSession instance
            target_path: Path to write ingested data
            data: List of data dictionaries
            schema: Optional Spark schema DDL string
        """
        super().__init__(source_name, spark, target_path)
        self.data = data
        self.schema = schema
    
    def read_source(self) -> DataFrame:
        """Create DataFrame from in-memory data"""
        logger.info(f"Creating DataFrame from {len(self.data)} records")
        if self.schema:
            return self.spark.createDataFrame(self.data, schema=self.schema)
        else:
            return self.spark.createDataFrame(self.data)
