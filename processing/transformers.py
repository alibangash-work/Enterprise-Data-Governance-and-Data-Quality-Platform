"""
Data transformation and processing logic for different layers.
Implements Bronze -> Silver -> Gold transformations.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, Any, Optional, List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, coalesce, when, trim, upper, lower, 
    md5, concat_ws, current_timestamp, lit
)
from config.logger import setup_logger
from config.constants import BRONZE_LAYER, SILVER_LAYER, GOLD_LAYER

logger = setup_logger(__name__)


class BaseTransformer(ABC):
    """Base class for all transformers"""
    
    def __init__(
        self,
        name: str,
        spark: SparkSession,
        source_path: str,
        target_path: str,
        target_layer: str
    ):
        """
        Initialize transformer.
        
        Args:
            name: Transformer name
            spark: SparkSession instance
            source_path: Source data path
            target_path: Target data path
            target_layer: Target layer (bronze/silver/gold)
        """
        self.name = name
        self.spark = spark
        self.source_path = source_path
        self.target_path = target_path
        self.target_layer = target_layer
        self.transformation_time = datetime.utcnow()
        self.metrics = {}
    
    @abstractmethod
    def transform(self) -> DataFrame:
        """Implement transformation logic"""
        pass
    
    def add_transformation_metadata(self, df: DataFrame) -> DataFrame:
        """Add transformation metadata columns"""
        df = df.withColumn("_transformation_time", current_timestamp())
        df = df.withColumn("_transformer_name", lit(self.name))
        df = df.withColumn("_target_layer", lit(self.target_layer))
        return df
    
    def write_output(self, df: DataFrame, mode: str = "overwrite") -> None:
        """Write transformed data to target"""
        df.write \
            .format("delta") \
            .mode(mode) \
            .option("mergeSchema", "true") \
            .save(self.target_path)
    
    def execute(self) -> Dict[str, Any]:
        """Execute the transformation"""
        try:
            logger.info(f"Starting transformation: {self.name}")
            
            # Read source
            df = self.spark.read.parquet(self.source_path)
            source_count = df.count()
            logger.info(f"Read {source_count} records from source")
            
            # Transform
            df = self.transform()
            target_count = df.count()
            
            # Add metadata
            df = self.add_transformation_metadata(df)
            
            # Write output
            self.write_output(df)
            
            result = {
                "transformer": self.name,
                "status": "success",
                "source_records": source_count,
                "target_records": target_count,
                "target_layer": self.target_layer,
                "target_path": self.target_path,
                "transformation_time": self.transformation_time.isoformat(),
                "columns": df.columns
            }
            
            self.metrics = result
            logger.info(f"Transformation completed: {self.name}")
            return result
            
        except Exception as e:
            logger.error(f"Transformation failed for {self.name}: {str(e)}")
            return {
                "transformer": self.name,
                "status": "failure",
                "error": str(e),
                "transformation_time": self.transformation_time.isoformat()
            }


class SalesTransformer(BaseTransformer):
    """Transform sales data from Bronze to Silver"""
    
    def __init__(self, spark: SparkSession, source_path: str, target_path: str):
        super().__init__(
            name="sales_transformer",
            spark=spark,
            source_path=source_path,
            target_path=target_path,
            target_layer=SILVER_LAYER
        )
    
    def transform(self) -> DataFrame:
        """Clean and standardize sales data"""
        df = self.spark.read.parquet(self.source_path)
        
        # Remove duplicates
        df = df.dropDuplicates(["sales_id"])
        
        # Data cleaning
        df = df \
            .withColumn("customer_id", col("customer_id").cast("long")) \
            .withColumn("quantity", col("quantity").cast("int")) \
            .withColumn("amount", col("amount").cast("decimal(10, 2)")) \
            .withColumn("product_id", upper(trim(col("product_id")))) \
            .withColumn("region", trim(col("region")))
        
        # Remove nulls in critical columns
        df = df.filter(col("customer_id").isNotNull() & col("amount").isNotNull())
        
        return df


class CustomerTransformer(BaseTransformer):
    """Transform customer data from Bronze to Silver"""
    
    def __init__(self, spark: SparkSession, source_path: str, target_path: str):
        super().__init__(
            name="customer_transformer",
            spark=spark,
            source_path=source_path,
            target_path=target_path,
            target_layer=SILVER_LAYER
        )
    
    def transform(self) -> DataFrame:
        """Clean and enrich customer data"""
        df = self.spark.read.parquet(self.source_path)
        
        # Remove duplicates
        df = df.dropDuplicates(["customer_id"])
        
        # Data cleaning
        df = df \
            .withColumn("customer_id", col("customer_id").cast("long")) \
            .withColumn("name", trim(col("name"))) \
            .withColumn("country", upper(trim(col("country")))) \
            .withColumn("email", lower(trim(col("email"))))
        
        # Remove nulls in critical columns
        df = df.filter(col("customer_id").isNotNull() & col("name").isNotNull())
        
        # Add customer hash for deduplication
        df = df.withColumn(
            "customer_hash",
            md5(concat_ws("|", col("name"), col("email"), col("country")))
        )
        
        return df


class TransactionTransformer(BaseTransformer):
    """Transform transaction data from Bronze to Silver"""
    
    def __init__(self, spark: SparkSession, source_path: str, target_path: str):
        super().__init__(
            name="transaction_transformer",
            spark=spark,
            source_path=source_path,
            target_path=target_path,
            target_layer=SILVER_LAYER
        )
    
    def transform(self) -> DataFrame:
        """Clean and validate transaction data"""
        df = self.spark.read.parquet(self.source_path)
        
        # Remove duplicates
        df = df.dropDuplicates(["transaction_id"])
        
        # Data cleaning
        df = df \
            .withColumn("customer_id", col("customer_id").cast("long")) \
            .withColumn("amount", col("amount").cast("decimal(10, 2)")) \
            .withColumn("status", upper(trim(col("status")))) \
            .withColumn("channel", upper(trim(col("channel"))))
        
        # Filter valid transactions
        df = df.filter(
            (col("customer_id").isNotNull()) & 
            (col("amount") > 0) & 
            (col("status").isin(["COMPLETED", "PENDING", "FAILED"]))
        )
        
        return df


class SalesAggregationTransformer(BaseTransformer):
    """Aggregate sales data from Silver to Gold"""
    
    def __init__(self, spark: SparkSession, source_path: str, target_path: str):
        super().__init__(
            name="sales_aggregation",
            spark=spark,
            source_path=source_path,
            target_path=target_path,
            target_layer=GOLD_LAYER
        )
    
    def transform(self) -> DataFrame:
        """Aggregate sales by region and time"""
        df = self.spark.read.parquet(self.source_path)
        
        # Aggregate sales by region
        agg_df = df.groupBy("region").agg({
            "amount": "sum",
            "quantity": "sum",
            "sales_id": "count"
        }).withColumnRenamed("count(sales_id)", "num_transactions")
        
        agg_df = agg_df.withColumn("avg_transaction_amount", 
                                    col("sum(amount)") / col("num_transactions"))
        
        return agg_df


class CustomerMetricsTransformer(BaseTransformer):
    """Create customer metrics dataset from Silver to Gold"""
    
    def __init__(self, spark: SparkSession, customers_path: str, transactions_path: str, target_path: str):
        super().__init__(
            name="customer_metrics",
            spark=spark,
            source_path=customers_path,
            target_path=target_path,
            target_layer=GOLD_LAYER
        )
        self.transactions_path = transactions_path
    
    def transform(self) -> DataFrame:
        """Create customer metrics"""
        customers = self.spark.read.parquet(self.source_path)
        transactions = self.spark.read.parquet(self.transactions_path)
        
        # Join and aggregate
        metrics = transactions \
            .filter(col("status") == "COMPLETED") \
            .groupBy("customer_id").agg({
                "amount": "sum",
                "transaction_id": "count"
            }) \
            .withColumnRenamed("sum(amount)", "total_spent") \
            .withColumnRenamed("count(transaction_id)", "num_transactions")
        
        # Join with customer data
        result = customers.join(metrics, on="customer_id", how="left")
        result = result.fillna(0, subset=["total_spent", "num_transactions"])
        
        return result
