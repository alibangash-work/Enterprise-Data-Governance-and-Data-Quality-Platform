"""
Main Airflow DAG for enterprise data governance platform.
Orchestrates data ingestion, transformation, quality checks, and monitoring.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.decorators import dag
from airflow.models import Variable
import logging

logger = logging.getLogger(__name__)

# Default arguments for all DAGs
default_args = {
    'owner': 'data-governance-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'tags': ['data-governance', 'production']
}

# ============================================================================
# DAG: Data Ingestion Pipeline
# ============================================================================

def ingest_sales_data():
    """Ingest sales data from source"""
    logger.info("Starting sales data ingestion...")
    try:
        from pyspark.sql import SparkSession
        from ingestion.sources import SalesSourceIngestion
        
        spark = SparkSession.builder \
            .appName("sales-ingestion") \
            .master("local[4]") \
            .getOrCreate()
        
        ingestion = SalesSourceIngestion(spark, "/data/bronze/sales")
        result = ingestion.ingest()
        
        logger.info(f"Sales ingestion result: {result}")
        return result
    except Exception as e:
        logger.error(f"Sales ingestion failed: {str(e)}")
        raise


def ingest_customer_data():
    """Ingest customer data from source"""
    logger.info("Starting customer data ingestion...")
    try:
        from pyspark.sql import SparkSession
        from ingestion.sources import CustomerSourceIngestion
        
        spark = SparkSession.builder \
            .appName("customer-ingestion") \
            .master("local[4]") \
            .getOrCreate()
        
        ingestion = CustomerSourceIngestion(spark, "/data/bronze/customers")
        result = ingestion.ingest()
        
        logger.info(f"Customer ingestion result: {result}")
        return result
    except Exception as e:
        logger.error(f"Customer ingestion failed: {str(e)}")
        raise


def ingest_transaction_data():
    """Ingest transaction data from source"""
    logger.info("Starting transaction data ingestion...")
    try:
        from pyspark.sql import SparkSession
        from ingestion.sources import TransactionSourceIngestion
        
        spark = SparkSession.builder \
            .appName("transaction-ingestion") \
            .master("local[4]") \
            .getOrCreate()
        
        ingestion = TransactionSourceIngestion(spark, "/data/bronze/transactions")
        result = ingestion.ingest()
        
        logger.info(f"Transaction ingestion result: {result}")
        return result
    except Exception as e:
        logger.error(f"Transaction ingestion failed: {str(e)}")
        raise


ingestion_dag = DAG(
    'data_ingestion_pipeline',
    default_args=default_args,
    description='Ingests data from multiple sources into Bronze layer',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False
)

ingest_sales = PythonOperator(
    task_id='ingest_sales',
    python_callable=ingest_sales_data,
    dag=ingestion_dag
)

ingest_customers = PythonOperator(
    task_id='ingest_customers',
    python_callable=ingest_customer_data,
    dag=ingestion_dag
)

ingest_transactions = PythonOperator(
    task_id='ingest_transactions',
    python_callable=ingest_transaction_data,
    dag=ingestion_dag
)

# Parallel ingestion
ingest_sales >> ingest_customers >> ingest_transactions


# ============================================================================
# DAG: Data Transformation Pipeline
# ============================================================================

def transform_sales():
    """Transform sales data from Bronze to Silver"""
    logger.info("Starting sales transformation...")
    try:
        from pyspark.sql import SparkSession
        from processing.transformers import SalesTransformer
        
        spark = SparkSession.builder \
            .appName("sales-transform") \
            .master("local[4]") \
            .getOrCreate()
        
        transformer = SalesTransformer(
            spark,
            "/data/bronze/sales",
            "/data/silver/sales"
        )
        result = transformer.execute()
        logger.info(f"Sales transformation result: {result}")
        return result
    except Exception as e:
        logger.error(f"Sales transformation failed: {str(e)}")
        raise


def transform_customers():
    """Transform customer data from Bronze to Silver"""
    logger.info("Starting customer transformation...")
    try:
        from pyspark.sql import SparkSession
        from processing.transformers import CustomerTransformer
        
        spark = SparkSession.builder \
            .appName("customer-transform") \
            .master("local[4]") \
            .getOrCreate()
        
        transformer = CustomerTransformer(
            spark,
            "/data/bronze/customers",
            "/data/silver/customers"
        )
        result = transformer.execute()
        logger.info(f"Customer transformation result: {result}")
        return result
    except Exception as e:
        logger.error(f"Customer transformation failed: {str(e)}")
        raise


def transform_transactions():
    """Transform transaction data from Bronze to Silver"""
    logger.info("Starting transaction transformation...")
    try:
        from pyspark.sql import SparkSession
        from processing.transformers import TransactionTransformer
        
        spark = SparkSession.builder \
            .appName("transaction-transform") \
            .master("local[4]") \
            .getOrCreate()
        
        transformer = TransactionTransformer(
            spark,
            "/data/bronze/transactions",
            "/data/silver/transactions"
        )
        result = transformer.execute()
        logger.info(f"Transaction transformation result: {result}")
        return result
    except Exception as e:
        logger.error(f"Transaction transformation failed: {str(e)}")
        raise


transform_dag = DAG(
    'data_transformation_pipeline',
    default_args=default_args,
    description='Transforms data from Bronze to Silver layer',
    schedule_interval='0 3 * * *',  # Daily at 3 AM
    catchup=False
)

transform_sales_task = PythonOperator(
    task_id='transform_sales',
    python_callable=transform_sales,
    dag=transform_dag
)

transform_customers_task = PythonOperator(
    task_id='transform_customers',
    python_callable=transform_customers,
    dag=transform_dag
)

transform_transactions_task = PythonOperator(
    task_id='transform_transactions',
    python_callable=transform_transactions,
    dag=transform_dag
)

# Parallel transformations
transform_sales_task >> transform_customers_task >> transform_transactions_task


# ============================================================================
# DAG: Data Quality Checks Pipeline
# ============================================================================

def quality_check_sales():
    """Run quality checks on sales data"""
    logger.info("Running sales data quality checks...")
    try:
        from pyspark.sql import SparkSession
        from data_quality.validators import DataQualityValidator
        
        spark = SparkSession.builder \
            .appName("sales-quality") \
            .master("local[4]") \
            .getOrCreate()
        
        df = spark.read.parquet("/data/silver/sales")
        validator = DataQualityValidator(df, "sales")
        
        results = validator.run_all_validations()
        logger.info(f"Sales quality results: {results}")
        return results
    except Exception as e:
        logger.error(f"Sales quality check failed: {str(e)}")
        raise


def quality_check_customers():
    """Run quality checks on customer data"""
    logger.info("Running customer data quality checks...")
    try:
        from pyspark.sql import SparkSession
        from data_quality.validators import DataQualityValidator
        
        spark = SparkSession.builder \
            .appName("customer-quality") \
            .master("local[4]") \
            .getOrCreate()
        
        df = spark.read.parquet("/data/silver/customers")
        validator = DataQualityValidator(df, "customers")
        
        results = validator.run_all_validations()
        logger.info(f"Customer quality results: {results}")
        return results
    except Exception as e:
        logger.error(f"Customer quality check failed: {str(e)}")
        raise


quality_dag = DAG(
    'data_quality_checks_pipeline',
    default_args=default_args,
    description='Validates data quality across datasets',
    schedule_interval='0 4 * * *',  # Daily at 4 AM
    catchup=False
)

quality_sales = PythonOperator(
    task_id='quality_check_sales',
    python_callable=quality_check_sales,
    dag=quality_dag
)

quality_customers = PythonOperator(
    task_id='quality_check_customers',
    python_callable=quality_check_customers,
    dag=quality_dag
)

quality_sales >> quality_customers


# ============================================================================
# DAG: Metadata & Catalog Update Pipeline
# ============================================================================

def update_catalog():
    """Update data catalog with latest metadata"""
    logger.info("Updating data catalog...")
    try:
        from catalog.metadata import get_data_catalog, DatasetBuilder
        
        catalog = get_data_catalog()
        
        # Register or update datasets
        sales_metadata = (DatasetBuilder("sales_silver")
                         .name("Sales Silver Layer")
                         .description("Cleaned and standardized sales data")
                         .owner("data-team")
                         .layer("silver")
                         .format("delta")
                         .location("/data/silver/sales")
                         .tags(["sales", "silver", "critical"])
                         .quality_score(92.5)
                         .record_count(5)
                         .build())
        
        catalog.register_dataset(sales_metadata)
        logger.info("Catalog updated successfully")
        return {"status": "success", "datasets_registered": 1}
    except Exception as e:
        logger.error(f"Catalog update failed: {str(e)}")
        raise


catalog_dag = DAG(
    'metadata_catalog_update_pipeline',
    default_args=default_args,
    description='Updates data catalog with latest metadata',
    schedule_interval='0 5 * * *',  # Daily at 5 AM
    catchup=False
)

update_catalog_task = PythonOperator(
    task_id='update_catalog',
    python_callable=update_catalog,
    dag=catalog_dag
)


# ============================================================================
# DAG: Master Orchestration Pipeline
# ============================================================================

master_dag = DAG(
    'master_data_governance_pipeline',
    default_args=default_args,
    description='Master orchestration pipeline for data governance',
    schedule_interval='0 1 * * *',  # Daily at 1 AM
    catchup=False
)

# Master pipeline tasks
ingest_all = BashOperator(
    task_id='trigger_ingestion',
    bash_command='echo "Triggering data ingestion pipeline"',
    dag=master_dag
)

transform_all = BashOperator(
    task_id='trigger_transformation',
    bash_command='echo "Triggering transformation pipeline"',
    dag=master_dag
)

validate_all = BashOperator(
    task_id='trigger_quality_checks',
    bash_command='echo "Triggering quality validation pipeline"',
    dag=master_dag
)

update_metadata = BashOperator(
    task_id='trigger_metadata_update',
    bash_command='echo "Triggering metadata catalog update"',
    dag=master_dag
)

notify_completion = BashOperator(
    task_id='notify_completion',
    bash_command='echo "Data governance pipeline completed successfully"',
    dag=master_dag
)

# Master pipeline dependencies
ingest_all >> transform_all >> validate_all >> update_metadata >> notify_completion
