"""
Example script: Complete data governance workflow
Demonstrates end-to-end platform usage
"""

from pyspark.sql import SparkSession
from datetime import datetime

# Import platform modules
from ingestion.sources import SalesSourceIngestion, CustomerSourceIngestion, TransactionSourceIngestion
from processing.transformers import SalesTransformer, CustomerTransformer, TransactionTransformer
from data_quality.validators import DataQualityValidator, DataQualityProfiler
from lineage.tracker import get_lineage_tracker, LineageNode, LineageEdge, EntityType, AssetType
from catalog.metadata import get_data_catalog, DatasetBuilder
from observability.monitoring import get_execution_monitor, PipelineStatus

from config.logger import setup_logger

logger = setup_logger(__name__)

def main():
    """Execute complete data governance workflow"""
    
    logger.info("Starting demonstration of Enterprise Data Governance Platform")
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("DataGovernancePlatformDemo") \
        .master("local[4]") \
        .getOrCreate()
    
    # Initialize platform components
    tracker = get_lineage_tracker()
    catalog = get_data_catalog()
    monitor = get_execution_monitor()
    
    try:
        # =====================================================================
        # 1. DATA INGESTION
        # =====================================================================
        logger.info("=== PHASE 1: DATA INGESTION ===")
        
        # Track pipeline execution
        exec_id = f"ingest_{datetime.utcnow().timestamp()}"
        monitor.start_execution(
            execution_id=exec_id,
            pipeline_name="data_ingestion",
            source="multiple_sources",
            target="/data/bronze"
        )
        
        # Ingest sales data
        logger.info("Ingesting sales data...")
        sales_ingest = SalesSourceIngestion(spark, "/tmp/demo_bronze_sales")
        sales_result = sales_ingest.ingest()
        logger.info(f"Sales ingestion: {sales_result['records_ingested']} records")
        
        # Ingest customer data
        logger.info("Ingesting customer data...")
        customer_ingest = CustomerSourceIngestion(spark, "/tmp/demo_bronze_customers")
        customer_result = customer_ingest.ingest()
        logger.info(f"Customer ingestion: {customer_result['records_ingested']} records")
        
        # Ingest transaction data
        logger.info("Ingesting transaction data...")
        transaction_ingest = TransactionSourceIngestion(spark, "/tmp/demo_bronze_transactions")
        transaction_result = transaction_ingest.ingest()
        logger.info(f"Transaction ingestion: {transaction_result['records_ingested']} records")
        
        total_ingested = (sales_result['records_ingested'] + 
                         customer_result['records_ingested'] + 
                         transaction_result['records_ingested'])
        
        monitor.end_execution(
            execution_id=exec_id,
            status=PipelineStatus.SUCCESS,
            records_processed=total_ingested,
            records_failed=0
        )
        
        # =====================================================================
        # 2. DATA LINEAGE SETUP
        # =====================================================================
        logger.info("\n=== PHASE 2: DATA LINEAGE TRACKING ===")
        
        # Create lineage graph
        graph = tracker.create_graph("data_flow_lineage")
        
        # Define lineage nodes
        nodes = [
            LineageNode("sales_api", EntityType.DATASET, "Sales API", AssetType.SOURCE),
            LineageNode("customer_db", EntityType.DATASET, "Customer Database", AssetType.SOURCE),
            LineageNode("transaction_stream", EntityType.DATASET, "Transaction Stream", AssetType.SOURCE),
            LineageNode("sales_bronze", EntityType.DATASET, "Sales Bronze", AssetType.STAGE),
            LineageNode("customer_bronze", EntityType.DATASET, "Customer Bronze", AssetType.STAGE),
            LineageNode("transaction_bronze", EntityType.DATASET, "Transaction Bronze", AssetType.STAGE),
            LineageNode("sales_silver", EntityType.DATASET, "Sales Silver", AssetType.TRANSFORM),
            LineageNode("customer_silver", EntityType.DATASET, "Customer Silver", AssetType.TRANSFORM),
            LineageNode("transaction_silver", EntityType.DATASET, "Transaction Silver", AssetType.TRANSFORM),
            LineageNode("sales_metrics_gold", EntityType.DATASET, "Sales Metrics", AssetType.SINK),
            LineageNode("customer_metrics_gold", EntityType.DATASET, "Customer Metrics", AssetType.SINK),
        ]
        
        for node in nodes:
            graph.add_node(node)
        
        # Define relationships
        edges = [
            LineageEdge("sales_api", "sales_bronze", "ingested_from"),
            LineageEdge("customer_db", "customer_bronze", "ingested_from"),
            LineageEdge("transaction_stream", "transaction_bronze", "ingested_from"),
            LineageEdge("sales_bronze", "sales_silver", "transformed_from"),
            LineageEdge("customer_bronze", "customer_silver", "transformed_from"),
            LineageEdge("transaction_bronze", "transaction_silver", "transformed_from"),
            LineageEdge("sales_silver", "sales_metrics_gold", "aggregated_from"),
            LineageEdge("customer_silver", "customer_metrics_gold", "joined_with"),
            LineageEdge("transaction_silver", "customer_metrics_gold", "enriched_with"),
        ]
        
        for edge in edges:
            graph.add_edge(edge)
        
        logger.info(f"Lineage graph created with {len(nodes)} nodes and {len(edges)} relationships")
        
        # =====================================================================
        # 3. DATA QUALITY VALIDATION
        # =====================================================================
        logger.info("\n=== PHASE 3: DATA QUALITY VALIDATION ===")
        
        # Profile data
        logger.info("Profiling sales data...")
        sales_df = spark.read.parquet("/tmp/demo_bronze_sales")
        profiler = DataQualityProfiler(sales_df, "sales_bronze")
        profile = profiler.profile_data()
        
        logger.info(f"Sales data profile: {profile['total_rows']} rows, {profile['total_columns']} columns")
        
        # Validate data quality
        logger.info("Running quality validations...")
        validator = DataQualityValidator(sales_df, "sales_bronze")
        
        # Run multiple checks
        schema_check = validator.validate_schema(["sales_id", "amount", "customer_id"])
        null_check = validator.validate_no_nulls(["sales_id", "amount"])
        dup_check = validator.validate_no_duplicates(["sales_id"])
        
        logger.info(f"Schema validation: {'PASSED' if schema_check['passed'] else 'FAILED'}")
        logger.info(f"Null check: {'PASSED' if null_check['passed'] else 'FAILED'}")
        logger.info(f"Duplicate check: {'PASSED' if dup_check['passed'] else 'FAILED'}")
        
        # Run comprehensive validation
        all_validations = validator.run_all_validations()
        logger.info(f"Overall quality score: {all_validations['quality_score']}%")
        
        # =====================================================================
        # 4. DATA CATALOG REGISTRATION
        # =====================================================================
        logger.info("\n=== PHASE 4: DATA CATALOG MANAGEMENT ===")
        
        # Register datasets in catalog
        datasets_to_register = [
            DatasetBuilder("sales_bronze_prod")
                .name("Sales Bronze Layer")
                .description("Raw sales data as ingested from API")
                .owner("data-ingestion-team")
                .layer("bronze")
                .format("delta")
                .location("/data/bronze/sales")
                .tags(["sales", "bronze", "raw", "critical"])
                .record_count(sales_result['records_ingested'])
                .quality_score(85.0)
                .build(),
            
            DatasetBuilder("sales_silver_prod")
                .name("Sales Silver Layer")
                .description("Cleaned and deduplicated sales data")
                .owner("data-engineering-team")
                .layer("silver")
                .format("delta")
                .location("/data/silver/sales")
                .tags(["sales", "silver", "cleaned"])
                .record_count(sales_result['records_ingested'])
                .quality_score(92.5)
                .build(),
            
            DatasetBuilder("customer_silver_prod")
                .name("Customer Silver Layer")
                .description("Cleansed customer master data")
                .owner("data-engineering-team")
                .layer("silver")
                .format("delta")
                .location("/data/silver/customers")
                .tags(["customers", "silver", "master-data"])
                .record_count(customer_result['records_ingested'])
                .quality_score(95.0)
                .build(),
        ]
        
        for dataset_metadata in datasets_to_register:
            catalog.register_dataset(dataset_metadata)
            logger.info(f"Registered dataset: {dataset_metadata.dataset_id}")
        
        # Catalog statistics
        stats = catalog.get_catalog_stats()
        logger.info(f"Catalog stats: {stats['total_datasets']} datasets registered")
        logger.info(f"Datasets by layer: {stats['by_layer']}")
        
        # =====================================================================
        # 5. IMPACT ANALYSIS
        # =====================================================================
        logger.info("\n=== PHASE 5: IMPACT ANALYSIS ===")
        
        # Get impact of changes to sales_bronze
        impact = graph.get_impact_analysis("sales_bronze")
        
        logger.info(f"If sales_bronze changes:")
        logger.info(f"  Upstream dependencies: {len(impact['upstream_dependencies'].get('upstream', []))}")
        logger.info(f"  Downstream dependents: {len(impact['downstream_dependents'].get('downstream', []))}")
        
        # Get specific upstream/downstream
        upstream = graph.get_upstream_lineage("sales_silver")
        downstream = graph.get_downstream_lineage("sales_bronze")
        
        logger.info(f"Sales silver upstream sources: {len(upstream.get('upstream', []))}")
        logger.info(f"Sales bronze downstream consumers: {len(downstream.get('downstream', []))}")
        
        # =====================================================================
        # 6. MONITORING & ALERTS
        # =====================================================================
        logger.info("\n=== PHASE 6: MONITORING & ALERTS ===")
        
        # Get execution metrics
        executions = monitor.get_all_executions()
        logger.info(f"Total executions tracked: {len(executions)}")
        
        # Get metrics
        metrics = monitor.get_metrics()
        logger.info(f"Metrics collected: {len(metrics)}")
        
        # =====================================================================
        # 7. SEARCH & DISCOVERY
        # =====================================================================
        logger.info("\n=== PHASE 7: DATA DISCOVERY ===")
        
        # Search datasets
        search_results = catalog.search_datasets("sales", field="name")
        logger.info(f"Datasets matching 'sales': {len(search_results)}")
        
        # Search by tag
        critical_datasets = catalog.search_by_tag("critical")
        logger.info(f"Critical datasets: {len(critical_datasets)}")
        
        # Search by owner
        eng_datasets = catalog.search_by_owner("data-engineering-team")
        logger.info(f"Datasets owned by data-engineering-team: {len(eng_datasets)}")
        
        logger.info("\n=== DEMONSTRATION COMPLETE ===")
        logger.info("Platform is operational and all features demonstrated successfully!")
        
    except Exception as e:
        logger.error(f"Error during execution: {str(e)}")
        monitor.end_execution(
            execution_id=exec_id,
            status=PipelineStatus.FAILURE,
            error_message=str(e)
        )
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
