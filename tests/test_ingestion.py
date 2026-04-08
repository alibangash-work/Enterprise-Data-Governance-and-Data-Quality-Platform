"""
Unit tests for ingestion module
"""

import pytest
from pyspark.sql import SparkSession
from ingestion.sources import SalesSourceIngestion, CustomerSourceIngestion, TransactionSourceIngestion


@pytest.fixture
def spark():
    """Create a Spark session for testing"""
    return SparkSession.builder \
        .appName("test-ingestion") \
        .master("local[1]") \
        .getOrCreate()


class TestSalesIngestion:
    """Tests for sales data ingestion"""
    
    def test_sales_ingestion(self, spark):
        """Test sales source data ingestion"""
        ingestion = SalesSourceIngestion(spark, "/tmp/test_sales")
        result = ingestion.ingest()
        
        assert result["status"] == "success"
        assert "records_ingested" in result
        assert result["source"] == "sales_source"
        assert result["records_ingested"] > 0
    
    def test_sales_data_structure(self, spark):
        """Test sales data has correct structure"""
        ingestion = SalesSourceIngestion(spark, "/tmp/test_sales")
        
        # Get raw data
        data = ingestion.get_sample_data()
        
        assert len(data) > 0
        assert "sales_id" in data[0]
        assert "customer_id" in data[0]
        assert "amount" in data[0]


class TestCustomerIngestion:
    """Tests for customer data ingestion"""
    
    def test_customer_ingestion(self, spark):
        """Test customer source data ingestion"""
        ingestion = CustomerSourceIngestion(spark, "/tmp/test_customers")
        result = ingestion.ingest()
        
        assert result["status"] == "success"
        assert result["source"] == "customer_source"
        assert result["records_ingested"] > 0
    
    def test_customer_nullable_fields(self, spark):
        """Test customer data handles null values"""
        ingestion = CustomerSourceIngestion(spark, "/tmp/test_customers")
        data = ingestion.get_sample_data()
        
        # Verify some fields can be null
        has_null_phone = any(record.get("phone") is None for record in data)
        assert has_null_phone  # Expected in test data


class TestTransactionIngestion:
    """Tests for transaction data ingestion"""
    
    def test_transaction_ingestion(self, spark):
        """Test transaction source data ingestion"""
        ingestion = TransactionSourceIngestion(spark, "/tmp/test_transactions")
        result = ingestion.ingest()
        
        assert result["status"] == "success"
        assert result["source"] == "transaction_source"
        assert "records_ingested" in result
