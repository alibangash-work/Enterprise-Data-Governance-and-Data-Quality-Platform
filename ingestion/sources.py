"""
Source-specific ingestion implementations.
"""

from config.logger import setup_logger
from ingestion.base import InMemoryIngestionSource
from pyspark.sql import SparkSession

logger = setup_logger(__name__)


class SalesSourceIngestion(InMemoryIngestionSource):
    """Simulated Sales data source"""
    
    @staticmethod
    def get_sample_data():
        """Generate sample sales data"""
        return [
            {
                "sales_id": 1,
                "customer_id": 101,
                "product_id": "P001",
                "quantity": 5,
                "amount": 500.00,
                "sale_date": "2024-01-15",
                "region": "North America"
            },
            {
                "sales_id": 2,
                "customer_id": 102,
                "product_id": "P002",
                "quantity": 3,
                "amount": 750.00,
                "sale_date": "2024-01-16",
                "region": "Europe"
            },
            {
                "sales_id": 3,
                "customer_id": 103,
                "product_id": "P001",
                "quantity": 2,
                "amount": 200.00,
                "sale_date": "2024-01-17",
                "region": "Asia"
            },
            {
                "sales_id": 4,
                "customer_id": 104,
                "product_id": "P003",
                "quantity": 10,
                "amount": 1000.00,
                "sale_date": "2024-01-18",
                "region": "North America"
            },
            {
                "sales_id": 5,
                "customer_id": 105,
                "product_id": "P002",
                "quantity": 1,
                "amount": 250.00,
                "sale_date": "2024-01-19",
                "region": "Europe"
            },
        ]
    
    def __init__(self, spark: SparkSession, target_path: str):
        data = self.get_sample_data()
        super().__init__(
            source_name="sales_source",
            spark=spark,
            target_path=target_path,
            data=data
        )


class CustomerSourceIngestion(InMemoryIngestionSource):
    """Simulated Customer data source"""
    
    @staticmethod
    def get_sample_data():
        """Generate sample customer data"""
        return [
            {
                "customer_id": 101,
                "name": "John Doe",
                "email": "john@example.com",
                "phone": "123-456-7890",
                "country": "USA",
                "created_date": "2023-01-10"
            },
            {
                "customer_id": 102,
                "name": "Jane Smith",
                "email": "jane@example.com",
                "phone": "098-765-4321",
                "country": "UK",
                "created_date": "2023-02-15"
            },
            {
                "customer_id": 103,
                "name": "Bob Johnson",
                "email": "bob@example.com",
                "phone": None,
                "country": "Canada",
                "created_date": "2023-03-20"
            },
            {
                "customer_id": 104,
                "name": "Alice Brown",
                "email": "alice@example.com",
                "phone": "555-1234",
                "country": "USA",
                "created_date": "2023-04-05"
            },
            {
                "customer_id": 105,
                "name": "Charlie Wilson",
                "email": None,
                "phone": "666-5678",
                "country": "Germany",
                "created_date": "2023-05-12"
            },
        ]
    
    def __init__(self, spark: SparkSession, target_path: str):
        data = self.get_sample_data()
        super().__init__(
            source_name="customer_source",
            spark=spark,
            target_path=target_path,
            data=data
        )


class TransactionSourceIngestion(InMemoryIngestionSource):
    """Simulated Transaction data source"""
    
    @staticmethod
    def get_sample_data():
        """Generate sample transaction data"""
        return [
            {
                "transaction_id": "TXN001",
                "customer_id": 101,
                "amount": 100.00,
                "status": "completed",
                "transaction_date": "2024-01-15T10:30:00",
                "channel": "online"
            },
            {
                "transaction_id": "TXN002",
                "customer_id": 102,
                "amount": 250.00,
                "status": "completed",
                "transaction_date": "2024-01-15T11:45:00",
                "channel": "mobile"
            },
            {
                "transaction_id": "TXN003",
                "customer_id": 103,
                "amount": 50.00,
                "status": "pending",
                "transaction_date": "2024-01-15T14:20:00",
                "channel": "in-store"
            },
            {
                "transaction_id": "TXN004",
                "customer_id": 101,
                "amount": 75.50,
                "status": "completed",
                "transaction_date": "2024-01-15T15:10:00",
                "channel": "online"
            },
            {
                "transaction_id": "TXN005",
                "customer_id": 104,
                "amount": 500.00,
                "status": "failed",
                "transaction_date": "2024-01-15T16:55:00",
                "channel": "api"
            },
        ]
    
    def __init__(self, spark: SparkSession, target_path: str):
        data = self.get_sample_data()
        super().__init__(
            source_name="transaction_source",
            spark=spark,
            target_path=target_path,
            data=data
        )
