"""
Example: API Client Usage
Demonstrates how to interact with the Data Governance Platform API
"""

import requests
import json
from typing import Dict, List, Any

class DataGovernanceAPIClient:
    """Client for Data Governance Platform API"""
    
    def __init__(self, base_url: str = "http://localhost:8000", username: str = "admin"):
        """
        Initialize API client.
        
        Args:
            base_url: Base URL of the API
            username: Username for authentication
        """
        self.base_url = base_url
        self.username = username
        self.session = requests.Session()
    
    def get_health(self) -> Dict[str, Any]:
        """Get system health status"""
        response = self.session.get(f"{self.base_url}/health")
        return response.json()
    
    def list_datasets(self, owner: str = None, layer: str = None) -> List[Dict]:
        """List all datasets with optional filters"""
        params = {"username": self.username}
        if owner:
            params["owner"] = owner
        if layer:
            params["layer"] = layer
        
        response = self.session.get(
            f"{self.base_url}/api/v1/datasets",
            params=params
        )
        return response.json()
    
    def get_dataset(self, dataset_id: str) -> Dict[str, Any]:
        """Get specific dataset metadata"""
        response = self.session.get(
            f"{self.base_url}/api/v1/datasets/{dataset_id}",
            params={"username": self.username}
        )
        return response.json()
    
    def search_datasets(self, query: str, field: str = "name") -> Dict[str, Any]:
        """Search datasets"""
        response = self.session.get(
            f"{self.base_url}/api/v1/datasets/search",
            params={
                "query": query,
                "field": field,
                "username": self.username
            }
        )
        return response.json()
    
    def get_lineage(self, dataset_id: str, direction: str = "both") -> Dict[str, Any]:
        """Get dataset lineage"""
        response = self.session.get(
            f"{self.base_url}/api/v1/lineage/{dataset_id}",
            params={
                "direction": direction,
                "username": self.username
            }
        )
        return response.json()
    
    def get_quality_score(self, dataset_id: str) -> Dict[str, Any]:
        """Get data quality score"""
        response = self.session.get(
            f"{self.base_url}/api/v1/quality/{dataset_id}",
            params={"username": self.username}
        )
        return response.json()
    
    def get_pipeline_status(self, pipeline_name: str) -> Dict[str, Any]:
        """Get pipeline execution status"""
        response = self.session.get(
            f"{self.base_url}/api/v1/pipelines/{pipeline_name}/status",
            params={"username": self.username}
        )
        return response.json()
    
    def list_executions(self, pipeline_name: str = None, status: str = None) -> Dict[str, Any]:
        """List pipeline executions"""
        params = {"username": self.username}
        if pipeline_name:
            params["pipeline_name"] = pipeline_name
        if status:
            params["status"] = status
        
        response = self.session.get(
            f"{self.base_url}/api/v1/executions",
            params=params
        )
        return response.json()
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get collected metrics"""
        response = self.session.get(
            f"{self.base_url}/api/v1/metrics",
            params={"username": self.username}
        )
        return response.json()


def demo_api_usage():
    """Demonstrate API client usage"""
    
    print("=" * 60)
    print("Data Governance Platform - API Client Demo")
    print("=" * 60)
    
    # Initialize client
    client = DataGovernanceAPIClient(
        base_url="http://localhost:8000",
        username="admin"
    )
    
    try:
        # 1. Check health
        print("\n1. Checking system health...")
        health = client.get_health()
        print(f"   Status: {health.get('status', 'unknown')}")
        print(f"   Components: {health.get('components', {})}")
        
        # 2. List datasets
        print("\n2. Listing all datasets...")
        datasets = client.list_datasets()
        if "datasets" in datasets:
            print(f"   Found {len(datasets['datasets'])} datasets")
            for ds in datasets["datasets"][:3]:
                print(f"   - {ds['name']} (Layer: {ds['layer']})")
        
        # 3. Search datasets
        print("\n3. Searching for 'sales' datasets...")
        search_results = client.search_datasets("sales", field="name")
        print(f"   Found {search_results.get('count', 0)} results")
        
        # 4. Get dataset lineage
        print("\n4. Getting lineage for first dataset...")
        if datasets.get("datasets"):
            dataset_id = datasets["datasets"][0]["dataset_id"]
            lineage = client.get_lineage(dataset_id)
            print(f"   Dataset: {dataset_id}")
            print(f"   Upstream dependencies: {len(lineage.get('upstream_dependencies', []))}")
            print(f"   Downstream dependents: {len(lineage.get('downstream_dependents', []))}")
        
        # 5. Get quality score
        print("\n5. Getting quality score...")
        if datasets.get("datasets"):
            dataset_id = datasets["datasets"][0]["dataset_id"]
            quality = client.get_quality_score(dataset_id)
            print(f"   Dataset: {dataset_id}")
            print(f"   Quality Score: {quality.get('quality_score', 'N/A')}%")
            print(f"   Validations: {len(quality.get('validations', []))}")
        
        # 6. Pipeline status
        print("\n6. Getting pipeline status...")
        pipeline_status = client.get_pipeline_status("data_ingestion_pipeline")
        print(f"   Pipeline: data_ingestion_pipeline")
        print(f"   Latest Status: {pipeline_status.get('latest_status', 'unknown')}")
        
        # 7. Executions
        print("\n7. Listing executions...")
        executions = client.list_executions()
        print(f"   Total executions: {executions.get('count', 0)}")
        
        # 8. Metrics
        print("\n8. Getting metrics...")
        metrics = client.get_metrics()
        print(f"   Total metrics collected: {metrics.get('count', 0)}")
        if metrics.get("metrics"):
            for metric in metrics["metrics"][:3]:
                print(f"   - {metric['metric_name']}: {metric['value']}")
        
        print("\n" + "=" * 60)
        print("API Demo completed successfully!")
        print("=" * 60)
        
    except requests.exceptions.ConnectionError:
        print("\nError: Could not connect to API at http://localhost:8000")
        print("Make sure the platform is running: docker-compose up")
    except Exception as e:
        print(f"\nError: {str(e)}")


if __name__ == "__main__":
    demo_api_usage()
