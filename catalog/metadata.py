"""
Data catalog for metadata management.
Stores and manages dataset metadata.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
from dataclasses import dataclass, asdict
from config.logger import setup_logger
import json

logger = setup_logger(__name__)


@dataclass
class DatasetMetadata:
    """Metadata for a dataset"""
    dataset_id: str
    name: str
    description: str
    owner: str
    layer: str  # bronze, silver, gold
    format: str
    location: str
    created_at: str
    updated_at: str
    schema: Dict[str, str]  # column: type mapping
    tags: List[str]
    properties: Dict[str, Any]
    quality_score: Optional[float] = None
    record_count: Optional[int] = None
    size_bytes: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)
    
    def to_json(self) -> str:
        """Convert to JSON"""
        return json.dumps(self.to_dict())


class DataCatalog:
    """Central catalog for all dataset metadata"""
    
    def __init__(self):
        """Initialize data catalog"""
        self.datasets: Dict[str, DatasetMetadata] = {}
        self.created_at = datetime.utcnow()
    
    def register_dataset(self, metadata: DatasetMetadata) -> None:
        """Register a dataset in the catalog"""
        try:
            self.datasets[metadata.dataset_id] = metadata
            logger.info(f"Registered dataset: {metadata.dataset_id} ({metadata.name})")
        except Exception as e:
            logger.error(f"Failed to register dataset {metadata.dataset_id}: {str(e)}")
    
    def get_dataset(self, dataset_id: str) -> Optional[DatasetMetadata]:
        """Get dataset metadata"""
        return self.datasets.get(dataset_id)
    
    def update_dataset(self, dataset_id: str, **updates) -> Optional[DatasetMetadata]:
        """Update dataset metadata"""
        dataset = self.datasets.get(dataset_id)
        if not dataset:
            logger.warning(f"Dataset not found: {dataset_id}")
            return None
        
        # Update fields
        for key, value in updates.items():
            if hasattr(dataset, key):
                setattr(dataset, key, value)
        
        dataset.updated_at = datetime.utcnow().isoformat()
        logger.info(f"Updated dataset: {dataset_id}")
        return dataset
    
    def search_datasets(self, query: str, field: str = "name") -> List[DatasetMetadata]:
        """Search datasets by query"""
        results = []
        query_lower = query.lower()
        
        for dataset in self.datasets.values():
            if field == "name":
                if query_lower in dataset.name.lower():
                    results.append(dataset)
            elif field == "owner":
                if query_lower in dataset.owner.lower():
                    results.append(dataset)
            elif field == "tags":
                if any(query_lower in tag.lower() for tag in dataset.tags):
                    results.append(dataset)
            elif field == "description":
                if query_lower in dataset.description.lower():
                    results.append(dataset)
        
        logger.info(f"Found {len(results)} datasets matching query: {query}")
        return results
    
    def search_by_tag(self, tag: str) -> List[DatasetMetadata]:
        """Search datasets by tag"""
        return self.search_datasets(tag, field="tags")
    
    def search_by_owner(self, owner: str) -> List[DatasetMetadata]:
        """Search datasets by owner"""
        return self.search_datasets(owner, field="owner")
    
    def get_datasets_by_layer(self, layer: str) -> List[DatasetMetadata]:
        """Get all datasets in a specific layer"""
        return [ds for ds in self.datasets.values() if ds.layer == layer]
    
    def list_all_datasets(self) -> List[DatasetMetadata]:
        """List all registered datasets"""
        return list(self.datasets.values())
    
    def get_catalog_stats(self) -> Dict[str, Any]:
        """Get catalog statistics"""
        total_datasets = len(self.datasets)
        
        layers = {}
        owners = {}
        formats = {}
        
        for dataset in self.datasets.values():
            # Count by layer
            layers[dataset.layer] = layers.get(dataset.layer, 0) + 1
            
            # Count by owner
            owners[dataset.owner] = owners.get(dataset.owner, 0) + 1
            
            # Count by format
            formats[dataset.format] = formats.get(dataset.format, 0) + 1
        
        return {
            "total_datasets": total_datasets,
            "by_layer": layers,
            "by_owner": owners,
            "by_format": formats,
            "created_at": self.created_at.isoformat()
        }
    
    def export_catalog(self, format: str = "json") -> str:
        """Export catalog in specified format"""
        data = {
            "total_datasets": len(self.datasets),
            "datasets": [ds.to_dict() for ds in self.datasets.values()],
            "stats": self.get_catalog_stats()
        }
        
        if format == "json":
            return json.dumps(data, indent=2)
        elif format == "csv":
            return self._export_as_csv()
        else:
            logger.error(f"Unsupported export format: {format}")
            return ""
    
    def _export_as_csv(self) -> str:
        """Export catalog as CSV"""
        if not self.datasets:
            return "dataset_id,name,owner,layer,format,location\n"
        
        lines = ["dataset_id,name,owner,layer,format,location"]
        for dataset in self.datasets.values():
            line = f"{dataset.dataset_id},{dataset.name},{dataset.owner},{dataset.layer},{dataset.format},{dataset.location}"
            lines.append(line)
        
        return "\n".join(lines)


# Global catalog instance
_data_catalog = None


def get_data_catalog() -> DataCatalog:
    """Get or create the global data catalog"""
    global _data_catalog
    if _data_catalog is None:
        _data_catalog = DataCatalog()
    return _data_catalog


class DatasetBuilder:
    """Builder pattern for creating dataset metadata"""
    
    def __init__(self, dataset_id: str):
        """Initialize builder"""
        self.data = {
            "dataset_id": dataset_id,
            "name": "",
            "description": "",
            "owner": "",
            "layer": "",
            "format": "",
            "location": "",
            "created_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
            "schema": {},
            "tags": [],
            "properties": {}
        }
    
    def name(self, name: str) -> "DatasetBuilder":
        self.data["name"] = name
        return self
    
    def description(self, description: str) -> "DatasetBuilder":
        self.data["description"] = description
        return self
    
    def owner(self, owner: str) -> "DatasetBuilder":
        self.data["owner"] = owner
        return self
    
    def layer(self, layer: str) -> "DatasetBuilder":
        self.data["layer"] = layer
        return self
    
    def format(self, format: str) -> "DatasetBuilder":
        self.data["format"] = format
        return self
    
    def location(self, location: str) -> "DatasetBuilder":
        self.data["location"] = location
        return self
    
    def schema(self, schema: Dict[str, str]) -> "DatasetBuilder":
        self.data["schema"] = schema
        return self
    
    def tags(self, tags: List[str]) -> "DatasetBuilder":
        self.data["tags"] = tags
        return self
    
    def add_tag(self, tag: str) -> "DatasetBuilder":
        self.data["tags"].append(tag)
        return self
    
    def properties(self, properties: Dict[str, Any]) -> "DatasetBuilder":
        self.data["properties"] = properties
        return self
    
    def quality_score(self, score: float) -> "DatasetBuilder":
        self.data["quality_score"] = score
        return self
    
    def record_count(self, count: int) -> "DatasetBuilder":
        self.data["record_count"] = count
        return self
    
    def size_bytes(self, size: int) -> "DatasetBuilder":
        self.data["size_bytes"] = size
        return self
    
    def build(self) -> DatasetMetadata:
        """Build the metadata object"""
        return DatasetMetadata(**self.data)
