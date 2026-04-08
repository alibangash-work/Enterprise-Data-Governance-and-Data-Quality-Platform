"""
Data lineage tracking and management.
Tracks data flow from source to final datasets.
"""

from typing import Dict, Any, List, Optional, Set, Tuple
from datetime import datetime
from dataclasses import dataclass, asdict
from enum import Enum
from config.logger import setup_logger
import json

logger = setup_logger(__name__)


class EntityType(Enum):
    """Types of lineage entities"""
    DATASET = "dataset"
    PROCESS = "process"
    COLUMN = "column"


class AssetType(Enum):
    """Types of data assets"""
    SOURCE = "source"
    STAGE = "stage"
    TRANSFORM = "transform"
    SINK = "sink"


@dataclass
class LineageNode:
    """Represents a node in the lineage graph"""
    entity_id: str
    entity_type: EntityType
    name: str
    asset_type: Optional[AssetType] = None
    properties: Dict[str, Any] = None
    created_at: str = None
    
    def __post_init__(self):
        if self.properties is None:
            self.properties = {}
        if self.created_at is None:
            self.created_at = datetime.utcnow().isoformat()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        data = asdict(self)
        data["entity_type"] = self.entity_type.value
        if self.asset_type:
            data["asset_type"] = self.asset_type.value
        return data


@dataclass
class LineageEdge:
    """Represents an edge (dependency) in the lineage graph"""
    source_id: str
    target_id: str
    relationship_type: str
    created_at: str = None
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.utcnow().isoformat()
        if self.metadata is None:
            self.metadata = {}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)


class LineageGraph:
    """Manages lineage information as a directed acyclic graph"""
    
    def __init__(self, graph_name: str):
        """
        Initialize lineage graph.
        
        Args:
            graph_name: Name of the lineage graph
        """
        self.graph_name = graph_name
        self.nodes: Dict[str, LineageNode] = {}
        self.edges: List[LineageEdge] = []
        self.created_at = datetime.utcnow()
    
    def add_node(self, node: LineageNode) -> None:
        """Add a node to the graph"""
        self.nodes[node.entity_id] = node
        logger.debug(f"Added node: {node.entity_id} ({node.name})")
    
    def add_edge(self, edge: LineageEdge) -> None:
        """Add an edge (dependency) to the graph"""
        if edge.source_id not in self.nodes:
            logger.warning(f"Source node {edge.source_id} not found in graph")
        if edge.target_id not in self.nodes:
            logger.warning(f"Target node {edge.target_id} not found in graph")
        
        self.edges.append(edge)
        logger.debug(f"Added edge: {edge.source_id} -> {edge.target_id} ({edge.relationship_type})")
    
    def get_upstream_lineage(self, entity_id: str, depth: int = -1) -> Dict[str, Any]:
        """
        Get upstream lineage for an entity (inputs/dependencies).
        
        Args:
            entity_id: ID of the entity
            depth: Traversal depth (-1 for all)
            
        Returns:
            Dictionary with upstream entities
        """
        upstream = {
            "entity_id": entity_id,
            "upstream": [],
            "depth": depth
        }
        
        visited = set()
        self._traverse_upstream(entity_id, upstream["upstream"], visited, depth)
        
        return upstream
    
    def get_downstream_lineage(self, entity_id: str, depth: int = -1) -> Dict[str, Any]:
        """
        Get downstream lineage for an entity (outputs/dependents).
        
        Args:
            entity_id: ID of the entity
            depth: Traversal depth (-1 for all)
            
        Returns:
            Dictionary with downstream entities
        """
        downstream = {
            "entity_id": entity_id,
            "downstream": [],
            "depth": depth
        }
        
        visited = set()
        self._traverse_downstream(entity_id, downstream["downstream"], visited, depth)
        
        return downstream
    
    def _traverse_upstream(self, entity_id: str, result: List, visited: Set[str], depth: int) -> None:
        """Recursively traverse upstream dependencies"""
        if depth == 0 or entity_id in visited:
            return
        
        visited.add(entity_id)
        
        # Find edges where target is entity_id (dependencies)
        for edge in self.edges:
            if edge.target_id == entity_id:
                source_node = self.nodes.get(edge.source_id)
                if source_node:
                    result.append({
                        "entity_id": source_node.entity_id,
                        "name": source_node.name,
                        "entity_type": source_node.entity_type.value,
                        "relationship": edge.relationship_type,
                        "node": source_node.to_dict()
                    })
                    
                    # Recurse
                    self._traverse_upstream(
                        edge.source_id, 
                        result, 
                        visited, 
                        depth - 1 if depth > 0 else -1
                    )
    
    def _traverse_downstream(self, entity_id: str, result: List, visited: Set[str], depth: int) -> None:
        """Recursively traverse downstream dependencies"""
        if depth == 0 or entity_id in visited:
            return
        
        visited.add(entity_id)
        
        # Find edges where source is entity_id
        for edge in self.edges:
            if edge.source_id == entity_id:
                target_node = self.nodes.get(edge.target_id)
                if target_node:
                    result.append({
                        "entity_id": target_node.entity_id,
                        "name": target_node.name,
                        "entity_type": target_node.entity_type.value,
                        "relationship": edge.relationship_type,
                        "node": target_node.to_dict()
                    })
                    
                    # Recurse
                    self._traverse_downstream(
                        edge.target_id,
                        result,
                        visited,
                        depth - 1 if depth > 0 else -1
                    )
    
    def get_impact_analysis(self, entity_id: str) -> Dict[str, Any]:
        """
        Analyze impact of changes to an entity.
        
        Returns:
            Dictionary with upstream/downstream impacts
        """
        return {
            "entity_id": entity_id,
            "upstream_dependencies": self.get_upstream_lineage(entity_id),
            "downstream_dependents": self.get_downstream_lineage(entity_id)
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert graph to dictionary"""
        return {
            "graph_name": self.graph_name,
            "created_at": self.created_at.isoformat(),
            "nodes": [node.to_dict() for node in self.nodes.values()],
            "edges": [edge.to_dict() for edge in self.edges]
        }
    
    def to_json(self) -> str:
        """Convert graph to JSON"""
        return json.dumps(self.to_dict(), indent=2)


class LineageTracker:
    """Track and manage data lineage"""
    
    def __init__(self):
        """Initialize lineage tracker"""
        self.graphs: Dict[str, LineageGraph] = {}
        self.lineage_events: List[Dict[str, Any]] = []
    
    def create_graph(self, graph_name: str) -> LineageGraph:
        """Create a new lineage graph"""
        graph = LineageGraph(graph_name)
        self.graphs[graph_name] = graph
        logger.info(f"Created lineage graph: {graph_name}")
        return graph
    
    def record_lineage_event(
        self,
        graph_name: str,
        source_entity: str,
        target_entity: str,
        relationship_type: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Record a lineage event.
        
        Args:
            graph_name: Name of the graph
            source_entity: Source entity ID
            target_entity: Target entity ID
            relationship_type: Type of relationship
            metadata: Additional metadata
        """
        event = {
            "timestamp": datetime.utcnow().isoformat(),
            "graph_name": graph_name,
            "source": source_entity,
            "target": target_entity,
            "relationship": relationship_type,
            "metadata": metadata or {}
        }
        
        self.lineage_events.append(event)
        logger.debug(f"Recorded lineage event: {source_entity} -> {target_entity}")
    
    def get_graph(self, graph_name: str) -> Optional[LineageGraph]:
        """Get a lineage graph"""
        return self.graphs.get(graph_name)
    
    def list_graphs(self) -> List[str]:
        """List all graphs"""
        return list(self.graphs.keys())
    
    def export_lineage(self, graph_name: str, format: str = "json") -> str:
        """Export lineage in specified format"""
        graph = self.get_graph(graph_name)
        if not graph:
            logger.error(f"Graph not found: {graph_name}")
            return ""
        
        if format == "json":
            return graph.to_json()
        elif format == "dict":
            return str(graph.to_dict())
        else:
            logger.error(f"Unsupported format: {format}")
            return ""


# Global lineage tracker instance
_lineage_tracker = None


def get_lineage_tracker() -> LineageTracker:
    """Get or create the global lineage tracker"""
    global _lineage_tracker
    if _lineage_tracker is None:
        _lineage_tracker = LineageTracker()
    return _lineage_tracker
