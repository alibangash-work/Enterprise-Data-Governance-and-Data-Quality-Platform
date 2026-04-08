"""
Unit tests for data lineage and catalog
"""

import pytest
from lineage.tracker import LineageGraph, LineageNode, LineageEdge, EntityType, AssetType
from catalog.metadata import DataCatalog, DatasetBuilder


class TestLineageGraph:
    """Tests for lineage graph tracking"""
    
    def test_create_graph(self):
        """Test graph creation"""
        graph = LineageGraph("sales_lineage")
        
        assert graph.graph_name == "sales_lineage"
        assert len(graph.nodes) == 0
        assert len(graph.edges) == 0
    
    def test_add_node(self):
        """Test adding nodes to graph"""
        graph = LineageGraph("test")
        node = LineageNode(
            entity_id="sales_bronze",
            entity_type=EntityType.DATASET,
            name="Sales Bronze"
        )
        graph.add_node(node)
        
        assert "sales_bronze" in graph.nodes
        assert graph.nodes["sales_bronze"].name == "Sales Bronze"
    
    def test_add_edge(self):
        """Test adding edges (relationships) to graph"""
        graph = LineageGraph("test")
        
        # Add nodes
        node1 = LineageNode("sales_bronze", EntityType.DATASET, "Sales Bronze")
        node2 = LineageNode("sales_silver", EntityType.DATASET, "Sales Silver")
        graph.add_node(node1)
        graph.add_node(node2)
        
        # Add edge
        edge = LineageEdge("sales_bronze", "sales_silver", "transformed")
        graph.add_edge(edge)
        
        assert len(graph.edges) == 1
        assert graph.edges[0].source_id == "sales_bronze"
    
    def test_upstream_lineage(self):
        """Test getting upstream lineage"""
        graph = LineageGraph("test")
        
        # Create chain: source -> transform1 -> transform2
        nodes = [
            LineageNode("source", EntityType.DATASET, "Source", AssetType.SOURCE),
            LineageNode("transform1", EntityType.DATASET, "Transform1", AssetType.TRANSFORM),
            LineageNode("transform2", EntityType.DATASET, "Transform2", AssetType.TRANSFORM),
        ]
        for node in nodes:
            graph.add_node(node)
        
        edges = [
            LineageEdge("source", "transform1", "input_to"),
            LineageEdge("transform1", "transform2", "input_to"),
        ]
        for edge in edges:
            graph.add_edge(edge)
        
        # Get upstream from transform2
        upstream = graph.get_upstream_lineage("transform2")
        
        assert upstream["entity_id"] == "transform2"
        assert len(upstream["upstream"]) > 0
    
    def test_downstream_lineage(self):
        """Test getting downstream lineage"""
        graph = LineageGraph("test")
        
        # Create chain
        nodes = [
            LineageNode("source", EntityType.DATASET, "Source", AssetType.SOURCE),
            LineageNode("sink1", EntityType.DATASET, "Sink1", AssetType.SINK),
            LineageNode("sink2", EntityType.DATASET, "Sink2", AssetType.SINK),
        ]
        for node in nodes:
            graph.add_node(node)
        
        edges = [
            LineageEdge("source", "sink1", "outputs_to"),
            LineageEdge("source", "sink2", "outputs_to"),
        ]
        for edge in edges:
            graph.add_edge(edge)
        
        # Get downstream from source
        downstream = graph.get_downstream_lineage("source")
        
        assert downstream["entity_id"] == "source"
        assert len(downstream["downstream"]) == 2


class TestDataCatalog:
    """Tests for data catalog"""
    
    def test_catalog_creation(self):
        """Test catalog creation"""
        catalog = DataCatalog()
        
        assert len(catalog.datasets) == 0
        assert catalog.created_at is not None
    
    def test_register_dataset(self):
        """Test registering dataset"""
        catalog = DataCatalog()
        
        metadata = (DatasetBuilder("sales_001")
                   .name("Sales Data")
                   .owner("data-team")
                   .layer("silver")
                   .format("delta")
                   .location("/data/silver/sales")
                   .build())
        
        catalog.register_dataset(metadata)
        
        assert "sales_001" in catalog.datasets
        assert catalog.datasets["sales_001"].name == "Sales Data"
    
    def test_get_dataset(self):
        """Test retrieving dataset"""
        catalog = DataCatalog()
        
        metadata = (DatasetBuilder("test_001")
                   .name("Test Dataset")
                   .owner("team")
                   .layer("gold")
                   .format("parquet")
                   .location("/data/test")
                   .build())
        
        catalog.register_dataset(metadata)
        retrieved = catalog.get_dataset("test_001")
        
        assert retrieved is not None
        assert retrieved.name == "Test Dataset"
    
    def test_search_by_name(self):
        """Test searching datasets by name"""
        catalog = DataCatalog()
        
        metadata1 = (DatasetBuilder("ds_001")
                    .name("Sales Silver")
                    .owner("team")
                    .layer("silver")
                    .format("delta")
                    .location("/data/silver/sales")
                    .build())
        
        metadata2 = (DatasetBuilder("ds_002")
                    .name("Customer Gold")
                    .owner("team")
                    .layer("gold")
                    .format("delta")
                    .location("/data/gold/customer")
                    .build())
        
        catalog.register_dataset(metadata1)
        catalog.register_dataset(metadata2)
        
        results = catalog.search_datasets("Sales", field="name")
        
        assert len(results) == 1
        assert results[0].name == "Sales Silver"
    
    def test_search_by_tag(self):
        """Test searching datasets by tag"""
        catalog = DataCatalog()
        
        metadata = (DatasetBuilder("ds_001")
                   .name("Sales")
                   .owner("team")
                   .layer("silver")
                   .format("delta")
                   .location("/data/silver")
                   .tags(["critical", "sales", "production"])
                   .build())
        
        catalog.register_dataset(metadata)
        results = catalog.search_by_tag("production")
        
        assert len(results) == 1
        assert "production" in results[0].tags
    
    def test_get_datasets_by_layer(self):
        """Test getting datasets by layer"""
        catalog = DataCatalog()
        
        # Register multiple datasets
        for i, layer in enumerate(["bronze", "silver", "gold"]):
            metadata = (DatasetBuilder(f"ds_{i}")
                       .name(f"Dataset {layer}")
                       .owner("team")
                       .layer(layer)
                       .format("delta")
                       .location(f"/data/{layer}")
                       .build())
            catalog.register_dataset(metadata)
        
        silver_datasets = catalog.get_datasets_by_layer("silver")
        
        assert len(silver_datasets) == 1
        assert silver_datasets[0].layer == "silver"
    
    def test_catalog_stats(self):
        """Test catalog statistics"""
        catalog = DataCatalog()
        
        for i in range(3):
            metadata = (DatasetBuilder(f"ds_{i}")
                       .name(f"Dataset {i}")
                       .owner("team1" if i < 2 else "team2")
                       .layer("silver")
                       .format("delta")
                       .location(f"/data/{i}")
                       .build())
            catalog.register_dataset(metadata)
        
        stats = catalog.get_catalog_stats()
        
        assert stats["total_datasets"] == 3
        assert "by_layer" in stats
        assert "by_owner" in stats
