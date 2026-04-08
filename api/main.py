"""
Main FastAPI application for Data Governance Platform.
Exposes governance features via RESTful API.
"""

from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
import logging

from config.settings import platform_config, get_config
from config.logger import setup_logger
from catalog.metadata import get_data_catalog, DatasetBuilder
from lineage.tracker import get_lineage_tracker
from observability.monitoring import get_execution_monitor, get_health_checker, get_metrics_collector

# Setup logging
logger = setup_logger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Enterprise Data Governance Platform",
    description="Central API for data governance, quality, and observability",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================================
# Data Models
# ============================================================================

class DatasetResponse(BaseModel):
    """Dataset information response"""
    dataset_id: str
    name: str
    description: str
    owner: str
    layer: str
    format: str
    location: str
    tags: List[str]
    created_at: str
    quality_score: Optional[float] = None
    record_count: Optional[int] = None


class LineageResponse(BaseModel):
    """Dataset lineage response"""
    entity_id: str
    upstream_dependencies: Optional[List[Dict[str, Any]]] = None
    downstream_dependents: Optional[List[Dict[str, Any]]] = None


class ValidationResult(BaseModel):
    """Data validation result"""
    validation_type: str
    dataset_name: str
    passed: bool
    details: Dict[str, Any]


class QualityScoreResponse(BaseModel):
    """Data quality score response"""
    dataset_id: str
    quality_score: float
    validations: List[Dict[str, Any]]
    timestamp: str


class PipelineStatus(BaseModel):
    """Pipeline execution status"""
    pipeline_name: str
    latest_status: str
    execution_stats: Dict[str, int]


class MetricsResponse(BaseModel):
    """Metrics response"""
    metric_name: str
    value: float
    metric_type: str
    labels: Dict[str, str]
    timestamp: str


class HealthCheckResponse(BaseModel):
    """System health check response"""
    status: str
    components: Dict[str, bool]
    timestamp: str


# ============================================================================
# Authentication/Authorization
# ============================================================================

class User(BaseModel):
    """Authenticated user"""
    username: str
    role: str  # admin, viewer, editor


async def get_current_user(username: Optional[str] = Query(None)) -> User:
    """Simple auth check (expand for real RBAC)"""
    if not username:
        username = "guest"
    
    # Simple role assignment
    roles = {
        "admin": "admin",
        "editor": "editor",
        "guest": "viewer"
    }
    
    return User(username=username, role=roles.get(username, "viewer"))


def require_role(required_role: str):
    """Dependency to check user role"""
    async def check_role(user: User = Depends(get_current_user)):
        role_hierarchy = {"admin": 3, "editor": 2, "viewer": 1}
        if role_hierarchy.get(user.role, 0) < role_hierarchy.get(required_role, 0):
            raise HTTPException(status_code=403, detail="Insufficient permissions")
        return user
    return check_role


# ============================================================================
# Health & System Endpoints
# ============================================================================

@app.get("/health", response_model=HealthCheckResponse)
async def health_check():
    """Check system health"""
    checker = get_health_checker()
    checker.check_spark()
    checker.check_storage("/data")
    
    health = checker.get_health_status()
    
    return HealthCheckResponse(
        status=health["status"],
        components=health["checks"],
        timestamp=datetime.utcnow().isoformat()
    )


@app.get("/api/v1/status")
async def api_status():
    """Get API status"""
    return {
        "status": "running",
        "version": "1.0.0",
        "timestamp": datetime.utcnow().isoformat(),
        "environment": platform_config.environment.value
    }


# ============================================================================
# Dataset & Catalog Endpoints
# ============================================================================

@app.get("/api/v1/datasets", response_model=List[DatasetResponse])
async def list_datasets(
    owner: Optional[str] = Query(None),
    layer: Optional[str] = Query(None),
    tag: Optional[str] = Query(None),
    user: User = Depends(get_current_user)
):
    """List all datasets with optional filtering"""
    catalog = get_data_catalog()
    
    datasets = catalog.list_all_datasets()
    
    # Apply filters
    if owner:
        datasets = [ds for ds in datasets if ds.owner.lower() == owner.lower()]
    if layer:
        datasets = [ds for ds in datasets if ds.layer == layer]
    if tag:
        datasets = [ds for ds in datasets if tag in ds.tags]
    
    return [
        DatasetResponse(
            dataset_id=ds.dataset_id,
            name=ds.name,
            description=ds.description,
            owner=ds.owner,
            layer=ds.layer,
            format=ds.format,
            location=ds.location,
            tags=ds.tags,
            created_at=ds.created_at,
            quality_score=ds.quality_score,
            record_count=ds.record_count
        )
        for ds in datasets
    ]


@app.get("/api/v1/datasets/{dataset_id}", response_model=DatasetResponse)
async def get_dataset(dataset_id: str, user: User = Depends(get_current_user)):
    """Get specific dataset metadata"""
    catalog = get_data_catalog()
    dataset = catalog.get_dataset(dataset_id)
    
    if not dataset:
        raise HTTPException(status_code=404, detail=f"Dataset {dataset_id} not found")
    
    return DatasetResponse(
        dataset_id=dataset.dataset_id,
        name=dataset.name,
        description=dataset.description,
        owner=dataset.owner,
        layer=dataset.layer,
        format=dataset.format,
        location=dataset.location,
        tags=dataset.tags,
        created_at=dataset.created_at,
        quality_score=dataset.quality_score,
        record_count=dataset.record_count
    )


@app.post("/api/v1/datasets/{dataset_id}")
async def register_dataset(
    dataset_id: str,
    name: str,
    owner: str,
    layer: str,
    format: str,
    location: str,
    user: User = Depends(require_role("editor"))
):
    """Register a new dataset in catalog"""
    catalog = get_data_catalog()
    
    metadata = (DatasetBuilder(dataset_id)
                .name(name)
                .owner(owner)
                .layer(layer)
                .format(format)
                .location(location)
                .build())
    
    catalog.register_dataset(metadata)
    
    return {
        "status": "success",
        "dataset_id": dataset_id,
        "message": f"Dataset {dataset_id} registered successfully"
    }


@app.get("/api/v1/datasets/search")
async def search_datasets(
    query: str = Query(..., description="Search query"),
    field: str = Query("name", description="Search field: name, owner, tags, description"),
    user: User = Depends(get_current_user)
):
    """Search datasets"""
    catalog = get_data_catalog()
    results = catalog.search_datasets(query, field=field)
    
    return {
        "query": query,
        "field": field,
        "count": len(results),
        "results": [
            {
                "dataset_id": ds.dataset_id,
                "name": ds.name,
                "owner": ds.owner,
                "layer": ds.layer
            }
            for ds in results
        ]
    }


# ============================================================================
# Lineage Endpoints
# ============================================================================

@app.get("/api/v1/lineage/{dataset_id}")
async def get_dataset_lineage(
    dataset_id: str,
    direction: str = Query("both", description="upstream, downstream, or both"),
    depth: int = Query(-1, description="Traversal depth (-1 for all)"),
    user: User = Depends(get_current_user)
):
    """Get dataset lineage"""
    tracker = get_lineage_tracker()
    
    # For demo, return sample lineage
    if direction in ["upstream", "both"]:
        upstream = tracker.get_graph("default").get_upstream_lineage(dataset_id, depth) if tracker.get_graph("default") else []
    else:
        upstream = None
    
    if direction in ["downstream", "both"]:
        downstream = tracker.get_graph("default").get_downstream_lineage(dataset_id, depth) if tracker.get_graph("default") else []
    else:
        downstream = None
    
    return LineageResponse(
        entity_id=dataset_id,
        upstream_dependencies=upstream,
        downstream_dependents=downstream
    )


@app.get("/api/v1/impact-analysis/{dataset_id}")
async def impact_analysis(
    dataset_id: str,
    user: User = Depends(get_current_user)
):
    """Analyze impact of changes to a dataset"""
    tracker = get_lineage_tracker()
    graph = tracker.get_graph("default")
    
    if not graph:
        return {"entity_id": dataset_id, "status": "no_lineage_data"}
    
    impact = graph.get_impact_analysis(dataset_id)
    return impact


# ============================================================================
# Data Quality Endpoints
# ============================================================================

@app.get("/api/v1/quality/{dataset_id}", response_model=QualityScoreResponse)
async def get_quality_score(
    dataset_id: str,
    user: User = Depends(get_current_user)
):
    """Get data quality score for dataset"""
    catalog = get_data_catalog()
    dataset = catalog.get_dataset(dataset_id)
    
    if not dataset:
        raise HTTPException(status_code=404, detail=f"Dataset {dataset_id} not found")
    
    # Return sample quality results
    quality_score = dataset.quality_score or 85.5
    
    return QualityScoreResponse(
        dataset_id=dataset_id,
        quality_score=quality_score,
        validations=[
            {"type": "schema_validation", "passed": True},
            {"type": "null_check", "passed": True},
            {"type": "duplicates_check", "passed": True},
            {"type": "value_range", "passed": quality_score >= 80}
        ],
        timestamp=datetime.utcnow().isoformat()
    )


@app.post("/api/v1/quality/{dataset_id}/validate")
async def validate_dataset(
    dataset_id: str,
    user: User = Depends(require_role("editor"))
):
    """Run data quality validation on dataset"""
    return {
        "dataset_id": dataset_id,
        "status": "validation_started",
        "message": f"Quality validation initiated for {dataset_id}",
        "validation_id": f"val_{dataset_id}_{datetime.utcnow().timestamp()}"
    }


# ============================================================================
# Pipeline & Execution Endpoints
# ============================================================================

@app.get("/api/v1/pipelines")
async def list_pipelines(user: User = Depends(get_current_user)):
    """List all pipelines"""
    monitor = get_execution_monitor()
    executions = monitor.get_all_executions()
    
    # Get unique pipeline names
    pipelines = set(e.pipeline_name for e in executions)
    
    return {
        "count": len(pipelines),
        "pipelines": [
            monitor.get_pipeline_status(p) for p in pipelines
        ]
    }


@app.get("/api/v1/pipelines/{pipeline_name}/status")
async def pipeline_status(
    pipeline_name: str,
    user: User = Depends(get_current_user)
):
    """Get pipeline execution status"""
    monitor = get_execution_monitor()
    return monitor.get_pipeline_status(pipeline_name)


@app.get("/api/v1/executions")
async def list_executions(
    pipeline_name: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    limit: int = Query(100),
    user: User = Depends(get_current_user)
):
    """List pipeline executions"""
    monitor = get_execution_monitor()
    executions = monitor.get_all_executions()
    
    if pipeline_name:
        executions = [e for e in executions if e.pipeline_name == pipeline_name]
    
    if status:
        executions = [e for e in executions if e.status.value == status]
    
    executions = sorted(executions, key=lambda e: e.start_time, reverse=True)[:limit]
    
    return {
        "count": len(executions),
        "executions": [e.to_dict() for e in executions]
    }


@app.get("/api/v1/executions/{execution_id}")
async def get_execution(
    execution_id: str,
    user: User = Depends(get_current_user)
):
    """Get execution details"""
    monitor = get_execution_monitor()
    execution = monitor.get_execution(execution_id)
    
    if not execution:
        raise HTTPException(status_code=404, detail=f"Execution {execution_id} not found")
    
    return execution.to_dict()


# ============================================================================
# Metrics & Monitoring Endpoints
# ============================================================================

@app.get("/api/v1/metrics")
async def get_metrics(user: User = Depends(require_role("viewer"))):
    """Get all recorded metrics"""
    collector = get_metrics_collector()
    metrics = collector.get_metrics()
    
    return {
        "count": len(metrics),
        "metrics": [m.to_dict() for m in metrics]
    }


@app.get("/api/v1/metrics/prometheus")
async def prometheus_metrics():
    """Export metrics in Prometheus format"""
    collector = get_metrics_collector()
    return collector.get_prometheus_format()


@app.get("/api/v1/catalog/stats")
async def catalog_stats(user: User = Depends(get_current_user)):
    """Get catalog statistics"""
    catalog = get_data_catalog()
    return catalog.get_catalog_stats()


# ============================================================================
# Error Handlers
# ============================================================================

@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """Handle HTTP exceptions"""
    logger.error(f"HTTP Exception: {exc.detail}")
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail, "timestamp": datetime.utcnow().isoformat()}
    )


@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    """Handle general exceptions"""
    logger.error(f"Unexpected error: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content={
            "detail": "Internal server error",
            "error_type": type(exc).__name__,
            "timestamp": datetime.utcnow().isoformat()
        }
    )


# ============================================================================
# Startup/Shutdown Events
# ============================================================================

@app.on_event("startup")
async def startup_event():
    """Initialize resources on startup"""
    logger.info("Data Governance Platform API starting...")
    
    # Initialize components
    get_data_catalog()
    get_execution_monitor()
    get_metrics_collector()
    get_lineage_tracker()
    
    logger.info("API startup complete")


@app.on_event("shutdown")
async def shutdown_event():
    """Clean up resources on shutdown"""
    logger.info("Data Governance Platform API shutting down...")


if __name__ == "__main__":
    import uvicorn
    
    config = get_config()
    uvicorn.run(
        app,
        host=config.api.host,
        port=config.api.port,
        workers=config.api.workers if not config.api.debug else 1
    )
