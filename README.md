# Enterprise Data Governance Platform with Data Quality, Lineage, and Observability

## Overview

The **Enterprise Data Governance Platform** is a production-grade, end-to-end data governance system designed to ensure data quality, track lineage, provide observability, and expose governance features via APIs. It implements a modern data lakehouse architecture with governance layers on top.

This platform is built with modern, production-ready technologies and follows industry best practices for:
- ✅ **Data Quality Management**: Automated validation and profiling using Great Expectations
- ✅ **Data Lineage Tracking**: Complete visibility of data flow from source to usage
- ✅ **Observability & Monitoring**: Real-time metrics and alerting with Prometheus/Grafana
- ✅ **Data Cataloging**: Searchable metadata registry with ownership and tagging
- ✅ **API-First Architecture**: RESTful governance APIs with RBAC
- ✅ **Orchestration**: Apache Airflow DAGs for automated pipelines
- ✅ **Scalability**: PySpark-based processing with distributed execution

---

## Table of Contents

1. [Architecture](#architecture)
2. [Tech Stack](#tech-stack)
3. [Project Structure](#project-structure)
4. [Prerequisites](#prerequisites)
5. [Installation & Setup](#installation--setup)
6. [Configuration](#configuration)
7. [Running the Platform](#running-the-platform)
8. [API Usage](#api-usage)
9. [Features & Examples](#features--examples)
10. [Monitoring & Observability](#monitoring--observability)
11. [Development & Testing](#development--testing)
12. [Troubleshooting](#troubleshooting)
13. [Production Deployment](#production-deployment)
14. [Contributing](#contributing)

---

## Architecture

The platform implements a **three-layer data lakehouse** with governance overlays:

```
Data Sources → Bronze Layer (Raw) → Quality Checks → Silver Layer (Cleaned)
    ↓                                                        ↓
Data Ingestion                                    Transformation
    ↓                                                        ↓
Lineage Tracking ←────── Data Catalog ←──────→ Gold Layer (Business Views)
    ↓                          ↓                            ↓
API Layer ←──→ Authentication ←──→ Observability System
```

**Key Layers:**

1. **Bronze Layer**: Raw data as ingested (immutable)
2. **Silver Layer**: Cleansed and validated data
3. **Gold Layer**: Aggregated, business-ready datasets
4. **Governance**: Quality, Lineage, Catalog, Observability

See [architecture-diagram/ARCHITECTURE.md](architecture-diagram/ARCHITECTURE.md) for detailed architecture diagrams.

---

## Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Core Processing** | PySpark 3.5.0 | Distributed data processing |
| **Storage** | Delta Lake / Parquet | Data lake with ACID compliance |
| **Quality Framework** | Great Expectations | Data validation & profiling |
| **Orchestration** | Apache Airflow 2.7 | Workflow scheduling & execution |
| **APIs** | FastAPI 0.104 | RESTful governance API |
| **Monitoring** | Prometheus + Grafana | Metrics & visualization |
| **Database** | PostgreSQL 14 | Metadata & Airflow backend |
| **Message Queue** | Redis 7 | Airflow task queue |
| **Containerization** | Docker & Docker Compose | Reproducible deployment |
| **Language** | Python 3.11 | All application code |

---

## Project Structure

```
data-governance-platform/
├── ingestion/                      # Data ingestion module
│   ├── __init__.py
│   ├── base.py                     # Abstract ingestion classes
│   └── sources.py                  # Source implementations (CSV, JSON, etc.)
├── processing/                     # Data transformation module  
│   ├── __init__.py
│   └── transformers.py             # Bronze→Silver→Gold transformations
├── data_quality/                   # Quality framework
│   ├── __init__.py
│   └── validators.py               # Great Expectations validators
├── lineage/                        # Data lineage tracking
│   ├── __init__.py
│   └── tracker.py                  # Lineage graph implementation
├── catalog/                        # Data catalog & metadata
│   ├── __init__.py
│   └── metadata.py                 # Metadata registry & search
├── observability/                  # Monitoring & metrics
│   ├── __init__.py
│   └── monitoring.py               # Prometheus metrics & health checks
├── api/                            # FastAPI application
│   ├── __init__.py
│   └── main.py                     # API endpoints with RBAC
├── airflow/                        # Orchestration DAGs
│   ├── __init__.py
│   └── dags.py                     # Airflow pipeline definitions
├── config/                         # Configuration management
│   ├── __init__.py
│   ├── settings.py                 # Environment-based config
│   ├── logger.py                   # Logging setup
│   └── constants.py                # Application constants
├── tests/                          # Unit & integration tests
├── dashboards/                     # Prometheus & Grafana configs
│   ├── prometheus.yml              # Prometheus scrape config
│   └── grafana-dashboard.json      # Grafana dashboard
├── architecture-diagram/           # Architecture documentation
│   └── ARCHITECTURE.md             # Detailed architecture
├── docker-compose.yml              # Docker Compose setup
├── Dockerfile                      # Application container
├── requirements.txt                # Python dependencies
├── .env.example                    # Environment template
├── README.md                       # This file
└── setup.py                        # Package installation (optional)
```

---

## Prerequisites

### System Requirements
- **OS**: Linux, macOS, or Windows (with WSL2)
- **Memory**: 8GB RAM minimum (16GB recommended)
- **Storage**: 20GB free space for data
- **Docker**: 20.10+
- **Docker Compose**: 2.0+

### Software Requirements
- Python 3.11+
- Java 11+ (for Spark)
- Git

### Optional
- AWS CLI (for S3 integration)
- Azure CLI (for ADLS integration)

---

## Installation & Setup

### 1. Clone Repository

```bash
cd /path/to/data-governance-platform
git clone https://github.com/yourusername/data-governance-platform.git
cd data-governance-platform
```

### 2. Create Environment File

```bash
cp .env.example .env
# Edit .env with your configuration
```

### 3. Build Docker Images

```bash
docker-compose build
```

### 4. Start Services

```bash
# Start all services
docker-compose up -d

# Check service health
docker-compose ps

# View logs
docker-compose logs -f api
```

### 5. Verify Installation

```bash
# Test API health
curl http://localhost:8000/health

# Access Airflow UI
open http://localhost:8080

# Access Grafana
open http://localhost:3000
```

---

## Configuration

### Environment Variables

Key configuration variables in `.env`:

```bash
# Environment
ENVIRONMENT=development          # local, staging, production
LOG_LEVEL=INFO

# API
API_HOST=0.0.0.0
API_PORT=8000
API_WORKERS=4

# Data Paths
BRONZE_PATH=/data/bronze
SILVER_PATH=/data/silver
GOLD_PATH=/data/gold

# Spark
SPARK_MASTER=local[4]
SPARK_EXECUTOR_MEMORY=2g
SPARK_DRIVER_MEMORY=2g

# AWS (optional)
AWS_REGION=us-east-1
S3_BUCKET=data-governance-bucket

# Monitoring
PROMETHEUS_ENDPOINT=http://prometheus:9090
GRAFANA_ENDPOINT=http://grafana:3000

# Features
ENABLE_LINEAGE=true
ENABLE_QUALITY_CHECKS=true
ENABLE_MONITORING=true
```

### Spark Configuration

Modify `config/settings.py` for production settings:

```python
spark_config = SparkConfig(
    app_name="DataGovernancePlatform",
    master="spark://spark-master:7077",  # Cluster mode
    executor_memory="4g",
    driver_memory="4g",
    shuffle_partitions=400
)
```

---

## Running the Platform

### Start All Services

```bash
# Development mode with hot-reload
docker-compose up

# Detached mode (background)
docker-compose up -d

# Specific services only
docker-compose up api prometheus grafana
```

### Run Manual Data Ingestion

```python
from pyspark.sql import SparkSession
from ingestion.sources import SalesSourceIngestion

spark = SparkSession.builder.appName("manual-ingest").getOrCreate()
ingestion = SalesSourceIngestion(spark, "/data/bronze/sales")
result = ingestion.ingest()
print(result)
```

### View Logs

```bash
# API logs
docker-compose logs -f api

# Airflow scheduler
docker-compose logs -f airflow-scheduler

# All services
docker-compose logs -f
```

### Stop Services

```bash
# Stop and remove containers
docker-compose down

# Also remove volumes (caution: deletes data)
docker-compose down -v
```

---

## API Usage

### Base URL
```
http://localhost:8000
```

### Authentication Token
```
X-Token: admin  # or editor, viewer
```

### Key Endpoints

#### 1. List Datasets
```bash
curl -X GET "http://localhost:8000/api/v1/datasets" \
  -H "Authorization: Bearer token"
```

**Response:**
```json
{
  "count": 3,
  "datasets": [
    {
      "dataset_id": "sales_silver",
      "name": "Sales Silver Layer",
      "owner": "data-team",
      "layer": "silver",
      "quality_score": 92.5
    }
  ]
}
```

#### 2. Search Datasets
```bash
curl -X GET "http://localhost:8000/api/v1/datasets/search?query=sales&field=name"
```

#### 3. Get Dataset Lineage
```bash
curl -X GET "http://localhost:8000/api/v1/lineage/sales_silver?direction=both&depth=2"
```

**Response:**
```json
{
  "entity_id": "sales_silver",
  "upstream_dependencies": [
    {
      "entity_id": "sales_bronze",
      "name": "Sales Bronze Layer",
      "relationship": "transformed_from"
    }
  ],
  "downstream_dependents": [
    {
      "entity_id": "sales_aggregation_gold",
      "name": "Sales Aggregation",
      "relationship": "aggregated_from"
    }
  ]
}
```

#### 4. Get Data Quality Score
```bash
curl -X GET "http://localhost:8000/api/v1/quality/sales_silver"
```

**Response:**
```json
{
  "dataset_id": "sales_silver",
  "quality_score": 92.5,
  "validations": [
    {"type": "schema_validation", "passed": true},
    {"type": "null_check", "passed": true},
    {"type": "duplicates_check", "passed": true}
  ]
}
```

#### 5. Pipeline Status
```bash
curl -X GET "http://localhost:8000/api/v1/pipelines/data_ingestion_pipeline/status"
```

#### 6. Metrics (Prometheus Format)
```bash
curl -X GET "http://localhost:8000/api/v1/metrics/prometheus"
```

#### 7. System Health
```bash
curl -X GET "http://localhost:8000/health"
```

**Response:**
```json
{
  "status": "healthy",
  "components": {
    "spark": true,
    "storage": true
  }
}
```

---

## Features & Examples

### 1. Data Ingestion

**Multiple Source Types:**
```python
# CSV Source
from ingestion.base import CSVIngestionSource
csv_ingest = CSVIngestionSource(
    source_name="sales_csv",
    spark=spark,
    target_path="/data/bronze/sales",
    file_path="data/sales.csv"
)

# In-Memory Source (Testing)
from ingestion.sources import SalesSourceIngestion
sales_ingest = SalesSourceIngestion(spark, "/data/bronze/sales")

# Execute ingestion
result = sales_ingest.ingest()
```

### 2. Data Quality Validation

```python
from data_quality.validators import DataQualityValidator, DataQualityProfiler

df = spark.read.parquet("/data/silver/sales")

# Profile data
profiler = DataQualityProfiler(df, "sales_silver")
profile = profiler.profile_data()

# Validate
validator = DataQualityValidator(df, "sales_silver")

# Run individual checks
schema_check = validator.validate_schema(["sales_id", "amount", "date"])
null_check = validator.validate_no_nulls(["sales_id", "amount"])
dup_check = validator.validate_no_duplicates(["sales_id"])

# Run all validations
results = validator.run_all_validations()
print(f"Quality Score: {results['quality_score']}%")
```

### 3. Data Lineage Tracking

```python
from lineage.tracker import get_lineage_tracker, LineageNode, LineageEdge, EntityType, AssetType

tracker = get_lineage_tracker()
graph = tracker.create_graph("sales_lineage")

# Add nodes
sales_bronze = LineageNode(
    entity_id="sales_bronze",
    entity_type=EntityType.DATASET,
    name="Sales Bronze",
    asset_type=AssetType.SOURCE
)
graph.add_node(sales_bronze)

sales_silver = LineageNode(
    entity_id="sales_silver",
    entity_type=EntityType.DATASET,
    name="Sales Silver",
    asset_type=AssetType.TRANSFORM
)
graph.add_node(sales_silver)

# Add relationships
edge = LineageEdge(
    source_id="sales_bronze",
    target_id="sales_silver",
    relationship_type="transformed"
)
graph.add_edge(edge)

# Get impact analysis
impact = graph.get_impact_analysis("sales_bronze")
```

### 4. Data Catalog

```python
from catalog.metadata import get_data_catalog, DatasetBuilder

catalog = get_data_catalog()

# Create metadata using builder
metadata = (DatasetBuilder("sales_silver_prod")
    .name("Sales Silver Production")
    .description("Cleaned and standardized sales data")
    .owner("data-team")
    .layer("silver")
    .format("delta")
    .location("/data/silver/sales")
    .schema({
        "sales_id": "long",
        "customer_id": "long",
        "amount": "decimal(10,2)",
        "sale_date": "date"
    })
    .tags(["sales", "silver", "critical", "production"])
    .quality_score(95.2)
    .record_count(1000000)
    .size_bytes(2147483648)  # 2GB
    .build())

# Register dataset
catalog.register_dataset(metadata)

# Search
results = catalog.search_by_tag("production")
catalog_stats = catalog.get_catalog_stats()
```

### 5. Monitoring & Observability

```python
from observability.monitoring import get_execution_monitor, PipelineStatus
from datetime import datetime

monitor = get_execution_monitor()

# Track pipeline execution
execution = monitor.start_execution(
    execution_id="exec_001",
    pipeline_name="data_ingestion",
    source="sales_api",
    target="/data/bronze/sales"
)

try:
    # ... pipeline processing ...
    monitor.end_execution(
        execution_id="exec_001",
        status=PipelineStatus.SUCCESS,
        records_processed=150000,
        records_failed=0
    )
except Exception as e:
    monitor.end_execution(
        execution_id="exec_001",
        status=PipelineStatus.FAILURE,
        error_message=str(e)
    )

# Get metrics
metrics = monitor.get_metrics()
prometheus_format = monitor.get_prometheus_metrics()
```

### 6. API with RBAC

```python
from api.main import app
from fastapi.testclient import TestClient

client = TestClient(app)

# Public endpoint
response = client.get("/health")
assert response.status_code == 200

# Viewer access
response = client.get("/api/v1/datasets?username=viewer")
assert response.status_code == 200

# Editor-only endpoint
response = client.post(
    "/api/v1/datasets/new_dataset?username=editor",
    params={
        "name": "New Dataset",
        "owner": "team",
        "layer": "gold",
        "format": "delta",
        "location": "/data/gold/new"
    }
)
assert response.status_code == 200

# Insufficient permissions
response = client.post(
    "/api/v1/datasets/new_dataset?username=viewer",
    params={...}
)
assert response.status_code == 403
```

---

## Monitoring & Observability

### Prometheus Metrics

Access metrics at: `http://localhost:9090`

**Key Metrics:**
- `pipeline_started_total`: Number of pipelines started
- `pipeline_success_total`: Successful pipeline executions
- `pipeline_failure_total`: Failed pipeline executions
- `pipeline_duration_seconds`: Pipeline execution duration
- `records_processed`: Records processed per pipeline
- `data_quality_score`: Quality score by dataset

### Grafana Dashboards

Access dashboards at: `http://localhost:3000` (admin/admin)

**Pre-configured Dashboards:**
1. **Platform Overview**: Overall system health
2. **Pipeline Performance**: Execution times and success rates
3. **Data Quality Trends**: Quality score trends over time
4. **Data Volume**: Records processed per dataset
5. **System Resources**: CPU, Memory, Disk usage

### Alerting

Configure alerts in `dashboards/prometheus.yml`:

```yaml
groups:
  - name: platform_alerts
    rules:
      - alert: PipelineFailed
        expr: pipeline_failure_total > 0
        for: 5m
        annotations:
          summary: "Pipeline failed"
      
      - alert: QualityScoreLow
        expr: data_quality_score < 80
        for: 10m
        annotations:
          summary: "Data quality below threshold"
```

---

## Development & Testing

### Run Tests

```bash
# Install test dependencies
pip install -r requirements.txt pytest pytest-cov

# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=. --cov-report=html

# Run specific test
pytest tests/test_ingestion.py::test_csv_ingestion -v
```

### Sample Test

```python
# tests/test_ingestion.py
import pytest
from pyspark.sql import SparkSession
from ingestion.sources import SalesSourceIngestion

@pytest.fixture
def spark():
    return SparkSession.builder \
        .appName("test") \
        .master("local") \
        .getOrCreate()

def test_sales_ingestion(spark):
    ingestion = SalesSourceIngestion(spark, "/tmp/test_sales")
    result = ingestion.ingest()
    
    assert result["status"] == "success"
    assert result["records_ingested"] == 5  # Sample data size
    assert result["source"] == "sales_source"
```

### Code Quality

```bash
# Format code
black . --line-length=100

# Sort imports
isort .

# Lint
flake8 . --max-line-length=100

# Type checking
mypy . --ignore-missing-imports
```

---

## Troubleshooting

### Issue: Docker container fails to start

**Solution:**
```bash
# Check logs
docker-compose logs api

# Rebuild image
docker-compose build --no-cache

# Restart services
docker-compose restart
```

### Issue: Out of memory

**Solution:**
```bash
# Increase Docker memory limit
# Update docker-compose.yml:
services:
  spark-master:
    mem_limit: 4g
  
  airflow-webserver:
    mem_limit: 2g
```

### Issue: Port already in use

**Solution:**
```bash
# Change ports in docker-compose.yml or
# Kill existing process:
lsof -ti:8000 | xargs kill -9
```

### Issue: Spark job fails

**Check:**
```bash
# View Spark logs
docker-compose logs spark-worker

# Check data files exist
docker-compose exec spark-worker ls -la /data/

# Test Spark connectivity
docker-compose exec spark-master spark-shell
```

---

## Production Deployment

### Kubernetes Deployment

```yaml
# kubernetes/api-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: data-gov-api
spec:
  replicas: 3
  selector:
    matchLabels:
      app: data-gov-api
  template:
    metadata:
      labels:
        app: data-gov-api
    spec:
      containers:
      - name: api
        image: data-gov-platform:latest
        ports:
        - containerPort: 8000
        env:
        - name: ENVIRONMENT
          value: "production"
        resources:
          requests:
            memory: "2Gi"
            cpu: "1"
          limits:
            memory: "4Gi"
            cpu: "2"
```

### AWS Deployment

```bash
# Push to ECR
aws ecr get-login-password | docker login --username AWS --password-stdin $ACCOUNT.dkr.ecr.$REGION.amazonaws.com
docker tag data-gov-platform:latest $ACCOUNT.dkr.ecr.$REGION.amazonaws.com/data-gov-platform:latest
docker push $ACCOUNT.dkr.ecr.$REGION.amazonaws.com/data-gov-platform:latest

# Deploy to ECS
aws ecs create-service --cluster data-governance \
  --service-name api \
  --task-definition data-gov-api:1 \
  --desired-count 3
```

### Security Best Practices

1. **Authentication**: Implement OAuth2/JWT tokens
2. **Data Encryption**: Enable TLS for all communications
3. **Access Control**: Implement detailed RBAC
4. **Audit Logging**: Log all API access
5. **Secret Management**: Use AWS Secrets Manager / HashiCorp Vault
6. **Network Security**: Deploy behind VPN/

 firewall

---

## Contributing

### Development Workflow

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Write tests for your changes
4. Commit: `git commit -m 'Add amazing feature'`
5. Push: `git push origin feature/amazing-feature`
6. Open a Pull Request

### Coding Standards

- Follow [PEP 8](https://pep8.org/)
- Write docstrings for all functions
- Add type hints
- Write unit tests (minimum 80% coverage)
- Use descriptive commit messages

---

## License

This project is licensed under the MIT License - see the LICENSE file for details.

---

## Support

For issues, questions, or suggestions:
- 📧 Email: support@data-governance-platform.io
- 💬 Discord: [Join our community](https://discord.gg/data-governance)
- 📚 Documentation: [docs.data-governance-platform.io](https://docs.data-governance-platform.io)
- 🐛 Issues: [GitHub Issues](https://github.com/yourusername/data-governance-platform/issues)

---

## Roadmap

- [ ] Multi-cloud support (AWS, Azure, GCP)
- [ ] Advanced ML-based anomaly detection
- [ ] Real-time data streaming (Kafka integration)
- [ ] Advanced lineage visualization UI
- [ ] OpenMetadata integration
- [ ] Data masking and PII detection
- [ ] Cost attribution and optimization
- [ ] Automated remediation for quality issues

---

**Last Updated:** January 2024  
**Version:** 1.0.0
#   E n t e r p r i s e - D a t a - G o v e r n a n c e - a n d - D a t a - Q u a l i t y - P l a t f o r m  
 