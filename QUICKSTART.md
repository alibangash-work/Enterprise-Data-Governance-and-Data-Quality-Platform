# Quick Start Guide

## 5-Minute Setup

### 1. Prerequisites
- Docker & Docker Compose installed
- 8GB RAM minimum

### 2. Start the Platform

```bash
# Clone repository
git clone <repo-url>
cd data-governance-platform

# Copy environment template
cp .env.example .env

# Start all services
docker-compose up

# Wait for services to be healthy (5-10 minutes)
docker-compose ps
```

### 3. Access the Platform

- **API**: http://localhost:8000
- **API Docs**: http://localhost:8000/docs
- **Airflow UI**: http://localhost:8080 (admin/admin)
- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000 (admin/admin)

### 4. Test the API

```bash
# Check health
curl http://localhost:8000/health

# List datasets
curl http://localhost:8000/api/v1/datasets?username=admin

# Get API documentation
open http://localhost:8000/docs
```

### 5. Run Example

```bash
# Inside container
docker-compose exec api python examples/api_client_demo.py

# Or run from host (if Python installed)
python examples/api_client_demo.py
```

## Key Components

| Component | URL | Purpose |
|-----------|-----|---------|
| **API** | http://localhost:8000 | RESTful governance API |
| **API Docs** | http://localhost:8000/docs | Interactive API documentation |
| **Airflow** | http://localhost:8080 | Workflow orchestration |
| **Prometheus** | http://localhost:9090 | Metrics collection |
| **Grafana** | http://localhost:3000 | Dashboards & visualization |

## Common Operations

### View Logs
```bash
docker-compose logs -f api
docker-compose logs -f airflow-scheduler
docker-compose logs -f prometheus
```

### Stop Services
```bash
docker-compose down
```

### Clean Everything
```bash
docker-compose down -v  # Warning: deletes all data
```

### Run Tests
```bash
docker-compose exec api pytest tests/ -v
```

## Example Workflows

### 1. Ingest Data
```python
from ingestion.sources import SalesSourceIngestion
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ingest").getOrCreate()
ingestion = SalesSourceIngestion(spark, "/data/bronze/sales")
result = ingestion.ingest()
```

### 2. Check Data Quality
```python
from data_quality.validators import DataQualityValidator

df = spark.read.parquet("/data/bronze/sales")
validator = DataQualityValidator(df, "sales")
results = validator.run_all_validations()
print(f"Quality Score: {results['quality_score']}%")
```

### 3. Search Catalog
```python
from catalog.metadata import get_data_catalog

catalog = get_data_catalog()
results = catalog.search_datasets("sales", field="name")
for dataset in results:
    print(f"- {dataset.name}")
```

### 4. Track Lineage
```python
from lineage.tracker import get_lineage_tracker

tracker = get_lineage_tracker()
graph = tracker.create_graph("lineage")
# ... add nodes and edges
impact = graph.get_impact_analysis("sales_bronze")
```

## Troubleshooting

**Docker services not starting?**
- Check Docker is running: `docker ps`
- Increase memory: Update Docker desktop settings
- Check logs: `docker-compose logs`

**API not responding?**
- Wait 2-3 minutes for startup
- Check health: `curl http://localhost:8000/health`
- View logs: `docker-compose logs api`

**Out of memory?**
- Reduce Spark memory in docker-compose.yml
- Close other applications
- Increase Docker memory limit

## Next Steps

1. Read [README.md](../README.md) for full documentation
2. Review [ARCHITECTURE.md](../architecture-diagram/ARCHITECTURE.md)
3. Check [API endpoints](../api/main.py) for available operations
4. Run example scripts in `examples/` directory
5. Explore Airflow DAGs in `airflow/dags.py`

## Support

- 📚 Documentation: See README.md
- 🐛 Issues: Create GitHub issue
- 💬 Questions: Check discussions

Happy governing your data! 🎉
