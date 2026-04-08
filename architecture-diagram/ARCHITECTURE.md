# Enterprise Data Governance Platform - Architecture

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         External Data Sources                                │
│    ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│    │   Sales      │  │   Customer   │  │   Transact.  │  │   External   │  │
│    │   Systems    │  │   Database   │  │   Streams    │  │   APIs       │  │
│    └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘  │
└────────────────┬─────────────────────────────────────────────────────┬───────┘
                 │                                                       │
                 └───────────────────┬───────────────────────────────────┘
                                     │
           ┌─────────────────────────▼─────────────────────────┐
           │      Data Ingestion Layer                         │
           │  ┌──────────────────────────────────────────┐   │
           │  │  Ingestion Adapters                      │   │
           │  │  • CSV/JSON/Parquet Readers             │   │
           │  │  • API Connectors                        │   │
           │  │  • Database Connectors (JDBC)           │   │
           │  └──────────────────────────────────────────┘   │
           └────────────────────────┬────────────────────────┘
                                    │
           ┌────────────────────────▼──────────────────────────────────┐
           │         Bronze Layer (Raw Data Lake)                      │
           │  ┌──────────────────────────────────────────────────┐   │
           │  │  Raw Data Ingestion                              │   │
           │  │  • Sales (Raw)                                   │   │
           │  │  • Customers (Raw)                               │   │
           │  │  • Transactions (Raw)                            │   │
           │  │  Storage: Delta Lake / Parquet                   │   │
           │  └──────────────────────────────────────────────────┘   │
           └────────────────────────┬──────────────────────────────────┘
                                    │
           ┌────────────────────────▼──────────────────────────────────┐
           │  Data Quality & Validation (Great Expectations)           │
           │  ┌──────────────────────────────────────────────────┐   │
           │  │  Quality Checks                                  │   │
           │  │  • Schema Validation                             │   │
           │  │  • Null Value Detection                          │   │
           │  │  • Duplicate Detection                           │   │
           │  │  • Range Checks                                  │   │
           │  │  • Anomaly Detection                             │   │
           │  └──────────────────────────────────────────────────┘   │
           └────────────────────────┬──────────────────────────────────┘
                                    │
           ┌────────────────────────▼──────────────────────────────────┐
           │         Silver Layer (Cleaned Data)                       │
           │  ┌──────────────────────────────────────────────────┐   │
           │  │  Transformed & Cleaned Data                      │   │
           │  │  • Sales (Cleaned)                               │   │
           │  │  • Customers (Cleaned)                           │   │
           │  │  • Transactions (Cleaned)                        │   │
           │  │  Storage: Delta Lake                             │   │
           │  └──────────────────────────────────────────────────┘   │
           └────────────────────────┬──────────────────────────────────┘
                                    │
           ┌────────────────────────▼──────────────────────────────────┐
           │         Gold Layer (Business Views)                       │
           │  ┌──────────────────────────────────────────────────┐   │
           │  │  Aggregated & Business-Ready Datasets           │   │
           │  │  • Sales by Region                               │   │
           │  │  • Customer Metrics                              │   │
           │  │  • Transaction Analytics                         │   │
           │  │  • Executive Dashboards                          │   │
           │  └──────────────────────────────────────────────────┘   │
           └────────────────────────┬──────────────────────────────────┘
                                    │
      ┌─────────────────────────────┴─────────────────────────────────┐
      │                                                                 │
      ▼                                                                 ▼
┌──────────────────────┐                                     ┌──────────────────────┐
│  Data Catalog &      │                                     │  Data Lineage        │
│  Metadata Management │                                     │  & Impact Analysis   │
│  ┌────────────────┐  │                                     │  ┌────────────────┐  │
│  │ Dataset Info   │  │                                     │  │ Lineage Graph  │  │
│  │ Tags/Desc      │  │                                     │  │ Dependencies   │  │
│  │ Ownership      │  │                                     │  │ Impact Mapping │  │
│  │ Search Index   │  │                                     │  │ Version Track  │  │
│  └────────────────┘  │                                     │  └────────────────┘  │
└──────────────────────┘                                     └──────────────────────┘
                                    │
      ┌─────────────────────────────┴─────────────────────────────────┐
      │                                                                 │
      ▼                                                                 ▼
┌──────────────────────────────────────────────┐  ┌──────────────────────────────┐
│   API Layer (FastAPI)                        │  │   Observability & Monitoring │
│   ┌────────────────────────────────────────┐ │  │   ┌────────────────────────┐  │
│   │ /datasets              - List datasets  │ │  │   │ Execution Monitoring   │  │
│   │ /lineage/{id}          - Dataset flows  │ │  │   │ Metrics Collection     │  │
│   │ /quality/{id}          - Quality scores │ │  │   │ Failure Alerts         │  │
│   │ /health                - System health  │ │  │   │ Performance Tracking   │  │
│   │ /pipelines             - Pipeline stats │ │  │   │ Drift Detection        │  │
│   │ /catalog/search        - Catalog search │ │  │   └────────────────────────┘  │
│   │ (+ Role-Based Access Control)          │ │  │                                │
│   └────────────────────────────────────────┘ │  │   ┌────────────────────────┐  │
│                                              │  │   │ Prometheus Metrics     │  │
│   Authentication & Authorization:           │  │   │ Grafana Dashboards     │  │
│   • Admin Role                               │  │   │ Alert Managers         │  │
│   • Editor Role                              │  │   │ Log Aggregation        │  │
│   • Viewer Role                              │  │   └────────────────────────┘  │
└──────────────────────────────────────────────┘  └──────────────────────────────┘
                                    │
                                    │
           ┌────────────────────────▼──────────────────────────────────┐
           │      Orchestration Layer (Apache Airflow)                 │
           │  ┌──────────────────────────────────────────────────┐   │
           │  │  DAGs (Directed Acyclic Graphs)                  │   │
           │  │  1. Data Ingestion Pipeline                      │   │
           │  │     └─ Parallel source loading                   │   │
           │  │  2. Transformation Pipeline                      │   │
           │  │     └─ Bronze → Silver → Gold                    │   │
           │  │  3. Quality Checks Pipeline                      │   │
           │  │     └─ Validation & Profiling                    │   │
           │  │  4. Metadata Update Pipeline                     │   │
           │  │     └─ Catalog Refresh                           │   │
           │  │  5. Master Orchestration                         │   │
           │  │     └─ End-to-End Workflow                       │   │
           │  └──────────────────────────────────────────────────┘   │
           │  Scheduling: Daily Jobs @ 1AM, 2AM, 3AM, 4AM, 5AM      │
           └────────────────────────┬──────────────────────────────────┘
                                    │
                    ┌───────────────┴───────────────┐
                    │                               │
                    ▼                               ▼
        ┌───────────────────────┐    ┌──────────────────────────┐
        │   Spark Engine        │    │   Storage Layer          │
        │   ┌─────────────────┐ │    │   ┌──────────────────┐   │
        │   │ PySpark Jobs    │ │    │   │ Delta Lake       │   │
        │   │ Processing      │ │    │   │ (Versioning)     │   │
        │   │ Transformations │ │    │   │ Parquet Files    │   │
        │   │ Aggregations    │ │    │   │ Time Travel      │   │
        │   │ (Local/Cluster) │ │    │   │ ACID Compliance  │   │
        │   └─────────────────┘ │    │   └──────────────────┘   │
        └───────────────────────┘    └──────────────────────────┘
                                    │
        ┌───────────────────────────┴────────────────────────┐
        │                                                     │
        ▼                                                     ▼
┌───────────────────────────────┐        ┌──────────────────────────┐
│ Storage Options               │        │ External Integrations    │
│ • Local Filesystem (/data)    │        │ • AWS S3 / Azure ADLS    │
│ • Delta Lake                  │        │ • Kafka / Cloud Messaging│
│ • Data Warehouse              │        │ • Email Notifications    │
│ • Time Travel & Versioning    │        │ • Slack/Teams Alerts     │
└───────────────────────────────┘        └──────────────────────────┘
```

## Data Flow Process

### 1. Ingestion Phase (2 AM Daily)
- Load data from multiple sources into Bronze layer
- Parallel processing of Sales, Customer, Transaction data
- Records ingestion time and source metadata
- Data is stored in Delta format for versioning

### 2. Transformation Phase (3 AM Daily)
- Clean and standardize data from Bronze → Silver
- Apply business rules and transformations
- Remove duplicates and handle null values
- Preserve data lineage

### 3. Quality Validation Phase (4 AM Daily)
- Schema validation against expected structure
- Null value detection in critical columns
- Duplicate detection across key columns
- Custom rule violations
- Generates quality report with scoring

### 4. Metadata & Catalog Update (5 AM Daily)
- Register datasets in data catalog
- Update metadata with quality scores
- Index for search capability
- Track ownership and tags

### 5. Exposure through APIs
- REST endpoints for catalog browsing
- Lineage visualization
- Quality metrics reporting
- Monitoring and alerting

## Component Interactions

1. **Data Sources** → Ingestion (via adapters) → Bronze Layer → Quality Checks
2. **Quality Results** → Quality Monitor → Observability Stack
3. **Transformations** → Processing Engine → Silver Layer → Gold Layer
4. **Metadata Updates** → Catalog Service → Search Index
5. **Lineage Tracking** → Graph Database → Impact Analysis
6. **APIs** → Authentication → Role-Based Access → Response

## Technical Stack Summary

| Layer | Technologies |
|-------|---|
| **Ingestion** | Python, PySpark, Custom Adapters |
| **Processing** | PySpark, Delta Lake |
| **Quality** | Great Expectations, Custom Validators |
| **Storage** | Delta Lake, Parquet, HDFS/S3 |
| **Orchestration** | Apache Airflow, Celery |
| **APIs** | FastAPI, Uvicorn |
| **Monitoring** | Prometheus, Grafana, Python Logging |
| **Lineage** | Custom Graph Implementation |
| **Catalog** | In-Memory + Optional: OpenMetadata |
| **Infrastructure** | Docker, Docker Compose, Kubernetes (Optional) |

## Deployment Architecture (Docker Compose)

- **postgres**: Metadata store for Airflow
- **redis**: Message broker for Airflow workers
- **airflow-webserver**: Orchestration UI
- **airflow-scheduler**: Task scheduling
- **airflow-worker**: Task execution
- **spark-master/worker**: Distributed processing
- **prometheus**: Metrics collection
- **grafana**: Visualization
- **api**: FastAPI application server

