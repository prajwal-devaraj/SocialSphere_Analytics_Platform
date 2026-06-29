# Architecture

## Overview

SocialSphere Analytics Platform follows a **Medallion (Bronze/Silver/Gold) Lakehouse architecture**, the same pattern used internally at companies like Netflix, Uber, and Airbnb for product analytics. The current implementation ships the MVP stack; a Kafka/Redpanda streaming layer and a Trino/Hive query layer are designed but not yet wired in (see "Future Improvements" in the main README).

## Data Flow

```
Synthetic Event Generator
        |
        v
FastAPI Event Ingestion Service  (validates against ProductEvent schema)
        |
        v
Bronze Layer (MinIO/S3 or local disk)
   raw NDJSON, partitioned by year/month/day/hour
        |
        v
Airflow DAG: daily_product_analytics_dag
        |
        v
PySpark: bronze_to_silver.py
   dedup, validate, standardize -> Parquet, partitioned by event_date
        |
        v
Silver Layer
   events_clean/
        |
        v
PySpark gold jobs (run in parallel where possible):
   silver_to_gold_daily_metrics.py  -> gold_daily_user_metrics
   retention_cohort_job.py          -> gold_retention_cohorts
   funnel_analysis_job.py           -> gold_funnel_metrics
   feature_usage_job.py             -> gold_feature_usage
   revenue_metrics_job.py           -> gold_revenue_metrics
   anomaly_detection_job.py         -> gold_anomaly_detection (depends on daily + revenue)
        |
        v
Gold Layer (business-ready Parquet tables)
        |
        v
Streamlit Dashboard (reads gold Parquet directly via pandas)
```

## Why Medallion Architecture

- **Bronze** preserves raw data exactly as received — if a downstream bug is found, you can always reprocess from bronze rather than losing history.
- **Silver** is the single place data quality rules live — every gold table reads from the same validated, deduplicated source, so metrics stay consistent across dashboards.
- **Gold** tables are narrow and purpose-built per question ("what's DAU", "what's the funnel") rather than one giant denormalized table, which keeps each Spark job simple and independently testable.

## Why These Specific Technology Choices

| Component | Choice | Why |
|---|---|---|
| Ingestion | FastAPI + Pydantic | Async-friendly, automatic request validation, OpenAPI docs for free |
| Storage | MinIO (S3-compatible) | Drop-in replacement for AWS S3 locally; same boto3 API works against real AWS later |
| Processing | PySpark | Industry-standard for the data volumes this project targets (1M–5M+ events); same DataFrame API scales from a laptop to a real cluster |
| Orchestration | Airflow | The industry-standard scheduler; DAG dependency graph maps directly onto the bronze→silver→gold lineage |
| Dashboard | Streamlit | Fastest path from Python + pandas to an interactive multi-page app; no separate frontend build step |

## Parallelism in the DAG

`silver_to_gold_daily_metrics`, `retention_cohort_job`, `funnel_analysis_job`, and `feature_usage_job` all read only from the silver layer and don't depend on each other, so the Airflow DAG runs them in parallel. `anomaly_detection_job` waits for both `silver_to_gold_daily_metrics` and `revenue_metrics_job` since it needs both of their outputs.

## Future: Streaming + Query Layer

The "Advanced Version" described in the README adds:
- **Redpanda/Kafka** between the ingestion API and the bronze layer, decoupling ingestion throughput from batch processing
- **Trino + Hive Metastore** as a SQL query layer over the gold Parquet tables (DDL already written in `sql/create_external_tables.sql`), enabling ad-hoc SQL analysis without going through the dashboard
- **Apache Iceberg** as the table format for ACID guarantees and time-travel queries on the data lake
