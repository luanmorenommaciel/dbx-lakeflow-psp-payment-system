# Databricks Implementation Patterns

> **Purpose:** Production patterns for DLT with expectations, Jobs API workflows, Unity Catalog setup, and MLflow model registry
> **Confidence:** 0.90
> **MCP Validated:** 2026-03-26

## Overview

Databricks patterns for production data engineering: Delta Live Tables (Declarative Pipelines) with data quality expectations, Jobs API for orchestrated multi-task workflows, Unity Catalog namespace and permissions setup, and MLflow 3 for experiment tracking and model registry.

## The Pattern

### DLT with Expectations (Data Quality Gates)

```python
import dlt
from pyspark.sql.functions import col, expr, current_timestamp, sha2, concat_ws

# Bronze: raw ingestion with schema inference
@dlt.table(
    name="customers_bronze",
    comment="Raw customer data from landing zone",
    table_properties={"quality": "bronze"}
)
def customers_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", "/mnt/checkpoints/customers_schema")
        .load("/mnt/landing/customers/")
    )

# Silver: cleaned with expectations
@dlt.table(
    name="customers_silver",
    comment="Validated customers with surrogate keys",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_email", "email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'")
@dlt.expect_or_fail("not_null_id", "customer_id IS NOT NULL")
@dlt.expect("has_name", "first_name IS NOT NULL AND last_name IS NOT NULL")
def customers_silver():
    return (
        dlt.read_stream("customers_bronze")
        .withColumn("customer_sk", sha2(concat_ws("||", col("customer_id"), col("email")), 256))
        .withColumn("processed_at", current_timestamp())
        .dropDuplicates(["customer_id"])
    )

# Gold: business aggregation
@dlt.table(
    name="customer_lifetime_value",
    comment="CLV metrics per customer",
    table_properties={"quality": "gold"}
)
def customer_lifetime_value():
    customers = dlt.read("customers_silver")
    orders = dlt.read("orders_silver")
    return (
        orders.groupBy("customer_id")
        .agg(
            expr("SUM(order_amount) AS total_spend"),
            expr("COUNT(*) AS order_count"),
            expr("DATEDIFF(MAX(order_date), MIN(order_date)) AS customer_tenure_days")
        )
        .join(customers, "customer_id", "inner")
    )
```

### Jobs API Workflow (Multi-Task Orchestration)

```python
# Databricks Jobs API -- multi-task workflow definition (JSON payload)
job_config = {
    "name": "daily_pipeline_v2",
    "tasks": [
        {
            "task_key": "ingest_raw",
            "pipeline_task": {
                "pipeline_id": "abc-123-dlt-pipeline"
            }
        },
        {
            "task_key": "run_dbt_models",
            "depends_on": [{"task_key": "ingest_raw"}],
            "dbt_task": {
                "project_directory": "/Workspace/dbt_project",
                "commands": ["dbt run --select tag:daily"],
                "warehouse_id": "xyz-789-sql-warehouse"
            }
        },
        {
            "task_key": "quality_checks",
            "depends_on": [{"task_key": "run_dbt_models"}],
            "notebook_task": {
                "notebook_path": "/Repos/pipelines/quality_checks",
                "base_parameters": {"date": "{{job.start_date.iso}}"}
            },
            "new_cluster": {
                "spark_version": "15.4.x-scala2.12",
                "num_workers": 2,
                "node_type_id": "i3.xlarge"
            }
        },
        {
            "task_key": "train_model",
            "depends_on": [{"task_key": "quality_checks"}],
            "notebook_task": {
                "notebook_path": "/Repos/ml/train_churn_model"
            },
            "job_cluster_key": "ml_cluster"
        }
    ],
    "job_clusters": [{
        "job_cluster_key": "ml_cluster",
        "new_cluster": {
            "spark_version": "15.4.x-ml-scala2.12",
            "num_workers": 4,
            "node_type_id": "g5.2xlarge"
        }
    }],
    "schedule": {"quartz_cron_expression": "0 0 6 * * ?", "timezone_id": "UTC"}
}
```

### Unity Catalog Namespace Setup

```sql
-- Create catalog for production environment
CREATE CATALOG IF NOT EXISTS prod_catalog
  COMMENT 'Production data catalog';

-- Create schemas (databases) within catalog
CREATE SCHEMA IF NOT EXISTS prod_catalog.raw
  COMMENT 'Raw ingested data';
CREATE SCHEMA IF NOT EXISTS prod_catalog.curated
  COMMENT 'Cleaned and validated data';
CREATE SCHEMA IF NOT EXISTS prod_catalog.analytics
  COMMENT 'Business-ready aggregations';

-- Grant access by role
GRANT USE CATALOG ON CATALOG prod_catalog TO `data-engineers`;
GRANT USE SCHEMA, SELECT ON SCHEMA prod_catalog.raw TO `data-engineers`;
GRANT ALL PRIVILEGES ON SCHEMA prod_catalog.curated TO `data-engineers`;
GRANT USE SCHEMA, SELECT ON SCHEMA prod_catalog.analytics TO `analysts`;

-- External location for cloud storage
CREATE EXTERNAL LOCATION prod_s3_landing
  URL 's3://company-data-lake/landing/'
  WITH (STORAGE CREDENTIAL aws_prod_credential);
```

### MLflow 3 Model Registry

```python
import mlflow
from mlflow.models import infer_signature

# Set Unity Catalog as the model registry
mlflow.set_registry_uri("databricks-uc")

with mlflow.start_run(run_name="churn_model_v3") as run:
    # Train model
    model = train_xgboost_model(X_train, y_train)
    signature = infer_signature(X_train, model.predict(X_train))

    # Log model to Unity Catalog
    mlflow.sklearn.log_model(
        model,
        artifact_path="model",
        registered_model_name="prod_catalog.ml_models.churn_predictor",
        signature=signature
    )
    mlflow.log_metrics({"auc": 0.89, "f1": 0.82, "precision": 0.85})

# Promote to production alias
client = mlflow.tracking.MlflowClient()
client.set_registered_model_alias(
    name="prod_catalog.ml_models.churn_predictor",
    alias="production",
    version=3
)
```

## Quick Reference

| Pattern | Use When | Key Detail |
|---------|----------|------------|
| DLT expectations | Data quality gates in pipelines | `expect_or_drop` / `expect_or_fail` |
| Jobs API workflow | Multi-task orchestration | Task dependencies + mixed task types |
| Unity Catalog | Governance and access control | `catalog.schema.table` namespace |
| MLflow 3 | Model versioning and deployment | UC-backed model registry |

## Common Mistakes

### Wrong
```python
# Registering models to legacy workspace registry
mlflow.set_registry_uri("databricks")
```

### Correct
```python
# Using Unity Catalog model registry for governance
mlflow.set_registry_uri("databricks-uc")
```

## Related

- [Databricks LakeFlow](../concepts/databricks-lakeflow.md) -- Platform concepts and LakeFlow Connect
- [Cross-Platform Patterns](../concepts/cross-platform-patterns.md) -- SQL dialect portability
- [Cost Optimization](cost-optimization.md) -- DBU management and cluster sizing
