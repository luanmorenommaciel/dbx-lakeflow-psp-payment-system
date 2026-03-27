# Operators

> **Purpose**: PythonOperator, BashOperator, dbt Cloud, SparkSubmit, BigQuery; @task/@asset decorators; deferrable operators
> **Confidence**: 0.95
> **MCP Validated**: 2026-03-26 | Updated for Airflow 3.0 (Task Execution API, @asset decorator)

## Overview

Operators are the building blocks of Airflow tasks. The TaskFlow API (`@task`) replaces `PythonOperator` for most Python work. Provider packages offer operators for dbt Cloud, Spark, BigQuery, and other systems. **Deferrable operators** free up worker slots by suspending during I/O waits. **Airflow 3.0** adds the `@asset` decorator for defining data assets, and the **Task Execution API** enables remote task execution in containerized or edge environments.

## The Concept

```python
from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from datetime import datetime

@dag(schedule="@daily", start_date=datetime(2026, 1, 1))
def multi_operator_dag():

    # TaskFlow API — replaces PythonOperator
    @task()
    def extract(ds=None) -> dict:
        return {"date": ds, "rows": 5000}

    # BigQuery operator
    run_bq = BigQueryInsertJobOperator(
        task_id="run_bq_query",
        configuration={
            "query": {
                "query": "SELECT * FROM `project.dataset.table`",
                "useLegacySql": False,
                "destinationTable": {"tableId": "output"},
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
        deferrable=True,  # free worker slot while BQ runs
    )

    # dbt Cloud operator
    run_dbt = DbtCloudRunJobOperator(
        task_id="run_dbt_models",
        job_id=12345,
        check_interval=30,
        deferrable=True,
    )

    # Spark Submit
    run_spark = SparkSubmitOperator(
        task_id="run_spark_job",
        application="/opt/spark/jobs/transform.py",
        conn_id="spark_default",
        conf={"spark.sql.adaptive.enabled": "true"},
    )

    extract() >> run_bq >> run_dbt >> run_spark

multi_operator_dag()
```

## Quick Reference

| Operator | Use Case | Deferrable? | Notes |
|----------|----------|-------------|-------|
| `@task` / `PythonOperator` | Python logic | No (runs in worker) | Default choice |
| `@asset` | Define data assets | N/A | **Airflow 3.0+** — event-driven |
| `BashOperator` | Shell commands | No | |
| `BigQueryInsertJobOperator` | BQ queries | Yes | |
| `DbtCloudRunJobOperator` | dbt Cloud jobs | Yes | |
| `SparkSubmitOperator` | Spark jobs | No | |
| `S3ToSnowflakeOperator` | S3 → Snowflake COPY | Yes | |
| `HttpOperator` | REST API calls | Yes | |

## Common Mistakes

### Wrong

```python
# Not using deferrable — blocks a worker slot for hours
run_bq = BigQueryInsertJobOperator(
    task_id="bq_query",
    configuration={...},
    # missing deferrable=True — worker sits idle while BQ runs
)
```

### Correct

```python
# Deferrable — frees worker slot, uses triggerer process
run_bq = BigQueryInsertJobOperator(
    task_id="bq_query",
    configuration={...},
    deferrable=True,  # worker freed, triggerer polls BQ status
)
```

## Related

- [dag-design](../concepts/dag-design.md)
- [task-dependencies](../concepts/task-dependencies.md)
