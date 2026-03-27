# Sensors & Triggers

> **Purpose**: Deferrable operators, ExternalTaskSensor, TriggerDagRunOperator, asset-aware scheduling
> **MCP Validated**: 2026-03-26 | Updated for Airflow 3.0 (Asset replaces Dataset)

## When to Use

- Waiting for upstream DAGs or external systems
- File/partition arrival detection before processing
- Cross-DAG orchestration without tight coupling
- Asset-aware scheduling (Airflow 3.0+; Dataset in 2.4-2.x)

## Implementation

```python
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor


# --- DATASETS (event-driven scheduling) ---
orders_dataset = Dataset("s3://lake/staging/orders/")
payments_dataset = Dataset("s3://lake/staging/payments/")


@dag(
    dag_id="producer_orders",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
)
def producer():
    @task(outlets=[orders_dataset])
    def ingest_orders() -> str:
        return "orders ingested"

    ingest_orders()


@dag(
    dag_id="consumer_analytics",
    schedule=[orders_dataset, payments_dataset],  # triggers when BOTH update
    start_date=datetime(2024, 1, 1),
    catchup=False,
)
def consumer():
    @task
    def build_analytics() -> str:
        return "analytics built from fresh data"

    build_analytics()


# --- DEFERRABLE SENSOR (frees worker slot while waiting) ---
@dag(
    dag_id="file_watcher",
    schedule="*/15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
)
def file_watcher():
    wait_for_file = S3KeySensor(
        task_id="wait_for_landing_file",
        bucket_name="data-lake-raw",
        bucket_key="landing/orders/{{ ds }}/data.parquet",
        aws_conn_id="aws_default",
        deferrable=True,           # async — no worker occupied
        poke_interval=60,
        timeout=3600,
    )

    wait_for_upstream = ExternalTaskSensor(
        task_id="wait_for_staging_dag",
        external_dag_id="staging_pipeline",
        external_task_id="final_quality_check",
        allowed_states=["success"],
        execution_delta=timedelta(hours=1),
        deferrable=True,
        timeout=7200,
    )

    trigger_downstream = TriggerDagRunOperator(
        task_id="trigger_transform",
        trigger_dag_id="transform_pipeline",
        conf={"source": "file_watcher", "date": "{{ ds }}"},
        wait_for_completion=False,
    )

    @task
    def process_file() -> str:
        return "file processed"

    wait_for_file >> wait_for_upstream >> process_file() >> trigger_downstream


producer()
consumer()
file_watcher()
```

## Configuration

| Sensor Pattern | Worker Usage | Best For | Version |
|---------------|-------------|----------|---------|
| `mode="poke"` | Holds worker slot | Short waits (<5 min) | All |
| `mode="reschedule"` | Releases between pokes | Medium waits (5-60 min) | All |
| `deferrable=True` | Async trigger (no worker) | Long waits (1h+) | 2.2+ |
| `Asset()` schedule | Event-driven (no sensor) | Cross-DAG dependencies | **3.0+** |
| `Asset() & Asset()` | AND logic on assets | Multiple upstream deps | **3.0+** |
| `Asset() \| Asset()` | OR logic on assets | Any-of triggering | **3.0+** |

## See Also

- [dag-design](../concepts/dag-design.md)
- [task-dependencies](../concepts/task-dependencies.md)
- [dynamic-task-mapping](../patterns/dynamic-task-mapping.md)
