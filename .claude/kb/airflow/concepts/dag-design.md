# DAG Design

> **Purpose**: Idempotency, atomicity, retries, SLAs, schedule strategies, naming conventions
> **Confidence**: 0.95
> **MCP Validated**: 2026-03-26 | Updated for Airflow 3.0 GA

## Overview

A well-designed DAG is idempotent (re-runs produce the same result), atomic (each task does one thing), and observable (SLAs, alerting). The `@dag` decorator (TaskFlow API) is the modern way to define DAGs. **Airflow 3.0** adds DAG versioning (code tracked per execution), asset-aware scheduling via `@asset` decorator, and a new React-based UI. Import from `airflow.sdk` in Airflow 3.0+.

## The Concept

```python
from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(hours=1),
    "email_on_failure": True,
    "email": ["data-alerts@company.com"],
}

@dag(
    dag_id="daily_orders_pipeline",
    schedule="0 6 * * *",          # 6 AM UTC daily
    start_date=datetime(2026, 1, 1),
    catchup=False,                  # don't backfill on deploy
    max_active_runs=1,              # prevent overlap
    tags=["finance", "daily"],
    default_args=default_args,
    doc_md="""
    ## Daily Orders Pipeline
    Extracts orders from Stripe, loads to warehouse, runs dbt models.
    **SLA:** Data available by 7:00 AM UTC.
    """,
)
def daily_orders_pipeline():
    @task()
    def extract_orders(ds=None):
        """Extract orders for the logical date (ds)."""
        # ds = execution date string (YYYY-MM-DD) — makes task idempotent
        return {"date": ds, "count": 1500}

    @task()
    def load_to_warehouse(extract_result: dict):
        """Load extracted data to staging table."""
        return f"Loaded {extract_result['count']} rows"

    @task()
    def run_dbt():
        """Run dbt models downstream."""
        pass

    # Define dependencies via return values
    data = extract_orders()
    loaded = load_to_warehouse(data)
    loaded >> run_dbt()

daily_orders_pipeline()
```

## Quick Reference

| Principle | Rule | Why |
|-----------|------|-----|
| Idempotency | Use `ds` (logical date), not `datetime.now()` | Re-runs produce same result |
| Atomicity | One task = one logical operation | Retry granularity, clear debugging |
| No top-level code | All logic inside tasks or callables | Runs on every scheduler heartbeat otherwise |
| `catchup=False` | Default for new DAGs | Prevents accidental backfill flood |
| `max_active_runs=1` | For pipelines that can't overlap | Prevents resource contention |

## Common Mistakes

### Wrong

```python
# Top-level code outside DAG — runs on every scheduler parse
import pandas as pd
df = pd.read_csv("big_file.csv")  # executed every 30 seconds!

@dag(schedule="@daily")
def bad_dag():
    pass
```

### Correct

```python
@dag(schedule="@daily")
def good_dag():
    @task()
    def process():
        import pandas as pd  # import inside task
        df = pd.read_csv("big_file.csv")
    process()
```

## Airflow 3.0: Asset-Driven DAG Example

```python
from airflow.sdk import DAG, task, asset
from datetime import datetime

@asset
def orders_data():
    """Define a data asset that other DAGs can depend on."""
    print("Producing orders data...")
    return "orders_ready"

@dag(
    dag_id="asset_consumer_pipeline",
    schedule=[Asset("s3://lake/silver/orders")],  # triggers on asset update
    start_date=datetime(2026, 1, 1),
    catchup=False,
)
def asset_consumer():
    @task()
    def process_orders():
        """Runs only when the orders asset is updated."""
        pass

    process_orders()

asset_consumer()
```

## Related

- [operators](../concepts/operators.md)
- [error-handling](../patterns/error-handling.md)
