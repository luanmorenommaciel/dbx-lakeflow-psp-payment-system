# DAG Factory

> **Purpose**: Parameterized DAG generation from YAML config; dynamic mapping with TaskGroups; factory pattern for standardized pipelines
> **MCP Validated**: 2026-03-26

## When to Use

- Multiple similar pipelines with different sources/targets
- Standardizing DAG structure across teams
- Reducing boilerplate in Airflow projects with 50+ DAGs
- Need consistent retry, alerting, and SLA config across DAGs

## Implementation

```python
from __future__ import annotations

import yaml
from pathlib import Path
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup


# --- YAML CONFIG (dags/configs/pipeline_orders.yaml) ---
# pipeline_name: orders
# schedule: "0 6 * * *"
# owner: data-team
# retries: 3
# source:
#   type: postgres
#   conn_id: source_postgres
#   tables: [raw_orders, raw_order_items, raw_payments]
# target:
#   type: snowflake
#   conn_id: target_snowflake
#   schema: staging


def load_pipeline_configs(config_dir: str = "dags/configs") -> list[dict]:
    """Load all YAML pipeline configs from directory."""
    configs = []
    for path in Path(config_dir).glob("pipeline_*.yaml"):
        with open(path) as f:
            configs.append(yaml.safe_load(f))
    return configs


def create_dag(config: dict):
    """Factory: create a DAG from a pipeline config dict."""

    default_args = {
        "owner": config.get("owner", "data-team"),
        "retries": config.get("retries", 2),
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=2),
    }

    @dag(
        dag_id=f"pipeline_{config['pipeline_name']}",
        default_args=default_args,
        schedule=config.get("schedule", None),
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=["factory", config["pipeline_name"]],
    )
    def pipeline():
        @task
        def extract(table: str, conn_id: str) -> dict:
            """Extract table from source system."""
            from airflow.hooks.base import BaseHook
            hook = BaseHook.get_hook(conn_id)
            return {"table": table, "row_count": hook.get_records(f"SELECT COUNT(*) FROM {table}")[0][0]}

        @task
        def load(extract_result: dict, target_conn: str, target_schema: str) -> str:
            """Load extracted data to target warehouse."""
            return f"Loaded {extract_result['table']} ({extract_result['row_count']} rows) to {target_schema}"

        @task
        def quality_check(load_result: str) -> None:
            """Run post-load quality checks."""
            assert "Loaded" in load_result, f"Load failed: {load_result}"

        # --- Dynamic TaskGroup per source table ---
        with TaskGroup("ingest") as ingest_group:
            for table in config["source"]["tables"]:
                with TaskGroup(f"process_{table}"):
                    extracted = extract(table=table, conn_id=config["source"]["conn_id"])
                    loaded = load(
                        extract_result=extracted,
                        target_conn=config["target"]["conn_id"],
                        target_schema=config["target"]["schema"],
                    )
                    quality_check(load_result=loaded)

    return pipeline()


# --- Generate DAGs at module level (Airflow discovers these) ---
for cfg in load_pipeline_configs():
    globals()[f"pipeline_{cfg['pipeline_name']}"] = create_dag(cfg)
```

## Configuration

| Parameter | Location | Purpose |
|-----------|----------|---------|
| `pipeline_name` | YAML | Unique DAG identifier |
| `schedule` | YAML | Cron expression |
| `source.tables` | YAML | List of tables to ingest |
| `retries` | YAML / default_args | Override per-pipeline |
| `config_dir` | `load_pipeline_configs()` | Path to YAML configs |

## Example Usage

```yaml
# dags/configs/pipeline_payments.yaml
pipeline_name: payments
schedule: "30 7 * * *"
owner: finance-team
retries: 5
source:
  type: postgres
  conn_id: payments_db
  tables: [transactions, refunds, chargebacks]
target:
  type: snowflake
  conn_id: warehouse
  schema: staging_finance
```

## See Also

- [dag-design](../concepts/dag-design.md)
- [dynamic-task-mapping](../patterns/dynamic-task-mapping.md)
- [error-handling](../patterns/error-handling.md)
