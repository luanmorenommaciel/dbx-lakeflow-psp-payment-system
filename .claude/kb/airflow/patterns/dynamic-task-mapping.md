# Dynamic Task Mapping

> **Purpose**: Runtime task generation with expand(), partial(), TaskGroups; fan-out/fan-in patterns for variable workloads
> **MCP Validated**: 2026-03-26 | Compatible with Airflow 2.3+ and 3.0+

## When to Use

- Number of tasks unknown until runtime (file lists, partition counts, API pages)
- Same operation applied to a variable-length collection
- Fan-out processing followed by aggregation (fan-in)
- Combining dynamic mapping with shared configuration via partial()

## Implementation

```python
from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup


@dag(
    dag_id="dynamic_task_mapping",
    schedule="0 8 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dynamic-mapping"],
)
def dynamic_mapping():

    # --- 1. Basic expand() with a list ---
    @task
    def get_file_list() -> list[str]:
        """Runtime resolution: returns variable-length list."""
        return ["orders_2024.parquet", "returns_2024.parquet", "shipments_2024.parquet"]

    @task
    def process_file(file_path: str) -> dict:
        """Mapped once per item from upstream list."""
        row_count = len(file_path) * 1000  # placeholder
        return {"file": file_path, "rows": row_count}

    files = get_file_list()
    processed = process_file.expand(file_path=files)

    # --- 2. partial() for shared params ---
    @task
    def load_to_warehouse(record: dict, target_schema: str, conn_id: str) -> str:
        """partial() pins target_schema and conn_id; expand() iterates records."""
        return f"Loaded {record['file']} -> {target_schema} via {conn_id}"

    loaded = load_to_warehouse.partial(
        target_schema="staging",
        conn_id="snowflake_default",
    ).expand(record=processed)

    # --- 3. TaskGroup with dynamic mapping ---
    @task
    def get_sources() -> list[dict]:
        return [
            {"name": "postgres_orders", "conn": "pg_main", "table": "orders"},
            {"name": "postgres_returns", "conn": "pg_main", "table": "returns"},
            {"name": "mysql_inventory", "conn": "mysql_wh", "table": "stock"},
        ]

    @task
    def extract(source: dict) -> dict:
        return {"source": source["name"], "rows": 50_000}

    @task
    def validate(result: dict) -> dict:
        result["valid"] = result["rows"] > 0
        return result

    with TaskGroup("ingest_sources") as ingest_group:
        sources = get_sources()
        extracted = extract.expand(source=sources)
        validated = validate.expand(result=extracted)

    # --- 4. Fan-out / fan-in pattern ---
    @task
    def aggregate(results: list[str]) -> str:
        """Fan-in: collects all mapped outputs into a single list."""
        return f"Aggregated {len(results)} loads"

    summary = aggregate(results=loaded)

    # --- 5. Mapped task with zip (cross-product alternative) ---
    @task
    def merge_datasets(table: str, partition: str) -> str:
        return f"Merged {table}/{partition}"

    tables = ["orders", "returns"]
    partitions = ["2024-01", "2024-02"]

    merge_datasets.expand_kwargs(
        [{"table": t, "partition": p} for t, p in zip(tables, partitions)]
    )

    # Execution order
    validated >> summary


dynamic_mapping()
```

## Configuration

| Feature | Minimum Version | Notes |
|---------|----------------|-------|
| `expand()` | Airflow 2.3+ | Replaces deprecated `map()` |
| `partial()` | Airflow 2.3+ | Pin shared kwargs before expand |
| `expand_kwargs()` | Airflow 2.3+ | Zip-style multi-param mapping |
| `TaskGroup` + mapping | Airflow 2.4+ | Nested groups with dynamic tasks |
| Max mapped tasks | `max_map_length` in airflow.cfg | Default 1024; raise for large fans |

## Example Usage

```python
# Minimal dynamic mapping — process S3 partitions at runtime
@task
def list_partitions() -> list[str]:
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    hook = S3Hook(aws_conn_id="aws_default")
    keys = hook.list_keys(bucket_name="lake", prefix="raw/orders/")
    return [k for k in keys if k.endswith(".parquet")]

@task
def transform(partition_key: str) -> str:
    return f"Transformed {partition_key}"

transform.expand(partition_key=list_partitions())
```

## See Also

- [dag-design](../concepts/dag-design.md)
- [task-dependencies](../concepts/task-dependencies.md)
- [dag-factory](../patterns/dag-factory.md)
- [error-handling](../patterns/error-handling.md)
