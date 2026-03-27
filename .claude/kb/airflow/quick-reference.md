# Airflow Quick Reference

> Fast lookup tables. For code examples, see linked files.
> MCP Validated: 2026-03-26 | Updated for Airflow 3.0 GA (April 2025)

## Operator Selection Matrix

| Task Type | Recommended Operator | Notes |
|-----------|---------------------|-------|
| Python function | `@task` (TaskFlow) | Modern default |
| Shell command | `BashOperator` | Simple CLI tasks |
| dbt run/test | `DbtCloudRunJobOperator` | Or BashOperator for dbt Core |
| Spark job | `SparkSubmitOperator` | Or `DatabricksSubmitRunOperator` |
| BigQuery SQL | `BigQueryInsertJobOperator` | Native GCP integration |
| Snowflake SQL | `SnowflakeOperator` | Via snowflake provider |
| Wait for file | `S3KeySensor` (deferrable) | Use deferrable=True |
| Wait for DAG | `ExternalTaskSensor` | Or dataset-driven scheduling |
| HTTP request | `SimpleHttpOperator` | For API calls |

## TaskFlow vs Classic

| Feature | Classic | TaskFlow API |
|---------|---------|-------------|
| Syntax | `PythonOperator(python_callable=fn)` | `@task def fn():` |
| XCom | `ti.xcom_push/pull` | Return value = automatic XCom |
| Dependencies | `task1 >> task2` | Same, or implicit from data flow |
| Dynamic mapping | `expand()` on operator | `fn.expand(arg=list)` |
| Readability | Verbose | Pythonic |

## Airflow 3.0 Key Changes (GA April 2025)

| Feature | Description |
|---------|-------------|
| **Asset-aware scheduling** | `@asset` decorator + `Asset()` for event-driven DAG triggering |
| **DAG Versioning** | Tracks and saves DAG code at each execution for debugging |
| **React-based UI** | New modern UI with dark mode, asset + task navigation views |
| **Task Execution API** | Decouples task execution from scheduler; enables remote execution |
| **Backfill improvements** | Run backfills within scheduler; launch from UI or API |
| **`airflow.sdk` imports** | New SDK module: `from airflow.sdk import DAG, task, asset` |
| **Dataset renamed to Asset** | `Dataset()` is now `Asset()` in Airflow 3.0 |
| **AND/OR asset logic** | `(asset1 & asset2)` or `(asset1 \| asset2)` for complex triggers |

## Schedule Intervals

| Expression | Meaning | Version |
|-----------|---------|---------|
| `@daily` | Midnight UTC | All |
| `@hourly` | Every hour | All |
| `0 6 * * 1-5` | 6 AM UTC weekdays | All |
| `Asset("s3://bucket/data")` | When upstream asset updates | **3.0+** |
| `Dataset("s3://bucket/data")` | When upstream dataset updates | 2.4+ (deprecated in 3.0) |
| `(asset1 & asset2)` | When BOTH assets update | **3.0+** |
| `(asset1 \| asset2)` | When EITHER asset updates | **3.0+** |
| `Timetable(...)` | Custom schedule logic | 2.2+ |

## Orchestrator Comparison

| Feature | Airflow 3.x | Dagster | Prefect 3.x |
|---------|------------|---------|-------------|
| Paradigm | Task + Asset hybrid | Asset-centric | Flow-centric |
| Learning curve | Medium | Medium-High | Low |
| UI quality | **Modern React UI** | Excellent | Good |
| Data lineage | **Asset-aware** | Built-in | Limited |
| Testing story | Improved | First-class | Good |
| Remote execution | **Task Execution API** | Built-in | Cloud-native |
| Community size | Largest (30M+ monthly downloads) | Growing fast | Medium |
| Ops burden | Medium (improved in 3.0) | Medium | Low (Cloud) |
| Best for | Enterprise-scale, hybrid execution | Data lineage focus | Python DX |

## Common Pitfalls

| Don't | Do |
|-------|-----|
| Top-level imports in DAG file | Import inside `@task` functions |
| Hardcode connections | Use `Connection` objects + Variables |
| Monolithic tasks (ETL in one) | One task per logical step |
| Skip retries | `retries=2, retry_delay=timedelta(minutes=5)` |
| Use sensors without deferrable | `deferrable=True` saves worker slots |

## Related Documentation

| Topic | Path |
|-------|------|
| Getting Started | `concepts/dag-design.md` |
| Full Index | `index.md` |
