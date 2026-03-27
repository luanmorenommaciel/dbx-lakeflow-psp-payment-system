---
name: pipeline
description: DAG/pipeline scaffolding — delegates to pipeline-architect agent
---

# Pipeline Command

> Scaffold a data pipeline (Airflow, Dagster) with best-practice patterns

## Usage

```bash
/pipeline <description-or-file>
```

## Examples

```bash
/pipeline "Daily orders ETL from Postgres to Snowflake"
/pipeline "Kafka → staging → dbt → marts with hourly refresh"
/pipeline requirements/pipeline-spec.md
```

---

## What This Command Does

1. Invokes the **pipeline-architect** agent
2. Analyzes your description or requirements file
3. Loads KB patterns from `airflow` and `dbt` domains
4. Generates:
   - DAG structure (Airflow or Dagster)
   - Task definitions with dependencies
   - Error handling and retry configuration
   - Sensor/trigger patterns for scheduling

## Agent Delegation

| Agent | Role |
|-------|------|
| `pipeline-architect` | Primary — DAG design, task orchestration |
| `spark-engineer` | Escalation — when pipeline includes Spark jobs |
| `dbt-specialist` | Escalation — when pipeline includes dbt models |

## KB Domains Used

- `airflow` — DAG patterns, operators, sensors
- `dbt` — model execution, incremental strategies
- `data-quality` — quality gates between pipeline stages

## Output

The agent generates pipeline code files and a summary of the DAG structure with task dependencies.
