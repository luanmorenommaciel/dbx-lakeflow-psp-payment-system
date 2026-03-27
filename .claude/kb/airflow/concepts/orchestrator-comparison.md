# Orchestrator Comparison

> **Purpose**: Airflow 3.x vs Dagster vs Prefect 3.x — decision matrix with trade-offs
> **Confidence**: 0.90
> **MCP Validated**: 2026-03-26 | Updated for Airflow 3.0 GA (April 2025)

## Overview

The three leading Python orchestrators each have a distinct philosophy: **Airflow 3.x** is now a **task + asset hybrid** (schedule tasks on a timeline OR trigger on asset updates), **Dagster** is asset-centric (define data assets and their dependencies), and **Prefect** is workflow-centric (decorate Python functions with retry/scheduling). Airflow 3.0's addition of asset-aware scheduling and remote execution significantly narrows the gap with Dagster on data lineage and with Prefect on ease of deployment.

## The Concept

```python
# Same pipeline in three orchestrators

# --- AIRFLOW 3.x ---
from airflow.sdk import DAG, task
@dag(schedule="@daily")
def orders_pipeline():
    @task()
    def extract(): ...
    @task()
    def transform(data): ...
    transform(extract())

# --- DAGSTER ---
from dagster import asset
@asset
def raw_orders():
    """Extract orders from API."""
    return extract_from_api()

@asset(deps=[raw_orders])
def clean_orders(raw_orders):
    """Transform raw orders."""
    return transform(raw_orders)

# --- PREFECT 3.x ---
from prefect import flow, task
@task(retries=2)
def extract(): ...
@task
def transform(data): ...

@flow(name="orders-pipeline")
def orders_pipeline():
    data = extract()
    transform(data)
```

## Quick Reference

| Dimension | Airflow 3.x | Dagster | Prefect 3.x |
|-----------|------------|---------|-------------|
| **Mental model** | Tasks + Assets hybrid | Data assets + lineage | Decorated Python functions |
| **Scheduling** | Cron, **assets (AND/OR)**, sensors | Cron, sensors, freshness policies | Cron, event-driven |
| **Dynamic tasks** | `expand()` / `map()` | `@multi_asset`, `DynamicPartitionsDefinition` | `.map()` on tasks |
| **Testing** | Improved (still needs context) | First-class (`materialize()` in pytest) | Easy (call functions directly) |
| **UI** | **Modern React UI** (dark mode) | Modern, asset-focused | Clean, flow-focused |
| **DAG versioning** | **Built-in (3.0+)** | Built-in | Via Git |
| **Remote execution** | **Task Execution API** | Built-in | Cloud-native |
| **Deployment** | Self-hosted, MWAA, Astronomer | Self-hosted, Dagster Cloud | Self-hosted, Prefect Cloud |
| **dbt integration** | Provider package | `dagster-dbt` (asset-level) | `prefect-dbt` |
| **Learning curve** | Medium | Medium | Low |
| **Community size** | Largest (30M+ monthly downloads) | Growing fast | Medium |
| **Best for** | Enterprise-scale, hybrid cloud, MLOps | Data-asset-centric teams, testing-heavy | Small teams, Python-first |

## Common Mistakes

### Wrong

```text
Choosing Airflow because "everyone uses it" without considering:
- Team already thinks in data assets → Dagster is natural
- Small team, simple pipelines → Prefect is faster to adopt
- Heavy testing requirements → Dagster's materialize() is unmatched
```

### Correct

```text
Decision framework:
1. Team > 10 DE + existing Airflow? → Upgrade to Airflow 3.0 (assets + versioning)
2. Asset-centric + strong testing culture? → Dagster
3. Small team + Python-first + fast iteration? → Prefect
4. Regulated industry + audit trail? → Airflow 3.x (DAG versioning + mature logging)
5. Multi-cloud/hybrid execution? → Airflow 3.x (Task Execution API)
6. MLOps + GenAI workflows? → Airflow 3.x (30% of users, growing fast)
```

## Related

- [dag-design](../concepts/dag-design.md)
- [dag-factory](../patterns/dag-factory.md)
