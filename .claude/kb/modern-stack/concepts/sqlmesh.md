# SQLMesh

> **Purpose**: SQLMesh virtual environments, change categories, automatic backfills, column-level lineage, plan/apply workflow
> **MCP Validated**: 2026-03-26

## Overview

SQLMesh is a data transformation framework that improves on dbt with virtual environments (no data duplication for dev/staging), automatic change classification, built-in scheduling, and column-level lineage. It can also run dbt projects via its compatibility layer, now including a dedicated `dbt` CLI wrapper.

**Key 2025 developments (v0.210 - v0.231):**
- **Microsoft Fabric Warehouse support** (Aug 2025) -- proper MERGE statements and alter table workarounds for Fabric
- **dbt CLI compatibility** -- `--project-dir`, `--profiles-dir`, `--log-level`, `--profile`, `--target` flags
- **Built-in linter rules** -- `NoMissingUnitTest` and others for code quality enforcement
- **VSCode multi-project support** -- work with multiple SQLMesh projects simultaneously
- **Table Diff view in VSCode** -- visual comparison of data changes
- **Dev-only VDE mode** -- virtual dev environments optimized for development workflows
- **`on_additive_change` support** -- handle additive model changes without full backfill
- **`ignore_destructive` support** -- safety guardrails for destructive schema changes
- **BigQuery materialized views** -- column types and comments support for Databricks MV

## Key Concepts

### Virtual Environments

Unlike dbt (which creates full schema copies for dev), SQLMesh uses virtual environments that point to the same physical tables via views. This means:
- Zero data duplication for dev/staging environments
- Instant environment creation
- Safe testing against production data without copies

### Change Categories

When you modify a model, SQLMesh classifies the change:

| Category | Description | Action |
|----------|-------------|--------|
| **Breaking** | Column removed, type changed, logic altered | Full backfill of model + all downstream |
| **Non-breaking** | New column added, comment changed | Forward-only (no backfill needed) |
| **Metadata-only** | Description, tags, owner changed | No data operation |

### Plan/Apply Workflow

```bash
# Preview what will change (like terraform plan)
sqlmesh plan dev

# Apply changes to dev environment
sqlmesh apply dev

# Promote dev → prod
sqlmesh plan prod
sqlmesh apply prod
```

### Column-Level Lineage

SQLMesh tracks lineage at the column level, not just the model level:

```bash
# Show which upstream columns feed into a specific column
sqlmesh lineage mart_revenue.total_amount

# Output:
# mart_revenue.total_amount
#   ← int_orders.net_amount
#     ← stg_orders.quantity × stg_orders.unit_price
```

### Built-in Scheduler

SQLMesh includes a scheduler (no Airflow required for simple setups):

```bash
# Run all models that need updating
sqlmesh run

# Run with Airflow integration
sqlmesh plan --enable-airflow
```

## SQLMesh vs dbt Comparison (2025)

| Feature | SQLMesh | dbt Core |
|---------|---------|----------|
| Environments | Virtual (no data copy) | Full schema clone |
| Change detection | Automatic (breaking/non-breaking) via SQLGlot | Manual (full refresh or incremental) |
| Backfill | Automatic, minimal (affected models only) | Manual via --full-refresh |
| Lineage | Column-level (native) | Model-level (Fusion for column-level) |
| Scheduler | Built-in | Requires Airflow/Dagster |
| Testing | Built-in audits + tests + linter rules | schema.yml tests + sqlfluff |
| SQL validation | Compile-time (SQLGlot parse) | Run-time (Jinja template) |
| dbt compatibility | Yes (CLI wrapper + adapter) | Native |
| IDE | VSCode extension (OSS) + web UI | dbt Cloud IDE or dbt Power User |
| Execution speed | ~9x faster (benchmarked) | Baseline (Fusion: 30x parse speed) |
| State management | Built-in, stateful | Stateless (relies on manifest) |
| Fabric Warehouse | Yes (Aug 2025) | Yes |

## When to Use

- **Choose SQLMesh** when: column-level lineage matters, you want zero-cost dev environments, you need automatic change classification, or you're starting fresh
- **Choose dbt** when: team already knows dbt, ecosystem packages are critical (dbt_utils, dbt_expectations), or dbt Cloud features needed (CI/CD, IDE, semantic layer)

## Trade-offs

| Pros | Cons |
|------|------|
| Virtual environments save storage/compute | Smaller community than dbt (3K GitHub stars vs 10K+) |
| Automatic change detection reduces errors | Fewer third-party packages |
| Column-level lineage for impact analysis | Learning curve for dbt users |
| Built-in scheduler simplifies stack | Less mature cloud offering |
| ~9x faster execution than dbt Core | Rapid release cadence may require frequent updates |
| Compile-time SQL validation catches errors earlier | SQLGlot parser may not support all SQL dialects perfectly |
| OSS VSCode extension with multi-project support | Ecosystem packages (dbt_utils equivalent) still limited |
| Built-in linter rules (NoMissingUnitTest) | State DB adds operational overhead |

## See Also

- [sqlmesh-workflow](../patterns/sqlmesh-workflow.md)
- [analytics-engineering](../concepts/analytics-engineering.md)
- [duckdb](../concepts/duckdb.md)
