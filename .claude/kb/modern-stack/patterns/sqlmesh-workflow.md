# SQLMesh Workflow

> **Purpose**: SQLMesh project setup, MODEL definitions, plan/apply cycle, virtual environments, audits, column lineage
> **MCP Validated**: 2026-03-26

## When to Use

- Starting a new SQLMesh project or migrating from dbt
- Defining models with automatic change detection
- Testing changes in virtual environments before production
- Running data quality audits as part of model definitions

## Implementation

```bash
# ============================================================
# Project setup
# ============================================================
pip install sqlmesh

# Initialize new project
sqlmesh init my_project

# Initialize from existing dbt project
sqlmesh init my_project -t dbt

# Project structure:
# my_project/
# ├── config.yaml
# ├── models/
# │   ├── staging/
# │   ├── intermediate/
# │   └── marts/
# ├── audits/
# ├── tests/
# └── macros/
```

```yaml
# config.yaml
gateways:
  local:
    connection:
      type: duckdb
      database: data/warehouse.db
  prod:
    connection:
      type: snowflake
      account: myorg-myaccount
      database: analytics
      warehouse: transform_wh

default_gateway: local

model_defaults:
  dialect: duckdb
  start: "2024-01-01"
```

```sql
-- ============================================================
-- MODEL definition (models/staging/stg_orders.sql)
-- ============================================================
MODEL (
    name staging.stg_orders,
    kind INCREMENTAL_BY_TIME_RANGE (
        time_column order_date,
        batch_size 7,              -- process 7 days per batch
    ),
    grain (order_id),
    cron '@daily',
    owner 'data-team',
    tags ['staging', 'orders'],
    audits (
        not_null(columns := [order_id, customer_id, order_date]),
        unique_values(columns := [order_id]),
        accepted_range(column := net_amount, min_v := 0),
    ),
);

SELECT
    order_id,
    customer_id,
    order_date,
    CAST(net_amount AS DECIMAL(18, 2)) AS net_amount,
    status,
    CURRENT_TIMESTAMP AS _loaded_at
FROM raw.orders
WHERE order_date BETWEEN @start_date AND @end_date;

-- ============================================================
-- MODEL with @DEF macros (models/marts/mart_revenue.sql)
-- ============================================================
@DEF(revenue_threshold, 1000);

MODEL (
    name marts.mart_revenue,
    kind FULL,
    grain (customer_id, revenue_month),
    cron '@daily',
    audits (
        not_null(columns := [customer_id, total_revenue]),
    ),
);

SELECT
    customer_id,
    DATE_TRUNC('month', order_date) AS revenue_month,
    SUM(net_amount) AS total_revenue,
    COUNT(DISTINCT order_id) AS order_count,
    AVG(net_amount) AS avg_order_value
FROM staging.stg_orders
WHERE net_amount >= @revenue_threshold
GROUP BY customer_id, DATE_TRUNC('month', order_date);
```

```bash
# ============================================================
# Plan/Apply workflow
# ============================================================

# Preview changes (like terraform plan)
sqlmesh plan
# Output:
# Models:
#   Directly Modified:
#     staging.stg_orders (Non-breaking)
#   Indirectly Modified:
#     marts.mart_revenue (Indirect Non-breaking)
# Apply - Backfill Tables [y/n]:

# Apply changes
sqlmesh plan --auto-apply

# ============================================================
# Virtual environments (zero-cost dev)
# ============================================================
# Create dev environment (points to same physical tables via views)
sqlmesh plan dev
sqlmesh apply dev

# Test in dev (instant, no data copy)
sqlmesh fetchdf "SELECT * FROM dev.marts.mart_revenue LIMIT 10"

# Promote dev → prod
sqlmesh plan prod
sqlmesh apply prod

# ============================================================
# Column-level lineage
# ============================================================
sqlmesh lineage marts.mart_revenue.total_revenue
# Output:
# marts.mart_revenue.total_revenue
#   ← staging.stg_orders.net_amount (SUM)
#     ← raw.orders.net_amount (CAST)

# ============================================================
# Run audits manually
# ============================================================
sqlmesh audit
```

## Configuration

| Model Kind | Description | Backfill |
|------------|-------------|----------|
| `FULL` | Full table replacement | Always full |
| `INCREMENTAL_BY_TIME_RANGE` | Append by date range | Only new ranges |
| `INCREMENTAL_BY_UNIQUE_KEY` | Upsert by key | Changed rows only |
| `VIEW` | Logical view (no materialization) | N/A |
| `SEED` | Static CSV data | On change |

## See Also

- [sqlmesh](../concepts/sqlmesh.md)
- [duckdb-patterns](../patterns/duckdb-patterns.md)
- [analytics-engineering](../concepts/analytics-engineering.md)
