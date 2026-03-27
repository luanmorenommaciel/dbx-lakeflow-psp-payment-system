# Cross-Platform Cost Optimization

> **Purpose:** Cost control patterns across Snowflake, Databricks, and BigQuery -- compute sizing, storage tiering, and query cost estimation
> **Confidence:** 0.90
> **MCP Validated:** 2026-03-26

## Overview

Cloud data platform costs are driven by compute (warehouses, clusters, slots), storage (active, failsafe, long-term), and data transfer. Each platform has different pricing models and levers. Proactive cost management requires auto-suspend policies, right-sizing compute, choosing the correct pricing model, and monitoring query-level spend.

## The Pattern

### Auto-Suspend and Resume

```sql
-- Snowflake: auto-suspend after 60 seconds of inactivity
ALTER WAREHOUSE transform_wh SET
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

-- Snowflake: resource monitor to cap monthly spend
CREATE OR REPLACE RESOURCE MONITOR monthly_limit
  WITH CREDIT_QUOTA = 5000
  TRIGGERS
    ON 75 PERCENT DO NOTIFY
    ON 90 PERCENT DO NOTIFY
    ON 100 PERCENT DO SUSPEND;

ALTER WAREHOUSE analytics_wh SET RESOURCE_MONITOR = monthly_limit;
```

```python
# Databricks: auto-termination on interactive clusters (minutes)
cluster_config = {
    "autotermination_minutes": 15,
    "autoscale": {"min_workers": 1, "max_workers": 8},
    "spark_conf": {
        "spark.databricks.cluster.profile": "serverless"
    }
}

# Databricks: use SQL Warehouses for BI (auto-suspend built in)
# SQL Warehouse auto-stops after configurable idle period (default: 10 min)
```

```sql
-- BigQuery: no compute to suspend (serverless)
-- Control cost via: reservations, custom quotas, max_bytes_billed

-- Estimate query cost before running (dry run via API or console)
-- In SQL: use max_bytes_billed to cap expensive queries
SELECT * FROM `project.dataset.large_table`
OPTIONS(max_bytes_billed = 10737418240);  -- 10 GB limit
```

### Pricing Model Selection

| Platform | On-Demand | Committed | When to Switch |
|----------|-----------|-----------|---------------|
| Snowflake | Per-second credits | Capacity commitment (1-3yr) | Steady usage > 30% utilization |
| Databricks | Per-DBU (pay-as-you-go) | Committed DBU (1yr) | Predictable workloads |
| BigQuery | Per-TB scanned ($7.50/TB) | Slot reservations (autoscale) | Spend > $1K/month |

### Compute Sizing Formulas

```text
Snowflake Sizing:
  Start with SMALL warehouse for all workloads
  If query P95 > 60s → try MEDIUM (2x compute, ~same cost per query)
  If concurrent users > 10 → add multi-cluster (scaling policy: STANDARD)
  Rule: Double warehouse size halves execution time (linear scaling)
  Monitor: SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY;

Databricks Sizing:
  Interactive clusters: 2-8 workers with autoscale
  Job clusters: fixed size based on data volume
  Rule: 1 worker per 50-100 GB of shuffle data
  Use Photon-enabled runtimes for SQL-heavy workloads (2-5x faster)
  Monitor: SELECT * FROM system.billing.usage;

BigQuery Sizing:
  Autoscale slots: set baseline + max slots
  Baseline = avg concurrent queries * slots_per_query (typically 500-2000)
  Max = peak concurrency * slots_per_query
  Rule: Start with 100 baseline slots, autoscale to 500
  Monitor: INFORMATION_SCHEMA.JOBS for bytes_processed per query
```

### Storage Tiering

| Tier | Snowflake | Databricks | BigQuery |
|------|-----------|------------|----------|
| Hot (active) | $40/TB/mo | Cloud storage cost | $0.02/GB/mo |
| Warm (90d+) | $40/TB/mo | Same (cloud storage) | $0.01/GB/mo (long-term) |
| Cold (archive) | N/A (manual offload) | Glacier/Cold tier | $0.01/GB/mo (auto after 90d) |
| Time travel | Included (1-90d) | Delta log retention | N/A (use snapshots) |
| Failsafe | 7 days (extra cost) | N/A | N/A |

### Query Cost Monitoring

```sql
-- Snowflake: find expensive queries
SELECT
  query_id,
  user_name,
  warehouse_name,
  total_elapsed_time / 1000 AS duration_sec,
  credits_used_cloud_services,
  bytes_scanned / POWER(1024, 3) AS gb_scanned
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY credits_used_cloud_services DESC NULLS LAST
LIMIT 20;

-- BigQuery: find expensive queries (last 7 days)
SELECT
  job_id,
  user_email,
  total_bytes_processed / POWER(1024, 4) AS tb_processed,
  total_bytes_processed / POWER(1024, 4) * 7.50 AS estimated_cost_usd,
  total_slot_ms / 1000 AS slot_seconds
FROM `region-us`.INFORMATION_SCHEMA.JOBS
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
ORDER BY total_bytes_processed DESC
LIMIT 20;
```

## Quick Reference

| Lever | Snowflake | Databricks | BigQuery |
|-------|-----------|------------|----------|
| Auto-suspend | `AUTO_SUSPEND = 60` | `autotermination_minutes` | N/A (serverless) |
| Spend cap | Resource Monitor | Budget alerts | `max_bytes_billed` |
| Right-size | Warehouse size | Worker count + type | Slot reservation |
| Cost view | `WAREHOUSE_METERING_HISTORY` | `system.billing.usage` | `INFORMATION_SCHEMA.JOBS` |
| Partitioning | Micro-partitions (auto) | Delta ZORDER/OPTIMIZE | Partition by date column |

## Common Mistakes

### Wrong
```sql
-- Snowflake: leaving a 4XL warehouse running overnight with AUTO_SUSPEND = 3600
ALTER WAREHOUSE reporting_wh SET WAREHOUSE_SIZE = '4X-LARGE' AUTO_SUSPEND = 3600;
```

### Correct
```sql
-- Scale up for the job, then scale back down; use aggressive auto-suspend
ALTER WAREHOUSE reporting_wh SET WAREHOUSE_SIZE = '4X-LARGE' AUTO_SUSPEND = 60;
-- After batch completes, resize for ad-hoc queries
ALTER WAREHOUSE reporting_wh SET WAREHOUSE_SIZE = 'MEDIUM';
```

## Related

- [Snowflake Patterns](snowflake-patterns.md) -- Warehouse sizing and compute patterns
- [Databricks Patterns](databricks-patterns.md) -- Cluster and DBU management
- [BigQuery Patterns](bigquery-patterns.md) -- Slot reservations and partitioning
