# Incremental Strategies

> **Purpose**: Choose the right incremental materialization strategy for your data shape
> **Confidence**: 0.95
> **MCP Validated**: 2026-03-26 | Updated with microbatch strategy (v1.9+)

## Overview

Incremental models process only new or changed data instead of rebuilding entire tables. dbt supports five strategies: **append** (insert-only), **merge** (upsert), **delete+insert** (replace matching rows), **insert_overwrite** (replace partitions), and **microbatch** (v1.9+, time-partitioned batch processing). Strategy selection depends on data mutability, volume, and warehouse capabilities.

## The Concept

```sql
-- Merge strategy: upsert rows by unique key
{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

select
    order_id,
    customer_id,
    order_total,
    status,
    updated_at
from {{ ref('stg_orders') }}

{% if is_incremental() %}
    -- Only process rows newer than the latest in target
    where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
```

## Microbatch Strategy (v1.9+)

```sql
-- Microbatch: dbt runs separate queries for each time batch
-- No is_incremental() logic needed — dbt handles it automatically
{{
    config(
        materialized='incremental',
        incremental_strategy='microbatch',
        event_time='event_timestamp',   -- required: time column for batching
        begin='2024-01-01',             -- earliest data to process
        batch_size='day',               -- hour, day, or month
        lookback=1                      -- re-process N previous batches for late data
    )
}}

select
    event_id,
    user_id,
    event_type,
    event_timestamp,
    properties
from {{ ref('stg_events') }}
-- No is_incremental() block needed — microbatch handles time filtering
```

## Quick Reference

| Strategy | Mutability | Unique Key | Best Warehouse | Volume Threshold |
|----------|-----------|------------|---------------|-----------------|
| `append` | Immutable events | Not needed | Any | Any |
| `merge` | Mutable rows | Required | Snowflake, BigQuery | < 100M rows |
| `delete+insert` | Mutable rows | Required | Redshift, Postgres | > 100M rows |
| `insert_overwrite` | Partition-level | Partition key | BigQuery, Spark | Any (partition) |
| `microbatch` | Time-series | `event_time` col | Any (v1.9+) | Any |

### Microbatch vs Traditional Incremental

| Aspect | Traditional Incremental | Microbatch |
|--------|------------------------|------------|
| Query structure | Single SQL for all new data | Separate SQL per time batch |
| User responsibility | Write `is_incremental()` logic | No conditional logic needed |
| Backfill | `--full-refresh` (rebuilds all) | Automatic per-batch backfill |
| Late-arriving data | Manual lookback window | `lookback` parameter |
| Batch granularity | User-defined | `hour`, `day`, or `month` |
| Retry on failure | Reruns all new data | Retries only the failed batch |

## Common Mistakes

### Wrong

```sql
-- No is_incremental() guard = full table scan every run
{{
    config(materialized='incremental', unique_key='event_id')
}}
select * from {{ ref('stg_events') }}
-- Missing WHERE clause for incremental filter!
```

### Correct

```sql
{{
    config(materialized='incremental', unique_key='event_id')
}}
select * from {{ ref('stg_events') }}
{% if is_incremental() %}
    where event_timestamp > (select max(event_timestamp) from {{ this }})
{% endif %}
```

## Related

- [model-types](../concepts/model-types.md)
- [incremental-model](../patterns/incremental-model.md)
