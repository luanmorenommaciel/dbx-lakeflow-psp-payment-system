# Delta Lake

> **Purpose**: Delta Lake 4.0/4.1 — catalog-managed tables, Variant type, liquid clustering, coordinated commits, UniForm, type widening
> **Confidence**: 0.92
> **MCP Validated**: 2026-03-26

## Overview

Delta Lake is an open table format built on Parquet with ACID transactions via a JSON-based transaction log (`_delta_log/`). Delta Lake 4.0 (Sep 2025) was the largest release in Delta history, introducing catalog-managed tables, Variant data type, coordinated commits, identity columns, and collations. Delta Lake 4.1 (Mar 2026) added server-side planning, atomic CTAS, AWS storage credentials in Unity Catalog, and conflict-free feature enablement for deletion vectors and column mapping.

## The Concept

```sql
-- Create Delta table with liquid clustering (replaces ZORDER)
CREATE TABLE catalog.analytics.orders (
    order_id        STRING,
    customer_id     STRING,
    order_date      DATE,
    total_amount    DECIMAL(12,2),
    status          STRING,
    region          STRING
)
USING delta
CLUSTER BY (region, order_date)  -- liquid clustering
TBLPROPERTIES (
    'delta.enableDeletionVectors' = 'true',
    'delta.universalFormat.enabledFormats' = 'iceberg',  -- UniForm
    'delta.enableChangeDataFeed' = 'true'
);

-- OPTIMIZE with liquid clustering (no ZORDER needed)
OPTIMIZE catalog.analytics.orders;

-- Change Data Feed: read changes between versions
SELECT * FROM table_changes('catalog.analytics.orders', 5, 10)
WHERE _change_type IN ('insert', 'update_postimage');

-- Deletion vectors: fast logical delete (no rewrite)
DELETE FROM catalog.analytics.orders WHERE status = 'cancelled';
-- Marks rows as deleted in deletion vector file; OPTIMIZE later reclaims space

-- VACUUM: clean up old files
VACUUM catalog.analytics.orders RETAIN 168 HOURS;  -- 7 days
```

## Quick Reference

| Feature | Delta OSS | Delta (Databricks) |
|---------|----------|-------------------|
| Liquid clustering | v3.2+ | DBR 13.3+ |
| Deletion vectors | v3.1+ | DBR 14.1+ |
| UniForm (Iceberg compat) | v3.2+ | DBR 13.3+ |
| Change Data Feed | v2.0+ | All versions |
| **Catalog-managed tables** | **v4.0+** | **DBR 16.0+** |
| **Variant type** | **v4.0+** | **DBR 16.0+** |
| **Coordinated commits** | **v4.0+** | **DBR 15.4+** |
| **Identity columns** | **v4.0+** | **DBR 16.0+** |
| **Type widening (preview)** | **v4.0+** | **DBR 15.4+** |
| **Server-side planning** | **v4.1 (preview)** | **DBR 17.0+** |
| **Atomic CTAS (UC)** | **v4.1** | **DBR 17.0+** |
| **Collations** | **v4.0+** | **DBR 16.0+** |
| Photon acceleration | No | Yes |
| Predictive optimization | No | Yes (auto-OPTIMIZE, auto-VACUUM) |

| Transaction Log | Purpose |
|----------------|---------|
| `_delta_log/` | JSON commit files (0-9 commits) + checkpoint Parquet files |
| Checkpoint | Parquet snapshot every 10 commits (configurable) |
| Commit info | Schema, partition changes, file additions/removals per commit |
| Coordinated commits | Multi-writer safety via catalog-based commit coordination (4.0+) |

## Delta 4.0/4.1 New Features

```sql
-- Variant type: flexible semi-structured data with structured performance
CREATE TABLE catalog.analytics.events (
    event_id    STRING,
    user_id     STRING,
    event_data  VARIANT,  -- semi-structured (replaces STRING JSON blobs)
    event_ts    TIMESTAMP
) USING delta;

-- Query Variant data with dot notation
SELECT event_data:user_agent::STRING, event_data:page_views::INT
FROM catalog.analytics.events;

-- Catalog-managed tables: catalog is source of truth for table state
-- Enables server-side scan planning, centralized governance
CREATE TABLE catalog.silver.orders (...)
USING delta
TBLPROPERTIES ('delta.catalogManaged' = 'true');

-- Identity columns: auto-incrementing surrogate keys
CREATE TABLE catalog.gold.dim_customer (
    customer_sk BIGINT GENERATED ALWAYS AS IDENTITY,
    customer_id STRING,
    customer_name STRING
) USING delta;

-- Type widening: widen column types without rewriting data
ALTER TABLE catalog.silver.orders
SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true');
ALTER TABLE catalog.silver.orders ALTER COLUMN quantity TYPE BIGINT;  -- INT -> BIGINT

-- Coordinated commits: safe multi-writer across environments
ALTER TABLE catalog.silver.orders
SET TBLPROPERTIES ('delta.coordinatedCommits.commitCoordinatorName' = 'unity-catalog');
```

## Common Mistakes

### Wrong

```sql
-- Using ZORDER with liquid clustering (conflict)
OPTIMIZE orders ZORDER BY (region, order_date);  -- ERROR if liquid clustering enabled

-- VACUUM with too-short retention
VACUUM orders RETAIN 0 HOURS;  -- breaks time travel, active queries fail
```

### Correct

```sql
-- Liquid clustering: just run OPTIMIZE without ZORDER
OPTIMIZE orders;  -- clustering happens automatically

-- VACUUM with safe retention
VACUUM orders RETAIN 168 HOURS;  -- 7 days minimum
```

## Related

- [iceberg-v3](iceberg-v3.md)
- [catalog-wars](catalog-wars.md)
- [delta-operations pattern](../patterns/delta-operations.md)
