# Delta Operations

> **Purpose**: MERGE INTO, OPTIMIZE, VACUUM, change data feed, UniForm, liquid clustering
> **Confidence**: 0.90
> **MCP Validated**: 2026-03-26

## Overview

Core Delta Lake operations for production: upsert with MERGE, optimization with liquid clustering, cleanup with VACUUM, change tracking with Change Data Feed, and cross-format compatibility with UniForm.

## The Pattern

```sql
-- ============================================================
-- Delta Lake Operations: CRUD → Optimize → Monitor
-- ============================================================

-- 1. Create table with liquid clustering + UniForm
CREATE TABLE catalog.silver.orders (
    order_id      STRING NOT NULL,
    customer_id   STRING NOT NULL,
    order_date    DATE NOT NULL,
    total_amount  DECIMAL(12,2),
    status        STRING,
    region        STRING,
    _updated_at   TIMESTAMP
)
USING delta
CLUSTER BY (region, order_date)
TBLPROPERTIES (
    'delta.enableDeletionVectors' = 'true',
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.enableChangeDataFeed' = 'true',
    'delta.logRetentionDuration' = 'interval 30 days',
    'delta.deletedFileRetentionDuration' = 'interval 7 days'
);

-- 2. MERGE INTO (upsert with matched/not-matched)
MERGE INTO catalog.silver.orders AS target
USING catalog.bronze.staging_orders AS source
ON target.order_id = source.order_id
WHEN MATCHED AND source._updated_at > target._updated_at THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *
WHEN NOT MATCHED BY SOURCE AND target.order_date < CURRENT_DATE - INTERVAL 90 DAYS THEN
    DELETE;

-- 3. OPTIMIZE (liquid clustering auto-applies)
OPTIMIZE catalog.silver.orders;
-- For legacy tables without liquid clustering:
-- OPTIMIZE catalog.silver.orders ZORDER BY (region, order_date);

-- 4. VACUUM (clean up old files)
VACUUM catalog.silver.orders RETAIN 168 HOURS;

-- 5. Change Data Feed (read changes)
-- Get all changes since version 10
SELECT * FROM table_changes('catalog.silver.orders', 10)
WHERE _change_type IN ('insert', 'update_postimage', 'delete');

-- Incremental downstream processing
SELECT
    _change_type,
    order_id,
    customer_id,
    total_amount,
    _commit_version,
    _commit_timestamp
FROM table_changes('catalog.silver.orders', 10, 20)
ORDER BY _commit_version;

-- 6. Time travel
SELECT * FROM catalog.silver.orders VERSION AS OF 5;
SELECT * FROM catalog.silver.orders TIMESTAMP AS OF '2026-03-25';
RESTORE TABLE catalog.silver.orders TO VERSION AS OF 5;  -- rollback

-- 7. DESCRIBE history
DESCRIBE HISTORY catalog.silver.orders;
```

## Delta 4.x New Operations

```sql
-- Variant type: flexible semi-structured data
INSERT INTO catalog.silver.events
SELECT event_id, PARSE_JSON(raw_payload) AS event_data FROM staging;

-- Query Variant with path extraction
SELECT event_data:user.name::STRING, event_data:metrics.page_views::INT
FROM catalog.silver.events;

-- Type widening: widen column types without data rewrite
ALTER TABLE catalog.silver.orders SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true');
ALTER TABLE catalog.silver.orders ALTER COLUMN quantity TYPE BIGINT;

-- Conflict-free feature enablement (4.1): enable features without blocking writes
ALTER TABLE catalog.silver.orders SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true');
-- No longer blocks concurrent write operations (4.1+)

-- Identity columns: auto-incrementing surrogate keys
CREATE TABLE catalog.gold.dim_product (
    product_sk BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
    product_id STRING NOT NULL,
    product_name STRING NOT NULL
) USING delta;
```

## Quick Reference

| Operation | Command | When to Use |
|-----------|---------|------------|
| OPTIMIZE | `OPTIMIZE table` | Daily (or auto-optimize on Databricks) |
| VACUUM | `VACUUM table RETAIN N HOURS` | Daily, min 7 days retention |
| CDF query | `table_changes(table, start, end)` | Incremental downstream loads |
| Time travel | `VERSION AS OF N` | Debug, audit, rollback |
| RESTORE | `RESTORE TABLE TO VERSION AS OF N` | Emergency rollback |
| Type widen | `ALTER COLUMN col TYPE wider_type` | Schema evolution (4.0+) |
| Variant query | `col:path::TYPE` | Semi-structured data (4.0+) |
| Identity column | `GENERATED ALWAYS AS IDENTITY` | Surrogate keys (4.0+) |

## Related

- [delta-lake concept](../concepts/delta-lake.md)
- [iceberg-operations](iceberg-operations.md)
- [migration-to-open-formats](migration-to-open-formats.md)
