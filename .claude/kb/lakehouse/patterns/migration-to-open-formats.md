# Migration to Open Formats

> **Purpose**: Hive to Iceberg, Parquet to Delta, dual-write validation, data comparison
> **Confidence**: 0.90
> **MCP Validated**: 2026-03-26

## Overview

Migrating from legacy formats (Hive, raw Parquet) to open table formats (Iceberg, Delta). Iceberg supports in-place migration (metadata-only, no data rewrite). Delta uses CONVERT TO DELTA. Both require validation to ensure data integrity post-migration.

## The Pattern

```sql
-- ============================================================
-- Strategy 1: Hive/Parquet → Iceberg (In-Place Migration)
-- No data rewrite — adds Iceberg metadata on top of existing Parquet
-- ============================================================

-- Spark SQL: migrate Hive table to Iceberg
CALL catalog.system.migrate('hive_catalog.analytics.orders');

-- Verify: table is now Iceberg with original data intact
SELECT COUNT(*) FROM catalog.analytics.orders;
DESCRIBE EXTENDED catalog.analytics.orders;
-- Should show: Provider = iceberg

-- Add Iceberg features post-migration
ALTER TABLE catalog.analytics.orders ADD PARTITION FIELD days(order_date);
ALTER TABLE catalog.analytics.orders SET TBLPROPERTIES (
    'format-version' = '3',
    'write.delete.mode' = 'merge-on-read'
);

-- ============================================================
-- Strategy 2: Parquet → Delta (CONVERT TO DELTA)
-- ============================================================

-- Convert existing Parquet directory to Delta
CONVERT TO DELTA parquet.`s3://data-lake/raw/orders/`
PARTITIONED BY (order_date DATE);

-- Register as managed table
CREATE TABLE catalog.bronze.orders
USING delta
LOCATION 's3://data-lake/raw/orders/';

-- Enable features
ALTER TABLE catalog.bronze.orders SET TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.enableDeletionVectors' = 'true'
);

-- ============================================================
-- Validation: Compare source vs target
-- ============================================================

-- Row count validation
SELECT 'source' AS dataset, COUNT(*) AS row_count FROM hive_catalog.analytics.orders
UNION ALL
SELECT 'target' AS dataset, COUNT(*) AS row_count FROM catalog.analytics.orders;

-- Checksum validation (column-level)
SELECT
    'source' AS dataset,
    COUNT(*) AS rows,
    SUM(HASH(order_id)) AS id_checksum,
    SUM(total_amount) AS amount_sum,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM hive_catalog.analytics.orders
UNION ALL
SELECT
    'target' AS dataset,
    COUNT(*) AS rows,
    SUM(HASH(order_id)) AS id_checksum,
    SUM(total_amount) AS amount_sum,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM catalog.analytics.orders;

-- Diff query: find mismatched rows
SELECT * FROM hive_catalog.analytics.orders
EXCEPT
SELECT * FROM catalog.analytics.orders;
```

## Quick Reference

| Migration Path | Method | Data Rewrite? | Downtime |
|---------------|--------|--------------|----------|
| Hive → Iceberg | `migrate()` procedure | No (metadata only) | Seconds |
| Parquet → Iceberg | `add_files()` procedure | No (metadata only) | Seconds |
| Parquet → Delta | `CONVERT TO DELTA` | No (adds _delta_log) | Seconds |
| Hive → Delta | CTAS or streaming copy | Yes (full rewrite) | Minutes-hours |
| Any → Any (large) | Dual-write + cutover | Yes (parallel writes) | Zero (blue-green) |

## Related

- [iceberg-operations](iceberg-operations.md)
- [delta-operations](delta-operations.md)
- [catalog-setup](catalog-setup.md)
