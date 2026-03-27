# Iceberg Operations

> **Purpose**: CREATE TABLE, MERGE INTO, time travel, partition evolution, compaction, snapshot management
> **Confidence**: 0.90
> **MCP Validated**: 2026-03-26

## Overview

Core Iceberg table operations for production use: table creation with hidden partitioning, upsert via MERGE, time travel queries, and maintenance procedures (compaction, snapshot expiration, orphan removal).

## The Pattern

```sql
-- ============================================================
-- Iceberg Table Lifecycle: Create → Write → Query → Maintain
-- ============================================================

-- 1. CREATE with hidden partitioning
CREATE TABLE catalog.bronze.raw_events (
    event_id        STRING NOT NULL,
    user_id         STRING NOT NULL,
    event_type      STRING NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    properties      MAP<STRING, STRING>,
    _ingested_at    TIMESTAMP NOT NULL
)
USING iceberg
PARTITIONED BY (days(event_timestamp), bucket(8, user_id))
TBLPROPERTIES (
    'format-version' = '3',
    'write.parquet.compression-codec' = 'zstd',
    'commit.retry.num-retries' = '4'
);

-- 2. MERGE INTO (upsert pattern)
MERGE INTO catalog.silver.dim_users AS target
USING catalog.bronze.staging_users AS source
ON target.user_id = source.user_id
WHEN MATCHED AND source.updated_at > target.updated_at THEN
    UPDATE SET
        target.user_name = source.user_name,
        target.email = source.email,
        target.updated_at = source.updated_at
WHEN NOT MATCHED THEN
    INSERT (user_id, user_name, email, created_at, updated_at)
    VALUES (source.user_id, source.user_name, source.email, source.created_at, source.updated_at);

-- 3. INSERT OVERWRITE (partition-level replace)
INSERT OVERWRITE catalog.silver.daily_aggregates
SELECT
    DATE(event_timestamp) AS event_date,
    event_type,
    COUNT(*) AS event_count,
    COUNT(DISTINCT user_id) AS unique_users
FROM catalog.bronze.raw_events
WHERE DATE(event_timestamp) = '2026-03-26'
GROUP BY 1, 2;

-- 4. Time travel queries
SELECT * FROM catalog.bronze.raw_events VERSION AS OF 12345;           -- by snapshot ID
SELECT * FROM catalog.bronze.raw_events TIMESTAMP AS OF '2026-03-25'; -- by timestamp

-- 5. Partition evolution
ALTER TABLE catalog.bronze.raw_events DROP PARTITION FIELD days(event_timestamp);
ALTER TABLE catalog.bronze.raw_events ADD PARTITION FIELD hours(event_timestamp);

-- 6. Maintenance: compaction (rewrite small files)
CALL catalog.system.rewrite_data_files(
    table => 'catalog.bronze.raw_events',
    strategy => 'sort',
    sort_order => 'event_timestamp ASC'
);

-- 7. Expire old snapshots (free metadata storage)
CALL catalog.system.expire_snapshots(
    table => 'catalog.bronze.raw_events',
    older_than => TIMESTAMP '2026-03-19',  -- keep 7 days
    retain_last => 10
);

-- 8. Remove orphan files (unreferenced data files)
CALL catalog.system.remove_orphan_files(
    table => 'catalog.bronze.raw_events',
    older_than => TIMESTAMP '2026-03-19'
);
```

## Quick Reference

| Operation | Command | Frequency |
|-----------|---------|-----------|
| Compaction | `rewrite_data_files` | Daily or when small file ratio > 50% |
| Snapshot expiration | `expire_snapshots` | Daily, retain 7-30 days |
| Orphan removal | `remove_orphan_files` | Weekly |
| Metadata rewrite | `rewrite_manifests` | Monthly or after large schema changes |

## Related

- [iceberg-v3 concept](../concepts/iceberg-v3.md)
- [delta-operations](delta-operations.md)
- [migration-to-open-formats](migration-to-open-formats.md)
