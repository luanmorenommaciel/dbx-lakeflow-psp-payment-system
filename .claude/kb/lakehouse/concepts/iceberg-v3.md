# Iceberg v3

> **Purpose**: Apache Iceberg v3 — manifest files, partition evolution, hidden partitioning, row-level deletes, REST catalog
> **Confidence**: 0.90
> **MCP Validated**: 2026-03-26

## Overview

Apache Iceberg is an open table format that brings ACID transactions, schema evolution, and partition evolution to data lakes. v3 adds row-level deletes (position + equality) and improved metadata handling. Hidden partitioning decouples physical layout from query predicates — users filter on `event_date`, the engine translates to `days(event_date)` partitioning.

## The Concept

```sql
-- Spark SQL: Create Iceberg v3 table with hidden partitioning
CREATE TABLE catalog.analytics.events (
    event_id        STRING,
    user_id         STRING,
    event_type      STRING,
    event_timestamp TIMESTAMP,
    payload         STRING,
    processed_at    TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(event_timestamp), bucket(16, user_id))
TBLPROPERTIES (
    'format-version' = '3',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read'
);

-- Partition evolution: change strategy without rewriting data
ALTER TABLE catalog.analytics.events
  DROP PARTITION FIELD days(event_timestamp);
ALTER TABLE catalog.analytics.events
  ADD PARTITION FIELD hours(event_timestamp);
-- Old data stays in day partitions; new data writes to hour partitions
-- Queries span both seamlessly via metadata

-- Row-level delete (v3 position deletes)
DELETE FROM catalog.analytics.events
WHERE user_id = 'user-to-forget' AND event_timestamp < '2025-01-01';
```

## Quick Reference

| Feature | Iceberg v2 | Iceberg v3 |
|---------|-----------|-----------|
| Row-level deletes | Position + equality deletes | **Binary deletion vectors** (bitmap-based, faster) |
| Delete mode | copy-on-write or merge-on-read | merge-on-read with deletion vectors |
| Partition evolution | Yes | Yes (improved metadata) |
| Hidden partitioning | Yes | Yes + **multi-argument transforms** |
| Schema evolution | Full | Full + **default column values** |
| New types | — | **Variant** (semi-structured), **Geometry/Geography** (geospatial), nanosecond timestamps |
| Row lineage | No | **Yes** (track row origin across writes) |
| REST Catalog | Community spec | Apache standard (Polaris TLP) |
| Engine support | Spark, Trino, Flink | Spark, Trino, Flink, DuckDB, Snowflake, Dremio, StarRocks |

### Iceberg v4 (Proposed, in development)

| Proposal | Purpose |
|----------|---------|
| Content-addressable metadata | Deduplicate metadata files, reduce storage |
| Streaming-friendly commits | Lower commit latency for real-time ingestion |
| Improved statistics | Better min/max/NDV stats for query planning |
| Relative file paths | Simplified table relocation and migration |
| Metadata compression | Reduce metadata overhead for large tables |

| Metadata Layer | Purpose |
|---------------|---------|
| Catalog | Tracks current metadata pointer |
| Metadata file | Schema, partition spec, properties |
| Manifest list | Points to manifest files per snapshot |
| Manifest file | Lists data files with stats (min/max/null counts) |
| Data file | Actual Parquet/ORC/Avro files |

## Common Mistakes

### Wrong

```sql
-- Explicit partitioning exposes layout to users
CREATE TABLE events (
    event_date DATE,  -- user must filter on this exact column
    ...
) PARTITIONED BY (event_date);
```

### Correct

```sql
-- Hidden partitioning: users filter on event_timestamp, engine handles layout
CREATE TABLE events (
    event_timestamp TIMESTAMP,
    ...
) USING iceberg
PARTITIONED BY (days(event_timestamp));
-- SELECT * FROM events WHERE event_timestamp > '2026-01-01' → auto-pruned
```

## Related

- [delta-lake](delta-lake.md)
- [catalog-wars](catalog-wars.md)
- [iceberg-operations pattern](../patterns/iceberg-operations.md)
