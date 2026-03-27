> **MCP Validated:** 2026-02-17

# Direct Lake Mode

> **Purpose**: Direct Lake connectivity, Parquet reading, auto-sync, fallback, and framing behavior
> **Confidence**: 0.95

## Overview

Direct Lake is a semantic model storage mode unique to Microsoft Fabric. It reads Delta Parquet files directly from OneLake into the Analysis Services engine memory, combining the performance of Import mode with the freshness of DirectQuery. Unlike Import (which copies data) or DirectQuery (which queries the source live), Direct Lake memory-maps Parquet files with zero data movement.

## How It Works

```text
ONELAKE (Delta Parquet files)
  |
  +-- Lakehouse Tables / Warehouse Tables
  |       |
  |       +-- Parquet files (V-Order optimized)
  |               |
  |               +-- FRAMING (snapshot of file list)
  |                       |
  |                       +-- Analysis Services engine
  |                       |     reads Parquet directly
  |                       |     into memory (columnar)
  |                       |
  |                       +-- DAX queries execute
  |                           on in-memory data
  |
  +-- Auto-sync: New data detected --> reframe
```

## Key Concepts

### Framing

Framing is the process where Direct Lake takes a snapshot of the Delta log to determine which Parquet files to read. This "frame" defines the data visible to the semantic model.

| Trigger | Behavior |
|---------|----------|
| Scheduled refresh | Reframes on schedule (e.g., every hour) |
| Manual refresh | User triggers reframe in Fabric portal |
| Auto-sync | Detects Delta log changes, reframes automatically |
| XMLA endpoint | Reframe via `TMSL Refresh` command |

### Reframing Triggers

```text
Data written to Lakehouse/Warehouse table
  --> Delta log updated
  --> Direct Lake detects new commit
  --> Reframes (reads updated file list)
  --> Next DAX query uses new data
  --> No full data copy needed
```

### Fallback to DirectQuery

When Direct Lake cannot serve a query from memory, it **falls back** to DirectQuery against the SQL endpoint. This is transparent to the user but slower.

| Fallback Trigger | Cause | Mitigation |
|-----------------|-------|------------|
| Data exceeds memory | Too many rows/columns for capacity SKU | Increase SKU or reduce model scope |
| Unsupported DAX | Certain DAX patterns force fallback | Simplify measures |
| Column cardinality | Very high cardinality columns | Reduce cardinality or exclude column |
| Stale frame | Frame expired, data not in memory | Increase refresh frequency |

### Guardrails by SKU

| SKU | Max rows per table | Max model size (memory) | Max columns per table |
|-----|-------------------|------------------------|----------------------|
| F2 | 300 million | 3 GB | 2,000 |
| F8 | 300 million | 12 GB | 2,000 |
| F64 | 1.5 billion | 96 GB | 2,000 |
| F128 | 3 billion | 192 GB | 2,000 |
| F256+ | 3+ billion | 384+ GB | 2,000 |

## Auto-Sync Configuration

Auto-sync is enabled by default for Direct Lake semantic models. When a Delta table is updated, the model automatically detects changes and reframes.

```text
Recommended settings:
  - Auto-sync: ON (default)
  - Fallback behavior: ALLOW (default, can disable)
  - V-Order on source tables: REQUIRED for performance
  - Refresh schedule: Backup for auto-sync failures
```

## Performance Best Practices

| Practice | Impact | Notes |
|----------|--------|-------|
| Enable V-Order on all source tables | High | Columnar compression for fast reads |
| Minimize column count in model | High | Only include columns used in reports |
| Reduce cardinality of string columns | Medium | Group or hash high-cardinality columns |
| Use star schema in source | High | Fewer joins, faster frame loading |
| Monitor fallback events | Medium | Use DMV to detect DirectQuery fallbacks |

## Monitoring Fallback

```sql
-- Check if Direct Lake is falling back to DirectQuery
-- Run in SQL Server Management Studio via XMLA endpoint
SELECT
    [TableName],
    [DirectLakeStatus],
    [FallbackReason]
FROM $SYSTEM.TMSCHEMA_DELTA_TABLE_METADATA_STORAGES;
```

## Common Mistakes

### Wrong

```text
Using Import mode in Fabric "because it is faster"
--> Direct Lake matches Import performance without data duplication
```

### Correct

```text
Use Direct Lake (default in Fabric), ensure V-Order on source tables,
monitor for fallback events, and right-size capacity SKU
```

## Related

- [Warehouse Basics](warehouse-basics.md)
- [T-SQL Advanced](t-sql-advanced.md)
- [Star Schema](../patterns/star-schema.md)
- [Capacity Planning](../../03-architecture-patterns/concepts/capacity-planning.md)
