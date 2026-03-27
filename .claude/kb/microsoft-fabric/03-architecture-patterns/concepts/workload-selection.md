> **MCP Validated:** 2026-02-17

# Workload Selection

> **Purpose**: Decision framework for choosing between Lakehouse, Warehouse, and Eventhouse
> **Confidence**: 0.95

## Overview

Microsoft Fabric offers three primary analytical storage workloads: Lakehouse, Warehouse, and Eventhouse. Each shares the OneLake foundation but targets different data patterns. Choosing the right workload is the most critical architectural decision in a Fabric implementation, as it determines your query language, write patterns, and optimization strategies.

## The Pattern

```text
DECISION TREE: Which Fabric Workload?

START
  |
  +-- Is data streaming / real-time?
  |     YES --> Eventhouse (KQL)
  |     NO  --> Continue
  |
  +-- Do you need full T-SQL DML (UPDATE, DELETE, MERGE)?
  |     YES --> Warehouse
  |     NO  --> Continue
  |
  +-- Do you need Spark / PySpark processing?
  |     YES --> Lakehouse
  |     NO  --> Continue
  |
  +-- Is your team SQL-first with stored procedures?
  |     YES --> Warehouse
  |     NO  --> Lakehouse (default recommendation)
```

## Quick Reference

| Criteria | Lakehouse | Warehouse | Eventhouse |
|----------|-----------|-----------|------------|
| Query Language | Spark SQL + T-SQL (read) | T-SQL (full DML) | KQL |
| Write Method | Spark, Dataflows | T-SQL INSERT/UPDATE/MERGE | Streaming ingestion |
| Best For | ELT, ML, unstructured | BI, complex joins | Real-time, time-series |
| Schema | Schema-on-read | Schema-on-write | Semi-structured |
| Cross-database | Yes (shortcuts) | Yes (cross-warehouse) | Yes (database shortcuts) |
| V-Order | Manual (OPTIMIZE) | Automatic | N/A |
| Security | Workspace roles | RLS + column-level + masking | Row-level |
| Latency | Seconds to minutes | Seconds | Sub-second |

## Common Mistakes

### Wrong

```text
Using Warehouse for petabyte-scale raw ingestion
--> Warehouse is optimized for structured analytics, not raw landing
```

### Correct

```text
Use Lakehouse for raw ingestion (bronze), then:
- Option A: Lakehouse for all layers (Spark-first teams)
- Option B: Warehouse for gold layer (SQL-first teams)
- Option C: Eventhouse for real-time analytics layer
```

## Hybrid Patterns

| Pattern | Bronze | Silver | Gold | Best For |
|---------|--------|--------|------|----------|
| All-Lakehouse | Lakehouse | Lakehouse | Lakehouse | Data engineering teams |
| Lake + Warehouse | Lakehouse | Lakehouse | Warehouse | BI-heavy organizations |
| Lake + Eventhouse | Lakehouse | Lakehouse | Eventhouse | IoT / streaming scenarios |
| Full hybrid | Lakehouse | Warehouse | Warehouse + Eventhouse | Enterprise mixed workloads |

## Related

- [Lakehouse](../../02-data-engineering/concepts/lakehouse.md)
- [Warehouse Basics](../../04-data-warehouse/concepts/warehouse-basics.md)
- [Medallion in Fabric](../patterns/medallion-fabric.md)
