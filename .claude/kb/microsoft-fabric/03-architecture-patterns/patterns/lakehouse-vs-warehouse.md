> **MCP Validated:** 2026-02-17

# Lakehouse vs Warehouse Decision Framework

> **Purpose**: Structured decision framework for choosing between Lakehouse and Warehouse in Fabric

## When to Use

- Starting a new Fabric project and choosing the primary analytical store
- Evaluating whether to migrate an existing workload to Lakehouse or Warehouse
- Deciding the storage target for each medallion layer (bronze, silver, gold)
- Reviewing architecture for a team with mixed Spark and SQL skill sets

## Overview

Both Lakehouse and Warehouse store data in Delta Parquet on OneLake, but they expose fundamentally different engines, write paths, and optimization strategies. This guide provides a structured decision matrix to choose the right workload for each use case.

## Decision Matrix

| Criteria | Lakehouse | Warehouse | Winner |
|----------|-----------|-----------|--------|
| **Query language** | Spark SQL + T-SQL (read-only endpoint) | Full T-SQL (read + write) | Depends on team |
| **Write operations** | Spark DataFrames, Dataflow Gen2 | T-SQL INSERT/UPDATE/DELETE/MERGE | Warehouse for DML |
| **Storage format** | Delta Parquet (managed + unmanaged) | Delta Parquet (managed only) | Lakehouse (flexibility) |
| **Unstructured data** | Files/ folder for images, JSON, CSV | Not supported | Lakehouse |
| **Schema approach** | Schema-on-read | Schema-on-write | Lakehouse (flexibility) |
| **V-Order optimization** | Manual (OPTIMIZE VORDER) | Automatic on write | Warehouse |
| **Row-level security** | Workspace roles only | RLS + column-level + masking | Warehouse |
| **Cross-database queries** | Shortcuts | Three-part naming | Both (different mechanism) |
| **Streaming ingestion** | Supported via Spark Structured Streaming | Not supported natively | Lakehouse |
| **Stored procedures** | Not supported | Full T-SQL procedures | Warehouse |
| **ML / Data Science** | Native Spark ML, MLflow | Not supported | Lakehouse |
| **Direct Lake for Power BI** | Yes (default mode) | Yes (via SQL endpoint) | Both |
| **Cost profile** | CU for Spark cluster time | CU for query execution | Varies |

## Implementation

### Decision Flow

```text
START: What does your team need?
  |
  +-- Raw data landing (CSV, JSON, images)?
  |     YES --> Lakehouse (Files/ folder)
  |     NO  --> Continue
  |
  +-- UPDATE/DELETE/MERGE on tables?
  |     YES --> Warehouse
  |     NO  --> Continue
  |
  +-- PySpark / Spark ML processing?
  |     YES --> Lakehouse
  |     NO  --> Continue
  |
  +-- Row-level security + data masking?
  |     YES --> Warehouse (superior security model)
  |     NO  --> Continue
  |
  +-- Stored procedures + complex T-SQL?
  |     YES --> Warehouse
  |     NO  --> Continue
  |
  +-- Streaming ingestion?
  |     YES --> Lakehouse (or Eventhouse for real-time)
  |     NO  --> Lakehouse (default recommendation)
```

### Layer-by-Layer Recommendation

```text
BRONZE (Raw Ingestion)
  --> Almost always Lakehouse
  --> Reason: Schema-on-read, Files/ folder for raw formats
  --> Exception: None (Warehouse is not designed for raw landing)

SILVER (Cleansed / Conformed)
  --> Lakehouse for Spark-first teams
  --> Warehouse if team is purely SQL and needs DML for cleansing
  --> Recommendation: Lakehouse (more flexible transformations)

GOLD (Business-Ready)
  --> Warehouse for BI-heavy orgs needing RLS, masking, stored procs
  --> Lakehouse if team uses Spark SQL and Direct Lake is sufficient
  --> Recommendation: Match to downstream consumer needs
```

## Configuration

| Scenario | Recommended | Rationale |
|----------|-------------|-----------|
| Data engineering team (Python/Spark) | Lakehouse all layers | Native tooling, notebooks |
| BI/analytics team (SQL-first) | Lakehouse bronze + Warehouse gold | Best of both worlds |
| Regulated industry (finance, health) | Warehouse for gold | RLS + column security + masking |
| IoT / streaming + analytics | Lakehouse + Eventhouse | Real-time + batch |
| Small team, simple pipeline | Lakehouse all layers | Less complexity |
| Enterprise with stored proc legacy | Warehouse | Familiar T-SQL patterns |

## Performance Comparison

| Operation | Lakehouse | Warehouse |
|-----------|-----------|-----------|
| Bulk write (millions of rows) | Fast (Spark parallelism) | Moderate (T-SQL batch) |
| Point updates (single row) | Slow (Delta merge) | Fast (T-SQL UPDATE) |
| Complex aggregation | Fast (Spark distributed) | Fast (MPP engine) |
| Concurrent BI queries | Good (Direct Lake) | Good (Direct Lake) |
| File-based ingestion | Native (Files/ folder) | Requires COPY INTO |
| Schema evolution | Flexible (mergeSchema) | Requires ALTER TABLE |

## Example Usage

```sql
-- Lakehouse: Query via SQL endpoint (read-only)
SELECT category, SUM(amount) AS total
FROM lakehouse_silver.dbo.silver_sales
GROUP BY category;

-- Warehouse: Full DML support
MERGE INTO wh_gold.dbo.dim_customer AS t
USING wh_gold.dbo.stg_customer AS s
ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET t.name = s.name
WHEN NOT MATCHED THEN INSERT (customer_id, name) VALUES (s.customer_id, s.name);
```

## See Also

- [Workload Selection](../concepts/workload-selection.md)
- [Hybrid Architecture](hybrid-architecture.md)
- [Warehouse Basics](../../04-data-warehouse/concepts/warehouse-basics.md)
- [Medallion in Fabric](medallion-fabric.md)
