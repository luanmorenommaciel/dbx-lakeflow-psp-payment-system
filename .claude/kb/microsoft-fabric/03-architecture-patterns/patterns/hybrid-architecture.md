> **MCP Validated:** 2026-02-17

# Hybrid Architecture Pattern

> **Purpose**: Combining Lakehouse, Warehouse, and Eventhouse in a single domain solution

## When to Use

- Enterprise scenarios requiring batch analytics AND real-time monitoring
- Domains with mixed data types: structured, semi-structured, and streaming
- Organizations with both data engineering (Spark) and BI (SQL) teams
- Use cases needing row-level security on curated data but flexible raw ingestion

## Overview

The hybrid architecture pattern leverages all three Fabric analytical workloads within a single business domain. The Lakehouse handles raw ingestion and heavy transformations, the Warehouse serves curated business data with full T-SQL and security, and the Eventhouse provides sub-second real-time analytics. OneLake shortcuts connect the workloads without data duplication.

## Implementation

### Architecture Diagram

```text
                        HYBRID ARCHITECTURE
================================================================

  SOURCES                 FABRIC DOMAIN                  CONSUMERS
  -------                 -------------                  ---------

  +---------+
  | Files   |--+
  | (S3/ADLS)|  |     +---------------------------+
  +---------+  +---->|     LAKEHOUSE (Bronze)      |
  +---------+  |     |  - Raw CSV, JSON, Parquet   |
  | APIs    |--+     |  - Files/ folder            |
  +---------+  |     |  - Spark notebooks          |
  +---------+  |     +-------------+---------------+
  | DB CDC  |--+                   |
  +---------+              Spark transforms
                                   |
                    +--------------+--------------+
                    |                             |
          +---------+----------+    +------------+-----------+
          |  LAKEHOUSE (Silver) |    |    EVENTHOUSE           |
          |  - Cleansed Delta   |    |  - KQL database         |
          |  - Conformed schema |    |  - Real-time streams    |
          |  - Spark SQL        |    |  - Sub-second queries   |
          +---------+-----------+    +------------+-----------+
                    |                             |
              Shortcut / View               KQL Queryset
                    |                             |
          +---------+-----------+                 |
          |   WAREHOUSE (Gold)  |                 |
          |  - Star schema      |       +---------+---------+
          |  - RLS + masking    |       | Real-Time         |
          |  - Stored procs     |       | Dashboard         |
          |  - T-SQL full DML   |       | (Power BI / KQL)  |
          +---------+-----------+       +-------------------+
                    |
          +---------+-----------+
          |    SEMANTIC MODEL   |
          |  - Direct Lake mode |
          |  - Measures / KPIs  |
          +---------+-----------+
                    |
          +---------+-----------+
          |     POWER BI        |
          |  - Reports          |
          |  - Dashboards       |
          +---------------------+
```

### Component Responsibilities

| Component | Workload | Role | Key Feature |
|-----------|----------|------|-------------|
| Raw landing | Lakehouse | Ingest all source data | Files/ folder, schema-on-read |
| Cleansed layer | Lakehouse | Transform and conform | Spark, Delta, OPTIMIZE VORDER |
| Business layer | Warehouse | Serve curated analytics | RLS, masking, stored procs |
| Real-time layer | Eventhouse | Stream analytics | KQL, sub-second latency |
| Reporting | Semantic Model + PBI | Business consumption | Direct Lake, DAX measures |

### Connecting Workloads with Shortcuts

```sql
-- In Warehouse: Create shortcut to Lakehouse silver tables
-- (Done via Fabric UI: Warehouse > Get Data > OneLake shortcut)
-- Result: silver tables appear as external tables in Warehouse

-- Query across workloads using three-part naming
SELECT
    w.customer_name,
    w.region,
    s.order_count,
    s.total_revenue
FROM gold_warehouse.dbo.dim_customer w
JOIN silver_lakehouse.dbo.silver_order_agg s
    ON w.customer_id = s.customer_id;
```

### Pipeline Orchestration

```text
DAILY BATCH PIPELINE (Data Factory)
  |
  +-- 06:00 UTC: Copy raw files --> Lakehouse Bronze (Files/)
  +-- 06:30 UTC: Notebook: bronze_to_silver (Spark)
  +-- 07:00 UTC: Notebook: OPTIMIZE VORDER on silver tables
  +-- 07:30 UTC: Stored Proc: Warehouse gold refresh
  +-- 08:00 UTC: Semantic Model: Scheduled refresh
  |
STREAMING PIPELINE (Eventstream)
  |
  +-- Continuous: Event Hub --> Eventhouse (KQL DB)
  +-- Real-time dashboard auto-refreshes
```

## Configuration

| Setting | Value | Notes |
|---------|-------|-------|
| Bronze workspace | `ws-{domain}-bronze` | Lakehouse only |
| Silver workspace | `ws-{domain}-silver` | Lakehouse only |
| Gold workspace | `ws-{domain}-gold` | Warehouse + Semantic Model |
| RT workspace | `ws-{domain}-realtime` | Eventhouse + RT dashboard |
| Capacity | F64+ recommended | Multiple workloads need headroom |
| Shortcuts | Silver --> Gold | Avoid data duplication |

## When NOT to Use

- Single-team projects with only batch workloads (use all-Lakehouse instead)
- Pure streaming scenarios with no batch analytics (use Eventhouse only)
- Small datasets under 1 GB (over-engineered; single Lakehouse is sufficient)
- Teams with no KQL experience and no real-time requirement

## Example Usage

```python
# Silver notebook: Write optimized Delta for Warehouse consumption
df_silver = spark.sql("""
    SELECT
        order_id, customer_id, product_id,
        order_date, quantity, unit_price,
        quantity * unit_price AS amount,
        current_timestamp() AS _processed_at
    FROM bronze_lakehouse.raw_orders
    WHERE order_date >= '2026-01-01'
""")

df_silver.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_orders")

spark.sql("OPTIMIZE silver_orders VORDER")
```

```sql
-- Gold warehouse: Refresh star schema from silver shortcut
EXEC dbo.usp_refresh_fact_orders;
EXEC dbo.usp_refresh_dim_customers;
```

## See Also

- [Lakehouse vs Warehouse](lakehouse-vs-warehouse.md)
- [Workload Selection](../concepts/workload-selection.md)
- [Medallion in Fabric](medallion-fabric.md)
- [Warehouse Basics](../../04-data-warehouse/concepts/warehouse-basics.md)
