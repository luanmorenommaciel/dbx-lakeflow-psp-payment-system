> **MCP Validated:** 2026-02-17

# Delta Lake Optimization

> **Purpose**: OPTIMIZE, VACUUM, Z-ORDER, V-ORDER, file compaction, and table maintenance patterns for Fabric Lakehouses

## When to Use

- Read performance degrades due to many small Parquet files
- Delta table has accumulated stale files after updates/deletes
- Queries filter on specific columns and need predicate pushdown
- Running regular table maintenance as part of a data engineering workflow

## Critical Rule

**IMPORTANT:** In Fabric, use `PARTITIONED BY` for table partitioning. Do **NOT** use `CLUSTER BY` -- it is not supported in Fabric's Delta Lake implementation. Use `Z-ORDER BY` within `OPTIMIZE` for data skipping.

## Implementation

### Table Creation with Partitioning

```sql
-- Correct: Use PARTITIONED BY
CREATE TABLE gold_invoices (
    invoice_id STRING, vendor_name STRING, invoice_date DATE,
    amount DECIMAL(18,2), region STRING, processed_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (region)
```

```python
df.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("region") \
    .saveAsTable("gold_invoices")
```

### OPTIMIZE (File Compaction)

```sql
OPTIMIZE gold_invoices
ZORDER BY (invoice_date, vendor_name)

-- Optimize a specific partition only
OPTIMIZE gold_invoices WHERE region = 'south' ZORDER BY (invoice_date)
```

```python
from delta.tables import DeltaTable
delta_table = DeltaTable.forName(spark, "gold_invoices")
delta_table.optimize().executeZOrderBy(["invoice_date", "vendor_name"])
```

### V-ORDER (Fabric-Specific)

```sql
-- Reorganizes Parquet row groups for faster Power BI and SQL endpoint reads
OPTIMIZE gold_invoices VORDER
```

```python
# V-ORDER during write
df.write.format("delta") \
    .mode("overwrite") \
    .option("vorder", "true") \
    .saveAsTable("gold_invoices")
```

### VACUUM (Stale File Cleanup)

```sql
VACUUM gold_invoices RETAIN 168 HOURS   -- 7 days (minimum default)
VACUUM gold_invoices DRY RUN            -- preview files to delete
```

```python
delta_table = DeltaTable.forName(spark, "gold_invoices")
delta_table.vacuum(retentionHours=168)
```

### Z-ORDER vs V-ORDER

| Feature | Z-ORDER | V-ORDER |
|---------|---------|---------|
| Purpose | Data skipping on filter columns | Parquet read optimization |
| Applied via | `OPTIMIZE ... ZORDER BY` | `OPTIMIZE ... VORDER` or write option |
| Best for | Queries with WHERE clauses | Power BI DirectQuery, SQL endpoint |
| Combinable | Yes: `OPTIMIZE t ZORDER BY (col) VORDER` | Yes |

## Table Maintenance Schedule

```python
from datetime import datetime

tables = ["bronze_invoices", "silver_invoices", "gold_invoices"]
for table_name in tables:
    start = datetime.now()
    spark.sql(f"OPTIMIZE {table_name} ZORDER BY (invoice_date)")
    spark.sql(f"OPTIMIZE {table_name} VORDER")
    spark.sql(f"VACUUM {table_name} RETAIN 168 HOURS")
    spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS FOR ALL COLUMNS")
    elapsed = (datetime.now() - start).total_seconds()
    print(f"Maintenance for {table_name}: {elapsed:.1f}s")
```

## Partitioning Guidelines

| Scenario | Recommendation |
|----------|---------------|
| < 1 GB table | No partitioning needed |
| 1-100 GB table | Partition by low-cardinality column (region, date) |
| > 100 GB table | Partition by date + Z-ORDER by high-cardinality filter |
| Each partition | Should be at least 1 GB |
| Max partitions | Keep under 10,000 |

## File Size Targets

| Metric | Target | Why |
|--------|--------|-----|
| Parquet file size | 256 MB - 1 GB | Balance parallelism and overhead |
| Files per partition | 1-10 | Fewer files = faster listing |
| Small file threshold | < 32 MB | Trigger OPTIMIZE if many exist |

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `delta.optimize.maxFileSize` | 1 GB | Target file size |
| `delta.retentionDurationCheck.enabled` | true | Prevent VACUUM below 7 days |
| `delta.autoOptimize.optimizeWrite` | false | Auto-optimize on write |
| `delta.autoOptimize.autoCompact` | false | Auto-compact small files |

## Common Mistakes

### Wrong

```sql
-- CLUSTER BY is NOT supported in Fabric
CREATE TABLE invoices (id STRING, date DATE) USING DELTA CLUSTER BY (date)
```

### Correct

```sql
-- Use PARTITIONED BY + OPTIMIZE with ZORDER
CREATE TABLE invoices (id STRING, date DATE, region STRING)
USING DELTA PARTITIONED BY (region)
-- Then run: OPTIMIZE invoices ZORDER BY (date)
```

## Related

- [Lakehouse](../concepts/lakehouse.md)
- [Spark Notebooks](../concepts/spark-notebooks.md)
- [Incremental Load](incremental-load.md)
- [Copy Activity](copy-activity.md)
