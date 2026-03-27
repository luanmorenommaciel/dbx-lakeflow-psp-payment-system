# Delta Integration

> **Purpose**: Delta Lake 4.1 + UniForm — MERGE patterns, time travel, OPTIMIZE + ZORDER, vacuum, change data feed
> **MCP Validated**: 2026-03-26

## When to Use

- ACID transactions required on data lake storage
- Upsert (MERGE) patterns for slowly changing data
- Time travel for audit or debugging
- Need Iceberg/Hudi compatibility via UniForm

## Implementation

```python
from delta.tables import DeltaTable
from pyspark.sql import functions as F


# --- MERGE (Upsert) Pattern ---
target = DeltaTable.forPath(spark, "s3://lake/silver/customers")
source = spark.read.parquet("s3://lake/raw/customers_daily/")

(target.alias("t")
    .merge(source.alias("s"), "t.customer_id = s.customer_id")
    .whenMatchedUpdate(set={
        "name": "s.name",
        "email": "s.email",
        "updated_at": "s.updated_at",
    })
    .whenNotMatchedInsert(values={
        "customer_id": "s.customer_id",
        "name": "s.name",
        "email": "s.email",
        "created_at": "s.created_at",
        "updated_at": "s.updated_at",
    })
    .execute())


# --- SCD Type 2 MERGE ---
(target.alias("t")
    .merge(source.alias("s"), "t.customer_id = s.customer_id AND t.is_current = true")
    .whenMatchedUpdate(
        condition="t.name != s.name OR t.email != s.email",
        set={"is_current": "false", "end_date": "current_date()"}
    )
    .whenNotMatchedInsert(values={
        "customer_id": "s.customer_id",
        "name": "s.name",
        "email": "s.email",
        "is_current": "true",
        "start_date": "current_date()",
        "end_date": "null",
    })
    .execute())
```

```sql
-- OPTIMIZE + ZORDER for query performance
OPTIMIZE delta.`s3://lake/silver/orders`
ZORDER BY (customer_id, order_date);

-- Liquid clustering (Delta 4.1 — replaces ZORDER)
ALTER TABLE silver.orders
CLUSTER BY (customer_id, order_date);

-- Time travel queries
SELECT * FROM silver.orders VERSION AS OF 42;
SELECT * FROM silver.orders TIMESTAMP AS OF '2026-03-25';

-- VACUUM — remove old files (default 7-day retention)
VACUUM silver.orders RETAIN 168 HOURS;

-- Change Data Feed (CDC from Delta)
ALTER TABLE silver.orders SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

SELECT * FROM table_changes('silver.orders', 2, 5)
WHERE _change_type IN ('insert', 'update_postimage');

-- UniForm — write Delta with Iceberg compatibility
ALTER TABLE silver.orders
SET TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.enableIcebergCompatV2' = 'true'
);
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `delta.enableChangeDataFeed` | `false` | Enable CDC tracking |
| `delta.logRetentionDuration` | `30 days` | Transaction log history |
| `delta.deletedFileRetentionDuration` | `7 days` | VACUUM safety window |
| `delta.autoOptimize.optimizeWrite` | `false` | Auto-coalesce small files on write |
| `delta.autoOptimize.autoCompact` | `false` | Auto-compact after writes |
| `delta.universalFormat.enabledFormats` | none | `iceberg` for UniForm |

## Example Usage

```python
# Read change data feed for incremental downstream processing
changes = (spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 10)
    .load("s3://lake/silver/orders"))

new_and_updated = changes.filter(
    F.col("_change_type").isin("insert", "update_postimage"))
```

## See Also

- [read-write-patterns](../patterns/read-write-patterns.md)
- [delta-lake](../../lakehouse/concepts/delta-lake.md)
