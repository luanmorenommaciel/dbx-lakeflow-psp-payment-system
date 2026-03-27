> **MCP Validated:** 2026-02-17

# Incremental Load

> **Purpose**: Incremental data loading patterns using watermark columns, change data capture, and Delta merge operations in Fabric

## When to Use

- Loading only new or changed records from source systems
- Implementing slowly changing dimensions (SCD Type 1 and Type 2)
- Syncing transactional data from OLTP systems to Lakehouse on a schedule
- Reducing pipeline runtime and compute costs for large tables

## Pattern Overview

```text
Source ──(modified_date > watermark)──▶ Staging ──MERGE──▶ Target (Delta)
                                                              │
                                                     Update watermark table
```

## Implementation

### Pattern 1: Watermark-Based Incremental Load

```python
from pyspark.sql.functions import col, max as spark_max, current_timestamp, lit
from delta.tables import DeltaTable

target_table = "silver_invoices"
watermark_column = "modified_date"

# Step 1: Read last watermark
try:
    wm = spark.sql(f"""
        SELECT max_watermark FROM pipeline_watermarks
        WHERE table_name = '{target_table}'
    """).collect()[0]["max_watermark"]
except Exception:
    wm = "1900-01-01T00:00:00"

# Step 2: Read new/changed records
incremental_df = (
    spark.read.format("delta").load("Tables/bronze_invoices")
    .filter(col(watermark_column) > wm)
)
if incremental_df.count() == 0:
    mssparkutils.notebook.exit("SKIP: No new records")

# Step 3: Transform
transformed_df = incremental_df.withColumn("loaded_at", current_timestamp())

# Step 4: Capture new watermark
new_wm = incremental_df.select(spark_max(col(watermark_column))).collect()[0][0]

# Step 5: Merge into target
DeltaTable.forName(spark, target_table).alias("t").merge(
    transformed_df.alias("s"), "t.invoice_id = s.invoice_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# Step 6: Update watermark
spark.sql(f"""
    MERGE INTO pipeline_watermarks AS w
    USING (SELECT '{target_table}' AS table_name, '{new_wm}' AS max_watermark) AS s
    ON w.table_name = s.table_name
    WHEN MATCHED THEN UPDATE SET w.max_watermark = s.max_watermark
    WHEN NOT MATCHED THEN INSERT *
""")
```

### Pattern 2: SCD Type 1 (Overwrite Changed Attributes)

```python
from delta.tables import DeltaTable

source_df = spark.read.format("delta").load("Tables/staging_vendors")
target = DeltaTable.forName(spark, "dim_vendors")

target.alias("t").merge(
    source_df.alias("s"), "t.vendor_id = s.vendor_id"
).whenMatchedUpdate(
    condition="""
        t.vendor_name != s.vendor_name OR
        t.vendor_address != s.vendor_address
    """,
    set={
        "vendor_name": "s.vendor_name",
        "vendor_address": "s.vendor_address",
        "updated_at": "current_timestamp()",
    }
).whenNotMatchedInsert(values={
    "vendor_id": "s.vendor_id",
    "vendor_name": "s.vendor_name",
    "vendor_address": "s.vendor_address",
    "created_at": "current_timestamp()",
    "updated_at": "current_timestamp()",
}).execute()
```

### Pattern 3: CDC with Delete Detection

```python
from delta.tables import DeltaTable

cdc_df = (
    spark.read.format("delta").load("Tables/cdc_invoices")
    .filter(col("_cdc_timestamp") > last_watermark)
)
target = DeltaTable.forName(spark, "silver_invoices")

target.alias("t").merge(
    cdc_df.alias("s"), "t.invoice_id = s.invoice_id"
).whenMatchedDelete(
    condition="s._cdc_operation = 'DELETE'"
).whenMatchedUpdate(
    condition="s._cdc_operation = 'UPDATE'",
    set={"vendor_name": "s.vendor_name", "amount": "s.amount",
         "updated_at": "current_timestamp()"}
).whenNotMatchedInsert(
    condition="s._cdc_operation = 'INSERT'",
    values={"invoice_id": "s.invoice_id", "vendor_name": "s.vendor_name",
            "amount": "s.amount", "created_at": "current_timestamp()",
            "updated_at": "current_timestamp()"}
).execute()
```

### Watermark Table Setup

```sql
CREATE TABLE IF NOT EXISTS pipeline_watermarks (
    table_name STRING, max_watermark STRING, updated_at TIMESTAMP
) USING DELTA
```

## Pattern Comparison

| Pattern | Best For | Complexity |
|---------|----------|-----------|
| Watermark + Append | Append-only sources | Low |
| Watermark + MERGE | Sources with updates | Medium |
| CDC + MERGE | Sources with updates and deletes | High |
| Full Overwrite | Small reference tables | Lowest |

## Configuration

| Setting | Recommended | Description |
|---------|-------------|-------------|
| Watermark column | `modified_date` | Must be indexed at source |
| Merge key | Primary/business key | Must be unique in target |
| Post-merge | OPTIMIZE + VACUUM | Maintain target table health |

## Common Mistakes

### Wrong

```python
# Appending without dedup causes duplicate records
incremental_df.write.format("delta").mode("append").saveAsTable("target")
```

### Correct

```python
# Use MERGE for upsert
DeltaTable.forName(spark, "target").alias("t").merge(
    incremental_df.alias("s"), "t.id = s.id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

## Related

- [Lakehouse](../concepts/lakehouse.md)
- [Spark Notebooks](../concepts/spark-notebooks.md)
- [Delta Lake Optimization](delta-lake-optimization.md)
- [Copy Activity](copy-activity.md)
