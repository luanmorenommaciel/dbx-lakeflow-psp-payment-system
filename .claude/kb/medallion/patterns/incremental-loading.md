# Incremental Loading

> **Purpose**: MERGE INTO and incremental processing patterns to avoid full reloads across layers
> **MCP Validated**: 2026-03-26

## When to Use

- Processing only new or changed records instead of full table scans
- Implementing upsert (insert + update) logic in Silver and Gold layers
- Using watermarks or change data capture (CDC) for efficient data movement
- Reducing compute costs and processing time for large datasets

## Implementation

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, max as _max, row_number, lit
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()


def get_high_watermark(table_name: str, watermark_col: str) -> str:
    """Get the latest watermark value from a target table."""
    if not spark.catalog.tableExists(table_name):
        return "1900-01-01T00:00:00"
    result = spark.table(table_name).agg(_max(watermark_col)).collect()
    return str(result[0][0]) if result[0][0] else "1900-01-01T00:00:00"


def incremental_bronze_to_silver(
    bronze_table: str,
    silver_table: str,
    business_keys: list[str],
    watermark_col: str = "_ingested_at",
):
    """Incrementally process new Bronze records into Silver via MERGE."""
    # Get high watermark
    hwm = get_high_watermark(silver_table, "_updated_at")

    # Read only new records from Bronze
    new_records = (
        spark.table(bronze_table)
        .filter(col(watermark_col) > lit(hwm))
    )

    if new_records.head(1) == []:
        print(f"No new records since {hwm}")
        return 0

    # Deduplicate new batch
    w = Window.partitionBy(*business_keys).orderBy(col(watermark_col).desc())
    deduped = (
        new_records
        .withColumn("_rn", row_number().over(w))
        .filter(col("_rn") == 1)
        .drop("_rn")
        .withColumn("_updated_at", current_timestamp())
    )

    # MERGE into Silver
    if spark.catalog.tableExists(silver_table):
        silver_delta = DeltaTable.forName(spark, silver_table)
        merge_condition = " AND ".join([f"t.{k} = s.{k}" for k in business_keys])

        (
            silver_delta.alias("t")
            .merge(deduped.alias("s"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        deduped.write.format("delta").saveAsTable(silver_table)

    return deduped.count()
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `watermark_column` | `_ingested_at` | Column to track incremental progress |
| `business_keys` | Required | Keys for MERGE deduplication |
| `batch_size` | No limit | Optional limit on records per batch |
| `merge_condition` | Auto-generated | Custom ON clause for MERGE |
| `enable_cdc` | `false` | Use Change Data Feed for incremental reads |

## SQL MERGE Pattern

```sql
-- Incremental Silver MERGE with high watermark
MERGE INTO silver_sales.cleansed_orders AS target
USING (
    WITH new_data AS (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY order_id ORDER BY _ingested_at DESC
            ) AS _rn
        FROM bronze_sales.raw_orders
        WHERE _ingested_at > (
            SELECT COALESCE(MAX(_updated_at), '1900-01-01')
            FROM silver_sales.cleansed_orders
        )
    )
    SELECT * FROM new_data WHERE _rn = 1
) AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN UPDATE SET
    target.customer_id = source.customer_id,
    target.amount = CAST(source.amount AS DECIMAL(10,2)),
    target.order_date = source.order_date,
    target._updated_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT *;
```

## Change Data Feed (CDF) Pattern

```python
def read_silver_changes(silver_table: str, start_version: int):
    """Read changes from Silver using Delta Change Data Feed."""
    changes = (
        spark.read
        .format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", start_version)
        .table(silver_table)
    )
    # _change_type: insert, update_preimage, update_postimage, delete
    return changes.filter(col("_change_type").isin("insert", "update_postimage"))


def incremental_silver_to_gold(silver_table: str, gold_table: str):
    """Use CDF to incrementally update Gold from Silver changes."""
    # Enable CDF on Silver table (one-time)
    spark.sql(f"""
        ALTER TABLE {silver_table}
        SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    """)

    # Get latest processed version
    latest_version = get_latest_processed_version(gold_table)

    # Read only changes
    changes = read_silver_changes(silver_table, latest_version + 1)

    # Merge changes into Gold
    if spark.catalog.tableExists(gold_table):
        gold_delta = DeltaTable.forName(spark, gold_table)
        gold_delta.alias("t").merge(
            changes.alias("s"), "t.customer_id = s.customer_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        changes.drop("_change_type", "_commit_version", "_commit_timestamp") \
               .write.format("delta").saveAsTable(gold_table)
```

## Auto Loader (Streaming Incremental)

```python
def auto_loader_to_bronze(landing_path: str, bronze_table: str, checkpoint: str):
    """Use Auto Loader for efficient file-based incremental ingestion."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{checkpoint}/schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(landing_path)
        .withColumn("_ingested_at", current_timestamp())
        .writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(bronze_table)
    )
```

## Example Usage

```python
# Incremental pipeline run
rows = incremental_bronze_to_silver(
    bronze_table="bronze_sales.raw_orders",
    silver_table="silver_sales.cleansed_orders",
    business_keys=["order_id"],
)
print(f"Processed {rows} new/updated records")
```

## See Also

- [Layer Transitions](../patterns/layer-transitions.md)
- [Bronze Layer](../concepts/bronze-layer.md)
- [Silver Layer](../concepts/silver-layer.md)
- [Schema Evolution](../patterns/schema-evolution.md)
