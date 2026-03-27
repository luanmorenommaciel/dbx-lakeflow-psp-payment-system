# Silver Layer

> **Purpose**: Cleansed, conformed, deduplicated data with schema enforcement and business key alignment
> **Confidence**: 0.95
> **MCP Validated**: 2026-03-26

## Overview

The Silver layer transforms raw Bronze data into a cleansed, validated, and
deduplicated dataset. It enforces schemas, applies data type casting, resolves
duplicates using business keys, and conforms naming conventions. Silver tables
serve as the enterprise-wide "single source of truth" for downstream analytics,
ML, and Gold layer aggregations.

## The Pattern

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, trim, upper, to_timestamp
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

def bronze_to_silver(bronze_table: str, silver_table: str, business_keys: list):
    """Cleanse, deduplicate, and merge Bronze data into Silver."""
    bronze_df = spark.table(bronze_table)

    # Step 1: Cleanse and cast types
    cleansed_df = (
        bronze_df
        .withColumn("order_id", trim(col("order_id")))
        .withColumn("customer_id", upper(trim(col("customer_id"))))
        .withColumn("order_date", to_timestamp(col("order_date"), "yyyy-MM-dd"))
        .withColumn("amount", col("amount").cast("decimal(10,2)"))
        .filter(col("order_id").isNotNull())
    )

    # Step 2: Deduplicate using business key + latest ingestion
    dedup_window = Window.partitionBy(*business_keys).orderBy(
        col("_ingested_at").desc()
    )
    deduped_df = (
        cleansed_df
        .withColumn("_row_num", row_number().over(dedup_window))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )

    return deduped_df
```

## Quick Reference

| Property | Value | Notes |
|----------|-------|-------|
| Write mode | `merge` (UPSERT) | Deduplicate on business keys |
| Schema enforcement | Schema-on-write | Strict types, NOT NULL constraints |
| Deduplication | `row_number()` window | Partition by business key, order by `_ingested_at` |
| Naming convention | `cleansed_{entity}` | Standardized, snake_case |
| SCD Type | Type 1 (default) or Type 2 | Based on business requirements |
| Partitioning | By business date | Event date, not ingestion date |

## MERGE INTO Pattern (SQL)

```sql
MERGE INTO silver.cleansed_orders AS target
USING (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY order_id ORDER BY _ingested_at DESC
    ) AS _rn
    FROM bronze.raw_orders
    WHERE _ingested_at > '${last_processed_timestamp}'
) AS source
ON target.order_id = source.order_id AND source._rn = 1
WHEN MATCHED AND source._ingested_at > target._updated_at THEN UPDATE SET
    target.customer_id = source.customer_id,
    target.amount = CAST(source.amount AS DECIMAL(10,2)),
    target.order_date = CAST(source.order_date AS TIMESTAMP),
    target._updated_at = current_timestamp()
WHEN NOT MATCHED AND source._rn = 1 THEN INSERT (
    order_id, customer_id, amount, order_date, _created_at, _updated_at
) VALUES (
    source.order_id, source.customer_id,
    CAST(source.amount AS DECIMAL(10,2)),
    CAST(source.order_date AS TIMESTAMP),
    current_timestamp(), current_timestamp()
);
```

## SCD Type 2 Pattern

```python
from delta.tables import DeltaTable

def apply_scd2(spark, silver_table: str, updates_df, business_key: str):
    """Apply Slowly Changing Dimension Type 2 to Silver table."""
    silver_delta = DeltaTable.forName(spark, silver_table)

    # Close existing active records that have changes
    silver_delta.alias("target").merge(
        updates_df.alias("source"),
        f"target.{business_key} = source.{business_key} AND target.is_current = true"
    ).whenMatchedUpdate(
        condition="target.hash_value != source.hash_value",
        set={
            "is_current": "false",
            "valid_to": "current_timestamp()",
            "_updated_at": "current_timestamp()"
        }
    ).whenNotMatchedInsert(
        values={
            business_key: f"source.{business_key}",
            "is_current": "true",
            "valid_from": "current_timestamp()",
            "valid_to": "lit('9999-12-31')",
            "_created_at": "current_timestamp()",
            "_updated_at": "current_timestamp()"
        }
    ).execute()
```

## Common Mistakes

### Wrong -- No Deduplication

```python
# Writing Bronze directly to Silver without dedup
bronze_df.write.format("delta").mode("append").saveAsTable("silver.orders")
```

### Correct -- Deduplicate Then Merge

```python
deduped = deduplicate(bronze_df, keys=["order_id"])
merge_into_silver(deduped, target="silver.cleansed_orders", keys=["order_id"])
```

## Silver Anti-Patterns (Field Lessons)

| Anti-Pattern | Problem | Fix |
|-------------|---------|-----|
| Silver = Bronze + renamed columns | No real cleansing, duplicates persist | Enforce dedup, type casting, and validation |
| No data contracts on Silver | Downstream consumers break on schema changes | Define contracts with schema, SLAs, ownership |
| Mixing domain data in one Silver DB | Ownership confusion, coupling | Organize by domain: `silver_sales`, `silver_inventory` |
| No incremental processing | Full reloads waste compute | Use watermarks or CDF for incremental MERGE |
| SCD Type 2 everywhere | Excessive storage, complex queries | Use Type 2 only where business requires history |

## Related

- [Bronze Layer](../concepts/bronze-layer.md)
- [Gold Layer](../concepts/gold-layer.md)
- [Data Quality Gates](../patterns/data-quality-gates.md)
- [Layer Transitions](../patterns/layer-transitions.md)
