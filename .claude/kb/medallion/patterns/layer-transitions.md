# Layer Transitions

> **Purpose**: End-to-end Bronze to Silver to Gold transformation flow with orchestration
> **MCP Validated**: 2026-03-26

## When to Use

- Building a complete medallion pipeline from raw ingestion to business aggregation
- Orchestrating multi-layer ETL with dependency tracking
- Implementing idempotent, rerunnable transformations between layers

## Implementation

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, input_file_name, lit,
    row_number, trim, upper, to_timestamp, sum as _sum, count, avg
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()


# ── BRONZE: Raw Ingestion ─────────────────────────────────────
def ingest_bronze(source_path: str, bronze_table: str, source: str):
    """Append raw data to Bronze with ingestion metadata."""
    raw_df = (
        spark.read
        .option("mergeSchema", "true")
        .json(source_path)
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", input_file_name())
        .withColumn("_source_system", lit(source))
    )
    raw_df.write.format("delta").mode("append").saveAsTable(bronze_table)
    return raw_df.count()


# ── SILVER: Cleanse + Deduplicate + Merge ─────────────────────
def process_silver(bronze_table: str, silver_table: str, keys: list):
    """Cleanse Bronze data and MERGE into Silver (SCD Type 1)."""
    bronze_df = spark.table(bronze_table)

    # Cleanse
    cleansed = (
        bronze_df
        .withColumn("order_id", trim(col("order_id")))
        .withColumn("customer_id", upper(trim(col("customer_id"))))
        .withColumn("amount", col("amount").cast("decimal(10,2)"))
        .withColumn("order_date", to_timestamp(col("order_date")))
        .filter(col("order_id").isNotNull())
    )

    # Deduplicate
    w = Window.partitionBy(*keys).orderBy(col("_ingested_at").desc())
    deduped = (
        cleansed
        .withColumn("_rn", row_number().over(w))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    # Merge into Silver
    if spark.catalog.tableExists(silver_table):
        silver_delta = DeltaTable.forName(spark, silver_table)
        merge_condition = " AND ".join([f"t.{k} = s.{k}" for k in keys])
        silver_delta.alias("t").merge(
            deduped.alias("s"), merge_condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        deduped.write.format("delta").saveAsTable(silver_table)

    return deduped.count()


# ── GOLD: Aggregate ───────────────────────────────────────────
def build_gold(silver_table: str, gold_table: str):
    """Build business aggregation from Silver into Gold."""
    silver_df = spark.table(silver_table)

    gold_df = (
        silver_df
        .groupBy("customer_id", "region")
        .agg(
            _sum("amount").alias("total_revenue"),
            count("order_id").alias("total_orders"),
            avg("amount").alias("avg_order_value"),
        )
        .withColumn("_computed_at", current_timestamp())
    )

    gold_df.write.format("delta").mode("overwrite").saveAsTable(gold_table)
    return gold_df.count()
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `bronze.write_mode` | `append` | Always append in Bronze |
| `silver.merge_keys` | `[]` | Business keys for deduplication |
| `silver.scd_type` | `1` | SCD Type 1 (overwrite) or 2 (history) |
| `gold.write_mode` | `overwrite` | Full refresh or incremental merge |
| `gold.partition_by` | `[]` | Partition columns for Gold tables |
| `checkpoint_path` | `/checkpoints/{table}` | Streaming checkpoint location |

## Orchestration Pattern

```python
def run_medallion_pipeline(config: dict):
    """Orchestrate full Bronze -> Silver -> Gold pipeline."""
    results = {}

    # Layer 1: Bronze
    results["bronze"] = ingest_bronze(
        source_path=config["source_path"],
        bronze_table=config["bronze_table"],
        source=config["source_system"],
    )

    # Layer 2: Silver
    results["silver"] = process_silver(
        bronze_table=config["bronze_table"],
        silver_table=config["silver_table"],
        keys=config["business_keys"],
    )

    # Layer 3: Gold
    results["gold"] = build_gold(
        silver_table=config["silver_table"],
        gold_table=config["gold_table"],
    )

    # Post-processing
    # Delta 4.x: liquid clustering (no ZORDER needed if CLUSTER BY is set)
    spark.sql(f"OPTIMIZE {config['silver_table']}")
    spark.sql(f"OPTIMIZE {config['gold_table']}")

    return results


# Usage
pipeline_config = {
    "source_path": "/mnt/landing/orders/2026-02-17/",
    "source_system": "ecommerce_api",
    "bronze_table": "bronze_sales.raw_orders",
    "silver_table": "silver_sales.cleansed_orders",
    "gold_table": "gold_sales.agg_customer_revenue",
    "business_keys": ["order_id"],
}

results = run_medallion_pipeline(pipeline_config)
```

## Example Usage

```python
# Individual layer execution (for testing or partial runs)
ingest_bronze("/mnt/landing/orders/", "bronze_sales.raw_orders", "api")
process_silver("bronze_sales.raw_orders", "silver_sales.cleansed_orders", ["order_id"])
build_gold("silver_sales.cleansed_orders", "gold_sales.agg_revenue")
```

## See Also

- [Bronze Layer](../concepts/bronze-layer.md)
- [Silver Layer](../concepts/silver-layer.md)
- [Gold Layer](../concepts/gold-layer.md)
- [Incremental Loading](../patterns/incremental-loading.md)
