# Bronze Layer

> **Purpose**: Raw ingestion layer -- append-only, schema-on-read, preserving full source fidelity
> **Confidence**: 0.95
> **MCP Validated**: 2026-03-26

## Overview

The Bronze layer is the landing zone of the Medallion Architecture. It captures raw data
exactly as received from source systems -- APIs, files, streams, databases -- with no
transformations applied. Bronze tables are append-only, preserving every version of every
record for auditability and reprocessing. Metadata columns track ingestion lineage.

## The Pattern

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit

spark = SparkSession.builder.getOrCreate()

def ingest_to_bronze(source_path: str, bronze_table: str, source_system: str):
    """Ingest raw data into Bronze layer with metadata columns."""
    raw_df = (
        spark.read
        .option("mergeSchema", "true")
        .format("json")  # or csv, parquet, avro
        .load(source_path)
    )

    bronze_df = (
        raw_df
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", input_file_name())
        .withColumn("_source_system", lit(source_system))
    )

    (
        bronze_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(bronze_table)
    )
    return bronze_df
```

## Quick Reference

| Property | Value | Notes |
|----------|-------|-------|
| Write mode | `append` | Never overwrite or merge in Bronze |
| Schema enforcement | Schema-on-read | Accept all source schemas |
| `mergeSchema` | `true` | Allow new columns from source |
| Metadata columns | `_ingested_at`, `_source_file`, `_source_system` | Required for lineage |
| Partitioning | By ingestion date | `_ingested_date` or `year/month/day` |
| Retention | Years | Keep raw data for reprocessing |

## Common Mistakes

### Wrong -- Transforming Data in Bronze

```python
# Never filter, cast, or transform in Bronze
bronze_df = raw_df.filter(col("status") == "active")
bronze_df = raw_df.withColumn("amount", col("amount").cast("decimal(10,2)"))
```

### Correct -- Raw Data with Metadata Only

```python
# Only add ingestion metadata, preserve everything else
bronze_df = (
    raw_df
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", input_file_name())
)
```

## Streaming Ingestion to Bronze

```python
def stream_to_bronze(kafka_topic: str, bronze_table: str, checkpoint: str):
    """Stream from Kafka to Bronze using Auto Loader pattern."""
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{checkpoint}/schema")
        .load(f"/mnt/landing/{kafka_topic}")
        .withColumn("_ingested_at", current_timestamp())
        .writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .outputMode("append")
        .toTable(bronze_table)
    )
```

## SQL Alternative (Delta Lake)

```sql
CREATE TABLE IF NOT EXISTS bronze.raw_orders (
    order_id STRING,
    customer_id STRING,
    order_data STRING,          -- raw JSON blob
    _ingested_at TIMESTAMP DEFAULT current_timestamp(),
    _source_system STRING,
    _source_file STRING
)
USING DELTA
PARTITIONED BY (_ingested_date)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
```

## SQL Alternative (Iceberg)

```sql
CREATE TABLE IF NOT EXISTS bronze.raw_orders (
    order_id STRING,
    customer_id STRING,
    order_data STRING,
    _ingested_at TIMESTAMP DEFAULT current_timestamp(),
    _source_system STRING,
    _source_file STRING
)
USING ICEBERG
PARTITIONED BY (days(_ingested_at))
TBLPROPERTIES (
    'format-version' = '3',
    'write.parquet.compression-codec' = 'zstd'
);
```

## Shift-Left Quality at Bronze (2025+ Best Practice)

While Bronze should remain raw, modern practice validates basic structural integrity at ingestion to catch issues early:

```python
def validate_bronze_structure(df, required_columns: list[str]) -> tuple:
    """Validate basic structure before writing to Bronze. Not transformation -- just sanity."""
    # Check required columns exist
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in source: {missing}")

    # Check not completely empty
    if df.head(1) == []:
        raise ValueError("Source data is empty -- skipping Bronze write")

    # Optionally check file is parseable (no corrupt JSON/CSV)
    return df
```

## Related

- [Silver Layer](../concepts/silver-layer.md)
- [Layer Transitions](../patterns/layer-transitions.md)
- [Incremental Loading](../patterns/incremental-loading.md)
