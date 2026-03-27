# Read/Write Patterns

> **Purpose**: Production Parquet/Delta/Iceberg I/O with schema evolution and partition discovery
> **MCP Validated**: 2026-03-26

## When to Use

- Reading from or writing to cloud storage (S3, GCS, ADLS)
- Schema evolution is expected (new columns added over time)
- Data is partitioned by date or category for query performance
- Multiple table formats coexist in the lakehouse

## Implementation

```python
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()


# --- PARQUET ---
# Read with schema evolution (mergeSchema for new columns)
df = (spark.read
    .option("mergeSchema", "true")
    .parquet("s3://lake/raw/events/"))

# Write with Hive-style partitioning
(df.write
    .mode("overwrite")
    .partitionBy("event_date")
    .option("maxRecordsPerFile", 1_000_000)
    .parquet("s3://lake/curated/events/"))


# --- DELTA LAKE ---
# Read Delta table
orders = spark.read.format("delta").load("s3://lake/silver/orders")

# Read with time travel
orders_yesterday = (spark.read.format("delta")
    .option("timestampAsOf", "2026-03-25")
    .load("s3://lake/silver/orders"))

# Write Delta with merge schema
(df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .save("s3://lake/silver/orders"))


# --- ICEBERG ---
# Read Iceberg table via catalog
events = spark.read.format("iceberg").load("catalog.db.events")

# Read Iceberg snapshot (time travel)
events_v2 = (spark.read.format("iceberg")
    .option("snapshot-id", 123456789)
    .load("catalog.db.events"))

# Write to Iceberg with partition spec
(df.writeTo("catalog.db.events")
    .using("iceberg")
    .partitionedBy(F.days("event_date"))
    .createOrReplace())
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `mergeSchema` | `false` | Auto-add new columns on read/write |
| `maxRecordsPerFile` | unlimited | Cap file size for downstream readers |
| `partitionOverwriteMode` | `static` | Set to `dynamic` for partition-level overwrite |
| `spark.sql.sources.partitionColumnTypeInference.enabled` | `true` | Infer partition column types |

## Example Usage

```python
# Common pattern: read raw → transform → write curated
def etl_pipeline(spark: SparkSession, date: str) -> None:
    raw = (spark.read
        .option("mergeSchema", "true")
        .parquet(f"s3://lake/raw/orders/dt={date}"))

    curated = transform_orders(raw)

    (curated.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"order_date = '{date}'")
        .save("s3://lake/silver/orders"))
```

## Spark 4.0: VARIANT Type for Semi-Structured Data

```python
# VARIANT type — no schema definition needed for JSON
from pyspark.sql.types import VariantType

# Read JSON data as VARIANT (schema-free)
df = spark.sql("""
    SELECT id, parse_json(raw_json) AS payload
    FROM raw_events
""")

# Query with JSONPath — type-safe extraction
df.select(
    F.col("id"),
    F.col("payload:user.name").cast("string").alias("user_name"),
    F.col("payload:items[0].price").cast("decimal(10,2)").alias("first_item_price"),
).show()

# VARIANT in table definition
spark.sql("""
    CREATE TABLE events (
        event_id BIGINT,
        event_data VARIANT
    ) USING DELTA
""")
```

## See Also

- [delta-integration](../patterns/delta-integration.md)
- [partitioning](../concepts/partitioning.md)
