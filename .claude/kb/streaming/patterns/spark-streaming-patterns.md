# Spark Streaming Patterns

> **Purpose**: Structured Streaming — foreachBatch, triggers, watermarks, stream-to-stream joins
> **Confidence**: 0.90
> **MCP Validated**: 2026-03-26

## Overview

Spark Structured Streaming treats streaming data as an unbounded DataFrame. Key patterns: foreachBatch for complex sinks, trigger modes for latency/cost tradeoffs, watermarks for late data handling, and stream-to-stream joins for enrichment.

## The Pattern

```python
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType

spark = SparkSession.builder.appName("streaming_orders").getOrCreate()

# ============================================================
# 1. Read from Kafka with watermark
# ============================================================
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "orders")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")
)

orders_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("amount", DecimalType(12, 2)),
    StructField("order_ts", TimestampType()),
])

orders = (
    raw_stream
    .select(F.from_json(F.col("value"), orders_schema).alias("data"))
    .select("data.*")
    .withWatermark("order_ts", "10 minutes")  # tolerate 10 min late data
)

# ============================================================
# 2. Windowed aggregation
# ============================================================
windowed_revenue = (
    orders
    .groupBy(F.window("order_ts", "5 minutes"))
    .agg(
        F.count("*").alias("order_count"),
        F.sum("amount").alias("total_revenue"),
    )
)

# ============================================================
# 3. foreachBatch: write to Delta + trigger downstream
# ============================================================
def write_to_delta(batch_df: DataFrame, batch_id: int) -> None:
    """Write micro-batch to Delta with deduplication."""
    (
        batch_df.dropDuplicates(["order_id"])
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save("s3://lake/silver/orders")
    )

query = (
    orders.writeStream
    .foreachBatch(write_to_delta)
    .option("checkpointLocation", "s3://lake/checkpoints/orders")
    .trigger(availableNow=True)  # process all available, then stop
    .start()
)

# ============================================================
# 4. Stream-to-stream join (enrich orders with clicks)
# ============================================================
clicks = (
    spark.readStream.format("kafka")
    .option("subscribe", "clicks").load()
    .select(F.from_json(F.col("value"), clicks_schema).alias("c"))
    .select("c.*")
    .withWatermark("click_ts", "15 minutes")
)

enriched = orders.join(
    clicks,
    expr=F.expr("""
        orders.customer_id = clicks.customer_id AND
        clicks.click_ts BETWEEN orders.order_ts - INTERVAL 30 MINUTES
                            AND orders.order_ts
    """),
    how="left"
)

# ============================================================
# 5. Trigger modes reference
# ============================================================
# .trigger(processingTime="30 seconds")  # micro-batch every 30s
# .trigger(availableNow=True)            # process all, then stop (replaces once)
# .trigger(continuous="1 second")         # experimental low-latency
```

## Quick Reference

| Trigger Mode | Latency | Cost | Use Case |
|-------------|---------|------|----------|
| `processingTime="30s"` | 30s+ | Medium (always on) | Near-real-time dashboards |
| `availableNow=True` | Batch | Low (runs then stops) | Scheduled incremental loads |
| `continuous="1s"` | ~1s | High (always on) | Experimental low-latency |

## Related

- [kafka-producer-consumer](kafka-producer-consumer.md)
- [stream-processing-fundamentals](../concepts/stream-processing-fundamentals.md)
- [performance-tuning](../../spark/patterns/performance-tuning.md)
