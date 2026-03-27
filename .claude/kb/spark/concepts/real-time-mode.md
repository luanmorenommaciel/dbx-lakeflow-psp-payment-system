# Real-Time Mode

> **Purpose**: Spark Structured Streaming Real-Time Mode — sub-5ms latency, watermarks, output modes
> **Confidence**: 0.90
> **MCP Validated**: 2026-03-26

## Overview

Spark Structured Streaming processes data as micro-batches or (on Databricks 2026+) in **Real-Time Mode** with ~5ms latency. The programming model is identical to batch — the same DataFrame API — but with continuous input sources (Kafka, Delta, Kinesis) and output sinks. Watermarks handle late-arriving data by defining how long to wait before finalizing windows.

## The Concept

```python
from pyspark.sql import functions as F

# Read from Kafka as a streaming DataFrame
orders_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker:9092")
    .option("subscribe", "orders")
    .option("startingOffsets", "latest")
    .load()
    .select(
        F.from_json(F.col("value").cast("string"), order_schema).alias("data")
    )
    .select("data.*")
)

# Windowed aggregation with watermark for late data
revenue_per_minute = (
    orders_stream
    .withWatermark("event_time", "10 minutes")
    .groupBy(F.window("event_time", "1 minute"))
    .agg(F.sum("amount").alias("total_revenue"))
)
```

## Quick Reference

| Mode | Latency | Trigger | Use Case |
|------|---------|---------|----------|
| Micro-batch (default) | 100ms-seconds | `processingTime="10 seconds"` | Most streaming workloads |
| AvailableNow | Batch-like | `availableNow=True` | Catch-up processing, backfills |
| Continuous (experimental) | ~1ms | `continuous="1 second"` | Ultra-low latency, limited ops |
| Real-Time Mode (DBR 2026) | ~5ms | Automatic | Production low-latency on Databricks |

| Output Mode | Behavior | Use With |
|-------------|----------|----------|
| `append` | Only new rows, after watermark | Windowed aggs, non-agg queries |
| `update` | Changed rows only | Aggregations, stateful ops |
| `complete` | Entire result table | Small aggregations only |

## Common Mistakes

### Wrong

```python
# No watermark with windowed aggregation — unbounded state growth
orders_stream.groupBy(F.window("event_time", "1 minute")).count()
```

### Correct

```python
# Watermark bounds state: drop data older than 10 min
(orders_stream
    .withWatermark("event_time", "10 minutes")
    .groupBy(F.window("event_time", "1 minute"))
    .count())
```

## Related

- [dataframe-api](../concepts/dataframe-api.md)
- [spark-streaming-patterns](../../streaming/patterns/spark-streaming-patterns.md)
