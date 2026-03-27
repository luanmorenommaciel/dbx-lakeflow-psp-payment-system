# Stream Processing Fundamentals

> **Purpose**: Event time vs processing time, watermarks, late data handling, windowing types
> **Confidence**: 0.93
> **MCP Validated**: 2026-03-26

## Overview

Stream processing engines must reconcile two timelines: **event time** (when data was generated) and **processing time** (when the engine sees it). Watermarks track progress in event time, enabling the engine to know when a window is complete. Windowing groups unbounded streams into finite sets for aggregation.

## The Concept

```sql
-- Flink SQL: Event-time tumbling window with watermarks
CREATE TABLE page_views (
    user_id     STRING,
    page_url    STRING,
    view_time   TIMESTAMP(3),
    -- Declare event-time attribute with 5-second watermark tolerance
    WATERMARK FOR view_time AS view_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic'     = 'page_views',
    'format'    = 'json'
);

-- Tumbling window: fixed, non-overlapping 1-minute buckets
SELECT
    window_start,
    window_end,
    COUNT(*)        AS view_count,
    COUNT(DISTINCT user_id) AS unique_users
FROM TABLE(
    TUMBLE(TABLE page_views, DESCRIPTOR(view_time), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end;
```

```python
# PySpark Structured Streaming: watermarks + sliding window
from pyspark.sql import functions as F

events = (
    spark.readStream
    .format("kafka")
    .option("subscribe", "page_views")
    .load()
    .selectExpr("CAST(value AS STRING)", "timestamp")
)

windowed = (
    events
    .withWatermark("timestamp", "10 minutes")   # late-data tolerance
    .groupBy(
        F.window("timestamp", "5 minutes", "1 minute")  # sliding window
    )
    .count()
)
```

## Quick Reference

| Concept | Definition | Example |
|---------|-----------|---------|
| Event time | When the event actually occurred | Click timestamp from browser |
| Processing time | When the engine processes the event | Server wall-clock at ingestion |
| Ingestion time | When the event enters the system | Kafka append timestamp |
| Watermark | Assertion: "no events older than W will arrive" | `current_max_event_time - tolerance` |
| Late data | Events arriving after their window's watermark | Mobile events sent after reconnect |
| Allowed lateness | Grace period after watermark fires | Flink: side output; Spark: watermark delay |

## Windowing Types

| Window | Behavior | Flink SQL | Use Case |
|--------|----------|-----------|----------|
| Tumbling | Fixed, non-overlapping | `TUMBLE(..., INTERVAL '1' HOUR)` | Hourly aggregations |
| Sliding (Hop) | Fixed, overlapping | `HOP(..., INTERVAL '5' MIN, INTERVAL '1' HOUR)` | Moving averages |
| Session | Gap-based, variable size | `SESSION(..., INTERVAL '30' MIN)` | User activity sessions |
| Cumulate | Expanding within a max | `CUMULATE(..., INTERVAL '1' MIN, INTERVAL '1' DAY)` | Running daily totals |

## Common Mistakes

### Wrong

```python
# Using processing time when event time is available
# Late or out-of-order data produces incorrect window assignments
events.groupBy(
    F.window(F.current_timestamp(), "5 minutes")  # processing time!
).count()
```

### Correct

```python
# Use event time with watermark for correct results
events \
    .withWatermark("event_timestamp", "10 minutes") \
    .groupBy(
        F.window("event_timestamp", "5 minutes")   # event time
    ).count()
```

## Related

- [flink-architecture](../concepts/flink-architecture.md)
- [flink-sql-patterns](../patterns/flink-sql-patterns.md)
- [spark-streaming-patterns](../patterns/spark-streaming-patterns.md)
