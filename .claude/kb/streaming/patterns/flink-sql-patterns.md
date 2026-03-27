# Flink SQL Patterns

> **Purpose**: Flink SQL — Kafka source/sink, windowed aggregation, temporal joins, CDC ingestion
> **Confidence**: 0.90
> **MCP Validated**: 2026-03-26

## Overview

Flink SQL provides streaming SQL with windowed aggregations, temporal joins, and CDC support. Tables are either bounded (batch) or unbounded (streaming). Watermarks handle event-time processing and late data.

## The Pattern

```sql
-- ============================================================
-- Flink SQL: E-commerce event processing pipeline
-- ============================================================

-- 1. Kafka source table with watermark
CREATE TABLE raw_orders (
    order_id    STRING,
    customer_id STRING,
    amount      DECIMAL(12,2),
    status      STRING,
    order_ts    TIMESTAMP(3),
    -- Watermark: tolerate 10 seconds of late data
    WATERMARK FOR order_ts AS order_ts - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'orders',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-orders',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);

-- 2. Tumbling window aggregation (fixed 1-minute windows)
CREATE TABLE order_metrics_per_minute (
    window_start TIMESTAMP(3),
    window_end   TIMESTAMP(3),
    order_count  BIGINT,
    total_revenue DECIMAL(12,2),
    avg_order_value DECIMAL(12,2),
    PRIMARY KEY (window_start) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'order-metrics-1m',
    'properties.bootstrap.servers' = 'kafka:9092',
    'key.format' = 'json',
    'value.format' = 'json'
);

INSERT INTO order_metrics_per_minute
SELECT
    window_start,
    window_end,
    COUNT(*) AS order_count,
    SUM(amount) AS total_revenue,
    AVG(amount) AS avg_order_value
FROM TABLE(
    TUMBLE(TABLE raw_orders, DESCRIPTOR(order_ts), INTERVAL '1' MINUTE)
)
WHERE status = 'completed'
GROUP BY window_start, window_end;

-- 3. Hop window (sliding: 1-hour window, 5-minute slide)
SELECT
    window_start,
    window_end,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(amount) AS revenue
FROM TABLE(
    HOP(TABLE raw_orders, DESCRIPTOR(order_ts), INTERVAL '5' MINUTE, INTERVAL '1' HOUR)
)
GROUP BY window_start, window_end;

-- 4. Temporal join (enrich events with latest dimension)
CREATE TABLE dim_customers (
    customer_id STRING,
    customer_name STRING,
    segment STRING,
    region STRING,
    update_ts TIMESTAMP(3),
    WATERMARK FOR update_ts AS update_ts - INTERVAL '5' SECOND,
    PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'customers-cdc',
    'format' = 'debezium-json'
);

SELECT
    o.order_id,
    o.amount,
    o.order_ts,
    c.customer_name,
    c.segment,
    c.region
FROM raw_orders o
JOIN dim_customers FOR SYSTEM_TIME AS OF o.order_ts AS c
ON o.customer_id = c.customer_id;

-- 5. Sink to Iceberg
CREATE TABLE iceberg_orders (
    order_id STRING, customer_id STRING, amount DECIMAL(12,2),
    status STRING, order_ts TIMESTAMP(3)
) WITH (
    'connector' = 'iceberg',
    'catalog-name' = 'lakehouse',
    'catalog-type' = 'rest',
    'uri' = 'http://rest-catalog:8181'
);

INSERT INTO iceberg_orders SELECT * FROM raw_orders WHERE status = 'completed';
```

## Quick Reference

| Window Type | Use Case | SQL Function |
|-------------|----------|-------------|
| TUMBLE | Fixed non-overlapping windows | `TUMBLE(TABLE, DESCRIPTOR, size)` |
| HOP | Sliding overlapping windows | `HOP(TABLE, DESCRIPTOR, slide, size)` |
| SESSION | Activity-based gaps | `SESSION(TABLE, DESCRIPTOR, gap)` |
| CUMULATE | Expanding windows | `CUMULATE(TABLE, DESCRIPTOR, step, max)` |

## Related

- [flink-architecture concept](../concepts/flink-architecture.md)
- [kafka-producer-consumer](kafka-producer-consumer.md)
- [cdc-patterns](cdc-patterns.md)
