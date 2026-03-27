# Streaming Databases

> **Purpose**: RisingWave 2.5, Materialize — streaming SQL databases for continuous aggregation
> **Version Coverage**: RisingWave 2.5+ (Aug 2025), Materialize (SaaS)
> **Confidence**: 0.88
> **MCP Validated**: 2026-03-26

## Overview

Streaming databases (RisingWave, Materialize) run SQL queries continuously over event streams, maintaining materialized views that update in real-time. Unlike Flink/Spark Streaming which process in micro-batches or event-at-a-time, streaming databases feel like Postgres — you write SQL and it stays current.

## The Concept

```sql
-- RisingWave: Postgres-compatible streaming SQL
-- Connect to Kafka source
CREATE SOURCE kafka_events (
    event_id    VARCHAR,
    user_id     VARCHAR,
    event_type  VARCHAR,
    amount      DECIMAL,
    event_ts    TIMESTAMPTZ
) WITH (
    connector = 'kafka',
    topic = 'raw_events',
    properties.bootstrap.server = 'kafka:9092',
    scan.startup.mode = 'earliest'
) FORMAT PLAIN ENCODE JSON;

-- Materialized view: auto-updates as events arrive
CREATE MATERIALIZED VIEW mv_revenue_per_minute AS
SELECT
    date_trunc('minute', event_ts) AS minute_window,
    event_type,
    COUNT(*) AS event_count,
    SUM(amount) AS total_revenue,
    COUNT(DISTINCT user_id) AS unique_users
FROM kafka_events
WHERE event_type = 'purchase'
GROUP BY 1, 2;

-- Query like Postgres (always fresh)
SELECT * FROM mv_revenue_per_minute
WHERE minute_window > NOW() - INTERVAL '1 hour'
ORDER BY minute_window DESC;
```

## Quick Reference

| Feature | RisingWave | Materialize | Flink SQL |
|---------|-----------|------------|-----------|
| SQL dialect | PostgreSQL | PostgreSQL | Flink SQL (ANSI+) |
| Deployment | Cloud / self-hosted | Cloud-only (SaaS) | Self-hosted / managed |
| Source | Kafka, Postgres CDC, S3 | Kafka, Postgres CDC | Kafka, files, JDBC |
| Sink | Kafka, JDBC, S3, Iceberg | Kafka, Postgres, webhooks | Kafka, JDBC, Iceberg |
| State backend | Hummock (LSM on S3) | Differential dataflow | RocksDB |
| Best for | Wide SQL compat, cost-efficient | Complex joins/aggregations | Full streaming ecosystem |
| Latency | Sub-second | Sub-second | Milliseconds (per-event) |
| Cost model | Compute-based | Consumption-based | Cluster-based |

## Common Mistakes

### Wrong

```sql
-- Running complex windowed aggregation in application code
-- Polling database every second for updated counts
SELECT COUNT(*) FROM events WHERE event_ts > NOW() - INTERVAL '5 min';
```

### Correct

```sql
-- Let the streaming database maintain the result continuously
CREATE MATERIALIZED VIEW live_counts AS
SELECT COUNT(*) AS recent_events
FROM kafka_events
WHERE event_ts > NOW() - INTERVAL '5 minutes';
-- Query is always pre-computed, sub-millisecond reads
```

## RisingWave 2.5 New Features (Aug 2025)

### Native Apache Iceberg Integration

```sql
-- RisingWave 2.5: native Iceberg sink with auto-compaction
CREATE SINK iceberg_orders INTO iceberg_catalog.db.orders
FROM mv_orders
WITH (
    connector = 'iceberg',
    type = 'upsert',
    primary_key = 'order_id',
    enable_compaction = true,           -- auto-compact small files
    snapshot_expiration_interval = '7d' -- auto-expire old snapshots
);
```

### OpenAI Embedding Function

```sql
-- Generate embeddings in real-time streaming pipelines
CREATE MATERIALIZED VIEW product_embeddings AS
SELECT
    product_id,
    product_name,
    openai_embedding(description, 'text-embedding-3-small') AS embedding
FROM products_stream;
```

### Backfill Order Control

```sql
-- Fine-grained control over MV backfill ordering
-- Prioritize specific tables during initial materialization
SET backfill_rate_limit = 1000;  -- rows per second
```

### RisingWave Positioning (2025)

| Capability | Detail |
|-----------|--------|
| Branding | "Event Streaming Platform" (not just streaming DB) |
| Iceberg native | First-class Iceberg table engine + sink |
| Cloud tiers | Basic (free), Pro (enterprise), Dedicated |
| Python interface | DataFrame-style API alongside SQL |
| AI integration | Built-in embedding functions, vector similarity |

## Related

- [stream-processing-fundamentals](stream-processing-fundamentals.md)
- [flink-architecture](flink-architecture.md)
- [flink-sql-patterns](../patterns/flink-sql-patterns.md)
