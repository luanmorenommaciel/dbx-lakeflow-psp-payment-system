# Streaming Knowledge Base

> **Purpose**: Stream processing — Flink 2.0, Kafka 4.0 (KRaft-only), Spark Streaming 4.0, RisingWave 2.x, CDC
> **Version Coverage**: Flink 2.0+ (March 2025), Kafka 4.0+ (March 2025), Debezium 3.x, RisingWave 2.5+
> **MCP Validated**: 2026-03-26

## Quick Navigation

### Concepts (< 150 lines each)

| File | Purpose |
|------|---------|
| [concepts/stream-processing-fundamentals.md](concepts/stream-processing-fundamentals.md) | Event time, watermarks, windowing |
| [concepts/flink-architecture.md](concepts/flink-architecture.md) | Checkpointing, state backends, Flink SQL |
| [concepts/kafka-fundamentals.md](concepts/kafka-fundamentals.md) | Topics, partitions, exactly-once, Redpanda |
| [concepts/streaming-databases.md](concepts/streaming-databases.md) | RisingWave, Materialize — streaming SQL |

### Patterns (< 200 lines each)

| File | Purpose |
|------|---------|
| [patterns/flink-sql-patterns.md](patterns/flink-sql-patterns.md) | Kafka tables, windowed agg, temporal joins |
| [patterns/kafka-producer-consumer.md](patterns/kafka-producer-consumer.md) | Idempotency, DLQ, transactions |
| [patterns/spark-streaming-patterns.md](patterns/spark-streaming-patterns.md) | foreachBatch, watermarks, stream joins |
| [patterns/cdc-patterns.md](patterns/cdc-patterns.md) | Debezium, Flink CDC, Delta CDF |

---

## Quick Reference

- [quick-reference.md](quick-reference.md) - Fast lookup tables

## Key Version Changes (2025)

| Technology | Version | Headline Change |
|-----------|---------|-----------------|
| **Apache Flink** | 2.0 (Mar 2025) | Disaggregated state, async state API, removed DataSet/Scala APIs |
| **Apache Kafka** | 4.0 (Mar 2025) | ZooKeeper fully removed, KRaft-only, new consumer group protocol |
| **Debezium** | 3.x (Oct 2024+) | Kafka 4.0 compatibility, async engine, Kubernetes operator |
| **RisingWave** | 2.5 (Aug 2025) | Native Iceberg integration, backfill control, OpenAI embeddings |
| **Spark Streaming** | 4.0 (May 2025) | TransformWithState operator, Python Data Source streaming |

## Agent Usage

| Agent | Primary Files | Use Case |
|-------|---------------|----------|
| streaming-engineer | All files | Stream pipeline design |
| pipeline-architect | patterns/cdc-patterns.md | CDC pipeline orchestration |
