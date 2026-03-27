# Streaming Quick Reference

> Fast lookup tables for stream processing (2025+). For code examples, see linked files.

## Framework Comparison (2025 Versions)

| Feature | Flink 2.0 | Spark Streaming 4.0 | RisingWave 2.5 | Kafka Streams | Kafka 4.0 |
|---------|-----------|---------------------|----------------|---------------|-----------|
| Latency | ms | seconds (RT: ms) | ms | ms | ms (broker) |
| Language | Java/SQL | Python/SQL | SQL (Postgres) | Java | N/A (broker) |
| State mgmt | ForSt (disaggregated) | In-memory/disk | Hummock (S3) | RocksDB | KRaft (metadata) |
| Exactly-once | Yes | Yes | Yes | Yes | Yes |
| SQL support | Flink SQL | Spark SQL | Full Postgres | ksqlDB | N/A |
| ZooKeeper | N/A | N/A | N/A | N/A | Removed (KRaft-only) |
| Best for | Complex event processing | Unified batch+stream | Streaming SQL apps | Kafka-native | Event backbone |

## Windowing Types

| Window | Behavior | Use Case |
|--------|----------|----------|
| Tumbling | Fixed, non-overlapping | Hourly aggregations |
| Sliding (Hop) | Fixed, overlapping | Moving averages |
| Session | Gap-based, dynamic | User session analysis |
| Global | Single window | All-time aggregations |

## Exactly-Once Comparison

| Engine | Mechanism |
|--------|-----------|
| Flink | Chandy-Lamport checkpointing |
| Kafka | Idempotent producer + transactions |
| Spark | Write-ahead log + idempotent sinks |
| RisingWave | Internal consistency (no external coordination) |

## Kafka 4.0 Migration Quick Reference

| Change | Before (Kafka 3.x) | After (Kafka 4.0) |
|--------|--------------------|--------------------|
| Coordination | ZooKeeper ensemble | KRaft (built-in, mandatory) |
| Consumer protocol | Classic rebalance | KIP-848 new consumer group protocol (GA) |
| Partition scaling | Limited by ZK | Faster metadata handling |
| Deployment | Kafka + ZK clusters | Kafka-only (simpler) |
| Queue semantics | N/A | Share groups for queue-like consumption |

## Flink 2.0 Migration Quick Reference

| Change | Before (Flink 1.x) | After (Flink 2.0) |
|--------|--------------------|--------------------|
| State backend | RocksDB (local disk) | ForSt (disaggregated, remote DFS) |
| State API | Synchronous state access | Async state API (State V2) |
| DataSet API | Available | Removed (use DataStream or Table API) |
| Scala API | Scala DataStream API | Removed (use Java DataStream API) |
| Source/Sink API | SourceFunction, SinkFunction | Source V2 / Sink V2 only |
| Table API types | TableSchema, TableColumn | Schema, Column, DataTypes |
| Materialized Tables | N/A | Native Flink SQL materialized tables |

## Debezium 3.x Quick Reference

| Feature | Detail |
|---------|--------|
| Kafka compat | Built against Kafka Connect 3.9+, works with Kafka 4.0 |
| Async engine | Debezium Engine runs asynchronously for higher throughput |
| K8s Operator | Native Kubernetes operator for cloud-native CDC |
| Signal channels | Source-based signaling for incremental snapshots |
| Supported DBs | MySQL, Postgres, MongoDB, Oracle, SQL Server, Vitess, Cassandra |

## RisingWave 2.5 Quick Reference

| Feature | Detail |
|---------|--------|
| Iceberg native | Auto compaction, snapshot expiration, native Iceberg sink |
| Backfill control | Fine-grained MV backfill ordering |
| OpenAI embeddings | `openai_embedding()` function for real-time vectorization |
| Join isolation | Isolate high-amplification joins from other operators |
| Window clause | Enhanced SQL WINDOW clause support |

## Common Pitfalls

| Don't | Do |
|-------|-----|
| Process-time when event-time needed | Use event-time + watermarks |
| Unbounded state | Set state TTL, use watermarks |
| No dead letter queue | Route bad records to DLQ |
| Skip checkpointing | Enable checkpointing every 1-5 min |
| Process everything as stream | Use batch for historical backfill |
| Run Kafka with ZooKeeper (4.0+) | Migrate to KRaft before upgrading to 4.0 |
| Use Flink DataSet API (2.0+) | Migrate to DataStream API or Table API/SQL |
| Synchronous state in Flink 2.0 | Use async state API for disaggregated state |
