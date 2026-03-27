# CDC Patterns

> **Purpose**: Change Data Capture with Debezium 3.x, Flink CDC, Delta Lake CDF, Iceberg incremental reads
> **Version Coverage**: Debezium 3.0+ (Oct 2024), Flink CDC 3.x, Kafka 4.0 compatible
> **Confidence**: 0.90
> **MCP Validated**: 2026-03-26

## Overview

Change Data Capture (CDC) tracks row-level changes (inserts, updates, deletes) in source databases and propagates them downstream. **Debezium** is the standard open-source CDC engine running as Kafka Connect source connectors. **Flink CDC** captures changes directly into Flink without Kafka. **Delta Lake Change Data Feed (CDF)** and **Iceberg incremental reads** enable CDC-style consumption from lakehouse tables, turning batch tables into streaming sources.

## The Pattern

```json
// =============================================
// 1. Debezium — Kafka Connect Source Connector
//    MySQL -> Kafka (Avro + Schema Registry)
// =============================================
{
  "name": "mysql-cdc-orders",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "database.hostname": "mysql-primary",
    "database.port": "3306",
    "database.user": "debezium",
    "database.password": "${file:/secrets/mysql.properties:password}",
    "database.server.id": "184054",
    "topic.prefix": "cdc.ecommerce",
    "database.include.list": "ecommerce",
    "table.include.list": "ecommerce.orders,ecommerce.customers",

    "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
    "schema.history.internal.kafka.topic": "schema-changes.ecommerce",

    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",

    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.unwrap.add.fields": "op,source.ts_ms",

    "snapshot.mode": "initial",
    "signal.enabled.channels": "source",
    "incremental.snapshot.enabled": "true"
  }
}
```

```sql
-- =============================================
-- 2. Flink CDC — Direct MySQL to Iceberg (no Kafka)
-- =============================================
-- Source: captures binlog changes directly
CREATE TABLE mysql_orders (
    order_id     BIGINT,
    customer_id  BIGINT,
    amount       DECIMAL(10, 2),
    status       STRING,
    created_at   TIMESTAMP(3),
    updated_at   TIMESTAMP(3),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector'      = 'mysql-cdc',
    'hostname'       = 'mysql-primary',
    'port'           = '3306',
    'username'       = 'flink_cdc',
    'password'       = '${secret_values.mysql_password}',
    'database-name'  = 'ecommerce',
    'table-name'     = 'orders',
    'server-time-zone' = 'UTC',
    'scan.incremental.snapshot.enabled' = 'true',
    'scan.incremental.snapshot.chunk.size' = '8096'
);

-- Sink: Iceberg table with upsert
CREATE TABLE iceberg_orders (
    order_id     BIGINT,
    customer_id  BIGINT,
    amount       DECIMAL(10, 2),
    status       STRING,
    created_at   TIMESTAMP(3),
    updated_at   TIMESTAMP(3),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector'              = 'iceberg',
    'catalog-name'           = 'hive_catalog',
    'catalog-type'           = 'hive',
    'warehouse'              = 's3://lakehouse/iceberg',
    'write.upsert.enabled'   = 'true',
    'write.format.default'   = 'parquet',
    'write.parquet.compression-codec' = 'zstd'
);

-- Single INSERT statement runs as a continuous streaming job
INSERT INTO iceberg_orders SELECT * FROM mysql_orders;
```

```sql
-- =============================================
-- 3. Delta Lake — Change Data Feed (CDF)
-- =============================================
-- Enable CDF on an existing Delta table
ALTER TABLE delta.`s3://warehouse/orders`
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- Read changes since a specific version
SELECT *
FROM table_changes('delta.`s3://warehouse/orders`', 5)
-- Returns: _change_type (insert, update_preimage, update_postimage, delete)
--          _commit_version, _commit_timestamp
WHERE _change_type IN ('insert', 'update_postimage')
ORDER BY _commit_timestamp;
```

```python
# Read CDF as a streaming source in PySpark
cdf_stream = (
    spark.readStream
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 5)
    .table("orders")
)

# Process only new inserts and updated rows
changes = cdf_stream.filter(
    "_change_type IN ('insert', 'update_postimage')"
)

changes.writeStream \
    .format("delta") \
    .option("checkpointLocation", "s3://warehouse/_checkpoints/orders_cdf") \
    .trigger(availableNow=True) \
    .start("s3://warehouse/orders_downstream")
```

```sql
-- =============================================
-- 4. Iceberg — Incremental Reads
-- =============================================
-- Spark SQL: read only new snapshots since a given snapshot ID
SELECT *
FROM ecommerce.orders
VERSION AS OF 1234567890  -- specific snapshot
;

-- Incremental read between two snapshots (Spark 3.x + Iceberg 1.4+)
CALL catalog.system.create_changelog_view(
    table => 'ecommerce.orders',
    options => map(
        'start-snapshot-id', '1234567890',
        'end-snapshot-id',   '1234567999'
    )
);

SELECT * FROM ecommerce.orders_changelog
WHERE _change_type IN ('INSERT', 'UPDATE_AFTER');
```

## Quick Reference

| CDC Engine | Source | Transport | Snapshot | Format |
|-----------|--------|-----------|----------|--------|
| Debezium | MySQL, Postgres, MongoDB, Oracle | Kafka (Connect) | Initial + incremental | Avro, JSON, Protobuf |
| Flink CDC | MySQL, Postgres, MongoDB, Oracle | Direct (no Kafka) | Incremental (parallel) | Internal Flink rows |
| Delta CDF | Delta Lake tables | Spark readStream | Version-based | Parquet (with _change_type) |
| Iceberg changelog | Iceberg tables | Spark / Flink | Snapshot-based | Parquet / ORC |

| Pattern | Latency | Complexity | Best For |
|---------|---------|------------|----------|
| Debezium -> Kafka -> Flink/Spark | Seconds | High (3 systems) | Multi-consumer CDC |
| Flink CDC -> Iceberg | Seconds | Medium (2 systems) | Direct DB-to-lake |
| Delta CDF streaming | Minutes | Low (Spark only) | Lake-to-lake pipelines |
| Iceberg incremental | Minutes | Low (Spark only) | Audit trails, downstream refresh |

## Common Mistakes

### Wrong

```json
// Debezium: no schema registry — schema embedded in every message
// Doubles message size and breaks downstream compatibility
{
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": "true",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "true"
}
// Also missing: ExtractNewRecordState — consumers get nested before/after
```

### Correct

```json
// Avro + Schema Registry: compact, schema-evolved, compatible
{
  "key.converter": "io.confluent.connect.avro.AvroConverter",
  "key.converter.schema.registry.url": "http://schema-registry:8081",
  "value.converter": "io.confluent.connect.avro.AvroConverter",
  "value.converter.schema.registry.url": "http://schema-registry:8081",
  "transforms": "unwrap",
  "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
  "transforms.unwrap.delete.handling.mode": "rewrite"
}
```

## Debezium 3.x Enhancements (2024-2025)

### Async Debezium Engine

```java
// Debezium 3.x: asynchronous engine for higher throughput
// No longer tied to Kafka Connect runtime — can embed anywhere
DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
    .using(props)
    .notifying(record -> {
        // Process CDC events asynchronously
        processChange(record);
    })
    .build();

ExecutorService executor = Executors.newSingleThreadExecutor();
executor.execute(engine);
```

### Kubernetes Operator

```yaml
# Debezium 3.x: native Kubernetes operator for cloud-native CDC
apiVersion: debezium.io/v1alpha1
kind: DebeziumServer
metadata:
  name: mysql-cdc
spec:
  version: 3.0.8
  sink:
    type: kafka
    config:
      bootstrap.servers: kafka:9092
  source:
    class: io.debezium.connector.mysql.MySqlConnector
    config:
      database.hostname: mysql
      database.port: "3306"
      topic.prefix: cdc.ecommerce
```

### Kafka 4.0 Compatibility

| Debezium Version | Kafka Connect | Kafka Broker |
|-----------------|---------------|-------------|
| 3.0.8+ | 3.9.0 | 3.9.0+ / 4.0 compatible |
| 3.1+ | 4.0+ | 4.0+ (KRaft-only) |

## Related

- [kafka-fundamentals](../concepts/kafka-fundamentals.md)
- [flink-sql-patterns](../patterns/flink-sql-patterns.md)
- [spark-streaming-patterns](../patterns/spark-streaming-patterns.md)
- [streaming-databases](../concepts/streaming-databases.md)
