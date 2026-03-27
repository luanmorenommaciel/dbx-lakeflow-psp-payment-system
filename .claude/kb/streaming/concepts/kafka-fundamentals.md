# Kafka Fundamentals

> **Purpose**: Topics, partitions, consumer groups, exactly-once semantics, KRaft, Schema Registry, Redpanda
> **Version Coverage**: Apache Kafka 4.0+ (March 2025) -- ZooKeeper fully removed
> **Confidence**: 0.94
> **MCP Validated**: 2026-03-26

## Overview

Apache Kafka is a distributed event streaming platform built around an immutable, partitioned, replicated commit log. **Topics** organize events by category, **partitions** provide parallelism and ordering guarantees, and **consumer groups** enable scalable consumption. Exactly-once semantics are achieved through idempotent producers and transactional writes. **Redpanda** is a Kafka-compatible alternative written in C++ that eliminates the JVM and ZooKeeper dependencies.

## The Concept

```python
# Kafka architecture — conceptual model
#
# Topic: "orders" (3 partitions, replication-factor=3)
#
# Partition 0:  [msg0] [msg3] [msg6] [msg9]  → Leader: Broker 1
# Partition 1:  [msg1] [msg4] [msg7] [msg10] → Leader: Broker 2
# Partition 2:  [msg2] [msg5] [msg8] [msg11] → Leader: Broker 3
#
# Consumer Group "order-processor" (3 consumers)
#   Consumer A  ← reads Partition 0
#   Consumer B  ← reads Partition 1
#   Consumer C  ← reads Partition 2
#
# Key guarantees:
#   - Messages with same key → same partition → total order
#   - Each partition consumed by exactly one consumer per group
#   - Offsets committed per partition per consumer group

from confluent_kafka import Producer
import json

conf = {
    'bootstrap.servers': 'kafka:9092',
    'enable.idempotence': True,           # exactly-once producer
    'acks':              'all',           # wait for all ISR replicas
    'max.in.flight.requests.per.connection': 5,  # safe with idempotence
    'compression.type':  'zstd',          # best compression ratio
}

producer = Producer(conf)

def produce_order(order: dict):
    producer.produce(
        topic='orders',
        key=str(order['customer_id']).encode(),   # partition by customer
        value=json.dumps(order).encode(),
        callback=lambda err, msg: print(f"Error: {err}") if err else None,
    )
    producer.flush()
```

## Quick Reference

| Concept | Description | Default |
|---------|-------------|---------|
| Topic | Named stream of records | — |
| Partition | Ordered, immutable sequence within a topic | `num.partitions=1` |
| Offset | Sequential ID per message within a partition | Auto-assigned |
| Consumer Group | Set of consumers sharing topic consumption | — |
| Replication Factor | Number of partition copies across brokers | `default.replication.factor=1` |
| ISR | In-Sync Replicas — replicas caught up to leader | Must include leader |
| Compacted Topic | Retains latest value per key (like a table) | `cleanup.policy=compact` |
| Schema Registry | Central schema store (Avro, Protobuf, JSON Schema) | Confluent / Apicurio |

| Kafka vs Redpanda | Kafka 4.0 | Redpanda |
|-------------------|-----------|----------|
| Runtime | JVM (Java) | C++ (no JVM) |
| Coordination | KRaft (ZooKeeper removed) | Raft (built-in) |
| Tail latency (p99) | ~10ms | ~2ms |
| API compatibility | Native | Kafka-compatible |
| Tiered storage | Confluent only | Built-in |
| Schema Registry | Confluent / Apicurio | Built-in |
| Queue semantics | Share groups (4.0) | N/A |

## Common Mistakes

### Wrong

```python
# Auto-commit with no error handling — offsets committed before processing
conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id':          'my-group',
    'enable.auto.commit': True,          # commits before processing finishes
    'auto.offset.reset':  'latest',      # skips existing data on first start
}
```

### Correct

```python
# Manual commit after successful processing
from confluent_kafka import Consumer

conf = {
    'bootstrap.servers':  'kafka:9092',
    'group.id':           'my-group',
    'enable.auto.commit': False,          # manual commit control
    'auto.offset.reset':  'earliest',     # process from beginning on first start
    'isolation.level':    'read_committed',# skip uncommitted transactional msgs
}

consumer = Consumer(conf)
consumer.subscribe(['orders'])

while True:
    msg = consumer.poll(timeout=1.0)
    if msg is None:
        continue
    if msg.error():
        handle_error(msg.error())
        continue
    process(msg)                           # process first
    consumer.commit(asynchronous=False)    # then commit
```

## Kafka 4.0: Key Changes (March 2025)

### ZooKeeper Fully Removed

```properties
# Kafka 4.0: KRaft is the ONLY coordination mode
# ZooKeeper support has been completely removed

# KRaft controller configuration
process.roles=controller
node.id=1
controller.quorum.voters=1@controller1:9093,2@controller2:9093,3@controller3:9093
controller.listener.names=CONTROLLER
listeners=CONTROLLER://:9093

# Combined mode (broker + controller in one process — dev/small clusters)
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
```

### New Consumer Group Protocol (KIP-848)

```python
# Kafka 4.0: new consumer group protocol — faster rebalances
# Dramatically reduces downtime during consumer scaling events
# Uses server-side assignment — no stop-the-world rebalance

conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'order-processor',
    'group.protocol': 'consumer',          # new protocol (4.0+)
    # 'group.protocol': 'classic',         # legacy protocol (backward compat)
    'enable.auto.commit': False,
    'auto.offset.reset': 'earliest',
}
```

### Queue Semantics (Share Groups)

```properties
# Kafka 4.0: share groups enable queue-like consumption
# Multiple consumers can process from the same partition
# Messages are distributed (not replicated) across consumers
# Useful for work distribution / task queue patterns
group.type=share
```

### Migration from ZooKeeper

| Step | Action |
|------|--------|
| 1 | Upgrade to Kafka 3.7+ with ZooKeeper (bridge release) |
| 2 | Run `kafka-metadata.sh` to migrate metadata to KRaft |
| 3 | Switch controllers to KRaft mode |
| 4 | Remove ZooKeeper dependency |
| 5 | Upgrade to Kafka 4.0 (KRaft-only) |

## Related

- [kafka-producer-consumer](../patterns/kafka-producer-consumer.md)
- [flink-sql-patterns](../patterns/flink-sql-patterns.md)
- [cdc-patterns](../patterns/cdc-patterns.md)
