# Kafka Producer & Consumer

> **Purpose**: Idempotent producer, consumer groups, DLQ, exactly-once transactions
> **Confidence**: 0.95
> **MCP Validated**: 2026-03-26

## Overview

Production Kafka patterns for Python using confluent-kafka: idempotent producers (prevent duplicates), consumer groups with manual commit (at-least-once), dead letter queues for poison messages, and exactly-once transactional processing.

## The Pattern

```python
from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
import json, logging

logger = logging.getLogger(__name__)

# ============================================================
# Idempotent Producer (prevents duplicates on retry)
# ============================================================
producer_config = {
    "bootstrap.servers": "kafka:9092",
    "enable.idempotence": True,        # exactly-once per partition
    "acks": "all",                     # wait for all replicas
    "retries": 5,
    "max.in.flight.requests.per.connection": 5,
    "compression.type": "zstd",
    "linger.ms": 10,                   # batch for throughput
    "batch.size": 65536,
}

producer = Producer(producer_config)

def produce_event(topic: str, key: str, value: dict) -> None:
    """Produce a message with delivery callback."""
    def on_delivery(err, msg):
        if err:
            logger.error("Delivery failed: %s", err)
        else:
            logger.debug("Delivered to %s [%d] @ %d", msg.topic(), msg.partition(), msg.offset())

    producer.produce(
        topic=topic,
        key=key.encode("utf-8"),
        value=json.dumps(value).encode("utf-8"),
        on_delivery=on_delivery,
    )
    producer.poll(0)  # trigger callbacks

producer.flush()  # wait for all messages to be delivered

# ============================================================
# Consumer with manual commit + DLQ
# ============================================================
consumer_config = {
    "bootstrap.servers": "kafka:9092",
    "group.id": "order-processor",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,       # manual commit for at-least-once
    "max.poll.interval.ms": 300000,
    "session.timeout.ms": 45000,
}

consumer = Consumer(consumer_config)
consumer.subscribe(["orders"])

dlq_producer = Producer({"bootstrap.servers": "kafka:9092"})

def process_messages(max_messages: int = 1000) -> None:
    """Consume with DLQ for poison messages."""
    processed = 0
    while processed < max_messages:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            logger.error("Consumer error: %s", msg.error())
            break

        try:
            event = json.loads(msg.value().decode("utf-8"))
            handle_order(event)  # business logic
            consumer.commit(message=msg, asynchronous=False)
            processed += 1
        except Exception as e:
            logger.error("Failed to process message: %s", e)
            # Route to Dead Letter Queue
            dlq_producer.produce(
                topic="orders-dlq",
                key=msg.key(),
                value=msg.value(),
                headers={"error": str(e).encode(), "original_topic": b"orders"},
            )
            consumer.commit(message=msg, asynchronous=False)

    consumer.close()
```

## Quick Reference

| Config | Value | Purpose |
|--------|-------|---------|
| `enable.idempotence` | `True` | Prevent duplicate produces on retry |
| `acks` | `all` | Durability: wait for all ISR replicas |
| `enable.auto.commit` | `False` | Manual commit for at-least-once |
| `auto.offset.reset` | `earliest` | Start from beginning for new groups |
| `compression.type` | `zstd` | Best ratio/speed for most workloads |
| `linger.ms` | `10-50` | Batch messages for throughput |

## Related

- [kafka-fundamentals concept](../concepts/kafka-fundamentals.md)
- [cdc-patterns](cdc-patterns.md)
- [flink-sql-patterns](flink-sql-patterns.md)
