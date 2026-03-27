# Pub/Sub

> **Purpose**: Fully managed asynchronous messaging service for decoupling pipeline components
> **Confidence**: 0.95
> **MCP Validated**: 2026-02-17

## Overview

Google Cloud Pub/Sub is a global messaging service that enables asynchronous communication
between services. Publishers send messages to topics, and subscribers receive messages through
subscriptions. It guarantees at-least-once delivery, supports push and pull delivery modes,
and handles automatic scaling to millions of messages per second.

## The Pattern

```python
from google.cloud import pubsub_v1
import json

# --- Publishing ---
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path("my-project", "my-topic")

def publish_event(data: dict, attributes: dict = None):
    """Publish a message with JSON data and optional attributes."""
    message_bytes = json.dumps(data).encode("utf-8")
    future = publisher.publish(
        topic_path,
        data=message_bytes,
        **(attributes or {})
    )
    message_id = future.result()  # Block until published
    return message_id

# --- Subscribing (pull mode) ---
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path("my-project", "my-sub")

def callback(message):
    """Process received message."""
    data = json.loads(message.data.decode("utf-8"))
    print(f"Received: {data}")
    message.ack()  # Acknowledge after successful processing

streaming_pull = subscriber.subscribe(subscription_path, callback=callback)
```

## Quick Reference

| Input | Output | Notes |
|-------|--------|-------|
| `publisher.publish(topic, data)` | `Future` with message ID | Reuse publisher client |
| `subscriber.subscribe(sub, cb)` | `StreamingPullFuture` | Blocks until cancelled |
| `message.ack()` | Acknowledge delivery | Must call or message redelivers |
| `message.nack()` | Negative acknowledge | Message redelivered immediately |

## Delivery Modes

| Mode | Use Case | How It Works |
|------|----------|-------------|
| **Push** | Cloud Run / HTTP endpoints | Pub/Sub sends POST to endpoint |
| **Pull** | Long-running workers | Client polls for messages |
| **StreamingPull** | High-throughput consumers | Persistent bidirectional stream |

## Common Mistakes

### Wrong

```python
# Creating a new publisher for every message (expensive)
def publish_bad(data):
    publisher = pubsub_v1.PublisherClient()  # Slow: new connection each time
    topic = publisher.topic_path("project", "topic")
    publisher.publish(topic, json.dumps(data).encode())
```

### Correct

```python
# Reuse publisher client across calls
publisher = pubsub_v1.PublisherClient()  # Create once at module level
topic_path = publisher.topic_path("project", "topic")

def publish_good(data):
    future = publisher.publish(topic_path, json.dumps(data).encode())
    return future.result()
```

## Message Attributes

```python
# Use attributes for routing and filtering without parsing the body
publisher.publish(
    topic_path,
    data=json.dumps(payload).encode("utf-8"),
    event_type="file_uploaded",
    bucket="raw-data",
    content_type="application/json"
)
```

## Related

- [Cloud Run](../concepts/cloud-run.md) - Push subscription target
- [GCS](../concepts/gcs.md) - Event notification source
- [Pub/Sub Fan-out](../patterns/pubsub-fanout.md) - Multiple subscriber pattern
- [Event-Driven Pipeline](../patterns/event-driven-pipeline.md) - Full pipeline pattern
