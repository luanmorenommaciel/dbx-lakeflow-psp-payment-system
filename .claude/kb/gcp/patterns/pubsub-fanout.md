# Pub/Sub Fan-out

> **Purpose**: Distribute a single event to multiple independent consumers for parallel processing
> **MCP Validated**: 2026-02-17

## When to Use

- One event must trigger multiple independent processing pipelines
- Different teams need to react to the same event differently
- Need to add new consumers without modifying the publisher
- Parallel processing of analytics, notifications, and archival

## Architecture

```text
Publisher
    |
    v
Pub/Sub Topic (single)
    |
    +---> Subscription A ---> Cloud Run (data processing)
    |
    +---> Subscription B ---> Cloud Run (analytics/BigQuery)
    |
    +---> Subscription C ---> Cloud Run (notifications/alerts)
    |
    +---> Subscription D ---> Cloud Run (audit/archive)
```

## Implementation

```python
"""Fan-out publisher: single publish, multiple consumers."""
import os
import json
from google.cloud import pubsub_v1
from datetime import datetime

# --- Publisher (shared) ---
publisher = pubsub_v1.PublisherClient()
TOPIC = publisher.topic_path(
    os.environ["GOOGLE_CLOUD_PROJECT"], "pipeline-events"
)


def publish_pipeline_event(
    event_type: str,
    source_bucket: str,
    object_name: str,
    metadata: dict = None,
):
    """Publish a single event consumed by all fan-out subscribers."""
    payload = {
        "event_type": event_type,
        "source": f"gs://{source_bucket}/{object_name}",
        "timestamp": datetime.utcnow().isoformat(),
        "metadata": metadata or {},
    }
    future = publisher.publish(
        TOPIC,
        data=json.dumps(payload).encode("utf-8"),
        event_type=event_type,
        source_bucket=source_bucket,
        object_name=object_name,
    )
    return future.result()


# --- Consumer A: Data Processing ---
def handle_processing(envelope: dict) -> dict:
    """Transform and store processed data."""
    data = _extract_data(envelope)
    # Download, transform, upload to processed bucket
    return {"status": "processed", "source": data["source"]}


# --- Consumer B: Analytics ---
def handle_analytics(envelope: dict) -> dict:
    """Load event metadata into BigQuery for analytics."""
    data = _extract_data(envelope)
    from google.cloud import bigquery
    client = bigquery.Client()
    errors = client.insert_rows_json(
        os.environ["BQ_ANALYTICS_TABLE"],
        [data],
    )
    return {"status": "loaded", "errors": errors}


# --- Consumer C: Notifications ---
def handle_notifications(envelope: dict) -> dict:
    """Send alerts for specific event types."""
    data = _extract_data(envelope)
    if data.get("event_type") == "error":
        send_alert(data)
    return {"status": "notified"}


# --- Consumer D: Archive ---
def handle_archive(envelope: dict) -> dict:
    """Archive raw event to cold storage for compliance."""
    data = _extract_data(envelope)
    from google.cloud import storage
    client = storage.Client()
    blob = client.bucket(os.environ["ARCHIVE_BUCKET"]).blob(
        f"events/{data['timestamp']}/{data['event_type']}.json"
    )
    blob.upload_from_string(json.dumps(data))
    return {"status": "archived"}


def _extract_data(envelope: dict) -> dict:
    """Extract and decode Pub/Sub push message data."""
    import base64
    message = envelope.get("message", {})
    raw = base64.b64decode(message["data"]).decode("utf-8")
    return json.loads(raw)


def send_alert(data: dict):
    """Placeholder for alert integration (Slack, PagerDuty, etc.)."""
    print(f"ALERT: {data['event_type']} from {data['source']}")
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| Topic | `pipeline-events` | Single topic for all events |
| Subscriptions | 1 per consumer | Each gets all messages independently |
| Ack deadline | 600s | Max processing time before redelivery |
| Retry policy | Exponential backoff | 10s min, 600s max |
| Dead-letter topic | Recommended | Capture failed messages after retries |

## Setup Commands

```bash
# Create shared topic
gcloud pubsub topics create pipeline-events

# Create independent subscriptions (each gets ALL messages)
for CONSUMER in processing analytics notifications archive; do
    gcloud pubsub subscriptions create ${CONSUMER}-sub \
        --topic=pipeline-events \
        --push-endpoint=https://${CONSUMER}-service-URL \
        --push-auth-service-account=pubsub-invoker@PROJECT.iam.gserviceaccount.com \
        --ack-deadline=600 \
        --min-retry-delay=10s \
        --max-retry-delay=600s
done

# Optional: Add dead-letter topic
gcloud pubsub topics create pipeline-events-deadletter
gcloud pubsub subscriptions update processing-sub \
    --dead-letter-topic=pipeline-events-deadletter \
    --max-delivery-attempts=5
```

## Message Filtering

```bash
# Filter subscriptions to receive only specific event types
gcloud pubsub subscriptions create error-alerts-sub \
    --topic=pipeline-events \
    --push-endpoint=https://alerts-service-URL \
    --message-filter='attributes.event_type = "error"'
```

## Example Usage

```python
# Publish once, all consumers receive independently
message_id = publish_pipeline_event(
    event_type="file_uploaded",
    source_bucket="raw-data",
    object_name="invoices/batch-42.json",
    metadata={"record_count": 1500, "format": "ndjson"},
)
print(f"Published {message_id} -> 4 consumers will process independently")
```

## See Also

- [Pub/Sub](../concepts/pubsub.md) - Core messaging concepts
- [Event-Driven Pipeline](../patterns/event-driven-pipeline.md) - Single consumer variant
- [Cloud Run Scaling](../patterns/cloud-run-scaling.md) - Scale each consumer independently
