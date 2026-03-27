# GCS Triggered Workflow

> **Purpose**: Configure GCS event notifications to trigger Cloud Run processing via Pub/Sub
> **MCP Validated**: 2026-02-17

## When to Use

- Files arriving in GCS must automatically trigger processing
- Need to filter events by prefix, suffix, or event type
- Multiple buckets must feed into a single processing service
- Require at-least-once delivery guarantees for file events

## Architecture

```text
GCS Bucket
    |
    |  Notification Config:
    |  - event: OBJECT_FINALIZE
    |  - prefix: incoming/
    |  - topic: file-events
    |
    v
Pub/Sub Topic (file-events)
    |
    v  Push Subscription
Cloud Run Service
    |
    v
Downstream (BigQuery, another bucket, API)
```

## Implementation

```python
"""Setup and handle GCS-triggered workflows programmatically."""
import os
import json
import base64
from flask import Flask, request, jsonify
from google.cloud import storage, pubsub_v1

app = Flask(__name__)
gcs_client = storage.Client()


# --- Notification Setup (run once) ---
def setup_gcs_notification(
    bucket_name: str,
    topic_name: str,
    project_id: str,
    event_types: list[str] = None,
    prefix: str = None,
    suffix: str = None,
):
    """Create a GCS notification that publishes to Pub/Sub."""
    bucket = gcs_client.bucket(bucket_name)
    topic_path = f"projects/{project_id}/topics/{topic_name}"

    notification = bucket.notification(
        topic_name=topic_path,
        event_types=event_types or ["OBJECT_FINALIZE"],
        blob_name_prefix=prefix,
    )
    notification.create()
    return notification.notification_id


# --- Event Handler ---
@app.route("/", methods=["POST"])
def handle_gcs_event():
    """Process GCS notification delivered via Pub/Sub push."""
    envelope = request.get_json()
    if not envelope or "message" not in envelope:
        return jsonify(error="invalid message"), 400

    message = envelope["message"]
    attributes = message.get("attributes", {})

    event_type = attributes.get("eventType", "")
    bucket_id = attributes.get("bucketId", "")
    object_id = attributes.get("objectId", "")
    generation = attributes.get("objectGeneration", "")

    # Skip non-finalize events
    if event_type != "OBJECT_FINALIZE":
        return jsonify(status="skipped", reason=f"event_type={event_type}"), 200

    # Skip temporary or system files
    if object_id.startswith("_") or object_id.endswith(".tmp"):
        return jsonify(status="skipped", reason="temp file"), 200

    # Idempotency: use generation to avoid reprocessing
    if is_already_processed(bucket_id, object_id, generation):
        return jsonify(status="skipped", reason="already processed"), 200

    # Process the file
    result = process_uploaded_file(bucket_id, object_id)
    mark_as_processed(bucket_id, object_id, generation)

    return jsonify(status="ok", result=result), 200


def is_already_processed(bucket: str, obj: str, generation: str) -> bool:
    """Check idempotency marker (use Firestore/Redis in production)."""
    # Simplified: check if processed marker exists in output bucket
    marker = gcs_client.bucket(f"{bucket}-markers").blob(
        f"{obj}.{generation}.done"
    )
    return marker.exists()


def mark_as_processed(bucket: str, obj: str, generation: str):
    """Set idempotency marker."""
    marker = gcs_client.bucket(f"{bucket}-markers").blob(
        f"{obj}.{generation}.done"
    )
    marker.upload_from_string("done")


def process_uploaded_file(bucket_name: str, object_name: str) -> dict:
    """Download and process the uploaded file."""
    blob = gcs_client.bucket(bucket_name).blob(object_name)
    content = blob.download_as_text()
    line_count = len(content.strip().split("\n"))
    return {"file": object_name, "lines": line_count}


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| Event types | `OBJECT_FINALIZE` | Trigger on file creation/overwrite |
| Prefix filter | None | Only match files with this prefix |
| Max notifications/bucket | 100 | Hard limit per bucket |
| Max notifications/event | 10 | Per event type per bucket |
| Pub/Sub delivery | At-least-once | Implement idempotent handlers |

## Setup Commands (gcloud)

```bash
# Grant GCS permission to publish to Pub/Sub
GCS_SA=$(gcloud storage service-agent --project=PROJECT_ID)
gcloud pubsub topics add-iam-policy-binding file-events \
    --member="serviceAccount:${GCS_SA}" \
    --role="roles/pubsub.publisher"

# Create notification with prefix filter
gcloud storage buckets notifications create gs://my-bucket \
    --topic=file-events \
    --event-types=OBJECT_FINALIZE \
    --object-prefix=incoming/

# Verify notification
gcloud storage buckets notifications list gs://my-bucket
```

## Example Usage

```python
# Setup notification programmatically
notification_id = setup_gcs_notification(
    bucket_name="my-raw-bucket",
    topic_name="file-events",
    project_id="my-project",
    event_types=["OBJECT_FINALIZE"],
    prefix="incoming/",
)
print(f"Created notification: {notification_id}")
```

## See Also

- [GCS](../concepts/gcs.md) - Bucket and blob operations
- [Pub/Sub](../concepts/pubsub.md) - Message delivery details
- [Event-Driven Pipeline](../patterns/event-driven-pipeline.md) - Full pipeline
- [Multi-Bucket Pipeline](../patterns/multi-bucket-pipeline.md) - Multi-stage variant
