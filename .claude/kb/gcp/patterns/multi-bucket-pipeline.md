# Multi-Bucket Pipeline

> **Purpose**: Stage-based processing pipeline using multiple GCS buckets for data lifecycle management
> **MCP Validated**: 2026-02-17

## When to Use

- Data must pass through distinct processing stages (raw -> validated -> enriched -> loaded)
- Need clear separation between pipeline stages for debugging and reprocessing
- Compliance requires retaining intermediate artifacts
- Multiple teams consume data at different stages

## Architecture

```text
gs://raw-data/          (landing zone)
    |
    v  (Cloud Run: validate)
gs://validated-data/    (schema-checked)
    |
    v  (Cloud Run: enrich)
gs://enriched-data/     (business logic applied)
    |
    v  (Cloud Run: load)
BigQuery                (analytics-ready)
```

## Implementation

```python
"""Multi-bucket pipeline with stage progression."""
import os
import json
import tempfile
from flask import Flask, request, jsonify
from google.cloud import storage, bigquery
from datetime import datetime

app = Flask(__name__)
gcs = storage.Client()
bq = bigquery.Client()

STAGES = {
    "raw": {
        "next_bucket": os.environ.get("VALIDATED_BUCKET", "validated-data"),
        "handler": "validate",
    },
    "validated": {
        "next_bucket": os.environ.get("ENRICHED_BUCKET", "enriched-data"),
        "handler": "enrich",
    },
    "enriched": {
        "next_bucket": None,  # Terminal: load to BigQuery
        "handler": "load_to_bq",
    },
}


@app.route("/", methods=["POST"])
def handle_event():
    """Route event to correct stage handler based on source bucket."""
    envelope = request.get_json()
    message = envelope.get("message", {})
    attributes = message.get("attributes", {})

    if attributes.get("eventType") != "OBJECT_FINALIZE":
        return jsonify(status="skipped"), 200

    bucket_name = attributes["bucketId"]
    object_name = attributes["objectId"]

    # Determine stage from bucket name
    stage = identify_stage(bucket_name)
    if stage not in STAGES:
        return jsonify(error=f"unknown stage for bucket {bucket_name}"), 400

    config = STAGES[stage]
    handler = globals()[config["handler"]]

    # Download source file
    blob = gcs.bucket(bucket_name).blob(object_name)
    content = blob.download_as_text()
    records = [json.loads(line) for line in content.strip().split("\n") if line]

    # Process
    processed = handler(records, object_name)

    # Move to next stage or load to BigQuery
    if config["next_bucket"]:
        output_blob = gcs.bucket(config["next_bucket"]).blob(object_name)
        output_blob.upload_from_string(
            "\n".join(json.dumps(r) for r in processed),
            content_type="application/json",
        )
        return jsonify(status="ok", stage=stage, next=config["next_bucket"]), 200
    else:
        return jsonify(status="ok", stage=stage, destination="bigquery"), 200


def identify_stage(bucket_name: str) -> str:
    """Map bucket name to pipeline stage."""
    for stage in STAGES:
        if stage in bucket_name:
            return stage
    return "unknown"


def validate(records: list[dict], filename: str) -> list[dict]:
    """Stage 1: Validate schema and data quality."""
    valid = []
    for record in records:
        if all(k in record for k in ["id", "timestamp", "payload"]):
            record["_validated_at"] = datetime.utcnow().isoformat()
            valid.append(record)
    return valid


def enrich(records: list[dict], filename: str) -> list[dict]:
    """Stage 2: Enrich records with derived fields."""
    for record in records:
        record["_enriched_at"] = datetime.utcnow().isoformat()
        record["_source_file"] = filename
    return records


def load_to_bq(records: list[dict], filename: str) -> list[dict]:
    """Stage 3: Load enriched records to BigQuery."""
    table_id = os.environ["BQ_TABLE"]
    errors = bq.insert_rows_json(table_id, records)
    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")
    return records


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `VALIDATED_BUCKET` | `validated-data` | Stage 2 bucket name |
| `ENRICHED_BUCKET` | `enriched-data` | Stage 3 bucket name |
| `BQ_TABLE` | (required) | Final BigQuery destination |
| Notifications per bucket | 1 topic each | Each bucket triggers same service |

## Setup Commands

```bash
# Create stage buckets
for BUCKET in raw-data validated-data enriched-data; do
    gcloud storage buckets create gs://${PROJECT_ID}-${BUCKET} --location=us-central1
done

# Create shared topic and notification per bucket
gcloud pubsub topics create pipeline-events
for BUCKET in raw-data validated-data enriched-data; do
    gcloud storage buckets notifications create \
        gs://${PROJECT_ID}-${BUCKET} \
        --topic=pipeline-events \
        --event-types=OBJECT_FINALIZE
done
```

## Example Usage

```python
# Trigger pipeline by uploading a file to raw bucket
from google.cloud import storage
client = storage.Client()
bucket = client.bucket(f"{PROJECT_ID}-raw-data")
blob = bucket.blob("incoming/batch-001.json")
blob.upload_from_string(
    '{"id":"1","timestamp":"2025-01-15T10:00:00Z","payload":{"amount":99.99}}\n'
    '{"id":"2","timestamp":"2025-01-15T10:01:00Z","payload":{"amount":150.00}}\n'
)
# Pipeline automatically: raw -> validated -> enriched -> BigQuery
```

## See Also

- [Event-Driven Pipeline](../patterns/event-driven-pipeline.md) - Single-stage variant
- [GCS Triggered Workflow](../patterns/gcs-triggered-workflow.md) - Notification setup
- [GCS](../concepts/gcs.md) - Bucket operations
- [BigQuery](../concepts/bigquery.md) - Loading data
