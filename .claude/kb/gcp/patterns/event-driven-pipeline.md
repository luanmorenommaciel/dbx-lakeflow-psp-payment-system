# Event-Driven Pipeline

> **Purpose**: End-to-end serverless pipeline: GCS file upload -> Pub/Sub notification -> Cloud Run processing -> BigQuery
> **MCP Validated**: 2026-02-17

## When to Use

- File lands in GCS and must be processed automatically
- Pipeline must scale to zero when idle (cost optimization)
- Processing steps must be decoupled and independently scalable
- Need audit trail via Pub/Sub message attributes

## Architecture

```text
GCS Bucket (raw)
    |
    v  (OBJECT_FINALIZE notification)
Pub/Sub Topic
    |
    v  (push subscription)
Cloud Run Service
    |
    +---> GCS Bucket (processed)
    |
    +---> BigQuery (analytics)
```

## Implementation

```python
"""Cloud Run service: event-driven file processing pipeline."""
import os
import json
import base64
import tempfile
from flask import Flask, request, jsonify
from google.cloud import storage, bigquery

app = Flask(__name__)
gcs_client = storage.Client()
bq_client = bigquery.Client()

PROJECT_ID = os.environ["GOOGLE_CLOUD_PROJECT"]
PROCESSED_BUCKET = os.environ["PROCESSED_BUCKET"]
BQ_TABLE = os.environ["BQ_TABLE"]  # "project.dataset.table"


@app.route("/", methods=["POST"])
def process_event():
    """Handle Pub/Sub push message from GCS notification."""
    envelope = request.get_json()
    if not envelope or "message" not in envelope:
        return jsonify(error="invalid message"), 400

    message = envelope["message"]
    attributes = message.get("attributes", {})
    event_type = attributes.get("eventType")

    if event_type != "OBJECT_FINALIZE":
        return jsonify(status="skipped"), 200

    bucket_name = attributes["bucketId"]
    object_name = attributes["objectId"]

    # Step 1: Download from source bucket
    source_bucket = gcs_client.bucket(bucket_name)
    blob = source_bucket.blob(object_name)

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
        blob.download_to_filename(tmp.name)
        tmp_path = tmp.name

    # Step 2: Process the file
    with open(tmp_path, "r") as f:
        records = [json.loads(line) for line in f if line.strip()]

    processed = [transform_record(r) for r in records]

    # Step 3: Upload processed file to output bucket
    output_blob = gcs_client.bucket(PROCESSED_BUCKET).blob(
        f"processed/{object_name}"
    )
    output_blob.upload_from_string(
        "\n".join(json.dumps(r) for r in processed),
        content_type="application/json",
    )

    # Step 4: Load into BigQuery
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True,
    )
    uri = f"gs://{PROCESSED_BUCKET}/processed/{object_name}"
    load_job = bq_client.load_table_from_uri(uri, BQ_TABLE, job_config=job_config)
    load_job.result()

    return jsonify(status="ok", records=len(processed)), 200


def transform_record(record: dict) -> dict:
    """Apply business logic transformation to a record."""
    record["processed"] = True
    record["pipeline_version"] = "1.0"
    return record


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `GOOGLE_CLOUD_PROJECT` | (required) | GCP project ID |
| `PROCESSED_BUCKET` | (required) | Output bucket for processed files |
| `BQ_TABLE` | (required) | BigQuery table ID `project.dataset.table` |
| `--concurrency` | 1 | One file at a time per instance |
| `--timeout` | 900 | Seconds for large file processing |
| `--memory` | 1Gi | Increase for large files |

## Setup Commands

```bash
# 1. Create Pub/Sub topic and push subscription
gcloud pubsub topics create file-events
gcloud pubsub subscriptions create file-events-push \
    --topic=file-events \
    --push-endpoint=https://SERVICE_URL \
    --push-auth-service-account=pubsub-invoker@PROJECT.iam.gserviceaccount.com

# 2. Configure GCS notification
gcloud storage buckets notifications create gs://raw-data-bucket \
    --topic=file-events \
    --event-types=OBJECT_FINALIZE

# 3. Deploy Cloud Run
gcloud run deploy pipeline-service \
    --image=gcr.io/PROJECT/pipeline:latest \
    --set-env-vars="PROCESSED_BUCKET=processed-bucket,BQ_TABLE=project.dataset.table" \
    --service-account=pipeline-sa@PROJECT.iam.gserviceaccount.com \
    --concurrency=1 --timeout=900 --memory=1Gi
```

## Example Usage

```python
# Test locally by simulating a Pub/Sub push message
import requests
test_message = {
    "message": {
        "attributes": {
            "eventType": "OBJECT_FINALIZE",
            "bucketId": "raw-data-bucket",
            "objectId": "incoming/data-2025-01-15.json"
        },
        "data": base64.b64encode(b"{}").decode()
    }
}
requests.post("http://localhost:8080/", json=test_message)
```

## See Also

- [Cloud Run](../concepts/cloud-run.md) - Container configuration
- [Pub/Sub](../concepts/pubsub.md) - Message delivery guarantees
- [GCS](../concepts/gcs.md) - Notification setup
- [BigQuery](../concepts/bigquery.md) - Load job configuration
- [GCS Triggered Workflow](../patterns/gcs-triggered-workflow.md) - Notification details
