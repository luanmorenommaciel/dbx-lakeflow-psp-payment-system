# Google Cloud Storage (GCS)

> **Purpose**: Object storage service with event notifications for triggering data pipelines
> **Confidence**: 0.95
> **MCP Validated**: 2026-02-17

## Overview

Google Cloud Storage provides durable, highly available object storage for unstructured data.
In serverless data pipelines, GCS serves as the primary landing zone for incoming files and
intermediate processing stages. Event notifications on buckets trigger Pub/Sub messages when
objects are created, updated, deleted, or archived, enabling fully event-driven architectures.

## The Pattern

```python
from google.cloud import storage

client = storage.Client()

def upload_file(bucket_name: str, source_path: str, destination_blob: str):
    """Upload a local file to GCS."""
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)
    blob.upload_from_filename(source_path)
    return f"gs://{bucket_name}/{destination_blob}"

def download_file(bucket_name: str, blob_name: str, dest_path: str):
    """Download a GCS object to local filesystem."""
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.download_to_filename(dest_path)

def list_blobs(bucket_name: str, prefix: str = None):
    """List objects in a bucket with optional prefix filter."""
    blobs = client.list_blobs(bucket_name, prefix=prefix)
    return [blob.name for blob in blobs]

def read_json_blob(bucket_name: str, blob_name: str) -> dict:
    """Read a JSON file directly from GCS into memory."""
    import json
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    content = blob.download_as_text()
    return json.loads(content)
```

## Quick Reference

| Input | Output | Notes |
|-------|--------|-------|
| `blob.upload_from_filename(path)` | Uploaded object | Sets content type automatically |
| `blob.download_as_text()` | String content | For text/JSON files |
| `blob.download_as_bytes()` | Bytes content | For binary files |
| `client.list_blobs(bucket, prefix=)` | Iterator of blobs | Use prefix for directory-like filtering |

## Event Notification Types

| Event | Trigger | Typical Use |
|-------|---------|-------------|
| `OBJECT_FINALIZE` | Object created/overwritten | Start processing pipeline |
| `OBJECT_DELETE` | Object deleted | Cleanup downstream data |
| `OBJECT_ARCHIVE` | Object archived (versioned bucket) | Audit logging |
| `OBJECT_METADATA_UPDATE` | Metadata changed | Reprocessing triggers |

## Setting Up Notifications (gcloud)

```bash
# Create notification: bucket events -> Pub/Sub topic
gcloud storage buckets notifications create gs://my-bucket \
    --topic=my-topic \
    --event-types=OBJECT_FINALIZE \
    --object-prefix=incoming/
```

## Common Mistakes

### Wrong

```python
# Reading entire large file into memory
blob = bucket.blob("large-file.csv")
content = blob.download_as_text()  # OOM for multi-GB files
```

### Correct

```python
# Stream large files or download to disk
blob = bucket.blob("large-file.csv")
blob.download_to_filename("/tmp/large-file.csv")
# Or use chunked download for very large files
```

## Related

- [Pub/Sub](../concepts/pubsub.md) - Receives GCS event notifications
- [BigQuery](../concepts/bigquery.md) - Load processed data from GCS
- [GCS Triggered Workflow](../patterns/gcs-triggered-workflow.md) - Notification setup
- [Multi-Bucket Pipeline](../patterns/multi-bucket-pipeline.md) - Stage-based processing
