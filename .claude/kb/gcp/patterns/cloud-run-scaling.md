# Cloud Run Scaling

> **Purpose**: Configure auto-scaling and concurrency for Cloud Run services in data pipelines
> **MCP Validated**: 2026-02-17

## When to Use

- Service must scale to zero during idle periods (cost savings)
- Need to handle burst traffic from Pub/Sub during batch arrivals
- Must limit parallelism to protect downstream resources (BigQuery, APIs)
- Cold start latency is unacceptable for user-facing endpoints

## Scaling Model

```text
Requests arrive
    |
    v
[Instance 1]  concurrency=N  (handles up to N requests simultaneously)
[Instance 2]  concurrency=N  (auto-created when Instance 1 is full)
[Instance 3]  concurrency=N  (auto-created on demand)
    ...
[Instance M]  max-instances=M (hard cap on total instances)

When idle:
[Instance 1]  min-instances=K (K instances always warm)
[  empty   ]  remaining instances scaled to zero
```

## Implementation

```python
"""Cloud Run service with scaling-aware design patterns."""
import os
import json
import base64
import time
from flask import Flask, request, jsonify
from google.cloud import storage

app = Flask(__name__)

# Initialize expensive clients ONCE at module level (shared across requests)
gcs_client = storage.Client()

# Track concurrent requests for observability
import threading
_active_requests = 0
_lock = threading.Lock()


@app.route("/", methods=["POST"])
def process():
    """Handle request with concurrency awareness."""
    global _active_requests
    with _lock:
        _active_requests += 1

    try:
        envelope = request.get_json()
        message = envelope.get("message", {})
        data = json.loads(base64.b64decode(message["data"]).decode("utf-8"))

        # Heavy processing: file download, transform, upload
        result = process_file(data)
        return jsonify(status="ok", result=result), 200

    except Exception as e:
        return jsonify(status="error", message=str(e)), 500
    finally:
        with _lock:
            _active_requests -= 1


@app.route("/health", methods=["GET"])
def health():
    """Health check with concurrency reporting."""
    return jsonify(
        status="healthy",
        active_requests=_active_requests,
    ), 200


def process_file(data: dict) -> dict:
    """CPU/memory-intensive file processing."""
    bucket = data["bucket"]
    filename = data["filename"]

    blob = gcs_client.bucket(bucket).blob(filename)
    content = blob.download_as_text()
    records = [json.loads(line) for line in content.strip().split("\n")]

    # Simulate heavy processing
    processed = [transform(r) for r in records]

    # Upload result
    output_blob = gcs_client.bucket(f"{bucket}-processed").blob(filename)
    output_blob.upload_from_string(
        "\n".join(json.dumps(r) for r in processed)
    )

    return {"records_processed": len(processed)}


def transform(record: dict) -> dict:
    """Business logic transformation."""
    record["processed"] = True
    return record


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
```

## Configuration

| Setting | Default | Recommended | Description |
|---------|---------|-------------|-------------|
| `--concurrency` | 80 | 1 | Requests per instance (1 for heavy processing) |
| `--min-instances` | 0 | 0-1 | Warm instances (1 to avoid cold starts) |
| `--max-instances` | 100 | 10-50 | Hard cap to protect downstream services |
| `--cpu` | 1 | 1-2 | vCPUs per instance |
| `--memory` | 512Mi | 1Gi-4Gi | RAM per instance |
| `--timeout` | 300 | 900 | Request timeout in seconds |
| `--cpu-boost` | off | on | Extra CPU during startup (reduces cold start) |
| `--cpu-throttling` | on | off | Keep CPU allocated between requests |

## Scaling Profiles

### Profile: Heavy File Processing

```bash
gcloud run deploy heavy-processor \
    --image=gcr.io/PROJECT/processor:latest \
    --concurrency=1 \
    --min-instances=0 \
    --max-instances=20 \
    --cpu=2 \
    --memory=2Gi \
    --timeout=900 \
    --no-cpu-throttling
```

### Profile: API Gateway (Low Latency)

```bash
gcloud run deploy api-gateway \
    --image=gcr.io/PROJECT/api:latest \
    --concurrency=80 \
    --min-instances=2 \
    --max-instances=100 \
    --cpu=1 \
    --memory=512Mi \
    --timeout=60 \
    --cpu-boost
```

### Profile: Batch Event Consumer

```bash
gcloud run deploy batch-consumer \
    --image=gcr.io/PROJECT/consumer:latest \
    --concurrency=10 \
    --min-instances=0 \
    --max-instances=50 \
    --cpu=1 \
    --memory=1Gi \
    --timeout=300
```

## Cold Start Optimization

```text
1. Use --min-instances=1         (keeps one instance warm)
2. Use --cpu-boost               (extra CPU at startup)
3. Initialize clients at import  (not inside request handler)
4. Use slim base images          (python:3.11-slim, not python:3.11)
5. Use gunicorn with preload     (--preload flag)
```

## Example Usage

```bash
# Monitor scaling behavior
gcloud run services describe my-service --format="yaml(status.traffic)"

# View instance count over time
gcloud logging read "resource.type=cloud_run_revision \
    AND resource.labels.service_name=my-service" \
    --format="table(timestamp,jsonPayload.message)" --limit=50
```

## See Also

- [Cloud Run](../concepts/cloud-run.md) - Core configuration
- [Event-Driven Pipeline](../patterns/event-driven-pipeline.md) - Pipeline context
- [Pub/Sub Fan-out](../patterns/pubsub-fanout.md) - Multiple consumer scaling
