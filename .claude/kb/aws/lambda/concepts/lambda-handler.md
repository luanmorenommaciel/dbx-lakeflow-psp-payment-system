# Lambda Handler

> **Purpose**: Python handler function patterns, initialization, and execution best practices
> **Confidence**: 0.95
> **MCP Validated**: 2026-02-17

## Overview

The Lambda handler is the entry point function that AWS invokes when the function is triggered.
It receives two arguments: `event` (the trigger payload) and `context` (runtime metadata).
Code outside the handler runs once per cold start and persists across warm invocations,
making it the correct place for SDK clients, DB connections, and configuration loading.

## The Pattern

```python
import os
import logging
import boto3

# --- COLD START: runs once, reused across invocations ---
logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

s3_client = boto3.client("s3")
OUTPUT_BUCKET = os.environ["OUTPUT_BUCKET"]


def lambda_handler(event: dict, context) -> dict:
    """Process incoming event.

    Args:
        event: Trigger payload (S3 event, API Gateway, etc.)
        context: LambdaContext with request_id, memory, time remaining

    Returns:
        dict with statusCode and body for API Gateway,
        or any JSON-serializable value for async invocations.
    """
    request_id = context.aws_request_id
    remaining_ms = context.get_remaining_time_in_millis()

    logger.info("Processing request %s, %d ms remaining", request_id, remaining_ms)

    try:
        result = process(event)
        return {"statusCode": 200, "body": result}
    except Exception:
        logger.exception("Failed to process event")
        raise  # Let Lambda retry or send to DLQ
```

## Quick Reference

| Context Attribute | Type | Description |
|-------------------|------|-------------|
| `context.aws_request_id` | str | Unique invocation ID |
| `context.function_name` | str | Lambda function name |
| `context.memory_limit_in_mb` | int | Configured memory |
| `context.get_remaining_time_in_millis()` | int | Time before timeout |
| `context.log_group_name` | str | CloudWatch log group |
| `context.log_stream_name` | str | CloudWatch log stream |

## Common Mistakes

### Wrong (initialize inside handler)

```python
def lambda_handler(event, context):
    s3 = boto3.client("s3")  # Created on EVERY invocation
    bucket = os.environ["BUCKET"]  # Read on EVERY invocation
    result = s3.get_object(Bucket=bucket, Key=event["key"])
    return result
```

### Correct (initialize outside handler)

```python
import boto3
import os

s3 = boto3.client("s3")  # Created once, reused
BUCKET = os.environ["BUCKET"]  # Read once, reused


def lambda_handler(event, context):
    result = s3.get_object(Bucket=BUCKET, Key=event["key"])
    return result
```

## Type Hints for Handler

```python
from typing import Any

# For S3 events
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.utilities.data_classes import S3Event


def lambda_handler(event: dict, context: LambdaContext) -> dict[str, Any]:
    s3_event = S3Event(event)
    for record in s3_event.records:
        bucket = record.s3.get_bucket.name
        key = record.s3.get_object.key
    return {"statusCode": 200}
```

## Keep-Alive for Connections

```python
from botocore.config import Config

# Persistent connections reduce latency on warm starts
config = Config(
    connect_timeout=5,
    read_timeout=10,
    retries={"max_attempts": 3, "mode": "adaptive"},
)

s3_client = boto3.client("s3", config=config)
```

## Python 3.14 Features in Handlers

Python 3.14 (GA on Lambda Nov 2025) brings:

- **Template strings** (`t-strings`): `t"Hello {name}"` returns a `Template` object for custom processing
- **Deferred type annotations** (PEP 649): Annotations evaluated lazily, reducing import overhead
- **Powertools v2.43+**: Full Python 3.14 support for Logger, Tracer, Metrics, and BatchProcessor

```python
# Python 3.14 handler with Powertools typing
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.utilities.data_classes import S3Event

def lambda_handler(event: dict, context: LambdaContext) -> dict[str, str]:
    s3_event = S3Event(event)
    for record in s3_event.records:
        bucket: str = record.s3.get_bucket.name  # deferred annotation
        key: str = record.s3.get_object.key
    return {"statusCode": "200"}
```

## Related

- [SAM Templates](../concepts/sam-templates.md)
- [S3 Triggers](../concepts/s3-triggers.md)
- [Powertools Logging](../patterns/powertools-logging.md)
- [Error Handling](../patterns/error-handling.md)
