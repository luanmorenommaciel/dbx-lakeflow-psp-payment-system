# Powertools Logging

> **Purpose**: AWS Lambda Powertools structured JSON logging with context enrichment
> **MCP Validated**: 2026-02-17

## When to Use

- Structured JSON logs for CloudWatch Logs Insights queries
- Automatic Lambda context injection (request ID, cold start, memory)
- Correlation IDs across distributed services
- Log sampling for high-throughput functions

## Implementation

```python
"""Structured logging with AWS Lambda Powertools.

Powertools Logger outputs JSON to CloudWatch, includes Lambda context
automatically, and supports correlation IDs for distributed tracing.
"""
import os
from aws_lambda_powertools import Logger, Tracer, Metrics
from aws_lambda_powertools.metrics import MetricUnit
from aws_lambda_powertools.utilities.typing import LambdaContext

# Initialize outside handler for reuse across invocations
logger = Logger(
    service=os.environ.get("POWERTOOLS_SERVICE_NAME", "my-service"),
    log_uncaught_exceptions=True,
)
tracer = Tracer()
metrics = Metrics()


@logger.inject_lambda_context(log_event=True)
@tracer.capture_lambda_handler
@metrics.log_metrics(capture_cold_start_metric=True)
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    """Handler with full Powertools observability."""

    # Structured log with extra fields
    logger.info("Processing started", extra={
        "record_count": len(event.get("Records", [])),
        "source": "s3",
    })

    # Append persistent keys for all subsequent logs in this invocation
    logger.append_keys(
        environment=os.environ.get("ENVIRONMENT", "dev"),
        pipeline="file-processor",
    )

    processed = 0
    for record in event.get("Records", []):
        try:
            process_record(record)
            processed += 1
        except Exception:
            logger.exception("Record processing failed", extra={
                "record": record,
            })
            raise

    metrics.add_metric(
        name="RecordsProcessed", unit=MetricUnit.Count, value=processed
    )

    logger.info("Processing complete", extra={"processed": processed})
    return {"processed": processed}


@tracer.capture_method
def process_record(record: dict) -> None:
    """Process a single record with tracing."""
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]

    logger.info("Processing file", extra={"bucket": bucket, "key": key})
    # ... business logic here
```

## Log Output Format

```json
{
  "level": "INFO",
  "location": "lambda_handler:35",
  "message": "Processing started",
  "timestamp": "2026-02-17T10:00:00.000Z",
  "service": "file-processor",
  "cold_start": true,
  "function_name": "my-stack-ProcessorFunction",
  "function_memory_size": 512,
  "function_arn": "arn:aws:lambda:us-east-1:123456789:function:...",
  "function_request_id": "abc-123-def",
  "record_count": 1,
  "source": "s3",
  "xray_trace_id": "1-abc-def"
}
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `POWERTOOLS_SERVICE_NAME` | `"service_undefined"` | Service name in every log |
| `POWERTOOLS_LOG_LEVEL` | `"INFO"` | Minimum log level |
| `POWERTOOLS_LOGGER_SAMPLE_RATE` | `0` | % of invocations logged at DEBUG (0.0-1.0) |
| `POWERTOOLS_LOGGER_LOG_EVENT` | `false` | Log entire event on every invocation |

## SAM Environment Variables

```yaml
Globals:
  Function:
    Environment:
      Variables:
        POWERTOOLS_SERVICE_NAME: file-processor
        POWERTOOLS_LOG_LEVEL: INFO
        POWERTOOLS_LOGGER_SAMPLE_RATE: 0.1  # 10% DEBUG sampling
```

## CloudWatch Logs Insights Queries

```sql
-- Find all errors in the last hour
fields @timestamp, service, message, error
| filter level = "ERROR"
| sort @timestamp desc
| limit 50

-- Cold start analysis
fields @timestamp, function_name, cold_start, @duration
| filter cold_start = true
| stats avg(@duration) as avg_cold_start by function_name

-- Correlation ID tracking
fields @timestamp, message, correlation_id
| filter correlation_id = "abc-123"
| sort @timestamp asc
```

## Example Usage

```python
# Correlation IDs for cross-service tracing
logger.append_keys(correlation_id=event.get("correlation_id", context.aws_request_id))

# Child logger for modules
child_logger = Logger(child=True)  # Inherits parent config

# Log sampling: 10% of invocations log at DEBUG
logger = Logger(sampling_rate=0.1)

# Custom log level per invocation
logger.setLevel("DEBUG")  # Override for this invocation only
```

## Powertools v2.43+ New Features

Powertools for AWS Lambda Python v2.43+ (supporting Python 3.14) includes:

| Feature | Description |
|---------|-------------|
| **BatchProcessor** | Process SQS, Kinesis, DynamoDB Streams with partial failure handling |
| **Event Handler** | APIGatewayRestResolver, APIGatewayHttpResolver, ALBResolver |
| **Idempotency** | DynamoDB-backed idempotency for exactly-once processing |
| **Feature Flags** | AppConfig-backed feature toggles |
| **Parameters** | SSM, Secrets Manager, AppConfig, DynamoDB parameter fetching |
| **Streaming** | S3 object streaming for large file processing |

```python
# BatchProcessor for SQS with Powertools v2.43+
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor, EventType, process_partial_response
)
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.typing import LambdaContext

processor = BatchProcessor(event_type=EventType.SQS)
logger = Logger()
tracer = Tracer()

@tracer.capture_method
def record_handler(record: SQSRecord):
    payload = record.json_body
    logger.info("Processing record", extra={"payload": payload})
    # ... business logic

@logger.inject_lambda_context
@tracer.capture_lambda_handler
def lambda_handler(event: dict, context: LambdaContext):
    return process_partial_response(
        event=event, record_handler=record_handler,
        processor=processor, context=context
    )
```

## See Also

- [Lambda Handler](../concepts/lambda-handler.md)
- [Error Handling](../patterns/error-handling.md)
- [Layers](../concepts/layers.md)
- [File Processing Pipeline](../patterns/file-processing.md)
