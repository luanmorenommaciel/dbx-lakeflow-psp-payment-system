# Error Handling

> **Purpose**: Dead letter queues, retry strategies, and structured error handling for Lambda
> **MCP Validated**: 2026-02-17

## When to Use

- Capturing failed async invocations for later reprocessing
- Implementing retry logic with exponential backoff
- Separating transient errors (retry) from permanent errors (DLQ)
- Building observable error pipelines with CloudWatch alarms

## Implementation

```python
"""Lambda error handling with structured exceptions and DLQ support.

Pattern: Separate transient from permanent errors.
- Transient (network, throttle): Raise to trigger Lambda retry.
- Permanent (bad data, schema): Log, send to DLQ, do NOT retry.
"""
import json
import os
from typing import Any

import boto3
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext
from botocore.exceptions import ClientError

logger = Logger()

s3_client = boto3.client("s3")
sqs_client = boto3.client("sqs")
DLQ_URL = os.environ.get("DLQ_URL", "")


class TransientError(Exception):
    """Retryable error: network issues, throttling, timeouts."""
    pass


class PermanentError(Exception):
    """Non-retryable error: bad data, missing fields, schema violation."""
    pass


@logger.inject_lambda_context
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    """Process event with error classification."""
    results: list[dict[str, Any]] = []

    for record in event.get("Records", []):
        try:
            result = process_record(record)
            results.append({"status": "success", "result": result})

        except TransientError as e:
            logger.warning("Transient error, will retry", extra={
                "error": str(e),
                "record": record,
            })
            raise  # Re-raise: Lambda retries (up to 2 for async)

        except PermanentError as e:
            logger.error("Permanent error, sending to DLQ", extra={
                "error": str(e),
                "record": record,
            })
            send_to_dlq(record, str(e))
            results.append({"status": "failed", "error": str(e)})

        except Exception as e:
            logger.exception("Unexpected error")
            raise  # Unknown errors should retry

    return {"results": results}


def process_record(record: dict) -> dict:
    """Process a single record with error classification."""
    try:
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
    except (KeyError, TypeError) as e:
        raise PermanentError(f"Invalid record structure: {e}") from e

    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        body = response["Body"].read()
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("NoSuchKey", "NoSuchBucket"):
            raise PermanentError(f"Object not found: s3://{bucket}/{key}") from e
        if code in ("SlowDown", "ServiceUnavailable"):
            raise TransientError(f"S3 throttled: {code}") from e
        raise TransientError(f"S3 client error: {code}") from e

    return validate_and_transform(body, key)


def send_to_dlq(record: dict, error_message: str) -> None:
    """Send failed record to Dead Letter Queue."""
    if not DLQ_URL:
        logger.warning("DLQ_URL not configured, skipping DLQ send")
        return

    message = {
        "original_record": record,
        "error_message": error_message,
    }
    sqs_client.send_message(
        QueueUrl=DLQ_URL,
        MessageBody=json.dumps(message, default=str),
    )
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `DLQ_URL` | (required) | SQS queue URL for failed records |
| `MaximumRetryAttempts` | `2` | Lambda async retry count (0-2) |
| `MaximumEventAgeInSeconds` | `21600` | Max event age before discard (6h) |

## SAM Template with DLQ

```yaml
Resources:
  DeadLetterQueue:
    Type: AWS::SQS::Queue
    Properties:
      QueueName: !Sub "${AWS::StackName}-dlq"
      MessageRetentionPeriod: 1209600  # 14 days

  ProcessorFunction:
    Type: AWS::Serverless::Function
    Properties:
      Handler: app.lambda_handler
      DeadLetterQueue:
        Type: SQS
        TargetArn: !GetAtt DeadLetterQueue.Arn
      EventInvokeConfig:
        MaximumRetryAttempts: 2
        MaximumEventAgeInSeconds: 3600
        DestinationConfig:
          OnFailure:
            Type: SQS
            Destination: !GetAtt DeadLetterQueue.Arn
      Policies:
        - SQSSendMessagePolicy:
            QueueName: !GetAtt DeadLetterQueue.QueueName
```

## Error Classification Reference

| Error Type | Action | Lambda Behavior |
|------------|--------|-----------------|
| `TransientError` | Raise | Lambda retries (0-2 times) |
| `PermanentError` | Send to DLQ, continue | No retry, event captured |
| `ClientError(NoSuchKey)` | Permanent | Object gone, no point retrying |
| `ClientError(SlowDown)` | Transient | S3 throttle, retry helps |
| Unhandled `Exception` | Raise | Retry, then DLQ on exhaust |

## See Also

- [File Processing Pipeline](../patterns/file-processing.md)
- [Powertools Logging](../patterns/powertools-logging.md)
- [IAM Policies](../concepts/iam-policies.md)
