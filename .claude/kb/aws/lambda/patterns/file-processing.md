# File Processing Pipeline

> **Purpose**: End-to-end S3 file processing pattern with Lambda, from trigger to output
> **MCP Validated**: 2026-02-17

## When to Use

- Processing CSV/JSON files uploaded to S3
- Transforming raw data into Parquet for analytics
- Event-driven ETL pipelines triggered by file uploads
- File validation and enrichment workflows

## Implementation

```python
"""S3 file processing Lambda handler.

Triggered by S3 ObjectCreated events on input bucket.
Reads CSV, transforms data, writes Parquet to output bucket.
Uses Powertools for structured logging and error handling.
"""
import os
import io
import urllib.parse

import boto3
import pandas as pd
from aws_lambda_powertools import Logger, Metrics
from aws_lambda_powertools.metrics import MetricUnit
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.utilities.data_classes import S3Event

logger = Logger()
metrics = Metrics()

s3_client = boto3.client("s3")
OUTPUT_BUCKET = os.environ["OUTPUT_BUCKET"]
OUTPUT_PREFIX = os.environ.get("OUTPUT_PREFIX", "processed/")


@logger.inject_lambda_context(log_event=True)
@metrics.log_metrics(capture_cold_start_metric=True)
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    """Process S3 file upload events."""
    s3_event = S3Event(event)
    processed = 0

    for record in s3_event.records:
        bucket = record.s3.get_bucket.name
        key = urllib.parse.unquote_plus(
            record.s3.get_object.key, encoding="utf-8"
        )

        logger.info("Processing file", extra={"bucket": bucket, "key": key})

        try:
            df = read_input(bucket, key)
            df = transform(df)
            output_key = build_output_key(key)
            write_parquet(df, OUTPUT_BUCKET, output_key)
            processed += 1

            metrics.add_metric(
                name="FilesProcessed", unit=MetricUnit.Count, value=1
            )
        except Exception:
            logger.exception("Failed to process file", extra={"key": key})
            metrics.add_metric(
                name="FilesFailed", unit=MetricUnit.Count, value=1
            )
            raise  # Re-raise for DLQ capture

    return {"processed": processed}


def read_input(bucket: str, key: str) -> pd.DataFrame:
    """Read CSV file from S3 into DataFrame."""
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(response["Body"].read()))


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Apply business transformations to DataFrame."""
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df = df.dropna(how="all")
    df["processed_at"] = pd.Timestamp.utcnow()
    return df


def build_output_key(input_key: str) -> str:
    """Convert input key to output Parquet key."""
    filename = os.path.splitext(os.path.basename(input_key))[0]
    return f"{OUTPUT_PREFIX}{filename}.parquet"


def write_parquet(df: pd.DataFrame, bucket: str, key: str) -> None:
    """Write DataFrame as Parquet to S3."""
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    s3_client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    logger.info("Wrote parquet", extra={"bucket": bucket, "key": key})
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `OUTPUT_BUCKET` | (required) | Target S3 bucket for processed files |
| `OUTPUT_PREFIX` | `processed/` | Key prefix for output files |
| `LOG_LEVEL` | `INFO` | Powertools log level |
| `POWERTOOLS_SERVICE_NAME` | (required) | Service name for structured logs |
| `MemorySize` | `512` | Min 512 MB for pandas layer |
| `Timeout` | `300` | 5 min for typical file processing |

## Example Usage

```yaml
# SAM template for this pipeline
Resources:
  InputBucket:
    Type: AWS::S3::Bucket

  OutputBucket:
    Type: AWS::S3::Bucket

  ProcessorFunction:
    Type: AWS::Serverless::Function
    Properties:
      CodeUri: src/
      Handler: app.lambda_handler
      Runtime: python3.12
      Timeout: 300
      MemorySize: 512
      Environment:
        Variables:
          OUTPUT_BUCKET: !Ref OutputBucket
          OUTPUT_PREFIX: processed/
          POWERTOOLS_SERVICE_NAME: file-processor
      Layers:
        - !Sub arn:aws:lambda:${AWS::Region}:336392948345:layer:AWSSDKPandas-Python312:15
        - !Sub arn:aws:lambda:${AWS::Region}:017000801446:layer:AWSLambdaPowertoolsPythonV3-python312-x86_64:7
      Policies:
        - S3ReadPolicy:
            BucketName: !Ref InputBucket
        - S3CrudPolicy:
            BucketName: !Ref OutputBucket
      Events:
        CsvUpload:
          Type: S3
          Properties:
            Bucket: !Ref InputBucket
            Events: s3:ObjectCreated:*
            Filter:
              S3Key:
                Rules:
                  - Name: suffix
                    Value: .csv
```

## See Also

- [Parquet Output](../patterns/parquet-output.md)
- [Error Handling](../patterns/error-handling.md)
- [Lambda Handler](../concepts/lambda-handler.md)
- [S3 Triggers](../concepts/s3-triggers.md)
