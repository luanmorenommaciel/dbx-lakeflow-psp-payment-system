# S3 Triggers

> **Purpose**: S3 event notifications that invoke Lambda functions on object operations
> **Confidence**: 0.95
> **MCP Validated**: 2026-02-17

## Overview

S3 event notifications invoke Lambda functions asynchronously when objects are created,
modified, or deleted. The S3 event payload contains bucket name, object key, size, and
metadata. S3 triggers are configured either through SAM templates, CloudFormation, or
directly on the S3 bucket. Events are delivered at least once (duplicates possible).

## The Pattern

```python
import urllib.parse
import boto3

s3_client = boto3.client("s3")


def lambda_handler(event: dict, context) -> None:
    """Process S3 event notification.

    S3 events can contain multiple records (batch).
    Each record has bucket name and object key.
    Keys are URL-encoded and must be decoded.
    """
    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(
            record["s3"]["object"]["key"], encoding="utf-8"
        )
        size = record["s3"]["object"]["size"]
        event_name = record["eventName"]  # e.g. ObjectCreated:Put

        print(f"Processing s3://{bucket}/{key} ({size} bytes) [{event_name}]")

        response = s3_client.get_object(Bucket=bucket, Key=key)
        body = response["Body"].read()
        process_file(body, key)
```

## S3 Event Structure

```json
{
  "Records": [
    {
      "eventVersion": "2.1",
      "eventSource": "aws:s3",
      "eventName": "ObjectCreated:Put",
      "eventTime": "2026-02-17T10:00:00.000Z",
      "s3": {
        "bucket": {
          "name": "my-input-bucket",
          "arn": "arn:aws:s3:::my-input-bucket"
        },
        "object": {
          "key": "raw/data+file%282%29.csv",
          "size": 1024,
          "eTag": "abc123"
        }
      }
    }
  ]
}
```

## Quick Reference

| Event Type | When Triggered |
|------------|----------------|
| `s3:ObjectCreated:*` | Any create (Put, Post, Copy, Multipart) |
| `s3:ObjectCreated:Put` | Direct PUT upload |
| `s3:ObjectCreated:CompleteMultipartUpload` | Multipart done |
| `s3:ObjectRemoved:*` | Any delete |
| `s3:ObjectRestore:Completed` | Glacier restore done |

## Common Mistakes

### Wrong (not decoding URL-encoded keys)

```python
def lambda_handler(event, context):
    key = event["Records"][0]["s3"]["object"]["key"]
    # FAILS for keys with spaces: "my+file%282%29.csv"
    s3_client.get_object(Bucket=bucket, Key=key)
```

### Correct (decode URL-encoded keys)

```python
import urllib.parse

def lambda_handler(event, context):
    key = urllib.parse.unquote_plus(
        event["Records"][0]["s3"]["object"]["key"],
        encoding="utf-8"
    )
    # Correctly handles: "my file(2).csv"
    s3_client.get_object(Bucket=bucket, Key=key)
```

## SAM Configuration

```yaml
Resources:
  InputBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub "${AWS::StackName}-input"

  ProcessorFunction:
    Type: AWS::Serverless::Function
    Properties:
      Handler: app.lambda_handler
      Events:
        CsvUpload:
          Type: S3
          Properties:
            Bucket: !Ref InputBucket
            Events: s3:ObjectCreated:*
            Filter:
              S3Key:
                Rules:
                  - Name: prefix
                    Value: raw/
                  - Name: suffix
                    Value: .csv
```

## Avoiding Infinite Loops

Use separate input and output buckets, or use non-overlapping prefixes
to prevent a Lambda writing to the same bucket from re-triggering itself.

```text
input-bucket/raw/*.csv  --> Lambda --> output-bucket/processed/*.parquet
```

## Related

- [Lambda Handler](../concepts/lambda-handler.md)
- [SAM Templates](../concepts/sam-templates.md)
- [File Processing Pipeline](../patterns/file-processing.md)
