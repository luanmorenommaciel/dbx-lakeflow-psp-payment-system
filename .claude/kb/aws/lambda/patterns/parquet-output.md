# Parquet Output

> **Purpose**: Write Parquet files to S3 from Lambda using pyarrow or pandas
> **MCP Validated**: 2026-02-17

## When to Use

- Converting CSV/JSON data to columnar Parquet for analytics
- Writing query-optimized output for Athena, Redshift Spectrum, or Spark
- Reducing storage costs with columnar compression
- Building data lake ingestion pipelines

## Implementation

```python
"""Write Parquet files to S3 from Lambda.

Two approaches:
1. pandas + pyarrow (via AWS SDK for pandas layer) - simpler API
2. pyarrow only - lighter, faster cold start

Both require the AWS SDK for pandas managed layer or a custom layer.
"""
import io
import os
from datetime import datetime, timezone

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from aws_lambda_powertools import Logger

logger = Logger()

s3_client = boto3.client("s3")
OUTPUT_BUCKET = os.environ["OUTPUT_BUCKET"]


# --- Approach 1: pandas DataFrame to Parquet ---

def write_parquet_pandas(df, bucket: str, key: str) -> None:
    """Write pandas DataFrame as Parquet to S3.

    Uses pandas .to_parquet() with pyarrow engine.
    Best for: DataFrames already in memory, simple transformations.
    """
    import pandas as pd

    buffer = io.BytesIO()
    df.to_parquet(
        buffer,
        index=False,
        engine="pyarrow",
        compression="snappy",  # Good balance of speed/compression
    )
    buffer.seek(0)

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="application/x-parquet",
    )
    logger.info("Wrote parquet (pandas)", extra={
        "bucket": bucket, "key": key, "size_bytes": buffer.tell()
    })


# --- Approach 2: pyarrow Table to Parquet (no pandas) ---

def write_parquet_pyarrow(records: list[dict], bucket: str, key: str) -> None:
    """Write list of dicts as Parquet to S3 using pyarrow only.

    Avoids pandas import entirely for faster cold starts.
    Best for: High-throughput, structured records, minimal transformation.
    """
    schema = pa.schema([
        pa.field("id", pa.string()),
        pa.field("value", pa.float64()),
        pa.field("timestamp", pa.timestamp("ms")),
        pa.field("category", pa.string()),
    ])

    # Build columnar arrays from row-oriented dicts
    arrays = [
        pa.array([r.get(field.name) for r in records], type=field.type)
        for field in schema
    ]
    table = pa.table(dict(zip(schema.names, arrays)), schema=schema)

    buffer = io.BytesIO()
    pq.write_table(
        table,
        buffer,
        compression="snappy",
        use_dictionary=True,
        write_statistics=True,
    )
    buffer.seek(0)

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="application/x-parquet",
    )
    logger.info("Wrote parquet (pyarrow)", extra={
        "bucket": bucket, "key": key, "rows": len(records)
    })


# --- Approach 3: AWS SDK for pandas (awswrangler) ---

def write_parquet_wrangler(df, bucket: str, key: str) -> None:
    """Write Parquet using awswrangler for advanced features.

    Supports: partitioning, Glue catalog registration, schema evolution.
    Best for: Data lake pipelines with Athena integration.
    """
    import awswrangler as wr

    wr.s3.to_parquet(
        df=df,
        path=f"s3://{bucket}/{key}",
        dataset=True,
        partition_cols=["year", "month"],
        database="my_database",       # Optional: Glue catalog
        table="my_table",             # Optional: Glue table
        mode="append",
        compression="snappy",
    )
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `OUTPUT_BUCKET` | (required) | Target S3 bucket |
| `compression` | `snappy` | Options: snappy, gzip, zstd, none |
| `MemorySize` | `512` | Min 512 MB for pyarrow operations |
| `Timeout` | `300` | 5 min for large file writes |

## Compression Comparison

| Codec | Speed | Ratio | Use Case |
|-------|-------|-------|----------|
| `snappy` | Fast | ~2x | Default, balanced |
| `gzip` | Slow | ~3x | Cold storage, max compression |
| `zstd` | Medium | ~3x | Good ratio + decent speed |
| `None` | Fastest | 1x | When speed is critical |

## Partitioned Output Key Pattern

```python
def partitioned_key(prefix: str, record_date: datetime) -> str:
    """Generate Hive-style partitioned key for Athena compatibility."""
    return (
        f"{prefix}"
        f"year={record_date.year}/"
        f"month={record_date.month:02d}/"
        f"day={record_date.day:02d}/"
        f"data-{record_date.strftime('%H%M%S')}.parquet"
    )

# Output: processed/year=2026/month=02/day=17/data-100000.parquet
```

## Example Usage

```python
import pandas as pd

def lambda_handler(event, context):
    # Read CSV from S3
    df = pd.read_csv(io.BytesIO(raw_bytes))

    # Transform
    df["processed_at"] = pd.Timestamp.utcnow()

    # Write Parquet
    write_parquet_pandas(
        df,
        bucket=OUTPUT_BUCKET,
        key=f"processed/{filename}.parquet",
    )
```

## See Also

- [File Processing Pipeline](../patterns/file-processing.md)
- [Layers](../concepts/layers.md)
- [Lambda Handler](../concepts/lambda-handler.md)
