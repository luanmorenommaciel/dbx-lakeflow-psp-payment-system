# Bronze Ingestion Patterns

> **MCP Validated**: 2025-01-19
> **Source**: https://docs.databricks.com/aws/en/dlt/auto-loader

## S3 Parquet Ingestion (KurvPay Pattern)

The Bronze layer ingests Parquet files from S3 stage bucket (output from Lambda parsers).

```python
import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="mdi_bronze",
    comment="Raw MDI merchant data from S3 stage",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def mdi_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/mnt/schema/mdi")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("s3://kurv-edp-stage-${env}/mdi/")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )
```

## Auto Loader Configuration

| Option | Value | Purpose |
|--------|-------|---------|
| `cloudFiles.format` | `parquet` | Lambda outputs Parquet |
| `cloudFiles.schemaLocation` | `/mnt/schema/{type}` | Schema evolution tracking |
| `cloudFiles.inferColumnTypes` | `true` | Auto-detect column types |
| `cloudFiles.schemaHints` | `amount DECIMAL(18,2)` | Override specific types |

## File Type Patterns

### MDI Bronze

```python
@dlt.table(name="mdi_bronze")
def mdi_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "/mnt/schema/mdi")
        .load(f"s3://kurv-edp-stage-{env}/mdi/*/merchants.parquet")
        .withColumn("_ingested_at", F.current_timestamp())
    )
```

### TDDF Bronze (Multiple Record Types)

```python
@dlt.table(name="tddf_transactions_bronze")
def tddf_transactions_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load(f"s3://kurv-edp-stage-{env}/tddf/*/daily_details.parquet")
        .withColumn("_ingested_at", F.current_timestamp())
    )

@dlt.table(name="tddf_adjustments_bronze")
def tddf_adjustments_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load(f"s3://kurv-edp-stage-{env}/tddf/*/adjustments.parquet")
        .withColumn("_ingested_at", F.current_timestamp())
    )
```

### ADF Bronze (Hierarchical)

```python
@dlt.table(name="adf_transactions_bronze")
def adf_transactions_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load(f"s3://kurv-edp-stage-{env}/adf/*/transactions.parquet")
        .withColumn("_ingested_at", F.current_timestamp())
    )

@dlt.table(name="adf_extensions_bronze")
def adf_extensions_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load(f"s3://kurv-edp-stage-{env}/adf/*/extensions.parquet")
        .withColumn("_ingested_at", F.current_timestamp())
    )
```

## Metadata Columns

Always add these columns to Bronze tables:

```python
.withColumn("_ingested_at", F.current_timestamp())
.withColumn("_source_file", F.input_file_name())
.withColumn("_processing_time", F.current_timestamp())
```

## Data Quality (Bronze)

Bronze layer uses WARN to track issues without blocking:

```python
@dlt.table(name="mdi_bronze")
@dlt.expect("valid_merchant_number", "merchant_number IS NOT NULL")
@dlt.expect("valid_file_date", "file_date IS NOT NULL")
def mdi_bronze():
    ...
```

## Related

- [Silver Cleansing](silver-cleansing.md) - Next layer
- [Python Streaming](python-streaming.md) - Streaming fundamentals
- [Expectations](expectations.md) - Data quality patterns
