# BigQuery

> **Purpose**: Serverless data warehouse for loading, querying, and analyzing pipeline results at scale
> **Confidence**: 0.95
> **MCP Validated**: 2026-02-17

## Overview

BigQuery is a fully managed, serverless data warehouse that supports SQL queries across
petabytes of data. In serverless data pipelines, BigQuery serves as the final destination
for processed data. It supports loading from GCS, streaming inserts, and direct DataFrame
uploads. Partitioning and clustering optimize both performance and cost.

## The Pattern

```python
from google.cloud import bigquery

client = bigquery.Client()

def load_from_gcs(table_id: str, gcs_uri: str, write_mode: str = "WRITE_APPEND"):
    """Load data from GCS into BigQuery."""
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=getattr(bigquery.WriteDisposition, write_mode),
        autodetect=True,
    )
    load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    load_job.result()  # Wait for completion
    table = client.get_table(table_id)
    return f"Loaded {table.num_rows} rows to {table_id}"

def query(sql: str) -> list[dict]:
    """Execute a SQL query and return results as list of dicts."""
    query_job = client.query(sql)
    results = query_job.result()
    return [dict(row) for row in results]

def load_dataframe(table_id: str, df, write_mode: str = "WRITE_APPEND"):
    """Load a pandas DataFrame into BigQuery."""
    job_config = bigquery.LoadJobConfig(
        write_disposition=getattr(bigquery.WriteDisposition, write_mode),
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
```

## Quick Reference

| Input | Output | Notes |
|-------|--------|-------|
| `client.load_table_from_uri(uri, table)` | `LoadJob` | GCS -> BigQuery |
| `client.load_table_from_dataframe(df, table)` | `LoadJob` | DataFrame -> BigQuery |
| `client.query(sql)` | `QueryJob` | Call `.result()` to get rows |
| `client.insert_rows_json(table, rows)` | Errors list | Streaming insert, near real-time |

## Supported Source Formats

| Format | Constant | Best For |
|--------|----------|----------|
| JSON (newline-delimited) | `NEWLINE_DELIMITED_JSON` | Semi-structured data |
| CSV | `CSV` | Tabular exports |
| Parquet | `PARQUET` | Columnar analytics data |
| Avro | `AVRO` | Schema-evolution workloads |
| ORC | `ORC` | Hadoop ecosystem data |

## Common Mistakes

### Wrong

```python
# Using SELECT * wastes money and is slow
results = client.query("SELECT * FROM `project.dataset.table`").result()
# Loading without write disposition can cause duplicates
client.load_table_from_uri(uri, table_id)
```

### Correct

```python
# Select only needed columns, always set write disposition
results = client.query(
    "SELECT id, name, amount FROM `project.dataset.table` WHERE date > '2025-01-01'"
).result()

job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
)
client.load_table_from_uri(uri, table_id, job_config=job_config)
```

## BigLake Iceberg Tables (2025-2026)

BigLake Iceberg tables bring open lakehouse capabilities to BigQuery with customer-owned GCS storage.

```sql
-- Create a BigLake Iceberg table in BigQuery
CREATE TABLE my_dataset.events
(
  event_id STRING,
  event_type STRING,
  payload JSON,
  created_at TIMESTAMP
)
WITH CONNECTION `my-project.us.my-connection`
OPTIONS (
  file_format = 'PARQUET',
  table_format = 'ICEBERG',
  storage_uri = 'gs://my-bucket/iceberg/events'
);

-- Insert data (tracked by BigQuery, Iceberg snapshots auto-exported)
INSERT INTO my_dataset.events VALUES
  ('evt-001', 'click', JSON '{"page": "/home"}', CURRENT_TIMESTAMP());

-- Time travel query
SELECT * FROM my_dataset.events
FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
```

### Key Features

| Feature | Status |
|---------|--------|
| Schema evolution (add/drop/rename columns) | GA |
| Automatic storage optimization (clustering, file sizing) | GA |
| Iceberg V2 snapshot export | GA |
| BigLake Metastore (managed catalog) | GA |
| Iceberg REST Catalog API | Preview |
| Table partitioning | Preview |
| Multi-statement transactions | Preview |
| Column-level security and data masking | GA |

### BigQuery DataFrames 2.0

Process multimodal data (images, audio, text) with Pandas-compatible API at BigQuery scale:

```python
import bigframes.pandas as bpd

# Read BigQuery table as DataFrame (scales to TB)
df = bpd.read_gbq("my_project.my_dataset.events")

# Standard Pandas operations execute in BigQuery
result = df.groupby("event_type").count()
```

## Related

- [GCS](../concepts/gcs.md) - Source for batch loading
- [Cloud Run](../concepts/cloud-run.md) - Orchestrates load jobs
- [Event-Driven Pipeline](../patterns/event-driven-pipeline.md) - End-to-end pipeline
- [Multi-Bucket Pipeline](../patterns/multi-bucket-pipeline.md) - Stage before loading
