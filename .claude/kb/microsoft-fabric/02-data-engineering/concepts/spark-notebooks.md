> **MCP Validated:** 2026-02-17

# Spark Notebooks

> **Purpose**: PySpark notebooks in Microsoft Fabric -- parameters, mssparkutils, session configuration, and library management
> **Confidence**: 0.95

## Overview

Fabric Spark notebooks provide an interactive PySpark environment backed by serverless Spark pools. Each notebook runs in a Spark session with pre-configured access to OneLake, built-in `mssparkutils` for file and credential operations, and support for parameterized execution from pipelines. Notebooks use Spark 3.4+ runtime with Delta Lake integrated by default.

## Session Configuration

```python
# Must be in the first cell
%%configure
{
    "conf": {
        "spark.sql.shuffle.partitions": "16",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true"
    },
    "driverMemory": "28g",
    "executorMemory": "28g",
    "numExecutors": 2
}
```

## Notebook Parameters

```python
# Tag this cell as "Parameters" -- values overridden by pipeline at runtime
pipeline_date = "2026-01-01"
source_table = "raw_invoices"
target_table = "bronze_invoices"
batch_size = 10000
```

## mssparkutils Reference

```python
# File system operations
mssparkutils.fs.ls("Files/raw/")
mssparkutils.fs.mkdirs("Files/staging/2026/")
mssparkutils.fs.cp("Files/raw/data.csv", "Files/archive/")
mssparkutils.fs.rm("Files/temp/", recurse=True)

# Credential management
token = mssparkutils.credentials.getToken("https://storage.azure.com/")
secret = mssparkutils.credentials.getSecret(
    "https://my-keyvault.vault.azure.net/", "my-secret-name"
)

# Notebook orchestration
mssparkutils.notebook.run("transform_notebook", timeout_seconds=600, arguments={
    "input_table": "bronze_invoices",
    "output_table": "silver_invoices"
})

# Exit with return value (for pipeline status)
mssparkutils.notebook.exit("SUCCESS: Processed 1500 rows")
```

## Library Management

```python
# Session-scoped installation
%pip install great-expectations==0.18.0
%pip install azure-identity requests

# Workspace-level: Settings > Data Engineering > Library Management
# Upload .whl or requirements.txt for environment-level packages
```

## Common Patterns

```python
from pyspark.sql.functions import col, current_timestamp, when, lit

# Read, transform, write
df = spark.read.format("delta").load("Tables/raw_invoices")

df_clean = (
    df.filter(col("invoice_date").isNotNull())
    .withColumn("amount", col("amount").cast("decimal(18,2)"))
    .withColumn("processed_at", current_timestamp())
    .withColumn("is_valid", when(col("amount") > 0, True).otherwise(False))
)

df_clean.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("clean_invoices")
```

## Quick Reference

| Feature | Details |
|---------|---------|
| Runtime | Spark 3.4+ with Delta Lake |
| Languages | PySpark, Scala, SparkSQL, R |
| Default timeout | 20 minutes (configurable) |
| Max session duration | 60 minutes (extendable) |
| Parameter injection | Via pipeline activity settings |
| Return values | `mssparkutils.notebook.exit(value)` |

## Common Mistakes

### Wrong

```python
# Using pandas for large datasets (OOM risk)
import pandas as pd
df = pd.read_csv("/lakehouse/default/Files/big_file.csv")
```

### Correct

```python
# Use PySpark for large datasets, pandas for small subsets
df = spark.read.csv("Files/big_file.csv", header=True, inferSchema=True)
pdf = df.limit(1000).toPandas()
```

## Related

- [Lakehouse](lakehouse.md)
- [Dataflow Gen2](dataflow-gen2.md)
- [Delta Lake Optimization](../patterns/delta-lake-optimization.md)
- [Incremental Load](../patterns/incremental-load.md)
