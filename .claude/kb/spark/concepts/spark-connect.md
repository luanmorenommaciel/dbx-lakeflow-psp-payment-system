# Spark Connect

> **Purpose**: Client-server architecture for remote Spark execution — thin Python client, multi-language support
> **Confidence**: 0.92
> **Since**: Apache Spark 3.4 (preview), 4.0 (GA with full parity)
> **MCP Validated**: 2026-03-26

## Overview

Spark Connect decouples the Spark client from the cluster. Instead of embedding a full Spark driver in your application, a lightweight client sends logical plans to a remote Spark server over gRPC. In Spark 4.0, Connect reaches high API parity with Spark Classic, supports ML workloads, and offers clients in Python, Scala, Java, Go, Swift, and Rust.

## The Concept

```python
# Install the thin client (1.5 MB vs ~300 MB full pyspark)
# pip install pyspark-client

from pyspark.sql import SparkSession

# Connect to remote Spark cluster
spark = (SparkSession.builder
    .remote("sc://spark-server:15002")
    .getOrCreate())

# Same DataFrame API — runs on remote cluster
df = spark.read.parquet("s3://lake/silver/orders")
result = df.groupBy("region").agg({"amount": "sum"})
result.show()

# Or toggle mode via config (when using full pyspark)
# spark.conf.set("spark.api.mode", "connect")
```

## Quick Reference

| Aspect | Classic Mode | Connect Mode |
|--------|-------------|-------------|
| Client size | ~300 MB (full pyspark) | 1.5 MB (pyspark-client) |
| Architecture | Driver embedded in app | Thin client + remote server |
| Protocol | JVM interop (py4j) | gRPC (language-agnostic) |
| API parity | Full | High (4.0+), expanding |
| ML support | Full | Supported (4.0+) |
| Streaming | Full | Supported (4.0+) |
| UDFs | Full | Python UDFs supported |
| Config toggle | `spark.api.mode = classic` | `spark.api.mode = connect` |

## When to Use

| Use Case | Mode |
|----------|------|
| Notebooks on managed clusters | Connect (default on Databricks) |
| CI/CD pipelines, lightweight jobs | Connect (small client footprint) |
| Custom JVM extensions, advanced configs | Classic |
| Multi-language client apps (Go, Rust) | Connect |
| IDE development with remote cluster | Connect |

## Common Mistakes

### Wrong

```python
# Installing full pyspark when only the client is needed
# pip install pyspark  # 300 MB, includes Spark server jars

# Running Spark driver locally when cluster is remote
spark = SparkSession.builder.master("local[*]").getOrCreate()
```

### Correct

```python
# Thin client for remote execution
# pip install pyspark-client  # 1.5 MB

spark = SparkSession.builder.remote("sc://cluster:15002").getOrCreate()
# All processing happens on the remote cluster
```

## Related

- [dataframe-api](../concepts/dataframe-api.md)
- [performance-tuning](../patterns/performance-tuning.md)
