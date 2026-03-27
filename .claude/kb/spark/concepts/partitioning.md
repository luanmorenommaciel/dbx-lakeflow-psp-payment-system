# Partitioning

> **Purpose**: Data distribution strategies — repartition vs coalesce, partition pruning, bucketing, AQE
> **Confidence**: 0.95
> **MCP Validated**: 2026-03-26

## Overview

Partitioning controls how data is distributed across Spark executors. Getting it right determines shuffle cost, parallelism, and write performance. The rule of thumb: **2-3x the number of cores** for partition count, but AQE (Adaptive Query Execution) can auto-tune this at runtime.

## The Concept

```python
from pyspark.sql import functions as F

# repartition: full shuffle, creates exactly N partitions
# Use when: upstream partitions are skewed or you need hash-based distribution
df_repartitioned = df.repartition(200, "customer_id")

# coalesce: no shuffle, reduces partitions by merging
# Use when: reducing partition count after a filter that removes most rows
df_coalesced = df.filter(F.col("status") == "active").coalesce(10)

# Partition on write: controls file layout on disk
(df
    .repartition("order_date")
    .write
    .partitionBy("order_date")
    .parquet("s3://bucket/orders/"))
```

## Quick Reference

| Strategy | Shuffle? | Use When | Partition Count |
|----------|----------|----------|-----------------|
| `repartition(n)` | Yes | Need even distribution | Exact N |
| `repartition(n, "col")` | Yes | Hash partition by key | Exact N |
| `coalesce(n)` | No | Reducing partitions only | At most N |
| AQE auto-coalesce | Auto | Default in Spark 3.x | Dynamic |
| `.partitionBy("col")` | Write | Hive-style file layout | Per distinct value |
| `.bucketBy(n, "col")` | Write | Pre-shuffle for joins | Exact N buckets |

## Common Mistakes

### Wrong

```python
# coalesce(1) on large data — creates a bottleneck, OOM risk
df.coalesce(1).write.parquet("output/")

# Fixed shuffle partitions — ignores data volume
spark.conf.set("spark.sql.shuffle.partitions", "200")  # hardcoded
```

### Correct

```python
# Let AQE handle it (Spark 3.2+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# If you must set it, scale to data: ~128MB per partition
partition_count = max(1, total_bytes // (128 * 1024 * 1024))
df.repartition(partition_count).write.parquet("output/")
```

## Related

- [catalyst-optimizer](../concepts/catalyst-optimizer.md)
- [performance-tuning](../patterns/performance-tuning.md)
