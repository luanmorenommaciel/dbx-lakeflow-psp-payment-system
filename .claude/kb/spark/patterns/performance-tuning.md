# Performance Tuning

> **Purpose**: Broadcast joins, skew handling, AQE settings, caching strategy, Spark UI diagnosis
> **MCP Validated**: 2026-03-26

## When to Use

- Spark job is slow or failing with OOM errors
- Shuffle stages dominate execution time
- Data skew causes straggler tasks
- Need to right-size cluster or partition count

## Implementation

```python
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


# --- AQE (Adaptive Query Execution) — enable everything ---
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256m")


# --- BROADCAST JOIN (small table < 10MB) ---
# Explicit hint — sends dim table to all executors, avoids shuffle
from pyspark.sql.functions import broadcast

result = orders_df.join(broadcast(dim_country), "country_code", "left")

# Auto-broadcast threshold (default 10MB)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50m")  # raise for medium dims


# --- SALTING SKEWED KEYS ---
# Problem: 80% of orders belong to customer_id = "GUEST"
salt_buckets = 10

# Salt the skewed side
orders_salted = (orders_df
    .withColumn("salt", (F.rand() * salt_buckets).cast("int"))
    .withColumn("join_key", F.concat("customer_id", F.lit("_"), "salt")))

# Explode the dimension side to match all salts
customers_exploded = (customers_df
    .crossJoin(spark.range(salt_buckets).withColumnRenamed("id", "salt"))
    .withColumn("join_key", F.concat("customer_id", F.lit("_"), "salt")))

# Join on salted key — distributes "GUEST" across 10 partitions
result = orders_salted.join(customers_exploded, "join_key", "inner")


# --- CACHING (with cleanup) ---
filtered = orders_df.filter(F.col("status") == "active").cache()
filtered.count()  # materialize the cache

# Use cached DataFrame multiple times
summary = filtered.groupBy("region").agg(F.sum("amount"))
detail = filtered.filter(F.col("amount") > 1000)

filtered.unpersist()  # always clean up


# --- REPARTITION FOR WRITE ---
# Target: ~128MB per file, ~128MB per partition
target_size_bytes = 128 * 1024 * 1024
data_size = spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size()
```

## Configuration

| Setting | Default | Recommendation |
|---------|---------|---------------|
| `spark.sql.adaptive.enabled` | `true` (3.2+) | Always on |
| `spark.sql.shuffle.partitions` | `200` | Let AQE auto-coalesce |
| `spark.sql.autoBroadcastJoinThreshold` | `10m` | Raise to 50-100m if memory allows |
| `spark.sql.adaptive.skewJoin.enabled` | `true` | Always on |
| `spark.sql.files.maxPartitionBytes` | `128m` | Match target file size |
| `spark.locality.wait` | `3s` | Lower to `0s` on cloud (no data locality) |

## Example Usage

```python
# Diagnose: check partition sizes after a shuffle
df.groupBy(F.spark_partition_id()).count().orderBy("count", ascending=False).show(10)

# Diagnose: check for skew in join keys
df.groupBy("join_key").count().orderBy("count", ascending=False).show(10)
```

## Spark 4.0 Performance Additions

### TransformWithState (Replaces mapGroupsWithState)

```python
# Spark 4.0: new stateful streaming operator
# Supports object-oriented state, composite types, auto TTL, timers
from pyspark.sql.streaming import TransformWithState

class SessionTracker(TransformWithState):
    """Track user sessions with automatic TTL-based expiry."""

    def init(self, handle):
        self.session = handle.getValueState("session", schema)

    def handleInputRows(self, key, rows, timer_values):
        # Process rows, update state with TTL
        for row in rows:
            self.session.update(new_state)
        # Set timer for session timeout
        timer_values.register_event_time_timer(expiry_ms)

    def handleExpiredTimer(self, key, timer_values):
        # Emit session summary when timer fires
        yield self.session.get()
        self.session.clear()
```

### Arrow UDFs (Replacing Pandas UDFs)

```python
# Spark 4.0: Arrow-optimized UDFs — faster than Pandas UDFs
# Operates on Arrow data directly, no serialization overhead
import pyarrow.compute as pc

@F.udf(returnType=StringType(), useArrow=True)
def normalize_email(email):
    return pc.utf8_lower(email)

df = df.withColumn("email_clean", normalize_email("email"))
```

### Spark Connect Mode Toggle

```python
# Switch between classic and Connect mode
spark.conf.set("spark.api.mode", "connect")  # thin client mode
spark.conf.set("spark.api.mode", "classic")   # traditional mode

# Or use the lightweight client package
# pip install pyspark-client  (1.5 MB vs ~300 MB full pyspark)
```

## See Also

- [partitioning](../concepts/partitioning.md)
- [catalyst-optimizer](../concepts/catalyst-optimizer.md)
- [spark-connect](../concepts/spark-connect.md)
