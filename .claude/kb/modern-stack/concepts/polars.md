# Polars

> **Purpose:** High-performance DataFrame library with lazy evaluation and native Arrow backend
> **Confidence:** 0.90
> **MCP Validated:** 2026-03-26

## Overview

Polars is a DataFrame library written in Rust, designed to replace Pandas for data engineering and analytical workloads. It uses Apache Arrow as its in-memory format, supports lazy evaluation with a query optimizer, and handles larger-than-RAM datasets through a new streaming execution engine. Polars operates on a single node but routinely outperforms Spark on datasets that fit within one machine's resources. As of early 2026, Polars is at version 1.39+ with nearly 1M daily PyPI downloads.

The library exposes two APIs: an eager API for interactive exploration (similar to Pandas) and a lazy API that builds a logical plan and optimizes it before execution.

**Key 2025 developments:**
- **New streaming engine** -- rewired sink pipelines for CSV, NDJSON, IPC, Parquet with better memory efficiency
- **`sink_batches`/`collect_batches`** -- process large datasets in batches without loading all into memory (v1.34)
- **Stable `Decimal` type** (v1.35) -- production-ready decimal arithmetic
- **`PartitionBy` API** (v1.37) -- native partitioned writes
- **Native streaming aggregations** -- `group_by`, `unique`, `n_unique`, `skew`, `kurtosis` all lowered to streaming engine
- **Iceberg filter pushdown** (v1.34) -- skip files in `scan_iceberg` based on metadata statistics
- **Polars Cloud** -- remote execution for larger-than-single-node workloads

## Key Concepts

### Lazy vs Eager Evaluation

- **Eager mode** (`pl.DataFrame`) -- Operations execute immediately. Good for exploration, prototyping, and small datasets. Familiar to Pandas users.
- **Lazy mode** (`pl.LazyFrame`) -- Operations build a logical plan. Calling `.collect()` triggers the query optimizer, which applies predicate pushdown, projection pushdown, and common subexpression elimination before executing. Always prefer lazy for production pipelines.

```python
# Eager: executes each step immediately
df = pl.read_parquet("events.parquet")
result = df.filter(pl.col("status") == "active").group_by("region").agg(pl.col("revenue").sum())

# Lazy: builds plan, optimizes, then executes
result = (
    pl.scan_parquet("events.parquet")
    .filter(pl.col("status") == "active")
    .group_by("region")
    .agg(pl.col("revenue").sum())
    .collect()
)
```

### Expression API

Polars replaces method chaining on columns with a composable expression system. Expressions are declarative, parallelizable, and optimizable:

```python
pl.col("price") * pl.col("quantity")          # arithmetic
pl.col("name").str.to_lowercase()             # string ops
pl.col("timestamp").dt.year()                 # datetime ops
pl.when(pl.col("score") > 90).then("A")      # conditional
```

### Query Optimizer

The lazy engine applies optimizations automatically:
- **Predicate pushdown** -- filters move to the earliest possible point, reducing I/O
- **Projection pushdown** -- only columns needed downstream are read from disk
- **Slice pushdown** -- LIMIT operations propagate to source scans
- **Common subexpression elimination** -- shared computations are computed once

### Streaming Engine (Rewritten 2025)

For datasets larger than available RAM, Polars processes data in streaming batches. The new streaming engine (2025) supports native streaming for most operations including group-by, unique, shift, arg_where, and cumulative functions:

```python
# Streaming collect (automatic engine selection)
result = pl.scan_parquet("huge_dataset/*.parquet").filter(...).collect(streaming=True)

# Explicit engine selection (v1.34+)
result = lf.collect(engine="streaming")

# Sink to files in streaming mode (new pipelines for CSV, NDJSON, IPC, Parquet)
lf.sink_parquet("output.parquet")
lf.sink_csv("output.csv")
lf.sink_ipc("output.ipc")

# Process in batches without loading all into memory (v1.34+)
for batch_df in lf.collect_batches():
    process(batch_df)
```

### Polars Cloud

Polars Cloud extends the single-node library to distributed execution. It takes the same Polars code and runs it remotely across a configured compute context:

```python
import polars_cloud as pc

ctx = pc.ComputeContext(workspace="your-workspace", cpus=16, memory=64)
query = pl.scan_parquet("s3://my-dataset/").group_by("region").agg(pl.mean("revenue"))
query.remote(context=ctx).sink_parquet("s3://my-dst/")
```

### Arrow Interop

Polars uses Arrow as its native memory format, enabling zero-copy data exchange with DuckDB, PyArrow, and any Arrow-compatible system. Convert freely between ecosystems without serialization overhead.

## When to Use

- **Replacing Pandas** in pipelines where performance or memory is a concern
- **Single-node data engineering** on datasets from MBs to hundreds of GBs
- **ETL pipelines** where lazy evaluation reduces I/O and compute
- **Feature engineering** in ML pipelines needing fast group-by and window operations
- **Interop layer** between DuckDB (SQL) and Python processing (expressions)
- **Streaming ingestion** of larger-than-RAM files without resorting to Spark

## Trade-offs

| Strength | Limitation |
|----------|------------|
| 5-50x faster than Pandas on typical workloads | Smaller ecosystem of plugins vs Pandas |
| Lazy evaluation with automatic optimization | Learning curve for expression API |
| Native Arrow format, zero-copy interop | Not a drop-in Pandas replacement (different API) |
| New streaming engine for larger-than-RAM data | True distributed execution requires Polars Cloud |
| Rust core with Python, Node, R bindings | Some niche Pandas functions not yet ported |
| Memory-efficient columnar processing | Community growing fast (~1M daily PyPI downloads) |
| Stable Decimal type (v1.35) | Breaking changes between versions (eager Expr removal in 1.33) |
| Iceberg filter pushdown (v1.34) | Delta Lake write support via community extension |

### Polars vs Pandas

| Dimension | Pandas | Polars |
|-----------|--------|--------|
| Execution | Eager only | Eager + Lazy |
| Backend | NumPy (row-ish) | Arrow (columnar) |
| Parallelism | Single-threaded | Multi-threaded by default |
| Memory | 2-10x dataset size | ~1x dataset size |
| API style | Method chaining on Series | Expression-based |
| Larger-than-RAM | No (without Dask) | Yes (streaming mode) |

## See Also

- [Polars Patterns](../patterns/polars-patterns.md) -- Lazy pipelines, expressions, Arrow interop
- [DuckDB](duckdb.md) -- Complementary SQL engine with Arrow exchange
- [Local-First Analytics](../patterns/local-first-analytics.md) -- Polars in local-first stacks
