# Local-First Analytics

> **Purpose**: Replace cloud warehouse / Spark for sub-500GB workloads using DuckDB + Evidence.dev, with cost comparison and CI pipeline patterns
> **MCP Validated**: 2026-03-26

## When to Use

- Analytical workloads under 500GB where Spark or a cloud warehouse is overkill
- Teams wanting instant local iteration without warehouse credentials
- CI/CD pipelines that need fast, zero-infrastructure data validation
- BI dashboards backed by Git-versioned SQL (Evidence.dev)
- Cost-sensitive environments replacing always-on Snowflake/Databricks compute

## Decision Framework

```text
Dataset Size        Recommended Stack
─────────────────────────────────────────────
< 1 GB              DuckDB in-process (single script)
1 GB – 50 GB        DuckDB + Polars (lazy eval)
50 GB – 500 GB      DuckDB on S3 + Evidence.dev dashboards
> 500 GB            Spark / cloud warehouse (leave local-first)
```

## Cost Comparison

| Stack | Monthly Cost (100GB daily scan) | Latency | Infrastructure |
|-------|-------------------------------|---------|----------------|
| Snowflake (XS warehouse) | ~$400–800 | 2–10s | Managed |
| Databricks (Jobs Lite) | ~$300–600 | 3–15s | Managed |
| BigQuery (on-demand) | ~$500 | 1–5s | Serverless |
| **DuckDB + Evidence.dev** | **$0 (local) / ~$20 (CI)** | **<1s** | **None** |

## Implementation

### DuckDB + Evidence.dev Dashboard Pipeline

```yaml
# evidence-project/evidence.config.yaml
name: local-analytics
database:
  type: duckdb
  path: data/analytics.duckdb
```

```sql
-- evidence-project/pages/revenue/index.md (Evidence SQL block)
-- Revenue dashboard backed by local Parquet files

SELECT
    DATE_TRUNC('month', order_date) AS month,
    segment,
    SUM(net_amount) AS revenue,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(net_amount) / COUNT(DISTINCT customer_id) AS revenue_per_customer
FROM read_parquet('data/orders/*.parquet')
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC;
```

### Python Pipeline: DuckDB + Polars + Parquet

```python
import duckdb
import polars as pl
from pathlib import Path

# ============================================================
# Extract: DuckDB reads raw files (S3 or local)
# ============================================================
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

raw = con.sql("""
    SELECT *
    FROM read_parquet('s3://data-lake/raw/events/**/*.parquet',
                      hive_partitioning=true)
    WHERE event_date >= '2026-03-01'
""").pl()  # → Polars DataFrame (zero-copy via Arrow)

# ============================================================
# Transform: Polars lazy pipeline (optimized execution)
# ============================================================
result = (
    raw.lazy()
    .filter(pl.col("event_type").is_in(["purchase", "subscription"]))
    .with_columns(
        (pl.col("amount") * pl.col("quantity")).alias("revenue"),
        pl.col("event_date").dt.month().alias("month"),
    )
    .group_by("customer_id", "month")
    .agg(
        pl.col("revenue").sum().alias("total_revenue"),
        pl.col("event_type").n_unique().alias("event_types"),
    )
    .sort("total_revenue", descending=True)
    .collect()
)

# ============================================================
# Load: Write optimized Parquet for Evidence.dev
# ============================================================
output = Path("evidence-project/data")
output.mkdir(parents=True, exist_ok=True)

result.write_parquet(
    output / "customer_revenue.parquet",
    compression="zstd",
    row_group_size=100_000,
)
```

### CI Pipeline: Data Validation Without a Warehouse

```yaml
# .github/workflows/data-quality.yml
name: Data Quality
on: [pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install DuckDB CLI
        run: |
          curl -fsSL https://install.duckdb.org | sh

      - name: Run data assertions
        run: |
          duckdb < tests/assertions.sql

      - name: Build Evidence dashboards
        run: |
          cd evidence-project
          npm install
          npm run build  # fails on SQL errors
```

```sql
-- tests/assertions.sql
-- Fast, zero-infrastructure data validation

-- Assert: no null primary keys
SELECT COUNT(*) AS null_keys
FROM read_parquet('data/orders/*.parquet')
WHERE order_id IS NULL
HAVING COUNT(*) > 0;
-- Returns rows only on failure → non-zero exit code

-- Assert: referential integrity
SELECT COUNT(*) AS orphan_orders
FROM read_parquet('data/orders/*.parquet') o
LEFT JOIN read_parquet('data/customers/*.parquet') c
    ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL
HAVING COUNT(*) > 0;

-- Assert: no future dates
SELECT COUNT(*) AS future_dates
FROM read_parquet('data/orders/*.parquet')
WHERE order_date > CURRENT_DATE
HAVING COUNT(*) > 0;

-- Assert: revenue within expected bounds
SELECT COUNT(*) AS outliers
FROM read_parquet('data/orders/*.parquet')
WHERE net_amount < 0 OR net_amount > 1000000
HAVING COUNT(*) > 0;
```

### Replacing Spark for Small-Medium Workloads

```python
# Before: Spark (heavy, slow startup, cluster needed)
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.getOrCreate()
# df = spark.read.parquet("data/events/")
# result = df.groupBy("customer_id").agg(sum("amount"))

# After: DuckDB (instant, zero infrastructure)
import duckdb

result = duckdb.sql("""
    SELECT customer_id, SUM(amount) AS total
    FROM read_parquet('data/events/**/*.parquet', hive_partitioning=true)
    GROUP BY customer_id
    ORDER BY total DESC
""").df()

# Performance comparison (100GB Parquet, M2 MacBook):
# Spark local:  ~45s startup + ~30s query = ~75s
# DuckDB:       ~0s startup  + ~12s query = ~12s
```

## Configuration

| Tool | Role | Install |
|------|------|---------|
| DuckDB | SQL engine on local/S3 files | `pip install duckdb` or `brew install duckdb` |
| Polars | DataFrame transforms (lazy eval) | `pip install polars` |
| Evidence.dev | Git-versioned BI dashboards | `npx degit evidence-dev/template my-report` |
| dbt-duckdb | dbt adapter for local dev | `pip install dbt-duckdb` |
| SQLMesh | Model framework with DuckDB default | `pip install sqlmesh` |

## Anti-Patterns

- **Running DuckDB on 1TB+ datasets** — beyond the sweet spot; use Spark or a warehouse
- **Sharing a DuckDB file across processes** — single-writer; use Parquet files as the shared layer
- **Skipping Parquet in favor of CSV** — CSV lacks schema, compression, and predicate pushdown
- **Not setting `hive_partitioning=true`** — miss partition pruning on directory-structured data

## See Also

- [duckdb-patterns](../patterns/duckdb-patterns.md)
- [polars-patterns](../patterns/polars-patterns.md)
- [duckdb](../concepts/duckdb.md)
- [polars](../concepts/polars.md)
- [cost-optimization](../../cloud-platforms/patterns/cost-optimization.md)
