# Modern Stack Quick Reference

> Fast lookup tables. For code examples, see linked files.

## "Do I Need Spark?" Decision Tree

| Question | If Yes | If No |
|----------|--------|-------|
| Data > 500GB per job? | Consider Spark | DuckDB or Polars |
| Need distributed compute? | Spark or Flink | DuckDB (single node) |
| Real-time streaming? | Spark Streaming or Flink | DuckDB (batch) |
| Team knows PySpark? | Spark | Polars (similar API, faster) |
| CI/CD data testing? | DuckDB (instant startup) | — |
| Analytics on laptop? | DuckDB or Polars | — |

## DuckDB Feature Highlights (through 1.2, Feb 2025)

| Feature | Description | Since |
|---------|-------------|-------|
| Friendlier SQL | `FROM` without `SELECT`, implicit column refs, `map[]` returns value | 1.2 |
| Improved randomness | `random()` uses larger state for better distribution | 1.2 |
| One-line install | `curl https://install.duckdb.org \| sh` on Linux/macOS | 1.2 |
| VARIANT type | Schema-flexible column (JSON-like) | 1.1 |
| GEOMETRY type | Native spatial data support | Extension |
| Iceberg support | Read + write via extension (frequently updated) | Extension |
| httpfs extension | Query S3/GCS/Azure directly | Extension |
| Delta Lake | Read Delta tables via extension | Extension |
| MCP server | AI agent integration | Community |
| AES-256 encryption | Encrypted database files | 1.0 |
| MERGE INTO | Upsert support | 1.1 |
| GSheet extension | Read/write Google Sheets | Community |
| Smallpond | Distributed DuckDB compute (via DeepSeek) | Community |

## Polars vs Pandas

| Feature | Polars | Pandas |
|---------|--------|--------|
| Speed (10GB groupby) | ~2s | ~30s |
| Memory efficiency | Columnar (Arrow) | Row-oriented |
| Lazy evaluation | Yes (query optimizer) | No |
| Multi-threaded | Yes (automatic) | No (GIL) |
| Null handling | Proper null type | NaN (float-based) |
| API style | Expression-based | Method chaining |
| Cloud support | Polars Cloud (2025) | None |

## Polars Release Velocity (2025)

| Version | Date | Highlights |
|---------|------|------------|
| 1.33 | Sep 2025 | Streaming engine improvements, `map_columns`, `pipe_with_schema` |
| 1.34 | Sep 2025 | `sink_batches`/`collect_batches`, Iceberg filter pushdown |
| 1.35 | Oct 2025 | Stable `Decimal` type, native streaming group-by, Iceberg pushdown |
| 1.37 | Jan 2026 | `PartitionBy` API, new streaming CSV/NDJSON sink pipelines |
| 1.39 | Mar 2026 | Latest stable release |

## SQLMesh vs dbt (Updated 2025)

| Feature | SQLMesh | dbt Core | dbt Cloud |
|---------|---------|----------|-----------|
| Virtual environments | Yes (no data copy) | No | Cloud IDE only |
| Change detection | Content hash (SQLGlot) | Timestamp | Timestamp |
| Automatic backfills | Yes (affected only) | No (manual) | No |
| Built-in scheduler | Yes | No (needs Airflow) | Yes |
| Column-level lineage | Yes (native) | Fusion only | Fusion only |
| dbt compatibility | Yes (adapter + CLI) | Native | Native |
| Fabric Warehouse | Yes (Aug 2025) | Yes | Yes |
| VSCode extension | Yes (OSS) | dbt Power User (3P) | Cloud IDE |
| Execution speed | ~9x faster than dbt Core | Baseline | Fusion (30x parse) |
| State management | Built-in (stateful) | Stateless (manifest) | Cloud-managed |
| SQL validation | Compile-time (SQLGlot) | Run-time (Jinja) | Run-time |
| Linter rules | Built-in (NoMissingUnitTest, etc.) | sqlfluff (separate) | Cloud linting |
| Price | Free (OSS) | Free (OSS) | Paid |

## Common Pitfalls

| Don't | Do |
|-------|-----|
| Default to Spark for everything | Start with DuckDB, scale up if needed |
| Ignore Polars for pandas workloads | Benchmark — often 10-15x faster |
| Skip SQLMesh evaluation | Virtual envs alone may justify switching |
| Build BI dashboards from scratch | Use Evidence.dev (Markdown + SQL) |
