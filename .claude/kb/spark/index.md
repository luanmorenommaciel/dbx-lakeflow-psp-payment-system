# Spark Knowledge Base

> **Purpose**: PySpark and Spark SQL patterns — DataFrames, performance, Spark Connect, VARIANT type, Delta integration
> **Version Coverage**: Apache Spark 4.0+ (released May 2025), PySpark 4.x, Delta Lake 4.x
> **MCP Validated**: 2026-03-26

## Quick Navigation

### Concepts (< 150 lines each)

| File | Purpose |
|------|---------|
| [concepts/dataframe-api.md](concepts/dataframe-api.md) | Core transformations, Column expressions, UDFs |
| [concepts/partitioning.md](concepts/partitioning.md) | repartition vs coalesce, bucketing, AQE |
| [concepts/catalyst-optimizer.md](concepts/catalyst-optimizer.md) | Query plans, predicate pushdown, codegen |
| [concepts/real-time-mode.md](concepts/real-time-mode.md) | Structured Streaming Real-Time Mode (~5ms) |
| [concepts/spark-connect.md](concepts/spark-connect.md) | Spark Connect client-server architecture (4.0+) |

### Patterns (< 200 lines each)

| File | Purpose |
|------|---------|
| [patterns/read-write-patterns.md](patterns/read-write-patterns.md) | Parquet, Delta, Iceberg I/O with schema evolution |
| [patterns/window-functions.md](patterns/window-functions.md) | ROW_NUMBER, RANK, LAG/LEAD, sessionization |
| [patterns/performance-tuning.md](patterns/performance-tuning.md) | Broadcast joins, skew handling, caching |
| [patterns/delta-integration.md](patterns/delta-integration.md) | Delta Lake 4.1, UniForm, MERGE, OPTIMIZE |

---

## Quick Reference

- [quick-reference.md](quick-reference.md) - Fast lookup tables

---

## Key Concepts

| Concept | Description |
|---------|-------------|
| **DataFrame API** | Lazy transformations with Column expressions and type safety |
| **Partitioning** | Data distribution strategy — repartition vs coalesce, AQE |
| **Spark Connect** | Client-server architecture with thin Python client (1.5 MB pyspark-client) |
| **VARIANT Type** | Native semi-structured data type for schema-free JSON handling (4.0+) |
| **SQL Enhancements** | PIPE syntax, SQL UDFs, session variables, SQL scripting (4.0+) |
| **Python Data Source API** | Write custom data sources/sinks in pure Python (4.0+) |
| **TransformWithState** | Next-gen arbitrary stateful operator replacing mapGroupsWithState (4.0+) |
| **Real-Time Mode** | Millisecond-latency streaming without changing APIs (Databricks 2026) |
| **Delta Integration** | Delta Lake 4.x with UniForm, liquid clustering, change data feed |

---

## Learning Path

| Level | Files |
|-------|-------|
| **Beginner** | concepts/dataframe-api.md |
| **Intermediate** | patterns/window-functions.md |
| **Advanced** | patterns/performance-tuning.md |

---

## Agent Usage

| Agent | Primary Files | Use Case |
|-------|---------------|----------|
| spark-engineer | All files | DataFrame transforms, optimization |
| build-agent | patterns/read-write-patterns.md | Verification via spark-submit |
