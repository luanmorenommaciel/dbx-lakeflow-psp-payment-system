# Lakeflow Declarative Pipelines - Core Concepts

> Source: https://docs.databricks.com/aws/en/dlt/concepts
> Status: Generally Available (July 2025), open-sourced as Spark Declarative Pipelines

## Overview

Lakeflow Declarative Pipelines is a declarative framework for developing batch and streaming data pipelines in SQL and Python.

## Key Concepts

### 1. Flows
- Foundational data processing concept
- Supports streaming and batch semantics
- Reads data from sources, applies processing logic, writes results
- **Flow types include:**
  - Append
  - Update
  - Complete
  - AUTO CDC
  - Materialized View

### 2. Streaming Tables
- Unity Catalog managed tables
- Can have multiple streaming flows
- Supports AUTO CDC flow type

### 3. Materialized Views
- Batch-oriented Unity Catalog managed tables
- Flows always defined implicitly within view definition

### 4. Sinks
- Streaming targets
- Currently supports:
  - Delta tables
  - Apache Kafka
  - Azure EventHubs

### 5. Pipelines
- Unit of development and execution
- Can contain flows, streaming tables, materialized views, sinks
- Automatically orchestrates execution and parallelization

## Key Benefits

- **Automatic orchestration** - No manual task coordination needed
- **Declarative processing** - Focus on "what" not "how"
- **Incremental processing** - Only processes new/changed data
- **Simplified CDC event handling** - Built-in change data capture
- **Reduced manual coding** - Framework handles complexity

## Technical Foundation

- Runs on Databricks Runtime (16.4+ / 17.3 preview)
- Uses the same DataFrame API as Apache Spark and Structured Streaming
- Integrates with Unity Catalog for governance
- Open-sourced as **Spark Declarative Pipelines** (contributed to Apache Spark at DAIS 2025)

## Recent Enhancements (2025-2026)

### Move Tables Between Pipelines (GA, 2025.29)

Move Materialized Views and Streaming Tables from one pipeline to another using a SQL command plus a minor code adjustment. Enables pipeline refactoring without data loss.

### AUTO CDC Enhancements (2025.30)

- **Multiple flows per target**: `create_auto_cdc_flow` now supports a `name` parameter, allowing multiple independent CDC flows writing to the same destination table
- **One-time backfills**: `once=True` parameter for initial hydration snapshots
- **SQL support**: New `CREATE AUTO CDC FLOW` SQL syntax alongside Python API

### Type Widening (Feb 2026)

Pipelines support safe column type broadening (e.g., `INT` to `LONG`, `FLOAT` to `DOUBLE`) without requiring a full pipeline reset. Enables schema evolution workflows that previously required manual intervention.

### SCD Type 1 with AUTO CDC (Feb 2026)

SCD Type 1 materialization with AUTO CDC provides a simpler CDC pattern that upserts the latest value without maintaining full change history.

### Multi-Catalog/Schema (2025.04)

New pipelines support creating and updating materialized views and streaming tables in multiple catalogs and schemas. The `LIVE` virtual schema and associated syntax is no longer required.

### Serverless TCO Reduction

Recent optimizations cut total cost of ownership (TCO) by up to 70% for serverless pipelines, making serverless the clear default for new pipelines.
