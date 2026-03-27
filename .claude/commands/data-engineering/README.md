# Data Engineering Commands

> **8 slash commands** for data pipeline development, from schema design to production migration

## Command Catalog

| Command | Purpose | Primary Agent |
|---------|---------|---------------|
| `/pipeline` | DAG/pipeline scaffolding | pipeline-architect |
| `/schema` | Interactive schema design | schema-designer |
| `/data-quality` | Quality rules generation | data-quality-analyst |
| `/lakehouse` | Table format + catalog guidance | lakehouse-architect |
| `/sql-review` | SQL-specific code review | code-reviewer + sql-optimizer |
| `/ai-pipeline` | RAG/embedding scaffolding | ai-data-engineer |
| `/data-contract` | Contract authoring (ODCS) | data-contracts-engineer |
| `/migrate` | Legacy ETL migration | dbt-specialist + spark-engineer |

## Quick Start

```bash
# Design a star schema
/schema "Star schema for e-commerce analytics"

# Generate quality checks for a dbt model
/data-quality models/staging/stg_orders.sql

# Scaffold an Airflow pipeline
/pipeline "Daily orders ETL from Postgres to Snowflake"

# Review SQL for anti-patterns
/sql-review models/marts/

# Create a data contract
/data-contract "Contract between orders team and analytics"

# Migrate stored procedures to dbt
/migrate legacy/etl_orders_proc.sql
```

## How Commands Work

Each command delegates to a specialized agent that:

1. Reads KB patterns from relevant domains (zero tokens for cached patterns)
2. Analyzes your input (SQL files, descriptions, requirements)
3. Generates production-ready code with confidence scoring
4. Escalates to other agents when the task crosses specializations
