# Soda Core

> **Purpose**: Soda 4.0 Data Contracts, SodaCL check syntax, anomaly detection (RAD), AI-powered rules
> **Confidence**: 0.90
> **MCP Validated**: 2026-03-26

## Overview

Soda Core is an open-source data quality framework. **Soda 4.0** (Jan 2026) introduced a major paradigm shift: Data Contracts are now the default way to define data quality rules, replacing the checks-based SodaCL syntax. The new approach offers a cleaner, more structured, and more maintainable way to define and manage data quality rules.

**Key Soda 4.0 features:**
- **Data Contracts as default** -- cleaner, structured rule definitions per table
- **AI-powered rule translation** -- write rules in natural language, AI converts to code
- **AI-powered observability** -- automatic anomaly detection runs in background
- **Extensible check types** -- Missing, Invalid, Duplicate, Aggregate, Failed Rows, Metric
- **Plugin system** -- extend functionality with custom plugins
- **Variables in contracts** -- parameterize contracts for environment-specific values
- **New CLI** -- noun-verb structure with better Soda Cloud API integration
- **Multi-source support** -- Postgres, Snowflake, BigQuery, Databricks, Redshift, SQL Server, Fabric, Synapse, Athena, DuckDB
- **Result handlers** -- post-processing hooks for contract verification results

> **Breaking change:** Soda Core v4 moves from the SodaCL checks language to a Data Contracts-based syntax.

## Soda 4.0 Data Contracts

```yaml
# contracts/orders.contract.yaml — Soda 4.0 Data Contract
dataset: orders
source: my_warehouse

columns:
  - name: order_id
    type: varchar
    checks:
      - type: missing
        fail: when count > 0
      - type: duplicate
        fail: when count > 0
  - name: customer_id
    type: varchar
    checks:
      - type: missing
        fail: when percent > 1
  - name: status
    type: varchar
    checks:
      - type: invalid
        fail: when count > 0
        valid_values: [pending, completed, cancelled, refunded]
  - name: amount
    type: decimal
    checks:
      - type: aggregate
        fail: when min < 0

checks:
  - type: metric
    metric: row_count
    fail: when value = 0
  - type: failed_rows
    query: "SELECT * FROM orders WHERE amount < 0"
```

## Legacy SodaCL Syntax (v3)

```yaml
# checks/orders.yml — SodaCL check definitions (v3 syntax)
checks for orders:
  # Completeness
  - missing_count(order_id) = 0
  - missing_percent(customer_id) < 1%

  # Uniqueness
  - duplicate_count(order_id) = 0

  # Validity
  - invalid_percent(status) = 0:
      valid values: [pending, completed, cancelled, refunded]
  - min(amount) >= 0

  # Freshness
  - freshness(updated_at) < 2h

  # Volume
  - row_count > 0
  - anomaly detection for row_count  # RAD — learns normal volume

  # Distribution anomaly
  - anomaly detection for avg(amount)

  # Schema
  - schema:
      warn:
        when schema changes:
          - column delete
          - column type change
```

## Quick Reference

### Soda 4.0 Check Types

| Check Type | Description | Example |
|-----------|-------------|---------|
| `missing` | Null/empty value detection | `fail: when count > 0` |
| `invalid` | Values outside accepted set | `valid_values: [a, b, c]` |
| `duplicate` | Duplicate value detection | `fail: when count > 0` |
| `aggregate` | Min, max, avg, sum checks | `fail: when min < 0` |
| `failed_rows` | Custom SQL returning failures | `query: "SELECT * FROM ..."` |
| `metric` | Row count and custom metrics | `fail: when value = 0` |

### Legacy SodaCL Syntax (v3)

| Check Type | Syntax | Example |
|-----------|--------|---------|
| Missing | `missing_count(col)` | `missing_count(email) = 0` |
| Duplicate | `duplicate_count(col)` | `duplicate_count(id) = 0` |
| Valid values | `invalid_percent(col)` | `invalid_percent(status) = 0` |
| Range | `min(col)`, `max(col)` | `min(age) >= 0` |
| Freshness | `freshness(col)` | `freshness(updated_at) < 2h` |
| Volume | `row_count` | `row_count between 1000 and 100000` |
| Anomaly | `anomaly detection` | `anomaly detection for row_count` |
| Custom SQL | `failed_rows` | SQL returning failing rows |

## Common Mistakes

### Wrong

```yaml
# Using Soda v3 checks language for new projects
checks for orders:
  - row_count between 50000 and 60000  # too rigid, v3 syntax
```

### Correct (Soda 4.0)

```yaml
# Use Data Contracts syntax with extensible check types
dataset: orders
source: my_warehouse
columns:
  - name: order_id
    checks:
      - type: missing
        fail: when count > 0
      - type: duplicate
        fail: when count > 0
checks:
  - type: metric
    metric: row_count
    fail: when value < 100
```

### Also Correct (v3 for existing projects)

```yaml
# Anomaly detection learns normal patterns automatically
checks for orders:
  - anomaly detection for row_count:
      severity_level: 3  # standard deviations
```

## Related

- [quality-dimensions](../concepts/quality-dimensions.md)
- [great-expectations](../patterns/great-expectations.md)
