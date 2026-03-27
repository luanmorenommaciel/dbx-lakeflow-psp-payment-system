# Window Functions

> **Purpose**: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, NTILE, running totals, QUALIFY — cross-dialect
> **Confidence**: 0.95
> **MCP Validated**: 2026-03-26 | Updated with QUALIFY support notes and Spark SQL 3.4+ QUALIFY

## Overview

Window functions compute values across a set of rows related to the current row without collapsing them into a single output row. They are essential for ranking, running totals, gap analysis, and deduplication. Syntax is mostly standard SQL, but `QUALIFY` (Snowflake, BigQuery, DuckDB) is a powerful shorthand not available in all dialects.

## The Concept

```sql
-- Core window function anatomy
SELECT
    order_id,
    customer_id,
    amount,
    -- Ranking
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rn,
    RANK()       OVER (PARTITION BY customer_id ORDER BY amount DESC)     AS rnk,
    DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY amount DESC)     AS dense_rnk,
    -- Offset
    LAG(amount, 1)  OVER (PARTITION BY customer_id ORDER BY order_date)  AS prev_amount,
    LEAD(amount, 1) OVER (PARTITION BY customer_id ORDER BY order_date)  AS next_amount,
    -- Aggregation
    SUM(amount) OVER (PARTITION BY customer_id ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)               AS running_total,
    AVG(amount) OVER (PARTITION BY customer_id ORDER BY order_date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)                       AS moving_avg_3
FROM orders
```

## Quick Reference

| Function | Purpose | Frame Required |
|----------|---------|---------------|
| `ROW_NUMBER()` | Unique sequential rank | No |
| `RANK()` | Rank with gaps on ties | No |
| `DENSE_RANK()` | Rank without gaps | No |
| `NTILE(n)` | Distribute into n buckets | No |
| `LAG(col, n)` | Previous nth row value | No |
| `LEAD(col, n)` | Next nth row value | No |
| `SUM/AVG/COUNT` | Running aggregates | Yes — specify ROWS/RANGE |
| `FIRST_VALUE()` | First value in window | Yes |
| `LAST_VALUE()` | Last value in window | Yes — need ROWS BETWEEN |

## Common Mistakes

### Wrong

```sql
-- Dedup without deterministic ordering — non-deterministic results
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY id) AS rn  -- no ORDER BY!
    FROM raw_events
) WHERE rn = 1
```

### Correct

```sql
-- Deterministic dedup with explicit ordering and tiebreaker
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY id ORDER BY updated_at DESC, _loaded_at DESC
    ) AS rn
    FROM raw_events
) WHERE rn = 1

-- Or use QUALIFY (Snowflake/BigQuery/DuckDB — cleaner)
SELECT *
FROM raw_events
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY id ORDER BY updated_at DESC
) = 1
```

## Frame Clause Quick Reference

| Frame | Meaning | Use Case |
|-------|---------|----------|
| `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` | All rows from start to here | Running total |
| `ROWS BETWEEN 2 PRECEDING AND CURRENT ROW` | 3-row sliding window | Moving average |
| `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` | Entire partition | Partition-level aggregate |
| `RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW` | Time-based window | 7-day rolling (DuckDB, Snowflake) |

## Related

- [deduplication](../patterns/deduplication.md)
- [gap-and-island](../patterns/gap-and-island.md)
