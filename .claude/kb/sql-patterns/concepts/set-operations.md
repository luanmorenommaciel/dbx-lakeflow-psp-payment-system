# Set Operations

> **Purpose**: UNION/INTERSECT/EXCEPT, LATERAL joins, UNNEST/FLATTEN, ASOF JOIN across dialects
> **Confidence**: 0.90
> **MCP Validated**: 2026-03-26 | Updated with ASOF JOIN cross-references

## Overview

Set operations combine result sets vertically (UNION, INTERSECT, EXCEPT), while LATERAL joins and UNNEST/FLATTEN handle array/struct expansion. Syntax varies significantly across Snowflake, BigQuery, DuckDB, and Spark SQL — especially for semi-structured data operations.

## The Concept

```sql
-- UNION ALL (keep duplicates) vs UNION (deduplicate)
SELECT order_id, amount FROM online_orders
UNION ALL
SELECT order_id, amount FROM store_orders;

-- INTERSECT — rows in both sets
SELECT customer_id FROM web_customers
INTERSECT
SELECT customer_id FROM mobile_customers;

-- EXCEPT — rows in first but not second
SELECT customer_id FROM all_customers
EXCEPT
SELECT customer_id FROM churned_customers;

-- LATERAL JOIN — correlated subquery as a table
-- PostgreSQL / DuckDB / Snowflake
SELECT c.customer_id, recent.order_id, recent.amount
FROM customers c
CROSS JOIN LATERAL (
    SELECT order_id, amount
    FROM orders o
    WHERE o.customer_id = c.customer_id
    ORDER BY order_date DESC
    LIMIT 3
) recent;
```

## Quick Reference

| Operation | Snowflake | BigQuery | DuckDB | Spark SQL |
|-----------|-----------|----------|--------|-----------|
| `UNION ALL` | Standard | Standard | Standard | Standard |
| `INTERSECT` | Standard | Standard | Standard | Standard |
| `EXCEPT` | Standard | `EXCEPT DISTINCT` | Standard | Standard |
| `LATERAL JOIN` | `LATERAL` | Subquery in SELECT | `LATERAL` | Not supported |
| Array unnest | `FLATTEN(col)` | `UNNEST(col)` | `UNNEST(col)` | `explode(col)` |
| Struct access | `col:key` | `col.key` | `col.key` | `col.key` |

## Common Mistakes

### Wrong

```sql
-- UNION when you mean UNION ALL — unnecessary dedup sort
SELECT * FROM table_a
UNION          -- sorts + deduplicates — expensive!
SELECT * FROM table_b
```

### Correct

```sql
-- UNION ALL when duplicates are acceptable or impossible
SELECT * FROM table_a
UNION ALL      -- no sort, no dedup — much faster
SELECT * FROM table_b
```

## ASOF JOIN (Temporal Lookup)

```sql
-- DuckDB: find most recent exchange rate for each order
SELECT o.order_id, o.amount, r.rate
FROM orders o
ASOF JOIN exchange_rates r
    ON o.currency = r.currency
   AND o.order_ts >= r.effective_ts;
-- Returns the r row with the largest effective_ts <= o.order_ts
```

| Dialect | ASOF JOIN Support | Alternative |
|---------|-------------------|-------------|
| DuckDB | Native `ASOF JOIN` | -- |
| Snowflake | Preview | LATERAL + ORDER BY + LIMIT 1 |
| BigQuery | No | Correlated subquery or window function |
| Spark SQL | No | Window function with `LAST_VALUE` |

## Related

- [cte-patterns](../concepts/cte-patterns.md)
- [cross-dialect](../patterns/cross-dialect.md)
