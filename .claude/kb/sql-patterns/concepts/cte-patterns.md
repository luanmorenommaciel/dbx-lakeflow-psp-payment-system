# CTE Patterns

> **Purpose**: Recursive CTEs, chained CTEs, materialized CTEs, CTE vs subquery performance
> **Confidence**: 0.95
> **MCP Validated**: 2026-03-26 | Updated with Spark SQL recursive CTE support notes

## Overview

Common Table Expressions (CTEs) organize complex queries into readable, named steps. They are the backbone of analytical SQL. Chained CTEs replace nested subqueries. Recursive CTEs handle hierarchical data. Performance varies by engine — some materialize CTEs, others inline them.

## The Concept

```sql
-- Chained CTEs: each step builds on the previous
WITH raw_orders AS (
    SELECT order_id, customer_id, amount, order_date
    FROM orders
    WHERE order_date >= '2026-01-01'
),
enriched AS (
    SELECT
        o.*,
        c.customer_name,
        c.segment
    FROM raw_orders o
    JOIN customers c ON o.customer_id = c.customer_id
),
aggregated AS (
    SELECT
        segment,
        COUNT(*) AS order_count,
        SUM(amount) AS total_revenue,
        AVG(amount) AS avg_order_value
    FROM enriched
    GROUP BY segment
)
SELECT * FROM aggregated ORDER BY total_revenue DESC;

-- Recursive CTE: org hierarchy traversal
WITH RECURSIVE org_tree AS (
    -- Base case: top-level managers
    SELECT employee_id, name, manager_id, 1 AS depth
    FROM employees WHERE manager_id IS NULL

    UNION ALL

    -- Recursive step: reports
    SELECT e.employee_id, e.name, e.manager_id, t.depth + 1
    FROM employees e
    JOIN org_tree t ON e.manager_id = t.employee_id
    WHERE t.depth < 10  -- safety limit
)
SELECT * FROM org_tree ORDER BY depth, name;
```

## Quick Reference

| Engine | CTE Materialized? | Recursive? | Notes |
|--------|-------------------|-----------|-------|
| PostgreSQL | Sometimes (MATERIALIZED hint) | Yes | `WITH x AS MATERIALIZED (...)` |
| Snowflake | Inlined (optimizer decides) | Yes | No materialization hint |
| BigQuery | Inlined | Yes | Recursive CTEs via `WITH RECURSIVE` |
| DuckDB | Inlined | Yes | Very efficient CTE handling; auto-optimization |
| Spark SQL | Inlined | Limited | Recursive CTEs added in Spark 3.4+ (Databricks) |

## Common Mistakes

### Wrong

```sql
-- CTE referenced multiple times but engine inlines it — double computation
WITH expensive AS (
    SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY 1
)
SELECT * FROM expensive WHERE total > 1000
UNION ALL
SELECT * FROM expensive WHERE total <= 1000
-- Some engines compute expensive twice
```

### Correct

```sql
-- PostgreSQL: force materialization when CTE is reused
WITH expensive AS MATERIALIZED (
    SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY 1
)
SELECT * FROM expensive WHERE total > 1000
UNION ALL
SELECT * FROM expensive WHERE total <= 1000

-- Other engines: use a temp table if the CTE is expensive and reused
```

## Related

- [window-functions](../concepts/window-functions.md)
- [cross-dialect](../patterns/cross-dialect.md)
