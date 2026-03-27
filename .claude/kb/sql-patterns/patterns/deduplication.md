# Deduplication

> **Purpose**: Exact dedup (ROW_NUMBER), fuzzy matching, SCD-aware dedup — cross-dialect
> **MCP Validated**: 2026-03-26 | QUALIFY supported in Snowflake, BigQuery, DuckDB, Databricks

## When to Use

- Raw data contains duplicate records from CDC or event replay
- Multiple sources feed the same entity with overlapping keys
- Late-arriving data creates duplicate rows in append-only tables
- Need to keep the most recent version of each record

## Implementation

```sql
-- === EXACT DEDUP (most common) ===
-- Keep latest version of each record by primary key

-- Standard SQL (all dialects)
WITH ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY updated_at DESC, _loaded_at DESC
        ) AS rn
    FROM raw_orders
)
SELECT * EXCEPT(rn) FROM ranked WHERE rn = 1;

-- DuckDB / Snowflake / BigQuery — QUALIFY shorthand
SELECT *
FROM raw_orders
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_id
    ORDER BY updated_at DESC
) = 1;


-- === DEDUP WITH MERGE (incremental) ===
-- Delta Lake / Iceberg — dedup on merge into target
MERGE INTO silver.orders AS t
USING (
    SELECT *
    FROM raw_orders
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY order_id ORDER BY updated_at DESC
    ) = 1
) AS s
ON t.order_id = s.order_id
WHEN MATCHED AND s.updated_at > t.updated_at
    THEN UPDATE SET *
WHEN NOT MATCHED
    THEN INSERT *;


-- === CROSS-SOURCE DEDUP ===
-- Multiple sources, pick highest priority
WITH all_sources AS (
    SELECT *, 1 AS source_priority FROM crm_customers
    UNION ALL
    SELECT *, 2 AS source_priority FROM web_customers
    UNION ALL
    SELECT *, 3 AS source_priority FROM manual_uploads
)
SELECT *
FROM all_sources
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY email
    ORDER BY source_priority ASC, updated_at DESC
) = 1;
```

## Configuration

| Strategy | Use When | Performance |
|----------|----------|-------------|
| `ROW_NUMBER` | Exact key match, keep latest | Fast — single pass with sort |
| `QUALIFY` | Same as ROW_NUMBER, cleaner syntax | Same — syntactic sugar |
| `GROUP BY + MAX` | Simple dedup, no column selection | Fastest — no window function |
| `MERGE` | Incremental dedup into target table | Efficient for append-heavy |

## Example Usage

```sql
-- Quick dedup check: how many duplicates exist?
SELECT order_id, COUNT(*) AS cnt
FROM raw_orders
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 20;
```

## See Also

- [window-functions](../concepts/window-functions.md)
- [gap-and-island](../patterns/gap-and-island.md)
