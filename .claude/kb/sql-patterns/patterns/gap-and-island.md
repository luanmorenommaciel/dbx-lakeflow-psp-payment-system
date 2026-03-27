# Gap and Island

> **Purpose**: Date gap detection, session identification, consecutive sequence analysis
> **MCP Validated**: 2026-03-26

## When to Use

- Detecting gaps in date sequences (missing days in daily data)
- Identifying sessions from event streams (activity with time gaps)
- Finding consecutive sequences (streaks, contiguous ranges)
- SLA monitoring — detecting periods of missing data

## Implementation

```sql
-- === DATE GAP DETECTION ===
-- Find missing dates in a daily pipeline
WITH date_spine AS (
    SELECT DATEADD(DAY, seq, '2026-01-01') AS expected_date
    FROM (SELECT ROW_NUMBER() OVER (ORDER BY 1) - 1 AS seq
          FROM TABLE(GENERATOR(ROWCOUNT => 365)))
),
actual_dates AS (
    SELECT DISTINCT order_date FROM orders
)
SELECT d.expected_date AS missing_date
FROM date_spine d
LEFT JOIN actual_dates a ON d.expected_date = a.order_date
WHERE a.order_date IS NULL
ORDER BY missing_date;


-- === ISLAND DETECTION (consecutive groups) ===
-- Find consecutive date ranges per customer
WITH numbered AS (
    SELECT
        customer_id,
        order_date,
        order_date - ROW_NUMBER() OVER (
            PARTITION BY customer_id ORDER BY order_date
        ) * INTERVAL '1 DAY' AS grp
    FROM (SELECT DISTINCT customer_id, order_date FROM orders)
)
SELECT
    customer_id,
    MIN(order_date) AS island_start,
    MAX(order_date) AS island_end,
    COUNT(*)        AS consecutive_days
FROM numbered
GROUP BY customer_id, grp
ORDER BY customer_id, island_start;


-- === SESSION IDENTIFICATION ===
-- Group events into sessions with 30-minute inactivity gap
WITH with_gap AS (
    SELECT *,
        DATEDIFF('SECOND', LAG(event_ts) OVER (
            PARTITION BY user_id ORDER BY event_ts
        ), event_ts) AS gap_seconds
    FROM clickstream
),
with_boundary AS (
    SELECT *,
        CASE WHEN gap_seconds > 1800 OR gap_seconds IS NULL
             THEN 1 ELSE 0 END AS new_session
    FROM with_gap
)
SELECT *,
    SUM(new_session) OVER (
        PARTITION BY user_id ORDER BY event_ts
    ) AS session_id
FROM with_boundary;
```

## Configuration

| Pattern | Key Technique | Dialect Notes |
|---------|--------------|---------------|
| Date gaps | LEFT JOIN on date spine | `GENERATE_SERIES` (Postgres/DuckDB), `GENERATOR` (Snowflake), `UNNEST(GENERATE_DATE_ARRAY(...))` (BigQuery) |
| Islands | `date - ROW_NUMBER()` trick | Universal SQL |
| Sessions | `LAG` + cumulative `SUM` | Universal SQL |

## Example Usage

```sql
-- Quick gap check: are there any missing dates this month?
SELECT COUNT(DISTINCT order_date) AS actual_days,
       DATEDIFF('DAY', '2026-03-01', CURRENT_DATE) AS expected_days
FROM orders
WHERE order_date >= '2026-03-01';
```

## See Also

- [window-functions](../concepts/window-functions.md)
- [deduplication](../patterns/deduplication.md)
