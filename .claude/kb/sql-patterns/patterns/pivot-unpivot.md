# Pivot / Unpivot

> **Purpose**: Cross-dialect PIVOT/UNPIVOT for Snowflake, BigQuery, DuckDB, Spark SQL; dynamic pivot
> **MCP Validated**: 2026-03-26

## When to Use

- Converting rows to columns (pivot) for reporting
- Converting columns to rows (unpivot) for normalization
- Creating cross-tab reports from transactional data
- Dynamic pivot where column values are not known in advance

## Implementation

```sql
-- === PIVOT: rows → columns ===

-- Snowflake
SELECT *
FROM monthly_revenue
PIVOT (SUM(revenue) FOR month IN ('Jan', 'Feb', 'Mar', 'Apr'))
    AS p (product, jan_rev, feb_rev, mar_rev, apr_rev);

-- BigQuery
SELECT *
FROM monthly_revenue
PIVOT (SUM(revenue) FOR month IN ('Jan', 'Feb', 'Mar', 'Apr'));

-- DuckDB
PIVOT monthly_revenue
ON month IN ('Jan', 'Feb', 'Mar', 'Apr')
USING SUM(revenue)
GROUP BY product;

-- Spark SQL
SELECT *
FROM monthly_revenue
PIVOT (SUM(revenue) FOR month IN ('Jan', 'Feb', 'Mar', 'Apr'));

-- Manual pivot (any dialect — CASE WHEN)
SELECT
    product,
    SUM(CASE WHEN month = 'Jan' THEN revenue END) AS jan_rev,
    SUM(CASE WHEN month = 'Feb' THEN revenue END) AS feb_rev,
    SUM(CASE WHEN month = 'Mar' THEN revenue END) AS mar_rev
FROM monthly_revenue
GROUP BY product;


-- === UNPIVOT: columns → rows ===

-- Snowflake
SELECT product, month, revenue
FROM quarterly_report
UNPIVOT (revenue FOR month IN (q1_rev, q2_rev, q3_rev, q4_rev));

-- BigQuery
SELECT product, month, revenue
FROM quarterly_report
UNPIVOT (revenue FOR month IN (q1_rev, q2_rev, q3_rev, q4_rev));

-- DuckDB
UNPIVOT quarterly_report
ON q1_rev, q2_rev, q3_rev, q4_rev
INTO NAME month VALUE revenue;

-- Manual unpivot (any dialect — UNION ALL)
SELECT product, 'Q1' AS quarter, q1_rev AS revenue FROM quarterly_report
UNION ALL
SELECT product, 'Q2', q2_rev FROM quarterly_report
UNION ALL
SELECT product, 'Q3', q3_rev FROM quarterly_report
UNION ALL
SELECT product, 'Q4', q4_rev FROM quarterly_report;
```

## Configuration

| Dialect | PIVOT | UNPIVOT | Dynamic PIVOT |
|---------|-------|---------|--------------|
| Snowflake | Native | Native | Via stored procedure |
| BigQuery | Native | Native | Via EXECUTE IMMEDIATE |
| DuckDB | Native (PIVOT ON) | Native (UNPIVOT ON) | Via prepared statement |
| Spark SQL | Native | Manual UNION ALL | Via DataFrame API |
| PostgreSQL | `crosstab()` extension | UNNEST | Via `crosstab()` |

## Example Usage

```sql
-- Dynamic pivot in Snowflake (when values aren't known ahead of time)
-- Step 1: get distinct values
-- Step 2: build and execute dynamic SQL
SET pivot_cols = (
    SELECT LISTAGG(DISTINCT '''' || month || '''', ', ')
    FROM monthly_revenue
);
```

## See Also

- [cross-dialect](../patterns/cross-dialect.md)
- [set-operations](../concepts/set-operations.md)
