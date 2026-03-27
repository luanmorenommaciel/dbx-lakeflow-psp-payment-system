# Cross-Dialect Patterns

> **Purpose**: DuckDB, Snowflake, BigQuery, Spark SQL syntax differences and translation patterns
> **MCP Validated**: 2026-03-26 | Updated with DuckDB 1.3+, ASOF JOIN, time_bucket patterns

## When to Use

- Migrating queries between data platforms
- Writing portable SQL that works across warehouses
- Translating Snowflake-specific syntax to BigQuery or vice versa
- Leveraging DuckDB-specific features (QUALIFY, STRUCT, list comprehensions)

## Implementation

```sql
-- === DATE/TIME FUNCTIONS ===

-- Date truncation
-- Snowflake:  DATE_TRUNC('MONTH', order_date)
-- BigQuery:   DATE_TRUNC(order_date, MONTH)
-- DuckDB:     DATE_TRUNC('MONTH', order_date)
-- Spark SQL:  DATE_TRUNC('MONTH', order_date)  -- or TRUNC(order_date, 'MM')

-- Date difference
-- Snowflake:  DATEDIFF('DAY', start_date, end_date)
-- BigQuery:   DATE_DIFF(end_date, start_date, DAY)
-- DuckDB:     DATE_DIFF('DAY', start_date, end_date)
-- Spark SQL:  DATEDIFF(end_date, start_date)

-- Current timestamp
-- Snowflake:  CURRENT_TIMESTAMP()
-- BigQuery:   CURRENT_TIMESTAMP()
-- DuckDB:     NOW() or CURRENT_TIMESTAMP
-- Spark SQL:  CURRENT_TIMESTAMP()


-- === STRING FUNCTIONS ===

-- String aggregation
-- Snowflake:  LISTAGG(col, ', ') WITHIN GROUP (ORDER BY col)
-- BigQuery:   STRING_AGG(col, ', ' ORDER BY col)
-- DuckDB:     STRING_AGG(col, ', ' ORDER BY col)
-- Spark SQL:  COLLECT_LIST(col)  -- returns array, not string


-- === ARRAY OPERATIONS ===

-- Array creation
-- Snowflake:  ARRAY_CONSTRUCT(1, 2, 3)
-- BigQuery:   [1, 2, 3]
-- DuckDB:     [1, 2, 3]
-- Spark SQL:  ARRAY(1, 2, 3)

-- Array unnest/flatten
-- Snowflake:  SELECT f.VALUE FROM table, LATERAL FLATTEN(array_col) f
-- BigQuery:   SELECT val FROM table, UNNEST(array_col) AS val
-- DuckDB:     SELECT UNNEST(array_col) FROM table
-- Spark SQL:  SELECT EXPLODE(array_col) FROM table


-- === QUALIFY (filter on window functions) ===
-- Supported: Snowflake, BigQuery, DuckDB, Databricks SQL
-- NOT supported: PostgreSQL, MySQL, standard Spark SQL

-- DuckDB/Snowflake/BigQuery:
SELECT * FROM orders
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) = 1;

-- PostgreSQL/Spark workaround:
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rn
    FROM orders
) sub WHERE rn = 1;


-- === STRUCT / SEMI-STRUCTURED ===

-- Struct access
-- Snowflake:  col:key::STRING  (variant) or col['key']
-- BigQuery:   col.key
-- DuckDB:     col.key  or  col['key']
-- Spark SQL:  col.key

-- JSON parsing
-- Snowflake:  PARSE_JSON('{"a":1}'):a::INT
-- BigQuery:   JSON_VALUE('{"a":1}', '$.a')
-- DuckDB:     '{"a":1}'::JSON->>'a'
-- Spark SQL:  GET_JSON_OBJECT('{"a":1}', '$.a')
```

## Configuration

| Feature | Snowflake | BigQuery | DuckDB | Spark SQL |
|---------|-----------|----------|--------|-----------|
| QUALIFY | Yes | Yes | Yes | Databricks only |
| STRUCT type | VARIANT/OBJECT | STRUCT | STRUCT | STRUCT |
| ARRAY type | ARRAY | ARRAY | LIST | ARRAY |
| MERGE | Yes | Yes | Yes (0.10+) | Yes |
| LATERAL JOIN | Yes | Correlated subquery | Yes | No |
| Regex | REGEXP_LIKE | REGEXP_CONTAINS | REGEXP_MATCHES | RLIKE |

## ASOF JOIN (Temporal Lookups)

```sql
-- DuckDB: ASOF JOIN for time-series lookups
-- "What was the exchange rate as of each order timestamp?"
SELECT o.order_id, o.amount, o.order_ts, r.rate
FROM orders o
ASOF JOIN exchange_rates r
    ON o.currency = r.currency
   AND o.order_ts >= r.effective_ts;

-- Snowflake (preview): ASOF JOIN with MATCH_CONDITION
SELECT o.order_id, o.amount, r.rate
FROM orders o
ASOF JOIN exchange_rates r
    MATCH_CONDITION(o.order_ts >= r.effective_ts)
    ON o.currency = r.currency;

-- Manual ASOF (any dialect): LATERAL + LIMIT 1
SELECT o.order_id, o.amount, r.rate
FROM orders o
CROSS JOIN LATERAL (
    SELECT rate FROM exchange_rates r
    WHERE r.currency = o.currency AND r.effective_ts <= o.order_ts
    ORDER BY r.effective_ts DESC LIMIT 1
) r;
```

## Time Bucketing

```sql
-- DuckDB: time_bucket for time-series aggregation
SELECT
    time_bucket(INTERVAL '15 minutes', reading_ts) AS bucket,
    AVG(value) AS avg_value
FROM sensor_readings
GROUP BY bucket
ORDER BY bucket;

-- Snowflake: TIME_SLICE equivalent
SELECT
    TIME_SLICE(reading_ts, 15, 'MINUTE') AS bucket,
    AVG(value) AS avg_value
FROM sensor_readings
GROUP BY bucket;

-- BigQuery: TIMESTAMP_BUCKET
SELECT
    TIMESTAMP_BUCKET(reading_ts, INTERVAL 15 MINUTE) AS bucket,
    AVG(value) AS avg_value
FROM sensor_readings
GROUP BY bucket;
```

## Example Usage

```sql
-- DuckDB-specific power features (1.3+)
-- List comprehension
SELECT [x * 2 FOR x IN [1, 2, 3, 4] IF x > 1];  -- [4, 6, 8]

-- Direct Parquet/CSV query
SELECT * FROM 's3://bucket/data/*.parquet' WHERE year = 2026;

-- COLUMNS expression (dynamic column selection)
SELECT COLUMNS('revenue_.*') FROM quarterly_report;

-- Lambda syntax (1.3+): single arrow deprecated, use parenthesized form
-- Old: list_transform([1,2,3], x -> x + 1)
-- New: list_transform([1,2,3], (x) -> x + 1)
```

## See Also

- [pivot-unpivot](../patterns/pivot-unpivot.md)
- [set-operations](../concepts/set-operations.md)
