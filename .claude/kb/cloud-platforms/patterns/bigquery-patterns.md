# BigQuery Implementation Patterns

> **Purpose:** Production patterns for scheduled queries, BQML model workflows, remote functions, and AI.GENERATE with Gemini
> **Confidence:** 0.90
> **MCP Validated:** 2026-03-26

## Overview

BigQuery patterns for production data engineering: scheduled queries for recurring transformations, BQML end-to-end model lifecycle (train, evaluate, predict), remote functions for calling Cloud Functions from SQL, and AI.GENERATE for LLM-powered transformations using Gemini directly in queries.

## The Pattern

### Scheduled Queries (Recurring Transformations)

```sql
-- Daily aggregation scheduled query
-- Config: Schedule = "every 24 hours", Destination = analytics.daily_metrics
-- Uses scripting with run_date parameter

DECLARE run_date DATE DEFAULT @run_date;

-- Idempotent: delete then insert for the target partition
DELETE FROM `project.analytics.daily_revenue`
WHERE revenue_date = run_date;

INSERT INTO `project.analytics.daily_revenue`
SELECT
  DATE(order_timestamp) AS revenue_date,
  region,
  product_category,
  COUNT(DISTINCT order_id) AS order_count,
  COUNT(DISTINCT customer_id) AS customer_count,
  SUM(order_total) AS gross_revenue,
  SUM(IF(refund_id IS NOT NULL, order_total, 0)) AS refunded_amount,
  SUM(order_total) - SUM(IF(refund_id IS NOT NULL, order_total, 0)) AS net_revenue
FROM `project.raw.orders`
WHERE DATE(order_timestamp) = run_date
GROUP BY 1, 2, 3;
```

### BQML: Full Model Lifecycle

```sql
-- Step 1: Create a time series forecasting model
CREATE OR REPLACE MODEL `analytics.models.demand_forecast`
OPTIONS(
  model_type = 'ARIMA_PLUS',
  time_series_timestamp_col = 'order_date',
  time_series_data_col = 'daily_orders',
  time_series_id_col = 'product_category',
  auto_arima = TRUE,
  holiday_region = 'US'
) AS
SELECT
  DATE(order_timestamp) AS order_date,
  product_category,
  COUNT(*) AS daily_orders
FROM `project.raw.orders`
WHERE DATE(order_timestamp) BETWEEN '2024-01-01' AND '2025-12-31'
GROUP BY 1, 2;

-- Step 2: Evaluate model fit
SELECT *
FROM ML.EVALUATE(MODEL `analytics.models.demand_forecast`);

-- Step 3: Forecast next 30 days
SELECT *
FROM ML.FORECAST(
  MODEL `analytics.models.demand_forecast`,
  STRUCT(30 AS horizon, 0.95 AS confidence_level)
);

-- Step 4: Explain predictions (feature attribution)
SELECT *
FROM ML.EXPLAIN_FORECAST(
  MODEL `analytics.models.demand_forecast`,
  STRUCT(30 AS horizon)
);
```

### Remote Functions (Cloud Functions Integration)

```sql
-- Step 1: Create connection to Cloud Functions
CREATE OR REPLACE CONNECTION `project.us.cf_connection`
  OPTIONS(type = 'CLOUD_RESOURCE', location = 'us');

-- Step 2: Create remote function pointing to a Cloud Function
CREATE OR REPLACE FUNCTION `analytics.functions.geocode_address`(address STRING)
RETURNS STRUCT<lat FLOAT64, lng FLOAT64, formatted STRING>
REMOTE WITH CONNECTION `project.us.cf_connection`
OPTIONS(
  endpoint = 'https://us-central1-project.cloudfunctions.net/geocode',
  max_batching_rows = 100
);

-- Step 3: Use in queries
SELECT
  store_id,
  address,
  analytics.functions.geocode_address(address).lat AS latitude,
  analytics.functions.geocode_address(address).lng AS longitude
FROM `project.raw.stores`
WHERE latitude IS NULL;
```

### AI.GENERATE with Gemini (LLM in SQL)

```sql
-- Create remote model connection to Vertex AI
CREATE OR REPLACE MODEL `analytics.models.gemini_flash`
  REMOTE WITH CONNECTION `project.us.vertex_connection`
  OPTIONS(ENDPOINT = 'gemini-2.0-flash');

-- Classify and extract entities from support tickets
SELECT
  ticket_id,
  ticket_text,
  AI.GENERATE(
    MODEL `analytics.models.gemini_flash`,
    CONCAT(
      'Analyze this support ticket. Return JSON with fields: ',
      'category (billing/technical/account/shipping), ',
      'urgency (low/medium/high), ',
      'sentiment (positive/neutral/negative). ',
      'Ticket: ', ticket_text
    ),
    STRUCT(0.1 AS temperature, 256 AS max_output_tokens)
  ).result AS ai_analysis
FROM `project.raw.support_tickets`
WHERE created_date = CURRENT_DATE() - 1;

-- Generate embeddings for semantic search
SELECT
  doc_id,
  content,
  ML.GENERATE_EMBEDDING(
    MODEL `analytics.models.text_embedding`,
    (SELECT content AS content),
    STRUCT(TRUE AS flatten_json_output)
  ).text_embedding AS embedding
FROM `project.knowledge.documents`;
```

## Quick Reference

| Pattern | Use When | Key Detail |
|---------|----------|------------|
| Scheduled query | Recurring batch transforms | Idempotent delete-insert |
| BQML ARIMA_PLUS | Time series forecasting | Auto-ARIMA with holidays |
| Remote functions | External API calls from SQL | Cloud Functions endpoint |
| AI.GENERATE | LLM inference in queries | Vertex AI connection required |
| ML.GENERATE_EMBEDDING | Vector creation from text | For semantic search / RAG |

## Common Mistakes

### Wrong
```sql
-- Using on-demand queries for large recurring transforms (expensive)
-- Running full table scan every hour without partitioning
SELECT * FROM `project.raw.orders` WHERE order_status = 'completed';
```

### Correct
```sql
-- Partition by date and filter on partition column
SELECT * FROM `project.raw.orders`
WHERE DATE(order_timestamp) = CURRENT_DATE() - 1
  AND order_status = 'completed';
```

## Related

- [BigQuery AI](../concepts/bigquery-ai.md) -- BQML, AI.GENERATE, BigLake concepts
- [Cross-Platform Patterns](../concepts/cross-platform-patterns.md) -- SQL dialect differences
- [Cost Optimization](cost-optimization.md) -- Slot reservations and query cost control
