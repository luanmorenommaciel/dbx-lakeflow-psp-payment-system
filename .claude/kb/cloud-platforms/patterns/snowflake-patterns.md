# Snowflake Implementation Patterns

> **Purpose:** Production SQL patterns for dynamic tables, tasks/streams CDC, Snowpipe, warehouse sizing, and Cortex AI
> **Confidence:** 0.90
> **MCP Validated:** 2026-03-26

## Overview

Snowflake patterns for production data engineering: declarative pipelines with dynamic tables, change data capture via tasks and streams, continuous ingestion through Snowpipe, compute right-sizing, and AI-powered transformations using Cortex functions.

## The Pattern

### Dynamic Tables (Declarative Pipeline)

```sql
-- Bronze: raw ingestion target
CREATE OR REPLACE DYNAMIC TABLE bronze.raw_events
  TARGET_LAG = '1 minute'
  WAREHOUSE = load_wh
AS
SELECT
  $1:event_id::STRING        AS event_id,
  $1:event_type::STRING      AS event_type,
  $1:user_id::STRING         AS user_id,
  $1:properties::VARIANT     AS properties,
  $1:timestamp::TIMESTAMP_NTZ AS event_ts,
  METADATA$FILENAME           AS source_file,
  CURRENT_TIMESTAMP()         AS loaded_at
FROM @landing_stage/events/;

-- Silver: cleaned and deduplicated
CREATE OR REPLACE DYNAMIC TABLE silver.events
  TARGET_LAG = '5 minutes'
  WAREHOUSE = transform_wh
AS
SELECT *
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY loaded_at DESC) AS rn
  FROM bronze.raw_events
)
WHERE rn = 1 AND event_id IS NOT NULL;

-- Gold: aggregated metrics
CREATE OR REPLACE DYNAMIC TABLE gold.daily_event_metrics
  TARGET_LAG = '1 hour'
  WAREHOUSE = analytics_wh
AS
SELECT
  DATE_TRUNC('day', event_ts)       AS event_date,
  event_type,
  COUNT(*)                           AS event_count,
  COUNT(DISTINCT user_id)            AS unique_users,
  AVG(properties:duration::FLOAT)    AS avg_duration
FROM silver.events
GROUP BY 1, 2;
```

### Tasks + Streams (CDC Pattern)

```sql
-- Create stream on source table to capture changes
CREATE OR REPLACE STREAM orders_stream ON TABLE raw.orders
  APPEND_ONLY = FALSE;

-- Task that processes stream changes every 5 minutes
CREATE OR REPLACE TASK process_order_changes
  WAREHOUSE = task_wh
  SCHEDULE = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('orders_stream')
AS
MERGE INTO curated.orders t
USING orders_stream s ON t.order_id = s.order_id
WHEN MATCHED AND s.METADATA$ACTION = 'DELETE' THEN DELETE
WHEN MATCHED AND s.METADATA$ACTION = 'INSERT' THEN
  UPDATE SET t.amount = s.amount, t.status = s.status, t.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED AND s.METADATA$ACTION = 'INSERT' THEN
  INSERT (order_id, amount, status, created_at) VALUES (s.order_id, s.amount, s.status, CURRENT_TIMESTAMP());

ALTER TASK process_order_changes RESUME;
```

### Snowpipe (Auto-Ingest)

```sql
-- Create pipe with auto_ingest from S3 notifications
CREATE OR REPLACE PIPE landing.orders_pipe
  AUTO_INGEST = TRUE
  INTEGRATION = 's3_notification_int'
AS
COPY INTO raw.orders_landing
FROM @landing_stage/orders/
FILE_FORMAT = (TYPE = 'PARQUET')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Check pipe status
SELECT SYSTEM$PIPE_STATUS('landing.orders_pipe');
```

### Warehouse Sizing Strategy

```sql
-- Right-size warehouses by workload type
-- Ingestion: small, always-on for Snowpipe serverless
ALTER WAREHOUSE load_wh SET
  WAREHOUSE_SIZE = 'SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 3
  SCALING_POLICY = 'ECONOMY';

-- Transformation: medium, suspend quickly after batch jobs
ALTER WAREHOUSE transform_wh SET
  WAREHOUSE_SIZE = 'MEDIUM'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

-- Analytics: large multi-cluster for concurrent BI queries
ALTER WAREHOUSE analytics_wh SET
  WAREHOUSE_SIZE = 'LARGE'
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 6
  SCALING_POLICY = 'STANDARD';
```

### Cortex AI in SQL

```sql
-- Text completion with Cortex COMPLETE
SELECT
  ticket_id,
  description,
  SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-large2',
    CONCAT('Classify this support ticket into one category ',
           '(billing, technical, account, other): ', description)
  ) AS category
FROM support.tickets
WHERE created_at >= CURRENT_DATE() - 1;

-- Generate embeddings for semantic search
SELECT
  doc_id,
  SNOWFLAKE.CORTEX.EMBED_TEXT_1024('snowflake-arctic-embed-l-v2.0', content) AS embedding
FROM knowledge_base.documents;
```

## Quick Reference

| Pattern | Use When | Key Config |
|---------|----------|------------|
| Dynamic Tables | Declarative multi-hop pipelines | `TARGET_LAG` controls freshness |
| Tasks + Streams | Event-driven CDC processing | `WHEN SYSTEM$STREAM_HAS_DATA` |
| Snowpipe | Continuous file-based ingestion | `AUTO_INGEST = TRUE` |
| Cortex COMPLETE | LLM inference on table data | Model name + prompt in SQL |
| Cortex EMBED | Vector embeddings from text | 1024-dim Arctic Embed model |

## Common Mistakes

### Wrong
```sql
-- Setting TARGET_LAG too aggressively on gold tables (wastes credits)
CREATE DYNAMIC TABLE gold.metrics TARGET_LAG = '1 minute' ...
```

### Correct
```sql
-- Gold tables rarely need sub-hour freshness
CREATE DYNAMIC TABLE gold.metrics TARGET_LAG = '1 hour' ...
```

## Related

- [Snowflake Cortex](../concepts/snowflake-cortex.md) -- Platform capabilities and AI features
- [Cross-Platform Patterns](../concepts/cross-platform-patterns.md) -- SQL dialect portability
- [Cost Optimization](cost-optimization.md) -- Warehouse auto-suspend and sizing formulas
