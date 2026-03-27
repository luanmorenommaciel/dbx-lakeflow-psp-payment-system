# SCD Types

> **Purpose**: Slowly Changing Dimension Types 1-6 — implementation patterns, trade-offs, MERGE SQL
> **Confidence**: 0.95
> **MCP Validated**: 2026-03-26

## Overview

Slowly Changing Dimensions (SCDs) handle changes to dimension attributes over time. Type 1 overwrites, Type 2 tracks history with effective dates, Type 3 stores previous values. Choose based on whether business users need historical context for that attribute.

## The Concept

```sql
-- SCD Type 2: Full history tracking with effective dates
-- Grain: one row = one version of a customer record

CREATE TABLE dim_customer_scd2 (
    customer_sk     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id     VARCHAR(36)   NOT NULL,  -- natural key (not unique here)
    customer_name   VARCHAR(200)  NOT NULL,
    email           VARCHAR(200),
    segment         VARCHAR(50),
    region          VARCHAR(50),
    effective_from  TIMESTAMP     NOT NULL,
    effective_to    TIMESTAMP     NOT NULL DEFAULT '9999-12-31 00:00:00',
    is_current      BOOLEAN       NOT NULL DEFAULT TRUE,
    _loaded_at      TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- MERGE pattern for SCD Type 2 loading
MERGE INTO dim_customer_scd2 AS target
USING staging_customers AS source
ON target.customer_id = source.customer_id AND target.is_current = TRUE
WHEN MATCHED AND (
    target.customer_name != source.customer_name OR
    target.segment != source.segment OR
    target.region != source.region
) THEN UPDATE SET
    effective_to = CURRENT_TIMESTAMP,
    is_current = FALSE
WHEN NOT MATCHED THEN INSERT (
    customer_id, customer_name, email, segment, region,
    effective_from, effective_to, is_current
) VALUES (
    source.customer_id, source.customer_name, source.email,
    source.segment, source.region,
    CURRENT_TIMESTAMP, '9999-12-31 00:00:00', TRUE
);

-- Insert new version for changed records (separate statement)
INSERT INTO dim_customer_scd2 (customer_id, customer_name, email, segment, region, effective_from)
SELECT s.customer_id, s.customer_name, s.email, s.segment, s.region, CURRENT_TIMESTAMP
FROM staging_customers s
JOIN dim_customer_scd2 t ON s.customer_id = t.customer_id
WHERE t.is_current = FALSE AND t.effective_to = CURRENT_TIMESTAMP;
```

## Quick Reference

| Type | Strategy | Storage | Query Complexity | Use When |
|------|----------|---------|-----------------|----------|
| Type 1 | Overwrite | Low | Simple | History not needed (typo fixes) |
| Type 2 | New row + dates | High | Medium (filter is_current) | Full history required |
| Type 3 | Previous column | Low | Simple | Only one prior value needed |
| Type 4 | Separate history table | Medium | JOIN required | Current + history separated |
| Type 6 | Hybrid (1+2+3) | High | Medium | Current + history + previous |

## Common Mistakes

### Wrong

```sql
-- SCD2 without is_current flag — forces date range filtering everywhere
SELECT * FROM dim_customer
WHERE effective_to = '9999-12-31';  -- brittle, sentinel-dependent
```

### Correct

```sql
-- Use is_current boolean for simple current-record queries
SELECT * FROM dim_customer WHERE is_current = TRUE;

-- Use date range for point-in-time queries
SELECT * FROM dim_customer
WHERE effective_from <= @report_date
  AND effective_to > @report_date;
```

## Related

- [dimensional-modeling](dimensional-modeling.md)
- [schema-evolution](schema-evolution.md)
- [snapshot-scd2 pattern](../../dbt/patterns/snapshot-scd2.md)
