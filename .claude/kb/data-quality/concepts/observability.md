# Observability

> **Purpose**: Freshness monitoring, volume tracking, distribution drift, metadata-driven anomaly detection
> **Confidence**: 0.90
> **MCP Validated**: 2026-03-26

## Overview

Data observability extends monitoring beyond "did the pipeline succeed?" to "is the data correct?" The five pillars are **freshness**, **volume**, **schema**, **distribution**, and **lineage**. Modern observability tools (Monte Carlo, Elementary, Metaplane, Anomalo, Bigeye) detect anomalies automatically using statistical models over metadata -- no manual threshold configuration required.

**2025-2026 trends:**
- **Gartner predicts 50% of enterprises** with distributed data architectures will adopt data observability by 2026 (up from <20% in 2024)
- **AI-powered anomaly detection** is becoming standard -- Monte Carlo (ML-based), Soda 4.0 (AI observability), Anomalo (automated profiling)
- **dbt-native observability** via Elementary (OSS) is gaining traction as a cost-effective alternative
- **Platform-native observability** emerging: Databricks Unity Catalog lineage, Snowflake data quality monitoring
- **Convergence of quality + observability** -- tools combining proactive checks (quality) with reactive monitoring (observability)

**Tool landscape (2026):**
- **Comprehensive**: Monte Carlo, Bigeye, Acceldata
- **Quality-focused**: Soda, Anomalo, Great Expectations
- **Platform-native**: Databricks Unity Catalog, Snowflake
- **dbt-native**: Elementary (OSS), SYNQ (acquired by dbt Labs)
- **Budget-conscious**: Great Expectations (OSS), Elementary (OSS)

## The Concept

```sql
-- Build a basic observability dashboard with SQL
-- Run these checks on a schedule (e.g., hourly via Airflow)

-- 1. FRESHNESS: how old is the newest record?
SELECT
    'orders' AS table_name,
    MAX(updated_at) AS last_record_at,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(updated_at))) / 3600
        AS staleness_hours,
    CASE WHEN MAX(updated_at) < CURRENT_TIMESTAMP - INTERVAL '2 hours'
         THEN 'STALE' ELSE 'FRESH' END AS freshness_status
FROM silver.orders;

-- 2. VOLUME: is row count within expected range?
WITH daily_counts AS (
    SELECT order_date, COUNT(*) AS row_count
    FROM silver.orders
    GROUP BY order_date
    ORDER BY order_date DESC
    LIMIT 30
)
SELECT
    order_date,
    row_count,
    AVG(row_count) OVER (ORDER BY order_date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING)
        AS rolling_avg,
    row_count::FLOAT / NULLIF(AVG(row_count) OVER (
        ORDER BY order_date ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING), 0)
        AS volume_ratio  -- alert if < 0.5 or > 2.0
FROM daily_counts;

-- 3. DISTRIBUTION: detect column-level drift
SELECT
    status,
    COUNT(*) AS current_count,
    COUNT(*)::FLOAT / SUM(COUNT(*)) OVER () AS current_pct
FROM silver.orders
WHERE order_date = CURRENT_DATE
GROUP BY status;
-- Compare against historical percentages to detect drift
```

## Quick Reference

| Pillar | What to Monitor | Alert When | Tool |
|--------|----------------|------------|------|
| Freshness | `MAX(updated_at)` | Exceeds SLA (e.g., 2 hours) | dbt source freshness, Monte Carlo |
| Volume | Daily row count | >2x or <0.5x rolling average | Elementary, custom SQL |
| Schema | Column additions/removals | Any unexpected change | Monte Carlo, dbt on_schema_change |
| Distribution | Value frequency distribution | KL divergence > threshold | Elementary, Great Expectations |
| Lineage | Upstream/downstream impact | Upstream failure detected | Monte Carlo, dbt docs |

## Common Mistakes

### Wrong

```sql
-- Fixed threshold — breaks when business grows or has seasonal patterns
SELECT CASE WHEN COUNT(*) < 10000 THEN 'ALERT' END FROM orders;
```

### Correct

```sql
-- Dynamic threshold based on rolling statistics
SELECT CASE
    WHEN today_count < rolling_avg * 0.5 THEN 'LOW_VOLUME_ALERT'
    WHEN today_count > rolling_avg * 3.0 THEN 'HIGH_VOLUME_ALERT'
    ELSE 'OK'
END FROM volume_stats;
```

## Related

- [quality-dimensions](../concepts/quality-dimensions.md)
- [data-contracts](../concepts/data-contracts.md)
