# DESIGN: Silver L2 Operational Layer

> Technical design for 4 domain-scoped Silver L2 materialized views, Gold migration, and unified table deprecation.

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | SILVER_L2_OPERATIONAL_LAYER |
| **Date** | 2026-03-26 |
| **Author** | design-agent |
| **DEFINE** | [DEFINE_SILVER_L2_OPERATIONAL_LAYER.md](./DEFINE_SILVER_L2_OPERATIONAL_LAYER.md) |
| **Status** | Ready for Build |

---

## Architecture Overview

```text
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                          PSP ANALYTICS DLT PIPELINE (psp-analytics-${target})           │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                         │
│  BRONZE (7 streaming tables)                                                            │
│  ┌──────────┬──────────┬──────────┬──────────┬──────────┬──────────┬──────────┐         │
│  │ txn      │ orders   │merchants │customers │ payments │ payouts  │ disputes │         │
│  └────┬─────┴────┬─────┴────┬─────┴────┬─────┴────┬─────┴────┬─────┴────┬─────┘         │
│       │          │          │          │          │          │          │                 │
│       ▼          ▼          ▼          ▼          ▼          ▼          ▼                 │
│  SILVER L1 (7 streaming tables - entity cleansing)                                      │
│  ┌──────────┬──────────┬──────────┬──────────┬──────────┬──────────┬──────────┐         │
│  │psp_      │psp_      │psp_      │psp_      │psp_      │psp_      │psp_      │         │
│  │transac-  │orders    │merchants │customers │payment_  │payouts   │disputes  │         │
│  │tions     │          │          │          │instrum.  │          │          │         │
│  └──┬───┬───┴──┬───┬───┴──┬───┬───┴──┬───┬───┴──┬───┬───┴──┬───┬───┴──┬───────┘         │
│     │   │      │   │      │   │      │   │      │   │      │   │      │                 │
│     │   │      │   │      │   │      │   │      │   │      │   │      │                 │
│  SILVER L2 (4 materialized views - domain-scoped operational joins)     [NEW]           │
│  ┌───────────────────┬───────────────────┬───────────────────┬──────────────────┐        │
│  │ silver_l2_        │ silver_l2_        │ silver_l2_        │ silver_l2_       │        │
│  │ settlement_ops    │ customer_service  │ risk_operations   │ merchant_ops     │        │
│  │                   │                   │                   │                  │        │
│  │ txn+ord+mer+pay   │ txn+ord+cust+pmt  │ txn+ord+cust+pmt  │ mer+ord+txn+pay  │        │
│  │ grain: txn+date   │ +disp             │ +disp+mer         │ grain: mer+date  │        │
│  │ ~50 cols          │ grain: txn        │ grain: txn        │ ~45 cols         │        │
│  │                   │ ~50 cols          │ ~55 cols          │                  │        │
│  └────────┬──────────┴────────┬──────────┴────────┬──────────┴────────┬─────────┘        │
│           │                   │                   │                   │                  │
│           ▼                   ▼                   ▼                   ▼                  │
│  GOLD (3 materialized views - business aggregations)                  [MIGRATED]        │
│  ┌──────────────────┬──────────────────┬──────────────────┐                              │
│  │ psp_merchant_    │ psp_customer_    │ psp_risk_fraud_  │                              │
│  │ performance      │ analytics        │ monitoring       │                              │
│  │ ◄─ merchant_ops  │ ◄─ cust_service  │ ◄─ risk_ops      │                              │
│  └──────────────────┴──────────────────┴──────────────────┘                              │
│                                                                                         │
│  DEPRECATED                                                                             │
│  ┌──────────────────────────────────────────┐                                           │
│  │ silver_unified_transactions [DEPRECATED] │                                           │
│  │ (retained for backward compat)           │                                           │
│  └──────────────────────────────────────────┘                                           │
│                                                                                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Components

| Component | Purpose | Technology | Status |
|-----------|---------|------------|--------|
| `silver_l2_settlement_ops` | Pre-joined settlement and payout data for finance/settlement analysts | DLT SQL Materialized View | New |
| `silver_l2_customer_service` | Pre-joined customer, order, payment, dispute data for support/CS analysts | DLT SQL Materialized View | New |
| `silver_l2_risk_operations` | Pre-joined risk scoring and fraud detection data for risk/fraud analysts | DLT SQL Materialized View | New |
| `silver_l2_merchant_operations` | Pre-joined merchant daily health and payout reconciliation for ops team | DLT SQL Materialized View | New |
| `psp_merchant_performance` | Daily merchant performance aggregation | DLT SQL Materialized View | Modify (repoint to L2) |
| `psp_customer_analytics` | Customer lifetime value aggregation | DLT SQL Materialized View | Modify (repoint to L2) |
| `psp_risk_fraud_monitoring` | Transaction-level risk scoring | DLT SQL Materialized View | Modify (repoint to L2) |
| `unified_transactions.py` | Monolithic 6-way join (deprecated) | DLT Python Table | Modify (add deprecation) |
| `psp_analytics_pipeline.yml` | DABs pipeline configuration | YAML | Modify (add L2 notebooks) |

---

## Key Decisions

### Decision 1: SQL Materialized Views for All 4 L2 Tables

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-03-26 |

**Context:** The existing `silver_unified_transactions` uses Python because DLT SQL doesn't support multi-way streaming joins with mixed join types. The L2 tables need similar multi-table joins.

**Choice:** Use `CREATE OR REFRESH MATERIALIZED VIEW` (DLT SQL) for all 4 L2 tables, reading from Silver L1 tables directly via catalog references.

**Rationale:** Materialized views in DLT can reference streaming tables by their catalog name (not `STREAM()`), which allows standard SQL multi-way JOINs without the streaming constraint. This is simpler, more readable, and follows the same pattern as existing Gold views (`merchant_performance.sql`, `risk_fraud_monitoring.sql`) which already do multi-table JOINs via catalog references.

**Alternatives Rejected:**
1. Python DLT tables (like `unified_transactions.py`) — more complex, harder to review, and unnecessary since MVs can reference by catalog name
2. Streaming tables with `STREAM()` — streaming-to-streaming multi-way JOINs require Python; overkill for L2 refresh semantics

**Consequences:**
- L2 tables are full-refresh materialized views (not incremental streaming)
- Simpler SQL code, easier for analysts to understand and review
- Consistent with existing Gold layer pattern

---

### Decision 2: L2 Tables in `silver/` Directory (Not a Separate Sub-folder)

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-03-26 |

**Context:** The DEFINE suggested optionally creating a `silver_l2/` sub-folder. However, the current pipeline has `bronze/`, `silver/`, `gold/` directories, and DLT auto-discovers notebooks within the configured paths.

**Choice:** Place L2 SQL files in the existing `pipelines/src/psp-analytics/silver/` directory, prefixed with `l2_` to distinguish from L1 files.

**Rationale:** The pipeline YAML already points to `../src/psp-analytics/silver/` for Silver notebooks. Adding a sub-folder would require either a new path entry in the YAML (management overhead) or restructuring the existing layout (unnecessary change). The `l2_` prefix provides clear visual separation within the directory.

**Alternatives Rejected:**
1. `silver_l2/` sub-folder — requires pipeline YAML changes, splits Silver files across two directories
2. No prefix — ambiguous which files are L1 vs L2

**Consequences:**
- All Silver files in one directory (L1 entity files + L2 domain views)
- Clear naming convention: `l2_settlement_ops.sql`, `l2_customer_service.sql`, etc.
- Pipeline YAML only needs 4 new notebook entries (no path changes)

---

### Decision 3: Gold Views Repoint via FROM Clause Change (Not Rewrite)

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-03-26 |

**Context:** The 3 Gold views currently read from `silver.silver_unified_transactions` or from individual Silver L1 tables. After L2 is deployed, Gold should read from domain-specific L2 tables.

**Choice:** Minimal migration — change only the `FROM` clause and remove any joins that the L2 table already handles. Keep the same Gold output schema (column names, types, aggregation logic).

**Rationale:** Gold views are consumer-facing aggregations. Changing their output schema would break dashboards and downstream consumers. The migration should be invisible to Gold consumers — same output, different internal source.

**Alternatives Rejected:**
1. Rewrite Gold views from scratch — risky, could introduce subtle differences in business logic
2. Keep Gold reading from L1 directly — defeats the purpose of the L2 layer, creates parallel lineage paths

**Consequences:**
- Gold output schema is unchanged (backward compatible)
- Gold SQL becomes simpler (fewer JOINs, reads pre-joined L2 data)
- `customer_analytics` and `risk_fraud_monitoring` migration is straightforward (they read from unified table)
- `merchant_performance` migration requires more care (it currently joins L1 tables directly)

---

### Decision 4: Merchant Operations Table at Aggregated Grain (merchant_id + transaction_date)

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-03-26 |

**Context:** Three of the four L2 tables are at transaction grain (`txn_id`). The merchant operations table serves a different purpose — daily merchant health monitoring — which requires aggregation.

**Choice:** `silver_l2_merchant_operations` will aggregate to `merchant_id + transaction_date` grain, pre-computing daily transaction counts, amounts, success rates, and payout reconciliation.

**Rationale:** At transaction grain, this table would overlap significantly with `silver_l2_settlement_ops` (both involve merchants + transactions). The merchant+date grain serves the ops team's primary query pattern ("show me merchant X's health today") and aligns with the existing Gold `merchant_performance` view, making the Gold migration simpler.

**Alternatives Rejected:**
1. Transaction grain — too much overlap with settlement_ops; doesn't match ops team's mental model
2. Merchant grain only (no date) — loses temporal dimension; can't answer "what happened today?"

**Consequences:**
- Pre-aggregated table is faster for dashboard queries (no GROUP BY at query time)
- Gold `merchant_performance` migration becomes trivial (L2 already has the right grain)
- Column count is lower (~45) because aggregation collapses transaction detail

---

## File Manifest

| # | File | Action | Purpose | Agent | Dependencies |
|---|------|--------|---------|-------|--------------|
| 1 | `pipelines/src/psp-analytics/silver/l2_settlement_ops.sql` | Create | Silver L2: Settlement and payout operational view | @lakeflow-pipeline-builder | None |
| 2 | `pipelines/src/psp-analytics/silver/l2_customer_service.sql` | Create | Silver L2: Customer service operational view | @lakeflow-pipeline-builder | None |
| 3 | `pipelines/src/psp-analytics/silver/l2_risk_operations.sql` | Create | Silver L2: Risk and fraud operational view | @lakeflow-pipeline-builder | None |
| 4 | `pipelines/src/psp-analytics/silver/l2_merchant_operations.sql` | Create | Silver L2: Merchant daily health operational view | @lakeflow-pipeline-builder | None |
| 5 | `pipelines/src/psp-analytics/gold/customer_analytics.sql` | Modify | Repoint to read from `silver_l2_customer_service` | @lakeflow-pipeline-builder | 2 |
| 6 | `pipelines/src/psp-analytics/gold/merchant_performance.sql` | Modify | Repoint to read from `silver_l2_merchant_operations` | @lakeflow-pipeline-builder | 4 |
| 7 | `pipelines/src/psp-analytics/gold/risk_fraud_monitoring.sql` | Modify | Repoint to read from `silver_l2_risk_operations` | @lakeflow-pipeline-builder | 3 |
| 8 | `pipelines/src/psp-analytics/silver/unified_transactions.py` | Modify | Add deprecation notice to table properties and comment | @lakeflow-pipeline-builder | None |
| 9 | `pipelines/resources/psp_analytics_pipeline.yml` | Modify | Register 4 new L2 notebooks in pipeline libraries | @lakeflow-pipeline-builder | 1, 2, 3, 4 |

**Total Files:** 9 (4 new + 5 modified)

---

## Agent Assignment Rationale

| Agent | Files Assigned | Why This Agent |
|-------|----------------|----------------|
| @lakeflow-pipeline-builder | 1, 2, 3, 4, 5, 6, 7, 8, 9 | Specializes in DLT pipeline development: materialized views, streaming tables, expectations, DABs configuration. All files are Lakeflow/DLT artifacts. |

**Agent Discovery:**
- Scanned: `.claude/agents/data-engineering/*.md`
- Primary match: `lakeflow-pipeline-builder` — builds Bronze/Silver/Gold DLT pipelines with expectations and DABs config
- Alternative considered: `lakeflow-architect` (design focus) and `lakeflow-expert` (troubleshooting) — but this is implementation, not design or troubleshooting

---

## Code Patterns

### Pattern 1: Silver L2 Materialized View (SQL Template)

```sql
-- Databricks notebook source
-- Entity: {Domain Name}
-- Layer: Silver L2 - Domain-scoped operational view
-- Upstream: Silver L1 tables ({list sources})

CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.silver.silver_l2_{domain_name}
COMMENT "{Description of what this operational view provides}"
TBLPROPERTIES (
    "quality" = "silver",
    "layer" = "silver_l2",
    "domain" = "{domain}",
    "grain" = "{grain columns}"
)
AS
{SELECT with JOINs from Silver L1 tables, domain-scoped columns, operational flags}
```

**Key conventions:**
- Table name: `silver_l2_{domain}` in the `silver` schema
- File name: `l2_{domain}.sql` in the `silver/` directory
- Table property `"layer" = "silver_l2"` distinguishes from L1 tables (`"quality" = "silver"` is shared)
- `COMMENT` describes the operational use case, not just the data content
- No `STREAM()` — reads L1 tables by catalog name for standard SQL JOINs

### Pattern 2: Settlement Ops — Full SQL

```sql
-- Databricks notebook source
-- Entity: Settlement Operations
-- Layer: Silver L2 - Domain-scoped operational view
-- Upstream: silver.psp_transactions, silver.psp_orders, silver.psp_merchants, silver.psp_payouts

CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.silver.silver_l2_settlement_ops
COMMENT "Pre-joined settlement view: transactions + orders + merchants + payouts with operational flags for settlement monitoring"
TBLPROPERTIES (
    "quality" = "silver",
    "layer" = "silver_l2",
    "domain" = "settlement",
    "grain" = "txn_id, transaction_date"
)
AS
SELECT
    -- Transaction core
    t.txn_id,
    t.transaction_date,
    t.transaction_authorized_at,
    t.transaction_state,
    t.transaction_state_category,
    t.transaction_amount,
    t.transaction_currency,
    t.response_code,
    t.response_code_description,
    t.is_successful,
    t.is_failed,
    t.is_declined,

    -- Fee breakdown
    t.fees_total_amount,
    t.network_fee_amount,
    t.effective_fee_rate_pct,
    t.net_amount,
    t.net_amount_cents,

    -- Order context
    o.order_id,
    o.subtotal_amount,
    o.tax_amount,
    o.tip_amount,
    o.total_amount AS order_total_amount,
    o.order_channel,
    o.is_ecommerce_order,

    -- Merchant context
    m.merchant_id,
    m.legal_name AS merchant_legal_name,
    m.merchant_category_code,
    m.country_code AS merchant_country,
    m.pricing_tier AS merchant_pricing_tier,
    m.risk_level AS merchant_risk_level,
    m.is_kyb_approved,
    m.is_high_risk,
    m.is_enterprise,

    -- Payout context (LEFT JOIN - may be NULL)
    p.payout_id,
    p.payout_status,
    p.gross_amount AS payout_gross_amount,
    p.fees_amount AS payout_fees_amount,
    p.net_amount AS payout_net_amount,
    p.settlement_delay_days,
    p.is_payout_completed,
    p.is_payout_failed,
    p.is_payout_in_transit,
    p.payout_paid_at,

    -- Operational flags
    CASE WHEN p.payout_id IS NULL THEN TRUE ELSE FALSE END AS payout_pending,
    CASE WHEN p.settlement_delay_days > 3 THEN TRUE ELSE FALSE END AS settlement_delayed,
    CASE
        WHEN t.effective_fee_rate_pct > 5.0 THEN TRUE
        ELSE FALSE
    END AS large_fee_variance,
    CASE
        WHEN t.is_successful AND p.is_payout_failed THEN TRUE
        ELSE FALSE
    END AS settlement_mismatch,

    -- Lineage
    current_timestamp() AS _processed_at

FROM `${catalog}`.silver.psp_transactions t
INNER JOIN `${catalog}`.silver.psp_orders o ON t.order_id = o.order_id
INNER JOIN `${catalog}`.silver.psp_merchants m ON o.merchant_id = m.merchant_id
LEFT JOIN `${catalog}`.silver.psp_payouts p
    ON m.merchant_id = p.merchant_id
    AND t.transaction_date = p.payout_batch_date;
```

### Pattern 3: Merchant Operations — Aggregated Grain

```sql
-- Databricks notebook source
-- Entity: Merchant Operations
-- Layer: Silver L2 - Domain-scoped operational view
-- Upstream: silver.psp_merchants, silver.psp_orders, silver.psp_transactions, silver.psp_payouts

CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.silver.silver_l2_merchant_operations
COMMENT "Daily merchant health view: profile + daily transaction summary + KYB health + payout reconciliation"
TBLPROPERTIES (
    "quality" = "silver",
    "layer" = "silver_l2",
    "domain" = "merchants",
    "grain" = "merchant_id, transaction_date"
)
AS
WITH daily_txn_summary AS (
    SELECT
        o.merchant_id,
        t.transaction_date,
        COUNT(DISTINCT t.txn_id) AS daily_transaction_count,
        COUNT(DISTINCT o.order_id) AS daily_order_count,
        COUNT(DISTINCT o.customer_id) AS daily_unique_customers,
        SUM(CASE WHEN t.is_successful THEN 1 ELSE 0 END) AS successful_transactions,
        SUM(CASE WHEN t.is_failed THEN 1 ELSE 0 END) AS failed_transactions,
        SUM(CASE WHEN t.is_declined THEN 1 ELSE 0 END) AS declined_transactions,
        ROUND(SUM(CASE WHEN t.is_successful THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS success_rate_pct,
        SUM(o.total_amount) AS daily_gross_revenue,
        AVG(o.total_amount) AS avg_order_value,
        SUM(t.fees_total_amount) AS daily_total_fees,
        SUM(t.net_amount) AS daily_net_revenue
    FROM `${catalog}`.silver.psp_transactions t
    INNER JOIN `${catalog}`.silver.psp_orders o ON t.order_id = o.order_id
    GROUP BY o.merchant_id, t.transaction_date
)
SELECT
    -- Merchant profile
    m.merchant_id,
    m.legal_name AS merchant_legal_name,
    m.merchant_category_code,
    m.country_code AS merchant_country,
    m.kyb_status AS merchant_kyb_status,
    m.pricing_tier AS merchant_pricing_tier,
    m.risk_level AS merchant_risk_level,
    m.is_kyb_approved,
    m.is_high_risk,
    m.is_enterprise,
    m.merchant_created_at,

    -- Daily summary
    d.transaction_date,
    d.daily_transaction_count,
    d.daily_order_count,
    d.daily_unique_customers,
    d.successful_transactions,
    d.failed_transactions,
    d.declined_transactions,
    d.success_rate_pct,
    d.daily_gross_revenue,
    d.avg_order_value,
    d.daily_total_fees,
    d.daily_net_revenue,

    -- Payout reconciliation (LEFT JOIN - may be NULL)
    p.payout_id,
    p.payout_status,
    p.gross_amount AS payout_gross_amount,
    p.net_amount AS payout_net_amount,
    p.settlement_delay_days,
    p.is_payout_completed,
    p.is_payout_failed,

    -- Operational flags
    CASE WHEN m.kyb_status != 'approved' THEN TRUE ELSE FALSE END AS kyb_action_needed,
    CASE WHEN d.success_rate_pct < 85 THEN TRUE ELSE FALSE END AS high_decline_rate,
    CASE
        WHEN p.payout_id IS NULL AND d.daily_gross_revenue > 0 THEN TRUE
        ELSE FALSE
    END AS payout_overdue,
    CASE WHEN m.is_high_risk AND d.daily_gross_revenue > 5000 THEN TRUE ELSE FALSE END AS high_risk_high_volume,

    -- Lineage
    current_timestamp() AS _processed_at

FROM `${catalog}`.silver.psp_merchants m
INNER JOIN daily_txn_summary d ON m.merchant_id = d.merchant_id
LEFT JOIN `${catalog}`.silver.psp_payouts p
    ON m.merchant_id = p.merchant_id
    AND d.transaction_date = p.payout_batch_date;
```

### Pattern 4: Gold Migration — Repoint FROM Clause

```sql
-- BEFORE (reads from monolithic unified table):
FROM `${catalog}`.silver.silver_unified_transactions

-- AFTER (reads from domain-specific L2 table):
FROM `${catalog}`.silver.silver_l2_customer_service
```

The Gold view's SELECT and aggregation logic stays the same — only the data source changes. Any JOINs that L2 already handles are removed from the Gold view.

### Pattern 5: Deprecation Notice

```python
@dlt.table(
    name=f"{catalog}.silver.silver_unified_transactions",
    comment="[DEPRECATED] Use domain-specific L2 tables instead: silver_l2_settlement_ops, silver_l2_customer_service, silver_l2_risk_operations, silver_l2_merchant_operations",
    table_properties={
        "quality": "silver",
        "layer": "silver_l2",
        "grain": "transaction",
        "deprecated": "true",
        "deprecated_date": "2026-03-26",
        "deprecated_migration": "See silver_l2_* tables for domain-scoped replacements",
    },
)
```

---

## Data Flow

```text
1. Bronze ingests raw JSON from landing zone (7 streaming tables)
   │
   ▼
2. Silver L1 cleanses and conforms each entity (7 streaming tables)
   │  - Type casting, NULL validation, normalization
   │  - DLT expectations drop invalid rows
   │
   ▼
3. Silver L2 joins domain-scoped subsets (4 materialized views)   [NEW]
   │  - settlement_ops:     txn ⋈ orders ⋈ merchants ⟕ payouts
   │  - customer_service:   txn ⋈ orders ⋈ customers ⋈ payments ⟕ disputes
   │  - risk_operations:    txn ⋈ orders ⋈ customers ⋈ payments ⟕ disputes ⋈ merchants
   │  - merchant_operations: merchants ⋈ orders ⋈ txn (agg) ⟕ payouts
   │
   ▼
4. Gold aggregates from L2 tables (3 materialized views)         [MIGRATED]
   │  - customer_analytics    ← silver_l2_customer_service
   │  - merchant_performance  ← silver_l2_merchant_operations
   │  - risk_fraud_monitoring ← silver_l2_risk_operations
   │
   ▼
5. Consumers: dashboards, analysts, reverse ETL
```

**Legend:** `⋈` = INNER JOIN, `⟕` = LEFT JOIN

---

## Integration Points

| External System | Integration Type | Notes |
|-----------------|-----------------|-------|
| DLT Pipeline Runtime | DLT notebook auto-discovery | New L2 notebooks registered in `psp_analytics_pipeline.yml` |
| Unity Catalog | `${catalog}` parameterization | L2 tables created in `${catalog}.silver` schema |
| Databricks SQL Warehouses | Direct SQL access | Analysts query L2 tables via SQL endpoints — no config change needed |
| Existing dashboards | Transparent (read Gold) | Gold output schema unchanged — dashboards unaffected |

---

## Testing Strategy

| Test Type | Scope | Method | Coverage Goal |
|-----------|-------|--------|---------------|
| Schema validation | Each L2 table | `DESCRIBE TABLE` — verify column count <= 60, operational flags present | All 4 L2 tables |
| Join correctness | Each L2 table | Sample 100 rows, verify join keys match across source L1 tables | Key paths |
| Gold equivalence | Each Gold view | Compare row counts and aggregate checksums between old (unified) and new (L2) sources | AT-005 |
| Expectation enforcement | L2 quality | Insert test row with NULL `txn_id`, verify it's dropped by DLT expectations | AT-007 |
| Deprecation markers | Unified table | `SHOW TBLPROPERTIES` — verify `deprecated=true` | AT-006 |
| Pipeline integration | Full DAG | Run DLT pipeline update, verify all 4 L2 + 3 Gold tables refresh without errors | End-to-end |

---

## Error Handling

| Error Type | Handling Strategy | Retry? |
|------------|-------------------|--------|
| NULL join keys in L1 data | DLT `EXPECT ... ON VIOLATION DROP ROW` on L2 views — invalid rows never reach L2 | No — data quality enforced at L1/L2 boundary |
| Orphaned records (INNER JOIN drops) | Expected behavior — L2 INNER JOINs exclude unmatched records; this is documented in the data contract | No — by design |
| Payout/dispute NULL columns (LEFT JOIN) | Expected — documented as nullable; operational flags handle the NULL case (e.g., `payout_pending = TRUE` when payout is NULL) | No — by design |
| DLT pipeline refresh failure | Existing pipeline notification config sends email to `psp-data-team@company.com` on update/flow failure | Yes — DLT auto-retry |

---

## Pipeline Architecture

### DAG Diagram

```text
[Landing Zone JSON]
        │
        ▼
┌─── Bronze (7 streaming) ───┐
│ txn  ord  mer  cus  pmt  po  dis │
└──────────┬─────────────────┘
           ▼
┌─── Silver L1 (7 streaming) ───┐
│ txn  ord  mer  cus  pmt  po  dis │
└──┬───┬───┬───┬───┬───┬───┬──┘
   │   │   │   │   │   │   │
   ▼   ▼   ▼   ▼   ▼   ▼   ▼
┌─── Silver L2 (4 materialized) ──────────────┐
│ settlement_ops  customer_svc  risk_ops  merchant_ops │
└──────┬──────────┬─────────┬──────────┬──┘
       │          │         │          │
       ▼          ▼         ▼          ▼
┌─── Gold (3 materialized) ──────────────────┐
│ merchant_perf   cust_analytics   risk_fraud │
└────────────────────────────────────────────┘

DEPRECATED: silver_unified_transactions (retained, no Gold consumers)
```

### Refresh Strategy

| Table | Type | Refresh |
|-------|------|---------|
| Silver L2 (all 4) | Materialized View | Full refresh on each pipeline update — DLT handles scheduling |
| Gold (all 3) | Materialized View | Full refresh after L2 completes — DLT resolves dependency order |

### Data Quality Gates

| Gate | Layer | Threshold | Action on Failure |
|------|-------|-----------|-------------------|
| NULL primary keys (txn_id, merchant_id) | L2 | 0 nulls | DLT drops row (expectation) |
| Join key orphans | L2 | Expected — INNER JOINs exclude | Logged by DLT event log |
| Column count | L2 | <= 60 per table | Manual validation during build |
| Gold row count delta (pre/post migration) | Gold | < 0.1% variance | Block migration if exceeded |

---

## Observability

| Aspect | Implementation |
|--------|----------------|
| Lineage | DLT auto-tracks: L1 → L2 → Gold visible in pipeline DAG UI |
| Data quality | DLT event log captures expectation violations per table |
| Refresh timing | DLT update history shows refresh duration and row counts |
| Deprecation awareness | `SHOW TBLPROPERTIES silver_unified_transactions` returns `deprecated=true` |

---

## Security Considerations

- L2 tables inherit Unity Catalog permissions from the `silver` schema — no additional grants needed
- PII handling: customer `email_hash` and `phone_hash` are pre-hashed at Silver L1 — L2 passes through hashes, never raw PII
- Card data: `card_last4_masked` (format `****-1234`) is the only card detail in L2 — BIN and full last4 are not included in customer_service or risk_operations views
- No new external access — all data stays within the existing `${catalog}` Unity Catalog

---

## Configuration

| Config Key | Type | Default | Description |
|------------|------|---------|-------------|
| `catalog` | string | `${var.catalog}` | Unity Catalog name — passed via DABs variables |
| `landing_volume` | string | `${var.landing_volume}` | Unchanged — L2 doesn't read from landing zone |

No new configuration required. L2 tables use the same `${catalog}` parameter as all other pipeline tables.

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-03-26 | design-agent | Initial version |

---

## Next Step

**Ready for:** `/build .claude/sdd/features/DESIGN_SILVER_L2_OPERATIONAL_LAYER.md`
