# DEFINE: Silver L2 Operational Layer

> Introduce domain-scoped Silver L2 materialized views to eliminate ad-hoc multi-join queries and prevent non-aggregated data from polluting the Gold layer.

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | SILVER_L2_OPERATIONAL_LAYER |
| **Date** | 2026-03-26 |
| **Author** | define-agent |
| **Status** | Ready for Design |
| **Clarity Score** | 15/15 |
| **Input Source** | `BRAINSTORM_SILVER_L2_OPERATIONAL_LAYER.md` (pre-validated) |

---

## Problem Statement

Data analysts repeatedly write complex multi-join queries across 7 Silver L1 entity tables for every operational investigation (e.g., "today's failed transactions with merchant and customer details"), because the single Silver L2 table (`silver_unified_transactions`) is a monolithic 200+ column, 6-way join that's too wide for domain-specific work. The workaround — copying non-aggregated operational views into Gold — pollutes the Gold layer with data that belongs in Silver, blurring Medallion Architecture layer boundaries and creating maintenance confusion about what Gold is supposed to contain.

---

## Target Users

| User | Role | Pain Point |
|------|------|------------|
| Data Analysts | Investigate operational issues, build ad-hoc reports | Write 4-6 table JOIN queries against Silver L1 for every investigation; each query takes 15-30 min to build and validate |
| Operations/Support Team | Monitor daily operations via dashboards | Gold only has aggregated views (customer analytics, merchant performance, risk scores); no pre-joined operational snapshots for triage |
| Downstream Systems | Consume pre-shaped data via reverse ETL or APIs | Need pre-joined, domain-scoped data in the right shape; currently require custom Gold tables that blur layer semantics |

---

## Goals

| Priority | Goal |
|----------|------|
| **MUST** | Create 4 domain-scoped Silver L2 DLT materialized views: settlement ops, customer service, risk operations, merchant operations |
| **MUST** | Each L2 table exposes only 40-60 domain-relevant columns with operational flags baked in |
| **MUST** | Migrate all 3 Gold aggregated views to read from domain-specific L2 tables instead of the monolithic unified table |
| **SHOULD** | Mark `silver_unified_transactions` as deprecated with a clear deprecation notice in table properties |
| **COULD** | Add a `silver_l2` sub-folder under the pipeline source to organize L2 SQL files separately from L1 |

---

## Success Criteria

Measurable outcomes:

- [ ] 4 Silver L2 materialized views deployed and queryable in the DLT pipeline
- [ ] Each L2 table has <= 60 columns (verified via `DESCRIBE TABLE`)
- [ ] Each L2 table includes >= 3 domain-specific operational flags (e.g., `settlement_delayed`, `needs_review`, `high_risk_transaction`)
- [ ] All 3 Gold views (`psp_customer_analytics`, `psp_merchant_performance`, `psp_risk_fraud_monitoring`) successfully read from L2 tables
- [ ] Gold views produce identical output after migration (verified via row count and checksum comparison)
- [ ] `silver_unified_transactions` table properties include `"deprecated" = "true"` and deprecation comment
- [ ] DLT pipeline DAG shows clean lineage: Bronze → Silver L1 → Silver L2 → Gold
- [ ] Zero non-aggregated tables remain in the Gold schema

---

## Acceptance Tests

| ID | Scenario | Given | When | Then |
|----|----------|-------|------|------|
| AT-001 | Settlement ops view serves analyst | Silver L1 tables (transactions, orders, merchants, payouts) are populated | Analyst queries `silver_l2_settlement_ops` for a merchant's daily settlement status | Returns pre-joined rows with transaction amounts, fees, payout status, and `settlement_delayed` flag — no multi-table joins needed |
| AT-002 | Customer service view serves support | Silver L1 tables (transactions, orders, customers, payments) are populated | Support agent queries `silver_l2_customer_service` for a customer's recent transactions | Returns customer context, order details, payment method info, and dispute status in a single query |
| AT-003 | Risk operations view serves fraud team | Silver L1 tables (transactions, disputes, customers, payments) are populated | Risk analyst queries `silver_l2_risk_operations` for high-risk transactions today | Returns transactions with risk flags, fraud indicators, 3DS status, and dispute lifecycle in one view |
| AT-004 | Merchant operations view serves ops | Silver L1 tables (merchants, orders, transactions, payouts) are populated | Ops team queries `silver_l2_merchant_operations` for a merchant's daily health | Returns merchant profile, daily transaction summary, KYB status, and payout reconciliation at merchant+date grain |
| AT-005 | Gold migration produces identical results | Gold views currently read from `silver_unified_transactions` | Gold views are migrated to read from domain-specific L2 tables | Row counts match within 0.1% and key aggregate values (sums, averages) match within rounding tolerance |
| AT-006 | Deprecated table has deprecation markers | `silver_unified_transactions` exists in the pipeline | Table properties are updated | `TBLPROPERTIES` includes `"deprecated" = "true"` and `COMMENT` includes deprecation notice with migration guidance |
| AT-007 | DLT expectations enforce L2 quality | L2 materialized views are created with DLT expectations | A row with NULL `txn_id` flows through L1 | The row is dropped by L2 expectations before reaching the materialized view |

---

## Out of Scope

Explicitly NOT included in this feature:

- **Real-time streaming for L2 tables** — materialized view refresh is sufficient for investigation/dashboard use cases
- **Generic view-builder framework** — 4 fixed domains cover all stated needs; no dynamic view generation
- **Immediate deletion of `silver_unified_transactions`** — deprecate only; removal is a separate future task after all consumers migrate
- **SLA monitoring or alerting infrastructure** — can be layered on top of L2 tables in a future feature
- **Additional L2 domains beyond the 4 identified** — settlement ops, customer service, risk operations, merchant operations cover all current use cases
- **Schema evolution strategy** — handled by DLT's built-in schema evolution; no custom migration tooling needed
- **Access control or row-level security on L2 tables** — follows existing Silver schema permissions

---

## Constraints

| Type | Constraint | Impact |
|------|------------|--------|
| Technical | Must integrate with existing Lakeflow/DLT pipeline using `${catalog}` parameterization | L2 views must use same DLT patterns as L1 tables |
| Technical | L2 tables must be DLT materialized views (not streaming tables) | Materialized views refresh on schedule; no need for streaming semantics at this layer |
| Technical | Must maintain backward compatibility during migration | `silver_unified_transactions` stays in the pipeline (deprecated) until all consumers migrate |
| Technical | Must follow existing Silver L1 conventions: DLT expectations, table properties (`quality`, `domain`, `grain`), lineage columns (`_processed_at`) | Consistency across the Silver layer |
| Technical | L2 SQL files can be SQL or Python notebooks depending on join complexity | Simple joins use SQL; multi-way mixed joins may require Python (like the current unified table) |
| Resource | No additional Databricks infrastructure — L2 tables run within the existing DLT pipeline | No new clusters, no new pipelines — just new tables in the existing `psp-analytics` pipeline |

---

## Technical Context

| Aspect | Value | Notes |
|--------|-------|-------|
| **Deployment Location** | `pipelines/src/psp-analytics/silver/` (L2 files alongside L1) | Optionally `pipelines/src/psp-analytics/silver_l2/` sub-folder |
| **KB Domains** | lakeflow, medallion, spark | DLT materialized views, layer responsibility patterns, join optimization |
| **IaC Impact** | None — new tables within existing DLT pipeline | No infrastructure changes; DLT auto-discovers new notebooks |

**Why This Matters:**
- **Location** → L2 files must be in the DLT pipeline source path for auto-discovery
- **KB Domains** → Design phase should consult Lakeflow patterns for materialized views and Medallion patterns for layer boundaries
- **IaC Impact** → Zero infrastructure overhead — DLT handles compute, scheduling, and lineage automatically

---

## Data Contract

### Source Inventory

| Source | Type | Key Columns | Volume | Freshness | Owner |
|--------|------|-------------|--------|-----------|-------|
| `silver.psp_transactions` | DLT Streaming Table | `txn_id`, `order_id`, `payment_id` | Highest volume — core fact | Streaming | Data Engineering |
| `silver.psp_orders` | DLT Streaming Table | `order_id`, `merchant_id`, `customer_id` | 1:1 with transactions | Streaming | Data Engineering |
| `silver.psp_merchants` | DLT Streaming Table | `merchant_id` | Dimension — low volume | Streaming | Data Engineering |
| `silver.psp_customers` | DLT Streaming Table | `customer_id` | Dimension — medium volume | Streaming | Data Engineering |
| `silver.psp_payment_instruments` | DLT Streaming Table | `payment_id`, `customer_id` | Dimension — medium volume | Streaming | Data Engineering |
| `silver.psp_payouts` | DLT Streaming Table | `payout_id`, `merchant_id` | Batch settlement data | Streaming | Data Engineering |
| `silver.psp_disputes` | DLT Streaming Table | `dispute_id`, `txn_id` | Low volume — sparse | Streaming | Data Engineering |

### L2 Table Contracts

#### `silver_l2_settlement_ops` — Grain: `txn_id, transaction_date`

| Join | Type | Key |
|------|------|-----|
| transactions → orders | INNER | `txn_id → order_id` (via `t.order_id = o.order_id`) |
| orders → merchants | INNER | `o.merchant_id = m.merchant_id` |
| merchants → payouts | LEFT | `m.merchant_id = p.merchant_id AND t.transaction_date = p.payout_batch_date` |

**Column Focus (~50):** Transaction amounts, fees, net revenue, payout status, settlement delays, merchant profile, operational flags (`settlement_delayed`, `large_fee_variance`, `payout_pending`)

#### `silver_l2_customer_service` — Grain: `txn_id`

| Join | Type | Key |
|------|------|-----|
| transactions → orders | INNER | `t.order_id = o.order_id` |
| orders → customers | INNER | `o.customer_id = c.customer_id` |
| transactions → payments | INNER | `t.payment_id = p.payment_id` |
| transactions → disputes | LEFT | `t.txn_id = d.txn_id` |

**Column Focus (~50):** Customer context, order details, payment method, transaction status, dispute status, operational flags (`has_open_dispute`, `is_refund_candidate`, `customer_at_risk`)

#### `silver_l2_risk_operations` — Grain: `txn_id`

| Join | Type | Key |
|------|------|-----|
| transactions → orders | INNER | `t.order_id = o.order_id` |
| orders → customers | INNER | `o.customer_id = c.customer_id` |
| transactions → payments | INNER | `t.payment_id = p.payment_id` |
| transactions → disputes | LEFT | `t.txn_id = d.txn_id` |
| orders → merchants | INNER | `o.merchant_id = m.merchant_id` |

**Column Focus (~55):** Risk scores, fraud indicators, 3DS authentication, dispute lifecycle, customer tenure, merchant risk level, operational flags (`high_risk_transaction`, `needs_manual_review`, `fraud_suspected`)

#### `silver_l2_merchant_operations` — Grain: `merchant_id, transaction_date`

| Join | Type | Key |
|------|------|-----|
| merchants → orders | INNER | `m.merchant_id = o.merchant_id` |
| orders → transactions | INNER | `o.order_id = t.order_id` (aggregated) |
| merchants → payouts | LEFT | `m.merchant_id = p.merchant_id AND transaction_date = p.payout_batch_date` |

**Column Focus (~45):** Merchant profile, daily transaction counts/amounts, KYB health, payout reconciliation, operational flags (`kyb_action_needed`, `high_decline_rate`, `payout_overdue`)

### Freshness SLAs

| Layer | Target | Measurement |
|-------|--------|-------------|
| Silver L1 | Near real-time (streaming) | DLT streaming table ingestion lag |
| Silver L2 | Refreshed within 15 minutes of L1 update | Materialized view refresh cycle |
| Gold | Refreshed within 30 minutes of L2 update | Materialized view refresh cycle |

### Completeness Metrics

- 100% of Silver L1 records with valid join keys appear in the corresponding L2 tables (INNER joins drop orphaned records by design)
- Zero NULL primary keys in any L2 table (enforced via DLT expectations)
- LEFT JOIN columns (disputes, payouts) may be NULL — this is expected and documented

### Lineage Requirements

- DLT automatically tracks table-level lineage: L1 → L2 → Gold
- Each L2 table documents its upstream sources in SQL comments and `TBLPROPERTIES`
- Gold views must reference L2 tables (not L1) after migration — no lineage shortcuts

---

## Assumptions

| ID | Assumption | If Wrong, Impact | Validated? |
|----|------------|------------------|------------|
| A-001 | Silver L1 streaming tables have stable schemas that won't change during L2 implementation | L2 materialized views would break on schema mismatch | [x] — schemas reviewed in brainstorm phase |
| A-002 | DLT materialized views can read from streaming tables in the same pipeline | Would need to use `dlt.read()` instead of direct table reference | [x] — existing `silver_unified_transactions` already does this |
| A-003 | 4 domains cover all current operational use cases | New domains would need additional L2 tables | [x] — user confirmed all 4 are needed equally |
| A-004 | Gold views produce equivalent results when reading from L2 vs. monolithic unified table | Would need to debug join differences or column mismatches | [ ] — will be validated during build with AT-005 |
| A-005 | Existing DLT pipeline has capacity for 4 additional materialized views without performance degradation | Would need to optimize pipeline scheduling or add compute | [ ] — assumed based on current pipeline headroom |

---

## Clarity Score Breakdown

| Element | Score (0-3) | Notes |
|---------|-------------|-------|
| Problem | 3 | Specific: monolithic L2 too wide, analysts write ad-hoc joins, Gold polluted with non-aggregated data |
| Users | 3 | Three distinct personas identified with concrete pain points |
| Goals | 3 | MoSCoW prioritized: 3 MUST, 1 SHOULD, 1 COULD — each measurable |
| Success | 3 | 8 measurable criteria with specific thresholds (column counts, row count matching, etc.) |
| Scope | 3 | 7 explicit exclusions with reasoning; constraints documented with impact |
| **Total** | **15/15** | |

---

## Open Questions

None — ready for Design. All questions were resolved during the Brainstorm phase:
- Domain scope confirmed (4 domains)
- Table types confirmed (DLT materialized views)
- Gold migration approach confirmed (repoint, don't rewrite)
- Deprecation strategy confirmed (mark deprecated, don't delete)

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-03-26 | define-agent | Initial version — extracted from BRAINSTORM_SILVER_L2_OPERATIONAL_LAYER.md |

---

## Next Step

**Ready for:** `/design .claude/sdd/features/DEFINE_SILVER_L2_OPERATIONAL_LAYER.md`
