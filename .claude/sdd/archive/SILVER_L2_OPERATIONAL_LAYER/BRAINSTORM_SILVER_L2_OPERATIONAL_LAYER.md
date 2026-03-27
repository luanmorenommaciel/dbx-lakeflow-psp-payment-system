# BRAINSTORM: Silver L2 Operational Layer

> Exploratory session to clarify intent and approach before requirements capture

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | SILVER_L2_OPERATIONAL_LAYER |
| **Date** | 2026-03-26 |
| **Author** | brainstorm-agent |
| **Status** | Ready for Define |

---

## Initial Idea

**Raw Input:** "I want to transform the three-layer Medallion Architecture into a new L2 silver layer. The current problem is that the Data Engineering team needs to copy data from Silver to Gold whenever they need to offer operational views. I want to solve this problem."

**Context Gathered:**
- PSP payment system with 7 Bronze tables, 7 Silver L1 entity tables, 1 monolithic Silver L2 unified join, and 3 Gold aggregated views
- `silver_unified_transactions` already exists as an implicit L2 concept (6-way join, 200+ columns, transaction grain)
- Gold tables (`customer_analytics`, `merchant_performance`, `risk_fraud_monitoring`) are aggregated views — not suited for operational lookups
- Data analysts repeatedly write complex multi-join queries against Silver L1 tables for operational investigations
- The workaround of copying Silver data into Gold pollutes the Gold layer with non-aggregated tables and blurs layer semantics

**Technical Context Observed (for Define):**

| Aspect | Observation | Implication |
|--------|-------------|-------------|
| Likely Location | `pipelines/src/psp-analytics/silver/` | New L2 tables live alongside existing Silver files |
| Relevant KB Domains | lakeflow, medallion, spark | DLT materialized views, layer design patterns |
| IaC Patterns | DLT pipeline with `${catalog}` parameterization | L2 tables follow same DLT/Lakeflow patterns |

---

## Discovery Questions & Answers

| # | Question | Answer | Impact |
|---|----------|--------|--------|
| 1 | What kind of views does the team create when copying Silver to Gold? | Filtered operational snapshots — e.g., "today's failed transactions with merchant and customer details" for operations dashboards | L2 needs pre-joined, filtered views — not aggregated, not monolithic |
| 2 | Who consumes these operational views? | Mix of all: operations/support dashboards, data analysts via SQL, and downstream systems via reverse ETL | L2 must serve multiple access patterns — dashboards, ad-hoc queries, and APIs |
| 3 | Which consumer causes the most pain? | Data analysts writing complex multi-join queries against Silver L1 every investigation | L2 must eliminate the need for ad-hoc multi-table joins |
| 4 | What's wrong with the existing `silver_unified_transactions`? | Too wide/monolithic — analysts need domain-scoped views, not 200+ columns | L2 should be split into domain-specific tables with 40-60 focused columns each |
| 5 | Which operational domains are needed? | All four roughly equally: settlement ops, customer service ops, risk/fraud ops, merchant ops | L2 layer must cover all 4 domains systematically |
| 6 | Any sample data or example queries available? | None — design from existing Silver L1 schemas | Use current Silver entity tables as the source of truth for L2 schema design |

---

## Sample Data Inventory

| Type | Location | Count | Notes |
|------|----------|-------|-------|
| Input files | N/A | - | No sample queries from analysts available |
| Output examples | N/A | - | No expected output schemas provided |
| Ground truth | N/A | - | No verified reference data |
| Related code | `pipelines/src/psp-analytics/silver/unified_transactions.py` | 1 | Existing monolithic L2 — pattern to decompose |
| Related code | `pipelines/src/psp-analytics/gold/*.sql` | 3 | Gold views show downstream consumption patterns |

**How samples will be used:**
- Existing `unified_transactions.py` as the decomposition source — each L2 table is a domain-scoped subset
- Gold view queries reveal which joins and columns each domain actually needs
- Silver L1 schemas define the available columns and data quality constraints

---

## Approaches Explored

### Approach A: Domain-Scoped Silver L2 Materialized Views (4 operational domains) :star: Recommended

**Description:** Create 4 domain-scoped Silver L2 DLT materialized views, each with a focused join strategy and only the columns relevant to that operational domain. Migrate Gold views to read from L2 instead of the monolithic unified table.

```text
Silver L1 (7 entity tables)
    |
    +-- silver_l2_settlement_ops     (transactions + orders + merchants + payouts)
    +-- silver_l2_customer_service    (transactions + orders + customers + payments)
    +-- silver_l2_risk_operations     (transactions + disputes + customers + payments)
    +-- silver_l2_merchant_operations (merchants + orders + transactions + payouts)
         |
         +-- Gold (3 aggregated views -- read from L2 instead of L1)
```

**Pros:**
- Each view has 40-60 columns instead of 200+ — analysts find what they need fast
- Domain ownership is clear — each view maps to a team/use case
- Gold layer stays clean — only aggregations, never raw operational lookups
- Includes operational flags (e.g., `needs_review`, `settlement_delayed`) that analysts currently compute ad-hoc
- Integrates with existing DLT/Lakeflow pipeline

**Cons:**
- Some column overlap between domains (transactions appear in all 4)
- 4 tables to maintain instead of 1
- Gold views need to be repointed from the monolithic table to domain-specific L2 tables

**Why Recommended:** Directly addresses the root cause — analysts get pre-joined, domain-scoped data without polluting Gold. Aligns with how operational teams actually think (by domain, not by one giant table). Each L2 table has a clear owner, clear grain, and clear purpose.

---

### Approach B: Keep Monolithic L2 + Create Gold Operational Views

**Description:** Keep `silver_unified_transactions` as-is. Create new Gold-level "operational views" (non-aggregated) alongside the existing aggregated Gold tables.

**Pros:**
- Minimal change — no new L2 tables, just add filtered Gold views
- Analysts already know the unified table structure

**Cons:**
- Gold layer becomes a mix of aggregated and non-aggregated tables — blurs layer semantics
- Doesn't solve the "too wide" problem — operational views still inherit 200+ columns
- Perpetuates the exact anti-pattern the user wants to eliminate

---

### Approach C: Virtual Layer via Databricks SQL Views (No Physical Tables)

**Description:** Create SQL views (not materialized) in a `silver_l2` schema that dynamically join Silver L1 at query time.

**Pros:**
- Zero storage cost — no data duplication
- Always fresh — reads L1 directly

**Cons:**
- Query performance degrades with complex joins at runtime
- No DLT expectations or data quality enforcement
- Breaks DLT lineage — doesn't integrate with Lakeflow streaming pipeline

---

## Data Engineering Context

### Source Systems

| Source | Type | Volume Estimate | Current Freshness |
|--------|------|-----------------|-------------------|
| Silver L1: psp_transactions | DLT Streaming Table | Core entity — highest volume | Streaming (near real-time) |
| Silver L1: psp_orders | DLT Streaming Table | 1:1 with transactions | Streaming |
| Silver L1: psp_merchants | DLT Streaming Table | Dimension — low volume | Streaming |
| Silver L1: psp_customers | DLT Streaming Table | Dimension — medium volume | Streaming |
| Silver L1: psp_payment_instruments | DLT Streaming Table | Dimension — medium volume | Streaming |
| Silver L1: psp_payouts | DLT Streaming Table | Batch settlement data | Streaming |
| Silver L1: psp_disputes | DLT Streaming Table | Low volume — not all txns have disputes | Streaming |

### Data Flow Sketch

```text
Bronze (7 raw tables)
  |
  v
Silver L1 (7 entity-cleansed streaming tables)
  |
  +-- Silver L2: settlement_ops      [txn + order + merchant + payout]
  +-- Silver L2: customer_service     [txn + order + customer + payment]
  +-- Silver L2: risk_operations      [txn + dispute + customer + payment]
  +-- Silver L2: merchant_operations  [merchant + order + txn + payout]
  |
  v
Gold (3 aggregated materialized views -- migrated to read from L2)
  +-- customer_analytics       <-- reads from silver_l2_customer_service
  +-- merchant_performance     <-- reads from silver_l2_merchant_operations
  +-- risk_fraud_monitoring    <-- reads from silver_l2_risk_operations
```

### Key Data Questions Explored

| # | Question | Answer | Impact |
|---|----------|--------|--------|
| 1 | What grains do the L2 tables need? | Transaction grain for 3 (settlement, customer service, risk) and merchant+date for merchant ops | Each table has a clear, documented grain |
| 2 | Should L2 tables be streaming tables or materialized views? | Materialized views — read from L1 streaming tables but refresh as MV for query performance | DLT handles refresh scheduling automatically |
| 3 | Should Gold be migrated to read from L2? | Yes — Gold should read from domain-scoped L2 tables instead of the monolithic unified table | Gold migration is part of the implementation scope |

---

## Selected Approach

| Attribute | Value |
|-----------|-------|
| **Chosen** | Approach A: Domain-Scoped Silver L2 Materialized Views |
| **User Confirmation** | 2026-03-26 |
| **Reasoning** | Directly solves root cause, clean layer boundaries, domain-scoped design matches operational team structure |

---

## Key Decisions Made

| # | Decision | Rationale | Alternative Rejected |
|---|----------|-----------|----------------------|
| 1 | 4 domain-scoped L2 tables instead of 1 monolithic table | Analysts need focused 40-60 column views, not 200+ columns | Single wide table (current state) |
| 2 | DLT materialized views (not streaming tables) for L2 | L2 serves investigation/dashboards — batch refresh is sufficient | Streaming tables (unnecessary complexity) |
| 3 | Migrate Gold to read from L2 | Clean lineage: L1 → L2 → Gold, no shortcuts | Gold continues reading from L1 directly |
| 4 | Deprecate `silver_unified_transactions` (not immediate removal) | Gold views depend on it — migrate first, retire later | Immediate deletion (breaks downstream) |
| 5 | Include operational flags in L2 tables | Analysts compute these ad-hoc today — bake them into the views | Raw columns only (forces ad-hoc derivation) |

---

## Features Removed (YAGNI)

| Feature Suggested | Reason Removed | Can Add Later? |
|-------------------|----------------|----------------|
| Real-time streaming for L2 tables | L2 serves analysts doing investigation, not real-time alerting — materialized view refresh is sufficient | Yes |
| Generic "build-your-own-view" framework | Over-engineering — 4 fixed domains covers the stated need | Yes |
| Immediate retirement of `silver_unified_transactions` | Gold views depend on it — migrate Gold first, retire later in a separate phase | Yes |
| Additional L2 tables beyond 4 domains | Not requested — 4 domains cover all stated use cases equally | Yes |
| SLA monitoring or alerting on L2 tables | Out of scope for layer architecture — can be added as a separate concern | Yes |

---

## Incremental Validations

| Section | Presented | User Feedback | Adjusted? |
|---------|-----------|---------------|-----------|
| Problem summary | Yes | Confirmed — accurate capture of the pain point and current state | No |
| L2 domain design (4 tables, grains, joins, column counts) | Yes | Confirmed — structure, grains, and design decisions approved | No |

---

## Suggested Requirements for /define

Based on this brainstorm session, the following should be captured in the DEFINE phase:

### Problem Statement (Draft)

Data analysts repeatedly write complex multi-join queries against Silver L1 entity tables for operational investigations, and the workaround of copying non-aggregated data into Gold pollutes the Gold layer and blurs Medallion Architecture boundaries.

### Target Users (Draft)

| User | Pain Point |
|------|------------|
| Data Analysts | Write complex multi-table joins against Silver L1 for every investigation |
| Operations/Support Team | Need pre-joined operational dashboards but Gold only has aggregations |
| Downstream Systems | Reverse ETL and APIs need pre-joined, filtered data in the right shape |

### Success Criteria (Draft)

- [ ] 4 domain-scoped Silver L2 materialized views deployed in DLT pipeline
- [ ] Each L2 table has 40-60 focused columns (not 200+)
- [ ] Gold views migrated to read from L2 tables instead of monolithic unified table
- [ ] `silver_unified_transactions` marked as deprecated
- [ ] Analysts can query operational data without writing multi-table joins
- [ ] No non-aggregated tables in the Gold layer

### Constraints Identified

- Must integrate with existing Lakeflow/DLT pipeline using `${catalog}` parameterization
- Must use DLT materialized views (not streaming tables) for L2
- Must maintain backward compatibility during migration (deprecate, don't delete)
- Must follow existing Silver L1 patterns (expectations, table properties, lineage columns)

### Out of Scope (Confirmed)

- Real-time streaming for L2 tables
- Generic view-builder framework
- Immediate retirement of `silver_unified_transactions`
- SLA monitoring or alerting infrastructure
- Additional domains beyond the 4 identified

---

## Session Summary

| Metric | Value |
|--------|-------|
| Questions Asked | 6 |
| Approaches Explored | 3 |
| Features Removed (YAGNI) | 5 |
| Validations Completed | 2 |

---

## Next Step

**Ready for:** `/define .claude/sdd/features/BRAINSTORM_SILVER_L2_OPERATIONAL_LAYER.md`
