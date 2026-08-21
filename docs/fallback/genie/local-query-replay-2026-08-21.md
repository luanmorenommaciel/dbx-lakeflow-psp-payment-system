# Local query replay — 2026-08-21

Evidence label: `LOCAL_EXPECTED_RESULT / NOT_GENIE_RUNTIME`

Use this only when Genie Code or the SQL warehouse is unavailable. The questions and SQL are the serialized
Genie contract; the values come from the deterministic local E2E rehearsal. They are not generated Genie
answers and do not prove a hosted query.

## Highest-risk merchant

Prompt: Which merchant has the highest risk score, and which observed facts explain it?

```sql
SELECT merchant_id, merchant_name, risk_score, risk_band,
       quarantine_count, brl_rejected_count, chargeback_count
FROM workspace.dbx_agentic_dev.gold_merchant_risk
ORDER BY risk_score DESC, merchant_id
LIMIT 1;
```

Baseline expected result: `m-007`, quarantine count `1209`, BRL rejected count `1204`, chargebacks `0`, risk
score `25`, risk band `normal`.

## Rejected BRL transactions

Prompt: How many BRL transactions were rejected, and which merchants received them?

```sql
SELECT merchant_id, brl_rejected_count
FROM workspace.dbx_agentic_dev.gold_merchant_risk
WHERE brl_rejected_count > 0
ORDER BY brl_rejected_count DESC, merchant_id;
```

Expected total: `1204`.

## Explainable quarantine

Prompt: Summarize the explainable quarantine reasons and their row counts.

```sql
SELECT _errors, count(*) AS affected_rows
FROM workspace.dbx_agentic_dev.quarantine_transactions
GROUP BY _errors
UNION ALL
SELECT _errors, count(*) AS affected_rows
FROM workspace.dbx_agentic_dev.quarantine_disputes
GROUP BY _errors;
```

Expected incident categories: null transaction ID, duplicate transaction ID, non-positive amount, unauthorized
BRL, unknown processor, orphan order, and dispute close before open.

## Late-chargeback delta

Prompt: What changed in merchant risk after the late valid chargeback replay?

```sql
SELECT merchant_id, quality_risk_score, chargeback_risk_points,
       chargeback_count, risk_score, risk_band
FROM workspace.dbx_agentic_dev.gold_merchant_risk
WHERE merchant_id = 'm-007';
```

Local E2E assertions:

```text
baseline: chargebacks=0 risk_score=25 risk_band=normal
replay:   chargebacks=1 risk_score=45 risk_band=elevated
```

Safety language: report observed payment-quality and dispute evidence. Do not infer that the merchant committed
fraud.
