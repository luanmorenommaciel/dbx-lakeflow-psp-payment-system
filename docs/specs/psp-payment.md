# Payment quality product specification

Build a small payment-quality product that turns deterministic JSON events into explainable merchant risk.
The learner owns the product implementation; the repository supplies governance, tests, recovery, and fallback
data.

## Business outcome

An operations analyst must be able to find rejected payment events, understand why each row was rejected, and
see whether a valid late chargeback changes a merchant's risk. The bounded demonstration merchant is `m-007`:
its risk must change from 25 (`normal`) to 45 (`elevated`) after the valid replay.

## Input contract

Generate two deterministic batches with seed `22082026` and only four entities:

| Entity | Required identity and relationship |
|---|---|
| merchants | `merchant_id`; 12 rows |
| orders | `order_id`, `merchant_id`; 100,000 rows |
| transactions | `txn_id`, `order_id`, `merchant_id`; exactly 100,000 rows across the complete story |
| disputes | `dispute_id`, `txn_id`; 102 rows after replay |

Valid payment currencies are `USD`, `GBP`, `CAD`, and `AUD`. `BRL` is intentionally unauthorized for this
product and must be retained as explained quarantine evidence.

The baseline contains 98,791 valid transactions plus 1,209 incident transactions. Seven invalid conditions must
be retained, not dropped:

1. null transaction ID;
2. duplicate transaction ID;
3. non-positive amount;
4. unauthorized BRL currency;
5. unknown processor;
6. orphan order;
7. dispute close timestamp before its open timestamp.

The eighth deterministic incident, `late-valid-chargeback`, is valid data held for a later incremental replay.

## Product contract

- Use dbldatagen for the seeded synthetic builder.
- Use one Lakeflow Spark Declarative Pipelines graph and the current `pyspark.pipelines` API.
- Bronze must use Auto Loader, retain file/batch lineage, configure schema evolution, and retain rescued data.
- DQX is the sole domain-quality rule source. Valid rows continue to Silver; invalid rows remain queryable with
  `_errors` and `_warnings`.
- Gold stays merchant-grain and exposes approved totals, chargebacks, quarantine counts, risk score, and band.
- Deploy only the `dev` target with Databricks Asset Bundles to the learner's own Free Edition workspace.
- Unity Catalog tables and lineage must be inspectable.
- Genie Code must answer the four questions defined by the learner in module 06. A reusable Genie Space is an
  optional instructor enhancement, not a completion dependency.

## Evidence boundaries

Local tests and an SDP dry-run prove code and graph construction. Bundle validation proves configuration.
Deployment proves resources exist. Hosted queries prove processed data. Instructor replay is fallback evidence
unless it was produced live in the learner's own workspace.
