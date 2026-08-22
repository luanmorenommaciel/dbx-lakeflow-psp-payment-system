# Genie Code instructions

Use `workspace.dbx_agentic_dev.gold_merchant_risk` as the primary table and the two quarantine tables as
supporting evidence. Risk is deterministic: every 50 quarantined transaction rows add one quality-risk point
(rounded up), chargebacks add 20 points, and the total is capped at 100. `quality_risk_score` is the baseline
component and `chargeback_risk_points` makes the replay delta inspectable. Do not infer fraud guilt; describe
only observed data-quality and dispute evidence.
