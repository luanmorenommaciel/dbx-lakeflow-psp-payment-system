"""Merchant-grain risk evidence used by Genie Code."""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame


@dp.materialized_view(
    comment="Merchant risk view combining approved payments, chargebacks, and explainable quarantine evidence"
)
def gold_merchant_risk() -> DataFrame:
    return spark.sql(
        """
        WITH transaction_metrics AS (
          SELECT merchant_id,
                 count(txn_id) AS approved_transaction_count,
                 sum(amount_cents) AS approved_amount_cents,
                 max(event_ts) AS last_transaction_at
          FROM silver_transactions
          GROUP BY merchant_id
        ), chargeback_metrics AS (
          SELECT t.merchant_id, count(d.dispute_id) AS chargeback_count
          FROM silver_disputes d
          LEFT JOIN silver_transactions t USING (txn_id)
          WHERE d.status = 'chargeback'
          GROUP BY t.merchant_id
        ), quarantine_metrics AS (
          SELECT merchant_id,
                 count(*) AS quarantine_count,
                 sum(CASE WHEN _incident_id = 'currency-brl-unauthorized' THEN 1 ELSE 0 END)
                   AS brl_rejected_count,
                 max(_ingested_at) AS last_incident_at
          FROM quarantine_transactions
          GROUP BY merchant_id
        ), scored AS (
          SELECT m.*,
                 coalesce(t.approved_transaction_count, 0) AS approved_transaction_count,
                 coalesce(t.approved_amount_cents, 0) AS approved_amount_cents,
                 t.last_transaction_at,
                 coalesce(c.chargeback_count, 0) AS chargeback_count,
                 coalesce(q.quarantine_count, 0) AS quarantine_count,
                 coalesce(q.brl_rejected_count, 0) AS brl_rejected_count,
                 q.last_incident_at,
                 cast(ceil(coalesce(q.quarantine_count, 0) / 50.0) AS INT)
                   AS quality_risk_score,
                 coalesce(c.chargeback_count, 0) * 20 AS chargeback_risk_points,
                 least(
                   100,
                   coalesce(c.chargeback_count, 0) * 20
                     + cast(ceil(coalesce(q.quarantine_count, 0) / 50.0) AS INT)
                 )
                   AS risk_score
          FROM silver_merchants m
          LEFT JOIN transaction_metrics t USING (merchant_id)
          LEFT JOIN chargeback_metrics c USING (merchant_id)
          LEFT JOIN quarantine_metrics q USING (merchant_id)
        )
        SELECT *, CASE
          WHEN risk_score >= 70 THEN 'critical'
          WHEN risk_score >= 30 THEN 'elevated'
          ELSE 'normal'
        END AS risk_band
        FROM scored
        """
    )
