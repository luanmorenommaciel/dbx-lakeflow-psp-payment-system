-- Databricks notebook source
-- Entity: Merchant Operations
-- Layer: Silver L2 - Domain-scoped operational view
-- Upstream: silver.psp_merchants, silver.psp_orders, silver.psp_transactions, silver.psp_payouts

-- =============================================================================
-- SILVER L2: Merchant Daily Health and Operations
-- =============================================================================
-- Pre-aggregated daily merchant view combining merchant profile, transaction
-- summary, and payout reconciliation. Provides daily health metrics with
-- operational flags for merchant monitoring.
--
-- Grain: merchant_id, transaction_date
-- Consumers: Merchant ops team, KYB compliance, payout reconciliation dashboards
-- =============================================================================

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
        ROUND(
            SUM(CASE WHEN t.is_successful THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
            2
        ) AS success_rate_pct,
        ROUND(
            SUM(CASE WHEN t.is_declined THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
            2
        ) AS decline_rate_pct,

        SUM(o.total_amount) AS daily_gross_revenue,
        AVG(o.total_amount) AS avg_order_value,
        MIN(o.total_amount) AS min_order_value,
        MAX(o.total_amount) AS max_order_value,

        SUM(t.fees_total_amount) AS daily_total_fees,
        SUM(t.network_fee_amount) AS daily_network_fees,
        AVG(t.effective_fee_rate_pct) AS avg_fee_rate_pct,
        SUM(t.net_amount) AS daily_net_revenue,

        SUM(CASE WHEN o.is_ecommerce_order THEN 1 ELSE 0 END) AS ecommerce_transactions,
        SUM(CASE WHEN o.order_channel = 'pos' THEN 1 ELSE 0 END) AS pos_transactions,
        SUM(CASE WHEN o.order_channel = 'mobile' THEN 1 ELSE 0 END) AS mobile_transactions,

        SUM(CASE WHEN t.is_3ds_authenticated THEN 1 ELSE 0 END) AS authenticated_3ds_count,
        COUNT(DISTINCT t.payment_id) AS unique_payment_instruments

    FROM `${catalog}`.silver.psp_transactions t
    INNER JOIN `${catalog}`.silver.psp_orders o ON t.order_id = o.order_id
    GROUP BY o.merchant_id, t.transaction_date
)

SELECT
    -- Merchant profile
    m.merchant_id,
    m.legal_name AS merchant_legal_name,
    m.merchant_category_code,
    m.mcc_category,
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
    d.decline_rate_pct,

    -- Revenue
    d.daily_gross_revenue,
    d.avg_order_value,
    d.min_order_value,
    d.max_order_value,
    d.daily_total_fees,
    d.daily_network_fees,
    d.avg_fee_rate_pct,
    d.daily_net_revenue,

    -- Channel distribution
    d.ecommerce_transactions,
    d.pos_transactions,
    d.mobile_transactions,
    d.authenticated_3ds_count,
    d.unique_payment_instruments,

    -- Payout reconciliation (LEFT JOIN - may be NULL if payout not yet issued)
    p.payout_id,
    p.payout_status,
    p.gross_amount AS payout_gross_amount,
    p.fees_amount AS payout_fees_amount,
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
