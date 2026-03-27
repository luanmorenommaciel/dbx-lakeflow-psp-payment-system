-- Databricks notebook source
-- Entity: Settlement Operations
-- Layer: Silver L2 - Domain-scoped operational view
-- Upstream: silver.psp_transactions, silver.psp_orders, silver.psp_merchants, silver.psp_payouts

-- =============================================================================
-- SILVER L2: Settlement and Payout Operations
-- =============================================================================
-- Pre-joined view combining transaction, order, merchant, and payout data
-- for settlement monitoring and reconciliation. Includes operational flags
-- that analysts currently compute ad-hoc.
--
-- Grain: txn_id, transaction_date
-- Consumers: Finance analysts, settlement ops team, reconciliation dashboards
-- =============================================================================

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
    t.processor_name,
    t.is_successful,
    t.is_failed,
    t.is_declined,

    -- Fee breakdown
    t.fees_total_amount,
    t.fees_total_cents,
    t.network_fee_amount,
    t.network_fee_cents,
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
    o.is_high_value_order,

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

    -- Payout context (LEFT JOIN - may be NULL if payout not yet issued)
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
    CASE WHEN t.effective_fee_rate_pct > 5.0 THEN TRUE ELSE FALSE END AS large_fee_variance,
    CASE WHEN t.is_successful AND p.is_payout_failed THEN TRUE ELSE FALSE END AS settlement_mismatch,

    -- Lineage
    current_timestamp() AS _processed_at

FROM `${catalog}`.silver.psp_transactions t
INNER JOIN `${catalog}`.silver.psp_orders o ON t.order_id = o.order_id
INNER JOIN `${catalog}`.silver.psp_merchants m ON o.merchant_id = m.merchant_id
LEFT JOIN `${catalog}`.silver.psp_payouts p
    ON m.merchant_id = p.merchant_id
    AND t.transaction_date = p.payout_batch_date;
