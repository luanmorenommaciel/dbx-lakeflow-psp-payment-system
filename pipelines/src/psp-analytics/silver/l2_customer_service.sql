-- Databricks notebook source
-- Entity: Customer Service Operations
-- Layer: Silver L2 - Domain-scoped operational view
-- Upstream: silver.psp_transactions, silver.psp_orders, silver.psp_customers, silver.psp_payment_instruments, silver.psp_disputes, silver.psp_merchants

-- =============================================================================
-- SILVER L2: Customer Service Operations
-- =============================================================================
-- Pre-joined view combining transaction, order, customer, payment instrument,
-- and dispute data for customer support and service investigations.
-- Includes operational flags for triage and escalation.
--
-- Grain: txn_id
-- Consumers: Customer service analysts, support dashboards, CRM reverse ETL
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.silver.silver_l2_customer_service
COMMENT "Pre-joined customer service view: transactions + orders + customers + payments + disputes with operational flags for support triage"
TBLPROPERTIES (
    "quality" = "silver",
    "layer" = "silver_l2",
    "domain" = "customer_service",
    "grain" = "txn_id"
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
    t.is_disputed,

    -- Order context
    o.order_id,
    o.subtotal_amount,
    o.tax_amount,
    o.tip_amount,
    o.total_amount AS order_total_amount,
    o.order_channel,
    o.is_ecommerce_order,
    o.has_tip,
    o.is_high_value_order,
    o.order_size_category,
    o.order_created_at,
    o.tip_rate,

    -- Merchant context (for Gold customer_analytics aggregation)
    m.merchant_id,
    m.country_code AS merchant_country,

    -- Customer context
    c.customer_id,
    c.customer_type,
    c.email_hash AS customer_email_hash,
    c.phone_hash AS customer_phone_hash,
    c.is_vip_customer,
    c.is_flagged_customer,
    c.customer_tenure_days,
    c.customer_created_at,

    -- Payment instrument context
    p.payment_id,
    p.card_brand,
    p.card_last4_masked,
    p.wallet_type,
    p.payment_status,
    p.is_wallet_payment,
    p.is_expired AS is_payment_expired,
    p.card_network_tier,

    -- 3DS authentication
    t.three_ds_status,
    t.is_3ds_authenticated,

    -- Dispute context (LEFT JOIN - may be NULL if no dispute)
    d.dispute_id,
    d.dispute_reason_code,
    d.dispute_category,
    d.dispute_stage,
    d.dispute_status,
    d.dispute_amount,
    d.dispute_age_days,
    d.is_dispute_closed,
    d.is_dispute_won,
    d.is_dispute_lost,
    d.is_merchant_liable,
    d.is_fraud_dispute,
    d.sla_status AS dispute_sla_status,

    -- Dispute derived flags
    CASE WHEN d.dispute_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_dispute,

    -- Operational flags
    CASE WHEN d.dispute_id IS NOT NULL AND NOT d.is_dispute_closed THEN TRUE ELSE FALSE END AS has_open_dispute,
    CASE
        WHEN t.is_failed AND t.response_code IN ('51', '61') THEN TRUE
        ELSE FALSE
    END AS is_refund_candidate,
    CASE
        WHEN c.is_flagged_customer THEN TRUE
        WHEN c.customer_tenure_days < 7 AND t.is_declined THEN TRUE
        ELSE FALSE
    END AS customer_at_risk,

    -- Lineage
    current_timestamp() AS _processed_at

FROM `${catalog}`.silver.psp_transactions t
INNER JOIN `${catalog}`.silver.psp_orders o ON t.order_id = o.order_id
INNER JOIN `${catalog}`.silver.psp_customers c ON o.customer_id = c.customer_id
INNER JOIN `${catalog}`.silver.psp_payment_instruments p ON t.payment_id = p.payment_id
INNER JOIN `${catalog}`.silver.psp_merchants m ON o.merchant_id = m.merchant_id
LEFT JOIN `${catalog}`.silver.psp_disputes d ON t.txn_id = d.txn_id;
