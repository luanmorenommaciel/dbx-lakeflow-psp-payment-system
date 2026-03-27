-- Databricks notebook source
-- Entity: Risk and Fraud Operations
-- Layer: Silver L2 - Domain-scoped operational view
-- Upstream: silver.psp_transactions, silver.psp_orders, silver.psp_customers, silver.psp_payment_instruments, silver.psp_disputes, silver.psp_merchants

-- =============================================================================
-- SILVER L2: Risk and Fraud Operations
-- =============================================================================
-- Pre-joined view combining transaction, order, customer, payment instrument,
-- dispute, and merchant data for risk assessment and fraud detection.
-- Includes risk scoring components and fraud indicators.
--
-- Grain: txn_id
-- Consumers: Risk analysts, fraud detection dashboards, compliance team
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.silver.silver_l2_risk_operations
COMMENT "Pre-joined risk view: transactions + orders + customers + payments + disputes + merchants with risk scores and fraud indicators"
TBLPROPERTIES (
    "quality" = "silver",
    "layer" = "silver_l2",
    "domain" = "risk",
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
    t.transaction_hour,
    t.transaction_day_of_week,

    -- 3DS authentication
    t.three_ds_status,
    t.is_3ds_authenticated,

    -- Fee context (for revenue-at-risk)
    t.fees_total_amount,
    t.net_amount,

    -- Order context
    o.order_id,
    o.order_channel,
    o.is_ecommerce_order,
    o.is_high_value_order,
    o.order_size_category,
    o.order_hour,
    o.order_day_of_week,

    -- Merchant context
    m.merchant_id,
    m.legal_name AS merchant_legal_name,
    m.merchant_category_code,
    m.country_code AS merchant_country,
    m.risk_level AS merchant_risk_level,
    m.is_kyb_approved AS is_merchant_kyb_approved,
    m.is_high_risk AS is_merchant_high_risk,

    -- Customer context
    c.customer_id,
    c.customer_type,
    c.is_vip_customer,
    c.is_flagged_customer,
    c.customer_tenure_days,

    -- Payment instrument context
    p.payment_id,
    p.card_brand,
    p.card_bin,
    p.wallet_type,
    p.is_wallet_payment,
    p.payment_first_seen_at,

    -- Dispute context (LEFT JOIN - may be NULL if no dispute)
    d.dispute_id,
    d.dispute_reason_code,
    d.dispute_category,
    d.dispute_stage,
    d.dispute_status,
    d.liability_party,
    d.dispute_amount,
    d.dispute_opened_at,
    d.dispute_closed_at,
    d.dispute_age_days,
    d.is_dispute_closed,
    d.is_dispute_won,
    d.is_dispute_lost,
    d.is_merchant_liable,
    d.is_fraud_dispute,
    d.is_escalated AS is_dispute_escalated,
    d.stage_severity_level AS dispute_severity_level,

    -- Derived: has_dispute flag
    CASE WHEN d.dispute_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_dispute,

    -- Derived: age-since fields
    DATEDIFF(t.transaction_date, c.customer_created_at) AS days_since_customer_created,
    DATEDIFF(t.transaction_date, m.merchant_created_at) AS days_since_merchant_created,
    DATEDIFF(t.transaction_date, p.payment_first_seen_at) AS days_since_payment_first_seen,

    -- Risk score components (same logic as Gold, but at L2 for operational use)
    (
        CASE
            WHEN m.risk_level = 'critical' THEN 30
            WHEN m.risk_level = 'high' THEN 20
            WHEN m.risk_level = 'medium' THEN 10
            ELSE 0
        END +
        CASE WHEN NOT m.is_kyb_approved THEN 10 ELSE 0 END
    ) AS merchant_risk_score,

    (
        CASE
            WHEN c.is_flagged_customer THEN 30
            WHEN c.customer_tenure_days < 7 THEN 20
            WHEN c.customer_tenure_days < 30 THEN 10
            ELSE 0
        END +
        CASE WHEN c.is_vip_customer THEN -10 ELSE 0 END
    ) AS customer_risk_score,

    (
        CASE
            WHEN t.transaction_amount > 1000 THEN 15
            WHEN t.transaction_amount > 500 THEN 10
            WHEN t.transaction_amount > 100 THEN 5
            ELSE 0
        END +
        CASE WHEN NOT t.is_3ds_authenticated THEN 15 ELSE 0 END +
        CASE WHEN t.transaction_hour < 6 OR t.transaction_hour > 23 THEN 10 ELSE 0 END +
        CASE WHEN DATEDIFF(t.transaction_date, p.payment_first_seen_at) < 1 THEN 10 ELSE 0 END
    ) AS transaction_pattern_risk_score,

    -- Operational flags
    CASE WHEN d.is_fraud_dispute THEN TRUE ELSE FALSE END AS confirmed_fraud,
    CASE WHEN d.dispute_id IS NOT NULL AND d.dispute_category = 'fraud_related' THEN TRUE ELSE FALSE END AS suspected_fraud,
    CASE
        WHEN (
            CASE WHEN m.risk_level = 'critical' THEN 30 WHEN m.risk_level = 'high' THEN 20 WHEN m.risk_level = 'medium' THEN 10 ELSE 0 END +
            CASE WHEN NOT m.is_kyb_approved THEN 10 ELSE 0 END +
            CASE WHEN c.is_flagged_customer THEN 30 WHEN c.customer_tenure_days < 7 THEN 20 WHEN c.customer_tenure_days < 30 THEN 10 ELSE 0 END +
            CASE WHEN c.is_vip_customer THEN -10 ELSE 0 END +
            CASE WHEN t.transaction_amount > 1000 THEN 15 WHEN t.transaction_amount > 500 THEN 10 WHEN t.transaction_amount > 100 THEN 5 ELSE 0 END +
            CASE WHEN NOT t.is_3ds_authenticated THEN 15 ELSE 0 END +
            CASE WHEN t.transaction_hour < 6 OR t.transaction_hour > 23 THEN 10 ELSE 0 END +
            CASE WHEN DATEDIFF(t.transaction_date, p.payment_first_seen_at) < 1 THEN 10 ELSE 0 END
        ) >= 50 THEN TRUE
        ELSE FALSE
    END AS high_risk_transaction,
    CASE
        WHEN (
            CASE WHEN m.risk_level = 'critical' THEN 30 WHEN m.risk_level = 'high' THEN 20 WHEN m.risk_level = 'medium' THEN 10 ELSE 0 END +
            CASE WHEN NOT m.is_kyb_approved THEN 10 ELSE 0 END +
            CASE WHEN c.is_flagged_customer THEN 30 WHEN c.customer_tenure_days < 7 THEN 20 WHEN c.customer_tenure_days < 30 THEN 10 ELSE 0 END +
            CASE WHEN c.is_vip_customer THEN -10 ELSE 0 END +
            CASE WHEN t.transaction_amount > 1000 THEN 15 WHEN t.transaction_amount > 500 THEN 10 WHEN t.transaction_amount > 100 THEN 5 ELSE 0 END +
            CASE WHEN NOT t.is_3ds_authenticated THEN 15 ELSE 0 END +
            CASE WHEN t.transaction_hour < 6 OR t.transaction_hour > 23 THEN 10 ELSE 0 END +
            CASE WHEN DATEDIFF(t.transaction_date, p.payment_first_seen_at) < 1 THEN 10 ELSE 0 END
        ) >= 70 THEN TRUE
        ELSE FALSE
    END AS needs_manual_review,

    -- Lineage
    current_timestamp() AS _processed_at

FROM `${catalog}`.silver.psp_transactions t
INNER JOIN `${catalog}`.silver.psp_orders o ON t.order_id = o.order_id
INNER JOIN `${catalog}`.silver.psp_customers c ON o.customer_id = c.customer_id
INNER JOIN `${catalog}`.silver.psp_payment_instruments p ON t.payment_id = p.payment_id
INNER JOIN `${catalog}`.silver.psp_merchants m ON o.merchant_id = m.merchant_id
LEFT JOIN `${catalog}`.silver.psp_disputes d ON t.txn_id = d.txn_id;
