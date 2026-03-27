-- Databricks notebook source
-- Entity: Risk & Fraud Monitoring
-- Layer: Gold - Business Aggregations
-- Upstream: silver.silver_l2_risk_operations

-- =============================================================================
-- GOLD: Transaction-Level Risk Scoring and Fraud Detection
-- =============================================================================
-- Per-transaction risk scoring combining merchant risk, customer risk, and
-- transaction pattern analysis. Produces fraud indicators, composite risk
-- scores, and recommended actions for the operations team.
--
-- Reads from silver_l2_risk_operations which pre-joins transactions, orders,
-- customers, payments, merchants, and disputes with pre-computed risk scores.
--
-- Grain: txn_id
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.gold.psp_risk_fraud_monitoring
COMMENT "Transaction-level risk scoring and fraud detection indicators - from Silver L2 risk operations view"
TBLPROPERTIES (
    "quality" = "gold",
    "domain" = "risk",
    "grain" = "txn_id"
)
AS
WITH risk_calculations AS (
    SELECT
        txn_id,
        transaction_date,
        transaction_authorized_at,
        order_id,

        merchant_id,
        merchant_legal_name,
        merchant_category_code,
        merchant_country,
        merchant_risk_level,
        is_merchant_high_risk,
        is_merchant_kyb_approved,

        customer_id,
        customer_type,
        is_vip_customer,
        is_flagged_customer,
        customer_tenure_days,

        payment_id,
        card_brand,
        card_bin,
        wallet_type,
        is_wallet_payment,

        transaction_amount,
        transaction_currency,
        transaction_state,
        transaction_state_category,
        response_code,
        response_code_description,
        is_successful,
        is_failed,
        is_declined,

        three_ds_status,
        is_3ds_authenticated,

        has_dispute,
        dispute_id,
        dispute_reason_code,
        dispute_category,
        dispute_stage,
        dispute_status,
        liability_party,
        is_dispute_won,
        is_dispute_lost,
        is_merchant_liable,
        is_fraud_dispute,
        is_dispute_escalated,
        dispute_severity_level,
        dispute_amount,
        dispute_opened_at,
        dispute_closed_at,
        dispute_age_days,
        is_dispute_closed,

        order_channel,
        is_ecommerce_order,
        order_size_category,
        is_high_value_order,
        order_hour,
        order_day_of_week,

        days_since_customer_created,
        days_since_merchant_created,
        days_since_payment_first_seen,

        fees_total_amount AS transaction_fees,
        net_amount AS merchant_net_revenue,

        -- Merchant risk score component
        (
            CASE
                WHEN merchant_risk_level = 'critical' THEN 30
                WHEN merchant_risk_level = 'high' THEN 20
                WHEN merchant_risk_level = 'medium' THEN 10
                ELSE 0
            END +
            CASE WHEN NOT is_merchant_kyb_approved THEN 10 ELSE 0 END
        ) AS merchant_risk_score,

        -- Customer risk score component
        (
            CASE
                WHEN is_flagged_customer THEN 30
                WHEN customer_tenure_days < 7 THEN 20
                WHEN customer_tenure_days < 30 THEN 10
                ELSE 0
            END +
            CASE WHEN is_vip_customer THEN -10 ELSE 0 END
        ) AS customer_risk_score,

        -- Transaction pattern risk score component
        (
            CASE
                WHEN transaction_amount > 1000 THEN 15
                WHEN transaction_amount > 500 THEN 10
                WHEN transaction_amount > 100 THEN 5
                ELSE 0
            END +
            CASE WHEN NOT is_3ds_authenticated THEN 15 ELSE 0 END +
            CASE WHEN order_hour < 6 OR order_hour > 23 THEN 10 ELSE 0 END +
            CASE WHEN days_since_payment_first_seen < 1 THEN 10 ELSE 0 END
        ) AS transaction_pattern_risk_score,

        -- Fraud indicators
        CASE WHEN is_fraud_dispute THEN TRUE ELSE FALSE END AS confirmed_fraud,
        CASE WHEN has_dispute AND dispute_category = 'fraud_related' THEN TRUE ELSE FALSE END AS suspected_fraud,
        CASE WHEN is_flagged_customer AND is_declined THEN TRUE ELSE FALSE END AS flagged_customer_decline,
        CASE WHEN NOT is_3ds_authenticated AND transaction_amount > 200 THEN TRUE ELSE FALSE END AS high_value_no_auth,
        CASE WHEN days_since_payment_first_seen = 0 AND transaction_amount > 100 THEN TRUE ELSE FALSE END AS new_card_high_value,
        CASE WHEN days_since_customer_created < 1 AND transaction_amount > 200 THEN TRUE ELSE FALSE END AS new_customer_high_value,
        CASE WHEN (order_hour < 2 OR order_hour > 23) AND transaction_amount > 300 THEN TRUE ELSE FALSE END AS late_night_high_value

    FROM `${catalog}`.silver.silver_l2_risk_operations
)

SELECT
    txn_id,
    transaction_date,
    transaction_authorized_at,
    order_id,

    merchant_id,
    merchant_legal_name,
    merchant_category_code,
    merchant_country,
    merchant_risk_level,
    is_merchant_high_risk,

    customer_id,
    customer_type,
    is_vip_customer,
    is_flagged_customer,
    customer_tenure_days,

    payment_id,
    card_brand,
    card_bin,
    wallet_type,
    is_wallet_payment,

    transaction_amount,
    transaction_currency,
    transaction_state,
    transaction_state_category,
    response_code,
    response_code_description,
    is_successful,
    is_failed,
    is_declined,

    three_ds_status,
    is_3ds_authenticated,

    has_dispute,
    dispute_id,
    dispute_reason_code,
    dispute_category,
    dispute_stage,
    dispute_status,
    liability_party,
    is_dispute_won,
    is_dispute_lost,
    is_merchant_liable,
    is_fraud_dispute,
    is_dispute_escalated,
    dispute_severity_level,
    dispute_amount,
    dispute_opened_at,
    dispute_closed_at,
    dispute_age_days,

    order_channel,
    is_ecommerce_order,
    order_size_category,
    is_high_value_order,
    order_hour,
    order_day_of_week,

    days_since_customer_created,
    days_since_merchant_created,
    days_since_payment_first_seen,

    transaction_fees,
    merchant_net_revenue,

    -- Risk scores
    merchant_risk_score,
    customer_risk_score,
    transaction_pattern_risk_score,

    LEAST(
        merchant_risk_score + customer_risk_score + transaction_pattern_risk_score,
        100
    ) AS total_risk_score,

    CASE
        WHEN LEAST(merchant_risk_score + customer_risk_score + transaction_pattern_risk_score, 100) >= 70 THEN 'critical'
        WHEN LEAST(merchant_risk_score + customer_risk_score + transaction_pattern_risk_score, 100) >= 50 THEN 'high'
        WHEN LEAST(merchant_risk_score + customer_risk_score + transaction_pattern_risk_score, 100) >= 30 THEN 'medium'
        ELSE 'low'
    END AS risk_classification,

    -- Fraud indicators
    confirmed_fraud,
    suspected_fraud,
    flagged_customer_decline,
    high_value_no_auth,
    new_card_high_value,
    new_customer_high_value,
    late_night_high_value,

    (
        CASE WHEN confirmed_fraud THEN 1 ELSE 0 END +
        CASE WHEN suspected_fraud THEN 1 ELSE 0 END +
        CASE WHEN flagged_customer_decline THEN 1 ELSE 0 END +
        CASE WHEN high_value_no_auth THEN 1 ELSE 0 END +
        CASE WHEN new_card_high_value THEN 1 ELSE 0 END +
        CASE WHEN new_customer_high_value THEN 1 ELSE 0 END +
        CASE WHEN late_night_high_value THEN 1 ELSE 0 END
    ) AS fraud_indicator_count,

    -- Recommended action
    CASE
        WHEN confirmed_fraud OR (
            CASE WHEN confirmed_fraud THEN 1 ELSE 0 END +
            CASE WHEN suspected_fraud THEN 1 ELSE 0 END +
            CASE WHEN flagged_customer_decline THEN 1 ELSE 0 END +
            CASE WHEN high_value_no_auth THEN 1 ELSE 0 END +
            CASE WHEN new_card_high_value THEN 1 ELSE 0 END +
            CASE WHEN new_customer_high_value THEN 1 ELSE 0 END +
            CASE WHEN late_night_high_value THEN 1 ELSE 0 END
        ) >= 3 THEN 'block_merchant'
        WHEN LEAST(merchant_risk_score + customer_risk_score + transaction_pattern_risk_score, 100) >= 70 THEN 'manual_review'
        WHEN has_dispute AND NOT is_dispute_closed THEN 'monitor'
        WHEN LEAST(merchant_risk_score + customer_risk_score + transaction_pattern_risk_score, 100) >= 50 THEN 'enhanced_monitoring'
        ELSE 'normal'
    END AS recommended_action,

    current_timestamp() AS gold_created_at

FROM risk_calculations;
