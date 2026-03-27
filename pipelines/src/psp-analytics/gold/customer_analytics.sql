-- Databricks notebook source
-- Entity: Customer Analytics
-- Layer: Gold - Business Aggregations
-- Upstream: silver.silver_l2_customer_service

-- =============================================================================
-- GOLD: Customer Lifetime Value and Behavioral Analytics
-- =============================================================================
-- Aggregates from the Silver L2 customer service view to produce per-customer
-- lifetime metrics including value segmentation, lifecycle stage, frequency
-- analysis, risk profiling, and a composite health score.
--
-- Reads from silver_l2_customer_service which pre-joins transactions, orders,
-- customers, payments, merchants, and disputes at transaction grain.
--
-- Grain: customer_id
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.gold.psp_customer_analytics
COMMENT "Customer lifetime value and behavioral analytics - aggregated from Silver L2 customer service view"
TBLPROPERTIES (
    "quality" = "gold",
    "domain" = "customers",
    "grain" = "customer_id"
)
AS
WITH customer_transactions AS (
    SELECT
        customer_id,
        customer_type,
        customer_email_hash,
        customer_phone_hash,
        is_vip_customer,
        is_flagged_customer,
        customer_tenure_days,
        customer_created_at,

        COUNT(DISTINCT txn_id) AS lifetime_transaction_count,
        COUNT(DISTINCT order_id) AS lifetime_order_count,
        COUNT(DISTINCT merchant_id) AS unique_merchants_count,
        COUNT(DISTINCT payment_id) AS unique_payment_methods,
        SUM(CASE WHEN is_successful THEN 1 ELSE 0 END) AS successful_transactions,
        SUM(CASE WHEN is_failed THEN 1 ELSE 0 END) AS failed_transactions,
        SUM(CASE WHEN is_declined THEN 1 ELSE 0 END) AS declined_transactions,

        ROUND(
            SUM(CASE WHEN is_successful THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
            2
        ) AS customer_success_rate_pct,

        SUM(transaction_amount) AS lifetime_gross_value,
        SUM(CASE WHEN is_successful THEN transaction_amount ELSE 0 END) AS lifetime_net_value,
        AVG(transaction_amount) AS avg_transaction_amount,
        MIN(transaction_amount) AS min_transaction_amount,
        MAX(transaction_amount) AS max_transaction_amount,
        AVG(order_total_amount) AS avg_order_value,
        SUM(tip_amount) AS lifetime_tips_given,
        AVG(tip_rate) AS avg_tip_rate,
        SUM(CASE WHEN has_tip THEN 1 ELSE 0 END) AS orders_with_tips,

        SUM(CASE WHEN is_ecommerce_order THEN 1 ELSE 0 END) AS ecommerce_orders,
        SUM(CASE WHEN order_channel = 'pos' THEN 1 ELSE 0 END) AS pos_orders,
        SUM(CASE WHEN order_channel = 'mobile' THEN 1 ELSE 0 END) AS mobile_orders,
        SUM(CASE WHEN order_channel = 'ivr' THEN 1 ELSE 0 END) AS ivr_orders,

        SUM(CASE WHEN is_wallet_payment THEN 1 ELSE 0 END) AS wallet_transactions,
        ROUND(
            SUM(CASE WHEN is_wallet_payment THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
            2
        ) AS wallet_usage_pct,

        SUM(CASE WHEN card_brand = 'visa' THEN 1 ELSE 0 END) AS visa_transactions,
        SUM(CASE WHEN card_brand = 'mastercard' THEN 1 ELSE 0 END) AS mastercard_transactions,
        SUM(CASE WHEN card_brand = 'amex' THEN 1 ELSE 0 END) AS amex_transactions,
        SUM(CASE WHEN is_3ds_authenticated THEN 1 ELSE 0 END) AS authenticated_transactions,
        ROUND(
            SUM(CASE WHEN is_3ds_authenticated THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
            2
        ) AS authentication_rate_pct,

        SUM(CASE WHEN has_dispute THEN 1 ELSE 0 END) AS total_disputes,
        SUM(CASE WHEN is_fraud_dispute THEN 1 ELSE 0 END) AS fraud_disputes,
        SUM(dispute_amount) AS total_disputed_amount,

        MIN(transaction_date) AS first_transaction_date,
        MAX(transaction_date) AS last_transaction_date,
        DATEDIFF(MAX(transaction_date), MIN(transaction_date)) AS customer_active_days,
        DATEDIFF(CURRENT_DATE(), MAX(transaction_date)) AS days_since_last_transaction,

        ROUND(
            COUNT(DISTINCT txn_id) * 1.0 / NULLIF(DATEDIFF(MAX(transaction_date), MIN(transaction_date)), 0),
            2
        ) AS avg_transactions_per_day,

        SUM(CASE WHEN is_high_value_order THEN 1 ELSE 0 END) AS high_value_orders,
        ROUND(
            SUM(CASE WHEN is_high_value_order THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
            2
        ) AS high_value_order_pct,

        COUNT(DISTINCT merchant_country) AS countries_transacted,
        COUNT(DISTINCT transaction_currency) AS currencies_used,
        SUM(CASE WHEN DATEDIFF(CURRENT_DATE(), transaction_date) <= 30 THEN 1 ELSE 0 END) AS transactions_last_30d,
        SUM(CASE WHEN DATEDIFF(CURRENT_DATE(), transaction_date) <= 30 THEN transaction_amount ELSE 0 END) AS spend_last_30d,
        SUM(CASE WHEN DATEDIFF(CURRENT_DATE(), transaction_date) <= 90 THEN 1 ELSE 0 END) AS transactions_last_90d,
        SUM(CASE WHEN DATEDIFF(CURRENT_DATE(), transaction_date) <= 90 THEN transaction_amount ELSE 0 END) AS spend_last_90d

    FROM `${catalog}`.silver.silver_l2_customer_service
    GROUP BY
        customer_id,
        customer_type,
        customer_email_hash,
        customer_phone_hash,
        is_vip_customer,
        is_flagged_customer,
        customer_tenure_days,
        customer_created_at
)

SELECT
    customer_id,
    customer_type,
    customer_email_hash,
    customer_phone_hash,
    is_vip_customer,
    is_flagged_customer,
    customer_tenure_days,
    customer_created_at,

    lifetime_transaction_count,
    lifetime_order_count,
    unique_merchants_count,
    unique_payment_methods,

    successful_transactions,
    failed_transactions,
    declined_transactions,
    customer_success_rate_pct,

    lifetime_gross_value,
    lifetime_net_value,
    avg_transaction_amount,
    min_transaction_amount,
    max_transaction_amount,
    avg_order_value,

    lifetime_tips_given,
    avg_tip_rate,
    orders_with_tips,
    ROUND(orders_with_tips * 100.0 / lifetime_order_count, 2) AS tip_frequency_pct,

    ecommerce_orders,
    pos_orders,
    mobile_orders,
    ivr_orders,
    ROUND(ecommerce_orders * 100.0 / lifetime_transaction_count, 2) AS ecommerce_preference_pct,

    wallet_transactions,
    wallet_usage_pct,
    visa_transactions,
    mastercard_transactions,
    amex_transactions,

    authenticated_transactions,
    authentication_rate_pct,
    total_disputes,
    fraud_disputes,
    total_disputed_amount,

    first_transaction_date,
    last_transaction_date,
    customer_active_days,
    days_since_last_transaction,
    avg_transactions_per_day,

    high_value_orders,
    high_value_order_pct,

    countries_transacted,
    currencies_used,

    transactions_last_30d,
    spend_last_30d,
    transactions_last_90d,
    spend_last_90d,

    -- Value segmentation
    CASE
        WHEN lifetime_net_value >= 10000 THEN 'whale'
        WHEN lifetime_net_value >= 5000 THEN 'high_value'
        WHEN lifetime_net_value >= 1000 THEN 'medium_value'
        WHEN lifetime_net_value >= 100 THEN 'low_value'
        ELSE 'minimal'
    END AS value_segment,

    -- Lifecycle stage
    CASE
        WHEN days_since_last_transaction <= 7 THEN 'active'
        WHEN days_since_last_transaction <= 30 THEN 'engaged'
        WHEN days_since_last_transaction <= 90 THEN 'at_risk'
        ELSE 'churned'
    END AS lifecycle_stage,

    -- Frequency segment
    CASE
        WHEN avg_transactions_per_day >= 1.0 THEN 'very_frequent'
        WHEN avg_transactions_per_day >= 0.1 THEN 'frequent'
        WHEN avg_transactions_per_day >= 0.01 THEN 'occasional'
        ELSE 'rare'
    END AS frequency_segment,

    -- Risk profile
    CASE
        WHEN is_flagged_customer THEN 'flagged'
        WHEN fraud_disputes > 0 THEN 'fraud_history'
        WHEN customer_success_rate_pct < 80 THEN 'high_decline_rate'
        WHEN total_disputes > 3 THEN 'dispute_prone'
        ELSE 'normal'
    END AS risk_profile,

    -- Composite health score (0-100)
    ROUND(
        (customer_success_rate_pct * 0.3) +
        (LEAST(avg_transactions_per_day * 100, 100) * 0.2) +
        (CASE WHEN days_since_last_transaction <= 30 THEN 100 ELSE 100 - LEAST(days_since_last_transaction, 100) END * 0.3) +
        (LEAST(lifetime_transaction_count * 2, 100) * 0.2),
        2
    ) AS customer_health_score,

    current_timestamp() AS gold_created_at

FROM customer_transactions;
