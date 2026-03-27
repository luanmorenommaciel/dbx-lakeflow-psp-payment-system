-- Databricks notebook source
-- Entity: Merchant Performance
-- Layer: Gold - Business Aggregations
-- Upstream: silver.silver_l2_merchant_operations, silver.psp_payouts

-- =============================================================================
-- GOLD: Daily Merchant Performance
-- =============================================================================
-- Aggregated daily merchant metrics combining pre-computed L2 merchant
-- operations data with payout settlement aggregation. Provides volume,
-- revenue, fees, approval rates, channel distribution, and payout
-- reconciliation for merchant dashboards.
--
-- Reads from silver_l2_merchant_operations for transaction-side metrics
-- (pre-aggregated at merchant_id + transaction_date grain), and aggregates
-- payouts separately since multiple payouts can exist per merchant per day.
--
-- Grain: merchant_id + transaction_date
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW `${catalog}`.gold.psp_merchant_performance
COMMENT "Daily merchant performance analytics - from Silver L2 merchant operations + payout aggregation"
TBLPROPERTIES (
    "quality" = "gold",
    "domain" = "merchants",
    "grain" = "merchant_id, transaction_date"
)
AS
WITH daily_payouts AS (
    SELECT
        merchant_id,
        payout_batch_date,
        COUNT(DISTINCT payout_id) AS daily_payout_count,
        SUM(gross_amount) AS daily_payout_gross,
        SUM(fees_amount) AS daily_payout_fees,
        SUM(reserve_amount) AS daily_payout_reserves,
        SUM(net_amount) AS daily_payout_net,
        SUM(payout_transaction_count) AS payout_transaction_volume,
        AVG(effective_fee_rate_pct) AS avg_payout_fee_rate_pct,
        SUM(CASE WHEN is_payout_completed THEN 1 ELSE 0 END) AS completed_payouts,
        SUM(CASE WHEN is_payout_failed THEN 1 ELSE 0 END) AS failed_payouts,
        AVG(settlement_delay_days) AS avg_settlement_delay_days

    FROM `${catalog}`.silver.psp_payouts
    GROUP BY merchant_id, payout_batch_date
)

SELECT
    l2.merchant_id,
    l2.merchant_legal_name,
    l2.merchant_category_code,
    l2.merchant_country,
    l2.merchant_kyb_status,
    l2.merchant_pricing_tier,
    l2.merchant_risk_level,
    l2.is_kyb_approved,
    l2.is_high_risk,
    l2.is_enterprise,

    l2.transaction_date,
    DAYOFWEEK(l2.transaction_date) AS day_of_week,
    WEEKOFYEAR(l2.transaction_date) AS week_of_year,
    MONTH(l2.transaction_date) AS month,
    QUARTER(l2.transaction_date) AS quarter,
    YEAR(l2.transaction_date) AS year,

    l2.daily_transaction_count,
    l2.daily_order_count,
    l2.daily_unique_customers,

    l2.successful_transactions,
    l2.failed_transactions,
    l2.declined_transactions,
    l2.success_rate_pct,
    l2.decline_rate_pct,

    l2.daily_gross_revenue,
    l2.avg_order_value,
    l2.min_order_value,
    l2.max_order_value,

    l2.daily_total_fees,
    l2.daily_network_fees,
    l2.avg_fee_rate_pct,
    l2.daily_net_revenue,
    ROUND(
        l2.daily_net_revenue * 100.0 / NULLIF(l2.daily_gross_revenue, 0),
        2
    ) AS net_margin_pct,

    l2.ecommerce_transactions,
    l2.pos_transactions,
    l2.mobile_transactions,
    ROUND(l2.ecommerce_transactions * 100.0 / l2.daily_transaction_count, 2) AS ecommerce_pct,

    l2.authenticated_3ds_count,
    l2.unique_payment_instruments,
    ROUND(
        l2.authenticated_3ds_count * 100.0 / l2.daily_transaction_count,
        2
    ) AS authentication_rate_pct,

    dp.daily_payout_count,
    dp.daily_payout_gross,
    dp.daily_payout_fees,
    dp.daily_payout_reserves,
    dp.daily_payout_net,
    dp.payout_transaction_volume,
    dp.avg_payout_fee_rate_pct,
    dp.completed_payouts,
    dp.failed_payouts,
    dp.avg_settlement_delay_days,

    -- Performance rating
    CASE
        WHEN l2.success_rate_pct >= 95 THEN 'excellent'
        WHEN l2.success_rate_pct >= 90 THEN 'good'
        WHEN l2.success_rate_pct >= 85 THEN 'fair'
        ELSE 'poor'
    END AS performance_rating,

    -- Revenue tier
    CASE
        WHEN l2.daily_gross_revenue >= 10000 THEN 'high'
        WHEN l2.daily_gross_revenue >= 5000 THEN 'medium'
        WHEN l2.daily_gross_revenue >= 1000 THEN 'low'
        ELSE 'minimal'
    END AS revenue_tier,

    current_timestamp() AS gold_created_at

FROM `${catalog}`.silver.silver_l2_merchant_operations l2
LEFT JOIN daily_payouts dp
    ON l2.merchant_id = dp.merchant_id
    AND l2.transaction_date = dp.payout_batch_date;
