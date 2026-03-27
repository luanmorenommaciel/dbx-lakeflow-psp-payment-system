# Databricks notebook source
# Entity: Unified Transactions (6-way join)
# Layer: Silver L2 - Cross-domain enrichment
# Upstream: All Silver L1 tables
#
# This is a Python notebook because DLT SQL does not support
# multi-way streaming joins with mixed join types (INNER + LEFT).

import dlt
from pyspark.sql import functions as F

catalog = spark.conf.get("catalog")


@dlt.table(
    name=f"{catalog}.silver.silver_unified_transactions",
    comment="[DEPRECATED] Monolithic unified transaction table - use domain-specific L2 views instead: silver_l2_settlement_ops, silver_l2_customer_service, silver_l2_risk_operations, silver_l2_merchant_operations",
    table_properties={
        "quality": "silver",
        "layer": "silver_l2",
        "grain": "transaction",
        "deprecated": "true",
        "deprecated_date": "2026-03-26",
        "deprecated_migration": "See silver_l2_* tables for domain-scoped replacements",
        "pipelines.autoOptimize.zOrderCols": "txn_id,transaction_date,merchant_id,customer_id",
    },
)
@dlt.expect_or_drop("valid_txn_id", "txn_id IS NOT NULL")
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_merchant_id", "merchant_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
def silver_unified_transactions():
    """
    Unified transaction table combining all PSP entities with derived business metrics.

    Join Strategy:
    - INNER JOIN: Required entities (orders, merchants, customers, payments)
    - LEFT JOIN: Optional entities (disputes - not all transactions have disputes)

    Streaming Semantics:
    - Uses read_stream() for all Silver L1 tables to maintain end-to-end streaming
    - Streaming-to-batch join for disputes (infrequent updates)
    """

    transactions = dlt.read_stream(f"{catalog}.silver.psp_transactions").alias("t")
    orders = dlt.read_stream(f"{catalog}.silver.psp_orders").alias("o")
    merchants = dlt.read_stream(f"{catalog}.silver.psp_merchants").alias("m")
    customers = dlt.read_stream(f"{catalog}.silver.psp_customers").alias("c")
    payments = dlt.read_stream(f"{catalog}.silver.psp_payment_instruments").alias("p")
    disputes = dlt.read(f"{catalog}.silver.psp_disputes").alias("d")

    # Build the join chain: transactions -> orders -> merchants -> customers -> payments
    txn_orders = transactions.join(
        orders,
        transactions.order_id == orders.order_id,
        how="inner",
    )

    txn_orders_merchants = txn_orders.join(
        merchants,
        F.col("o.merchant_id") == merchants.merchant_id,
        how="inner",
    )

    txn_orders_merchants_customers = txn_orders_merchants.join(
        customers,
        F.col("o.customer_id") == customers.customer_id,
        how="inner",
    )

    txn_full = txn_orders_merchants_customers.join(
        payments,
        F.col("t.payment_id") == payments.payment_id,
        how="inner",
    )

    # LEFT JOIN disputes (not all transactions have disputes)
    txn_complete = txn_full.join(
        disputes,
        F.col("t.txn_id") == disputes.txn_id,
        how="left",
    )

    return txn_complete.select(
        # -- Transaction core --
        F.col("t.txn_id"),
        F.col("t.transaction_state"),
        F.col("t.transaction_state_category"),
        F.col("t.state_timestamp"),
        F.col("t.transaction_amount"),
        F.col("t.amount_cents"),
        F.col("t.transaction_currency"),
        F.col("t.response_code"),
        F.col("t.response_code_description"),
        F.col("t.three_ds_status"),
        F.col("t.is_3ds_authenticated"),
        F.col("t.fees_total_amount"),
        F.col("t.fees_total_cents"),
        F.col("t.network_fee_amount"),
        F.col("t.network_fee_cents"),
        F.col("t.effective_fee_rate_pct"),
        F.col("t.net_amount"),
        F.col("t.net_amount_cents"),
        F.col("t.processor_name"),
        F.col("t.is_successful"),
        F.col("t.is_failed"),
        F.col("t.is_disputed"),
        F.col("t.is_declined"),
        F.col("t.transaction_authorized_at"),
        F.col("t.transaction_date"),
        F.col("t.transaction_hour"),
        F.col("t.transaction_day_of_week"),
        # -- Order --
        F.col("o.order_id"),
        F.col("o.order_currency"),
        F.col("o.subtotal_amount"),
        F.col("o.subtotal_cents"),
        F.col("o.tax_amount"),
        F.col("o.tax_cents"),
        F.col("o.tip_amount"),
        F.col("o.tip_cents"),
        F.col("o.total_amount").alias("order_total_amount"),
        F.col("o.total_amount_cents").alias("order_total_amount_cents"),
        F.col("o.tax_rate"),
        F.col("o.tip_rate"),
        F.col("o.order_channel"),
        F.col("o.is_ecommerce_order"),
        F.col("o.has_tip"),
        F.col("o.is_high_value_order"),
        F.col("o.order_size_category"),
        F.col("o.order_created_at"),
        F.col("o.order_date"),
        F.col("o.order_hour"),
        F.col("o.order_day_of_week"),
        # -- Merchant --
        F.col("m.merchant_id"),
        F.col("m.legal_name").alias("merchant_legal_name"),
        F.col("m.merchant_category_code"),
        F.col("m.country_code").alias("merchant_country"),
        F.col("m.kyb_status").alias("merchant_kyb_status"),
        F.col("m.pricing_tier").alias("merchant_pricing_tier"),
        F.col("m.risk_level").alias("merchant_risk_level"),
        F.col("m.is_kyb_approved").alias("is_merchant_kyb_approved"),
        F.col("m.is_high_risk").alias("is_merchant_high_risk"),
        F.col("m.is_enterprise").alias("is_merchant_enterprise"),
        F.col("m.merchant_created_at"),
        # -- Customer --
        F.col("c.customer_id"),
        F.col("c.email_hash").alias("customer_email_hash"),
        F.col("c.phone_hash").alias("customer_phone_hash"),
        F.col("c.customer_type"),
        F.col("c.is_vip_customer"),
        F.col("c.is_flagged_customer"),
        F.col("c.customer_tenure_days"),
        F.col("c.customer_created_at"),
        # -- Payment instrument --
        F.col("p.payment_id"),
        F.col("p.card_brand"),
        F.col("p.card_bin"),
        F.col("p.card_last4_masked"),
        F.col("p.card_expiry_month"),
        F.col("p.card_expiry_year"),
        F.col("p.wallet_type"),
        F.col("p.payment_status"),
        F.col("p.is_active").alias("is_active_payment"),
        F.col("p.is_wallet_payment"),
        F.col("p.is_expired").alias("is_payment_expired"),
        F.col("p.card_network_tier"),
        F.col("p.payment_first_seen_at"),
        # -- Dispute (LEFT JOIN - nullable) --
        F.col("d.dispute_id"),
        F.col("d.dispute_reason_code"),
        F.col("d.dispute_stage"),
        F.col("d.dispute_category"),
        F.col("d.liability_party"),
        F.col("d.dispute_status"),
        F.col("d.dispute_amount"),
        F.col("d.dispute_amount_cents"),
        F.col("d.dispute_opened_at"),
        F.col("d.dispute_closed_at"),
        F.col("d.dispute_age_days"),
        F.col("d.is_dispute_closed"),
        F.col("d.is_dispute_won"),
        F.col("d.is_dispute_lost"),
        F.col("d.is_merchant_liable"),
        F.col("d.is_fraud_dispute"),
        F.col("d.is_escalated").alias("is_dispute_escalated"),
        F.col("d.stage_severity_level").alias("dispute_severity_level"),
        # -- Derived fields --
        F.when(F.col("d.dispute_id").isNotNull(), True)
        .otherwise(False)
        .alias("has_dispute"),
        F.datediff(F.col("t.transaction_date"), F.col("c.customer_created_at")).alias(
            "days_since_customer_created"
        ),
        F.datediff(F.col("t.transaction_date"), F.col("m.merchant_created_at")).alias(
            "days_since_merchant_created"
        ),
        F.datediff(
            F.col("t.transaction_date"), F.col("p.payment_first_seen_at")
        ).alias("days_since_payment_first_seen"),
        (
            F.unix_timestamp(F.col("t.transaction_authorized_at"))
            - F.unix_timestamp(F.col("o.order_created_at"))
        ).alias("order_to_auth_seconds"),
        F.col("t.net_amount").alias("merchant_net_revenue"),
        F.col("t.fees_total_amount").alias("psp_revenue"),
        (F.col("o.total_amount") - F.col("t.net_amount")).alias("total_psp_fees"),
        F.current_timestamp().alias("unified_created_at"),
    )
