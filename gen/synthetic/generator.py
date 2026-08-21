"""Spark-native deterministic data generation using dbldatagen."""

from __future__ import annotations

import json
from dataclasses import dataclass
from hashlib import sha256
from typing import Any

import dbldatagen as dg
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType

SEED = 22082026
TRANSACTION_COUNT = 100_000
BASELINE_COUNT = 98_791
INCIDENT_COUNT = 1_209
BRL_START_ID = 98_794
BRL_END_ID = 99_997
FOCUS_MERCHANT = "m-007"


@dataclass(frozen=True)
class GeneratorConfig:
    seed: int = SEED
    partitions: int = 4


def _base(spark: SparkSession, name: str, rows: int, config: GeneratorConfig) -> dg.DataGenerator:
    return dg.DataGenerator(
        sparkSession=spark,
        name=name,
        rows=rows,
        partitions=config.partitions,
        randomSeed=config.seed,
    ).withIdOutput()


def build_merchants(spark: SparkSession, config: GeneratorConfig) -> DataFrame:
    return (
        _base(spark, "merchants", 12, config)
        .withColumn("merchant_id", StringType(), expr="format_string('m-%03d', id + 1)")
        .withColumn("merchant_name", StringType(), expr="format_string('Merchant %02d', id + 1)")
        .withColumn("country", StringType(), expr="CASE WHEN id % 4 = 0 THEN 'BR' ELSE 'US' END")
        .withColumn("risk_tier", StringType(), expr="CASE WHEN id = 6 THEN 'high' ELSE 'standard' END")
        .build()
        .drop("id")
        .withColumn("_batch_id", F.lit("batch-001"))
    )


def build_orders(spark: SparkSession, config: GeneratorConfig) -> DataFrame:
    return (
        _base(spark, "orders", TRANSACTION_COUNT, config)
        .withColumn("order_id", StringType(), expr="format_string('ord-%06d', id)")
        .withColumn(
            "merchant_id",
            StringType(),
            expr=f"CASE WHEN id >= {BASELINE_COUNT} THEN '{FOCUS_MERCHANT}' "
            "ELSE format_string('m-%03d', (id % 12) + 1) END",
        )
        .withColumn("order_ts", StringType(), expr="CAST(timestamp_seconds(1787389200 + id) AS STRING)")
        .withColumn("amount_cents", LongType(), expr="1000 + (id % 25000)")
        .withColumn("currency", StringType(), expr="CASE WHEN id % 10 = 0 THEN 'EUR' ELSE 'USD' END")
        .build()
        .withColumn("_batch_id", F.when(F.col("id") < BASELINE_COUNT, "batch-001").otherwise("batch-002-incident"))
        .drop("id")
    )


def build_transactions(spark: SparkSession, config: GeneratorConfig) -> DataFrame:
    frame = (
        _base(spark, "transactions", TRANSACTION_COUNT, config)
        .withColumn("txn_id", StringType(), expr="format_string('txn-%06d', id)")
        .withColumn("order_id", StringType(), expr="format_string('ord-%06d', id)")
        .withColumn(
            "merchant_id",
            StringType(),
            expr=f"CASE WHEN id >= {BASELINE_COUNT} THEN '{FOCUS_MERCHANT}' "
            "ELSE format_string('m-%03d', (id % 12) + 1) END",
        )
        .withColumn("event_ts", StringType(), expr="CAST(timestamp_seconds(1787389260 + id) AS STRING)")
        .withColumn("amount_cents", LongType(), expr="1000 + (id % 25000)")
        .withColumn("currency", StringType(), expr="CASE WHEN id % 10 = 0 THEN 'EUR' ELSE 'USD' END")
        .withColumn("processor", StringType(), expr="CASE WHEN id % 2 = 0 THEN 'stripe' ELSE 'adyen' END")
        .withColumn("status", StringType(), expr="'captured'")
        .build()
    )
    return (
        frame.withColumn(
            "txn_id",
            F.when(F.col("id") == 98_791, F.lit(None).cast("string"))
            .when(F.col("id") == 98_792, F.lit("txn-000000"))
            .otherwise(F.col("txn_id")),
        )
        .withColumn("amount_cents", F.when(F.col("id") == 98_793, F.lit(0)).otherwise(F.col("amount_cents")))
        .withColumn(
            "currency",
            F.when(F.col("id").between(BRL_START_ID, BRL_END_ID), F.lit("BRL")).otherwise(F.col("currency")),
        )
        .withColumn("processor", F.when(F.col("id") == 99_998, "unknown").otherwise(F.col("processor")))
        .withColumn("order_id", F.when(F.col("id") == 99_999, "ord-missing").otherwise(F.col("order_id")))
        .withColumn("_batch_id", F.when(F.col("id") < BASELINE_COUNT, "batch-001").otherwise("batch-002-incident"))
        .withColumn(
            "_incident_id",
            F.when(F.col("id") == 98_791, "txn-id-null")
            .when(F.col("id") == 98_792, "txn-id-duplicate")
            .when(F.col("id") == 98_793, "amount-non-positive")
            .when(F.col("id").between(BRL_START_ID, BRL_END_ID), "currency-brl-unauthorized")
            .when(F.col("id") == 99_998, "processor-unknown")
            .when(F.col("id") == 99_999, "order-orphan"),
        )
        .drop("id")
    )


def build_disputes(spark: SparkSession, config: GeneratorConfig) -> DataFrame:
    frame = (
        _base(spark, "disputes", 102, config)
        .withColumn("dispute_id", StringType(), expr="format_string('dsp-%04d', id)")
        .withColumn("txn_id", StringType(), expr="format_string('txn-%06d', id * 12 + 6)")
        .withColumn("opened_at", StringType(), expr="CAST(timestamp_seconds(1787475600 + id * 60) AS STRING)")
        .withColumn("closed_at", StringType(), expr="CAST(timestamp_seconds(1787562000 + id * 60) AS STRING)")
        .withColumn("reason", StringType(), expr="'fraudulent'")
        .withColumn("status", StringType(), expr="CASE WHEN id = 101 THEN 'chargeback' ELSE 'closed' END")
        .build()
    )
    return (
        frame.withColumn(
            "closed_at",
            F.when(F.col("id") == 100, F.lit("2026-08-21 08:00:00")).otherwise(F.col("closed_at")),
        )
        .withColumn("_batch_id", F.when(F.col("id") < 100, "batch-001").otherwise("batch-002-incident"))
        .withColumn(
            "_incident_id",
            F.when(F.col("id") == 100, "dispute-closes-before-open").when(F.col("id") == 101, "late-valid-chargeback"),
        )
        .withColumn("_release_state", F.when(F.col("id") == 101, "held").otherwise("active"))
        .drop("id")
    )


def build_frames(spark: SparkSession, config: GeneratorConfig | None = None) -> dict[str, DataFrame]:
    resolved = config or GeneratorConfig()
    return {
        "merchants": build_merchants(spark, resolved),
        "orders": build_orders(spark, resolved),
        "transactions": build_transactions(spark, resolved),
        "disputes": build_disputes(spark, resolved),
    }


def manifest(config: GeneratorConfig | None = None) -> dict[str, Any]:
    resolved = config or GeneratorConfig()
    result = {
        "contract": "PSPSyntheticManifest/v1",
        "seed": resolved.seed,
        "entities": {"merchants": 12, "orders": 100_000, "transactions": 100_000, "disputes": 102},
        "batches": {"batch-001": 98_791, "batch-002-incident": 1_209},
        "focus_merchant": FOCUS_MERCHANT,
        "incidents": {
            "txn-id-null": 1,
            "txn-id-duplicate": 1,
            "amount-non-positive": 1,
            "currency-brl-unauthorized": 1_204,
            "processor-unknown": 1,
            "order-orphan": 1,
            "dispute-closes-before-open": 1,
            "late-valid-chargeback": 1,
        },
        "expected_risk": {"before_replay": 25, "after_replay": 45},
    }
    canonical = json.dumps(result, sort_keys=True, separators=(",", ":")).encode()
    result["logical_digest"] = f"sha256:{sha256(canonical).hexdigest()}"
    return result
