"""DQX checked Silver and quarantine tables."""

from pathlib import Path
from types import SimpleNamespace

import yaml
from databricks.labs.dqx.config import ExtraParams
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.metrics_observer import DQMetricsObserver
from databricks.labs.dqx.schema import dq_result_schema
from databricks.sdk import WorkspaceClient
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

RULES_PATH = Path(spark.conf.get("dqx_rules_path", "configs/dqx-rules.yaml"))
RULE_DOCUMENT = yaml.safe_load(RULES_PATH.read_text(encoding="utf-8"))
LOCAL_MODE = spark.conf.get("workshop.local", "false").lower() == "true"
DQ_RESULT_DDL = (
    "ARRAY<STRUCT<name: STRING, message: STRING, columns: ARRAY<STRING>, filter: STRING, "
    "function: STRING, run_time: TIMESTAMP, run_id: STRING, user_metadata: MAP<STRING, STRING>, "
    "rule_fingerprint: STRING, rule_set_fingerprint: STRING, skipped: BOOLEAN>>"
)
TRANSACTION_CHECKED_SCHEMA = f"""
    txn_id STRING, order_id STRING, merchant_id STRING, event_ts STRING, amount_cents BIGINT,
    currency STRING, processor STRING, status STRING, _batch_id STRING, _incident_id STRING,
    _source_file STRING, _ingested_at TIMESTAMP, _rescued_data STRING, _order_exists BOOLEAN,
    _errors {DQ_RESULT_DDL}, _warnings {DQ_RESULT_DDL}
"""
DISPUTE_CHECKED_SCHEMA = f"""
    dispute_id STRING, txn_id STRING, opened_at STRING, closed_at STRING, reason STRING, status STRING,
    _batch_id STRING, _incident_id STRING, _source_file STRING, _ingested_at TIMESTAMP,
    _rescued_data STRING, _errors {DQ_RESULT_DDL}, _warnings {DQ_RESULT_DDL}
"""


def rules(entity: str) -> list[dict[str, object]]:
    return RULE_DOCUMENT[entity]


def engine() -> DQEngine:
    parameters = ExtraParams(
        run_time_overwrite="2026-08-22T09:00:00Z",
        run_id_overwrite="dbx-agentic-workshop",
    )
    if LOCAL_MODE:
        workspace = SimpleNamespace(
            config=SimpleNamespace(_product_info=None),
            clusters=SimpleNamespace(select_spark_version=list),
        )
    else:
        workspace = WorkspaceClient()
    return DQEngine(workspace, observer=DQMetricsObserver(), extra_params=parameters)


def _local_checked(df: DataFrame, conditions: list[tuple[object, str]]) -> DataFrame:
    """Build the same valid/quarantine boundary without schema analysis for local SDP graph checks."""
    failures = F.array_compact(
        F.array(
            *[
                F.when(
                    condition,
                    F.struct(
                        F.lit(name).alias("name"),
                        F.lit(name.replace("_", " ")).alias("message"),
                        F.array(F.lit(name)).alias("columns"),
                        F.lit(None).cast("string").alias("filter"),
                        F.lit("local_sdp_projection").alias("function"),
                        F.to_timestamp(F.lit("2026-08-22T09:00:00Z")).alias("run_time"),
                        F.lit("dbx-agentic-workshop").alias("run_id"),
                        F.lit(None).cast("map<string,string>").alias("user_metadata"),
                        F.lit(None).cast("string").alias("rule_fingerprint"),
                        F.lit(None).cast("string").alias("rule_set_fingerprint"),
                        F.lit(False).alias("skipped"),
                    ),
                )
                for condition, name in conditions
            ]
        )
    )
    return df.withColumn("_errors", F.when(F.size(failures) > 0, failures)).withColumn(
        "_warnings", F.lit(None).cast(dq_result_schema)
    )


def _local_transaction_checked() -> DataFrame:
    return _local_checked(
        _transactions_enriched(),
        [
            (F.col("txn_id").isNull(), "txn_id_is_null"),
            (F.col("_incident_id") == "txn-id-duplicate", "txn_id_is_duplicate"),
            (F.col("amount_cents") < 1, "amount_is_not_positive"),
            (~F.col("currency").isin("USD", "GBP", "CAD", "AUD"), "currency_is_not_authorized"),
            (~F.col("processor").isin("stripe", "adyen"), "processor_is_unknown"),
            (~F.col("_order_exists"), "order_is_orphan"),
        ],
    )


def _local_dispute_checked() -> DataFrame:
    return _local_checked(
        spark.table("bronze_disputes"),
        [
            (F.col("dispute_id").isNull(), "dispute_id_is_null"),
            (
                F.to_timestamp("closed_at") < F.to_timestamp("opened_at"),
                "dispute_closes_before_open",
            ),
        ],
    )


def _transactions_checked() -> DataFrame:
    source = _transactions_enriched()
    if LOCAL_MODE:
        return _local_transaction_checked()
    return engine().apply_checks_by_metadata(source, rules("transactions"))


def _disputes_checked() -> DataFrame:
    source = spark.table("bronze_disputes")
    if LOCAL_MODE:
        return _local_dispute_checked()
    return engine().apply_checks_by_metadata(source, rules("disputes"))


@dp.materialized_view(comment="Conformed merchant dimension")
def silver_merchants() -> DataFrame:
    return spark.table("bronze_merchants").select(
        "merchant_id", "merchant_name", "country", "risk_tier", "_batch_id", "_source_file", "_ingested_at"
    )


@dp.materialized_view(comment="Conformed order facts")
def silver_orders() -> DataFrame:
    return spark.table("bronze_orders").select(
        "order_id",
        "merchant_id",
        F.to_timestamp("order_ts").alias("order_ts"),
        "amount_cents",
        "currency",
        "_batch_id",
        "_source_file",
        "_ingested_at",
    )


def _transactions_enriched() -> DataFrame:
    transactions = spark.table("bronze_transactions").alias("t")
    orders = spark.table("bronze_orders").select("order_id").alias("o")
    return transactions.join(orders, F.col("t.order_id") == F.col("o.order_id"), "left").select(
        "t.*", F.col("o.order_id").isNotNull().alias("_order_exists")
    )


@dp.materialized_view(schema=TRANSACTION_CHECKED_SCHEMA)
def transactions_dq_checked() -> DataFrame:
    return _transactions_checked()


@dp.materialized_view(schema=DISPUTE_CHECKED_SCHEMA)
def disputes_dq_checked() -> DataFrame:
    return _disputes_checked()
