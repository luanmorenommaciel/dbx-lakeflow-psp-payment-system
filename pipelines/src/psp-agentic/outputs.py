"""Valid, quarantine, and metrics projections from DQX-annotated datasets."""

from pathlib import Path

import yaml
from databricks.labs.dqx.config import ExtraParams
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.metrics_observer import DQMetricsObserver
from databricks.sdk import WorkspaceClient
from pyspark import pipelines as dp
from pyspark.sql import DataFrame

RULES_PATH = Path(__file__).resolve().parents[3] / "configs" / "dqx-rules.yaml"
RULE_DOCUMENT = yaml.safe_load(RULES_PATH.read_text(encoding="utf-8"))
LOCAL_MODE = spark.conf.get("workshop.local", "false").lower() == "true"


def rules(entity: str) -> list[dict[str, object]]:
    return RULE_DOCUMENT[entity]


def engine() -> DQEngine:
    parameters = ExtraParams(
        run_time_overwrite="2026-08-22T09:00:00Z",
        run_id_overwrite="dbx-agentic-workshop",
    )
    return DQEngine(WorkspaceClient(), observer=DQMetricsObserver(), extra_params=parameters)


def _local_metrics(df: DataFrame) -> DataFrame:
    return df.selectExpr(
        "count(*) AS input_row_count",
        "sum(CASE WHEN _errors IS NOT NULL THEN 1 ELSE 0 END) AS error_row_count",
        "sum(CASE WHEN _warnings IS NOT NULL THEN 1 ELSE 0 END) AS warning_row_count",
    )


@dp.materialized_view
def silver_transactions() -> DataFrame:
    source = spark.table("transactions_dq_checked")
    if LOCAL_MODE:
        return source.where("_errors IS NULL")
    return engine().get_valid(source)


@dp.materialized_view(comment="Invalid transactions preserved with DQX errors and warnings")
def quarantine_transactions() -> DataFrame:
    source = spark.table("transactions_dq_checked")
    if LOCAL_MODE:
        return source.where("_errors IS NOT NULL")
    return engine().get_invalid(source)


@dp.materialized_view(comment="Valid disputes after DQX")
def silver_disputes() -> DataFrame:
    source = spark.table("disputes_dq_checked")
    if LOCAL_MODE:
        return source.where("_errors IS NULL")
    return engine().get_valid(source)


@dp.materialized_view(comment="Invalid disputes preserved with DQX errors and warnings")
def quarantine_disputes() -> DataFrame:
    source = spark.table("disputes_dq_checked")
    if LOCAL_MODE:
        return source.where("_errors IS NOT NULL")
    return engine().get_invalid(source)


@dp.materialized_view(comment="Cumulative DQX transaction metrics")
def dq_transaction_metrics() -> DataFrame:
    source = spark.table("transactions_dq_checked")
    if LOCAL_MODE:
        return _local_metrics(source)
    return engine().compute_summary_metrics(source, checks=rules("transactions"))


@dp.materialized_view(comment="Cumulative DQX dispute metrics")
def dq_dispute_metrics() -> DataFrame:
    source = spark.table("disputes_dq_checked")
    if LOCAL_MODE:
        return _local_metrics(source)
    return engine().compute_summary_metrics(source, checks=rules("disputes"))
