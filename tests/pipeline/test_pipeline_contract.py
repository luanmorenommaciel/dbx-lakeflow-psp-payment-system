from pathlib import Path

import yaml

PIPELINE = Path("pipelines/src")


def test_pipeline_uses_current_sdp_api_and_no_drop_rows() -> None:
    source = "\n".join(path.read_text() for path in PIPELINE.glob("*.py"))
    assert "from pyspark import pipelines as dp" in source
    assert "ON VIOLATION DROP ROW" not in source
    assert "gold_merchant_risk" in source
    assert "get_invalid" in source
    assert "compute_summary_metrics" in source


def test_dqx_rules_cover_the_six_transaction_failures_and_temporal_dispute() -> None:
    rules = yaml.safe_load(Path("configs/dqx-rules.yaml").read_text())
    names = {rule["name"] for rule in rules["transactions"]}
    assert names == {
        "txn_id_is_null",
        "txn_id_is_duplicate",
        "amount_is_not_positive",
        "currency_is_not_authorized",
        "processor_is_unknown",
        "order_is_orphan",
    }
    assert any(rule["name"] == "dispute_closes_before_open" for rule in rules["disputes"])
    orphan = next(rule for rule in rules["transactions"] if rule["name"] == "order_is_orphan")
    assert orphan["check"]["arguments"]["expression"] == "_order_exists"
    temporal = next(rule for rule in rules["disputes"] if rule["name"] == "dispute_closes_before_open")
    assert ">=" in temporal["check"]["arguments"]["expression"]


def test_local_sdp_spec_is_one_pipeline() -> None:
    spec = yaml.safe_load(Path("pipelines/spark-pipeline.yaml").read_text())
    assert spec["libraries"] == [
        {"glob": {"include": "src/bronze.py"}},
        {"glob": {"include": "src/silver.py"}},
        {"glob": {"include": "src/outputs.py"}},
        {"glob": {"include": "src/gold.py"}},
    ]
    assert spec["configuration"]["source_format"] == "json"


def test_baseline_pipeline_keeps_one_declarative_graph() -> None:
    source = "\n".join(path.read_text() for path in PIPELINE.glob("*.py"))
    assert source.count("@dp.table") == 4
    assert "@dp.materialized_view" in source
    assert ".write" not in source
    assert ".collect(" not in source
    assert ".count()" not in source


def test_bronze_uses_real_auto_loader_schema_evolution_and_rescue() -> None:
    bronze = (PIPELINE / "bronze.py").read_text()
    assert 'option("cloudFiles.schemaLocation"' in bronze
    assert 'option("cloudFiles.schemaEvolutionMode", "addNewColumns")' in bronze
    assert 'option("rescuedDataColumn", "_rescued_data")' in bronze
    assert 'withColumn("_rescued_data", F.lit(None)' not in bronze
    assert Path("data/fallback/drift/transactions/schema-drift-rescue.json").exists()


def test_hosted_autoloader_uses_schema_hints_not_reader_schema() -> None:
    bronze = (PIPELINE / "bronze.py").read_text()
    cloud_block = bronze.split('if SOURCE_FORMAT == "cloudFiles":', 1)[1].split("else:", 1)[0]
    assert 'option("cloudFiles.schemaHints"' in cloud_block
    assert ".schema(" not in cloud_block
    assert ".load(path)" in cloud_block


def test_dqx_rules_load_from_configured_path_not_file() -> None:
    silver = (PIPELINE / "silver.py").read_text()
    outputs = (PIPELINE / "outputs.py").read_text()
    assert "__file__" not in silver
    assert "__file__" not in outputs
    assert 'spark.conf.get("dqx_rules_path"' in silver
    assert 'spark.conf.get("dqx_rules_path"' in outputs


def test_incident_pipeline_preserves_quarantine_and_replay_evidence() -> None:
    silver = "\n".join((PIPELINE / name).read_text() for name in ("silver.py", "outputs.py"))
    gold = (PIPELINE / "gold.py").read_text()
    release = Path("gen/synthetic/release.py").read_text()
    assert "get_invalid" in silver
    assert "dq_result_schema" in silver
    assert "quarantine_count" in gold
    assert "quality_risk_score" in gold
    assert "chargeback_risk_points" in gold
    assert "coalesce(q.quarantine_count, 0) / 50.0" in gold
    assert "coalesce(c.chargeback_count, 0) * 20" in gold
    assert 'mode("append")' in release
