from pathlib import Path

import yaml


def test_module_01_contract_plan() -> None:
    payment = yaml.safe_load(Path("configs/contracts/psp-payment.contract.yaml").read_text())
    incidents = yaml.safe_load(Path("configs/contracts/incident-ledger.yaml").read_text())
    assert payment["allowed_currencies"] == ["USD", "GBP", "CAD", "AUD"]
    assert payment["transaction_count"] == 100_000
    assert len(incidents["incidents"]) == 8


def test_module_02_synthetic_data() -> None:
    source = Path("gen/synthetic/generator.py").read_text()
    assert "dbldatagen" in source
    assert "22082026" in source
    assert "late-valid-chargeback" in source
    assert Path("gen/synthetic/release.py").exists()


def test_module_03_bronze() -> None:
    source = Path("pipelines/src/psp-agentic/bronze.py").read_text()
    spec = yaml.safe_load(Path("pipelines/spark-pipeline.yaml").read_text())
    assert source.count("@dp.table") == 4
    assert 'option("cloudFiles.schemaEvolutionMode", "addNewColumns")' in source
    assert 'option("rescuedDataColumn", "_rescued_data")' in source
    assert spec["libraries"][0] == {"glob": {"include": "src/psp-agentic/bronze.py"}}


def test_module_04_dqx_silver() -> None:
    rules = yaml.safe_load(Path("configs/dqx-rules.yaml").read_text())
    source = "\n".join(
        Path(f"pipelines/src/psp-agentic/{name}").read_text() for name in ("silver.py", "outputs.py")
    )
    assert len(rules["transactions"]) == 6
    assert {rule["name"] for rule in rules["disputes"]} == {
        "dispute_id_is_null",
        "dispute_closes_before_open",
    }
    assert "get_invalid" in source
    assert "_errors" in source and "_warnings" in source
    assert "ON VIOLATION DROP ROW" not in source


def test_module_05_gold_dabs() -> None:
    gold = Path("pipelines/src/psp-agentic/gold.py").read_text()
    bundle = yaml.safe_load(Path("databricks.yml").read_text())
    resource = yaml.safe_load(Path("pipelines/resources/agentic/pipeline.yml").read_text())
    assert "gold_merchant_risk" in gold
    assert set(bundle["targets"]) == {"dev"}
    assert resource["resources"]["pipelines"]["workshop_pipeline"]["serverless"] is True


def test_module_06_genie_delivered() -> None:
    questions = yaml.safe_load(Path("docs/genie/questions.yaml").read_text())
    instructions = Path("docs/genie/instructions.md").read_text().lower()
    assert len(questions["questions"]) == 4
    assert "genie code" in instructions
    assert "fraud" in instructions
