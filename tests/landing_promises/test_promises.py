from pathlib import Path

import yaml


def test_all_landing_promises_have_artifacts() -> None:
    required = [
        "AGENTS.md",
        "CLAUDE.md",
        "configs/contracts/psp-payment.contract.yaml",
        "gen/synthetic/generator.py",
        "pipelines/src/bronze.py",
        "configs/dqx-rules.yaml",
        "pipelines/src/gold.py",
        "databricks.yml",
        "docs/genie/questions.yaml",
        "docs/genie/space.json",
        "docs/fallback/README.md",
        "docs/fallback/dry-run/local-sdp-2026-08-21.txt",
        "docs/fallback/genie/local-query-replay-2026-08-21.md",
        "docs/fallback/wheels/README.md",
        "scripts/cache_dqx_wheel.sh",
        "scripts/upload_fallback.sh",
        "scripts/install_agent_skills.sh",
        "scripts/e2e_remote.sh",
    ]
    assert not [path for path in required if not Path(path).exists()]


def test_genie_contract_questions_have_expected_results() -> None:
    questions = yaml.safe_load(Path("docs/genie/questions.yaml").read_text())
    expected = yaml.safe_load(Path("docs/genie/expected-results.yaml").read_text())
    assert len(questions["questions"]) == 4
    assert {item["id"] for item in questions["questions"]} == set(expected["expected"])


def test_no_top_level_workshop_or_lab_directory() -> None:
    assert not Path("workshop").exists()
    assert not Path("lab").exists()
