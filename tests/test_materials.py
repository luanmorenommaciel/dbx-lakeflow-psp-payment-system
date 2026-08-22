import json
import re
from pathlib import Path


def test_materials_and_fallbacks_exist() -> None:
    expected = [
        "docs/learner/setup.md",
        "docs/learner/workshop-guide.md",
        "docs/learner/cheat-sheet.md",
        "docs/learner/prompts/01-contract-plan.md",
        "docs/learner/prompts/06-genie-delivered.md",
        "docs/specs/psp-payment.md",
        "docs/instructor/instructor-runbook.md",
        "docs/instructor/expected-evidence.md",
        "docs/fallback/README.md",
        "docs/genie/space.json",
        "data/fallback/manifest.json",
        "scripts/e2e_local.sh",
        "scripts/e2e_remote.sh",
        "scripts/create_genie_space.sh",
        "scripts/test_genie.sh",
        "scripts/verify_remote_data.sh",
        "scripts/readiness.sh",
        "scripts/checkpoint.sh",
        "scripts/verify_remote_drift.sh",
    ]
    assert not [path for path in expected if not Path(path).exists()]


def test_readiness_gate_requires_named_release_evidence() -> None:
    gate = Path("scripts/readiness.sh").read_text()
    assert "DBXWorkshopRemoteCompletion" not in gate  # Receipt content, not a hard-coded synthetic pass.
    assert "reset_and_restore=true" in gate
    assert "checkpoints.env" in gate
    assert "claude.env" in gate
    assert "codex.env" in gate
    assert "fallback.env" in gate
    assert "website.env" in gate
    assert "tasks/done" not in gate
    assert "WORKSHOP_READINESS=READY" in gate


def test_guide_is_a_complete_four_hour_delivery_path() -> None:
    guide = Path("docs/learner/workshop-guide.md").read_text()
    expected_markers = [
        "Before the clock starts",
        "09:00–09:35",
        "12:30–13:00",
        "./scripts/e2e_local.sh",
        "databricks bundle validate --strict",
        "databricks bundle deploy",
        "Genie Code",
        "./scripts/checkpoint.sh 06",
        "./scripts/release_incident.sh --remote",
        "workshop-v1-starter",
        "Recovery",
    ]
    assert not [marker for marker in expected_markers if marker not in guide]
    assert "Workshop/" not in guide
    assert "labs/" not in guide
    assert guide.index("databricks bundle validate --strict") < guide.index("databricks bundle deploy")
    assert guide.index("databricks bundle deploy") < guide.index("databricks pipelines dry-run")


def test_six_prompts_are_tool_neutral_and_plan_gated() -> None:
    prompts = sorted(Path("docs/learner/prompts").glob("*.md"))
    assert len(prompts) == 6
    for index, prompt in enumerate(prompts, start=1):
        text = prompt.read_text()
        assert "propose" in text.lower()
        assert "wait for my approval" in text.lower()
        assert f"./scripts/checkpoint.sh {index:02d}" in text


def test_serialized_genie_space_has_stable_ids_and_bounded_sources() -> None:
    space = json.loads(Path("docs/genie/space.json").read_text())
    questions = space["config"]["sample_questions"]
    examples = space["instructions"]["example_question_sqls"]
    text_instructions = space["instructions"]["text_instructions"]
    ids = [item["id"] for item in questions + examples + text_instructions]
    assert space["version"] == 2
    assert len(questions) == 4
    assert len(text_instructions) == 1
    assert len(ids) == len(set(ids))
    assert all(re.fullmatch(r"[0-9a-f]{32}", item_id) for item_id in ids)
    assert [item["identifier"] for item in space["data_sources"]["tables"]] == sorted(
        item["identifier"] for item in space["data_sources"]["tables"]
    )
