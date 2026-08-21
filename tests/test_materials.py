import json
import re
from pathlib import Path


def test_materials_and_fallbacks_exist() -> None:
    expected = [
        "docs/learner/setup.md",
        "docs/learner/workshop-guide.md",
        "docs/learner/cheat-sheet.md",
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
    ]
    assert not [path for path in expected if not Path(path).exists()]


def test_readiness_gate_requires_hosted_restore_and_archived_tasks() -> None:
    gate = Path("scripts/readiness.sh").read_text()
    assert "DBXWorkshopRemoteCompletion" not in gate  # Receipt content, not a hard-coded synthetic pass.
    assert "reset_and_restore=true" in gate
    assert "tasks/done" in gate
    assert "expected_tasks=9" in gate
    assert "WORKSHOP_READINESS=READY" in gate


def test_guide_is_a_complete_four_hour_delivery_path() -> None:
    guide = Path("docs/learner/workshop-guide.md").read_text()
    expected_markers = [
        "Before the clock starts",
        "00:00–00:15",
        "03:45–04:00",
        "./scripts/e2e_local.sh",
        "databricks bundle validate --strict",
        "databricks bundle deploy",
        "./scripts/create_genie_space.sh",
        "./scripts/release_incident.sh --remote",
        "--reset-and-restore",
        "Deliver and explain",
    ]
    assert not [marker for marker in expected_markers if marker not in guide]
    assert "Workshop/" not in guide
    assert "labs/" not in guide
    assert guide.index("databricks bundle validate --strict") < guide.index("databricks bundle deploy")
    assert guide.index("databricks bundle deploy") < guide.index("databricks pipelines dry-run")


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
