import json
import re
from pathlib import Path


def test_materials_and_fallbacks_exist() -> None:
    expected = [
        "docs/README.md",
        "docs/learner/README.md",
        "docs/learner/setup.md",
        "docs/learner/brd-psp.md",
        "docs/learner/brd-psp.pdf",
        "docs/learner/foundations.md",
        "docs/learner/techs.md",
        "docs/learner/how-we-work.md",
        "docs/learner/lesson-template.md",
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
    assert not Path("tasks").exists()
    assert "WORKSHOP_READINESS=READY" in gate


def test_guide_is_a_complete_four_hour_delivery_path() -> None:
    guide = Path("docs/learner/workshop-guide.md").read_text()
    expected_markers = [
        "Before the clock starts",
        "09:00–09:15",
        "09:15–09:35",
        "12:30–13:00",
        "./scripts/e2e_local.sh",
        "databricks bundle validate --strict",
        "databricks bundle deploy",
        "Genie Code",
        "./scripts/checkpoint.sh 06",
        "./scripts/release_incident.sh --remote",
        "workshop-v2-starter",
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
        for ritual in ("Own", "Plan", "Execute", "Verify", "Review", "Lesson"):
            assert ritual in text, f"{prompt.name} missing ritual step {ritual}"


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


def test_root_readme_is_workshop_entry() -> None:
    text = Path("README.md").read_text()
    assert "docs/learner/brd-psp.md" in text
    assert "18 DLT" not in text
    assert "ShadowTraffic" not in text
    assert not Path("docs/psp-use-case.pdf").exists()
    assert not Path("docs/reference").exists()


def test_brd_is_four_entity_ticket() -> None:
    brd = Path("docs/learner/brd-psp.md").read_text()
    in_scope, out_of_scope = brd.split("## 4. Out of scope", maxsplit=1)
    for entity in ("merchants", "orders", "transactions", "disputes"):
        assert entity in in_scope
    for advanced in ("customers", "payment_instruments", "payouts"):
        assert advanced not in in_scope
        assert advanced in out_of_scope
    assert "m-007" in brd
    assert "25" in brd and "45" in brd
    assert Path("docs/learner/brd-psp.pdf").stat().st_size > 0
