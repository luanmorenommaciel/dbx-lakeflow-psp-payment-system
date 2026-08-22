from pathlib import Path

import yaml


def load(name: str) -> dict:
    return yaml.safe_load((Path("configs/contracts") / name).read_text())


def test_payment_contract_is_the_promised_slice() -> None:
    contract = load("psp-payment.contract.yaml")
    assert contract["seed"] == 22_082_026
    assert contract["transaction_count"] == 100_000
    assert contract["allowed_currencies"] == ["USD", "GBP", "CAD", "AUD"]
    assert set(contract["entities"]) == {"merchants", "orders", "transactions", "disputes"}
    assert contract["quality"]["invalid_rows_are_preserved"] is True
    assert contract["gold"]["table"] == "gold_merchant_risk"


def test_incident_ledger_has_seven_quarantines_and_one_replay() -> None:
    ledger = load("incident-ledger.yaml")
    incidents = ledger["incidents"]
    assert len(incidents) == 8
    assert sum(item["disposition"] == "quarantine" for item in incidents) == 7
    assert sum(item["disposition"] == "held_then_replay" for item in incidents) == 1


def test_agent_contract_is_dev_only() -> None:
    contract = load("agent-contract.yaml")
    assert contract["authority"]["allowed_target"] == "dev"
    assert contract["authority"]["deploy_requires_human_approval"] is True
    assert contract["scope"]["protected"] == []


def test_agent_projections_point_to_canonical_contract() -> None:
    agents = Path("AGENTS.md").read_text()
    claude = Path("CLAUDE.md").read_text()
    assert "configs/contracts/agent-contract.yaml" in agents
    assert "@AGENTS.md" in claude
    assert "permission-mode plan" in claude


def test_claude_project_contains_no_legacy_agentspec() -> None:
    legacy_paths = [
        Path(".claude/agents"),
        Path(".claude/commands"),
        Path(".claude/kb"),
        Path(".claude/sdd"),
        Path(".claude/settings.json"),
        Path(".claude/settings.local.json"),
    ]
    assert not [path for path in legacy_paths if path.exists()]
    assert Path(".claude/README.md").exists()
