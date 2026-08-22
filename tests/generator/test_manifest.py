import json
from pathlib import Path

from gen.synthetic.generator import GeneratorConfig, manifest


def test_manifest_is_deterministic() -> None:
    first = manifest(GeneratorConfig(seed=22_082_026))
    second = manifest(GeneratorConfig(seed=22_082_026))
    assert first == second
    assert first["logical_digest"].startswith("sha256:")


def test_manifest_exact_counts_and_incidents() -> None:
    result = manifest()
    assert result["entities"]["transactions"] == 100_000
    assert result["batches"] == {"batch-001": 98_791, "batch-002-incident": 1_209}
    assert result["incidents"]["currency-brl-unauthorized"] == 1_204
    assert len(result["incidents"]) == 8
    assert result["focus_merchant"] == "m-007"
    assert result["expected_risk"] == {"before_replay": 25, "after_replay": 45}


def test_fallback_manifest_has_four_semantic_content_digests() -> None:
    result = json.loads(Path("data/fallback/manifest.json").read_text())
    assert set(result["entity_digests"]) == {"merchants", "orders", "transactions", "disputes"}
    assert all(value.startswith("sha256:") for value in result["entity_digests"].values())


def test_generator_declares_only_the_four_valid_currencies_before_incident_override() -> None:
    source = Path("gen/synthetic/generator.py").read_text()
    for currency in ("USD", "GBP", "CAD", "AUD"):
        assert f"'{currency}'" in source
    assert "'EUR'" not in source
