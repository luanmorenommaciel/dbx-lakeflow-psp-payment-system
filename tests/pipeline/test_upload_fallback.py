import os
import subprocess
from pathlib import Path


def run_upload(tmp_path: Path, mode: str) -> tuple[str, list[str]]:
    fake_cli = tmp_path / "databricks"
    fake_cli.write_text('#!/bin/sh\nprintf "%s\\n" "$*" >> "$UPLOAD_LOG"\n')
    fake_cli.chmod(0o755)
    log = tmp_path / "upload.log"
    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{tmp_path}:{env['PATH']}",
            "UPLOAD_LOG": str(log),
            "DATABRICKS_CONFIG_PROFILE": "test-profile",
        }
    )
    result = subprocess.run(
        ["./scripts/upload_fallback.sh", mode],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    return result.stdout, log.read_text().splitlines()


def test_baseline_upload_selects_only_active_entity_parts(tmp_path: Path) -> None:
    output, calls = run_upload(tmp_path, "baseline")
    assert "UPLOAD=PASS mode=baseline files=16" in output
    assert len(calls) == 16
    assert all("data/fallback/generated/held" not in call for call in calls)
    assert all("dbfs:/Volumes/workspace/dbx_agentic_dev/landing/" in call for call in calls)


def test_replay_upload_selects_only_held_dispute_parts(tmp_path: Path) -> None:
    output, calls = run_upload(tmp_path, "replay")
    assert "UPLOAD=PASS mode=replay files=2" in output
    assert len(calls) == 2
    assert all("data/fallback/generated/held/disputes/" in call for call in calls)
    assert all("/landing/disputes/" in call for call in calls)


def test_drift_upload_selects_only_the_schema_rescue_probe(tmp_path: Path) -> None:
    output, calls = run_upload(tmp_path, "drift")
    assert "UPLOAD=PASS mode=drift files=1" in output
    assert len(calls) == 1
    assert "data/fallback/drift/transactions/schema-drift-rescue.json" in calls[0]
    assert "/landing/transactions/schema-drift-rescue.json" in calls[0]
