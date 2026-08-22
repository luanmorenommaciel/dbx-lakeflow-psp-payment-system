from pathlib import Path

import yaml


def test_root_bundle_is_dev_only_and_host_neutral() -> None:
    text = Path("databricks.yml").read_text()
    bundle = yaml.safe_load(text)
    assert set(bundle["targets"]) == {"dev"}
    assert "host:" not in text
    assert bundle["include"] == ["pipelines/resources/agentic/*.yml"]
    assert bundle["experimental"]["skip_name_prefix_for_schema"] is True


def test_pipeline_is_serverless_and_environment_is_pinned() -> None:
    resource = yaml.safe_load(Path("pipelines/resources/agentic/pipeline.yml").read_text())
    pipeline = resource["resources"]["pipelines"]["workshop_pipeline"]
    assert pipeline["serverless"] is True
    assert pipeline["environment"]["environment_version"] == "4"
    assert pipeline["environment"]["dependencies"] == ["databricks-labs-dqx==0.16.0"]
    assert pipeline["configuration"]["landing_path"] == "/Volumes/${var.catalog}/${var.schema}/${var.landing_volume}"


def test_workshop_avoids_an_unnecessary_jobs_api_dependency() -> None:
    assert not Path("pipelines/resources/agentic/workshop-job.yml").exists()
    upload = Path("scripts/upload_fallback.sh").read_text()
    assert "databricks fs cp" in upload
    assert "dbfs:/Volumes/$catalog/$schema/$volume" in upload


def test_workshop_bundle_does_not_include_other_pipelines() -> None:
    root = Path("databricks.yml").read_text()
    assert "pipelines/resources/psp_analytics_pipeline.yml" not in root
    assert not Path("pipelines/databricks.yml").exists()
    assert not Path("pipelines/src/psp-analytics").exists()
    assert not Path("cli").exists()


def test_reset_is_exactly_scoped_and_requires_confirmation() -> None:
    reset = Path("scripts/reset.sh").read_text()
    assert '"${1:-}" != "--confirm"' in reset
    assert '"${2:-}" != "$schema"' in reset
    assert "DROP SCHEMA IF EXISTS ${catalog}.${schema} CASCADE" in reset
    assert "bundle destroy -t dev" in reset
    assert "rm -rf" not in reset


def test_remote_e2e_checks_compute_before_deploy_and_cleans_partial_failure() -> None:
    e2e = Path("scripts/e2e_remote.sh").read_text()
    assert e2e.index("./scripts/preflight.sh --runtime") < e2e.index("databricks bundle deploy")
    assert "cleanup_after_failure" in e2e
    assert 'stage="deploy"' in e2e
    assert "failure-cleanup.log" in e2e
    assert 'rm -f "${stale_evidence[@]}"' in e2e
    assert 'databricks workspace delete "$bundle_root" --recursive' in e2e
    assert "bundle run workshop_pipeline" in e2e
    assert "bundle run workshop_run" not in e2e
