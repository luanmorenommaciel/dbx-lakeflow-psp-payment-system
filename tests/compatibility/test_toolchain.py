from importlib.metadata import version
from pathlib import Path

import yaml


def test_pinned_python_dependencies_import() -> None:
    import dbldatagen  # noqa: F401
    import pyspark
    from databricks.labs import dqx  # noqa: F401

    assert pyspark.__version__ == "4.2.0"
    assert version("dbldatagen") == "0.4.0.post1"
    assert version("databricks-labs-dqx") == "0.16.0"


def test_toolchain_lock_matches_candidate_tuple() -> None:
    lock = yaml.safe_load(Path("toolchain.lock.yaml").read_text())
    assert lock["databricks_cli"]["required"] == "1.13.0"
    assert lock["spark"]["pyspark"] == "4.2.0"
    assert lock["lakeflow"]["pipeline_environment_version"] == "4"


def test_dqx_fallback_wheel_matches_uv_lock() -> None:
    script = Path("scripts/cache_dqx_wheel.sh").read_text()
    uv_lock = Path("uv.lock").read_text()
    expected_hash = "71006c42cb89f4b8ad2333f19d4b51552040c57582ff9345300de4597b14c8b0"
    expected_wheel = "databricks_labs_dqx-0.16.0-py3-none-any.whl"
    assert expected_hash in script and expected_hash in uv_lock
    assert expected_wheel in script and expected_wheel in uv_lock
