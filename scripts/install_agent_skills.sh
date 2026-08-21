#!/usr/bin/env bash
set -euo pipefail

databricks aitools install \
  --scope project \
  --agents claude-code \
  --skills-only \
  --experimental \
  --skills databricks-core,databricks-dabs,databricks-pipelines,databricks-synthetic-data-gen,databricks-unity-catalog,databricks-spark-structured-streaming,databricks-genie

# Databricks CLI 1.13 does not register project scope for Codex directly.
# Codex discovers repository skills from .agents/skills, so resolve the same pinned set there.
databricks aitools install \
  --path .agents/skills \
  --experimental \
  --skills databricks-core,databricks-dabs,databricks-pipelines,databricks-synthetic-data-gen,databricks-unity-catalog,databricks-spark-structured-streaming,databricks-genie

databricks aitools list --scope project
