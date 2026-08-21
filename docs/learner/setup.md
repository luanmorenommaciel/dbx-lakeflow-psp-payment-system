# Learner setup

Complete this before 09:00.

1. Create your own Databricks Free Edition workspace.
2. Install Git, Python 3.11–3.13, uv, Java 17 or 21, Databricks CLI 1.13.0, and Claude Code or Codex.
3. Clone the repository and work from its root.
4. Authenticate without committing credentials:

   ```bash
   databricks auth login --host https://YOUR-WORKSPACE.cloud.databricks.com --profile dbx-workshop
   export DATABRICKS_CONFIG_PROFILE=dbx-workshop
   ```

5. Run `./scripts/bootstrap.sh`. The bootstrap installs the selected skills to Claude Code project scope and
   resolves the identical Codex set under `.agents/skills`, the Codex repository discovery path.
6. Before any workshop deployment, run `./scripts/preflight.sh --runtime`. This starts the available SQL warehouse
   with `SELECT 1` and detects exhausted Free Edition compute before the bundle can leave partial resources.

Use `./scripts/sdp.sh` for local Spark pipeline commands. It selects Java 17/21 and prevents a global Spark
installation from shadowing the pinned Spark 4.2 environment.

Your workspace should be your own. The instructor workspace is for canonical demonstration and replay evidence.
