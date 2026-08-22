# Learner setup

Complete this before 09:00; setup is not part of the four-hour clock.

1. Create your own Databricks Free Edition workspace.
2. Install Git, Python 3.11–3.13, uv, Java 17 or 21, Databricks CLI 1.13.0, and Claude Code or Codex.
3. Clone the repository, fetch tags, and create a learner branch from the starter:

   ```bash
   git clone https://github.com/luanmorenommaciel/dbx-lakeflow-psp-payment-system.git
   cd dbx-lakeflow-psp-payment-system
   git fetch --tags
   git switch -c student/<name> workshop-v1-starter
   ```

4. Authenticate without committing credentials. Choose the profile yourself; the agent must never choose one:

   ```bash
   databricks auth login --host https://YOUR-WORKSPACE.cloud.databricks.com --profile dbx-workshop
   export DATABRICKS_CONFIG_PROFILE=dbx-workshop
   databricks auth profiles
   databricks current-user me -p "$DATABRICKS_CONFIG_PROFILE"
   ```

5. Run `./scripts/bootstrap.sh`. It installs the seven selected Databricks Agent Skills for both project entry
   points, verifies the pinned tools and authentication, and resolves Python dependencies.
6. Confirm that the `workspace` catalog is writable, a SQL warehouse is available, and serverless pipeline
   creation is enabled in your Free Edition workspace. Run `./scripts/preflight.sh --runtime` again after module
   05 creates the bundle.

Use `./scripts/sdp.sh` for local Spark pipeline commands. It selects Java 17/21 and prevents a global Spark
installation from shadowing the pinned Spark environment.

If authentication cannot pass within ten minutes, pair with a green learner while retaining your own local
branch. The instructor workspace is only canonical demonstration and explicitly labelled fallback evidence.
