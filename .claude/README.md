# Claude Code integration

The workshop uses the root `CLAUDE.md` and `AGENTS.md` contracts. Run
`./scripts/install_agent_skills.sh` to create the official Databricks Agent Skills links under
`.claude/skills/databricks-*`.

The previous AgentSpec agents, commands, knowledge base, SDD workflow, broad permissions, and
`bypassPermissions` setting were removed because they are unrelated to this four-hour workshop.
Do not add repository-specific permissions here; start Claude Code with `claude --permission-mode plan`.
