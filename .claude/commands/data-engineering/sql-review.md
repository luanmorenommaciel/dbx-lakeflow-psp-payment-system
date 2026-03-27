---
name: sql-review
description: SQL-specific code review — delegates to sql-optimizer + code-reviewer agents
---

# SQL Review Command

> Review SQL code for anti-patterns, performance, and cross-dialect correctness

## Usage

```bash
/sql-review <file-or-directory>
```

## Examples

```bash
/sql-review models/marts/mart_revenue.sql
/sql-review models/staging/
/sql-review "Review all SQL in this PR for Snowflake compatibility"
```

---

## What This Command Does

1. Invokes the **code-reviewer** agent with DE capability enabled
2. Escalates SQL optimization questions to **sql-optimizer** agent
3. Loads KB patterns from `sql-patterns`, `data-quality`, and `dbt` domains
4. Reviews for:
   - SQL anti-patterns (`SELECT *`, implicit coercion, missing partition filters)
   - Performance issues (full table scans, N+1 patterns, missing indexes)
   - Cross-dialect correctness (Snowflake, BigQuery, Postgres, DuckDB)
   - PII detection and masking compliance
   - dbt-specific issues (ref usage, incremental guards, test coverage)

## Agent Delegation

| Agent | Role |
|-------|------|
| `code-reviewer` | Primary — DE review capability (anti-patterns, PII, tests) |
| `sql-optimizer` | Escalation — query plan analysis, window functions |
| `dbt-specialist` | Escalation — dbt-specific patterns and macros |

## KB Domains Used

- `sql-patterns` — SQL anti-patterns, window functions, CTEs
- `data-quality` — quality checks, PII detection
- `dbt` — model patterns, testing conventions

## Output

A structured review report with severity-classified issues and fix suggestions.
