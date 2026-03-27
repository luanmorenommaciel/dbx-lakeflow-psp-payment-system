# Shared Anti-Pattern Library

> Referenced by all agents via `anti_pattern_refs: [shared-anti-patterns]` in frontmatter.
> Agent-specific anti-patterns live in each agent's Edge Cases section.

---

## SQL Anti-Patterns

| Anti-Pattern | Severity | Why It's Bad | Correct Approach |
|-------------|----------|-------------|-----------------|
| `SELECT *` in production queries | HIGH | Schema changes break consumers, wastes I/O, prevents partition pruning | Explicit column list: `SELECT col_a, col_b, col_c` |
| `CROSS JOIN` without filter | CRITICAL | Cartesian product explodes row count (N x M rows) | Add `WHERE` clause or use `INNER JOIN` with join key |
| Missing `WHERE` on `UPDATE`/`DELETE` | CRITICAL | Modifies or deletes entire table | Always include filter predicate; test on `SELECT` first |
| Correlated subqueries at scale | HIGH | O(n^2) execution — outer query re-runs subquery per row | Refactor to `JOIN` or window function |
| Implicit type casting | MEDIUM | Silent data loss, prevents index usage, non-portable | Explicit `CAST(col AS type)` or `TRY_CAST` |
| `DISTINCT` to mask duplication | MEDIUM | Hides root cause — data is duplicated upstream | Fix the JOIN or source that creates duplicates |
| Hardcoded dates/values | MEDIUM | Breaks on next run, not parameterized, not testable | Use variables, macros, or `{{ var('date') }}` in dbt |
| `ORDER BY` without `LIMIT` | MEDIUM | Full sort on large datasets with no purpose | Add `LIMIT` or remove `ORDER BY` if not needed |

---

## Schema Anti-Patterns

| Anti-Pattern | Severity | Why It's Bad | Correct Approach |
|-------------|----------|-------------|-----------------|
| Missing primary key | CRITICAL | No uniqueness guarantee, breaks incremental models, no merge key | Define PK on every table (even if composite) |
| No partition key on large tables | HIGH | Full table scans on every query, high cost and latency | Partition by date column or high-cardinality filter column |
| Nullable fact measures | HIGH | Aggregations (SUM, AVG) silently exclude rows, misleading results | Use `NOT NULL DEFAULT 0` for numeric measures |
| `VARCHAR` without length limits | MEDIUM | Unpredictable storage, no validation boundary | Set reasonable max length: `VARCHAR(256)` |
| No column documentation | MEDIUM | Tribal knowledge, onboarding friction, ambiguous semantics | Add `description` in dbt schema.yml or DDL comments |
| No clustering/sort key | MEDIUM | Suboptimal query performance on analytical workloads | Add clustering key aligned with common filter/join columns |

---

## Pipeline Anti-Patterns

| Anti-Pattern | Severity | Why It's Bad | Correct Approach |
|-------------|----------|-------------|-----------------|
| No idempotency | CRITICAL | Re-runs produce different results, data corruption risk | Design all tasks to produce identical output on re-run |
| Monolithic DAG tasks | HIGH | Single task does extract + transform + load — impossible to debug, retry, or parallelize | One task per logical operation; compose into task groups |
| Hardcoded connections/credentials | CRITICAL | Security risk, breaks across environments, not rotatable | Use Airflow Connections, environment variables, or secrets manager |
| No retry configuration | HIGH | Transient failures cause full pipeline failure | Set `retries: 2-3` with exponential backoff |
| Missing SLA / alerting | HIGH | Pipeline failures go unnoticed for hours or days | Define SLA miss callbacks, Slack/PagerDuty alerts |
| Top-level code outside DAG context | MEDIUM | Executes on every scheduler heartbeat (Airflow), wastes resources | All logic inside task callables or `@task` decorators |
| No error handling / dead letter queue | HIGH | Bad records silently dropped or crash entire pipeline | Route failures to DLQ, log and continue, alert on threshold |

---

## Data Quality Anti-Patterns

| Anti-Pattern | Severity | Why It's Bad | Correct Approach |
|-------------|----------|-------------|-----------------|
| No uniqueness tests | CRITICAL | Duplicate rows corrupt aggregations, break joins | `unique` test on every PK/business key (dbt, GE, or Soda) |
| No null checks on required fields | HIGH | Downstream queries produce wrong results silently | `not_null` test on all required columns |
| No freshness monitoring | HIGH | Stale data served to consumers without warning | `dbt source freshness` or Soda freshness checks |
| Testing only happy path | MEDIUM | Edge cases (empty tables, nulls, duplicates) missed | Include boundary tests: 0 rows, all-null column, max-length strings |
| Data contracts without owners | MEDIUM | No one responsible for enforcement, contracts rot | Every contract must have `owner: team@company.com` |
| No distribution drift detection | MEDIUM | Silent data quality degradation over time | Monitor value distributions with anomaly detection (Soda RAD, Monte Carlo) |

---

## Performance Anti-Patterns

| Anti-Pattern | Severity | Why It's Bad | Correct Approach |
|-------------|----------|-------------|-----------------|
| Full refresh on tables >1M rows without justification | HIGH | Wastes compute, extends pipeline duration, increases cost | Use incremental strategy (merge, delete+insert, append) |
| `collect()` on large Spark DataFrames | CRITICAL | Pulls all data to driver memory — OOM crash | Use `take(n)`, `show(n)`, or write to storage |
| No caching strategy | MEDIUM | Repeated reads of same data across tasks | `.cache()` or `.persist()` for reused DataFrames (with `.unpersist()` after) |
| Small file proliferation | HIGH | Thousands of tiny files degrade read performance and metadata overhead | Use compaction (OPTIMIZE), coalesce before write, or auto-compaction |
| Unbounded streaming state | CRITICAL | State grows indefinitely — OOM crash on streaming jobs | Set state TTL, use watermarks, configure state cleanup |
| `coalesce(1)` on large data | HIGH | Bottlenecks all data through single partition — slow, OOM risk | Use appropriate partition count; use single file only for small outputs |
| UDFs where built-in functions exist | MEDIUM | UDFs prevent Catalyst optimization, 2-10x slower | Check Spark/DuckDB built-in functions first; UDF as last resort |

---

## Governance Anti-Patterns

| Anti-Pattern | Severity | Why It's Bad | Correct Approach |
|-------------|----------|-------------|-----------------|
| PII in logs or query output | CRITICAL | Compliance violation (GDPR, CCPA, HIPAA), legal risk | Mask/hash PII columns, exclude from logging, tag as sensitive |
| No access control on sensitive tables | CRITICAL | Anyone can read PII/financial data | Implement RBAC with row-level security and column masking |
| Schema changes without versioning | HIGH | Breaks consumers without warning, no rollback path | Use schema evolution (Iceberg/Delta), version contracts, notify consumers |
| No data lineage tracking | MEDIUM | Cannot trace data origin or impact of changes | Use dbt lineage, catalog metadata, or manual documentation |
| Shared service accounts for all pipelines | HIGH | No audit trail, no blast radius isolation | Per-pipeline or per-team service accounts with least-privilege grants |
