# Data Quality Quick Reference

> Fast lookup tables. For code examples, see linked files.

## Quality Dimensions

| Dimension | Definition | Measurement |
|-----------|-----------|-------------|
| Completeness | % non-null for required fields | `COUNT(col) / COUNT(*)` |
| Accuracy | Data matches real-world truth | Cross-reference with source |
| Consistency | Same value across systems | Compare source vs target |
| Timeliness | Data available within SLA | `NOW() - MAX(loaded_at)` |
| Uniqueness | No duplicate records | `COUNT(*) = COUNT(DISTINCT pk)` |
| Validity | Values within accepted range | Accepted values, regex, range |

## Framework Comparison (Updated 2026)

| Feature | dbt Tests | Great Expectations | Soda Core | Monte Carlo |
|---------|----------|-------------------|-----------|-------------|
| Language | YAML + SQL | Python + YAML | Data Contracts YAML (v4) / SodaCL (v3) | Config UI |
| Setup time | Minutes | Hours (Cloud: minutes with ExpectAI) | Minutes | Hours |
| Custom checks | Generic test macros | Custom expectations | Custom checks + extensible types (v4) | Limited |
| Anomaly detection | No | Limited | RAD (built-in) + AI-powered (v4) | ML-based |
| AI features | None | ExpectAI (auto-generate rules) | AI-translated rules from natural language (v4) | ML anomalies |
| Cost | Free (OSS) | Free (Core) / Paid (Cloud) | Free (Core) / Paid (Cloud) | Paid |
| Best for | dbt projects | Python pipelines | Any (universal, contracts-first) | Enterprise observability |
| Latest version | dbt Core 1.9+ | GX Core 1.3.10+ | Soda Core 4.0 (Feb 2026) | SaaS |

## Data Contract Format Comparison (Updated 2026)

| Feature | ODCS v3.1 | Soda Contracts (v4) | dbt Contracts |
|---------|-----------|---------------------|---------------|
| Format | YAML (12 sections) | Data Contracts YAML | YAML (schema.yml) |
| Schema enforcement | Full + relationships | Full (default in v4) | Column types + tests |
| SLA support | Built-in (section 10) | Via checks | Via source freshness |
| Versioning | Semantic versions | Implicit | Model versions |
| CI/CD integration | `datacontract lint` | `soda contract verify` | `dbt build` |
| Relationships | Yes (FK, v3.1) | No | ref() only |
| Validation | JSON Schema (stricter in v3.1) | Runtime verification | Build-time |
| AI features | None | Natural language rules (v4) | None |
| Multi-source | Platform-agnostic | Postgres, Snowflake, BQ, Databricks, Fabric, DuckDB | dbt-supported sources |

## What's New (2025-2026)

| Tool | Feature | Date |
|------|---------|------|
| Soda | **v4.0** -- Data Contracts as default, AI-translated rules, plugin system | Jan 2026 |
| Soda | v4 supports: Postgres, Snowflake, BigQuery, Databricks, Fabric, DuckDB | Feb 2026 |
| GX | **ExpectAI** -- auto-generate data quality rules from dataset patterns | Feb 2025 |
| GX | Atlan App Framework partnership | Aug 2025 |
| GX | GX Core 1.3.10 latest stable release | Mar 2025 |
| GX | GX Core 0.18 retired (Oct 2025) | Oct 2025 |
| ODCS | **v3.1.0** -- Relationships, stricter validation, richer metadata | Dec 2025 |
| Observability | Gartner: 50% of enterprises to adopt data observability by 2026 | 2025 |
| Elementary | dbt-native observability gaining traction (OSS) | 2025 |

## Common Pitfalls

| Don't | Do |
|-------|-----|
| Ship models without tests | Minimum: unique + not_null on PK |
| Ignore freshness | Monitor with `dbt source freshness` or Soda |
| Test only happy path | Include edge cases: 0 rows, all nulls |
| Contracts without owners | Every contract needs `owner: team@email` |
| Use Soda v3 checks for new projects | Adopt Soda v4 Data Contracts syntax |
| Manually craft all GX expectations | Use ExpectAI to bootstrap from data patterns |
| Ignore ODCS v3.1 relationships | Define FK relationships in contracts for lineage |
