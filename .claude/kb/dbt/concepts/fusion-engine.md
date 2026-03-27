# Fusion Engine

> **Purpose**: Ground-up Rust rewrite of dbt Core execution engine with multi-dialect SQL comprehension
> **Confidence**: 0.90
> **MCP Validated**: 2026-03-26 | Updated with latest Fusion architecture details

## Overview

dbt Labs acquired SDF Labs (a Rust-based SQL compiler) in late 2024, creating the **Fusion Engine**. It is a ground-up, first-principles rewrite of the dbt Core execution engine in Rust, achieving sub-second parse times for projects with 10,000+ models. Beyond speed, Fusion introduces **multi-dialect SQL compilation and validation**, **Arrow Database Connectivity (ADBC) drivers** for faster data transfer, a **Language Server Protocol (LSP)** for IDE integration, and **automatic dependency management** (no JVM or Python required). Fusion enforces the dbt authoring spec more strictly than Core for correctness, and discrepancies can be auto-fixed with the `dbt-autofix` tool.

## The Concept

```yaml
# dbt_project.yml — works with both Core and Fusion engines
name: my_analytics
version: "1.0.0"
config-version: 2

# Key Fusion capabilities:
# - Sub-second parsing (Rust-based SQL compiler)
# - Column-level lineage without extra config
# - Multi-dialect SQL compilation, validation, and static analysis
# - ADBC drivers for improved data transfer and connection handling
# - Language Server with VS Code extension for IDE-grade autocomplete
# - Automatic .env file loading
# - Standalone binary — no JVM or Python runtime required
# - Automatic dependency installation (packages + database drivers)
# - dbt code-signed and secure distributions

# Architecture (modular Rust crates):
#   dbt-adapter    — database operations
#   dbt-parser     — project resolution
#   dbt-jinja      — MiniJinja template rendering (Jinja2-compatible)
#   dbt-schemas    — manifest and artifact handling
#   dbt-xdbc       — database connectivity (ADBC)
```

## Quick Reference

| Feature | dbt Core (Python) | dbt Fusion (Rust) |
|---------|-------------------|-------------------|
| Parse 10K models | 30-60 seconds | < 1 second |
| Column-level lineage | Requires dbt Cloud | Built-in |
| IDE autocomplete | Basic (extension) | Full LSP server (VS Code extension) |
| Multi-dialect SQL | No | Compiles, validates, and analyzes across dialects |
| .env file support | Manual loading | Automatic |
| Cross-project refs | dbt Mesh (separate) | Integrated |
| Database connectivity | Python DB-API | ADBC (Arrow-native, faster transfers) |
| Dependency install | Manual (pip) | Automatic (drivers + packages) |
| Runtime requirement | Python | Standalone binary (no JVM/Python) |
| Jinja rendering | Python Jinja2 | MiniJinja (Rust, Jinja2-compatible) |
| Status | GA, stable | Beta (GA expected mid-2026) |

### Migration: Core to Fusion

| Step | Details |
|------|---------|
| 1. Validate | Run `dbt-autofix` to resolve strict-mode discrepancies |
| 2. Test in dev | Compare `dbt compile` output between Core and Fusion |
| 3. Check YAML | Fusion validates YAML configs more strictly than Core |
| 4. Verify macros | MiniJinja is Jinja2-compatible but edge cases may differ |
| 5. Promote | Migrate to Fusion only after output parity confirmed |

## Common Mistakes

### Wrong

```bash
# Assuming Fusion is a drop-in replacement today
# Fusion is in beta — test before migrating production
dbt run --target prod  # on Fusion CLI without testing
```

### Correct

```bash
# Test Fusion in dev first, compare output with Core
dbt compile --target dev  # Fusion CLI
dbt compile --target dev  # Core CLI
diff target/compiled/     # Compare generated SQL

# Migrate to Fusion only after output parity confirmed
```

## Related

- [mesh-architecture](../concepts/mesh-architecture.md)
- [model-types](../concepts/model-types.md)
