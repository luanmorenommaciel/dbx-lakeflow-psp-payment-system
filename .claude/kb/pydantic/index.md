# Pydantic Knowledge Base

> **Purpose**: Pydantic V2 data validation, computed fields, TypeAdapter, discriminated unions, settings, LLM output
> **MCP Validated**: 2026-03-26

## Quick Navigation

### Concepts (< 150 lines each)

| File | Purpose |
|------|---------|
| [concepts/base-model.md](concepts/base-model.md) | BaseModel fundamentals, model methods, serialization |
| [concepts/field-types.md](concepts/field-types.md) | Field types, Optional, Annotated, constraints |
| [concepts/validators.md](concepts/validators.md) | field_validator, model_validator, modes |
| [concepts/nested-models.md](concepts/nested-models.md) | Nested model composition, recursive structures |

### Patterns (< 200 lines each)

| File | Purpose |
|------|---------|
| [patterns/llm-output-validation.md](patterns/llm-output-validation.md) | Validate and parse LLM JSON responses |
| [patterns/extraction-schema.md](patterns/extraction-schema.md) | Build schemas for document data extraction |
| [patterns/error-handling.md](patterns/error-handling.md) | Handle ValidationError, retries, fallbacks |
| [patterns/custom-validators.md](patterns/custom-validators.md) | Reusable custom validation logic |

### Specs (Machine-Readable)

| File | Purpose |
|------|---------|
| [specs/invoice-schema.yaml](specs/invoice-schema.yaml) | Invoice extraction Pydantic schema spec |

---

## Quick Reference

- [quick-reference.md](quick-reference.md) - Fast lookup tables

---

## Key Concepts

| Concept | Description |
|---------|-------------|
| **BaseModel** | Core class for defining data schemas with automatic validation |
| **Field Types** | Type annotations with Optional, Annotated, and Field constraints |
| **Validators** | Decorators for custom field-level and model-level validation |
| **Nested Models** | Composable hierarchical data structures |
| **Computed Fields** | `@computed_field` for derived properties included in serialization/schema |
| **TypeAdapter** | Validate non-BaseModel types (lists, dicts, unions) |
| **Discriminated Unions** | `Field(discriminator=...)` for efficient tagged union validation |
| **Pydantic Settings** | `BaseSettings` for env vars, .env, TOML, YAML config loading |

---

## Learning Path

| Level | Files |
|-------|-------|
| **Beginner** | concepts/base-model.md, concepts/field-types.md |
| **Intermediate** | concepts/validators.md, patterns/llm-output-validation.md |
| **Advanced** | patterns/extraction-schema.md, patterns/custom-validators.md |

---

## Agent Usage

| Agent | Primary Files | Use Case |
|-------|---------------|----------|
| ai-prompt-specialist-gcp | patterns/llm-output-validation.md, patterns/extraction-schema.md | Define Pydantic schemas for LLM structured output |
| python-developer | concepts/base-model.md, concepts/validators.md | Build validated data models |
| data-quality-analyst | patterns/custom-validators.md | Define data quality checks with Pydantic |
