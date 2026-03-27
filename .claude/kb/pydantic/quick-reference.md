# Pydantic Quick Reference

> Fast lookup tables. For code examples, see linked files.
> **MCP Validated:** 2026-03-26

## Core Model Methods

| Method | Input | Output | Notes |
|--------|-------|--------|-------|
| `Model(**data)` | kwargs | Model instance | Validates on creation |
| `model_validate(obj)` | dict | Model instance | Parse dict into model |
| `model_validate_json(json_str)` | str | Model instance | Parse JSON string |
| `model_dump()` | -- | dict | Serialize to dict |
| `model_dump_json()` | -- | str | Serialize to JSON |
| `model_json_schema()` | -- | dict | Get JSON Schema |
| `model_copy(update={})` | dict | Model instance | Clone with overrides |

## Field Type Patterns

| Pattern | Syntax | Use Case |
|---------|--------|----------|
| Required | `name: str` | Field must be provided |
| Optional | `name: Optional[str] = None` | Field can be missing |
| Default | `name: str = "default"` | Has fallback value |
| Constrained | `name: Annotated[str, Field(min_length=1)]` | With validation |
| List | `items: list[Item]` | Collection of models |
| Literal | `status: Literal["ok", "error"]` | Enum-like choices |

## Validator Modes

| Decorator | Mode | Receives | Use Case |
|-----------|------|----------|----------|
| `@field_validator` | `"before"` | Raw input | Coerce types |
| `@field_validator` | `"after"` | Validated value | Post-validation logic |
| `@field_validator` | `"wrap"` | value + handler | Control validation |
| `@model_validator` | `"before"` | Raw dict | Pre-process all data |
| `@model_validator` | `"after"` | Model instance | Cross-field validation |

## Computed Fields

| Pattern | Syntax | Notes |
|---------|--------|-------|
| Derived value | `@computed_field` + `@property` | Included in `model_dump()` and JSON schema |
| Read-only in schema | Auto-marked `readOnly` | In serialization-mode schema |
| Cached computed | `@computed_field` + `@cached_property` | Computed once, cached |

## TypeAdapter (Non-BaseModel Validation)

| Method | Input | Output |
|--------|-------|--------|
| `TypeAdapter(list[int])` | type | adapter instance |
| `adapter.validate_python(data)` | Python object | validated value |
| `adapter.validate_json(json_str)` | JSON string | validated value |
| `adapter.json_schema()` | -- | JSON Schema dict |
| `adapter.dump_python(value)` | validated value | Python object |
| `adapter.dump_json(value)` | validated value | JSON bytes |

## Discriminated Unions

| Pattern | Syntax | Use Case |
|---------|--------|----------|
| Tagged union | `Field(discriminator='type')` | Polymorphic models with type tag |
| Literal discriminator | `type: Literal['cat']` on each variant | Efficient variant selection |
| Union mode | `union_mode='left_to_right'` | Explicit validation order |
| Smart mode | Default for untagged unions | Pydantic finds best match |

## Pydantic Settings (`pydantic-settings`)

| Feature | Syntax | Notes |
|---------|--------|-------|
| Env vars | `class S(BaseSettings):` | Auto-loads from environment |
| Prefix | `env_prefix='APP_'` | Namespace env vars |
| .env file | `env_file='.env'` | Load from dotenv file |
| Multiple .env | `env_file=('.env', '.env.prod')` | Later files override |
| TOML config | `toml_file='config.toml'` | Requires `settings_customise_sources` |
| YAML config | `yaml_file='config.yaml'` | Requires `YamlConfigSettingsSource` |
| Nested | Use `BaseModel` sub-models | Maps `DB_HOST` to `db.host` |

## Decision Matrix

| Use Case | Choose |
|----------|--------|
| Parse LLM JSON output | `model_validate_json()` |
| Define extraction schema | BaseModel + Field descriptions |
| Cross-field validation | `@model_validator(mode="after")` |
| Type coercion before validation | `@field_validator(mode="before")` |
| Reusable constraint | `Annotated[type, AfterValidator(fn)]` |
| Generate prompt instructions | `Model.model_json_schema()` |
| Derived/calculated field | `@computed_field` + `@property` |
| Validate non-model types | `TypeAdapter(list[int])` |
| Polymorphic JSON parsing | Discriminated union with `Literal` tag |
| App config from env/files | `BaseSettings` from `pydantic-settings` |

## Common Pitfalls

| Don't | Do |
|-------|-----|
| `name: Optional[str]` (no default) | `name: Optional[str] = None` |
| `@validator` (v1 syntax) | `@field_validator` (v2 syntax) |
| `.dict()` / `.json()` (v1) | `.model_dump()` / `.model_dump_json()` (v2) |
| `Config` inner class (v1) | `model_config = ConfigDict(...)` (v2) |
| Trust raw LLM output | Always validate with `model_validate_json()` |
| `parse_obj_as()` (v1) | `TypeAdapter(type).validate_python()` (v2) |
| `schema_of()` (v1) | `TypeAdapter(type).json_schema()` (v2) |
| Untagged union (slow) | Discriminated union with `Literal` tag (fast) |
| Manual env parsing | `BaseSettings` from `pydantic-settings` |

## Related Documentation

| Topic | Path |
|-------|------|
| BaseModel basics | `concepts/base-model.md` |
| Validators deep dive | `concepts/validators.md` |
| LLM output parsing | `patterns/llm-output-validation.md` |
| Full Index | `index.md` |
