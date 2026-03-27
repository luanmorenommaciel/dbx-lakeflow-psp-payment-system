# Field Types

> **Purpose**: Type annotations, Optional, Annotated, Field constraints in Pydantic v2
> **Confidence**: 0.95
> **MCP Validated**: 2026-02-17

## Overview

Pydantic v2 uses Python type annotations to define field types and applies validation
automatically. The `Field()` function adds metadata, constraints, and descriptions.
The `Annotated` type allows attaching validators and constraints directly to types,
making them reusable across models.

## The Pattern

```python
from typing import Annotated, Literal, Optional
from pydantic import BaseModel, Field
from datetime import date


# Reusable annotated types
NonEmptyStr = Annotated[str, Field(min_length=1, strip_whitespace=True)]
PositiveFloat = Annotated[float, Field(gt=0)]
CurrencyCode = Annotated[str, Field(pattern=r"^[A-Z]{3}$")]


class LineItem(BaseModel):
    description: NonEmptyStr = Field(..., description="Item description")
    quantity: Annotated[int, Field(ge=1)] = 1
    unit_price: PositiveFloat
    category: Optional[str] = None


class ExtractionResult(BaseModel):
    confidence: Annotated[float, Field(ge=0.0, le=1.0)]
    source: Literal["invoice", "receipt", "contract"]
    extracted_date: Optional[date] = None
    items: list[LineItem] = Field(default_factory=list)
    tags: set[str] = Field(default_factory=set)
```

## Quick Reference

| Type Annotation | Meaning | Example Value |
|-----------------|---------|---------------|
| `str` | Required string | `"hello"` |
| `Optional[str]` | String or None (must set `= None`) | `None` |
| `int` | Required integer | `42` |
| `float` | Required float (accepts int too) | `3.14` |
| `bool` | Boolean | `True` |
| `list[str]` | List of strings | `["a", "b"]` |
| `dict[str, Any]` | Dictionary | `{"key": "val"}` |
| `set[str]` | Unique set of strings | `{"a", "b"}` |
| `Literal["a", "b"]` | Constrained choices | `"a"` |
| `date` / `datetime` | Date objects | `"2026-01-15"` |
| `Annotated[str, Field()]` | String with constraints | `"constrained"` |

## Field() Constraints

| Parameter | Types | Purpose | Example |
|-----------|-------|---------|---------|
| `min_length` | str, list | Minimum length | `Field(min_length=1)` |
| `max_length` | str, list | Maximum length | `Field(max_length=100)` |
| `pattern` | str | Regex pattern | `Field(pattern=r"^\d+$")` |
| `gt` / `ge` | int, float | Greater than / or equal | `Field(gt=0)` |
| `lt` / `le` | int, float | Less than / or equal | `Field(le=100)` |
| `multiple_of` | int, float | Must be multiple of | `Field(multiple_of=5)` |
| `description` | Any | Field description for schema | `Field(description="...")` |
| `alias` | Any | Alternative name for parsing | `Field(alias="fieldName")` |
| `default` | Any | Default value | `Field(default="USD")` |
| `default_factory` | Any | Factory for mutable defaults | `Field(default_factory=list)` |
| `exclude` | Any | Exclude from serialization | `Field(exclude=True)` |

## Common Mistakes

### Wrong (implicit None default removed in v2)

```python
class Model(BaseModel):
    # In v2, Optional does NOT auto-set default to None
    name: Optional[str]  # REQUIRED field that accepts None
```

### Correct (explicit default)

```python
class Model(BaseModel):
    name: Optional[str] = None  # Optional with explicit default
    label: str = "default"      # Has default value
    value: str                   # Truly required
```

## LLM Schema Generation

```python
import json

class Entity(BaseModel):
    name: NonEmptyStr = Field(..., description="Entity name")
    entity_type: Literal["person", "org", "location"]
    confidence: Annotated[float, Field(ge=0, le=1, description="Extraction confidence")]

# Field descriptions appear in JSON Schema for LLM prompts
schema = Entity.model_json_schema()
# {"properties": {"name": {"description": "Entity name", ...}}}
```

## Related

- [BaseModel](../concepts/base-model.md)
- [Validators](../concepts/validators.md)
- [Extraction Schema](../patterns/extraction-schema.md)
