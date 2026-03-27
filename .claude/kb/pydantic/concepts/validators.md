# Validators

> **Purpose**: field_validator, model_validator decorators and validation modes in Pydantic v2
> **Confidence**: 0.95
> **MCP Validated**: 2026-03-26

## Overview

Pydantic v2 provides two decorator-based validators: `@field_validator` for single-field
validation and `@model_validator` for cross-field logic. Each supports `mode="before"` (raw
input), `mode="after"` (validated data), and `mode="wrap"` (control flow). Validators raise
`ValueError` or `AssertionError` to reject data and return the value to accept it.

## The Pattern

```python
from pydantic import BaseModel, field_validator, model_validator, Field
from typing import Optional
from datetime import date


class InvoiceExtraction(BaseModel):
    invoice_number: str
    vendor_name: str
    issue_date: date
    due_date: Optional[date] = None
    subtotal: float = Field(gt=0)
    tax: float = Field(ge=0)
    total: float = Field(gt=0)

    @field_validator("invoice_number")
    @classmethod
    def validate_invoice_number(cls, v: str) -> str:
        v = v.strip().upper()
        if len(v) < 3:
            raise ValueError("Invoice number must be at least 3 characters")
        return v

    @field_validator("vendor_name", mode="before")
    @classmethod
    def coerce_vendor_name(cls, v) -> str:
        if isinstance(v, list):
            return " ".join(str(item) for item in v)
        return str(v).strip()

    @model_validator(mode="after")
    def check_dates_and_totals(self) -> "InvoiceExtraction":
        if self.due_date and self.due_date < self.issue_date:
            raise ValueError("due_date cannot be before issue_date")
        expected_total = round(self.subtotal + self.tax, 2)
        if abs(self.total - expected_total) > 0.01:
            raise ValueError(
                f"total ({self.total}) != subtotal + tax ({expected_total})"
            )
        return self
```

## Validator Modes

| Decorator | Mode | Input Type | When It Runs | Use Case |
|-----------|------|------------|-------------|----------|
| `@field_validator` | `"before"` | Raw input (Any) | Before type coercion | Coerce/normalize data |
| `@field_validator` | `"after"` | Validated type | After coercion (default) | Business rules |
| `@field_validator` | `"wrap"` | value + handler | Wraps inner validation | Conditional validation |
| `@model_validator` | `"before"` | Raw dict (Any) | Before all field validation | Pre-process payload |
| `@model_validator` | `"after"` | Model instance | After all fields validated | Cross-field logic |

## Quick Reference

| Pattern | Syntax |
|---------|--------|
| Validate one field | `@field_validator("field_name")` |
| Validate multiple fields | `@field_validator("field_a", "field_b")` |
| Access all fields | `@model_validator(mode="after")` |
| Pre-process raw input | `@model_validator(mode="before")` |
| Must be classmethod | `@classmethod` (required for field_validator) |
| Reject value | `raise ValueError("message")` |
| Accept value | `return value` |

## Common Mistakes

### Wrong (v1 syntax)

```python
from pydantic import validator  # DEPRECATED in v2

class Model(BaseModel):
    name: str

    @validator("name")  # v1 decorator
    def check_name(cls, v):
        return v.strip()
```

### Correct (v2 syntax)

```python
from pydantic import field_validator

class Model(BaseModel):
    name: str

    @field_validator("name")
    @classmethod
    def check_name(cls, v: str) -> str:
        return v.strip()
```

## Annotated Validators (Functional Style)

```python
from typing import Annotated
from pydantic import BaseModel
from pydantic.functional_validators import AfterValidator, BeforeValidator


def normalize_whitespace(v: str) -> str:
    return " ".join(v.split())


def ensure_uppercase(v: str) -> str:
    return v.upper()


CleanStr = Annotated[str, BeforeValidator(normalize_whitespace)]
UpperStr = Annotated[str, AfterValidator(ensure_uppercase)]


class Document(BaseModel):
    title: CleanStr
    code: UpperStr
```

## Related

- [BaseModel](../concepts/base-model.md)
- [Custom Validators](../patterns/custom-validators.md)
- [Error Handling](../patterns/error-handling.md)
