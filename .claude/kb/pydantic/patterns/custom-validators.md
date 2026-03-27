# Custom Validators

> **Purpose**: Build reusable custom validation logic for LLM extraction schemas
> **MCP Validated**: 2026-02-17

## When to Use

- Enforcing domain-specific business rules on extracted data
- Creating reusable validation types across multiple models
- Normalizing LLM output (case, whitespace, format inconsistencies)
- Validating cross-field dependencies in extraction results

## Implementation

```python
from typing import Annotated, Any
from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic.functional_validators import AfterValidator, BeforeValidator
from datetime import date, timedelta
import re


# --- Reusable Annotated Validators ---

def strip_and_title(v: str) -> str:
    """Normalize names: strip whitespace, title case."""
    return " ".join(v.split()).title()


def normalize_currency_code(v: str) -> str:
    """Ensure 3-letter uppercase currency code."""
    v = v.strip().upper()
    if len(v) != 3 or not v.isalpha():
        raise ValueError(f"Invalid currency code: {v}")
    return v


def validate_date_not_future(v: date) -> date:
    """Ensure date is not in the future."""
    if v > date.today():
        raise ValueError(f"Date {v} is in the future")
    return v


def validate_email(v: str) -> str:
    """Basic email format validation."""
    v = v.strip().lower()
    if not re.match(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", v):
        raise ValueError(f"Invalid email: {v}")
    return v


def coerce_to_float(v: Any) -> float:
    """Coerce string numbers, removing currency symbols."""
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        cleaned = re.sub(r"[^\d.\-]", "", v.strip())
        if cleaned:
            return float(cleaned)
    raise ValueError(f"Cannot convert to float: {v}")


# --- Reusable Annotated Types ---
PersonName = Annotated[str, AfterValidator(strip_and_title)]
CurrencyCode = Annotated[str, AfterValidator(normalize_currency_code)]
PastDate = Annotated[date, AfterValidator(validate_date_not_future)]
Email = Annotated[str, AfterValidator(validate_email)]
CoercedFloat = Annotated[float, BeforeValidator(coerce_to_float)]
```

## Configuration

| Validator Type | When to Use | Performance |
|---------------|-------------|-------------|
| `AfterValidator` | Post-coercion rules | Fast (runs after type check) |
| `BeforeValidator` | Pre-coercion normalization | Runs on raw input |
| `@field_validator` | Model-specific logic | Tied to specific model |
| `@model_validator` | Cross-field rules | Access to all fields |

## Example Usage

```python
class ContactExtraction(BaseModel):
    """Extract contact information from documents."""
    name: PersonName = Field(..., description="Person's full name")
    email: Email = Field(..., description="Email address")
    company: PersonName = Field(..., description="Company name")
    revenue: CoercedFloat = Field(..., description="Annual revenue")

    @field_validator("name")
    @classmethod
    def name_must_have_parts(cls, v: str) -> str:
        if len(v.split()) < 2:
            raise ValueError("Full name must have at least first and last name")
        return v


# LLM might return messy data -- validators clean it up
data = {
    "name": "  john   DOE  ",       # -> "John Doe"
    "email": " John@ACME.com ",     # -> "john@acme.com"
    "company": "acme corp",          # -> "Acme Corp"
    "revenue": "$1,500,000.00",      # -> 1500000.0
}
contact = ContactExtraction.model_validate(data)
print(contact.name)     # "John Doe"
print(contact.email)    # "john@acme.com"
print(contact.revenue)  # 1500000.0


# --- Cross-field validator example ---
class DateRange(BaseModel):
    """Validated date range extracted from documents."""
    start_date: PastDate = Field(..., description="Start date")
    end_date: date = Field(..., description="End date")
    duration_days: int = Field(default=0, description="Duration in days")

    @model_validator(mode="after")
    def validate_range(self) -> "DateRange":
        if self.end_date < self.start_date:
            raise ValueError("end_date must be after start_date")
        self.duration_days = (self.end_date - self.start_date).days
        return self


# --- Composing validators on a single field ---
CleanUpperStr = Annotated[
    str,
    BeforeValidator(lambda v: str(v).strip()),
    AfterValidator(lambda v: v.upper()),
]


class DocumentCode(BaseModel):
    code: CleanUpperStr = Field(..., description="Document reference code")
    # Input: "  abc-123  " -> "ABC-123"
```

## Validator Composition Rules

| Rule | Example |
|------|---------|
| BeforeValidator runs first | Raw input normalization |
| Multiple validators chain | `Annotated[str, Before(...), After(...)]` |
| AfterValidator gets typed value | Already coerced to declared type |
| Raise ValueError to reject | `raise ValueError("reason")` |
| Return value to accept | `return transformed_value` |

## See Also

- [Validators](../concepts/validators.md)
- [Field Types](../concepts/field-types.md)
- [Error Handling](../patterns/error-handling.md)
