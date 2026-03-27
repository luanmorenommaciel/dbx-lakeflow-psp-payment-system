# Nested Models

> **Purpose**: Composing hierarchical data structures with nested Pydantic models
> **Confidence**: 0.95
> **MCP Validated**: 2026-02-17

## Overview

Pydantic supports nested model composition by using BaseModel subclasses as field types.
This creates validated hierarchical structures ideal for complex data extraction from LLMs.
Nested models are validated recursively -- if any nested field fails validation, the entire
parent model raises a ValidationError with the full path to the error.

## The Pattern

```python
from pydantic import BaseModel, Field
from typing import Optional
from datetime import date


class Address(BaseModel):
    street: str = Field(..., description="Street address")
    city: str = Field(..., description="City name")
    state: Optional[str] = None
    postal_code: str = Field(..., description="Postal/ZIP code")
    country: str = Field(default="US", description="ISO country code")


class LineItem(BaseModel):
    description: str = Field(..., min_length=1)
    quantity: int = Field(default=1, ge=1)
    unit_price: float = Field(..., gt=0)

    @property
    def total(self) -> float:
        return round(self.quantity * self.unit_price, 2)


class Invoice(BaseModel):
    invoice_number: str
    vendor_name: str
    vendor_address: Address
    billing_address: Optional[Address] = None
    line_items: list[LineItem] = Field(..., min_length=1)
    issue_date: date
    notes: Optional[str] = None

    @property
    def total_amount(self) -> float:
        return round(sum(item.total for item in self.line_items), 2)
```

## Quick Reference

| Pattern | Syntax | Validates |
|---------|--------|-----------|
| Required nested | `address: Address` | Must provide valid Address |
| Optional nested | `address: Optional[Address] = None` | Can be None |
| List of models | `items: list[LineItem]` | Each item validated |
| Dict with models | `data: dict[str, LineItem]` | Values validated |
| Nested in nested | `order: Order` (Order has Address) | Recursive validation |

## Parsing Nested JSON

```python
import json

# LLM returns nested JSON
llm_response = '''
{
    "invoice_number": "INV-2026-001",
    "vendor_name": "Acme Corp",
    "vendor_address": {
        "street": "123 Main St",
        "city": "Springfield",
        "postal_code": "62701"
    },
    "line_items": [
        {"description": "Widget A", "quantity": 5, "unit_price": 19.99},
        {"description": "Widget B", "quantity": 2, "unit_price": 49.99}
    ],
    "issue_date": "2026-02-15"
}
'''

invoice = Invoice.model_validate_json(llm_response)
print(invoice.vendor_address.city)   # "Springfield"
print(invoice.line_items[0].total)   # 99.95
print(invoice.total_amount)          # 199.93
```

## Error Paths in Nested Models

```python
from pydantic import ValidationError

try:
    Invoice.model_validate_json('{"invoice_number": "X", "vendor_name": "V", '
        '"vendor_address": {"street": "S", "city": "C", "postal_code": ""}, '
        '"line_items": [{"description": "", "unit_price": -1}], '
        '"issue_date": "2026-01-01"}')
except ValidationError as e:
    for error in e.errors():
        print(error["loc"], error["msg"])
        # ('vendor_address', 'postal_code') String should have at least 1 character
        # ('line_items', 0, 'description') String should have at least 1 character
        # ('line_items', 0, 'unit_price') Input should be greater than 0
```

## Common Mistakes

### Wrong (flat structure, no reuse)

```python
class Invoice(BaseModel):
    vendor_street: str
    vendor_city: str
    vendor_postal: str
    billing_street: Optional[str] = None
    billing_city: Optional[str] = None
```

### Correct (nested, reusable models)

```python
class Address(BaseModel):
    street: str
    city: str
    postal_code: str

class Invoice(BaseModel):
    vendor_address: Address
    billing_address: Optional[Address] = None
```

## Related

- [BaseModel](../concepts/base-model.md)
- [Field Types](../concepts/field-types.md)
- [Extraction Schema](../patterns/extraction-schema.md)
