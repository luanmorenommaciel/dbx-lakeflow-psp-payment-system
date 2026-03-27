# Extraction Schema

> **Purpose**: Design Pydantic schemas optimized for LLM-based document data extraction
> **MCP Validated**: 2026-02-17

## When to Use

- Building structured extraction pipelines for invoices, receipts, contracts
- Defining schemas that LLMs can reliably populate from unstructured text
- Creating reusable extraction models across different document types
- Generating JSON Schema instructions to embed in extraction prompts

## Implementation

```python
from pydantic import BaseModel, Field, model_validator
from typing import Annotated, Literal, Optional
from datetime import date
from enum import Enum


# --- Reusable annotated types for extraction ---
NonEmptyStr = Annotated[str, Field(min_length=1, strip_whitespace=True)]
Confidence = Annotated[float, Field(ge=0.0, le=1.0, description="Extraction confidence 0-1")]
MoneyAmount = Annotated[float, Field(ge=0, description="Monetary amount")]


class Currency(str, Enum):
    USD = "USD"
    EUR = "EUR"
    GBP = "GBP"
    BRL = "BRL"


class Address(BaseModel):
    """Postal address extracted from document."""
    street: Optional[str] = Field(None, description="Street address line")
    city: Optional[str] = Field(None, description="City name")
    state: Optional[str] = Field(None, description="State or province")
    postal_code: Optional[str] = Field(None, description="ZIP or postal code")
    country: str = Field(default="US", description="ISO 3166-1 alpha-2 country code")


class LineItem(BaseModel):
    """Single line item from an invoice or receipt."""
    description: NonEmptyStr = Field(..., description="Item or service description")
    quantity: float = Field(default=1.0, ge=0, description="Quantity")
    unit_price: MoneyAmount = Field(..., description="Price per unit")
    total: Optional[MoneyAmount] = Field(None, description="Line total if stated")

    @model_validator(mode="after")
    def compute_total(self) -> "LineItem":
        if self.total is None:
            self.total = round(self.quantity * self.unit_price, 2)
        return self


class InvoiceExtraction(BaseModel):
    """Complete invoice extraction schema for LLM output."""
    invoice_number: NonEmptyStr = Field(..., description="Invoice ID or number")
    vendor_name: NonEmptyStr = Field(..., description="Vendor or supplier name")
    vendor_address: Optional[Address] = Field(None, description="Vendor address")
    customer_name: Optional[str] = Field(None, description="Customer or buyer name")
    issue_date: date = Field(..., description="Invoice issue date (YYYY-MM-DD)")
    due_date: Optional[date] = Field(None, description="Payment due date")
    currency: Currency = Field(default=Currency.USD, description="Currency code")
    line_items: list[LineItem] = Field(..., min_length=1, description="Invoice line items")
    subtotal: Optional[MoneyAmount] = None
    tax_amount: Optional[MoneyAmount] = Field(default=0.0)
    total_amount: MoneyAmount = Field(..., description="Total invoice amount")
    extraction_confidence: Confidence = Field(
        default=0.5, description="Overall extraction confidence"
    )

    @model_validator(mode="after")
    def validate_totals(self) -> "InvoiceExtraction":
        computed = sum(item.total or 0 for item in self.line_items)
        if self.subtotal is None:
            self.subtotal = round(computed, 2)
        return self
```

## Configuration

| Schema Design Rule | Rationale |
|--------------------|-----------|
| Use `Field(description=...)` on every field | Descriptions become LLM instructions via JSON Schema |
| Make non-essential fields `Optional` with defaults | LLMs may miss fields; graceful degradation |
| Use `Literal` or `Enum` for constrained values | Reduces LLM hallucination in category fields |
| Add `extraction_confidence` field | Lets LLM self-report uncertainty |
| Keep line items as `list[Model]` | Structured, validated nested extraction |

## Example Usage

```python
import json
from patterns.llm_output_validation import validate_llm_output, build_format_instruction


# Build extraction prompt
schema_instructions = build_format_instruction(InvoiceExtraction)
prompt = f"""Extract all invoice data from the following document text.

DOCUMENT:
\"\"\"
Invoice #2026-0142
From: DataFlow Corp, 456 Tech Ave, Austin TX 78701
To: Acme Industries
Date: February 10, 2026
Due: March 10, 2026

Items:
- Cloud Processing (10 hours @ $150/hr): $1,500.00
- Data Storage (500GB @ $0.10/GB): $50.00

Subtotal: $1,550.00
Tax (8.25%): $127.88
Total: $1,677.88
\"\"\"

{schema_instructions}"""

# Parse LLM response
llm_json = call_llm(prompt, temperature=0.0)
invoice = validate_llm_output(llm_json, InvoiceExtraction)

# Access structured data
print(f"Invoice: {invoice.invoice_number}")
print(f"Vendor: {invoice.vendor_name}")
print(f"Items: {len(invoice.line_items)}")
print(f"Total: {invoice.currency.value} {invoice.total_amount}")
```

## Schema Design Checklist

```text
[ ] Every field has a description (for JSON Schema generation)
[ ] Non-critical fields are Optional with defaults
[ ] Enum/Literal used for categorical fields
[ ] Nested models for structured sub-objects
[ ] model_validator for cross-field consistency
[ ] Confidence field for LLM self-assessment
[ ] Monetary fields use float with ge=0
[ ] Date fields use date type (YYYY-MM-DD)
```

## See Also

- [LLM Output Validation](../patterns/llm-output-validation.md)
- [Nested Models](../concepts/nested-models.md)
- [Invoice Schema Spec](../specs/invoice-schema.yaml)
