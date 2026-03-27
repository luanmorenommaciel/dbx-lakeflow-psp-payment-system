# Document Extraction Pattern

> **Purpose**: Production-ready pattern for extracting structured data from documents (invoices, contracts, reports)
> **MCP Validated:** 2026-03-26

## When to Use

- Extracting typed fields from PDF/image documents
- Processing invoices, receipts, contracts, or forms
- Building automated document processing pipelines
- Any task requiring schema-validated extraction from unstructured text

## Implementation

```python
import json
from pydantic import BaseModel, Field, ValidationError
from typing import Optional, List
from openai import OpenAI

client = OpenAI()


# --- Schema Definition ---

class LineItem(BaseModel):
    description: str = Field(description="Item description")
    quantity: Optional[float] = Field(None, description="Quantity as number")
    unit_price: Optional[float] = Field(None, description="Unit price without currency")
    total: Optional[float] = Field(None, description="Line total without currency")

class DocumentExtraction(BaseModel):
    document_type: str = Field(description="invoice | receipt | contract | form")
    document_number: Optional[str] = Field(None, description="Document ID or number")
    date: Optional[str] = Field(None, description="Document date in YYYY-MM-DD")
    issuer_name: Optional[str] = Field(None, description="Company or person who issued")
    recipient_name: Optional[str] = Field(None, description="Recipient name")
    total_amount: Optional[float] = Field(None, description="Total amount as number")
    currency: Optional[str] = Field(None, description="ISO 4217 currency code")
    line_items: List[LineItem] = Field(default_factory=list)
    notes: Optional[str] = Field(None, description="Additional notes or flags")


# --- Extraction Prompt ---

DOCUMENT_EXTRACTION_PROMPT = """You are an expert document data extraction system.

## Task
Extract all relevant fields from the document below into structured JSON.

## Schema
{schema}

## Rules
1. Extract ONLY data explicitly present in the document
2. If a field is not found, set it to null
3. Dates: Convert to ISO 8601 (YYYY-MM-DD)
4. Amounts: Numeric only, no currency symbols or commas
5. Currency: Use ISO 4217 codes (USD, EUR, BRL, etc.)
6. Line items: Extract all individual items if present
7. NEVER fabricate or infer data not in the document

## Document Content
{document_text}

## Output
Return ONLY a valid JSON object matching the schema above.
"""


# --- Extraction Function ---

def extract_document(
    document_text: str,
    max_retries: int = 2
) -> DocumentExtraction:
    """Extract structured data from a document with retry logic."""

    schema = json.dumps(DocumentExtraction.model_json_schema(), indent=2)

    for attempt in range(max_retries + 1):
        try:
            response = client.chat.completions.create(
                model="gpt-4o",
                temperature=0.0,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a precise document extraction system. "
                                   "Return only valid JSON."
                    },
                    {
                        "role": "user",
                        "content": DOCUMENT_EXTRACTION_PROMPT.format(
                            schema=schema,
                            document_text=document_text
                        )
                    }
                ],
                response_format={"type": "json_object"}
            )

            raw_json = response.choices[0].message.content
            return DocumentExtraction.model_validate_json(raw_json)

        except ValidationError as e:
            if attempt == max_retries:
                raise ValueError(
                    f"Extraction failed after {max_retries + 1} attempts: {e}"
                )
            # Log and retry
            print(f"Validation error (attempt {attempt + 1}): {e}")

    raise ValueError("Unreachable")
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `model` | `gpt-4o` | Model with strong extraction capability |
| `temperature` | `0.0` | Deterministic extraction |
| `max_retries` | `2` | Retry on validation failure |
| `response_format` | `json_object` | API-level JSON enforcement |

## Example Usage

```python
sample_invoice = """
INVOICE #INV-2024-0847
Date: January 15, 2024

From: Acme Corp
To: Widget Inc

Items:
1. Cloud Hosting (Monthly) - 3 units @ $299.00 = $897.00
2. SSL Certificate - 1 unit @ $49.99 = $49.99
3. Support Plan (Premium) - 1 unit @ $199.00 = $199.00

Subtotal: $1,145.99
Tax (8%): $91.68
Total: $1,237.67 USD
"""

result = extract_document(sample_invoice)
print(result.model_dump_json(indent=2))
# {
#   "document_type": "invoice",
#   "document_number": "INV-2024-0847",
#   "date": "2024-01-15",
#   "issuer_name": "Acme Corp",
#   "recipient_name": "Widget Inc",
#   "total_amount": 1237.67,
#   "currency": "USD",
#   "line_items": [...],
#   "notes": null
# }
```

## See Also

- [Structured Extraction Concept](../concepts/structured-extraction.md)
- [Multi-Pass Extraction](../patterns/multi-pass-extraction.md)
- [Validation Prompts](../patterns/validation-prompts.md)
