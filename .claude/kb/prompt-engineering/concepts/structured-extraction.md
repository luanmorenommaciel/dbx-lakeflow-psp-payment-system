# Structured Extraction

> **Purpose**: Extract typed, validated data fields from unstructured documents using LLM prompts and schema enforcement
> **Confidence**: 0.95
> **MCP Validated:** 2026-03-26

## Overview

Structured extraction combines precise prompt instructions with schema enforcement to pull specific data fields from documents (invoices, contracts, reports). In 2026, the most reliable approach is native JSON Schema mode (OpenAI), tool-use-as-schema (Anthropic), or the Instructor library (any provider). Combined with Pydantic validation, this achieves over 99% schema adherence in production.

## The Pattern: Instructor (Recommended for 2026)

```python
import instructor
from pydantic import BaseModel, Field
from typing import Optional
from openai import OpenAI

# Instructor patches the client for native structured output
client = instructor.from_openai(OpenAI())

class Address(BaseModel):
    street: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None

class InvoiceData(BaseModel):
    invoice_number: str = Field(description="Unique invoice identifier")
    date: str = Field(description="Invoice date in ISO 8601 format")
    vendor_name: str = Field(description="Name of the vendor/supplier")
    total_amount: float = Field(description="Total amount without currency symbol")
    line_items: list[dict] = Field(default_factory=list)
    vendor_address: Optional[Address] = None

def extract_invoice(document_text: str) -> InvoiceData:
    """Extract invoice data with automatic retries and validation."""
    return client.chat.completions.create(
        model="gpt-4o",
        response_model=InvoiceData,  # Instructor handles schema + validation
        max_retries=2,
        messages=[{
            "role": "user",
            "content": f"Extract invoice data from:\n\n{document_text}"
        }],
    )
# Returns a validated InvoiceData object -- no manual JSON parsing needed
```

## The Pattern: Anthropic Tool-Use-as-Schema

```python
import anthropic

client = anthropic.Anthropic()

def extract_with_claude(document_text: str) -> dict:
    """Use Claude's tool_use to enforce output schema."""
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        tools=[{
            "name": "extract_invoice",
            "description": "Extract structured invoice data",
            "input_schema": InvoiceData.model_json_schema(),
        }],
        tool_choice={"type": "tool", "name": "extract_invoice"},
        messages=[{
            "role": "user",
            "content": f"Extract invoice data from:\n\n{document_text}"
        }],
    )
    tool_use = next(b for b in response.content if b.type == "tool_use")
    return InvoiceData.model_validate(tool_use.input)
```

## Quick Reference

| Method | Provider | Reliability | Notes |
|--------|----------|-------------|-------|
| Instructor library | Any | Highest | Pydantic-native, auto-retries, all providers |
| JSON Schema mode | OpenAI | Highest | `response_format={"type": "json_schema"}` |
| Tool-use-as-schema | Anthropic | Highest | Force tool call with output schema |
| JSON mode | OpenAI | High | Valid JSON but no schema enforcement |
| Prompt-only | Any | Medium | Relies on instruction following |

## Common Mistakes

### Wrong
```python
# Vague prompt, no schema, no validation
prompt = "Extract data from this invoice and return JSON."
```

### Correct
```python
# Instructor: Pydantic model = schema + validation + retries
result = client.chat.completions.create(
    model="gpt-4o",
    response_model=InvoiceData,  # enforces schema, validates, retries
    max_retries=2,
    messages=[{"role": "user", "content": f"Extract:\n\n{doc}"}],
)
```

## Extraction Pipeline

```text
Document --> Instructor/Tool-Use --> LLM --> Pydantic Validated Object
                                                   |
                                              On Error --> Auto-retry with error context
```

## Related

- [Output Formatting](../concepts/output-formatting.md)
- [Document Extraction Pattern](../patterns/document-extraction.md)
- [Multi-Pass Extraction](../patterns/multi-pass-extraction.md)
