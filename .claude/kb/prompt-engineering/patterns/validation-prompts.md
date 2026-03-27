# Validation Prompts Pattern

> **Purpose**: Self-validation and self-checking prompts that verify LLM extraction accuracy before returning results
> **MCP Validated:** 2026-03-26

## When to Use

- High-stakes extraction where errors have real cost
- Financial data, medical records, legal documents
- When you need auditable confidence scores
- Reducing hallucination in extraction pipelines

## Implementation

```python
import json
from pydantic import BaseModel, Field
from typing import Optional, List
from openai import OpenAI

client = OpenAI()


class ValidationResult(BaseModel):
    field_name: str
    extracted_value: Optional[str]
    is_valid: bool
    confidence: float = Field(ge=0.0, le=1.0)
    evidence: Optional[str] = Field(None, description="Quote from source supporting value")
    correction: Optional[str] = Field(None, description="Corrected value if invalid")
    reason: Optional[str] = Field(None, description="Why invalid or low confidence")

class ValidatedExtraction(BaseModel):
    fields: List[ValidationResult]
    overall_confidence: float = Field(ge=0.0, le=1.0)
    flagged_issues: List[str] = Field(default_factory=list)


# --- Step 1: Extract ---

EXTRACT_PROMPT = """Extract these fields from the document:
{field_list}

Document:
{document}

Return JSON with field names as keys and extracted values."""


# --- Step 2: Validate ---

VALIDATION_PROMPT = """You are a quality assurance auditor. Your job is to verify
extracted data against the original document.

## Extracted Data
{extracted_data}

## Original Document
{document}

## Task
For EACH extracted field, verify:
1. Is the value actually present in the document?
2. Is it accurately extracted (no typos, correct format)?
3. Could it be confused with another value?

## Output Format
Return JSON with:
- fields: array of objects with field_name, extracted_value, is_valid (bool),
  confidence (0-1), evidence (quote from doc), correction (if wrong), reason
- overall_confidence: float 0-1
- flagged_issues: array of strings describing problems found

Be STRICT. Flag anything uncertain."""


def extract_and_validate(
    document: str,
    fields: List[str],
    confidence_threshold: float = 0.85
) -> ValidatedExtraction:
    """Two-pass extraction: extract then validate."""

    field_list = "\n".join(f"- {f}" for f in fields)

    # Step 1: Extract
    extract_response = client.chat.completions.create(
        model="gpt-4o",
        temperature=0.0,
        messages=[{"role": "user", "content": EXTRACT_PROMPT.format(
            field_list=field_list, document=document
        )}],
        response_format={"type": "json_object"}
    )
    extracted = extract_response.choices[0].message.content

    # Step 2: Validate
    validate_response = client.chat.completions.create(
        model="gpt-4o",
        temperature=0.0,
        messages=[{"role": "user", "content": VALIDATION_PROMPT.format(
            extracted_data=extracted, document=document
        )}],
        response_format={"type": "json_object"}
    )
    validation_raw = validate_response.choices[0].message.content

    result = ValidatedExtraction.model_validate_json(validation_raw)

    # Apply corrections for low-confidence fields
    for field in result.fields:
        if field.confidence < confidence_threshold and field.correction:
            field.extracted_value = field.correction
            field.is_valid = True

    return result
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `confidence_threshold` | `0.85` | Below this, apply corrections |
| `model` | `gpt-4o` | Must support JSON mode |
| `temperature` | `0.0` | Deterministic for both passes |

## Self-Consistency Validation

```python
def validate_with_consistency(
    document: str,
    fields: List[str],
    n_samples: int = 3
) -> dict:
    """Run extraction multiple times and compare results."""
    extractions = []
    for _ in range(n_samples):
        resp = client.chat.completions.create(
            model="gpt-4o",
            temperature=0.3,  # Slight variation
            messages=[{"role": "user", "content": EXTRACT_PROMPT.format(
                field_list="\n".join(f"- {f}" for f in fields),
                document=document
            )}],
            response_format={"type": "json_object"}
        )
        extractions.append(json.loads(resp.choices[0].message.content))

    # Compare: fields with same value across all runs = high confidence
    consensus = {}
    for field in fields:
        values = [e.get(field) for e in extractions]
        unique = set(str(v) for v in values)
        consensus[field] = {
            "value": values[0],
            "agreement": len([v for v in values if v == values[0]]) / len(values),
            "all_values": values
        }
    return consensus
```

## Example Usage

```python
doc = "Invoice #1234, dated 2024-03-15, from Acme Corp, total $5,250.00 USD"
fields = ["invoice_number", "date", "vendor", "total_amount", "currency"]

result = extract_and_validate(doc, fields)

for field in result.fields:
    status = "OK" if field.is_valid else "FLAGGED"
    print(f"[{status}] {field.field_name}: {field.extracted_value} "
          f"(confidence: {field.confidence})")
```

## See Also

- [Chain-of-Thought](../concepts/chain-of-thought.md)
- [Multi-Pass Extraction](../patterns/multi-pass-extraction.md)
- [Document Extraction](../patterns/document-extraction.md)
