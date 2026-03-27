# Multi-Pass Extraction Pattern

> **Purpose**: Improve extraction accuracy through multiple LLM passes with progressive refinement
> **MCP Validated:** 2026-03-26

## When to Use

- Complex documents with dense information or single-pass accuracy below 90%
- Documents with tables, nested structures, or ambiguous formatting
- High-value extraction where cost of errors exceeds cost of extra API calls

## Implementation

```python
import json
from pydantic import BaseModel, Field, ValidationError
from typing import Optional, List, Dict, Any
from openai import OpenAI

client = OpenAI()


class ExtractionPass(BaseModel):
    pass_number: int
    fields_extracted: Dict[str, Any]
    confidence: float = Field(ge=0.0, le=1.0)
    issues_found: List[str] = Field(default_factory=list)

class MultiPassResult(BaseModel):
    final_extraction: Dict[str, Any]
    passes: List[ExtractionPass]
    overall_confidence: float = Field(ge=0.0, le=1.0)
    total_passes: int

PASS_1_PROMPT = """You are a document analyst. Perform a thorough first-pass extraction.

## Task
Extract ALL available fields from this document. Be comprehensive.
For each field, include a confidence score (0-1).

## Document
{document}

## Output Format
Return JSON: {{"fields": {{"field_name": {{"value": ..., "confidence": 0.0-1.0}}}}}}
"""

PASS_2_PROMPT = """You are a data quality specialist reviewing an extraction.

## Previous Extraction
{previous_extraction}

## Original Document
{document}

## Task
1. Review each extracted field against the original document
2. Fix any errors you find
3. Fill in any fields that were missed
4. Re-score confidence for each field
5. Flag fields where the document is ambiguous

## Output Format
Return JSON: {{
  "fields": {{"field_name": {{"value": ..., "confidence": 0.0-1.0}}}},
  "corrections": ["description of each correction made"],
  "missed_fields": ["fields found in pass 2 but missed in pass 1"]
}}
"""

PASS_3_PROMPT = """You are a senior auditor performing final verification.

## Extraction to Verify
{extraction}

## Original Document
{document}

## Task
Perform these specific checks:
1. Do numeric totals match line item sums?
2. Are dates logically consistent?
3. Do names and references match across the document?
4. Are there any contradictions between fields?
5. Final confidence score for each field

## Output Format
Return JSON: {{
  "verified_fields": {{"field_name": {{"value": ..., "confidence": 0.0-1.0}}}},
  "discrepancies": ["any issues found"],
  "final_confidence": 0.0-1.0
}}
"""


def multi_pass_extract(
    document: str,
    target_confidence: float = 0.95,
    max_passes: int = 3
) -> MultiPassResult:
    """Run progressive extraction passes until confidence target is met."""

    passes = []
    # Pass 1: Broad extraction
    resp1 = client.chat.completions.create(
        model="gpt-4o",
        temperature=0.0,
        messages=[{"role": "user", "content": PASS_1_PROMPT.format(
            document=document
        )}],
        response_format={"type": "json_object"}
    )
    pass1_data = json.loads(resp1.choices[0].message.content)
    pass1_confidence = _avg_confidence(pass1_data.get("fields", {}))
    passes.append(ExtractionPass(
        pass_number=1,
        fields_extracted=pass1_data.get("fields", {}),
        confidence=pass1_confidence
    ))

    if pass1_confidence >= target_confidence or max_passes < 2:
        return _build_result(passes)
    # Pass 2: Focused refinement
    resp2 = client.chat.completions.create(
        model="gpt-4o",
        temperature=0.0,
        messages=[{"role": "user", "content": PASS_2_PROMPT.format(
            previous_extraction=json.dumps(pass1_data, indent=2),
            document=document
        )}],
        response_format={"type": "json_object"}
    )
    pass2_data = json.loads(resp2.choices[0].message.content)
    pass2_confidence = _avg_confidence(pass2_data.get("fields", {}))
    passes.append(ExtractionPass(
        pass_number=2,
        fields_extracted=pass2_data.get("fields", {}),
        confidence=pass2_confidence,
        issues_found=pass2_data.get("corrections", [])
    ))

    if pass2_confidence >= target_confidence or max_passes < 3:
        return _build_result(passes)
    # Pass 3: Cross-validation
    resp3 = client.chat.completions.create(
        model="gpt-4o",
        temperature=0.0,
        messages=[{"role": "user", "content": PASS_3_PROMPT.format(
            extraction=json.dumps(pass2_data.get("fields", {}), indent=2),
            document=document
        )}],
        response_format={"type": "json_object"}
    )
    pass3_data = json.loads(resp3.choices[0].message.content)
    pass3_confidence = pass3_data.get("final_confidence", pass2_confidence)
    passes.append(ExtractionPass(
        pass_number=3,
        fields_extracted=pass3_data.get("verified_fields", {}),
        confidence=pass3_confidence,
        issues_found=pass3_data.get("discrepancies", [])
    ))

    return _build_result(passes)


def _avg_confidence(fields: dict) -> float:
    if not fields:
        return 0.0
    scores = [f.get("confidence", 0.5) if isinstance(f, dict) else 0.5 for f in fields.values()]
    return sum(scores) / len(scores)

def _build_result(passes: List[ExtractionPass]) -> MultiPassResult:
    last = passes[-1]
    final = {k: (v.get("value", v) if isinstance(v, dict) else v) for k, v in last.fields_extracted.items()}
    return MultiPassResult(final_extraction=final, passes=passes,
                           overall_confidence=last.confidence, total_passes=len(passes))
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `target_confidence` | `0.95` | Stop early if reached |
| `max_passes` | `3` | Maximum extraction passes |
| `model` | `gpt-4o` | Consistent model across passes |
| `temperature` | `0.0` | Deterministic for all passes |

## Example Usage

```python
result = multi_pass_extract("Invoice #123...", target_confidence=0.95)
print(f"Passes: {result.total_passes}, Confidence: {result.overall_confidence:.2f}")
```

## See Also

- [Document Extraction](../patterns/document-extraction.md)
- [Validation Prompts](../patterns/validation-prompts.md)
- [Chain-of-Thought](../concepts/chain-of-thought.md)
