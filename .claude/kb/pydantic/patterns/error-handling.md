# Error Handling

> **Purpose**: Handle Pydantic ValidationError gracefully in LLM extraction pipelines
> **MCP Validated**: 2026-02-17

## When to Use

- LLM returns malformed or partial JSON that fails validation
- Building retry logic when extraction fails schema validation
- Logging structured error information for monitoring
- Providing actionable feedback to retry with corrected prompts

## Implementation

```python
import json
import logging
from typing import Optional, TypeVar
from pydantic import BaseModel, ValidationError

logger = logging.getLogger(__name__)
T = TypeVar("T", bound=BaseModel)


class ExtractionResult(BaseModel):
    """Wrapper for extraction results with error metadata."""
    success: bool
    data: Optional[dict] = None
    errors: Optional[list[dict]] = None
    error_summary: Optional[str] = None
    attempts: int = 1


def parse_validation_errors(error: ValidationError) -> list[dict]:
    """Convert ValidationError to structured error list."""
    return [
        {
            "field": " -> ".join(str(loc) for loc in err["loc"]),
            "message": err["msg"],
            "type": err["type"],
            "input": err.get("input"),
        }
        for err in error.errors()
    ]


def format_error_for_retry(error: ValidationError) -> str:
    """Format validation errors as LLM retry instructions."""
    lines = ["The previous response had validation errors. Fix these issues:"]
    for err in error.errors():
        field_path = " -> ".join(str(loc) for loc in err["loc"])
        lines.append(f"  - Field '{field_path}': {err['msg']}")
    lines.append("\nReturn corrected JSON only.")
    return "\n".join(lines)


def validate_with_retry(
    call_llm_fn,
    prompt: str,
    model_class: type[T],
    max_retries: int = 2,
    temperature: float = 0.0,
) -> ExtractionResult:
    """Validate LLM output with automatic retry on failure.

    Args:
        call_llm_fn: Callable that takes (prompt, temperature) and returns str.
        prompt: Initial extraction prompt.
        model_class: Pydantic model to validate against.
        max_retries: Maximum retry attempts after first failure.
        temperature: LLM temperature setting.

    Returns:
        ExtractionResult with success status and data or errors.
    """
    current_prompt = prompt

    for attempt in range(1, max_retries + 2):
        try:
            raw_response = call_llm_fn(current_prompt, temperature)
            cleaned = _extract_json(raw_response)
            result = model_class.model_validate_json(cleaned)

            logger.info("Validation succeeded on attempt %d", attempt)
            return ExtractionResult(
                success=True,
                data=result.model_dump(),
                attempts=attempt,
            )

        except ValidationError as e:
            error_details = parse_validation_errors(e)
            logger.warning(
                "Attempt %d/%d failed: %d errors",
                attempt, max_retries + 1, e.error_count(),
            )

            if attempt <= max_retries:
                retry_instruction = format_error_for_retry(e)
                current_prompt = f"{prompt}\n\n{retry_instruction}"
            else:
                return ExtractionResult(
                    success=False,
                    errors=error_details,
                    error_summary=f"{e.error_count()} validation errors after {attempt} attempts",
                    attempts=attempt,
                )

        except (ValueError, json.JSONDecodeError) as e:
            logger.error("JSON parse error on attempt %d: %s", attempt, str(e))
            if attempt > max_retries:
                return ExtractionResult(
                    success=False,
                    errors=[{"field": "root", "message": str(e), "type": "json_invalid"}],
                    error_summary=f"Invalid JSON after {attempt} attempts",
                    attempts=attempt,
                )
            current_prompt = (
                f"{prompt}\n\nYour previous response was not valid JSON. "
                "Return ONLY a valid JSON object, no markdown or text."
            )

    # Should not reach here, but safety fallback
    return ExtractionResult(success=False, error_summary="Unexpected failure")


def _extract_json(text: str) -> str:
    """Strip markdown code fences from LLM response."""
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [l for l in lines[1:] if l.strip() != "```"]
        text = "\n".join(lines)
    return text.strip()
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `max_retries` | `2` | Number of retry attempts after first failure |
| `temperature` | `0.0` | Lower temperature for more deterministic retries |
| `log_level` | `WARNING` | Log level for validation failures |

## Example Usage

```python
from pydantic import BaseModel, Field


class Receipt(BaseModel):
    merchant: str = Field(..., min_length=1)
    total: float = Field(..., gt=0)
    date: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$")


# With retry logic
result = validate_with_retry(
    call_llm_fn=my_llm_client.generate,
    prompt="Extract receipt data from: ...",
    model_class=Receipt,
    max_retries=2,
)

if result.success:
    print(f"Extracted: {result.data}")
else:
    print(f"Failed: {result.error_summary}")
    for err in result.errors or []:
        print(f"  {err['field']}: {err['message']}")
```

## Error Type Reference

| Error Type | Cause | Fix Strategy |
|------------|-------|-------------|
| `missing` | Required field not in JSON | Retry with explicit field list |
| `string_type` | Wrong type for string field | Retry with type instruction |
| `greater_than` | Number below minimum | Retry with constraint reminder |
| `json_invalid` | Not valid JSON at all | Retry asking for JSON only |
| `string_pattern_mismatch` | Regex pattern failed | Retry with format example |

## See Also

- [LLM Output Validation](../patterns/llm-output-validation.md)
- [Validators](../concepts/validators.md)
- [Custom Validators](../patterns/custom-validators.md)
