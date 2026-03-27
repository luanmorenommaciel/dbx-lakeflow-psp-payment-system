# LLM Output Validation

> **Purpose**: Validate and parse LLM JSON responses into typed Pydantic models
> **MCP Validated**: 2026-03-26

## When to Use

- Parsing structured JSON output from any LLM (Gemini, GPT, Claude)
- Enforcing schema compliance on non-deterministic LLM responses
- Building reliable extraction pipelines that fail gracefully on malformed output
- Generating format instructions from Pydantic schemas to embed in prompts

## Implementation

```python
import json
import logging
from typing import Optional, TypeVar
from pydantic import BaseModel, Field, ValidationError

logger = logging.getLogger(__name__)
T = TypeVar("T", bound=BaseModel)


def validate_llm_output(
    raw_response: str,
    model_class: type[T],
    strict: bool = False,
) -> T:
    """Parse and validate LLM JSON output against a Pydantic model.

    Args:
        raw_response: Raw string from LLM (may contain markdown fences).
        model_class: Pydantic model class to validate against.
        strict: If True, disallow type coercion.

    Returns:
        Validated model instance.

    Raises:
        ValidationError: If the output does not match the schema.
        ValueError: If the output is not valid JSON.
    """
    cleaned = _extract_json(raw_response)
    return model_class.model_validate_json(cleaned, strict=strict)


def validate_llm_output_safe(
    raw_response: str,
    model_class: type[T],
) -> tuple[Optional[T], Optional[list[dict]]]:
    """Non-raising version that returns (result, errors)."""
    try:
        result = validate_llm_output(raw_response, model_class)
        return result, None
    except ValidationError as e:
        logger.warning("Validation failed: %s", e.error_count())
        return None, e.errors()
    except (ValueError, json.JSONDecodeError) as e:
        logger.warning("JSON parse failed: %s", str(e))
        return None, [{"type": "json_invalid", "msg": str(e)}]


def _extract_json(text: str) -> str:
    """Strip markdown code fences and whitespace from LLM output."""
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        # Remove first line (```json) and last line (```)
        lines = [l for l in lines[1:] if l.strip() != "```"]
        text = "\n".join(lines)
    return text.strip()


def build_format_instruction(model_class: type[BaseModel]) -> str:
    """Generate LLM prompt instructions from a Pydantic model schema."""
    schema = model_class.model_json_schema()
    return (
        "You must respond with valid JSON matching this schema. "
        "Do not include any text outside the JSON object.\n\n"
        f"```json\n{json.dumps(schema, indent=2)}\n```"
    )
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `strict` | `False` | When True, disables type coercion (string "5" won't become int 5) |
| `strip_fences` | `True` | Remove markdown code fences from LLM output |
| `temperature` | `0.0` | Recommended LLM temperature for structured output |

## Example Usage

```python
from pydantic import BaseModel, Field
from typing import Optional


class ExtractedEntity(BaseModel):
    name: str = Field(..., description="Entity name")
    entity_type: str = Field(..., description="One of: person, org, location")
    confidence: float = Field(..., ge=0, le=1, description="Extraction confidence")
    context: Optional[str] = Field(None, description="Surrounding text snippet")


# 1. Generate prompt instructions
instructions = build_format_instruction(ExtractedEntity)

# 2. Send to LLM (example with Gemini)
prompt = f"Extract entities from: 'John works at Acme in NYC'\n\n{instructions}"
llm_response = call_llm(prompt)  # returns JSON string

# 3. Validate the response
entity, errors = validate_llm_output_safe(llm_response, ExtractedEntity)
if entity:
    print(f"Found: {entity.name} ({entity.entity_type})")
else:
    print(f"Validation errors: {errors}")


# 4. Batch validation for list outputs
class EntityList(BaseModel):
    entities: list[ExtractedEntity]

batch_result, errors = validate_llm_output_safe(llm_response, EntityList)
```

## Partial JSON Parsing (Pydantic v2.7+)

```python
from pydantic_core import from_json

# Useful for streaming LLM responses
partial_json = '{"name": "John", "entity_type": "pers'
try:
    data = from_json(partial_json, allow_partial=True)
    # data = {"name": "John", "entity_type": "pers"}
except ValueError:
    pass
```

## TypeAdapter for List Validation

```python
from pydantic import TypeAdapter

# Validate a list of entities without wrapping in a BaseModel
EntityListAdapter = TypeAdapter(list[ExtractedEntity])

# Parse LLM output that returns a JSON array directly
entities = EntityListAdapter.validate_json(llm_response)

# Generate JSON Schema for arrays
schema = EntityListAdapter.json_schema()
```

## Discriminated Unions for Multi-Type LLM Output

```python
from typing import Literal, Union
from pydantic import BaseModel, Field

class PersonEntity(BaseModel):
    entity_type: Literal["person"]
    name: str
    role: str | None = None

class OrgEntity(BaseModel):
    entity_type: Literal["org"]
    name: str
    industry: str | None = None

class EntityResult(BaseModel):
    entities: list[Union[PersonEntity, OrgEntity]] = Field(
        discriminator="entity_type"
    )

# Pydantic picks the right model based on entity_type
result = EntityResult.model_validate_json(llm_response)
```

## See Also

- [Error Handling](../patterns/error-handling.md)
- [Extraction Schema](../patterns/extraction-schema.md)
- [BaseModel](../concepts/base-model.md)
