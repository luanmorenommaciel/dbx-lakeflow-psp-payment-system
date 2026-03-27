# Output Formatting

> **Purpose**: Enforce reliable structured output from LLMs using JSON Schema mode, Instructor, tool use, and validation
> **Confidence**: 0.95
> **MCP Validated:** 2026-03-26

## Overview

Getting consistent structured output from LLMs requires API-level constraints plus validation. In 2026, three approaches dominate: OpenAI's native JSON Schema mode (guarantees schema conformance), Anthropic's tool-use-as-schema pattern, and the Instructor library (works with any provider, Pydantic-native). The old approach of "please return JSON" in the prompt is no longer recommended for production.

## The Pattern: Instructor (Any Provider)

```python
import instructor
from pydantic import BaseModel, Field
from typing import Optional, List
from openai import OpenAI

client = instructor.from_openai(OpenAI())

class TaskResult(BaseModel):
    status: str = Field(description="completed | failed | partial")
    summary: str = Field(description="Brief result summary")
    items: List[dict] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0)
    errors: Optional[List[str]] = None

def get_structured_output(data: str) -> TaskResult:
    return client.chat.completions.create(
        model="gpt-4o",
        response_model=TaskResult,
        max_retries=2,
        messages=[{"role": "user", "content": f"Analyze:\n\n{data}"}],
    )
# Returns validated TaskResult -- guaranteed schema conformance
```

## The Pattern: OpenAI JSON Schema Mode

```python
from openai import OpenAI

client = OpenAI()

response = client.chat.completions.create(
    model="gpt-4o",
    messages=[{"role": "user", "content": f"Analyze:\n\n{data}"}],
    response_format={
        "type": "json_schema",
        "json_schema": {
            "name": "task_result",
            "schema": TaskResult.model_json_schema(),
            "strict": True,  # guaranteed conformance
        }
    }
)
result = TaskResult.model_validate_json(response.choices[0].message.content)
```

## The Pattern: Anthropic Tool-Use-as-Schema

```python
import anthropic

client = anthropic.Anthropic()

response = client.messages.create(
    model="claude-sonnet-4-6",
    max_tokens=1024,
    tools=[{
        "name": "return_result",
        "description": "Return the structured analysis result",
        "input_schema": TaskResult.model_json_schema(),
    }],
    tool_choice={"type": "tool", "name": "return_result"},
    messages=[{"role": "user", "content": f"Analyze:\n\n{data}"}],
)
tool_use = next(b for b in response.content if b.type == "tool_use")
result = TaskResult.model_validate(tool_use.input)
```

## Structured Output Methods (2026)

| Method | Provider | Reliability | Complexity | Use When |
|--------|----------|-------------|------------|----------|
| Instructor library | Any | Highest | Low | Default choice for Python projects |
| JSON Schema mode (strict) | OpenAI | Highest | Medium | OpenAI-only, guaranteed schema |
| Tool-use-as-schema | Anthropic | Highest | Medium | Claude projects, structured output |
| Extended thinking + output | Claude 4.x | High | Low | Complex reasoning + structured result |
| JSON mode | OpenAI | High | Low | Simple JSON, no schema guarantee |
| Prompt-only | Any | Medium | Low | Quick prototypes only |

## Validation Pipeline

```python
from pydantic import BaseModel, ValidationError

def parse_with_retry(raw_json: str, model_class: type, max_retries: int = 2) -> BaseModel:
    """Parse JSON with retry on validation failure."""
    for attempt in range(max_retries + 1):
        try:
            return model_class.model_validate_json(raw_json)
        except ValidationError as e:
            if attempt == max_retries:
                raise
            raw_json = repair_json(raw_json, str(e))
    raise ValueError("Exceeded max retries")
```

## Common Mistakes

### Wrong
```python
# No format enforcement -- output may include markdown, text, etc.
prompt = "Return the data as JSON"
```

### Correct
```python
# Use Instructor for guaranteed structured output
result = client.chat.completions.create(
    model="gpt-4o",
    response_model=MySchema,  # Pydantic model = schema + validation
    max_retries=2,
    messages=[{"role": "user", "content": prompt}],
)
```

## Related

- [Structured Extraction](../concepts/structured-extraction.md)
- [Document Extraction Pattern](../patterns/document-extraction.md)
- [Prompt Template Pattern](../patterns/prompt-template.md)
