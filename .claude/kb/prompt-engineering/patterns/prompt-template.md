# Reusable Prompt Templates

> **Purpose**: Production-ready, composable prompt templates for common LLM tasks with Python integration
> **MCP Validated:** 2026-03-26

## When to Use

- Building prompt libraries for teams
- Standardizing prompt structure across projects
- Creating version-controlled, testable prompts
- Any project with multiple prompt types

## Implementation

```python
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from string import Template
import json


@dataclass
class PromptTemplate:
    """Reusable, composable prompt template."""

    name: str
    version: str
    system: str
    user: str
    temperature: float = 0.0
    model: str = "gpt-4o"
    response_format: Optional[str] = "json_object"
    metadata: Dict[str, Any] = field(default_factory=dict)

    def render(self, **kwargs) -> Dict[str, Any]:
        """Render the template with variables."""
        return {
            "model": self.model,
            "temperature": self.temperature,
            "messages": [
                {"role": "system", "content": self.system.format(**kwargs)},
                {"role": "user", "content": self.user.format(**kwargs)},
            ],
            "response_format": {"type": self.response_format}
            if self.response_format else None,
        }

    def to_dict(self) -> dict:
        """Serialize template for storage."""
        return {
            "name": self.name,
            "version": self.version,
            "system": self.system,
            "user": self.user,
            "temperature": self.temperature,
            "model": self.model,
            "response_format": self.response_format,
            "metadata": self.metadata,
        }


# --- Pre-Built Templates ---

EXTRACTION_TEMPLATE = PromptTemplate(
    name="document-extraction",
    version="1.0.0",
    system="""You are a precise data extraction system.
Extract ONLY data present in the document. Use null for missing fields.
Dates: ISO 8601. Amounts: numeric only. Always return valid JSON.""",
    user="""## Schema
{schema}

## Document
{document}

Extract all fields matching the schema. Return JSON only.""",
    temperature=0.0,
)

CLASSIFICATION_TEMPLATE = PromptTemplate(
    name="text-classification",
    version="1.0.0",
    system="""You are a text classification system.
Classify the input into exactly one of the provided categories.
Return JSON with: category, confidence (0-1), reasoning.""",
    user="""## Categories
{categories}

## Text
{text}

Classify this text. Return JSON only.""",
    temperature=0.0,
)

SUMMARIZATION_TEMPLATE = PromptTemplate(
    name="document-summarization",
    version="1.0.0",
    system="""You are a document summarization expert.
Produce concise, accurate summaries. Never add information not in the source.""",
    user="""## Instructions
Summarize the following document in {max_sentences} sentences.
Focus on: {focus_areas}

## Document
{document}

Return JSON: {{"summary": "...", "key_points": ["..."], "word_count": N}}""",
    temperature=0.3,
)

QA_TEMPLATE = PromptTemplate(
    name="question-answering",
    version="1.0.0",
    system="""You are a question-answering system.
Answer based ONLY on the provided context. If the answer is not in the context,
say so explicitly. Never fabricate information.""",
    user="""## Context
{context}

## Question
{question}

Return JSON: {{"answer": "...", "confidence": 0.0-1.0, "evidence": "quote"}}""",
    temperature=0.0,
)


# --- Template Registry ---

class PromptRegistry:
    """Manage and version prompt templates."""

    def __init__(self):
        self._templates: Dict[str, PromptTemplate] = {}

    def register(self, template: PromptTemplate) -> None:
        key = f"{template.name}@{template.version}"
        self._templates[key] = template

    def get(self, name: str, version: str = "latest") -> PromptTemplate:
        if version == "latest":
            matches = [
                (k, t) for k, t in self._templates.items()
                if k.startswith(f"{name}@")
            ]
            if not matches:
                raise KeyError(f"Template '{name}' not found")
            return sorted(matches, key=lambda x: x[0])[-1][1]
        key = f"{name}@{version}"
        if key not in self._templates:
            raise KeyError(f"Template '{key}' not found")
        return self._templates[key]

    def list_templates(self) -> List[str]:
        return list(self._templates.keys())


# --- Usage ---

registry = PromptRegistry()
registry.register(EXTRACTION_TEMPLATE)
registry.register(CLASSIFICATION_TEMPLATE)
registry.register(SUMMARIZATION_TEMPLATE)
registry.register(QA_TEMPLATE)
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `temperature` | Per-template | Extraction=0.0, Creative=0.7 |
| `model` | `gpt-4o` | Override per template |
| `response_format` | `json_object` | Set to None for free-text |
| `version` | `1.0.0` | Semantic versioning |

## Example Usage

```python
from openai import OpenAI

client = OpenAI()

# Get and render a template
template = registry.get("document-extraction")
params = template.render(
    schema='{"invoice_number": "string", "total": "float"}',
    document="Invoice #123, Total: $500.00"
)

# Call the API
response = client.chat.completions.create(**params)
print(response.choices[0].message.content)
```

## See Also

- [System Prompts](../concepts/system-prompts.md)
- [Few-Shot Prompting](../concepts/few-shot-prompting.md)
- [Document Extraction](../patterns/document-extraction.md)
