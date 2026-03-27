# System Prompts

> **Purpose**: Design effective system prompts and context engineering for LLMs -- persona, behavior, constraints
> **Confidence**: 0.95
> **MCP Validated:** 2026-03-26

## Overview

System prompts set the behavioral foundation for an LLM conversation. In 2026, the discipline has evolved from "prompt engineering" to "context engineering" -- treating the entire LLM input as a structured system rather than a text string. A well-designed system prompt reduces hallucinations, enforces consistent formatting, and establishes the boundaries within which the model operates. With 1M token context windows (Claude 4.x), careful context curation is more important than ever.

## The Pattern

```python
from anthropic import Anthropic

client = Anthropic()

SYSTEM_PROMPT = """You are a senior financial data analyst specializing in invoice processing.

## Role
- Extract and validate financial data from documents
- Flag anomalies or suspicious values
- Always provide confidence scores for extracted fields

## Constraints
- NEVER fabricate data that is not present in the document
- If a field cannot be found, return null with confidence 0.0
- Dates must be in ISO 8601 format (YYYY-MM-DD)
- Monetary amounts must be numeric without currency symbols
- Always respond in valid JSON format

## Quality Standards
- Double-check extracted numbers against document context
- Cross-validate totals against line item sums
- Flag discrepancies rather than silently correcting them

## Error Handling
- If uncertain about a field, set confidence below 0.5
- If the document is unreadable, return error_type: "unreadable"
- If the document type is unexpected, note it in the metadata field
"""

def process_document(document: str) -> str:
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2048,
        system=SYSTEM_PROMPT,  # Anthropic: separate system parameter
        messages=[
            {"role": "user", "content": f"Extract all data from:\n\n{document}"}
        ],
    )
    return response.content[0].text
```

## System Prompt Anatomy (RCCF)

| Section | Purpose | Required |
|---------|---------|----------|
| Role | Define expertise and persona | Yes |
| Context Scope | What data/domain the model operates in | Yes |
| Constraints | What the model must NOT do | Yes |
| Format | Response structure rules (JSON, markdown) | Yes |
| Quality Rules | Standards, validation checks | Recommended |
| Error Handling | What to do when uncertain | Recommended |
| Examples | 1-2 ideal outputs | Recommended |

## Context Engineering Principles (2026)

```text
1. CURATE, don't dump -- include relevant context, exclude noise
2. ORDER matters -- important information first and last (primacy/recency)
3. SEPARATE concerns -- system vs context vs instructions vs query
4. STRUCTURE data -- use headers, lists, XML tags for clear sections
5. EXCLUDE irrelevant -- what you leave out is as important as what you include
6. ADAPT per model -- Claude handles 1M tokens well; shorter models need pruning
```

## Common Mistakes

### Wrong
```python
# Vague, no constraints, no format
system = "You are a helpful assistant."
```

### Correct
```python
# Specific role, clear constraints, defined output, error handling
system = """You are a medical record classifier.

Role: Classify clinical notes into ICD-10 categories.
Constraints: NEVER suggest diagnoses. Only classify based on explicit text.
Format: JSON with fields: code, description, confidence, evidence_quote.
Error: If uncertain, set confidence below 0.5 and explain in notes field."""
```

## Provider-Specific Notes (2026)

| Provider | System Prompt Support | Context Window | Notes |
|----------|----------------------|---------------|-------|
| Anthropic Claude 4.x | `system` parameter | 1M tokens | Very strong adherence, use XML tags for structure |
| OpenAI GPT-4o | `role: "system"` | 128K tokens | Strong adherence, supports JSON Schema mode |
| Google Gemini 2.x | `system_instruction` | 1M tokens | Good adherence, native thinking mode |
| OpenRouter | `role: "system"` | Varies | Depends on underlying model |

## Related

- [Output Formatting](../concepts/output-formatting.md)
- [Few-Shot Prompting](../concepts/few-shot-prompting.md)
- [Prompt Template Pattern](../patterns/prompt-template.md)
