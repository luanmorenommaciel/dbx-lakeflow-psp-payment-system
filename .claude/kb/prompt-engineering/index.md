# Prompt Engineering Knowledge Base

> **Purpose**: Techniques and patterns for effective LLM prompting -- context engineering, structured output, chain-of-thought, extraction, and multi-modal
> **MCP Validated:** 2026-03-26

## Quick Navigation

### Concepts (< 150 lines each)

| File | Purpose |
|------|---------|
| [concepts/chain-of-thought.md](concepts/chain-of-thought.md) | Step-by-step reasoning: CoT, self-consistency, ReAct |
| [concepts/structured-extraction.md](concepts/structured-extraction.md) | Data extraction with Pydantic, Instructor, native JSON Schema |
| [concepts/few-shot-prompting.md](concepts/few-shot-prompting.md) | Learning from examples, dynamic few-shot selection |
| [concepts/system-prompts.md](concepts/system-prompts.md) | System prompt design, context engineering, persona rules |
| [concepts/output-formatting.md](concepts/output-formatting.md) | JSON Schema mode, Instructor, tool-use for structured output |

### Patterns (< 200 lines each)

| File | Purpose |
|------|---------|
| [patterns/document-extraction.md](patterns/document-extraction.md) | Extract structured data from documents with retry logic |
| [patterns/validation-prompts.md](patterns/validation-prompts.md) | Self-validation and self-checking prompts |
| [patterns/multi-pass-extraction.md](patterns/multi-pass-extraction.md) | Multi-pass refinement for high accuracy |
| [patterns/prompt-template.md](patterns/prompt-template.md) | Reusable prompt templates with Python |

### Specs (Machine-Readable)

| File | Purpose |
|------|---------|
| [specs/prompt-formats.yaml](specs/prompt-formats.yaml) | Prompt format specification and schema |

---

## Quick Reference

- [quick-reference.md](quick-reference.md) - Fast lookup tables

---

## Key Concepts

| Concept | Description |
|---------|-------------|
| **Context Engineering** | Treat prompts as structured systems, not text strings (2026 paradigm) |
| **Chain-of-Thought** | Guide LLMs through step-by-step reasoning for accuracy |
| **Structured Extraction** | Extract typed fields with Pydantic + JSON Schema mode |
| **Few-Shot Prompting** | Provide examples to teach output format and behavior |
| **System Prompts** | Define persona, constraints, and behavioral rules |
| **Output Formatting** | Enforce JSON via native JSON Schema mode, Instructor, or tool use |

---

## Learning Path

| Level | Files |
|-------|-------|
| **Beginner** | concepts/system-prompts.md, concepts/output-formatting.md |
| **Intermediate** | concepts/chain-of-thought.md, concepts/few-shot-prompting.md |
| **Advanced** | concepts/structured-extraction.md, patterns/multi-pass-extraction.md |

---

## Agent Usage

| Agent | Primary Files | Use Case |
|-------|---------------|----------|
| ai-prompt-specialist | patterns/document-extraction.md, patterns/prompt-template.md | Design extraction and structured output prompts |
