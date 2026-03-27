# Prompt Engineering Quick Reference

> Fast lookup tables. For code examples, see linked files.
> **MCP Validated:** 2026-03-26

## The 2026 Paradigm: Context Engineering

```text
2023-2024: "Prompt Engineering" -- writing better text prompts
2025-2026: "Context Engineering" -- designing structured input systems

Context engineering treats the LLM's full input as a data pipeline:
  [System] Role + constraints + output format + rubric
  [Context] Retrieved docs, tool results, prior turns (curated, not dumped)
  [Instructions] Task-specific rules, citation format, fallback behavior
  [Examples] Dynamic few-shot, selected by embedding similarity
  [Query] User input + disambiguation + metadata

Key insight: what you EXCLUDE from context matters as much as what you include.
```

## Technique Selection

| Technique | Best For | Accuracy Boost | Token Cost |
|-----------|----------|---------------|------------|
| Zero-shot | Simple tasks, classification | Baseline | Low |
| Few-shot | Format-sensitive, tone matching | +15-25% | Medium |
| Chain-of-Thought | Reasoning, math, logic | +20-40% | Medium |
| ReAct (Reason+Act) | Tool use, multi-step research | +30-50% | High |
| Self-consistency | High-stakes decisions | +10-15% | High |
| Multi-pass | Document extraction | +25-35% | High |
| Extended Thinking | Complex reasoning (Claude) | +30-50% | Medium-High |

## Temperature Guide

| Task Type | Temperature | Reason |
|-----------|-------------|--------|
| Data extraction | 0.0 | Deterministic, factual |
| Classification | 0.0-0.2 | Consistent labels |
| Summarization | 0.3-0.5 | Slight variation OK |
| Creative writing | 0.7-1.0 | Diversity desired |
| Code generation | 0.0-0.2 | Correctness critical |
| Brainstorming | 0.8-1.0 | Maximize diversity |

## Structured Output Methods (2026)

| Method | Provider | Reliability | Notes |
|--------|----------|-------------|-------|
| JSON Schema mode | OpenAI | Highest | `response_format={"type": "json_schema"}` |
| Tool use + schema | Anthropic | Highest | Define output as a "tool" Claude calls |
| Extended thinking | Claude 4.x | High | Think step-by-step, then structured output |
| Instructor library | Any | Highest | Pydantic models -> structured output (any provider) |
| `response_mime_type` | Google | High | `application/json` + schema |
| JSON mode | OpenAI | High | `response_format={"type": "json_object"}` |

## Prompt Structure (RCCF Framework)

| Section | Required | Purpose |
|---------|----------|---------|
| Role/System | Yes | Define persona, expertise, constraints |
| Context | Yes | Relevant data, retrieved docs, prior results |
| Constraints | Yes | What the model must NOT do + edge cases |
| Format | Yes | Output structure, schema, examples |
| Examples | Recommended | Teach by demonstration (dynamic few-shot) |
| Fallback | Recommended | What to do when uncertain |

## Model-Specific Tips (March 2026)

| Model | Tips |
|-------|------|
| Claude Opus 4.6 | Use extended thinking for complex reasoning. 1M context stays coherent. Use `<thinking>` tags. |
| Claude Sonnet 4.6 | Best cost/quality ratio. Use tool_use for structured output. 1M context in beta. |
| GPT-4o | JSON Schema mode is most reliable structured output. Strong multimodal. |
| Gemini 2.5 Pro | Native thinking model. 1M context. Strong at code and math. |

## Decision Matrix

| Use Case | Choose |
|----------|--------|
| Extract fields from invoice | `patterns/document-extraction.md` |
| Need step-by-step reasoning | `concepts/chain-of-thought.md` |
| Consistent JSON output | `concepts/output-formatting.md` |
| Teach format by example | `concepts/few-shot-prompting.md` |
| Validate LLM output accuracy | `patterns/validation-prompts.md` |
| High-accuracy extraction | `patterns/multi-pass-extraction.md` |
| Reusable prompt code | `patterns/prompt-template.md` |

## Common Pitfalls

| Don't | Do |
|-------|-----|
| Use vague instructions ("be helpful") | Be explicit ("Extract the invoice number as a string") |
| Skip output format specification | Use JSON Schema mode or Instructor for guaranteed structure |
| Use high temperature for extraction | Set temperature to 0.0 for factual tasks |
| Send raw documents without context | Pre-process, chunk, and curate context |
| Trust LLM output without validation | Always validate with Pydantic or JSON Schema |
| Write monolithic prompts | Split into composable template sections |
| Dump entire context blindly | Curate: include relevant, exclude noise |
| Ignore model differences | Adapt prompts per model (Claude vs GPT vs Gemini) |

## Related Documentation

| Topic | Path |
|-------|------|
| Chain-of-Thought | `concepts/chain-of-thought.md` |
| System Prompts | `concepts/system-prompts.md` |
| Full Index | `index.md` |
