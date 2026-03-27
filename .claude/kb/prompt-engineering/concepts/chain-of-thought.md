# Chain-of-Thought Prompting

> **Purpose**: Guide LLMs through explicit step-by-step reasoning to improve accuracy on complex tasks
> **Confidence**: 0.95
> **MCP Validated:** 2026-03-26

## Overview

Chain-of-Thought (CoT) prompting instructs the model to reason step by step before producing a final answer. In 2026, CoT has evolved into several variants: zero-shot CoT ("think step by step"), few-shot CoT (worked examples), self-consistency (sample multiple paths), and ReAct (interleave reasoning with tool actions). Claude 4.x models support "extended thinking" -- a native CoT mode where the model reasons in a dedicated thinking block before responding.

## The Pattern

```python
from anthropic import Anthropic

client = Anthropic()

# Claude extended thinking -- native CoT
response = client.messages.create(
    model="claude-sonnet-4-6",
    max_tokens=8000,
    thinking={
        "type": "enabled",
        "budget_tokens": 4000  # tokens allocated for reasoning
    },
    messages=[{
        "role": "user",
        "content": "Analyze this financial data and determine quarterly growth rate.\n\n{data}"
    }]
)
# Response includes thinking block (reasoning) + text block (answer)
for block in response.content:
    if block.type == "thinking":
        print(f"Reasoning: {block.thinking}")
    elif block.type == "text":
        print(f"Answer: {block.text}")
```

```python
# Traditional CoT with explicit instructions
from openai import OpenAI

client = OpenAI()

COT_PROMPT = """You are an expert data analyst.

## Task
Analyze the following financial data and determine the quarterly growth rate.

## Instructions
Think through this step by step:
1. Identify the revenue for each quarter
2. Calculate the difference between consecutive quarters
3. Compute the percentage change
4. Provide the final growth rate

## Important
Show your reasoning for each step before giving the final answer.

## Data
{data}

## Output Format
Return JSON: {{"steps": ["..."], "final_answer": <float>, "confidence": <float>}}
"""

def analyze_with_cot(data: str) -> dict:
    response = client.chat.completions.create(
        model="gpt-4o",
        temperature=0.0,
        messages=[
            {"role": "system", "content": "You reason step by step before answering."},
            {"role": "user", "content": COT_PROMPT.format(data=data)}
        ],
        response_format={"type": "json_object"}
    )
    return response.choices[0].message.content
```

## Quick Reference

| Variant | When to Use | Accuracy | Cost |
|---------|-------------|----------|------|
| Zero-shot CoT | Simple reasoning, add "think step by step" | +20-30% | Low |
| Few-shot CoT | Complex logic, provide worked examples | +25-40% | Medium |
| Self-consistency | High-stakes, sample N paths then vote | +10-15% over CoT | High |
| ReAct | Tool use, multi-step research | +30-50% | High |
| Extended Thinking (Claude) | Complex reasoning, coding, analysis | +30-50% | Medium |

## ReAct Pattern (Reason + Act)

```python
REACT_PROMPT = """Answer the question using the following format:

Thought: I need to figure out...
Action: search("query") or calculate("expression")
Observation: [result of action]
...repeat Thought/Action/Observation...
Thought: I now have enough information
Final Answer: [the answer]

Question: {question}"""

# ReAct is now built into agent frameworks (LangGraph, Claude Agent SDK)
# The manual prompt pattern above is for understanding -- use frameworks in production
```

## When NOT to Use CoT

- Simple lookups or classifications (adds unnecessary tokens)
- When latency is critical and accuracy is already high
- For tasks with single-step answers (e.g., sentiment: positive/negative)

## Self-Consistency Extension

```python
import json
from collections import Counter

def cot_with_self_consistency(prompt: str, n_samples: int = 5) -> str:
    """Sample multiple CoT paths, return majority answer."""
    answers = []
    for _ in range(n_samples):
        response = client.chat.completions.create(
            model="gpt-4o",
            temperature=0.7,  # Higher temp for diverse paths
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"}
        )
        result = json.loads(response.choices[0].message.content)
        answers.append(result.get("final_answer"))
    most_common = Counter(answers).most_common(1)[0][0]
    return most_common
```

## Related

- [Structured Extraction](../concepts/structured-extraction.md)
- [Validation Prompts](../patterns/validation-prompts.md)
- [Multi-Pass Extraction](../patterns/multi-pass-extraction.md)
