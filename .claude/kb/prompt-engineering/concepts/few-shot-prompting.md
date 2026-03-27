# Few-Shot Prompting

> **Purpose**: Teach LLMs expected output format and behavior through in-context examples
> **Confidence**: 0.95
> **MCP Validated:** 2026-03-26

## Overview

Few-shot prompting provides 1-5 input-output examples directly in the prompt to teach the model the desired format, tone, and behavior. This technique improves accuracy by 15-25% for format-sensitive tasks compared to zero-shot. The model learns the pattern from examples rather than relying solely on instructions, making outputs more consistent and predictable.

## The Pattern

```python
from openai import OpenAI

client = OpenAI()

FEW_SHOT_CLASSIFICATION = """You are an expert email classifier.

## Examples

Input: "Your order #12345 has shipped and will arrive by Friday"
Output: {{"category": "order_update", "priority": "low", "action": "none"}}

Input: "URGENT: Your account has been compromised, change password immediately"
Output: {{"category": "security_alert", "priority": "critical", "action": "escalate"}}

Input: "Hey, just wanted to follow up on our meeting next Tuesday"
Output: {{"category": "meeting", "priority": "medium", "action": "respond"}}

## Task
Classify the following email using the same format as the examples above.

Input: {email_text}
Output:"""

def classify_email(email_text: str) -> dict:
    response = client.chat.completions.create(
        model="gpt-4o",
        temperature=0.0,
        messages=[{"role": "user", "content": FEW_SHOT_CLASSIFICATION.format(
            email_text=email_text
        )}],
        response_format={"type": "json_object"}
    )
    return response.choices[0].message.content
```

## Quick Reference

| Shot Count | When to Use | Token Cost | Accuracy |
|------------|-------------|------------|----------|
| Zero-shot | Simple, well-defined tasks | Lowest | Baseline |
| One-shot | Format demonstration | Low | +10-15% |
| Few-shot (2-3) | Complex classification | Medium | +15-25% |
| Many-shot (4-5) | Nuanced edge cases | Higher | +20-30% |

## Example Selection Rules

1. **Cover edge cases** -- include at least one tricky example
2. **Show all categories** -- represent every possible output class
3. **Consistent format** -- all examples must follow the same structure
4. **Diverse inputs** -- vary length, style, and complexity
5. **Include negative** -- show what should NOT match

## Common Mistakes

### Wrong

```python
# Examples with inconsistent format
examples = """
Example 1: The sentiment is positive
Example 2: {"sentiment": "negative"}
Example 3: Neutral sentiment detected
"""
```

### Correct

```python
# Consistent JSON format across all examples
examples = """
Input: "I love this product!"
Output: {"sentiment": "positive", "confidence": 0.95}

Input: "Terrible experience, never again"
Output: {"sentiment": "negative", "confidence": 0.92}

Input: "The package arrived on time"
Output: {"sentiment": "neutral", "confidence": 0.78}
"""
```

## Dynamic Few-Shot Selection

```python
from typing import List

def select_examples(query: str, example_bank: List[dict], k: int = 3) -> List[dict]:
    """Select the most relevant examples for a given query.

    In production, use embedding similarity to find the closest
    examples from your example bank.
    """
    # Simple keyword overlap (replace with embeddings in production)
    scored = []
    query_words = set(query.lower().split())
    for ex in example_bank:
        input_words = set(ex["input"].lower().split())
        overlap = len(query_words & input_words)
        scored.append((overlap, ex))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [ex for _, ex in scored[:k]]


def build_few_shot_prompt(query: str, examples: List[dict], task: str) -> str:
    """Build a few-shot prompt from selected examples."""
    example_text = "\n\n".join(
        f'Input: "{ex["input"]}"\nOutput: {ex["output"]}'
        for ex in examples
    )
    return f"""## Examples\n\n{example_text}\n\n## Task\n{task}\n\nInput: "{query}"\nOutput:"""
```

## Related

- [System Prompts](../concepts/system-prompts.md)
- [Output Formatting](../concepts/output-formatting.md)
- [Prompt Template Pattern](../patterns/prompt-template.md)
