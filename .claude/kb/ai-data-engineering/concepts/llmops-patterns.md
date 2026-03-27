# LLMOps Patterns

> **Purpose**: Prompt versioning, token economics, guardrails, evaluation, observability (Langfuse, Braintrust)
> **Confidence**: 0.90
> **MCP Validated**: 2026-03-26

## Overview

LLMOps applies MLOps principles to large language model deployments: version prompts like code, track token costs as a first-class metric, enforce guardrails on both input and output, evaluate quality systematically, and observe production behavior with tracing. In 2026, the tooling has matured significantly: Langfuse (OSS, best free tier), Braintrust ($80M raised, best eval pipeline), and LangSmith (LangChain native) are the production standards.

## The Concept

```python
# LLMOps: Prompt versioning + guardrails + cost tracking + observability
from dataclasses import dataclass, field
from datetime import datetime
import tiktoken

@dataclass
class VersionedPrompt:
    """Prompt with version tracking, cost estimation, and guardrails."""
    name: str
    version: str
    template: str
    model: str = "claude-sonnet-4-6"
    max_input_tokens: int = 4096
    max_output_tokens: int = 1024
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    # Cost per million tokens (input / output) -- March 2026
    COST_TABLE = {
        "claude-opus-4-6": (15.00, 75.00),
        "claude-sonnet-4-6": (3.00, 15.00),
        "claude-haiku-3-5": (0.80, 4.00),
        "gpt-4o": (2.50, 10.00),
        "gpt-4o-mini": (0.15, 0.60),
        "gemini-2.5-pro": (1.25, 10.00),
        "gemini-2.0-flash": (0.10, 0.40),
    }

    def estimate_cost(self, input_tokens: int, output_tokens: int) -> float:
        """Estimate cost in USD for a single call."""
        input_rate, output_rate = self.COST_TABLE.get(self.model, (0, 0))
        return (input_tokens * input_rate + output_tokens * output_rate) / 1_000_000

    def validate_input(self, user_input: str) -> tuple[bool, str]:
        """Input guardrails: length, injection, PII patterns."""
        enc = tiktoken.encoding_for_model("gpt-4o")
        tokens = len(enc.encode(user_input))
        if tokens > self.max_input_tokens:
            return False, f"Input too long: {tokens} > {self.max_input_tokens}"
        injection_patterns = ["ignore previous", "system:", "you are now",
                              "disregard", "forget your instructions"]
        for pattern in injection_patterns:
            if pattern.lower() in user_input.lower():
                return False, f"Potential prompt injection: '{pattern}'"
        return True, "OK"
```

## Observability with Langfuse (2026)

```python
from langfuse import Langfuse
from langfuse.decorators import observe, langfuse_context

langfuse = Langfuse()

@observe()  # auto-traces function calls, LLM invocations, costs
def rag_pipeline(query: str) -> str:
    # Langfuse auto-captures: latency, tokens, cost, model
    chunks = retrieve(query)
    langfuse_context.update_current_observation(
        metadata={"num_chunks": len(chunks), "query": query}
    )
    answer = generate(query, chunks)
    # Score the output for quality monitoring
    langfuse_context.score_current_trace(
        name="relevancy", value=score_relevancy(query, answer)
    )
    return answer
```

## Quick Reference

| LLMOps Area | Tools (2026) | Key Metric |
|-------------|-------------|-----------|
| Prompt versioning | Git, Langfuse, Humanloop, LangSmith Hub | Prompt version -> quality delta |
| Token economics | tiktoken, LiteLLM, Langfuse | Cost per query, cost per user |
| Input guardrails | Llama Guard 4, NeMo, Guardrails AI | Blocked injection rate |
| Output guardrails | Guardrails AI, LMQL, Pydantic | Hallucination rate, refusal rate |
| Evaluation | RAGAS, DeepEval, Braintrust | Faithfulness, relevancy, F1 |
| Observability | Langfuse, Braintrust, LangSmith, Arize | Latency p99, error rate, drift |
| A/B testing | Braintrust, custom | Win rate, preference score |
| Cost tracking | Langfuse, Helicone, LiteLLM | Cost per query, budget alerts |

| Eval Metric (RAG) | Measures | Target |
|--------------------|----------|--------|
| Faithfulness | Answer grounded in context | > 0.90 |
| Answer relevancy | Answer addresses the question | > 0.85 |
| Context precision | Retrieved docs are relevant | > 0.80 |
| Context recall | All needed info was retrieved | > 0.80 |
| Hallucination | Unsupported claims | < 0.20 |

## Model Pricing (March 2026)

| Model | Input/1M | Output/1M | Context | Strength |
|-------|---------|----------|---------|----------|
| Claude Opus 4.6 | $15 | $75 | 1M | Best agents, complex reasoning |
| Claude Sonnet 4.6 | $3 | $15 | 1M | Best cost/quality ratio |
| Claude Haiku 3.5 | $0.80 | $4 | 200K | Fastest, cheapest Claude |
| GPT-4o | $2.50 | $10 | 128K | Strong multimodal, tool use |
| GPT-4o-mini | $0.15 | $0.60 | 128K | Cheapest quality option |
| Gemini 2.5 Pro | $1.25 | $10 | 1M | Thinking model, code |
| Gemini 2.0 Flash | $0.10 | $0.40 | 1M | Cheapest, fastest |

## Common Mistakes

### Wrong
```python
# Hardcoded prompts with no versioning, cost tracking, or observability
response = llm.chat("You are a helpful assistant. Answer: " + user_query)
```

### Correct
```python
# Versioned prompt with Langfuse observability
from langfuse.decorators import observe

@observe()
def answer_query(user_query: str) -> str:
    prompt = VersionedPrompt(
        name="rag-answer", version="2.1.0",
        template="Answer based on context:\n{context}\n\nQuestion: {question}",
        model="claude-sonnet-4-6",
    )
    valid, msg = prompt.validate_input(user_query)
    if not valid:
        return fallback_response(msg)
    response = llm.chat(prompt.template.format(context=ctx, question=user_query))
    cost = prompt.estimate_cost(input_tokens=850, output_tokens=200)
    return response
```

## Related

- [rag-pipelines](../concepts/rag-pipelines.md)
- [feature-stores](../concepts/feature-stores.md)
- [rag-pipeline-implementation](../patterns/rag-pipeline-implementation.md)
