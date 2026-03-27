# Guardrails

> **Purpose**: Defense-in-depth safety layers for LLM applications -- prompt injection defense, content safety, OWASP LLM Top 10
> **Confidence**: 0.95
> **MCP Validated**: 2026-03-26

## Overview

Guardrails are programmable safety layers that intercept inputs and outputs of LLM systems to enforce policies. In 2026, guardrails are no longer optional -- OWASP LLM Top 10 lists prompt injection, sensitive information disclosure, and excessive agency as top threats. Production systems require defense-in-depth: multiple layers of input validation, output filtering, tool-use restrictions, and continuous monitoring. Prompt injection appears in 73% of production AI deployments assessed during security audits, yet only 35% of organizations have dedicated defenses.

## The Pattern

```python
from dataclasses import dataclass
from typing import Optional
from enum import Enum
from abc import ABC, abstractmethod

class RailAction(Enum):
    ALLOW = "allow"
    BLOCK = "block"
    MODIFY = "modify"
    ESCALATE = "escalate"

@dataclass
class RailResult:
    action: RailAction
    message: Optional[str] = None
    modified_content: Optional[str] = None
    violated_policy: Optional[str] = None

class GuardRail(ABC):
    @abstractmethod
    def check(self, content: str, context: dict) -> RailResult:
        pass

class PromptInjectionRail(GuardRail):
    """Detect and block prompt injection attacks (OWASP LLM01)."""
    def __init__(self, classifier_model: str = "meta-llama/Llama-Guard-4"):
        self.classifier = classifier_model

    def check(self, content: str, context: dict) -> RailResult:
        # Multi-layer detection: pattern matching + classifier
        if self._pattern_check(content):
            return RailResult(action=RailAction.BLOCK,
                violated_policy="prompt_injection_pattern")
        if self._classifier_check(content):
            return RailResult(action=RailAction.BLOCK,
                violated_policy="prompt_injection_classifier")
        return RailResult(action=RailAction.ALLOW)

    def _pattern_check(self, content: str) -> bool:
        patterns = ["ignore previous", "system:", "you are now",
                     "disregard", "forget your instructions"]
        return any(p.lower() in content.lower() for p in patterns)

class OutputFactualityRail(GuardRail):
    """Check output is grounded in provided context (anti-hallucination)."""
    def check(self, content: str, context: dict) -> RailResult:
        retrieved = context.get("retrieved_chunks", "")
        if not retrieved:
            return RailResult(action=RailAction.ALLOW)
        is_grounded = self._check_grounding(content, retrieved)
        if is_grounded:
            return RailResult(action=RailAction.ALLOW)
        return RailResult(action=RailAction.MODIFY,
            modified_content="I don't have enough information to answer that.",
            violated_policy="factuality")
```

## Defense-in-Depth Pipeline

```text
User Input
    |
[Input Rails]      -- prompt injection (Llama Guard 4), PII redaction, topic check
    |
[Tool Rails]       -- validate tool arguments, block destructive operations
    |
[Retrieval Rails]  -- relevance filtering, content safety on retrieved docs
    |
[LLM Generation]
    |
[Output Rails]     -- factuality check, toxicity filter, PII scan, format validation
    |
Safe Response
```

## Guardrail Tools (2026)

| Tool | Type | Strength | Latency |
|------|------|----------|---------|
| Llama Guard 4 | OSS classifier | Multi-category safety, 14 risk categories | 50-200ms |
| NeMo Guardrails | NVIDIA OSS framework | Configurable rails, Colang DSL | 100-300ms |
| Guardrails AI | OSS Python library | Pydantic-based validators, 50+ pre-built | 10-100ms |
| LMQL | Query language | Constrained decoding, output control | 5-20ms |
| Galileo Protect | Commercial | Real-time hallucination detection | 50-150ms |
| Prompt Armor | Commercial | Injection detection API | 30-100ms |

## OWASP LLM Top 10 (2025-2026)

| Risk | Guardrail Strategy |
|------|-------------------|
| LLM01: Prompt Injection | Llama Guard + pattern detection + input/output separation |
| LLM02: Sensitive Information Disclosure | PII redaction + output scanning + data masking |
| LLM03: Supply Chain | Model provenance + package scanning |
| LLM06: Excessive Agency | Tool confirmation gates + action allowlists |
| LLM09: Overreliance | Confidence scores + factuality checking |

## Quick Reference

| Rail | Stage | Latency | Technique |
|------|-------|---------|-----------|
| Prompt injection | Input | 50-200ms | Llama Guard 4 + pattern matching |
| PII redaction | Input | 10-50ms | Regex + NER + Microsoft Presidio |
| Topic control | Input | 100-300ms | LLM classifier or NeMo Guardrails |
| Tool validation | Pre-execution | 5-20ms | Pydantic + allowlist |
| Relevance filter | Retrieval | 10-50ms | Similarity threshold |
| Factuality check | Output | 200-500ms | LLM-as-judge |
| Toxicity filter | Output | 50-200ms | Llama Guard 4 classifier |
| Format validation | Output | 5-20ms | JSON Schema / Pydantic |

## Common Mistakes

### Wrong
```python
# Guardrails only on output -- misses prompt injection
response = llm.generate(user_input)  # user_input could be a jailbreak
filtered = toxicity_filter(response)  # too late
```

### Correct
```python
pipeline = GuardrailPipeline(rails=[
    PromptInjectionRail(model="llama-guard-4"),  # pre-LLM
    PIIRedactor(),
    TopicRail(["billing", "support"]),
    ToolValidationRail(allowed_tools=["search", "lookup"]),
    FactualityChecker(),      # post-LLM
    ToxicityFilter(),
    FormatValidator(schema),
])
result = pipeline.run(user_input)
```

## Related

- [State Machines](../concepts/state-machines.md)
- [Chatbot Architecture Pattern](../patterns/chatbot-architecture.md)
- [Tool Calling](../concepts/tool-calling.md)
