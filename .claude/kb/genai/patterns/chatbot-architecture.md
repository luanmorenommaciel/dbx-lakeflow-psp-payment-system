# Chatbot Architecture Pattern

> **Purpose**: Production chatbot with LangGraph state management, MCP tools, guardrails, and RAG integration
> **MCP Validated**: 2026-03-26

## When to Use

- Customer-facing conversational AI requiring reliability and safety
- Support bots with intent routing and escalation flows
- Domain-specific assistants that need grounded answers (RAG)
- Any chatbot requiring auditable conversation flows

## Architecture

```text
                 +------------------+
                 |   User Message   |
                 +--------+---------+
                          |
                 +--------v---------+
                 |   Input Rails    |  Jailbreak, PII, topic check
                 +--------+---------+
                          |
                 +--------v---------+
                 | Intent Classifier |  LLM or fine-tuned model
                 +--------+---------+
                          |
          +---------------+---------------+
          |               |               |
  +-------v------+ +-----v------+ +------v-------+
  |   FAQ / RAG  | |  Transact  | |  Escalation  |
  | (retrieval)  | | (tool call)| | (human hand) |
  +-------+------+ +-----+------+ +------+-------+
          |               |               |
          +---------------+---------------+
                          |
                 +--------v---------+
                 |   Output Rails   |  Factuality, toxicity, format
                 +--------+---------+
                          |
                 +--------v---------+
                 |  Response + Cite  |
                 +------------------+
```

## Implementation

```python
from dataclasses import dataclass, field
from typing import Optional
from enum import Enum

class Intent(Enum):
    FAQ = "faq"
    TRANSACTIONAL = "transactional"
    ESCALATION = "escalation"
    CHITCHAT = "chitchat"
    UNKNOWN = "unknown"

@dataclass
class ConversationTurn:
    role: str  # "user" or "assistant"
    content: str
    intent: Optional[Intent] = None
    metadata: dict = field(default_factory=dict)

@dataclass
class ChatbotConfig:
    model: str = "claude-sonnet-4-6"
    max_history_turns: int = 20
    rag_enabled: bool = True
    guardrails_enabled: bool = True
    escalation_threshold: int = 3  # failed turns before escalation
    system_prompt: str = ""

class ProductionChatbot:
    def __init__(self, config: ChatbotConfig, rag_pipeline=None,
                 tool_registry=None, guardrail_pipeline=None):
        self.config = config
        self.rag = rag_pipeline
        self.tools = tool_registry
        self.guardrails = guardrail_pipeline
        self.history: list[ConversationTurn] = []
        self.failed_turns: int = 0

    async def respond(self, user_message: str) -> str:
        # 1. Input guardrails
        if self.guardrails:
            rail_result = self.guardrails.check_input(user_message)
            if rail_result.action == "block":
                return rail_result.message

        # 2. Classify intent
        intent = await self._classify_intent(user_message)

        # 3. Route by intent
        if intent == Intent.FAQ and self.rag:
            response = await self._handle_faq(user_message)
        elif intent == Intent.TRANSACTIONAL and self.tools:
            response = await self._handle_transaction(user_message)
        elif intent == Intent.ESCALATION or self.failed_turns >= self.config.escalation_threshold:
            response = await self._handle_escalation(user_message)
        else:
            response = await self._handle_general(user_message)

        # 4. Output guardrails
        if self.guardrails:
            rail_result = self.guardrails.check_output(response)
            if rail_result.action == "modify":
                response = rail_result.modified_content

        # 5. Store turn
        self.history.append(ConversationTurn("user", user_message, intent))
        self.history.append(ConversationTurn("assistant", response))

        return response

    async def _handle_faq(self, query: str) -> str:
        """Retrieve context and generate grounded answer."""
        chunks = self.rag.retrieve(query)
        if not chunks:
            self.failed_turns += 1
            return "I don't have information on that topic. Let me connect you with support."
        context = "\n".join(c.text for c in chunks)
        citations = [c.metadata.get("source", "") for c in chunks]
        prompt = (
            f"{self.config.system_prompt}\n\n"
            f"Context:\n{context}\n\n"
            f"Answer the user's question. Cite sources.\n"
            f"Question: {query}"
        )
        response = await self._call_llm(prompt)
        self.failed_turns = 0
        return response

    async def _handle_transaction(self, message: str) -> str:
        """Execute transactional operations via tool calling."""
        response = await self._call_llm_with_tools(
            message, self.tools.get_schemas()
        )
        return response

    async def _handle_escalation(self, message: str) -> str:
        """Hand off to human agent."""
        self.failed_turns = 0
        return ("I'm connecting you with a human agent who can help further. "
                "Please hold while I transfer your conversation.")
```

## Session Management

```python
@dataclass
class Session:
    session_id: str
    user_id: str
    history: list[ConversationTurn] = field(default_factory=list)
    context: dict = field(default_factory=dict)
    created_at: str = ""
    ttl_minutes: int = 30

    def trim_history(self, max_turns: int = 20):
        """Keep system prompt + last N turns to manage context window."""
        if len(self.history) > max_turns:
            self.history = self.history[-max_turns:]

    def to_messages(self) -> list[dict]:
        """Convert to LLM message format."""
        return [{"role": t.role, "content": t.content} for t in self.history]
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `max_history_turns` | `20` | Context window management |
| `escalation_threshold` | `3` | Failed turns before human handoff |
| `rag_similarity_threshold` | `0.7` | Minimum retrieval relevance |
| `session_ttl_minutes` | `30` | Session expiration |

## See Also

- [State Machines](../concepts/state-machines.md)
- [Guardrails](../concepts/guardrails.md)
- [RAG Pipeline](../patterns/rag-pipeline.md)
