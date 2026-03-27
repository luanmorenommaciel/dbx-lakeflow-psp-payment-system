# GenAI Architecture Knowledge Base

> **Purpose**: Architecture patterns for GenAI systems -- multi-agent orchestration, agentic workflows, RAG, chatbots, LLM pipelines, MCP, Agent SDKs
> **MCP Validated**: 2026-03-26

## Quick Navigation

### Concepts (< 150 lines each)

| File | Purpose |
|------|---------|
| [concepts/multi-agent-systems.md](concepts/multi-agent-systems.md) | Agent orchestration: LangGraph, CrewAI, Claude Agent SDK, OpenAI Agents SDK |
| [concepts/rag-architecture.md](concepts/rag-architecture.md) | RAG variants: naive, agentic, GraphRAG, corrective, modular |
| [concepts/state-machines.md](concepts/state-machines.md) | Finite state machines with LangGraph graph-based orchestration |
| [concepts/tool-calling.md](concepts/tool-calling.md) | Tool calling protocols, MCP (Model Context Protocol), Agent SDKs |
| [concepts/guardrails.md](concepts/guardrails.md) | Defense-in-depth: NeMo, Llama Guard 4, prompt injection defense |

### Patterns (< 200 lines each)

| File | Purpose |
|------|---------|
| [patterns/agentic-workflow.md](patterns/agentic-workflow.md) | Multi-step agent workflow with plan-and-execute and model tiering |
| [patterns/chatbot-architecture.md](patterns/chatbot-architecture.md) | Production chatbot with state management, routing, and MCP tools |
| [patterns/rag-pipeline.md](patterns/rag-pipeline.md) | End-to-end RAG with hybrid search, reranking, and context engineering |
| [patterns/evaluation-framework.md](patterns/evaluation-framework.md) | LLM evaluation with LLM-as-judge, RAGAS, DeepEval, Braintrust |

### Specs (Machine-Readable)

| File | Purpose |
|------|---------|
| [specs/genai-patterns.yaml](specs/genai-patterns.yaml) | Architecture patterns specification with decision matrix |

---

## Quick Reference

- [quick-reference.md](quick-reference.md) - Fast lookup tables

---

## Key Concepts

| Concept | Description |
|---------|-------------|
| **Multi-Agent Systems** | Orchestration via LangGraph, CrewAI, Claude Agent SDK, OpenAI Agents SDK |
| **RAG Architecture** | Agentic RAG, GraphRAG, hybrid search, context engineering |
| **State Machines** | Graph-based orchestration with LangGraph StateGraph and Functional API |
| **Tool Calling** | MCP protocol, structured function invocation, Agent SDKs |
| **Guardrails** | Defense-in-depth with Llama Guard 4, NeMo, OWASP LLM Top 10 compliance |

---

## Learning Path

| Level | Files |
|-------|-------|
| **Beginner** | concepts/tool-calling.md, concepts/rag-architecture.md |
| **Intermediate** | concepts/multi-agent-systems.md, patterns/rag-pipeline.md |
| **Advanced** | patterns/agentic-workflow.md, patterns/evaluation-framework.md |

---

## Agent Usage

| Agent | Primary Files | Use Case |
|-------|---------------|----------|
| genai-architect | patterns/agentic-workflow.md, patterns/chatbot-architecture.md | Design multi-agent and chatbot systems |
| genai-architect | patterns/rag-pipeline.md, concepts/rag-architecture.md | Build RAG pipelines with evaluation |
