# GenAI Architecture Quick Reference

> Fast lookup tables. For code examples, see linked files.
> **MCP Validated:** 2026-03-26

## Multi-Agent Frameworks (2026)

| Framework | Maintainer | Architecture | Strength | Production Ready |
|-----------|-----------|-------------|----------|-----------------|
| LangGraph | LangChain | Graph-based state machines | Durable execution, human-in-the-loop, memory | Yes |
| CrewAI | CrewAI Inc | Role-based crews | Simplest multi-agent setup, rapid prototyping | Yes |
| Claude Agent SDK | Anthropic | Tool-calling agent loop | Claude Code capabilities as library, MCP native | Yes |
| OpenAI Agents SDK | OpenAI | Handoff-based delegation | Native handoffs, Tracing, Guardrails | Yes |
| AutoGen (AG2) | Microsoft | Conversation-based | Enterprise multi-agent, Semantic Kernel integration | Yes |
| Agno | Agno | Minimal code agents | Fastest setup, model-agnostic | Yes |
| DSPy | Stanford NLP | Prompt optimization | Automated prompt tuning, self-improving | Yes |

## Orchestration Patterns

| Pattern | Topology | Best For | Complexity |
|---------|----------|----------|------------|
| Sequential | Chain | Step-by-step pipelines | Low |
| Concurrent | Fan-out | Independent parallel tasks | Medium |
| Hub-and-Spoke | Star | Centralized coordination (supervisor) | Medium |
| Mesh | Peer-to-peer | Resilient distributed systems | High |
| Plan-and-Execute | Hierarchical | Cost-optimized multi-step | High |
| Supervisor | LangGraph prebuilt | Multi-agent with central router | Medium |
| Swarm | Handoff-based | Dynamic agent delegation | Medium |

## RAG Architecture Variants (2026)

| Variant | Mechanism | Best For |
|---------|-----------|----------|
| Naive RAG | Single retrieval + generation | Simple Q&A |
| Advanced RAG | Hybrid search + reranking + query transform | Production systems |
| Agentic RAG | Agent decides when/what/how to retrieve | Multi-hop reasoning |
| GraphRAG | Knowledge graph + entity extraction | Relational reasoning |
| Corrective RAG | Verify relevance, re-retrieve if needed | Critical applications |
| Self-RAG | Self-reflective retrieval decisions | High-accuracy needs |
| Modular RAG | Swappable LEGO components | Flexible pipelines |
| Multimodal RAG | Text + image + table retrieval | Document understanding |

## Tool Calling Protocols

| Protocol | Provider | Transport | Status |
|----------|----------|-----------|--------|
| Function Calling | OpenAI | JSON Schema | Production |
| Tool Use | Anthropic | JSON Schema | Production |
| MCP | Anthropic (open standard) | Streamable HTTP | Production -- 12+ framework integrations |
| Function Calling | Google | JSON Schema | Production |
| Computer Use | Anthropic | Screenshots + actions | Production (Claude 4.x) |

## Model Landscape (March 2026)

| Model | Provider | Context | Strengths |
|-------|----------|---------|-----------|
| Claude Opus 4.6 | Anthropic | 1M tokens | Best for agents, coding, extended thinking |
| Claude Sonnet 4.6 | Anthropic | 1M tokens | Best speed/intelligence ratio, $3/$15 per M |
| GPT-4o | OpenAI | 128K tokens | Multimodal, fast, strong tool use |
| GPT-4.5 | OpenAI | 128K tokens | Enhanced reasoning, creative tasks |
| Gemini 2.0 Flash | Google | 1M tokens | Fast multimodal, grounding with Search |
| Gemini 2.5 Pro | Google | 1M tokens | Thinking model, strong reasoning |

## Guardrail Types

| Type | Layer | Purpose | Tools (2026) |
|------|-------|---------|-------------|
| Input rails | Pre-LLM | Filter harmful prompts | Llama Guard 4, NeMo, Guardrails AI |
| Output rails | Post-LLM | Validate responses | LLM-as-judge, Pydantic, LMQL |
| Topic rails | Pre-LLM | Restrict conversation scope | NeMo Guardrails, custom classifiers |
| Retrieval rails | Mid-pipeline | Filter retrieved content | Relevance threshold, cross-encoder |
| Tool rails | Pre-execution | Validate tool arguments | Pydantic, confirmation gates |

## Evaluation Metrics

| Metric | Measures | Range | Framework |
|--------|----------|-------|-----------|
| Faithfulness | Grounding in context | 0-1 | RAGAS, DeepEval |
| Answer Relevancy | Query-answer alignment | 0-1 | RAGAS, Braintrust |
| Context Precision | Retrieved relevance | 0-1 | RAGAS |
| Context Recall | Coverage of ground truth | 0-1 | RAGAS |
| Hallucination | Unsupported claims | 0-1 | Langfuse, DeepEval |
| Agent task success | End-to-end completion | 0-1 | Custom, Braintrust |

## Observability Tools (2026)

| Tool | Type | Strength | Cost |
|------|------|----------|------|
| Langfuse | OSS | Best free tier, prompt management, evals | Free / self-host |
| LangSmith | Commercial | Zero-config for LangChain, playground | Paid |
| Braintrust | Commercial | Best evaluation pipeline, $80M raised | $249/mo+ |
| Arize Phoenix | OSS | Traces, spans, embeddings | Free / self-host |
| Helicone | Commercial | Proxy integration, no code changes | Freemium |
| Datadog LLM | Commercial | Unified infra + LLM monitoring | Enterprise |

## Decision Matrix

| Use Case | Choose |
|----------|--------|
| Simple Q&A over docs | Advanced RAG pipeline (hybrid + rerank) |
| Multi-step research | Agentic RAG with tool calling |
| Customer support bot | State machine + RAG + guardrails |
| Data pipeline monitoring | Multi-agent crew with escalation |
| Content generation | LLM chain with evaluation loop |
| Code generation agent | Claude Agent SDK or LangGraph |

## Common Pitfalls

| Don't | Do |
|-------|-----|
| Use frontier models for every step | Plan-and-Execute with model tiering |
| Skip evaluation metrics | Implement RAGAS + LLM-as-judge from day one |
| Build monolithic agents | Compose specialized agents with clear handoffs |
| Ignore guardrails in production | Layer input, output, tool, and topic rails |
| Chunk documents blindly | Use semantic chunking with overlap |
| Skip observability | Instrument with Langfuse/LangSmith from day one |
| Build custom agent loop from scratch | Use LangGraph or Claude Agent SDK |

## Related Documentation

| Topic | Path |
|-------|------|
| Multi-Agent Design | `concepts/multi-agent-systems.md` |
| RAG Fundamentals | `concepts/rag-architecture.md` |
| Full Index | `index.md` |
