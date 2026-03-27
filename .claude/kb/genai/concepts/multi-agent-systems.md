# Multi-Agent Systems

> **Purpose**: Orchestration patterns for coordinating multiple specialized LLM agents
> **Confidence**: 0.95
> **MCP Validated**: 2026-03-26

## Overview

Multi-agent systems coordinate multiple specialized LLM agents to solve complex tasks. Each agent has a defined role, tools, and instructions. An orchestrator manages handoffs, state sharing, and task routing. In 2026, the ecosystem has consolidated around production-grade frameworks: LangGraph (graph-based state machines), CrewAI (role-based crews), Claude Agent SDK (tool-calling agent loop), and OpenAI Agents SDK (handoff-based delegation). MCP (Model Context Protocol) has emerged as the universal tool connectivity standard.

## Core Topologies

```text
SEQUENTIAL         CONCURRENT          HUB-AND-SPOKE        MESH
A -> B -> C        A --|                    B                A <-> B
                   B --|-> Merge        /   |   \            |  X  |
                   C --|            A --  Hub  -- D           C <-> D
                                        \   |   /
                                            C

SUPERVISOR (LangGraph)           SWARM (OpenAI Agents SDK)
     Supervisor                  A --handoff--> B
    /    |    \                  B --handoff--> C
   A     B     C                 C --handoff--> A
   (router decides next)         (agents delegate dynamically)
```

## Framework Comparison (2026)

| Framework | Architecture | Best For | MCP Support |
|-----------|-------------|----------|-------------|
| LangGraph | StateGraph + conditional edges | Production control, durable execution | Yes |
| CrewAI | Role-based crews with tasks | Rapid prototyping, role assignment | Yes |
| Claude Agent SDK | Agent loop with tool calling | Claude-native apps, security-first | Native |
| OpenAI Agents SDK | Handoff-based delegation | OpenAI ecosystem, tracing built-in | Yes |
| AutoGen (AG2) | Conversation patterns | Enterprise, Semantic Kernel integration | Yes |
| Agno | Minimal-code agents | Fastest setup, model-agnostic | Yes |

## The Pattern: LangGraph Multi-Agent

```python
from langgraph.graph import StateGraph, START, END, MessagesState
from langchain.chat_models import init_chat_model

# Specialized agents as graph nodes
researcher_model = init_chat_model("claude-sonnet-4-6", temperature=0)
writer_model = init_chat_model("claude-sonnet-4-6", temperature=0.3)

def researcher(state: MessagesState):
    """Research agent with search tools."""
    return {"messages": [researcher_model.invoke(state["messages"])]}

def writer(state: MessagesState):
    """Writing agent -- synthesizes research into content."""
    return {"messages": [writer_model.invoke(state["messages"])]}

def router(state: MessagesState) -> str:
    """Supervisor decides next agent based on last message."""
    last = state["messages"][-1]
    if "RESEARCH_COMPLETE" in last.content:
        return "writer"
    return "researcher"

# Build the multi-agent graph
graph = StateGraph(MessagesState)
graph.add_node("researcher", researcher)
graph.add_node("writer", writer)
graph.add_edge(START, "researcher")
graph.add_conditional_edges("researcher", router, ["writer", "researcher"])
graph.add_edge("writer", END)
agent = graph.compile()
```

## The Pattern: Claude Agent SDK

```python
# Claude Agent SDK -- packages Claude Code capabilities as a library
from claude_agent_sdk import Agent, Tool

agent = Agent(
    model="claude-sonnet-4-6",
    tools=[Tool.computer_use(), Tool.bash(), Tool.file_editor()],
    system_prompt="You are a code review specialist.",
    max_turns=10,
)
# Agent loop handles tool calling, retries, and context management
result = await agent.run("Review the PR at https://github.com/org/repo/pull/42")
```

## Quick Reference

| Topology | Latency | Cost | Fault Tolerance | Use Case |
|----------|---------|------|-----------------|----------|
| Sequential | High | Low | Low | Step-by-step workflows |
| Concurrent | Low | High | Medium | Parallel analysis |
| Hub-and-Spoke | Medium | Medium | Medium | Centralized control |
| Mesh | Medium | High | High | Resilient systems |
| Supervisor | Variable | Medium | Medium | Multi-agent routing |
| Plan-and-Execute | Variable | Low | Medium | Cost-optimized pipelines |

## Cost Optimization: Plan-and-Execute

```python
# Frontier model plans, cheaper models execute
# Claude Opus 4.6 plans -> Sonnet 4.6 or Haiku executes
# Result: ~70-90% cost reduction vs. frontier-for-everything
plan_agent = Agent(model="claude-opus-4-6", role="planner")
exec_agents = {
    "search": Agent(model="claude-sonnet-4-6", role="researcher", tools=[web_search]),
    "write": Agent(model="claude-sonnet-4-6", role="writer"),
}
```

## Common Mistakes

### Wrong
```python
# Single monolithic agent doing everything
agent = Agent(
    system_prompt="You are an expert at research, writing, coding, review...",
    tools=[search, write, code, review, deploy, monitor],  # too many tools
)
```

### Correct
```python
# Specialized agents with clear responsibilities and model tiering
researcher = Agent(model="claude-sonnet-4-6", tools=[web_search, doc_search])
writer = Agent(model="claude-sonnet-4-6", tools=[])
reviewer = Agent(model="claude-opus-4-6", tools=[])  # frontier for quality
```

## Related

- [State Machines](../concepts/state-machines.md)
- [Tool Calling](../concepts/tool-calling.md)
- [Agentic Workflow Pattern](../patterns/agentic-workflow.md)
