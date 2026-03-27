# State Machines for Conversations

> **Purpose**: Graph-based orchestration for deterministic conversational flow control with LangGraph
> **Confidence**: 0.95
> **MCP Validated**: 2026-03-26

## Overview

State machines bring deterministic control to LLM-powered conversations. Each conversation exists in exactly one state at a time, with defined transitions triggered by user input or LLM decisions. In 2026, LangGraph has become the standard implementation: its StateGraph API provides typed state, conditional edges, durable execution, and built-in support for human-in-the-loop patterns. The Functional API (@task/@entrypoint decorators) offers an alternative for simpler workflows.

## The Pattern: LangGraph StateGraph

```python
from langgraph.graph import StateGraph, START, END, MessagesState
from langchain.chat_models import init_chat_model
from langchain.messages import SystemMessage

model = init_chat_model("claude-sonnet-4-6", temperature=0)

def greet(state: MessagesState):
    return {"messages": [model.invoke(
        [SystemMessage(content="Greet the user warmly.")] + state["messages"])]}

def classify_intent(state: MessagesState):
    return {"messages": [model.invoke(
        [SystemMessage(content="Classify intent: billing, technical, general.")] +
        state["messages"])]}

def gather_info(state: MessagesState):
    return {"messages": [model.invoke(
        [SystemMessage(content="Collect required info. Ask one question at a time.")] +
        state["messages"])]}

def resolve(state: MessagesState):
    return {"messages": [model.invoke(
        [SystemMessage(content="Resolve the issue using available context.")] +
        state["messages"])]}

def route_by_intent(state: MessagesState) -> str:
    last = state["messages"][-1].content.lower()
    if "billing" in last:
        return "gather_info"
    elif "technical" in last:
        return "gather_info"
    return "resolve"

# Build the conversation graph
workflow = StateGraph(MessagesState)
workflow.add_node("greet", greet)
workflow.add_node("classify", classify_intent)
workflow.add_node("gather_info", gather_info)
workflow.add_node("resolve", resolve)

workflow.add_edge(START, "greet")
workflow.add_edge("greet", "classify")
workflow.add_conditional_edges("classify", route_by_intent, {
    "gather_info": "gather_info",
    "resolve": "resolve",
})
workflow.add_edge("gather_info", "resolve")
workflow.add_edge("resolve", END)

app = workflow.compile()
```

## The Pattern: LangGraph Functional API

```python
from langgraph.func import entrypoint, task
from langgraph.graph import add_messages
from langchain_core.messages import BaseMessage

@task
def classify(messages: list[BaseMessage]):
    """Classify user intent."""
    return model.invoke(
        [SystemMessage(content="Classify: billing, technical, general")] + messages)

@task
def handle_billing(messages: list[BaseMessage]):
    return model.invoke(
        [SystemMessage(content="Handle billing inquiry.")] + messages)

@task
def handle_technical(messages: list[BaseMessage]):
    return model.invoke(
        [SystemMessage(content="Handle technical issue.")] + messages)

@entrypoint()
def support_bot(messages: list[BaseMessage]):
    intent = classify(messages).result()
    if "billing" in intent.content.lower():
        response = handle_billing(add_messages(messages, intent)).result()
    else:
        response = handle_technical(add_messages(messages, intent)).result()
    return add_messages(messages, [intent, response])
```

## Quick Reference

| State Pattern | Description | Example |
|---------------|-------------|---------|
| Linear | Fixed sequence of states | Onboarding wizard |
| Branching | Multiple paths from one state | Intent routing |
| Looping | Return to previous state | Slot filling, retries |
| Parallel | Multiple active substates | Multi-topic chat |
| Hierarchical | Nested state machines (subgraphs) | Complex workflows |
| Interrupt | Pause for human approval | Human-in-the-loop |

## LangGraph Features (2026)

| Feature | Description |
|---------|-------------|
| Durable execution | Survives process restarts, checkpoints state |
| Human-in-the-loop | `interrupt()` pauses graph for approval |
| Memory | Short-term (thread) and long-term (cross-thread) |
| Streaming | Token-by-token and node-by-node streaming |
| Subgraphs | Nested graphs for modular design |
| Time travel | Replay from any checkpoint |
| Deployment | LangGraph Platform or self-hosted |

## Design Principles

```text
1. Each node has ONE clear purpose (single responsibility)
2. Transitions are explicit and auditable (conditional edges)
3. Every state has a fallback/timeout (no dead ends)
4. Use interrupt() for human-in-the-loop approval gates
5. LLM decides WITHIN nodes; edges decide BETWEEN nodes
6. Use subgraphs to compose complex multi-agent systems
```

## Common Mistakes

### Wrong
```python
# No state boundaries -- LLM controls entire flow
response = llm.chat("You are a support bot. Handle everything.")
# Result: unpredictable flow, no auditability, no escalation
```

### Correct
```python
# Graph-based orchestration with clear boundaries
graph = StateGraph(MessagesState)
graph.add_node("classify", classify_intent)
graph.add_node("gather", gather_info)
graph.add_node("resolve", resolve)
graph.add_conditional_edges("classify", route_by_intent)
# Result: deterministic routing, auditable transitions, testable nodes
```

## Related

- [Multi-Agent Systems](../concepts/multi-agent-systems.md)
- [Chatbot Architecture Pattern](../patterns/chatbot-architecture.md)
- [Guardrails](../concepts/guardrails.md)
