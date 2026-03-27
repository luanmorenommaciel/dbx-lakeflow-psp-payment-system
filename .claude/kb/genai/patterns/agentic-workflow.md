# Agentic Workflow Pattern

> **Purpose**: Multi-step agent workflow with plan-and-execute orchestration and model tiering
> **MCP Validated**: 2026-03-26

## When to Use

- Complex tasks requiring multiple reasoning steps and tool calls
- Tasks that benefit from decomposition into subtasks
- Workflows where cheaper models can execute plans from frontier models
- Scenarios needing iterative refinement with self-correction

## Architecture

```text
                    +-------------------+
                    |   User Request    |
                    +--------+----------+
                             |
                    +--------v----------+
                    |   Planner Agent   |  (Claude Opus 4.6 / GPT-4o)
                    | - Decompose task  |
                    | - Assign workers  |
                    | - Define success  |
                    +--------+----------+
                             |
              +--------------+--------------+
              |              |              |
     +--------v------+ +----v-------+ +----v--------+
     | Worker Agent 1 | | Worker 2   | | Worker 3    |  (Sonnet 4.6 / Haiku)
     | (Research)     | | (Analyze)  | | (Write)     |
     | + MCP tools    | | + tools    | | + tools     |
     +--------+------+ +----+-------+ +----+--------+
              |              |              |
              +--------------+--------------+
                             |
                    +--------v----------+
                    |  Reviewer Agent   |  (frontier model)
                    | - Check quality   |
                    | - Request revise  |
                    +--------+----------+
                             |
                    +--------v----------+
                    |   Final Output    |
                    +-------------------+
```

## Implementation with LangGraph

```python
from langgraph.graph import StateGraph, START, END
from langchain.chat_models import init_chat_model
from typing import TypedDict, Annotated, Any
import operator

class WorkflowState(TypedDict):
    messages: Annotated[list, operator.add]
    plan: list[dict]
    results: dict[str, Any]
    review_passed: bool

planner = init_chat_model("claude-opus-4-6", temperature=0)
worker = init_chat_model("claude-sonnet-4-6", temperature=0)
reviewer = init_chat_model("claude-opus-4-6", temperature=0)

def plan_step(state: WorkflowState):
    """Frontier model decomposes request into tasks."""
    response = planner.invoke([{
        "role": "system",
        "content": "Decompose into 2-5 executable tasks. Return JSON array."
    }] + state["messages"])
    return {"plan": parse_plan(response.content)}

def execute_step(state: WorkflowState):
    """Worker models execute each task with tools."""
    results = {}
    for task in state["plan"]:
        response = worker.invoke([{
            "role": "system",
            "content": f"Execute: {task['description']}. Context: {results}"
        }])
        results[task["id"]] = response.content
    return {"results": results}

def review_step(state: WorkflowState):
    """Frontier model reviews all outputs."""
    response = reviewer.invoke([{
        "role": "system",
        "content": "Review results. Respond PASS or NEEDS_REVISION."
    }, {
        "role": "user",
        "content": f"Plan: {state['plan']}\nResults: {state['results']}"
    }])
    return {"review_passed": "PASS" in response.content}

def route_review(state: WorkflowState) -> str:
    return END if state["review_passed"] else "execute"

graph = StateGraph(WorkflowState)
graph.add_node("plan", plan_step)
graph.add_node("execute", execute_step)
graph.add_node("review", review_step)
graph.add_edge(START, "plan")
graph.add_edge("plan", "execute")
graph.add_edge("execute", "review")
graph.add_conditional_edges("review", route_review, [END, "execute"])
workflow = graph.compile()
```

## Model Tiering Cost Comparison

```text
Frontier-only (Opus 4.6 everywhere):
  10 steps x $0.015/step = $0.15 per workflow

Plan-and-Execute (Opus plans, Sonnet executes):
  2 Opus ($0.015) + 8 Sonnet ($0.003) = $0.054
  Savings: ~64%

Plan-and-Execute (Opus plans, Haiku executes):
  2 Opus ($0.015) + 8 Haiku ($0.0005) = $0.034
  Savings: ~77%

Model Pricing (per 1M tokens, input/output):
  Claude Opus 4.6:   $15 / $75
  Claude Sonnet 4.6:  $3 / $15
  Claude Haiku 3.5:  $0.80 / $4
  GPT-4o:             $2.50 / $10
  GPT-4o-mini:        $0.15 / $0.60
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `planner_model` | `claude-opus-4-6` | Frontier model for planning and review |
| `worker_model` | `claude-sonnet-4-6` | Balanced model for task execution |
| `max_retries` | `2` | Max revision attempts per task |
| `timeout_seconds` | `120` | Per-task execution timeout |
| `parallel_execution` | `True` | Execute independent tasks concurrently |

## Example Usage

```python
result = await workflow.ainvoke({
    "messages": [{
        "role": "user",
        "content": "Research the top 3 competitors in the LLM observability space, "
                   "analyze their pricing models, and write a comparison report."
    }],
    "plan": [],
    "results": {},
    "review_passed": False,
})
```

## See Also

- [Multi-Agent Systems](../concepts/multi-agent-systems.md)
- [Tool Calling](../concepts/tool-calling.md)
- [Evaluation Framework](../patterns/evaluation-framework.md)
