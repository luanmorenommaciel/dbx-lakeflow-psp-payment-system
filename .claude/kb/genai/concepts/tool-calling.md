# Tool Calling

> **Purpose**: LLM function/tool calling protocols, MCP, Agent SDKs, and production integration
> **Confidence**: 0.95
> **MCP Validated**: 2026-03-26

## Overview

Tool calling enables LLMs to invoke external functions by generating structured JSON arguments. The LLM does not execute tools directly -- it produces a tool call request, and the application executes the function and returns results. In 2026, the Model Context Protocol (MCP) has emerged as the universal open standard for tool connectivity, with 12+ framework integrations. Agent SDKs from Anthropic and OpenAI package the full tool-calling loop into production-ready libraries.

## The Pattern

```python
from dataclasses import dataclass
from typing import Any, Callable
import json

@dataclass
class ToolDefinition:
    name: str
    description: str
    parameters: dict  # JSON Schema
    function: Callable
    requires_confirmation: bool = False

class ToolRegistry:
    """Protocol-agnostic tool registry."""
    def __init__(self):
        self._tools: dict[str, ToolDefinition] = {}

    def register(self, tool: ToolDefinition):
        self._tools[tool.name] = tool

    def get_schemas(self) -> list[dict]:
        """Export tool schemas for LLM (OpenAI format)."""
        return [{"type": "function", "function": {
            "name": t.name, "description": t.description,
            "parameters": t.parameters,
        }} for t in self._tools.values()]

    def execute(self, name: str, arguments: dict) -> Any:
        """Execute a tool by name with validated arguments."""
        tool = self._tools.get(name)
        if not tool:
            raise ValueError(f"Unknown tool: {name}")
        if tool.requires_confirmation:
            raise ConfirmationRequired(name, arguments)
        return tool.function(**arguments)
```

## Tool Calling Loop

```text
User Message --> LLM (with tool schemas)
                   |
                   +--> Text Response (no tool needed)
                   +--> Tool Call Request (name + args)
                          --> App executes function
                          --> Tool Result returned to LLM
                          --> LLM generates final response
```

## MCP (Model Context Protocol) -- The Universal Standard

```python
# MCP: Open standard for tool discovery and invocation
# Transport: Streamable HTTP (2025 revision, replaces SSE)
# Adopted by: Claude, LangGraph, CrewAI, OpenAI Agents SDK, Agno, and 12+ frameworks

from mcp.server import FastMCP

app = FastMCP("my-tools")

@app.tool()
def get_weather(city: str, units: str = "celsius") -> dict:
    """Get current weather for a city."""
    return weather_api.get(city, units)

@app.tool()
def query_database(sql: str) -> list[dict]:
    """Execute a read-only SQL query."""
    return db.execute(sql)

# MCP Resources -- expose data sources
@app.resource("docs://company-wiki")
def get_wiki():
    """Company knowledge base for RAG."""
    return wiki_content

# Client discovers and calls tools dynamically
# Any MCP-compatible client can connect: Claude Desktop, Claude Code, VS Code, etc.
tools = await mcp_client.list_tools()
result = await mcp_client.call_tool("get_weather", {"city": "Berlin"})
```

## Provider Comparison (2026)

| Feature | OpenAI | Anthropic | Google | MCP |
|---------|--------|-----------|--------|-----|
| Format | `tools` array | `tools` array | `tools` array | JSON-RPC |
| Schema | JSON Schema | JSON Schema | JSON Schema | JSON Schema |
| Parallel calls | Yes | Yes | Yes | Yes |
| Force tool use | `tool_choice: required` | `tool_choice: any` | `tool_config` | N/A |
| Max tools | 128 | 64 | 64 | Unlimited |
| Computer Use | No | Yes (Claude 4.x) | No | Via tools |
| Agent SDK | OpenAI Agents SDK | Claude Agent SDK | ADK | LangGraph |

## Common Mistakes

### Wrong
```python
# Trusting LLM-generated arguments without validation
tool_args = json.loads(llm_response.tool_call.arguments)
result = dangerous_function(**tool_args)  # SQL injection, path traversal
```

### Correct
```python
from pydantic import BaseModel, field_validator

class SearchArgs(BaseModel):
    query: str
    limit: int = 10
    @field_validator("limit")
    @classmethod
    def limit_range(cls, v):
        return max(1, min(v, 100))
    @field_validator("query")
    @classmethod
    def sanitize_query(cls, v):
        return v.replace(";", "").replace("--", "")

args = SearchArgs(**json.loads(llm_response.tool_call.arguments))
result = search_database_fn(query=args.query, limit=args.limit)
```

## Best Practices

| Practice | Rationale |
|----------|-----------|
| Validate all arguments with Pydantic | Prevent injection attacks |
| Add `requires_confirmation` for destructive ops | Human-in-the-loop safety |
| Limit tool count per request (< 20) | Reduces confusion and latency |
| Include clear descriptions and examples | Improves tool selection accuracy |
| Return structured results (not raw text) | Easier for LLM to reason about |
| Use MCP for tool distribution | Universal standard, reusable across clients |
| Version your tool schemas | Breaking changes break agents |

## Related

- [Multi-Agent Systems](../concepts/multi-agent-systems.md)
- [Guardrails](../concepts/guardrails.md)
- [Agentic Workflow Pattern](../patterns/agentic-workflow.md)
