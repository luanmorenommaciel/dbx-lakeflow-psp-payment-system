> **MCP Validated:** 2026-02-17

# Custom AI Skills Integration

> **Purpose**: Building, configuring, and integrating custom AI Skills in Fabric -- skill definition, endpoint config, input/output mapping, and programmatic usage

## When to Use

- Enabling business users to query structured data using natural language
- Creating reusable AI endpoints that combine data context with LLMs
- Integrating AI Skills into notebooks, dataflows, or external applications
- Building domain-specific Q&A interfaces over Fabric datasets

## Overview

AI Skills in Fabric are custom natural language interfaces backed by Azure OpenAI. Each AI Skill is configured with data sources (Lakehouse tables, Warehouse tables), instructions for the LLM, and example questions. When a user asks a question, the AI Skill generates a query against the configured data, executes it, and returns the result. AI Skills can be created via the Fabric UI or REST API and consumed programmatically through the Fabric REST API.

## Architecture

```text
┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│   USER       │───▶│  AI SKILL    │───▶│  AZURE       │───▶│  DATA SOURCE │
│              │    │  ENDPOINT    │    │  OPENAI      │    │              │
│ "What were   │    │              │    │              │    │ Lakehouse /  │
│  top sellers │    │ Instructions │    │ Generate SQL │    │ Warehouse    │
│  last month?"│    │ + Schema     │    │ from NL      │    │ tables       │
└──────────────┘    └──────────────┘    └──────────────┘    └──────────────┘
                           │                                       │
                           └───────── Execute query ───────────────┘
                                          │
                                    ┌─────▼─────┐
                                    │  RESULTS   │
                                    │ (formatted)│
                                    └────────────┘
```

## Creating an AI Skill via REST API

```python
import requests

BASE_URL = "https://api.fabric.microsoft.com/v1"

def create_ai_skill(workspace_id: str, headers: dict) -> dict:
    """Create a custom AI Skill with data source configuration."""
    payload = {
        "displayName": "SalesAnalyticsAssistant",
        "description": "Ask questions about sales data in natural language",
    }
    resp = requests.post(
        f"{BASE_URL}/workspaces/{workspace_id}/aiSkills",
        headers=headers,
        json=payload,
    )
    resp.raise_for_status()
    return resp.json()


def configure_ai_skill(
    workspace_id: str, skill_id: str, headers: dict,
    lakehouse_id: str, table_names: list[str],
) -> dict:
    """Configure data sources and instructions for an AI Skill."""
    payload = {
        "dataSources": [{
            "type": "Lakehouse",
            "itemId": lakehouse_id,
            "tables": table_names,
        }],
        "instructions": (
            "You are a sales analytics assistant. Answer questions about "
            "sales performance, product trends, and customer behavior. "
            "Always include the time period in your response. "
            "Format currency values with $ prefix and 2 decimal places."
        ),
        "exampleQuestions": [
            "What were the top 5 products by revenue last month?",
            "Show me the monthly sales trend for Q4 2025",
            "Which region had the highest customer growth?",
        ],
    }
    resp = requests.patch(
        f"{BASE_URL}/workspaces/{workspace_id}/aiSkills/{skill_id}",
        headers=headers,
        json=payload,
    )
    resp.raise_for_status()
    return resp.json()
```

## Querying an AI Skill Programmatically

```python
def query_ai_skill(
    workspace_id: str, skill_id: str, headers: dict,
    question: str,
) -> dict:
    """Send a natural language question to an AI Skill."""
    payload = {
        "userMessage": question,
    }
    resp = requests.post(
        f"{BASE_URL}/workspaces/{workspace_id}/aiSkills/{skill_id}/query",
        headers=headers,
        json=payload,
    )
    resp.raise_for_status()
    return resp.json()


# Usage
result = query_ai_skill(
    workspace_id="ws-abc-123",
    skill_id="skill-xyz-456",
    headers=auth_headers,
    question="What were total sales by category last quarter?",
)
print(f"Answer: {result.get('answer')}")
print(f"SQL Generated: {result.get('generatedQuery')}")
```

## Integration with Notebooks

```python
# Use AI Skills within a Fabric notebook for batch analysis
import json

questions = [
    "What is the total revenue for January 2026?",
    "Which product category has the highest return rate?",
    "List the top 10 customers by lifetime value",
]

results = []
for q in questions:
    response = query_ai_skill(workspace_id, skill_id, headers, q)
    results.append({
        "question": q,
        "answer": response.get("answer"),
        "query": response.get("generatedQuery"),
    })

# Convert to DataFrame for analysis
df_insights = spark.createDataFrame(results)
df_insights.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_lakehouse.ai_generated_insights")

display(df_insights)
```

## AI Skill Configuration Options

| Setting | Description | Example |
|---------|-------------|---------|
| `displayName` | Skill name visible in Fabric UI | "SalesAnalyticsAssistant" |
| `description` | Purpose description | "Ask about sales data" |
| `dataSources` | Tables the skill can query | Lakehouse or Warehouse tables |
| `instructions` | System prompt for the LLM | Domain rules, formatting |
| `exampleQuestions` | Sample questions shown to users | Up to 10 examples |

## Input/Output Mapping

| Component | Format | Notes |
|-----------|--------|-------|
| User input | Natural language string | Max ~4000 characters |
| Generated query | T-SQL or SparkSQL | Based on data source type |
| Answer | Formatted text | Includes data values |
| Metadata | JSON | Query, execution time, confidence |

## Common Mistakes

### Wrong

```text
Connecting an AI Skill to 50+ tables without instructions,
expecting it to understand the full data model.
```

### Correct

```text
1. Select 3-5 relevant tables for the skill's domain
2. Add clear column descriptions in the Lakehouse/Warehouse
3. Write specific instructions about business rules
4. Provide example questions covering common query patterns
5. Test and iterate on instruction quality
```

## See Also

- [AI Skills](ai-skills.md)
- [Copilot and ML](../concepts/copilot-ml.md)
- [Copilot Customization](../concepts/copilot-customization.md)
- [ML Model Lifecycle](../concepts/ml-model-lifecycle.md)
