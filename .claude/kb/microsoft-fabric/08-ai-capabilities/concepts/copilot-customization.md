> **MCP Validated:** 2026-02-17

# Copilot Customization

> **Purpose**: Fabric Copilot capabilities, natural language to code/DAX generation, data insights, and organizational configuration
> **Confidence**: 0.95

## Overview

Fabric Copilot is an AI assistant powered by Azure OpenAI that operates across all Fabric workloads. It generates PySpark code in notebooks, writes T-SQL queries in warehouses, creates DAX measures in Power BI, and provides data insights from datasets. Copilot reads workspace metadata (schemas, table definitions, relationships) to produce context-aware suggestions. Administrators can control Copilot availability at the tenant, capacity, and workspace levels through the Fabric Admin Portal.

## Copilot Capabilities by Workload

| Workload | Capability | Input | Output |
|----------|-----------|-------|--------|
| Notebooks | Code generation | Natural language prompt | PySpark/Python code |
| Notebooks | Code explanation | Selected code block | Plain-text explanation |
| Notebooks | Fix errors | Error traceback | Corrected code |
| Warehouse | Query generation | Natural language prompt | T-SQL query |
| Warehouse | Query explanation | Selected T-SQL | Plain-text explanation |
| Power BI | DAX generation | Measure description | DAX formula |
| Power BI | Report creation | Data description | Visual suggestions |
| Power BI | Data insights | Dataset selection | Summary narratives |
| Data Factory | Pipeline design | Task description | Pipeline activity layout |

## DAX Generation Example

```text
User prompt in Power BI Copilot:
  "Create a measure that calculates year-over-year revenue growth percentage"

Copilot output:
```

```dax
Revenue YoY Growth % =
VAR CurrentYearRevenue =
    CALCULATE(
        SUM(Sales[Revenue]),
        DATESYTD(Calendar[Date])
    )
VAR PriorYearRevenue =
    CALCULATE(
        SUM(Sales[Revenue]),
        DATESYTD(DATEADD(Calendar[Date], -1, YEAR))
    )
RETURN
    IF(
        PriorYearRevenue <> 0,
        DIVIDE(CurrentYearRevenue - PriorYearRevenue, PriorYearRevenue),
        BLANK()
    )
```

## Organizational Settings

### Admin Portal Configuration

| Setting | Level | Default | Description |
|---------|-------|---------|-------------|
| Copilot enabled | Tenant | On | Master toggle for all workloads |
| Copilot for notebooks | Capacity | On | Enable in Data Engineering |
| Copilot for warehouse | Capacity | On | Enable in Data Warehouse |
| Copilot for Power BI | Capacity | On | Enable in Power BI |
| Data sent to Azure OpenAI | Tenant | On | Required for Copilot |
| Bing data grounding | Tenant | Off | Augment with web data |
| Feedback collection | Tenant | On | Thumbs up/down on responses |

### Capacity Requirements

| Copilot Feature | Minimum SKU | Notes |
|-----------------|-------------|-------|
| Notebook Copilot | F64 or P1 | Requires paid capacity |
| Warehouse Copilot | F64 or P1 | Same as notebook |
| Power BI Copilot | F64 or P1 or PPU | PPU for per-user |
| Data Factory Copilot | F64 or P1 | Preview availability |

### Programmatic Settings Check

```python
import requests

BASE_URL = "https://api.fabric.microsoft.com/v1"

def check_copilot_settings(headers: dict) -> dict:
    """Check tenant-level Copilot configuration."""
    resp = requests.get(
        f"{BASE_URL}/admin/tenantsettings",
        headers=headers,
    )
    resp.raise_for_status()
    settings = resp.json()
    copilot_settings = {
        s["settingName"]: s["enabled"]
        for s in settings.get("tenantSettings", [])
        if "copilot" in s.get("settingName", "").lower()
    }
    return copilot_settings
```

## Best Practices

| Practice | Rationale |
|----------|-----------|
| Provide schema context in prompts | Copilot uses metadata but explicit hints improve accuracy |
| Review generated DAX carefully | Complex time intelligence can have edge cases |
| Use Copilot for boilerplate, refine manually | Best for scaffolding, not production-ready output |
| Enable at capacity level first, then expand | Controlled rollout for governance |
| Disable Bing grounding for sensitive data | Prevents external data leakage |

## Common Mistakes

### Wrong

```text
Deploying Copilot-generated DAX measures directly to production
without validation against known results.
```

### Correct

```text
1. Generate DAX measure with Copilot
2. Test against sample data with known results
3. Validate edge cases (nulls, zeros, missing periods)
4. Peer review before publishing to production semantic model
```

## Related

- [Copilot and ML](copilot-ml.md)
- [ML Model Lifecycle](ml-model-lifecycle.md)
- [AI Skills](../patterns/ai-skills.md)
- [AI Skill Integration](../patterns/ai-skill-integration.md)
