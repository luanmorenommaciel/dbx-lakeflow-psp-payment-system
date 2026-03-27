# Text-to-SQL

> **Purpose**: Schema-aware text-to-SQL pipeline with validation, error correction, guardrails, and LLM-as-judge quality scoring
> **MCP Validated**: 2026-03-26

## When to Use

- Enabling natural language querying over structured databases
- Building self-service analytics for non-technical users
- Creating a conversational BI layer on top of a data warehouse
- Need SQL generation with safety guardrails (read-only, row limits)

## Implementation

```python
"""Text-to-SQL pipeline with schema context, validation loop, and guardrails."""

import sqlparse
from sqlalchemy import text
from openai import OpenAI

client = OpenAI()

# --- 1. Schema-Aware Prompting ---
def build_schema_context(engine, tables: list[str]) -> str:
    """Extract DDL and sample rows for context injection."""
    context_parts = []
    with engine.connect() as conn:
        for table in tables:
            ddl = conn.execute(text(f"SHOW CREATE TABLE {table}")).scalar()
            sample = conn.execute(text(f"SELECT * FROM {table} LIMIT 3")).fetchall()
            columns = conn.execute(text(f"SELECT * FROM {table} LIMIT 1")).keys()
            sample_str = "\n".join([str(dict(zip(columns, row))) for row in sample])
            context_parts.append(f"-- {table}\n{ddl}\n\n-- Sample rows:\n{sample_str}")
    return "\n\n".join(context_parts)

SYSTEM_PROMPT = """You are a SQL expert. Generate valid SQL based on the user's question.
Rules:
- Use only SELECT statements (no INSERT, UPDATE, DELETE, DROP, ALTER)
- Always include a LIMIT clause (max 1000 rows)
- Use table and column names exactly as shown in the schema
- Return ONLY the SQL query, no explanation

Schema:
{schema}"""

# --- 2. SQL Generation ---
def generate_sql(question: str, schema_context: str) -> str:
    """Generate SQL from natural language question."""
    response = client.chat.completions.create(
        model="gpt-4o",
        temperature=0,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT.format(schema=schema_context)},
            {"role": "user", "content": question},
        ],
    )
    return response.choices[0].message.content.strip().strip("```sql").strip("```").strip()

# --- 3. SQL Validation + Guardrails ---
FORBIDDEN_KEYWORDS = {"INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE", "GRANT", "EXEC"}

def validate_sql(sql: str) -> tuple[bool, str]:
    """Parse, validate syntax, and enforce read-only guardrails."""
    try:
        parsed = sqlparse.parse(sql)
        if not parsed:
            return False, "Empty or unparsable SQL"
        statement_type = parsed[0].get_type()
        if statement_type and statement_type.upper() != "SELECT":
            return False, f"Only SELECT allowed, got {statement_type}"
        for token in parsed[0].flatten():
            if token.value.upper() in FORBIDDEN_KEYWORDS:
                return False, f"Forbidden keyword: {token.value}"
        return True, "Valid"
    except Exception as e:
        return False, f"Parse error: {e}"

# --- 4. Execute with Timeout and Row Limit ---
def execute_sql(engine, sql: str, timeout_seconds: int = 30, max_rows: int = 1000):
    """Execute validated SQL with timeout and row cap."""
    with engine.connect() as conn:
        conn.execute(text(f"SET statement_timeout = '{timeout_seconds}s'"))
        result = conn.execute(text(sql))
        rows = result.fetchmany(max_rows)
        columns = list(result.keys())
        return {"columns": columns, "rows": rows, "truncated": len(rows) == max_rows}

# --- 5. Error Correction Chain ---
def text_to_sql_with_retry(question: str, schema_context: str, engine, max_retries: int = 3):
    """Full pipeline: generate -> validate -> execute, with error correction."""
    sql = generate_sql(question, schema_context)
    for attempt in range(max_retries):
        is_valid, msg = validate_sql(sql)
        if not is_valid:
            sql = _fix_sql(question, sql, f"Validation failed: {msg}", schema_context)
            continue
        try:
            return execute_sql(engine, sql)
        except Exception as e:
            sql = _fix_sql(question, sql, f"Execution error: {e}", schema_context)
    raise RuntimeError(f"Failed after {max_retries} retries")

def _fix_sql(question: str, bad_sql: str, error: str, schema: str) -> str:
    """Feed error back to LLM for correction."""
    response = client.chat.completions.create(
        model="gpt-4o",
        temperature=0,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT.format(schema=schema)},
            {"role": "user", "content": question},
            {"role": "assistant", "content": bad_sql},
            {"role": "user", "content": f"That SQL failed: {error}\nPlease fix it."},
        ],
    )
    return response.choices[0].message.content.strip().strip("```sql").strip("```").strip()

# --- 6. LLM-as-Judge Quality Scoring ---
def score_sql_quality(question: str, sql: str, result_summary: str) -> dict:
    """Use LLM to score the generated SQL on correctness and relevance."""
    import json
    response = client.chat.completions.create(
        model="gpt-4o",
        temperature=0,
        messages=[{"role": "user", "content": f"""Score this text-to-SQL result (1-5 each):
Question: {question}
SQL: {sql}
Result summary: {result_summary}

Score on: correctness, relevance, efficiency. Return JSON."""}],
    )
    return json.loads(response.choices[0].message.content)
```

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `timeout_seconds` | 30 | Max query execution time |
| `max_rows` | 1000 | Row cap per query result |
| `max_retries` | 3 | Error correction attempts |
| `temperature` | 0 | LLM sampling (0 = deterministic) |
| `FORBIDDEN_KEYWORDS` | DML + DDL | Blocked SQL operations |

## Example Usage

```python
# Multi-turn conversation
schema_ctx = build_schema_context(engine, ["orders", "customers", "products"])

# Turn 1
result = text_to_sql_with_retry("How many orders last month?", schema_ctx, engine)
print(result)

# Turn 2 (follow-up)
result = text_to_sql_with_retry(
    "Break that down by product category", schema_ctx, engine
)
```

## See Also

- [LLMOps Patterns](../concepts/llmops-patterns.md)
- [RAG Pipeline Implementation](rag-pipeline-implementation.md)
- [Feature Engineering](feature-engineering.md)
