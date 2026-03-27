---
name: ai-pipeline
description: RAG/embedding pipeline scaffolding — delegates to ai-data-engineer agent
---

# AI Pipeline Command

> Scaffold RAG pipelines, embedding workflows, feature stores, and text-to-SQL

## Usage

```bash
/ai-pipeline <description-or-file>
```

## Examples

```bash
/ai-pipeline "RAG pipeline for internal docs with pgvector"
/ai-pipeline "Embedding pipeline from S3 PDFs to Pinecone"
/ai-pipeline "Feature store setup with Feast for ML models"
/ai-pipeline "Text-to-SQL agent for analytics queries"
```

---

## What This Command Does

1. Invokes the **ai-data-engineer** agent
2. Analyzes your AI/ML data requirements
3. Loads KB patterns from `ai-data-engineering` and `streaming` domains
4. Generates:
   - RAG pipeline architecture and code
   - Embedding pipeline with chunking strategies
   - Vector database setup and indexing
   - Feature store definitions
   - Text-to-SQL prompt templates

## Agent Delegation

| Agent | Role |
|-------|------|
| `ai-data-engineer` | Primary — RAG, embeddings, vector DBs, features |
| `streaming-engineer` | Escalation — real-time embedding pipelines |
| `data-quality-analyst` | Escalation — embedding quality metrics |

## KB Domains Used

- `ai-data-engineering` — RAG pipelines, vector databases, feature stores, LLMOps
- `streaming` — real-time embedding ingestion
- `data-quality` — embedding quality, drift detection

## Output

The agent generates pipeline code, configuration, and architecture documentation for your AI data workflow.
