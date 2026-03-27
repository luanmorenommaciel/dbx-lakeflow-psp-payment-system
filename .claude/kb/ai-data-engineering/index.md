# AI Data Engineering Knowledge Base

> **Purpose**: AI data engineering -- RAG pipelines, vector DBs, feature stores, LLMOps, embeddings, observability
> **MCP Validated**: 2026-03-26

## Quick Navigation

### Concepts (< 150 lines each)

| File | Purpose |
|------|---------|
| [concepts/rag-pipelines.md](concepts/rag-pipelines.md) | Chunking, indexing, hybrid search, reranking, context engineering |
| [concepts/vector-databases.md](concepts/vector-databases.md) | HNSW, IVF, distance metrics, 7 platforms compared |
| [concepts/feature-stores.md](concepts/feature-stores.md) | Online/offline stores, Feast, Tecton, Hopsworks |
| [concepts/embedding-pipelines.md](concepts/embedding-pipelines.md) | Model selection (Gemini, OpenAI, Cohere), Matryoshka, versioning |
| [concepts/llmops-patterns.md](concepts/llmops-patterns.md) | Prompt versioning, guardrails, eval, observability (Langfuse, Braintrust) |

### Patterns (< 200 lines each)

| File | Purpose |
|------|---------|
| [patterns/rag-pipeline-implementation.md](patterns/rag-pipeline-implementation.md) | End-to-end RAG with LangChain + evaluation |
| [patterns/vector-db-operations.md](patterns/vector-db-operations.md) | pgvector, Qdrant, Milvus, hybrid search |
| [patterns/feature-engineering.md](patterns/feature-engineering.md) | Feast definitions, materialization, feature services |
| [patterns/text-to-sql.md](patterns/text-to-sql.md) | Schema-aware prompting, validation, guardrails |
| [patterns/training-data-pipelines.md](patterns/training-data-pipelines.md) | DVC, labeling, bias detection, reproducibility |

---

## Quick Reference

- [quick-reference.md](quick-reference.md) - Fast lookup tables

## Agent Usage

| Agent | Primary Files | Use Case |
|-------|---------------|----------|
| ai-data-engineer | All files | RAG, embeddings, feature stores |
| data-quality-analyst | concepts/rag-pipelines.md | Training data quality |
