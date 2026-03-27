# AI Data Engineering Quick Reference

> Fast lookup tables. For code examples, see linked files.
> **MCP Validated**: 2026-03-26

## Vector DB Comparison (2026)

| Feature | pgvector | Qdrant | Pinecone | Weaviate | Milvus | LanceDB | Chroma |
|---------|----------|--------|----------|----------|--------|---------|--------|
| Type | Extension | Standalone | Managed | Standalone | Standalone | Embedded | Embedded |
| Language | SQL | Rust | API-only | Go | Go/C++ | Rust | Python |
| Hybrid search | BM25 (pg_search) | Built-in sparse | Sparse-dense | Native BM25 | Built-in | Built-in | No |
| Metadata filter | SQL WHERE | Payload filter | Filter API | GraphQL | Expression | SQL | Where dict |
| Max scale | ~10M vectors | Billions | Billions | Billions | Billions | Billions | Millions |
| Multi-tenancy | Row-level security | Built-in | Namespaces | Built-in | Partitions | Tables | Collections |
| Best for | Postgres teams | Complex RAG | Zero-ops | Hybrid search | Billion-scale | Embedded/local | Prototyping |
| Cost | Free (self-host) | Free (OSS) | Pay-per-use | Free (OSS) | Free (OSS) | Free (OSS) | Free (OSS) |

## Feature Store Comparison (2026)

| Feature | Feast | Tecton | Hopsworks | Databricks FS | SageMaker FS |
|---------|-------|--------|-----------|--------------|-------------|
| License | OSS (Apache 2.0) | Commercial | OSS + Enterprise | Databricks-only | AWS-only |
| Online serving | Redis, DynamoDB | Managed (<5ms) | RonDB (<5ms) | Databricks | DynamoDB |
| Offline store | Any (Parquet, BQ) | Managed | Hudi/Delta | Delta Lake | S3/Parquet |
| Streaming features | Basic (push) | Full | Full | Full | Full |
| Vector features | No (external) | No | Yes (native) | Yes | No |
| Best for | OSS, flexible | Enterprise | Feature platform | Databricks shops | AWS shops |

## Embedding Model Selection (March 2026)

| Model | Provider | Dimensions | Context | Matryoshka | MTEB Score | Cost/1M tokens |
|-------|----------|-----------|---------|------------|-----------|---------------|
| Gemini Embedding 001 | Google | 3072 | 8K | Yes | 68.3 | ~$0.004/1K chars |
| text-embedding-3-large | OpenAI | 3072 (truncatable) | 8191 | Yes | 64.6 | $0.13 |
| embed-v4 | Cohere | 1024 | 512 | No | 65.2 | $0.10 |
| Voyage 3 Large | Voyage AI | 1024 | 32K | No | 67.2 | $0.18 |
| text-embedding-3-small | OpenAI | 1536 (truncatable) | 8191 | Yes | 62.3 | $0.02 |
| Nomic embed-text-v1.5 | Nomic (OSS) | 768 | 8192 | Yes | 60.1 | Free (self-host) |
| BGE-M3 | BAAI (OSS) | 1024 | 8192 | No | 63.0 | Free (self-host) |
| Gemini Embedding 2 Preview | Google | 3072 | 8K | Yes | TBD | ~$0.004/1K chars |

## RAG Pipeline Checklist

| Step | Component | Key Decision |
|------|-----------|-------------|
| 1. Load | Document loader | PDF, HTML, Markdown parser |
| 2. Chunk | Splitter | Recursive (default), semantic, or late chunking |
| 3. Embed | Embedding model | Gemini/OpenAI (API) vs BGE/Nomic (self-host) |
| 4. Store | Vector DB | pgvector (Postgres) vs Qdrant (standalone) vs managed |
| 5. Retrieve | Search strategy | Hybrid (dense + BM25) with RRF fusion |
| 6. Rerank | Cross-encoder | Cohere Rerank v3.5, cross-encoder, ColBERT |
| 7. Generate | LLM | Claude Sonnet 4.6, GPT-4o, or Gemini 2.5 |
| 8. Evaluate | Metrics | RAGAS (faithfulness, relevancy, precision) |

## LLMOps Observability Tools (2026)

| Tool | Type | Strength | Cost |
|------|------|----------|------|
| Langfuse | OSS | Tracing, prompt management, evals, best free tier | Free / self-host |
| Braintrust | Commercial | Best eval pipeline, experiments, datasets | $249/mo+ |
| LangSmith | Commercial | LangChain native, zero-config tracing | Paid |
| Arize Phoenix | OSS | Visual traces, embeddings, LLM evals | Free / self-host |
| Helicone | Commercial | Proxy integration, no code changes | Freemium |
| Datadog LLM | Commercial | Unified infra + LLM monitoring | Enterprise |

## Common Pitfalls

| Don't | Do |
|-------|-----|
| Single embedding model for all content | Match model to content type and language |
| No chunk overlap | 10-20% overlap for context preservation |
| Skip metadata in vector store | Store source, page, section, timestamp with vectors |
| Stale embeddings | Re-embed on source updates, track embedding version |
| No eval pipeline | Measure faithfulness, relevancy, precision with RAGAS |
| Skip observability | Instrument with Langfuse/Braintrust from day one |
| Use Chroma in production | Use pgvector, Qdrant, or Pinecone for production scale |
