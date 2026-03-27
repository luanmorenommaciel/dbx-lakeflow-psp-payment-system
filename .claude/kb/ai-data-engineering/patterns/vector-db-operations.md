# Vector DB Operations

> **Purpose**: Vector database operations across pgvector, Qdrant, Milvus, and hybrid search with index tuning
> **MCP Validated**: 2026-03-26

## When to Use

- Storing and querying high-dimensional embeddings at scale
- Need ACID guarantees alongside vector search (pgvector)
- Building filtered vector search with complex metadata predicates
- Combining semantic search with keyword search (hybrid)
- Tuning recall/latency tradeoffs for production workloads
- Multi-tenant vector isolation (Qdrant, Milvus)

## Implementation

```sql
-- =============================================
-- pgvector: PostgreSQL Vector Extension
-- =============================================

-- Enable the extension
CREATE EXTENSION IF NOT EXISTS vector;

-- Create table with vector column (1536 = OpenAI text-embedding-3-small)
CREATE TABLE documents (
    id          BIGSERIAL PRIMARY KEY,
    content     TEXT NOT NULL,
    metadata    JSONB DEFAULT '{}',
    embedding   vector(1536),
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- HNSW index for fast approximate nearest neighbor search
-- m=16: connections per layer, ef_construction=200: build-time search width
CREATE INDEX idx_documents_embedding ON documents
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 200);

-- Cosine distance query (<=>) -- returns most similar documents
SELECT id, content, metadata,
       1 - (embedding <=> $1::vector) AS similarity
FROM documents
WHERE metadata->>'department' = 'engineering'
ORDER BY embedding <=> $1::vector
LIMIT 10;

-- Set search-time precision (higher = better recall, slower)
SET hnsw.ef_search = 100;
```

```python
"""Qdrant: Managed vector database with rich filtering."""

from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance, VectorParams, PointStruct,
    Filter, FieldCondition, MatchValue, Range,
)

client = QdrantClient(url="http://localhost:6333")

# Create collection with HNSW configuration
client.create_collection(
    collection_name="documents",
    vectors_config=VectorParams(
        size=1536,
        distance=Distance.COSINE,
        hnsw_config={"m": 16, "ef_construct": 200},
    ),
)

# Upsert points with payload (metadata)
client.upsert(
    collection_name="documents",
    points=[
        PointStruct(
            id=1,
            vector=embedding_vector,
            payload={"department": "engineering", "year": 2026, "source": "wiki"},
        ),
    ],
)

# Filtered vector search
results = client.search(
    collection_name="documents",
    query_vector=query_embedding,
    query_filter=Filter(
        must=[
            FieldCondition(key="department", match=MatchValue(value="engineering")),
            FieldCondition(key="year", range=Range(gte=2025)),
        ]
    ),
    limit=10,
)
```

```python
"""Hybrid Search: BM25 sparse + dense vectors with Reciprocal Rank Fusion."""

from rank_bm25 import BM25Okapi
import numpy as np

def hybrid_search(
    query: str,
    query_embedding: list[float],
    corpus: list[dict],
    vectorstore,
    k: int = 10,
    alpha: float = 0.5,
) -> list[dict]:
    """Combine BM25 keyword scores with dense vector similarity."""
    # Sparse retrieval (BM25)
    tokenized_corpus = [doc["content"].lower().split() for doc in corpus]
    bm25 = BM25Okapi(tokenized_corpus)
    bm25_scores = bm25.get_scores(query.lower().split())

    # Dense retrieval (vector similarity)
    dense_results = vectorstore.similarity_search_with_score(query, k=k * 2)

    # Reciprocal Rank Fusion (RRF)
    rrf_k = 60  # standard RRF constant
    bm25_ranks = np.argsort(-bm25_scores)
    dense_ids = [doc.metadata["id"] for doc, _ in dense_results]

    fused_scores = {}
    for rank, idx in enumerate(bm25_ranks[:k * 2]):
        doc_id = corpus[idx]["id"]
        fused_scores[doc_id] = fused_scores.get(doc_id, 0) + (1 - alpha) / (rrf_k + rank + 1)
    for rank, doc_id in enumerate(dense_ids):
        fused_scores[doc_id] = fused_scores.get(doc_id, 0) + alpha / (rrf_k + rank + 1)

    # Sort by fused score and return top-k
    ranked = sorted(fused_scores.items(), key=lambda x: x[1], reverse=True)[:k]
    return [{"id": doc_id, "score": score} for doc_id, score in ranked]
```

## Configuration

| Engine | Index Type | Key Params | Tradeoff |
|--------|-----------|------------|----------|
| pgvector | HNSW | `m=16`, `ef_construction=200` | Higher m = better recall, more memory |
| pgvector | IVF-Flat | `lists=100` | More lists = faster search, lower recall |
| Qdrant | HNSW | `m=16`, `ef_construct=200` | Same as pgvector HNSW |
| Any | -- | `ef_search=100` | Runtime: higher = better recall, slower |

| Distance Metric | pgvector Op | Use Case |
|----------------|-------------|----------|
| Cosine | `<=>` | Normalized embeddings (most common) |
| L2 (Euclidean) | `<->` | Absolute distance comparisons |
| Inner Product | `<#>` | Pre-normalized vectors, dot product similarity |

## Example Usage

```python
# pgvector with SQLAlchemy
from sqlalchemy import create_engine, text

engine = create_engine("postgresql://user:pass@localhost/vectordb")
with engine.connect() as conn:
    results = conn.execute(
        text("SELECT id, content, 1 - (embedding <=> :q) AS sim "
             "FROM documents ORDER BY embedding <=> :q LIMIT 5"),
        {"q": str(query_embedding)},
    )
    for row in results:
        print(f"[{row.sim:.3f}] {row.content[:80]}")
```

## See Also

- [Vector Databases Concept](../concepts/vector-databases.md)
- [Embedding Pipelines](../concepts/embedding-pipelines.md)
- [RAG Pipeline Implementation](rag-pipeline-implementation.md)
