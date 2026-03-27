# Vector Databases

> **Purpose**: ANN algorithms, distance metrics, metadata filtering, and 7-platform comparison (2026)
> **Confidence**: 0.90
> **MCP Validated**: 2026-03-26

## Overview

Vector databases are purpose-built for storing and searching high-dimensional embeddings using approximate nearest neighbor (ANN) algorithms. In 2026, seven serious production options exist: pgvector (Postgres extension), Qdrant (Rust, complex filtering), Pinecone (managed), Weaviate (hybrid search), Milvus (billion-scale), LanceDB (embedded), and Chroma (prototyping). The choice of index type, distance metric, and metadata filtering strategy determines retrieval latency, recall, and cost.

## The Concept

```text
ANN Index Algorithms

HNSW (Hierarchical Navigable Small World)
==========================================
Layer 3:  A ─────────────── D              (few nodes, long-range links)
Layer 2:  A ──── C ──── D ──── F          (more nodes, medium links)
Layer 1:  A ─ B ─ C ─ D ─ E ─ F ─ G      (all nodes, short links)

- Search: start at top layer, greedily descend
- Build time: O(N log N)  |  Query: O(log N)
- Memory: HIGH (in-memory graph)
- Best for: low-latency serving (<10ms), datasets < 50M vectors

IVF-PQ (Inverted File + Product Quantization)
=============================================
Step 1 - IVF: Partition vectors into K clusters (Voronoi cells)
Step 2 - PQ:  Compress each vector into sub-quantized codes

  [Full Vector 768d] --> [8 sub-vectors x 96d] --> [8 centroid IDs]
  Memory: 768 x 4B = 3KB  -->  8 x 1B = 8 bytes (375x compression)

- Best for: billion-scale, cost-sensitive, batch retrieval

DiskANN (Microsoft, used by LanceDB)
=====================================
- Stores graph on SSD, not RAM
- Near-HNSW recall at fraction of memory cost
- Best for: large datasets on budget hardware
```

## Quick Reference

| Distance Metric | Formula | Range | Use Case |
|-----------------|---------|-------|----------|
| Cosine similarity | 1 - (A . B) / (|A| |B|) | [0, 2] | Text embeddings (normalized) |
| L2 (Euclidean) | sqrt(sum((a-b)^2)) | [0, inf) | Image embeddings, spatial data |
| Dot product | A . B | (-inf, inf) | Pre-normalized, max inner product |

| Platform | Index Types | Hybrid Search | Multi-tenancy | Best For |
|----------|-------------|---------------|--------------|----------|
| **pgvector** | HNSW, IVF | BM25 via pg_search | Row-level security | Postgres teams, < 10M vectors |
| **Qdrant** | HNSW + quantization | Built-in sparse vectors | Native | Complex filtering, multi-tenancy |
| **Pinecone** | Proprietary | Sparse-dense | Namespaces | Zero-ops, fast prototyping |
| **Weaviate** | HNSW + PQ | Native BM25 + vector | Built-in | GraphQL API, hybrid search |
| **Milvus** | HNSW, IVF, DiskANN | Built-in | Partitions | Billion-scale, GPU acceleration |
| **LanceDB** | IVF-PQ, DiskANN | Built-in | Tables | Embedded/local, serverless |
| **Chroma** | HNSW | No | Collections | Prototyping (not production) |

## Common Mistakes

### Wrong
```python
# Using L2 distance with non-normalized embeddings for text search
index = faiss.IndexFlatL2(768)
index.add(embeddings)  # embeddings not normalized -- wrong for text!

# No metadata filtering -- scanning entire index for tenant-specific query
results = collection.search(query_vector, limit=10)
# Returns results from ALL tenants -- data leak!
```

### Correct
```python
# Normalize embeddings, use cosine (or IP after normalization)
import numpy as np
embeddings = embeddings / np.linalg.norm(embeddings, axis=1, keepdims=True)
index = faiss.IndexFlatIP(768)  # inner product = cosine after normalization
index.add(embeddings)

# Always scope search with metadata filters
results = collection.search(
    query_vector,
    limit=10,
    query_filter=Filter(
        must=[FieldCondition(key="tenant_id", match=MatchValue(value="acme"))]
    )
)
```

## Related

- [rag-pipelines](../concepts/rag-pipelines.md)
- [embedding-pipelines](../concepts/embedding-pipelines.md)
- [vector-db-operations](../patterns/vector-db-operations.md)
