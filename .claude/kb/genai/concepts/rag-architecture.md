# RAG Architecture

> **Purpose**: Retrieval-Augmented Generation pipeline design -- variants, chunking, hybrid search, context engineering
> **Confidence**: 0.95
> **MCP Validated**: 2026-03-26

## Overview

Retrieval-Augmented Generation (RAG) grounds LLM responses in external knowledge by retrieving relevant documents before generation. In 2026, RAG has evolved from a simple "embed + retrieve + generate" pattern into an umbrella term covering fundamentally different architectures: Agentic RAG (agent decides when and what to retrieve), GraphRAG (knowledge graph augmentation), Corrective RAG (self-verification), and Multimodal RAG (text + image + tables). Context engineering -- treating the LLM's input as a structured system rather than a simple prompt -- has replaced basic prompt stuffing.

## The Pattern

```python
from dataclasses import dataclass
from typing import Optional

@dataclass
class Chunk:
    text: str
    metadata: dict
    embedding: Optional[list[float]] = None
    score: Optional[float] = None

@dataclass
class RAGConfig:
    chunk_size: int = 512
    chunk_overlap: int = 50
    top_k: int = 20           # increased: retrieve more, rerank down
    rerank_top_n: int = 5
    similarity_threshold: float = 0.7
    embedding_model: str = "text-embedding-3-large"
    generation_model: str = "claude-sonnet-4-6"

class RAGPipeline:
    def __init__(self, config: RAGConfig):
        self.config = config

    def chunk(self, document: str) -> list[Chunk]:
        """Split document into overlapping chunks."""
        chunks = []
        for i in range(0, len(document), self.config.chunk_size - self.config.chunk_overlap):
            text = document[i:i + self.config.chunk_size]
            chunks.append(Chunk(text=text, metadata={"offset": i}))
        return chunks

    def retrieve(self, query: str, chunks: list[Chunk]) -> list[Chunk]:
        """Hybrid search -> rerank -> threshold filter."""
        # Step 1: Hybrid search (vector + BM25 keyword)
        candidates = self._hybrid_search(query, chunks, self.config.top_k)
        # Step 2: Cross-encoder rerank for precision
        reranked = self._rerank(query, candidates, self.config.rerank_top_n)
        # Step 3: Filter by threshold
        return [c for c in reranked if c.score >= self.config.similarity_threshold]

    def generate(self, query: str, context: list[Chunk]) -> str:
        """Generate answer with context engineering."""
        context_text = "\n---\n".join(
            f"[Source {i+1}] {c.text}" for i, c in enumerate(context))
        prompt = f"""Answer based ONLY on the sources below. Cite using [Source N].
If sources lack the answer, say so.

Sources:
{context_text}

Question: {query}"""
        return self._call_llm(prompt)
```

## RAG Variants (2026)

| Variant | Mechanism | Best For |
|---------|-----------|----------|
| Naive RAG | Single retrieval + generation | Simple Q&A, prototypes |
| Advanced RAG | Hybrid search + reranking + query transform | Production systems |
| Agentic RAG | Agent decides when/what/how to retrieve | Multi-hop reasoning |
| GraphRAG | Knowledge graph + entity extraction + community summaries | Relational reasoning |
| Corrective RAG | Verify relevance, re-retrieve if needed | Critical applications |
| Self-RAG | Self-reflective retrieval decisions | High-accuracy needs |
| Modular RAG | Swappable LEGO components | Flexible pipelines |
| Multimodal RAG | Text + image + table retrieval | Document understanding |

## Chunking Strategies

| Strategy | Pros | Cons |
|----------|------|------|
| Fixed-size | Simple, predictable | Breaks semantic units |
| Semantic | Preserves meaning | Slower, variable size |
| Recursive | Tries multiple separators | Good balance (default choice) |
| Document-aware | Respects structure (headers, tables) | Requires parsing |
| Late chunking | Embed full doc, then chunk (preserves context) | Higher compute |

## Context Engineering (2026)

```text
Traditional prompt stuffing:
  "Here are some documents: {chunks}. Answer: {query}"

Context engineering (structured input):
  [System] Role + constraints + output format
  [Context] Retrieved chunks with source IDs + relevance scores
  [Instructions] Citation rules + fallback behavior
  [Query] User question with any disambiguation
  [Examples] Few-shot examples of ideal answers (optional)

Key insight: treat the LLM's context window as a structured data pipeline,
not a text concatenation. Order, format, and selection of context matters.
```

## Common Mistakes

### Wrong
```python
# Vector search alone has low precision for production
results = vector_search(query, top_k=3)  # may miss relevant chunks
# No reranking, no hybrid search, no query transformation
```

### Correct
```python
# Hybrid search -> rerank -> threshold filter
candidates = hybrid_search(query, top_k=20)     # vector + BM25
reranked = cross_encoder_rerank(query, candidates, top_n=5)
filtered = [c for c in reranked if c.score >= 0.7]
```

## Related

- [RAG Pipeline Pattern](../patterns/rag-pipeline.md)
- [Evaluation Framework](../patterns/evaluation-framework.md)
- [Guardrails](../concepts/guardrails.md)
