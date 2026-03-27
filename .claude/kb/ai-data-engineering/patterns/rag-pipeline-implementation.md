# RAG Pipeline Implementation

> **Purpose**: End-to-end RAG with LangChain -- loading, chunking, embedding, hybrid retrieval, generation, and RAGAS evaluation
> **MCP Validated**: 2026-03-26

## When to Use

- Building a question-answering system over private documents
- Need grounded LLM responses with source attribution
- Replacing keyword search with semantic retrieval
- Prototyping a RAG system before moving to production infrastructure

## Implementation

```python
"""End-to-end RAG pipeline with LangChain, Chroma, and RAGAS evaluation."""

from langchain_community.document_loaders import PyPDFLoader, TextLoader, DirectoryLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_openai import OpenAIEmbeddings, ChatOpenAI
from langchain_community.vectorstores import Chroma
from langchain.prompts import PromptTemplate
from langchain.chains import RetrievalQA

# --- 1. Document Loading ---
# Load PDFs from a directory (recursive)
pdf_loader = DirectoryLoader(
    "data/documents/",
    glob="**/*.pdf",
    loader_cls=PyPDFLoader,
    show_progress=True,
)
txt_loader = DirectoryLoader(
    "data/documents/",
    glob="**/*.txt",
    loader_cls=TextLoader,
)
documents = pdf_loader.load() + txt_loader.load()

# --- 2. Chunking ---
splitter = RecursiveCharacterTextSplitter(
    chunk_size=1000,
    chunk_overlap=200,
    length_function=len,
    separators=["\n\n", "\n", ". ", " ", ""],
)
chunks = splitter.split_documents(documents)

# --- 3. Embedding + Vector Store ---
embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
vectorstore = Chroma.from_documents(
    documents=chunks,
    embedding=embeddings,
    persist_directory="data/chroma_db",
    collection_name="rag_documents",
)

# --- 4. Retrieval ---
retriever = vectorstore.as_retriever(
    search_type="similarity",
    search_kwargs={"k": 4},
)

# --- 5. Prompt Template ---
RAG_PROMPT = PromptTemplate(
    input_variables=["context", "question"],
    template="""Use the following context to answer the question.
If the answer is not in the context, say "I don't know based on the provided documents."

Context:
{context}

Question: {question}

Answer:""",
)

# --- 6. Generation Chain ---
llm = ChatOpenAI(model="gpt-4o", temperature=0)
qa_chain = RetrievalQA.from_chain_type(
    llm=llm,
    chain_type="stuff",
    retriever=retriever,
    return_source_documents=True,
    chain_type_kwargs={"prompt": RAG_PROMPT},
)

# Query the pipeline
result = qa_chain.invoke({"query": "What are the data retention policies?"})
print(result["result"])
for doc in result["source_documents"]:
    print(f"  Source: {doc.metadata['source']} (page {doc.metadata.get('page', 'N/A')})")

# --- 7. Evaluation with RAGAS ---
from ragas import evaluate
from ragas.metrics import faithfulness, answer_relevancy, context_precision
from datasets import Dataset

eval_data = Dataset.from_dict({
    "question": ["What are the data retention policies?"],
    "answer": [result["result"]],
    "contexts": [[doc.page_content for doc in result["source_documents"]]],
    "ground_truth": ["Data is retained for 7 years per compliance requirements."],
})

scores = evaluate(eval_data, metrics=[faithfulness, answer_relevancy, context_precision])
print(f"Faithfulness: {scores['faithfulness']:.2f}")
print(f"Answer Relevancy: {scores['answer_relevancy']:.2f}")
print(f"Context Precision: {scores['context_precision']:.2f}")
```

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `chunk_size` | 1000 | Max characters per chunk |
| `chunk_overlap` | 200 | Overlap between adjacent chunks |
| `search_type` | `similarity` | `similarity`, `mmr`, or `similarity_score_threshold` |
| `k` | 4 | Number of documents to retrieve |
| `temperature` | 0 | LLM sampling temperature (0 = deterministic) |
| `persist_directory` | required | Path for Chroma on-disk storage |
| `collection_name` | `langchain` | Chroma collection identifier |

## Example Usage

```python
# Quick single-file RAG
from langchain_community.document_loaders import PyPDFLoader

loader = PyPDFLoader("report.pdf")
docs = loader.load()
chunks = splitter.split_documents(docs)
vectorstore.add_documents(chunks)

# MMR retrieval for diversity (reduces redundant results)
retriever = vectorstore.as_retriever(
    search_type="mmr",
    search_kwargs={"k": 4, "fetch_k": 20, "lambda_mult": 0.7},
)
```

## See Also

- [RAG Pipelines Concept](../concepts/rag-pipelines.md)
- [Embedding Pipelines](../concepts/embedding-pipelines.md)
- [Vector DB Operations](vector-db-operations.md)
