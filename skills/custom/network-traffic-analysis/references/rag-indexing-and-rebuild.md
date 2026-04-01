# RAG Indexing And Rebuild

This reference explains the artifact chain for RAG indexing and when rebuild is required.

## Artifact chain

### 1. `flow.csv`

Produced by:

- `prepare_pcap.py`

Purpose:

- structured analysis input
- source input for RAG document construction

### 2. `rag_docs.jsonl`

Produced by:

- `build_rag_docs.py`

Purpose:

- retrieval-ready text documents with metadata
- no embeddings yet

### 3. `rag_embeddings.jsonl`

Produced by:

- `embed_rag_docs.py`

Purpose:

- local artifact containing RAG documents plus embedding vectors

### 4. Elasticsearch indexed documents

Produced by:

- `index_rag_docs.py`

Purpose:

- online retrieval store containing document text, metadata, and vectors

## Manifest files

- `rag_manifest.json`
  - records document build details
- `embedding_manifest.json`
  - records embedding build details
- `index_manifest.json`
  - records successful Elasticsearch indexing for one dataset

## Reuse rules

If local artifacts already exist:

- `flow.csv` can be reused for analysis and RAG doc generation
- `rag_docs.jsonl` can be reused for embedding
- `rag_embeddings.jsonl` can be reused for Elasticsearch indexing

## Rebuild triggers

Rebuild `flow.csv` when:

- preprocessing logic changes
- sessionization changes
- time semantics change
- canonical fields emitted by preprocessing change

Rebuild `rag_docs.jsonl` when:

- summary templates change
- time bucket semantics change
- short-connection logic changes
- endpoint/port/protocol/anomaly document logic changes

Rebuild embeddings and Elasticsearch index when:

- embedding model changes
- embedding provider changes
- RAG document text changes
- Elasticsearch index was deleted

## Recommended rebuild order

1. delete or replace outdated Elasticsearch index
2. regenerate processed artifacts when preprocessing changed
3. regenerate `rag_docs.jsonl`
4. regenerate `rag_embeddings.jsonl`
5. reindex into Elasticsearch

## Interpretation rule

- Local `rag` artifacts are build and audit artifacts.
- Elasticsearch is the online retrieval store used at search time.
