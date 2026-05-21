---
name: code-rag-elasticsearch
description: Use this skill when building, indexing, querying, or debugging Elasticsearch-backed code chunk retrieval for DeerFlow source code. It supports local open-source sentence-transformers embeddings, code chunk metadata, dense vector search, keyword search, and application-level RRF fusion.
---

# Code RAG Elasticsearch

This skill indexes source-code chunks into Elasticsearch and retrieves them with a hybrid strategy:

```text
source files
  -> filtering
  -> AST chunking
  -> metadata
  -> local open-source embeddings
  -> Elasticsearch dense_vector + keyword fields
  -> dense search + keyword search
  -> application-level RRF fusion
```

Use scripts under:

```text
elasticsearch/code-rag/scripts
```

Default index:

```text
code_chunks
```

Default local embedding model:

```text
BAAI/bge-m3
```

## Index

```bash
python scripts/code_indexer.py \
  --root-path D:/PythonProject/deerflow \
  --repo deerflow \
  --index code_chunks \
  --embedding-model BAAI/bge-m3 \
  --batch-size 32 \
  --es-url http://localhost:9200
```

Use `--force` to re-index all changed files, and `--recreate-index` to delete and recreate the mapping.

## Retrieve

```bash
python scripts/code_retrieve_topk.py \
  --query "where is code_search_tool implemented" \
  --index code_chunks \
  --repo deerflow \
  --language python \
  --k 8 \
  --es-url http://localhost:9200
```

Optional filters:

```text
--language python
--kind function
--tag retrieval
--path-glob "backend/packages/harness/*"
```

## Debug

```bash
python scripts/code_list_indices.py --es-url http://localhost:9200
python scripts/code_get_mapping.py --index code_chunks --es-url http://localhost:9200
python scripts/code_query_dsl.py --index code_chunks --dsl '{"query":{"match_all":{}}}'
```

