# BGE-M3 Embedding Service

## Overview

The BGE-M3 embedding service provides an OpenAI-compatible `/v1/embeddings` API backed by the local `BAAI/bge-m3` model. It is used by network traffic analysis scripts for RAG document embedding and semantic search.

**Endpoint**: `http://192.168.200.1:7799/v1/embeddings`
**Model**: `BAAI/bge-m3` (1024-dimensional vectors)

## Starting the Service

```bash
python3 scripts/serve_bge_m3.py
```

The service loads on `0.0.0.0:7799` by default. Override with environment variables:

```bash
BGE_M3_PORT=8899 python3 scripts/serve_bge_m3.py
BGE_M3_MODEL_PATH=/path/to/bge-m3 python3 scripts/serve_bge_m3.py
```

### Running in Background

```bash
nohup python3 scripts/serve_bge_m3.py > logs/bge-m3.log 2>&1 &
```

### Health Check

```bash
curl http://192.168.200.1:7799/health
```

### Test

```bash
curl -s http://192.168.200.1:7799/v1/embeddings \
  -H "Content-Type: application/json" \
  -d '{"model":"BAAI/bge-m3","input":["hello world"]}'
```

## Configuration

### .env

```
NETWORK_TRAFFIC_EMBEDDING_BASE_URL=http://192.168.200.1:7799/v1
```

### config.yaml

```yaml
embedding:
  provider: openai-compatible
  model: BAAI/bge-m3
  api_key: unused
  base_url: $NETWORK_TRAFFIC_EMBEDDING_BASE_URL
```

### Sandbox Environment

The `sandbox.environment` section in `config.yaml` injects `NETWORK_TRAFFIC_EMBEDDING_BASE_URL` into sandbox containers so `rag_search.py` and `embed_rag_docs.py` can reach the service from inside the sandbox.

## Scripts That Use This Service

| Script | Purpose |
| --- | --- |
| `rag_search.py` | Semantic RAG search over indexed network traffic |
| `embed_rag_docs.py` | Embed RAG documents into Elasticsearch |
| `build_full_rag_index.py` | Full RAG index build (calls `embed_rag_docs.py`) |

All three scripts resolve the embedding base URL in this priority:

1. `config.yaml` `embedding.base_url` (via `$NETWORK_TRAFFIC_EMBEDDING_BASE_URL`)
2. Environment variable `NETWORK_TRAFFIC_EMBEDDING_BASE_URL`
3. Empty (falls back to remote API provider behavior)

## Requirements

- Python 3.12+
- `sentence-transformers`
- `fastapi`
- `uvicorn`
- Local model at `.models/bge-m3` (or path set via `BGE_M3_MODEL_PATH`)

## Stopping the Service

```bash
pkill -f "serve_bge_m3.py"
```
