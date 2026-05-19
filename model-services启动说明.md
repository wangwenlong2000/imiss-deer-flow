# Embedding and SkillRouter Services

## Overview

These local services provide OpenAI-compatible embedding and reranking APIs for the repository's RAG and skill routing workflows.

- BGE-M3 for general RAG embeddings and semantic search
- SkillRouter Embedding for skill retrieval
- SkillRouter Reranker for cross-encoder reranking

The services are designed to run locally and can be started independently or together with the one-click dogfood script.

## BGE-M3 Embedding Service

The BGE-M3 embedding service provides an OpenAI-compatible `/v1/embeddings` API backed by the local `BAAI/bge-m3` model. It is used by network traffic analysis scripts for RAG document embedding and semantic search.

**Endpoint**: `http://192.168.200.1:7799/v1/embeddings`
**Model**: `BAAI/bge-m3` (1024-dimensional vectors)

### Starting the Service

```bash
python3 scripts/serve_bge_m3.py
```

The service loads on `0.0.0.0:7799` by default. Override with environment variables:

```bash
BGE_M3_PORT=8899 python3 scripts/serve_bge_m3.py
BGE_M3_MODEL_PATH=/path/to/bge-m3 python3 scripts/serve_bge_m3.py
```

#### Running in Background

```bash
nohup python3 scripts/serve_bge_m3.py > logs/bge-m3.log 2>&1 &
```

#### Health Check

```bash
curl http://192.168.200.1:7799/health
```

#### Test

```bash
curl -s http://192.168.200.1:7799/v1/embeddings \
  -H "Content-Type: application/json" \
  -d '{"model":"BAAI/bge-m3","input":["hello world"]}'
```

### Configuration

#### .env

```
EMBEDDING_BASE_URL=http://192.168.200.1:7799/v1
```

#### config.yaml

```yaml
embedding:
  provider: openai-compatible
  model: BAAI/bge-m3
  api_key: unused
  base_url: $EMBEDDING_BASE_URL
```

#### Sandbox Environment

The `sandbox.environment` section in `config.yaml` injects `EMBEDDING_BASE_URL` (and `EMBEDDING_API_KEY`) into sandbox containers so `rag_search.py` and `embed_rag_docs.py` can reach the service from inside the sandbox.

### Scripts That Use This Service

| Script | Purpose |
| --- | --- |
| `rag_search.py` | Semantic RAG search over indexed network traffic |
| `embed_rag_docs.py` | Embed RAG documents into Elasticsearch |
| `build_full_rag_index.py` | Full RAG index build (calls `embed_rag_docs.py`) |

All three scripts resolve the embedding base URL in this priority:

1. `config.yaml` `embedding.base_url` (via `$EMBEDDING_BASE_URL`)
2. Environment variable `EMBEDDING_BASE_URL`
3. Empty (falls back to remote API provider behavior)

### Requirements

- Python 3.12+
- `sentence-transformers`
- `fastapi`
- `uvicorn`
- Local model at `.models/bge-m3` (or path set via `BGE_M3_MODEL_PATH`)

### Stopping the Service

```bash
pkill -f "serve_bge_m3.py"
```

## SkillRouter Services

The SkillRouter services provide local HTTP APIs backed by the fine-tuned SkillRouter models:

- `pipizhao/SkillRouter-Embedding-0.6B` for query/document embeddings
- `pipizhao/SkillRouter-Reranker-0.6B` for cross-encoder reranking

They are used for skill retrieval and reranking in the agent skill router pipeline.

### Embedding Service

**Endpoint**: `http://192.168.200.1:7800/v1/embeddings`
**Model**: `pipizhao/SkillRouter-Embedding-0.6B`

#### Starting the Service

```bash
python3 scripts/serve_skillrouter_embedding.py
```

The service loads on `0.0.0.0:7800` by default. Override with environment variables:

```bash
SKILLROUTER_EMBEDDING_PORT=8898 python3 scripts/serve_skillrouter_embedding.py
SKILLROUTER_EMBEDDING_MODEL_PATH=.models/skillrouter-embedding-0.6b python3 scripts/serve_skillrouter_embedding.py
```

#### Running in Background

```bash
nohup python3 scripts/serve_skillrouter_embedding.py > logs/skillrouter-embedding.log 2>&1 &
```

#### Health Check

```bash
curl http://192.168.200.1:7800/health
```

#### Test

```bash
curl -s http://192.168.200.1:7800/v1/embeddings \
  -H "Content-Type: application/json" \
  -d '{"model":"pipizhao/SkillRouter-Embedding-0.6B","mode":"query","input":["Implement a feature branch workflow with PR checks."]}'
```

### Reranker Service

**Endpoint**: `http://192.168.200.1:7801/v1/rerank`
**Model**: `pipizhao/SkillRouter-Reranker-0.6B`

#### Starting the Service

```bash
python3 scripts/serve_skillrouter_reranker.py
```

The service loads on `0.0.0.0:7801` by default. Override with environment variables:

```bash
SKILLROUTER_RERANKER_PORT=8897 python3 scripts/serve_skillrouter_reranker.py
SKILLROUTER_RERANKER_MODEL_PATH=.models/skillrouter-reranker-0.6b python3 scripts/serve_skillrouter_reranker.py
```

#### Running in Background

```bash
nohup python3 scripts/serve_skillrouter_reranker.py > logs/skillrouter-reranker.log 2>&1 &
```

#### Health Check

```bash
curl http://192.168.200.1:7801/health
```

#### Test

```bash
curl -s http://192.168.200.1:7801/v1/rerank \
  -H "Content-Type: application/json" \
  -d '{
    "model":"pipizhao/SkillRouter-Reranker-0.6B",
    "query":"Implement a feature branch workflow with PR checks.",
    "documents":[
      {"name":"moai-foundation-git","desc":"Git workflow conventions","body":"# Git Workflow ..."},
      {"name":"concurrency-control","desc":"Mutex patterns for CI","body":"# Concurrency Control ..."}
    ],
    "top_n": 1
  }'
```

### Configuration

#### Environment Variables

```bash
SKILLROUTER_EMBEDDING_MODEL_PATH=.models/skillrouter-embedding-0.6b
SKILLROUTER_EMBEDDING_MODEL_ID=pipizhao/SkillRouter-Embedding-0.6B
SKILLROUTER_EMBEDDING_PORT=7800

SKILLROUTER_RERANKER_MODEL_PATH=.models/skillrouter-reranker-0.6b
SKILLROUTER_RERANKER_MODEL_ID=pipizhao/SkillRouter-Reranker-0.6B
SKILLROUTER_RERANKER_PORT=7801
```

#### Suggested Base URLs

```bash
SKILLROUTER_EMBEDDING_BASE_URL=http://192.168.200.1:7800/v1
SKILLROUTER_RERANKER_BASE_URL=http://192.168.200.1:7801/v1
```

### Requirements

- Python 3.12+
- `torch`
- `transformers`
- `fastapi`
- `uvicorn`
- Local models at:
  - `.models/skillrouter-embedding-0.6b`
  - `.models/skillrouter-reranker-0.6b`

### Stopping the Services

```bash
pkill -f "serve_skillrouter_embedding.py"
pkill -f "serve_skillrouter_reranker.py"
```

## One-Click Dogfood

启动并验证三个模型服务：

```bash
bash scripts/dogfood_skillrouter_models.sh
```

这个脚本会按需启动以下服务并做健康检查与样例请求：

- `scripts/serve_bge_m3.py`
- `scripts/serve_skillrouter_embedding.py`
- `scripts/serve_skillrouter_reranker.py`

## 通过 Makefile 管理（推荐）

仓库已在 `Makefile` 中增加便捷目标，可以用 `make` 调用脚本并统一管理服务：

- 启动三项模型服务（后台）：

```bash
make model-services-start
```

- 查看三项服务状态：

```bash
make model-services-status
```

- 停止三项模型服务：

```bash
make model-services-stop
```

- 启动并做烟测（health + 示例请求）：

```bash
make model-services-dogfood
```

这些目标只是对已有脚本的封装，不需要重新编译；只要系统有 Python 及运行时依赖（`torch`/`transformers`/`fastapi` 等）并把模型文件放到 `.models/` 下或通过环境变量指定路径，即可直接使用。