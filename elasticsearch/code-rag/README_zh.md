# Elasticsearch 代码片段 RAG 使用说明

这个目录提供一套独立的代码片段检索 RAG：扫描 DeerFlow 源码，按文件/函数/类切成 chunk，写入 Elasticsearch，并用本地开源 embedding 模型做向量检索，再和关键词检索做 hybrid 融合。

默认索引名：

```text
code_chunks
```

默认本地 embedding 模型：

```text
BAAI/bge-m3
```

如果只是快速验证流程，可以先用更轻量的模型：

```text
sentence-transformers/all-MiniLM-L6-v2
```

## 目录结构

```text
elasticsearch/code-rag/
  config.example.yaml
  pyproject.toml
  README_zh.md
  SKILL.md
  scripts/
    code_chunker.py
    code_embedding.py
    code_indexer.py
    code_retrieve_topk.py
    code_get_mapping.py
    code_list_indices.py
    code_query_dsl.py
    smoke_test_code_rag.py
    es_common.py
    test/
```

## 安装依赖

建议在 DeerFlow 的 Python 环境里安装：

```bash
cd D:/PythonProject/deerflow/elasticsearch/code-rag
pip install -e .
```

`pyproject.toml` 中声明的核心依赖是：

```text
elasticsearch>=8,<9
pyyaml>=6
sentence-transformers>=3
```

`sentence-transformers` 通常会安装或复用 `torch`。如果你的环境缺少 PyTorch，可以按本机 CPU/GPU 情况单独安装 PyTorch，然后再执行 `pip install -e .`。

模型会在第一次运行索引或检索时下载。若网络无法访问 Hugging Face，可以提前把模型下载到本地目录，并在配置里填写 `embedding.model_cache_dir`，或者把 `embedding.model` 改成已缓存的本地模型路径。

## 配置文件

先复制示例配置：

```bash
cd D:/PythonProject/deerflow/elasticsearch/code-rag
copy config.example.yaml config.yaml
```

配置含义如下：

```yaml
elasticsearch:
  url: "http://localhost:9200"
  username: null
  password: null
  api_key: null

source:
  root_path: "D:/PythonProject/deerflow"
  repo: "deerflow"
  max_files_scanned: 20000

index:
  name: "code_chunks"
  recreate_index: false
  force: false
  batch_size: 32

embedding:
  model: "BAAI/bge-m3"
  device: null
  model_cache_dir: null

retrieval:
  k: 8
  window_size: 50
  language: null
  kind: null
  tags: []
  path_glob: null
  rank_constant: 60
```

字段说明：

- `elasticsearch.url`：Elasticsearch 地址，例如 `http://localhost:9200`。
- `elasticsearch.username/password`：启用 Basic Auth 时填写。
- `elasticsearch.api_key`：使用 API Key 时填写；如果同时填写 API Key 和用户名密码，脚本优先使用 API Key。
- `source.root_path`：要索引的代码仓库根目录。
- `source.repo`：写入 ES 的仓库标识，检索时可用它过滤。
- `source.max_files_scanned`：最多扫描文件数，避免误扫过大的目录。
- `index.name`：ES 索引名。
- `index.recreate_index`：是否删除并重建索引。首次调试可设为 `true`，正式增量索引建议设为 `false`。
- `index.force`：是否忽略 `file_hash` 缓存，强制重建所有 chunk。
- `index.batch_size`：embedding 和 bulk 写入批大小。
- `embedding.model`：本地开源 embedding 模型名或本地模型路径。
- `embedding.device`：可填 `cpu`、`cuda`，或保持 `null` 让模型库自动选择。
- `embedding.model_cache_dir`：模型缓存目录，可为空。
- `retrieval.k`：最终返回 top K。
- `retrieval.window_size`：dense 和 keyword 各自召回的候选窗口。
- `retrieval.language/kind/tags/path_glob`：默认检索过滤条件。
- `retrieval.rank_constant`：RRF 融合参数，通常保持默认即可。

这些配置也可以用环境变量补充：

```text
ES_URL
ES_USERNAME
ES_PASSWORD
ES_API_KEY
```

命令行参数会覆盖配置文件里的同名配置。

## 连接 Elasticsearch

确认 ES 可访问后，先查看索引列表：

```bash
cd D:/PythonProject/deerflow/elasticsearch/code-rag
python scripts/code_list_indices.py --es-url http://localhost:9200
```

如果 ES 开启了用户名密码：

```bash
python scripts/code_list_indices.py ^
  --es-url http://localhost:9200 ^
  --es-username elastic ^
  --es-password your_password
```

也可以用环境变量：

```bash
set ES_URL=http://localhost:9200
set ES_USERNAME=elastic
set ES_PASSWORD=your_password
python scripts/code_list_indices.py
```

## 建立索引

使用配置文件建索引：

```bash
cd D:/PythonProject/deerflow/elasticsearch/code-rag
python scripts/code_indexer.py --config config.yaml
```

首次调试建议把 `config.yaml` 中的 `index.recreate_index` 设为 `true`，这样会删除并重建索引。稳定后改回 `false`，脚本会使用 `file_hash` 跳过未变化文件，只更新发生变化的代码文件。

也可以临时覆盖配置：

```bash
python scripts/code_indexer.py ^
  --config config.yaml ^
  --embedding-model sentence-transformers/all-MiniLM-L6-v2 ^
  --batch-size 16 ^
  --force
```

索引脚本会写入这些主要字段：

```text
path, language, symbol, kind, start_line, end_line,
imports, tags, content_hash, file_hash, code, embedding_text, metadata
```

## 执行检索

使用配置文件检索：

```bash
python scripts/code_retrieve_topk.py ^
  --config config.yaml ^
  --query "code_search_tool 在哪里实现"
```

带过滤条件检索：

```bash
python scripts/code_retrieve_topk.py ^
  --config config.yaml ^
  --query "LangChain Tool 代码检索入口" ^
  --language python ^
  --kind function ^
  --path-glob "backend/packages/harness/*" ^
  --k 5
```

输出结果包含：

```text
rank
rrf_score
dense_score
keyword_score
path
symbol
kind
language
start_line / end_line
tags
imports
code
metadata
```

当前检索策略是：

```text
1. 用本地 embedding 模型生成 query vector
2. Elasticsearch dense_vector kNN 召回候选
3. Elasticsearch multi_match 关键词召回候选
4. 应用层用 RRF 融合 dense 和 keyword 排名
5. 返回 top K 代码片段
```

## 测试脚本

单元测试不需要真实 Elasticsearch，也不需要加载 embedding 模型：

```bash
cd D:/PythonProject/deerflow/elasticsearch/code-rag/scripts
python -m unittest discover -s test -p "test_*.py" -v
```

本地 smoke test 只验证切片、metadata 和 embedding 输入文本构造，也不需要 ES 或模型：

```bash
cd D:/PythonProject/deerflow/elasticsearch/code-rag/scripts
python smoke_test_code_rag.py --mode local --config ../config.yaml
```

连接 smoke test 会访问 Elasticsearch：

```bash
python smoke_test_code_rag.py --mode connection --config ../config.yaml
```

端到端 smoke test 会创建临时小仓库、建临时索引、加载本地 embedding 模型并执行检索。首次运行会下载模型：

```bash
python smoke_test_code_rag.py --mode end-to-end --config ../config.yaml
```

为了让端到端测试更快，可以先把 `config.yaml` 改成：

```yaml
embedding:
  model: "sentence-transformers/all-MiniLM-L6-v2"
  device: "cpu"
  model_cache_dir: null
```

## 调试命令

查看 mapping：

```bash
python scripts/code_get_mapping.py --index code_chunks --es-url http://localhost:9200
```

执行原始 DSL：

```bash
python scripts/code_query_dsl.py ^
  --index code_chunks ^
  --dsl "{\"query\":{\"match_all\":{}},\"size\":3}" ^
  --es-url http://localhost:9200
```

## 常见问题

如果提示找不到 `elasticsearch`、`yaml` 或 `sentence_transformers`，说明依赖没有安装到当前 Python 环境，重新执行：

```bash
cd D:/PythonProject/deerflow/elasticsearch/code-rag
pip install -e .
```

如果模型下载失败，可以改用本地模型路径：

```yaml
embedding:
  model: "D:/models/bge-m3"
  device: "cpu"
  model_cache_dir: "D:/models/cache"
```

如果检索时报 dense vector 字段不存在，通常是索引时使用的 `embedding.model` 和检索时不同。解决方式是保持索引与检索配置一致，或用新的模型重新建索引。
