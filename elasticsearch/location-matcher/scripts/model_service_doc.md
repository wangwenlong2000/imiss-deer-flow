``` bash
ssh -N -L 172.17.0.1:3130:localhost:3130 akzhaoslm@219.245.185.245 &
ssh -N -L localhost:3130:localhost:3130 akzhaoslm@219.245.185.245 &
```


## API 接口说明

> **服务地址**：由运维人员告知当前服务的 `HOST` 和 `PORT`，以下示例统一用 `http://localhost:3130` 代替。  
> **所有请求**：`Content-Type: application/json`，请求体为 JSON。  
> **文件路径与内联数据**：`video/document` 仍需使用服务器本地路径；`image` 除本地路径外，还支持通过 `items.data` 直接传 base64（含 data URL）。服务端不会主动下载远程 URL。  
> **交互式文档**：服务启动后访问 `http://<HOST>:<PORT>/docs` 可在浏览器中查看并调试所有接口。

---

### GET /health — 健康检查

确认服务是否正常运行，以及当前已加载的模型。

```bash
curl http://localhost:3130/health
curl http://172.17.0.1:3130/health

```

**响应示例：**

```json
{
  "status": "ok",
  "loaded_embed_models": ["Qwen3-VL-Embedding-2B"],
  "loaded_rerank_models": []
}
```

---

### GET /models — 模型列表

列出服务支持的所有模型及其加载状态。

```bash
curl http://localhost:3130/models
```

**响应示例：**

```json
{
  "models": [
    {
      "name": "Qwen3-VL-Embedding-2B",
      "kind": "embedding",
      "path": "/path/to/models/Qwen3-VL-Embedding-2B",
      "exists": true,
      "loaded": true
    },
    {
      "name": "Qwen3-VL-Reranker-2B",
      "kind": "reranker",
      "path": "/path/to/models/Qwen3-VL-Reranker-2B",
      "exists": true,
      "loaded": false
    },
    {
      "name": "ImAge4VPR",
      "kind": "embedding",
      "path": "/path/to/models/ImAge4VPR",
      "exists": true,
      "loaded": false
    }
  ]
}
```

---

### POST /embed — 多模态 Embedding

对一批输入提取 embedding 向量，返回 L2 归一化的 float32 向量列表。

**首次请求会触发模型加载（约需数十秒），后续请求直接复用已加载模型。**

#### 请求体字段

| 字段 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `model_name` | string | ✅ | — | 模型名称，见下方支持列表 |
| `items` | object[] | 否 | — | 新的统一多模态输入列表，优先级高于 `image_paths` |
| `image_paths` | string[] | 否 | — | 旧版兼容字段，仅图片路径列表 |
| `instruction` | string | ❌ | null | 任务描述前缀；`ImAge4VPR` 不支持，传入会被忽略 |
| `batch_size` | int | ❌ | 16 | 内部推理批大小（1~128） |

**支持的模型名称：**
- `Qwen3-VL-Embedding-2B` — 输出维度 2048，支持 `items` 多模态输入和 `instruction`
- `ImAge4VPR` — 输出维度 6144，不支持 `instruction`，支持 `image_paths` 与 `items`（仅 `type=image` + `uri`、`type=image_base64` + `data`），输入图片会缩放到 322×322

#### `items` 元素格式

```json
{
  "type": "image | image_base64 | text | video | document",
  "uri": "/path/to/file",
  "data": "base64或data URL",
  "encoding": "base64",
  "media_type": "image/jpeg",
  "content": "纯文本内容"
}
```

字段说明：

| 字段 | 适用类型 | 说明 |
|------|----------|------|
| `type` | 全部 | `image` / `image_base64` / `text` / `video` / `document` |
| `uri` | `image` / `video` / `document` | 服务器本地路径 |
| `data` | `image` / `image_base64` | 内联图片数据（base64）；可为纯 base64 或 data URL |
| `encoding` | `image` / `image_base64` | `data` 的编码方式，当前仅支持 `base64` |
| `media_type` | `image` / `image_base64` | 可选，图片 MIME 类型（如 `image/jpeg`） |
| `content` | `text` | 直接传入的文本内容 |

当前实现限制：
- `text`：使用 `content`，不读取 `uri`
- `image`：优先使用 `data`（base64/data URL）；若无 `data`，回退读取本地 `uri`
- `image_base64`：必须提供 `data`（base64/data URL）
- `video`：传递本地视频路径给 Qwen3 底层多模态模型处理
- `document`：当前仅按 UTF-8 文本文件读取；`pdf/docx` 等二进制文档尚未做解析
- `ImAge4VPR` 的 `items` 仅支持 `image` 和 `image_base64` 两种 `type`；不支持 `text/video/document`
- 至少提供 `items` 或 `image_paths` 其中一种

#### 请求示例

多模态 `items`：

```bash
curl -X POST http://localhost:3130/embed \
  -H "Content-Type: application/json" \
  -d '{
    "model_name": "Qwen3-VL-Embedding-2B",
    "instruction": "Represent the user's input.",
    "items": [
      {"type": "text", "content": "Tokyo street at night"},
      {"type": "image", "uri": "/data/images/street_001.jpg"},
      {"type": "video", "uri": "/data/videos/street_001.mp4"},
      {"type": "document", "uri": "/data/docs/street_notes.txt"}
    ],
    "batch_size": 8
  }'
```

内联 base64 图片（跨服务器传输推荐）：

```bash
curl -X POST http://localhost:3130/embed \
  -H "Content-Type: application/json" \
  -d '{
    "model_name": "ImAge4VPR",
    "items": [
      {
        "type": "image_base64",
        "data": "data:image/jpeg;base64,/9j/4AAQSkZJRgABAQ...",
        "encoding": "base64",
        "media_type": "image/jpeg"
      }
    ],
    "batch_size": 8
  }'
```

```bash
{
  "type": "image_base64",
  "data": "data:image/png;base64,iVBORw0KGgoAAA...",
  "encoding": "base64",
  "media_type": "image/png"
}
```

旧版图片路径接口：

```bash
curl -X POST http://localhost:3130/embed \
  -H "Content-Type: application/json" \
  -d '{
    "model_name": "Qwen3-VL-Embedding-2B",
    "instruction": "Represent this street-view image for place recognition:",
    "image_paths": [
      "/data/images/street_001.jpg",
      "/data/images/street_002.jpg"
    ],
    "batch_size": 16
  }'
```

#### 响应体字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `model_name` | string | 实际使用的模型名称 |
| `embeddings` | float[][] | 向量列表，形状 `[N, D]`，已 L2 归一化 |
| `shape` | int[] | `[N, D]`，N 为图片数，D 为向量维度 |

**响应示例：**

```json
{
  "model_name": "Qwen3-VL-Embedding-2B",
  "embeddings": [
    [0.0312, -0.0214, 0.0089, "...共 2048 维..."],
    [0.0156, 0.0423, -0.0301, "...共 2048 维..."]
  ],
  "shape": [2, 2048]
}
```

#### Python 调用示例

```python
import requests
import numpy as np

resp = requests.post("http://localhost:3130/embed", json={
    "model_name": "Qwen3-VL-Embedding-2B",
    "instruction": "Represent the user's input.",
    "items": [
        {"type": "text", "content": "Tokyo street"},
        {"type": "image", "uri": "/data/images/street_001.jpg"},
    ],
})
resp.raise_for_status()
data = resp.json()
embeddings = np.array(data["embeddings"], dtype=np.float32)  # shape: (N, D)
```

---

### POST /rerank — 多模态相关性打分

计算每个 query 与所有 candidate 之间的相关性分数（Sigmoid 归一化到 0~1），值越高表示越相关。

#### 请求体字段

| 字段 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `model_name` | string | ❌ | `Qwen3-VL-Reranker-2B` | 目前仅支持 `Qwen3-VL-Reranker-2B` |
| `query_items` | object[] | 否 | — | 新的 query 多模态输入列表，优先级高于 `query_images` |
| `candidate_items` | object[] | 否 | — | 新的 candidate 多模态输入列表，优先级高于 `candidate_images` |
| `query_images` | string[] | 否 | — | 旧版兼容字段，仅 query 图片路径列表 |
| `candidate_images` | string[] | 否 | — | 旧版兼容字段，仅 candidate 图片路径列表 |
| `instruction` | string | ❌ | null | 任务描述前缀 |

说明：
- `query_items` 和 `candidate_items` 的元素格式与 `/embed` 的 `items` 一致
- 当前实现要求同时提供一组 query 和一组 candidate
- 若同时提供新旧字段，优先使用 `query_items` / `candidate_items`
- `document` 当前仅按 UTF-8 文本文件读取；`pdf/docx` 等尚未解析
- 对图片类型同样支持 `data`（base64/data URL）直传

#### 请求示例

多模态 `items`：

```bash
curl -X POST http://localhost:3130/rerank \
  -H "Content-Type: application/json" \
  -d '{
    "model_name": "Qwen3-VL-Reranker-2B",
    "instruction": "Retrieve relevant images for the query.",
    "query_items": [
      {"type": "text", "content": "night street with neon signs"}
    ],
    "candidate_items": [
      {"type": "image", "uri": "/data/images/cand_001.jpg"},
      {"type": "image", "uri": "/data/images/cand_002.jpg"},
      {"type": "document", "uri": "/data/docs/candidate_desc.txt"}
    ]
  }'
```

旧版图片路径接口：

```bash
curl -X POST http://localhost:3130/rerank \
  -H "Content-Type: application/json" \
  -d '{
    "model_name": "Qwen3-VL-Reranker-2B",
    "instruction": "Find the most visually similar place:",
    "query_images": ["/data/images/query.jpg"],
    "candidate_images": [
      "/data/images/cand_001.jpg",
      "/data/images/cand_002.jpg",
      "/data/images/cand_003.jpg"
    ]
  }'
```

#### 响应体字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `scores` | float[][] | 形状 `[Q, C]`，`scores[i][j]` 为第 i 张 query 与第 j 张 candidate 的相关性分数（0~1） |

**响应示例：**

```json
{
  "scores": [[0.923, 0.341, 0.756]]
}
```

#### Python 调用示例

```python
import requests

resp = requests.post("http://localhost:3130/rerank", json={
    "model_name": "Qwen3-VL-Reranker-2B",
  "instruction": "Retrieve relevant images for the query.",
  "query_items": [
    {"type": "text", "content": "Tokyo street with crosswalk"}
  ],
  "candidate_items": [
    {"type": "image", "uri": "/data/images/cand_001.jpg"},
    {"type": "image", "uri": "/data/images/cand_002.jpg"}
  ],
})
resp.raise_for_status()
scores = resp.json()["scores"]  # [[0.923, 0.341]]
best_match_idx = scores[0].index(max(scores[0]))
```

---

### POST /batch — JSONL 批量推理

读取服务器本地的 JSONL 文件，批量提取所有图片的 embedding，结果写入服务器本地输出文件。适合大规模离线处理。

**注意：该接口为同步阻塞请求，处理完成后才返回响应。数据量大时请适当设置客户端超时时间。**

#### 请求体字段

| 字段 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `jsonl_path` | string | ✅ | — | 输入 JSONL 文件的服务器本地绝对路径 |
| `model_name` | string | ✅ | — | 模型名称 |
| `instruction` | string | ❌ | null | 任务描述前缀 |
| `output_dir` | string | ❌ | 服务默认输出目录 | 输出文件存放目录（服务器本地） |
| `batch_size` | int | ❌ | 16 | 内部推理批大小（1~128） |
| `image_key` | string | ❌ | `"image_path"` | JSONL 每条记录中存放图片路径的字段名 |
| `id_key` | string | ❌ | `"id"` | JSONL 每条记录中存放样本 ID 的字段名 |

#### 输入 JSONL 格式

每行一条 JSON 记录，必须包含 `image_key` 和 `id_key` 对应的字段：

```jsonl
{"id": "img_001", "image_path": "/data/images/001.jpg"}
{"id": "img_002", "image_path": "/data/images/002.jpg"}
{"id": "img_003", "image_path": "/data/images/003.jpg"}
```

#### 请求示例

```bash
curl -X POST http://localhost:3130/batch \
  -H "Content-Type: application/json" \
  -d '{
    "jsonl_path": "/data/input/dataset.jsonl",
    "model_name": "Qwen3-VL-Embedding-2B",
    "instruction": "Represent this street-view image for place recognition:",
    "output_dir": "/data/output",
    "batch_size": 16,
    "image_key": "image_path",
    "id_key": "id"
  }'
```

#### 响应体字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `output_path` | string | 输出 JSONL 文件的服务器本地绝对路径 |
| `processed_count` | int | 成功处理的图片数量 |

**响应示例：**

```json
{
  "output_path": "/data/output/Qwen3-VL-Embedding-2B_place_recognition.jsonl",
  "processed_count": 1500
}
```

#### 输出 JSONL 格式

第 1 行为元信息，后续每行对应一条记录的 embedding：

```jsonl
{"meta": {"model_name": "Qwen3-VL-Embedding-2B", "instruction": "Represent this street-view image for place recognition:"}}
{"id": "img_001", "embedding_vector": [0.0312, -0.0214, "...共 2048 维..."]}
{"id": "img_002", "embedding_vector": [0.0156, 0.0423, "...共 2048 维..."]}
```

#### Python 读取输出示例

```python
import json
import numpy as np

results = {}
with open("/data/output/Qwen3-VL-Embedding-2B_place_recognition.jsonl") as f:
    meta = json.loads(f.readline())  # 第一行是 meta
    for line in f:
        rec = json.loads(line)
        results[rec["id"]] = np.array(rec["embedding_vector"], dtype=np.float32)
```

---

### 错误响应格式

所有接口在出错时返回标准 HTTP 错误码和 JSON 响应体：

| 状态码 | 含义 |
|--------|------|
| `400` | 请求参数错误（文件不存在、模型名称无效等） |
| `404` | 指定模型不受支持 |
| `500` | 服务器推理内部错误 |

```json
{ "detail": "错误描述信息" }
```
