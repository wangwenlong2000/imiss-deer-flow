# DeerFlow 视频级向量接入方案

## 目标

DeerFlow 将视频文件路径转换为 StreetModel Server 可访问的路径。为了避免长视频超过模型处理能力，默认先在 DeerFlow/MCP 侧按时间抽帧并重编码成一个短 proxy MP4，再把 proxy 视频路径传给 StreetModel Server 生成视频级 embedding。

```text
1 个原视频 -> 1 个 frame-proxy 视频 -> 1 条 2048 维向量 -> 1 条 ES 文档
```

服务地址：

```text
http://219.245.185.245:3130
```

使用模型：

```text
Qwen3-VL-Embedding-2B
```

推荐 ES 字段：

```text
video_vector-Qwen3-VL-Embedding-2B_urban_governance
```

## 架构

```text
DeerFlow
  |
  | ffmpeg frame-proxy
  | 1 fps, max 300 sampled frames
  v
Proxy MP4 in shared video directory
  |
  | POST /embed
  | items[].type = video
  | items[].uri  = 模型服务器可访问的 proxy 视频绝对路径
  v
StreetModel Server on GPU
  |
  v
Qwen3-VL-Embedding-2B
  |
  v
2048 维视频向量
```

关键点：

```text
uri 必须是 StreetModel Server / GPU 计算节点能访问的路径。
不能直接传 DeerFlow 服务器自己的本地路径，除非该路径在 GPU 集群上也同样可访问。
```

## 视频路径方案

推荐准备一个共享视频目录，例如：

```text
/nfsdat2/home/xhuangslm/shared_videos/
```

DeerFlow 侧可以通过同步、挂载或拷贝的方式，把待入库视频放到该目录下。

示例路径映射：

```text
DeerFlow 原始路径:
/data/deerflow/videos/camera01/0001.mp4

StreetModel Server 可访问路径:
/nfsdat2/home/xhuangslm/shared_videos/camera01/0001.mp4
```

DeerFlow 侧需要把本地路径转换成模型服务器路径：

```python
def to_streetmodel_video_uri(local_video_path: str) -> str:
    return local_video_path.replace(
        "/data/deerflow/videos",
        "/nfsdat2/home/xhuangslm/shared_videos",
        1,
    )
```

## StreetModel Server 验证

在任意能访问该服务的机器上验证：

```bash
curl http://219.245.185.245:3130/health
curl http://219.245.185.245:3130/models
```

期望 `/models` 中：

```json
{
  "name": "Qwen3-VL-Embedding-2B",
  "kind": "embedding",
  "exists": true
}
```

如果模型还没加载，`loaded` 为 `false` 是正常的。第一次 `/embed` 请求会触发模型加载。

## DeerFlow 配置建议

```yaml
streetmodel_embedding:
  enabled: true
  base_url: http://219.245.185.245:3130
  model_name: Qwen3-VL-Embedding-2B
  mode: video
  vector_field: video_vector-Qwen3-VL-Embedding-2B_urban_governance
  dimensions: 2048
  instruction: Represent this surveillance video for urban scene retrieval.
  batch_size: 1
  timeout_seconds: 600

  video:
    item_type: video
    deerflow_path_prefix: /data/deerflow/videos
    streetmodel_path_prefix: /nfsdat2/home/xhuangslm/shared_videos

  video_preprocess:
    enabled: true
    mode: frame_proxy
    sample_fps: 1.0
    max_sampled_frames: 300
    proxy_output_fps: 4.0
    max_width: 768
    output_subdir: embedding_proxies
```

说明：

```text
batch_size 建议先用 1，稳定后再考虑增大。
timeout_seconds 建议至少 600，避免首批模型冷启动超时。
长视频默认使用 frame-proxy：约 1 秒 1 帧，最多 300 帧，重新编码为短 MP4 后再传给 embedding 模型。短视频如果确认可直接处理，可用 `--video-preprocess none` 关闭。
```

## DeerFlow 请求格式

## DeerFlow 脚本链路

### 单视频生成向量并写入 ES

如果视频已经在 StreetModel Server 可访问的共享目录中：

```bash
python skills/custom/video-embedding-index/scripts/run.py \
  --embedding-provider streetmodel \
  --video-id camera01_0001 \
  --video-uri /nfsdat2/home/xhuangslm/shared_videos/camera01/0001.mp4 \
  --target-index huangxiao-video-library-vector-v1 \
  --config config.yaml \
  --output outputs/video-embedding-result.json
```

如果视频来自 DeerFlow 上传目录，需要先复制到共享挂载：

```bash
python skills/custom/video-embedding-index/scripts/run.py \
  --embedding-provider streetmodel \
  --video-id camera01_0001 \
  --video-uri /mnt/user-data/uploads/0001.mp4 \
  --copy-video-to-shared \
  --target-index huangxiao-video-library-vector-v1 \
  --config config.yaml \
  --output outputs/video-embedding-result.json
```

注意：`--copy-video-to-shared` 要求 DeerFlow 侧的 `deerflow_path_prefix` 是真实共享挂载。例如 DeerFlow 写入 `/data/deerflow/videos/...` 后，StreetModel Server 必须能从 `/nfsdat2/home/xhuangslm/shared_videos/...` 读到同一个文件。

### 自然语言检索视频

检索脚本可以直接调用 StreetModel 把文本 query 转成同模型 2048 维向量，然后查询 ES 视频向量字段：

```bash
python skills/custom/video-search/scripts/run.py \
  --query "夜晚路口有很多车辆经过" \
  --index huangxiao-video-library-vector-v1 \
  --embedding-provider streetmodel \
  --vector-query-mode semantic \
  --top-k 10 \
  --config config.yaml \
  --output outputs/video-search-result.json
```

检查目标索引中是否已经有视频向量：

```bash
curl -u citybrain-street:123456 \
  -H "Content-Type: application/json" \
  http://localhost:3128/huangxiao-video-library-vector-v1/_count \
  -d '{"query":{"exists":{"field":"video_vector-Qwen3-VL-Embedding-2B_urban_governance"}}}'
```

### curl 示例

```bash
curl -X POST http://219.245.185.245:3130/embed \
  -H "Content-Type: application/json" \
  -d '{
    "model_name": "Qwen3-VL-Embedding-2B",
    "instruction": "Represent this surveillance video for urban scene retrieval.",
    "items": [
      {
        "type": "video",
        "uri": "/nfsdat2/home/xhuangslm/shared_videos/camera01/0001.mp4"
      }
    ],
    "batch_size": 1
  }'
```

期望响应：

```json
{
  "model_name": "Qwen3-VL-Embedding-2B",
  "embeddings": [[0.01, -0.02]],
  "shape": [1, 2048]
}
```

### Python 示例

```python
import requests

BASE_URL = "http://219.245.185.245:3130"

def video_to_vector(video_uri: str) -> list[float]:
    payload = {
        "model_name": "Qwen3-VL-Embedding-2B",
        "instruction": "Represent this surveillance video for urban scene retrieval.",
        "items": [
            {
                "type": "video",
                "uri": video_uri,
            }
        ],
        "batch_size": 1,
    }

    resp = requests.post(
        f"{BASE_URL}/embed",
        json=payload,
        timeout=600,
    )
    resp.raise_for_status()

    data = resp.json()
    assert data["shape"] == [1, 2048], data
    return data["embeddings"][0]


video_uri = "/nfsdat2/home/xhuangslm/shared_videos/camera01/0001.mp4"
vector = video_to_vector(video_uri)
```

## ES 写入建议

每条视频文档写入一个视频级向量字段：

```json
{
  "video_id": "camera01_0001",
  "video_path": "/nfsdat2/home/xhuangslm/shared_videos/camera01/0001.mp4",
  "video_vector-Qwen3-VL-Embedding-2B_urban_governance": [0.01, -0.02]
}
```

向量字段维度：

```text
2048
```

如果 ES 使用 `dense_vector`，mapping 需要和实际维度一致。

## 文本检索视频

可以使用同一个模型把用户文本 query 转为 2048 维向量，再检索视频向量字段。

文本 query embedding 请求：

```bash
curl -X POST http://219.245.185.245:3130/embed \
  -H "Content-Type: application/json" \
  -d '{
    "model_name": "Qwen3-VL-Embedding-2B",
    "instruction": "Represent this surveillance/street-view query for urban scene retrieval.",
    "items": [
      {
        "type": "text",
        "content": "夜晚路口有很多车辆经过"
      }
    ],
    "batch_size": 1
  }'
```

返回：

```text
shape: [1, 2048]
```

然后用该文本向量检索：

```text
video_vector-Qwen3-VL-Embedding-2B_urban_governance
```

## 性能参考

当前在 `GPU49` V100 节点上实测：

```text
首批冷启动：约 52 秒，主要耗时为加载模型权重
热加载后 2 秒测试视频：约 0.11 秒/条
热加载后 5 秒测试视频：约 0.14 秒/条
```

建议 DeerFlow 正式批量入库前，先发一个小视频做 warm-up。

## 验收步骤

1. 准备 5 个视频文件到模型服务器可访问的共享目录。
2. DeerFlow 将本地视频路径映射为 StreetModel Server 可访问路径。
3. 使用 `items[].type=video` 调用 `/embed`。
4. 确认每个请求返回 `shape: [1, 2048]`。
5. 将向量写入 ES 字段 `video_vector-Qwen3-VL-Embedding-2B_urban_governance`。
6. 使用文本 query 生成 2048 维向量，检索该视频向量字段。

## 常见问题

### 1. 返回文件不存在

检查 DeerFlow 传入的 `uri` 是否能在 StreetModel Server 所在机器访问：

```bash
ls -lh /nfsdat2/home/xhuangslm/shared_videos/camera01/0001.mp4
```

### 2. 第一次请求很慢

正常。第一次 `/embed` 会加载模型，之后请求会明显变快。

### 3. DeerFlow 能 ping 通但 curl 不通

`ping` 只代表网络 ICMP 可达，不代表 HTTP 端口可达。需要确认：

```bash
curl http://219.245.185.245:3130/health
```

### 4. 服务重启后无法访问

确认 SLURM job 和登录节点端口转发是否还在：

```bash
squeue -u "$USER"
ss -ltnp | awk '$4 ~ /:3130$/ {print}'
```

如转发不存在，需要按 README 重新执行 `scripts/expose_public_port.sh`。
