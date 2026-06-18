# StreetModel MCP Docker 迁移配置说明

这份文档只讲一种场景：把当前这套 DeerFlow 复制/迁移到服务器上的另一个目录后，在 **Docker 部署模式** 下重新配置 `streetmodel-video-embedding` MCP 服务。

重点先说清楚：**不需要在宿主机手动配置 `backend/.venv/bin/python`**。Docker 镜像构建时会在容器内执行 `cd backend && uv sync`，生成容器里的 `/app/backend/.venv`。MCP wrapper 也是在 LangGraph 容器内执行，用的是容器内 Python 环境。

## 1. 当前真实链路

当前 MCP 不是直接把一组照片传给 StreetModel。真实链路是：

```text
原视频
  -> ffmpeg 抽帧/降采样
  -> 重新编码成短代理视频 proxy.mp4
  -> 调 StreetModel /embed，item 仍是 {"type": "video", "uri": "...proxy.mp4"}
  -> 写入 Elasticsearch 向量索引
```

所以配置里 `mode: video`、`item_type: video` 保留即可。所谓“抽帧策略”指的是生成代理 MP4，不是直接传 JPG/PNG 图片列表。

## 2. 迁移后确认文件

进入新的 DeerFlow 目录：

```bash
cd <NEW_DEERFLOW_DIR>
```

确认这些文件存在即可：

```bash
ls -l config.yaml
ls -l extensions_config.json
ls -l scripts/streetmodel-video-embedding-mcp
chmod +x scripts/streetmodel-video-embedding-mcp
```


```text
/app/backend/.venv/bin/python
```

## 3. `config.yaml` 直接复制这段

把下面这段放到新目录的 `config.yaml` 中，保持和当前系统一致：

```yaml
streetmodel_embedding:
  enabled: true
  base_url: http://219.245.185.245:3130
  model_name: Qwen3-VL-Embedding-2B
  mode: video
  vector_field: video_vector-Qwen3-VL-Embedding-2B_urban_governance
  dimensions: 2048
  instruction: Represent this surveillance video for urban scene retrieval.
  text_instruction: Represent this surveillance/street-view query for urban scene retrieval.
  batch_size: 1
  timeout_seconds: 600
  video:
    item_type: video
    deerflow_path_prefix: /data/deerflow/videos
    streetmodel_path_prefix: /nfsdat2/home/xhuangslm/shared_videos

video_library:
  vector_dims: 1024
```

说明：

- `video_preprocess` 不写也可以，代码默认就是 `frame_proxy`。
- 默认代理视频会写到 `/data/deerflow/videos/embedding_proxies/`。
- StreetModel 侧会按 `/nfsdat2/home/xhuangslm/shared_videos/embedding_proxies/.../proxy.mp4` 读取代理视频。

如果新部署环境不能写 `/data/deerflow/videos`，再改 `deerflow_path_prefix` 和 `streetmodel_path_prefix`；否则不要动。

## 4. `extensions_config.json` 直接复制这段

如果只想启用 Elasticsearch MCP 和 StreetModel MCP，可以把新目录的 `extensions_config.json` 写成下面这样：

```json
{
  "mcpServers": {
    "elasticsearch": {
      "enabled": true,
      "type": "stdio",
      "command": "npx",
      "args": [
        "-y",
        "@modelcontextprotocol/server-elasticsearch"
      ],
      "env": {
        "ELASTICSEARCH_URL": "http://219.245.186.45:9200"
      },
      "url": null,
      "headers": {},
      "oauth": null,
      "description": "Elasticsearch search and query"
    },
    "streetmodel-video-embedding": {
      "enabled": true,
      "type": "stdio",
      "command": "bash",
      "args": [
        "-lc",
        "if [ -x ./scripts/streetmodel-video-embedding-mcp ]; then exec ./scripts/streetmodel-video-embedding-mcp; elif [ -x ../scripts/streetmodel-video-embedding-mcp ]; then exec ../scripts/streetmodel-video-embedding-mcp; else echo 'streetmodel-video-embedding MCP wrapper not found' >&2; exit 1; fi"
      ],
      "env": {
        "DEER_FLOW_CONFIG_PATH": "$DEER_FLOW_CONFIG_PATH",
        "ES_URL": "$ES_URL",
        "ES_USERNAME": "$ES_USERNAME",
        "ES_PASSWORD": "$ES_PASSWORD",
        "STREETMODEL_BASE_URL": "$STREETMODEL_BASE_URL"
      },
      "url": null,
      "headers": {},
      "oauth": null,
      "description": "StreetModel/Qwen3-VL video embedding tools for health checks, frame-proxy video vector indexing, and query vector generation"
    }
  },
  "skills": {}
}
```

Docker 容器内的工作目录通常是 `/app/backend`，所以 wrapper 会走 `../scripts/streetmodel-video-embedding-mcp`，也就是容器里的：

```text
/app/scripts/streetmodel-video-embedding-mcp
```

`DEER_FLOW_CONFIG_PATH` 如果为空，wrapper 会自动使用：

```text
/app/config.yaml
```

## 5. 启动 Docker

第一次迁移到新目录，建议先构建再启动：

```bash
DEER_FLOW_DOCKER_BUILD_ON_START=1 make docker-start
```

后续普通重启：

```bash
make docker-stop
make docker-start
```

查看日志：

```bash
make docker-logs
```

如果旧目录里的 Docker 服务还在运行，先停旧目录的服务，或者先处理端口/容器名冲突。这和 MCP 本身无关。

## 6. 验证

先检查 JSON 格式：

```bash
python -m json.tool extensions_config.json >/dev/null
```

然后让 Agent 调用：

```text
请调用 streetmodel_health 检查 StreetModel MCP 是否可用。
```

正常结果应包含：

```text
status: success
embedding_model: Qwen3-VL-Embedding-2B
dimensions: 2048
models_count > 0
```

也可以进 LangGraph 容器确认容器内 Python 环境存在：

```bash
docker exec -it huangxiao-deer-flow-langgraph ls -l /app/backend/.venv/bin/python
```

## 7. 常见错误

### 找不到 wrapper

如果报：

```text
streetmodel-video-embedding MCP wrapper not found
```

优先检查新镜像里是否有 wrapper：

```bash
docker exec -it huangxiao-deer-flow-langgraph ls -l /app/scripts/streetmodel-video-embedding-mcp
```

如果没有，重新构建：

```bash
DEER_FLOW_DOCKER_BUILD_ON_START=1 make docker-start
```

### 找不到 backend 虚拟环境

如果报：

```text
backend virtualenv not found
```

这不是让你在宿主机执行 `uv sync`。Docker 模式下应重新构建镜像：

```bash
make docker-stop
DEER_FLOW_DOCKER_BUILD_ON_START=1 make docker-start
```

如果仍然失败，再清理开发环境的 venv volume 后重建。

### 代理视频路径不可读

如果视频向量失败，优先检查：

```bash
ls -ld /data/deerflow/videos
ls -ld /data/deerflow/videos/embedding_proxies
```

DeerFlow 容器要能写 `/data/deerflow/videos/embedding_proxies/`，StreetModel 要能读映射后的 `/nfsdat2/home/xhuangslm/shared_videos/embedding_proxies/.../proxy.mp4`。
