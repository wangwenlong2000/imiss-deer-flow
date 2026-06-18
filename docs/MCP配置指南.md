# DeerFlow MCP 服务配置指南

本文说明本项目中 MCP（Model Context Protocol）服务的配置、启用、验证和排障方式。MCP 服务用于把外部工具能力接入 DeerFlow Agent，例如文件系统、Elasticsearch、GitHub，以及本项目内置的 StreetModel 视频向量服务。

## 1. 配置文件位置

MCP 与 Skill 的启用状态统一放在项目根目录：

```text
extensions_config.json
```

首次部署时可以从示例文件复制：

```bash
cp extensions_config.example.json extensions_config.json
```

DeerFlow 读取配置文件的优先级如下：

1. 显式传入的配置路径。
2. 环境变量 `DEER_FLOW_EXTENSIONS_CONFIG_PATH` 指向的文件。
3. 当前工作目录下的 `extensions_config.json`。
4. 当前工作目录父目录下的 `extensions_config.json`。
5. 兼容旧版本的 `mcp_config.json`。

常规部署建议始终把配置放在仓库根目录的 `extensions_config.json`。

## 2. MCP 配置结构

`extensions_config.json` 的核心结构如下：

```json
{
  "mcpServers": {
    "server-name": {
      "enabled": true,
      "type": "stdio",
      "command": "npx",
      "args": ["-y", "some-mcp-server"],
      "env": {},
      "url": null,
      "headers": {},
      "oauth": null,
      "description": "Human readable description"
    }
  },
  "skills": {}
}
```

字段说明：

| 字段 | 说明 |
| --- | --- |
| `enabled` | 是否启用该 MCP server。只有 `true` 的服务会被 Agent 加载。 |
| `type` | 传输类型，支持 `stdio`、`sse`、`http`。 |
| `command` | `stdio` 类型的启动命令，例如 `npx`、`bash`、`python`。 |
| `args` | `stdio` 类型的命令参数。 |
| `env` | 传给 MCP server 进程的环境变量。值可以写成 `$ENV_NAME`，运行时会从环境变量解析。 |
| `url` | `sse` 或 `http` 类型 MCP server 的服务地址。 |
| `headers` | `sse` 或 `http` 类型请求头。 |
| `oauth` | `sse` 或 `http` 类型的 OAuth 配置。 |
| `description` | 服务说明，只用于展示和维护。 |

注意：如果配置值以 `$` 开头，例如 `$ES_URL`，DeerFlow 会尝试从同名环境变量读取。未设置时会解析为空字符串，而不是保留字面量 `$ES_URL`。

## 3. 内置 MCP 服务

当前示例配置包含以下 MCP server：

| 名称 | 默认状态 | 用途 |
| --- | --- | --- |
| `filesystem` | 关闭 | 允许 Agent 访问指定目录内的文件。 |
| `github` | 关闭 | 访问 GitHub 仓库能力，需要 `GITHUB_TOKEN`。 |
| `postgres` | 关闭 | 访问 PostgreSQL 数据库。 |
| `elasticsearch` | 示例中可开启 | 查询 Elasticsearch。 |
| `streetmodel-video-embedding` | 示例中关闭，当前环境可开启 | 调用 StreetModel/Qwen3-VL 生成视频向量或文本查询向量。 |

## 4. 启用 StreetModel 视频向量 MCP

本项目已内置 `streetmodel-video-embedding` MCP server，代码位于：

```text
mcp_servers/streetmodel_video_embedding/
scripts/streetmodel-video-embedding-mcp
```

它向 Agent 暴露三个工具：

| 工具 | 用途 |
| --- | --- |
| `streetmodel_health` | 检查 StreetModel `/health`、`/models`、向量维度和向量字段配置。 |
| `streetmodel_embed_video` | 对单个视频生成 StreetModel 视频向量，并写入 Elasticsearch 目标索引。 |
| `streetmodel_embed_query` | 对自然语言查询生成文本向量，用于语义视频检索。 |

### 4.1 依赖准备

确保 backend 虚拟环境已安装依赖：

```bash
cd backend
uv sync
```

确认 MCP wrapper 可执行：

```bash
test -x ../scripts/streetmodel-video-embedding-mcp
```

如果不可执行：

```bash
chmod +x ../scripts/streetmodel-video-embedding-mcp
```

### 4.2 配置 `config.yaml`

StreetModel MCP 会读取根目录 `config.yaml`，重点检查以下配置：

```yaml
streetmodel_embedding:
  base_url: http://<streetmodel-host>:<port>
  model_name: Qwen3-VL-Embedding-2B
  dimensions: 2048
  vector_field: video_vector-Qwen3-VL-Embedding-2B_urban_governance
  video:
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

关键点：

- `base_url` 必须能从 DeerFlow 后端所在机器访问。
- `dimensions` 要与 StreetModel 返回向量长度一致，当前视频向量链路通常为 `2048`。
- `vector_field` 要与 Elasticsearch 向量索引字段一致。
- 当前默认策略是 `video_preprocess.mode=frame_proxy`：DeerFlow 先在本机用 `ffmpeg` 抽帧/降采样，生成一个短代理 MP4，再把代理视频交给 StreetModel 做向量。
- `deerflow_path_prefix` 是代理视频在 DeerFlow 侧的输出根目录；默认代理文件会写到 `${deerflow_path_prefix}/embedding_proxies/`。
- `streetmodel_path_prefix` 是同一批代理文件在 StreetModel/GPU 服务侧可见的路径前缀。它不表示原视频必须提前放在共享目录，只表示代理视频输出目录需要能映射到 StreetModel 可读取的位置。

### 4.3 配置 `extensions_config.json`

开启 `streetmodel-video-embedding`：

```json
{
  "mcpServers": {
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

建议在 `.env` 或服务启动环境中设置：

```bash
export DEER_FLOW_CONFIG_PATH=/home/huangxiao/City_brain/imiss-deer-flow-main/config.yaml
export ES_URL=http://<elasticsearch-host>:9200
export ES_USERNAME=
export ES_PASSWORD=
export STREETMODEL_BASE_URL=http://<streetmodel-host>:<port>
```

如果 `config.yaml` 已包含 Elasticsearch 和 StreetModel 地址，上述环境变量可以只保留 `DEER_FLOW_CONFIG_PATH`。环境变量优先级通常高于配置文件，适合不同服务器复用同一份配置模板。

## 5. 启用 Elasticsearch MCP

如果需要让 Agent 直接查询 Elasticsearch，可以开启：

```json
{
  "mcpServers": {
    "elasticsearch": {
      "enabled": true,
      "type": "stdio",
      "command": "npx",
      "args": ["-y", "@modelcontextprotocol/server-elasticsearch"],
      "env": {
        "ELASTICSEARCH_URL": "$ES_URL"
      },
      "url": null,
      "headers": {},
      "oauth": null,
      "description": "Elasticsearch search and query"
    }
  }
}
```

依赖要求：

- 机器上可执行 `npx`。
- 网络能访问 `ELASTICSEARCH_URL`。
- 如 Elasticsearch 需要认证，应按所用 MCP server 的官方参数补充用户名、密码或 token。

## 6. 启用 filesystem MCP

filesystem MCP 必须收敛允许访问的目录，禁止直接给 `/`、用户家目录或项目上级大目录。

示例：

```json
{
  "mcpServers": {
    "filesystem": {
      "enabled": true,
      "type": "stdio",
      "command": "npx",
      "args": [
        "-y",
        "@modelcontextprotocol/server-filesystem",
        "/home/huangxiao/City_brain/imiss-deer-flow-main/outputs",
        "/home/huangxiao/City_brain/imiss-deer-flow-main/datasets"
      ],
      "env": {},
      "description": "Provides filesystem access within allowed directories"
    }
  }
}
```

安全建议：

- 只开放 Agent 必须读取或写入的目录。
- 不要开放包含密钥、`.env`、私有仓库凭证或系统配置的目录。
- filesystem MCP 的访问边界独立于 DeerFlow sandbox，需要分别配置和验证。

## 7. HTTP/SSE MCP 与 OAuth

`http` 和 `sse` 类型 MCP 支持 OAuth 自动取 token 和刷新。示例：

```json
{
  "mcpServers": {
    "secure-http-server": {
      "enabled": true,
      "type": "http",
      "url": "https://api.example.com/mcp",
      "headers": {},
      "oauth": {
        "enabled": true,
        "token_url": "https://auth.example.com/oauth/token",
        "grant_type": "client_credentials",
        "client_id": "$MCP_OAUTH_CLIENT_ID",
        "client_secret": "$MCP_OAUTH_CLIENT_SECRET",
        "scope": "mcp.read",
        "refresh_skew_seconds": 60
      },
      "description": "Secure remote MCP server"
    }
  }
}
```

支持的 `grant_type`：

- `client_credentials`
- `refresh_token`

密钥类配置建议始终使用环境变量，不要把明文 secret 写入仓库。

## 8. 通过 API 查看和更新配置

Gateway 提供 MCP 配置接口：

```bash
curl http://localhost:<gateway-port>/api/mcp/config
```

更新配置：

```bash
curl -X PUT http://localhost:<gateway-port>/api/mcp/config \
  -H 'Content-Type: application/json' \
  -d @mcp-config-update.json
```

请求体字段使用 `mcp_servers`，服务端会写回 `extensions_config.json` 的 `mcpServers`：

```json
{
  "mcp_servers": {
    "streetmodel-video-embedding": {
      "enabled": true,
      "type": "stdio",
      "command": "bash",
      "args": ["-lc", "exec ./scripts/streetmodel-video-embedding-mcp"],
      "env": {
        "DEER_FLOW_CONFIG_PATH": "$DEER_FLOW_CONFIG_PATH"
      },
      "url": null,
      "headers": {},
      "oauth": null,
      "description": "StreetModel video embedding tools"
    }
  }
}
```

说明：

- Gateway 更新配置后会保存文件并刷新 Gateway 进程内缓存。
- LangGraph/Agent 执行进程会根据 `extensions_config.json` 文件修改时间重新加载 MCP tools。
- 如果服务长期运行且未检测到变化，可以重启 DeerFlow 服务强制重新加载。

## 9. 启动和验证

### 9.1 启动服务

Linux 服务器部署通常使用：

```bash
make linux-server-start
```

查看状态：

```bash
make linux-server-status
```

停止后重启：

```bash
make linux-server-stop
make linux-server-start
```

### 9.2 本地验证配置文件

检查 JSON 格式：

```bash
python -m json.tool extensions_config.json >/dev/null
```

检查 enabled server 是否能被解析：

```bash
cd backend
uv run python - <<'PY'
from deerflow.config.extensions_config import ExtensionsConfig
from deerflow.mcp.client import build_servers_config

cfg = ExtensionsConfig.from_file("../extensions_config.json")
servers = build_servers_config(cfg)
print("enabled servers:", sorted(servers))
PY
```

### 9.3 验证 StreetModel MCP wrapper

检查依赖和 wrapper：

```bash
./scripts/streetmodel-video-embedding-mcp --help
```

由于 stdio MCP server 会进入协议监听状态，直接运行后不一定输出普通 CLI 帮助；只要没有出现以下错误，就说明基础环境基本可用：

- `backend virtualenv not found`
- `Python MCP SDK is not installed`
- `streetmodel-video-embedding MCP wrapper not found`

### 9.4 在 Agent 中验证工具

启动服务后，可以让 Agent 执行：

```text
请调用 streetmodel_health 检查 StreetModel MCP 是否可用。
```

期望返回：

- `status: success`
- 正确的 `base_url`
- 正确的 `embedding_model`
- 正确的 `dimensions`
- `models_count > 0`

## 10. 常见问题

### 10.1 配置了 MCP 但 Agent 看不到工具

检查：

1. `extensions_config.json` 是否在 DeerFlow 进程工作目录或父目录。
2. 目标 server 的 `enabled` 是否为 `true`。
3. JSON 是否有效。
4. `stdio` 类型是否配置了 `command`。
5. `http`/`sse` 类型是否配置了 `url`。
6. 修改配置后是否重启服务，或文件修改时间是否已变化。

### 10.2 `streetmodel-video-embedding MCP wrapper not found`

说明当前工作目录下找不到：

```text
./scripts/streetmodel-video-embedding-mcp
../scripts/streetmodel-video-embedding-mcp
```

处理方式：

- 确认服务从仓库根目录或 `backend/` 目录启动。
- 使用绝对路径替换 `args` 中的 wrapper 路径。
- 确认文件有可执行权限。

### 10.3 `backend virtualenv not found`

执行：

```bash
cd backend
uv sync
```

然后重新启动 DeerFlow。

### 10.4 StreetModel 健康检查失败

检查：

1. `STREETMODEL_BASE_URL` 或 `config.yaml` 中的 `streetmodel_embedding.base_url` 是否正确。
2. DeerFlow 后端机器是否能访问 StreetModel 服务。
3. StreetModel `/health` 和 `/models` 是否正常。
4. `model_name` 是否存在于 `/models` 返回结果中。
5. `dimensions` 是否与模型实际返回向量长度一致。

### 10.5 视频向量写入 Elasticsearch 失败

检查：

1. `ES_URL` 或 `config.yaml` 中 Elasticsearch 地址是否正确。
2. 认证信息是否正确。
3. 目标索引是否允许写入。
4. 目标索引向量字段维度是否与 `dimensions` 一致。
5. 默认安全策略只允许写入以 `huangxiao-` 开头的个人索引；写共享索引需要显式允许。

### 10.6 视频路径或代理视频映射失败

当前默认策略是抽帧代理：原视频只需要 DeerFlow 后端本机可读；工具会生成 frame-proxy 代理 MP4，并把代理 MP4 的路径传给 StreetModel。真正需要被 StreetModel 读取的是代理 MP4，而不一定是原视频。

工具要求路径满足以下条件之一：

- 已经位于 `streetmodel_path_prefix` 下。
- 位于 `deerflow_path_prefix` 下，并可映射到 `streetmodel_path_prefix`。
- 原视频在其他本地路径，但 DeerFlow 后端可读；工具可基于本地原视频生成代理 MP4，代理 MP4 写入 `deerflow_path_prefix/embedding_proxies/` 后再映射给 StreetModel。
- 调用 `streetmodel_embed_video` 时设置 `copy_video_to_shared=true`，由工具先复制原视频到 `deerflow_path_prefix/direct_uploads/`，再生成代理 MP4。

常见修复：

- 检查 `config.yaml` 中两个 prefix 是否与真实挂载路径一致。
- 确认 StreetModel/GPU 服务能读取映射后的代理 MP4 路径。
- 确认 DeerFlow 后端对 `deerflow_path_prefix/embedding_proxies/` 有写权限。
- 对长视频优先使用默认 frame-proxy 预处理，不要随意设置 `video_preprocess=none`。

## 11. 配置变更流程建议

推荐流程：

1. 从 `extensions_config.example.json` 复制或对照修改。
2. 只开启本次需要的 MCP server。
3. 用环境变量注入密钥和服务地址。
4. 用 `python -m json.tool` 验证 JSON。
5. 启动或重启 DeerFlow。
6. 先调用健康检查工具，例如 `streetmodel_health`。
7. 再执行真实任务，例如视频向量入库或语义查询。

生产环境建议：

- 不把密钥写入 Git。
- filesystem MCP 只开放最小目录。
- StreetModel 默认写个人索引，例如 `huangxiao-video-library-vector-v1`。
- 变更 `extensions_config.json` 后记录变更人、变更时间和启用的 server。
- 对外部 HTTP/SSE MCP server 设置合理的超时、认证和网络访问控制。
