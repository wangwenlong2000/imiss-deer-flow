# 文件上传修复与监控方案说明

本文档记录两类改动：

1. 文件上传修复
2. LangSmith + 本地 JSONL 双监控方案

适用场景：

- 无 nginx 的本地开发模式
- 前端和网关不同源直连
- 需要同时保留云端 tracing 和本地线程级落盘日志

## 1. 文件上传修复

### 1.1 问题现象

在 no-nginx 模式下，前端通常运行在 3000 或 3001，网关运行在 8001。此前端上传文件时会直接向网关发起跨域请求。

当网关未启用 CORS 中间件时，浏览器会先发起 OPTIONS 预检请求。若预检返回 405 或缺少 Access-Control-Allow-Origin，浏览器就会报 Failed to fetch，表现为：

- 前端上传失败
- 网关日志里可能已经出现 POST 200
- 但浏览器仍然认为请求失败

这类问题的根因是跨域预检失败，而不是上传逻辑本身写错。

### 1.2 修复内容

修复分为两部分：

1. 在网关启用 FastAPI 的 CORSMiddleware
2. 扩展默认允许源，覆盖本地常见前端地址

当前默认允许的来源包括：

- http://localhost:3000
- http://localhost:3001
- http://127.0.0.1:3000
- http://127.0.0.1:3001

如果你的前端使用其他端口，可以通过环境变量 CORS_ORIGINS 覆盖。

### 1.3 相关代码位置

- backend/app/gateway/app.py
- backend/app/gateway/config.py
- backend/tests/test_gateway_cors.py

### 1.4 上传文件的实际存储路径

用户上传文件会落到线程级目录：

backend/.deer-flow/threads/{thread_id}/user-data/uploads

Agent 在运行时看到的是虚拟路径：

/mnt/user-data/uploads

前端访问附件/制品时走的是 artifacts HTTP 路径，不直接暴露物理路径。

### 1.5 使用建议

如果环境允许 nginx，优先使用统一入口，避免跨域。

如果是无 sudo、无 nginx 的机器，保持 no-nginx 模式即可，但要确保网关已启用 CORS。

## 2. LangSmith + 本地 JSONL 双监控方案

### 2.1 方案目标

这套方案分成两层：

1. LangSmith
   负责云端 tracing、模型调用链可视化、运行过程检索
2. 本地 JSONL
   负责线程级、本机可直接 grep 和 tail 的运行事件落盘

两套方案可以同时开启，也可以单独启用其中一套。

### 2.2 LangSmith 链路说明

LangSmith 在当前项目里原本就有基础支持：

- tracing 开关和环境变量解析在 tracing_config.py
- 模型工厂在 tracing 打开时会自动挂 LangSmith tracer

因此这次没有重写 LangSmith 主链路，而是沿用现有实现。

相关代码位置：

- backend/packages/harness/deerflow/config/tracing_config.py
- backend/packages/harness/deerflow/models/factory.py

### 2.3 本地 JSONL 方案改动

这次补齐了本地 JSONL 监控链路，包含以下内容：

#### 2.3.1 公共监控工具

新增统一 JSONL 落盘工具：

- backend/packages/harness/deerflow/monitoring.py

职责：

- 读取监控开关
- 解析落盘目录
- 做 JSON 可序列化转换
- 按 thread_id 追加写入 jsonl 文件

#### 2.3.2 Lead Agent 运行历史中间件

新增中间件：

- backend/packages/harness/deerflow/agents/middlewares/run_history_middleware.py

并接入 lead agent：

- backend/packages/harness/deerflow/agents/lead_agent/agent.py

记录内容包括：

- runtime context
- 最终 response_text
- messages
- artifacts
- title
- todos
- uploaded_files
- thread_data

事件名：

- agent.run.final

#### 2.3.3 Channel 层事件落盘

在 channel manager 中补充了两条链路的事件记录：

- runs.wait.result
- runs.stream.start
- runs.stream.chunk
- runs.stream.error
- runs.stream.final

相关代码：

- backend/app/channels/manager.py

### 2.4 环境变量说明

#### 2.4.1 LangSmith

LangSmith 相关环境变量：

- LANGSMITH_TRACING
- LANGSMITH_API_KEY
- LANGSMITH_PROJECT
- LANGSMITH_ENDPOINT

示例：

```env
LANGSMITH_TRACING=true
LANGSMITH_API_KEY=your-langsmith-api-key
LANGSMITH_PROJECT=deer-flow
LANGSMITH_ENDPOINT=https://api.smith.langchain.com
```

#### 2.4.2 本地 JSONL

本地 JSONL 相关环境变量：

- DEERFLOW_RUN_EVENT_LOG_ENABLED
- DEERFLOW_RUN_EVENT_LOG_DIR

示例：

```env
DEERFLOW_RUN_EVENT_LOG_ENABLED=1
DEERFLOW_RUN_EVENT_LOG_DIR=/nfsdat/home/akzhaoslm/deer-flow/logs/run_events
```

### 2.5 开关组合

只开 LangSmith：

```env
LANGSMITH_TRACING=true
DEERFLOW_RUN_EVENT_LOG_ENABLED=0
```

只开本地 JSONL：

```env
LANGSMITH_TRACING=false
DEERFLOW_RUN_EVENT_LOG_ENABLED=1
```

两套都开：

```env
LANGSMITH_TRACING=true
DEERFLOW_RUN_EVENT_LOG_ENABLED=1
```

两套都关：

```env
LANGSMITH_TRACING=false
DEERFLOW_RUN_EVENT_LOG_ENABLED=0
```

### 2.6 启动与关闭

修改 .env 后，需要重启服务才能让新配置生效。

无 nginx 模式：

```bash
make stop-no-nginx
make dev-no-nginx
```

统一入口模式：

```bash
make stop
make dev
```

### 2.7 本地 JSONL 查看方式

如果开启了本地 JSONL，日志会按线程落盘到目录：

logs/run_events

常见查看命令：

查看目录：

```bash
ls /nfsdat/home/akzhaoslm/deer-flow/logs/run_events
```

查看某个线程最新日志：

```bash
tail -n 50 /nfsdat/home/akzhaoslm/deer-flow/logs/run_events/<thread_id>.jsonl
```

筛选最终 agent 结果：

```bash
grep '"event": "agent.run.final"' /nfsdat/home/akzhaoslm/deer-flow/logs/run_events/<thread_id>.jsonl
```

筛选流式结束事件：

```bash
grep '"event": "runs.stream.final"' /nfsdat/home/akzhaoslm/deer-flow/logs/run_events/<thread_id>.jsonl
```

筛选非流式 wait 结果：

```bash
grep '"event": "runs.wait.result"' /nfsdat/home/akzhaoslm/deer-flow/logs/run_events/<thread_id>.jsonl
```

### 2.8 LangSmith 查看方式

1. 在 .env 中设置 LangSmith 相关变量
2. 重启服务
3. 发起一次对话、工具调用或文件上传相关操作
4. 打开 LangSmith 控制台，进入 project deer-flow 查看 traces

### 2.9 测试覆盖

本次为新增功能补了回归测试：

- backend/tests/test_gateway_cors.py
- backend/tests/test_run_history_middleware.py
- backend/tests/test_channels.py

覆盖点包括：

- CORS 默认来源与预检行为
- agent.run.final 本地落盘
- runs.wait.result 本地落盘
- runs.stream.start/chunk/final 本地落盘
