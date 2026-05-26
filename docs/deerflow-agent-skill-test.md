# DeerFlow Agent 调用 Skill 的 Docker 测试记录

本文记录一次通过 Docker 部署的 DeerFlow Agent 调用 `video-stream-ingestion` skill 的完整测试过程。测试目标是验证真实链路：

```text
LangGraph API -> lead_agent -> read_file 工具 -> bash 工具 -> AioSandbox Docker 容器 -> skill 脚本
```

本次测试没有由操作者直接执行 skill 脚本，而是让 Agent 自己读取 `SKILL.md` 并调用工具完成执行。

## 测试环境

已运行的 Docker 服务：

```text
huangxiao-deer-flow-nginx
huangxiao-deer-flow-langgraph
huangxiao-deer-flow-gateway
huangxiao-deer-flow-frontedn
```

关键配置：

```text
LangGraph container: huangxiao-deer-flow-langgraph
LangGraph URL in container: http://localhost:3231
Assistant: lead_agent
Model: Qwen3.5 Plus(bailian)
Sandbox provider: deerflow.community.aio_sandbox:AioSandboxProvider
Sandbox image: huangxiao-deerflow-sandbox:network-tools
Skill path in sandbox: /mnt/skills/custom/video-stream-ingestion
Test video path in sandbox: /mnt/datasets/Vedio-demo/Trafic.mp4
```

测试日志文件：

```text
/tmp/deerflow-agent-skill-test.log
```

## 测试请求

创建 LangGraph thread 后，向 `lead_agent` 发送如下 prompt：

```text
请使用 video-stream-ingestion skill 规范化本地视频 /mnt/datasets/Vedio-demo/Trafic.mp4，camera_id=CAM_DEERFLOW_001，source_type=local_file，capture_seconds=10。请先读取该 skill 的 SKILL.md，再按脚本入口执行，并返回脚本 JSON 结果。
```

最终成功的 run 使用 `context` 传入 `thread_id`，不再同时传 `config.configurable`。这是因为当前 LangGraph API 返回过如下限制：

```text
Cannot specify both configurable and context. Prefer setting context alone.
```

## 执行过程

### 1. 创建线程

成功创建的线程：

```text
thread_id: 13314699-a2c6-4ed1-b67d-2698d5273684
run_id: 019e627f-d5f1-7752-bb41-9d064dd73efa
```

关键日志：

```text
[thread-create] status=200
[thread-id] 13314699-a2c6-4ed1-b67d-2698d5273684
[run-stream] status=200
[sse] event=metadata data={"run_id":"019e627f-d5f1-7752-bb41-9d064dd73efa","attempt":1}
```

### 2. Agent 读取 skill 文档

Agent 先调用 `read_file` 读取 skill 说明：

```text
tool-call: read_file
path: /mnt/skills/custom/video-stream-ingestion/SKILL.md
description: 读取 video-stream-ingestion skill 的 SKILL.md 了解执行流程
```

这一步证明 Agent 按 skill system prompt 的要求先加载了 skill 文件，而不是直接猜测脚本参数。

### 3. Agent 调用 bash 执行 skill

Agent 随后调用 `bash` 工具，在 AioSandbox 中执行：

```bash
python /mnt/skills/custom/video-stream-ingestion/scripts/run.py \
  --video /mnt/datasets/Vedio-demo/Trafic.mp4 \
  --camera-id CAM_DEERFLOW_001 \
  --source-type local_file \
  --capture-seconds 10 \
  --output /mnt/user-data/workspace/video_ingestion_result.json
```

关键日志：

```text
[tool-call] [{"name": "bash", "args": {"description": "执行 video-stream-ingestion 脚本规范化视频文件", "command": "python /mnt/skills/custom/video-stream-ingestion/scripts/run.py --video /mnt/datasets/Vedio-demo/Trafic.mp4 --camera-id CAM_DEERFLOW_001 --source-type local_file --capture-seconds 10 --output /mnt/user-data/workspace/video_ingestion_result.json"}}]
```

### 4. AioSandbox 容器创建

测试前没有活跃的 `deer-flow-sandbox-*` 容器。测试后出现：

```text
deer-flow-sandbox-51418350
image: huangxiao-deerflow-sandbox:network-tools
status: Up 15 seconds (healthy)
ports: 0.0.0.0:8182->8080/tcp
```

这说明 Agent 的工具调用触发了 AioSandbox 的 Docker 容器创建或复用流程。

## 测试结果

`bash` 工具返回的 skill JSON：

```json
{
  "skill": "video-stream-ingestion",
  "version": "1.0.0",
  "status": "success",
  "confidence": 1.0,
  "data": {
    "camera_id": "CAM_DEERFLOW_001",
    "source_type": "local_file",
    "file_status": "ok",
    "video_session_id": "VS_a2f47408",
    "started_at": "2026-05-20T10:00:00+08:00",
    "ended_at": "2026-05-20T10:00:10+08:00",
    "raw_segment_uri": "/mnt/datasets/Vedio-demo/Trafic.mp4",
    "duration_seconds": 10,
    "width": 1280,
    "height": 674
  }
}
```

Agent 最终回复中也返回了同一份 JSON，并总结：

```text
视频规范化已完成。
视频文件状态：正常 (ok)
摄像头 ID：CAM_DEERFLOW_001
视频会话 ID：VS_a2f47408
时长：10 秒
分辨率：1280 x 674
规范化 URI：/mnt/datasets/Vedio-demo/Trafic.mp4
```

## 踩坑记录

### 缺少 `context.thread_id`

第一次直接调用 LangGraph run stream 时，Agent middleware 报错：

```text
ValueError: Thread ID is required in the context
```

原因：DeerFlow 的部分 middleware 从 `runtime.context["thread_id"]` 获取线程 ID。直接使用 LangGraph API 时，需要在 run payload 里传入：

```json
{
  "context": {
    "thread_id": "<thread_id>"
  }
}
```

### 不能同时传 `configurable` 和 `context`

补充 `context` 后，如果仍同时传 `config.configurable`，LangGraph API 返回：

```text
Cannot specify both configurable and context. Prefer setting context alone.
```

本次成功测试采用只传 `context` 的方式，模型使用服务端默认解析结果，最终运行元数据显示：

```text
model_name: Qwen3.5 Plus(bailian)
thinking_enabled: true
is_plan_mode: false
subagent_enabled: false
```

## 成功判定

本次测试满足以下条件：

- Agent 自主调用 `read_file` 读取 `/mnt/skills/custom/video-stream-ingestion/SKILL.md`
- Agent 自主调用 `bash` 执行 `/mnt/skills/custom/video-stream-ingestion/scripts/run.py`
- Docker 中创建了健康的 `deer-flow-sandbox-51418350` 容器
- skill 返回 `status: success`
- 输出包含 `raw_segment_uri`、`video_session_id`、`duration_seconds`、`file_status`、`width`、`height`

结论：`video-stream-ingestion` 可以通过 Docker 部署的 DeerFlow Agent + AioSandbox 完整调用成功。

