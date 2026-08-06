# UrbanBrain 视频监控 Agent 小规模测试记录

本文是对旧版
`imiss-deer-flow-main/docs/deerflow-agent-skill-test.md` 的新架构版本更新。

旧版主要验证：

```text
LangGraph API -> lead_agent -> read_file -> bash -> AioSandbox -> 单个 skill 脚本
```

当前 qwen 架构需要区分三层：

```text
用户自然语言
  -> 场景识别 / SkillRouter
  -> city-video-intelligence 业务编排
  -> 原子视频 skill 或结构化 video_object_analytics 工具
  -> 证据、复核和最终结果
```

因此，本次测试不再只验证“Agent 能否直接调用某个底层脚本”，还要验证：

- 直接的视频业务请求是否先经过业务编排。
- 事件语义是否统一进入 `single-video-event-analysis`。
- 目标检测和跟踪是否只作为对象上下文，不直接判断事件。
- `video-object-analytics` 是否作为结构化 Agent 工具使用，而不是让模型自行拼接底层命令。
- 新 bundle 的嵌套路径、Router Card、registry 和实际可执行文件是否一致。

## 1. 测试范围

### 1.1 新架构的推荐链路

单视频事件：

```text
city-video-intelligence
  -> single-video-event-analysis
  -> evidence-snapshot / video-segment-extraction
  -> privacy-masking
  -> human-review-routing
```

目标分析：

```text
video_object_analytics
  -> frame-sampling
  -> object-detection
  -> optional object-tracking
```

视频库：

```text
city-video-intelligence
  -> batch-video-ingestion
  -> video-search / object-statistics
  -> evidence-package-generation
```

关键边界：

- `single-video-event-analysis` 是当前唯一的视频事件语义入口。
- YOLO、目标跟踪、ROI 和几何结果不能单独推出打架、摔倒、车祸、拥堵、入侵或烟火事件。
- `video-object-analytics` 负责检测/跟踪的参数和脚本编排，Agent 不应为对象分析手写 bash 命令。
- 旧版按密度、ROI、目标组合和时间规则直接判定业务事件的 skill 已从可执行 bundle 中移除，不作为新架构的执行链路。
- 证据不足、画面模糊、遮挡或高风险事件必须返回 `requires_human_review=true` 或进入人工复核。

### 1.2 路径差异

旧版单个 skill 通常位于：

```text
/mnt/skills/custom/<skill-id>/SKILL.md
```

当前 qwen 视频 bundle 的实际路径是：

```text
/mnt/skills/custom/video_surveillance/<skill-id>/SKILL.md
```

例如：

```text
/mnt/skills/custom/video_surveillance/city-video-intelligence/SKILL.md
/mnt/skills/custom/video_surveillance/single-video-event-analysis/SKILL.md
/mnt/skills/custom/video_surveillance/video-stream-ingestion/scripts/run.py
```

`video_object_analytics` 是 Agent 结构化工具，对应代码：

```text
backend/packages/harness/deerflow/tools/builtins/video_object_analytics_tool.py
```

它不是让用户直接指定的 skill 名称，也不应通过 bash 手工拼接检测和跟踪命令。

## 2. 测试环境与前置条件

完整 Agent 测试需要：

| 项目 | 要求 |
| --- | --- |
| DeerFlow 服务 | LangGraph API、Gateway、前端或可访问的 Agent API |
| Agent | `lead_agent` |
| 线程上下文 | run payload 传 `context.thread_id`；不要同时传旧式 `config.configurable` |
| Sandbox | AioSandbox 或已配置的本地 sandbox |
| 视频路径 | 运行时能访问 `/mnt/datasets`、`/mnt/user-data` 或 `/data/deerflow/videos` |
| 视频处理 | `ffmpeg`、`ffprobe` |
| 抽帧/打码 | OpenCV |
| 目标检测 | `ultralytics` 和 `models/yolov8n.pt` |
| 语义视频检索 | Elasticsearch；向量检索还需要 SkillRouter/StreetModel 服务 |

当前仓库中的示例视频引用了：

```text
/home/huangxiao/City_brain/imiss-deer-flow-main/datasets/Vedio-demo/Trafic.mp4
```

运行 Agent 时必须把该文件映射到 Agent sandbox 能访问的路径，例如：

```text
/mnt/datasets/Vedio-demo/Trafic.mp4
```

不要直接把宿主机路径当成容器内路径，除非 sandbox 已配置对应映射。

## 3. 小规模测试用例

本次只选择四类请求，避免在没有视频依赖和在线服务时运行大批量测试。

### T01：业务编排与单视频事件分析计划

用户问题：

```text
请分析 /mnt/datasets/Vedio-demo/Trafic.mp4 是否存在打架、摔倒或交通事故，并给出可见证据时间线。
```

期望：

- `city-video-intelligence` 返回 `status=success`。
- `capability=single_video_event_understanding`。
- `recommended_skill_chain=["single-video-event-analysis"]`。
- 高风险事件应设置 `requires_human_review=true`。
- 下游事件分析只基于抽样帧和可见证据，不由 YOLO 框直接得出结论。

实际结果：通过编排计划测试。返回单视频事件理解、统一事件分析链路，并标记需要人工复核。

### T02：交警视频调阅计划

用户问题：

```text
我是交警，需要调取大成路路口今天上午 8-10 点的监控。
```

期望：

- 识别为 `traffic_police_video_review`。
- 推荐 `video-search`。
- 不应因为用户提到“监控”就直接运行事件分析。
- 视频没有入库时，应报告索引或数据缺失，而不是伪造命中结果。

实际结果：通过编排计划测试。正确识别为交警视频调阅，并推荐视频检索。

### T03：证据包缺少关键输入

用户问题：

```text
请把仓库事件的截图、前后 20 秒片段、哈希和打码副本整理成证据包。
```

期望：

- 识别为证据固化场景。
- 不能为了生成证据包而凭空创造事件。
- 缺少视频路径和事件时间时，只提出一个合并后的补充问题。

实际结果：通过边界测试。返回缺少“视频文件路径和事件时间或片段范围”的单一追问。

### T04：原子 skill 的失败契约与几何逻辑

测试内容：

- 对不存在的本地视频执行 `video-stream-ingestion`。
- 验证事件去重空输入仍返回标准成功 JSON。
- 验证 ROI 点在多边形内/外的结果。
- 验证人工复核契约能够返回 `manual_review`。

实际结果：通过。不存在文件返回 `SOURCE_NOT_FOUND`，没有伪造视频元数据；去重、ROI 和复核结果符合各自的 JSON 契约。

## 4. 可复现命令

以下命令在仓库根目录执行。路径均针对当前 qwen bundle，不能替换成旧版的平铺路径。

### 4.1 业务编排 smoke test

```bash
python skills/custom/video_surveillance/city-video-intelligence/scripts/run.py \
  --request "请分析 /mnt/datasets/Vedio-demo/Trafic.mp4 是否存在打架、摔倒或交通事故，并给出可见证据时间线" \
  --format json \
  --output /tmp/citybrain-video-plan-event.json
```

查看关键字段：

```bash
python -c 'import json; d=json.load(open("/tmp/citybrain-video-plan-event.json")); print({k:d.get(k) for k in ["status","capability","recommended_skill_chain","requires_human_review","capability_gap"]})'
```

### 4.2 本地视频输入失败契约

```bash
python skills/custom/video_surveillance/video-stream-ingestion/scripts/run.py \
  --video /tmp/city-brain-video-does-not-exist.mp4 \
  --camera-id CAM_TEST \
  --source-type local_file \
  --capture-seconds 1 \
  --config skills/custom/video_surveillance/configs/deerflow_config.json \
  --output /tmp/citybrain-video-ingestion-smoke.json
```

预期：进程返回非零退出码，JSON 中为：

```json
{
  "status": "failed",
  "error_code": "SOURCE_NOT_FOUND"
}
```

### 4.3 Router Card 基础检查

```bash
python - <<'PY'
import json
from pathlib import Path

cards = list(Path("skills").rglob("router_card.json"))
valid = 0
ids = []
for path in cards:
    data = json.loads(path.read_text(encoding="utf-8"))
    identity = data.get("identity", {})
    assert identity.get("id") and identity.get("name")
    assert data.get("scope") and data.get("routing")
    valid += 1
    ids.append(identity["id"])
print({"router_cards": len(cards), "basic_valid": valid, "unique_ids": len(set(ids))})
PY
```

本次结果：`117` 个 Router Card 均能被 JSON 解析，`117` 个 ID 唯一，并具备基础的 identity、scope 和 routing 字段。由于当前 Python 环境没有 `jsonschema`，这不是完整 JSON Schema 验证。

### 4.4 路由冲突检查

```bash
python scripts/check_skill_router_conflicts.py \
  --all \
  --skills-root skills \
  --no-embedding \
  --json
```

本次结果：返回 `conflicts=[]`。该命令关闭了 embedding，只验证 Router Card 的集合边界，不能替代在线向量和 reranker 测试。

### 4.5 通用离线路由基线

```bash
python scripts/eval_skill_router.py --offline --json
```

本次结果：`6/9` 通过。失败的 3 条是法规检索、通用数据分析和模糊数据分析，属于脚本内置的非视频用例；该脚本当前没有视频问题集，且只扫描平铺的 `skills/custom/<id>/router_card.json`，不能作为新视频 bundle 的完整评测。

## 5. Agent + Sandbox 端到端测试

具备服务和视频依赖后，按以下顺序运行。

### 5.1 创建 thread

通过 LangGraph API 为每一个问题创建独立 thread，保存返回的 `thread_id`。测试 assistant 和 thread 默认保留，便于在前端历史中逐条查看；只有明确传入 `--cleanup` 时才删除。

如果问题需要本地视频，先调用：

```text
POST /api/threads/{thread_id}/uploads
files[]=<对应视频文件>
```

然后把上传响应中的 `filename`、`size` 和虚拟路径放入消息的 `additional_kwargs.files`。不能只把宿主机视频路径写进问题文本，否则这只是路径字符串，不是用户上传的视频。

### 5.2 向 `lead_agent` 发送自然用户问题

不要在用户问题中指定 skill：

```text
我上传了一段路口监控，请检查是否存在交通事故、拥堵或烟火，并给出证据时间线；证据不足时请标记需要人工复核。
```

run payload 的关键形式：

```json
{
  "assistant_id": "lead_agent",
  "input": {
    "messages": [
      {
        "role": "user",
        "content": "我上传了一段路口监控，请检查是否存在交通事故、拥堵或烟火，并给出证据时间线；证据不足时请标记需要人工复核。",
        "additional_kwargs": {
          "files": [
            {
              "filename": "Trafic.mp4",
              "size": 75965424,
              "path": "/mnt/user-data/uploads/Trafic.mp4",
              "status": "uploaded"
            }
          ]
        }
      }
    ]
  },
  "context": {
    "thread_id": "<thread_id>"
  }
}
```

不要同时传：

```json
{
  "config": {
    "configurable": {
      "thread_id": "<thread_id>"
    }
  },
  "context": {
    "thread_id": "<thread_id>"
  }
}
```

旧架构测试已经证明，这种同时传递方式会触发：

```text
Cannot specify both configurable and context. Prefer setting context alone.
```

### 5.3 Agent 侧成功判定

#### 事件请求

- Agent 先调用 `city-video-intelligence` 规划。
- Agent 读取 `/mnt/skills/custom/video_surveillance/city-video-intelligence/SKILL.md`。
- Agent 继续读取 `/mnt/skills/custom/video_surveillance/single-video-event-analysis/SKILL.md`。
- 脚本生成 `review_manifest` 或等价审帧产物。
- 最终结果包含 `visual_timeline`、事件候选、`evidence_frame_ids`、置信度和 `requires_review`。
- Agent 不调用旧规则事件 skill，不用 YOLO 框直接判定事件。

#### 对象请求

用户问题：

```text
统计这段视频前 10 秒里的人、轿车和公交车，并跟踪它们是否移动或停留。
```

成功判定：

- Agent 调用结构化工具 `video_object_analytics`。
- `operation=detect` 或 `operation=track` 与用户目标一致。
- 结果包含 `executed_skills`、帧/检测/轨迹 artifact URI。
- 不通过 bash 让模型自行拼接 `frame-sampling`、`object-detection` 和 `object-tracking` 命令。
- 结果只描述对象和轨迹，不输出打架、拥堵或入侵结论。

### 5.4 Agent 失败判定

以下任一情况视为失败：

- 直接跳过 `city-video-intelligence` 执行下游业务 skill。
- 把 `/mnt/skills/custom/<skill-id>` 当成当前 bundle 的唯一路径。
- 目标检测结果直接被写成事件结论。
- 视频不可访问时仍返回具体事件。
- 没有 `evidence_frame_ids` 却输出确定的事件结论。
- 证据包缺少视频路径/事件时间时自动生成虚假 artifact。
- 事件脚本失败后继续私自调试或改写命令，而不是报告具体错误。

## 6. 本次测试结果

| 测试 | 结果 | 说明 |
| --- | --- | --- |
| T01 事件业务编排 | 通过 | 正确返回 `single_video_event_understanding` 和 `single-video-event-analysis`，并要求人工复核。 |
| T02 交警调阅编排 | 通过 | 正确识别 `traffic_police_video_review`，推荐 `video-search`。 |
| T03 证据包缺失输入 | 通过 | 只提出视频路径和事件时间/片段范围这一项合并追问。 |
| T04 原子失败/契约/几何 smoke | 通过 | `SOURCE_NOT_FOUND`、去重、人工复核和 ROI 几何检查符合预期。 |
| Router Card 基础 JSON 检查 | 通过 | 117/117 可解析且 ID 唯一；未完成 JSON Schema 检查。 |
| 无 embedding 路由冲突检查 | 通过 | `conflicts=[]`。 |
| 通用离线路由基线 | 部分通过 | 6/9；3 条失败属于内置的非视频用例。 |
| Agent + 视频文件端到端 | 已执行，未完全通过 | qwen35B 5 个自然问题已通过 LangGraph API 实测；修正版为 4 个用例上传真实 `Trafic.mp4` 并保留 5 个独立 thread。复杂任务仍出现递归上限，对象分析工具失败，元数据请求绕过目标 skill。详见 [`video-surveillance-qwen35b-test-report.md`](video-surveillance-qwen35b-test-report.md)。 |

本次 qwen35B 小规模批次使用 `C-001`、`C-002`、`C-011`、`C-038`、`C-040`。修正版结果和原始 SSE 保存在 `outputs/skill-tests/video-surveillance-qwen35b/20260723-175833/`。自动统计为 1/5，但该统计会把场景过滤器注入的候选 skill 误计为实际 skill；最终结论以报告中的实际工具调用为准。

## 7. 当前发现的问题

### 7.1 `skills/registry.json` 与视频 bundle 不同步

本次只读检查发现：

- `skills/custom/video_surveillance` 当前有 21 个 Router Card。
- `skills/registry.json` 中标记为 `video_surveillance` 的条目为 20 个。
- registry 缺少当前 bundle 中的：
  - `batch-video-ingestion`
  - `city-video-intelligence`
  - `evidence-package-generation`
  - `object-statistics`
  - `single-video-event-analysis`
  - `video-embedding-index`
  - `video-search`
- registry 仍保留当前 bundle 中没有可执行 `SKILL.md`/脚本的旧方法条目：
  - `density-aggregation-event`
  - `event-rule-engine`
  - `event-template-mapping`
  - `object-composition-event`
  - `spatial-occupancy-event`
  - `temporal-persistence-event`
- 多数 registry 条目的 `skill_md_path` 仍指向 `skills/custom/<skill-id>/SKILL.md`，但实际文件在 `skills/custom/video_surveillance/<skill-id>/SKILL.md`。

这会影响真实 Agent 的 skill 加载和路由注入。需要先重新生成/修正 registry，再做完整 Agent + Sandbox 测试。本次没有直接改写已有 `skills/registry.json`，避免覆盖当前工作区已有改动。

### 7.2 业务编排的一条路由边界

本次对“北门昨晚有没有陌生人尾随进入，并给出时间线”的计划测试返回了：

```text
video-search -> human-review-routing
```

没有直接推荐 `single-video-event-analysis`。如果业务要求对“尾随进入”做单视频事件语义审查，这条路由需要补充事件词和场景规则后再验证。

## 8. 后续完整测试顺序

1. 修正或重新生成视频 bundle 的 `registry.json`，确保 Router Card、SKILL.md 和实际脚本路径一致。
2. 确认 `skill_router.enabled`、Elasticsearch、Embedding 和 Reranker 服务配置。
3. 准备一个 sandbox 可访问的短视频，并安装 `ffmpeg/ffprobe`、OpenCV 和 Ultralytics。
4. 先运行 T01 的 `city-video-intelligence` 计划测试。
5. 再运行单视频事件分析，人工查看 manifest 中的少量审帧图片。
6. 再运行 `video_object_analytics` 的 `detect` 和 `track` 两个最小操作。
7. 最后串联证据截图、片段、隐私打码和人工复核，并记录每个 artifact URI。
