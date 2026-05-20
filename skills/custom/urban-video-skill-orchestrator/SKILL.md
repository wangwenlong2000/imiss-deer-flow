---
name: urban-video-skill-orchestrator
description: 城市超脑视频分析 skill 编排中枢，根据任务类型规划清晰调用链路，统一调度视频源巡检、目标跟踪、事件识别、空间规则、告警去重、多源关联、证据生成、查询摘要和模型治理。
version: 1.0.0
author: Urban Intelligence Lab
compatibility: ">=2.0.0"
allowed-tools:
  - bash
  - read_file
---

# 城市视频 Skill 编排中枢

## 定位

本 skill 是 Agent 调用城市视频分析能力时的第一入口。它不直接替代具体业务 skill，而是负责判断任务类型、选择主链路、补充前置条件和约束，避免 Agent 在大量 skill 中随意选择。

当用户的问题涉及城市视频、监控、摄像头、告警、违章、交通、人群、证据、地图关联、模型治理时，Agent 应优先读取本 skill，再按规划链路调用下游 skill。

## 调用原则

1. 先判断任务类型，再选链路。
2. 输入是视频源或摄像头时，必须先调用 `video-source-operations`。
3. 涉及事件识别时，必须经过 `object-tracking-analysis`，再进入 `scenario-event-recognition`。
4. 涉及违规、越界、占道、方向、时间段判断时，必须调用 `spatial-rule-engine`。
5. 涉及告警输出、派单、日报、统计前，必须调用 `alert-deduplication`。
6. 涉及地图、网格、道路、工单、IoT 时，调用 `multi-source-correlation`。
7. 涉及执法、复核、派单材料时，调用 `evidence-package-builder`。
8. 涉及“今天/过去/最多/高风险/汇总/查询”时，优先调用 `structured-video-query`，并要求输入是结构化事件或告警。
9. 涉及误报、阈值、模型版本、性能时，调用 `model-ops-governance`。

## 任务规划脚本

```bash
python3 /mnt/skills/custom/urban-video-skill-orchestrator/scripts/plan_chain.py "<用户任务描述>"
```

如果已有结构化输入类型，可补充：

```bash
python3 /mnt/skills/custom/urban-video-skill-orchestrator/scripts/plan_chain.py "<用户任务描述>" --input-kind video|stream|alerts|events|feedback|unknown
```

## 机器可读 registry

编排规则位于：

```text
/mnt/skills/custom/urban-video-skill-orchestrator/references/skill_registry.json
```

Agent 可以读取该 registry 理解：

- skill 分层
- 主链路
- 旧 skill 的兼容位置
- 不同任务的必经节点
- 每条链路的最终输出

## 常见任务链路

### 实时告警发现

```text
video-source-operations
-> object-tracking-analysis
-> scenario-event-recognition
-> spatial-rule-engine
-> alert-deduplication
-> multi-source-correlation
-> evidence-package-builder
```

### 城市治理违章分析

```text
video-source-operations
-> object-tracking-analysis
-> scenario-event-recognition --scenario governance
-> spatial-rule-engine
-> alert-deduplication
-> multi-source-correlation
-> evidence-package-builder
```

### 交通运行分析

```text
video-source-operations
-> object-tracking-analysis
-> scenario-event-recognition --scenario traffic
-> spatial-rule-engine
-> alert-deduplication
-> structured-video-query
```

### 公共安全事件分析

```text
video-source-operations
-> object-tracking-analysis
-> scenario-event-recognition --scenario safety
-> spatial-rule-engine
-> alert-deduplication
-> evidence-package-builder
```

### 指挥中心查询摘要

```text
structured-video-query
```

前提：输入必须是结构化事件、告警、关联 case 或指标结果。如果用户只给原始视频，应先走事件发现链路。

### 模型治理和误报回流

```text
alert-deduplication
-> model-ops-governance
```

## 旧 Skill 使用规则

旧 skill 不作为 Agent 首选入口，只作为兼容或增强节点：

- `camsnap`：被 `video-source-operations` 吸收，只有明确要求“抓拍快照”时直接调用。
- `ffmpeg-utils`、`imagemagick`、`skill-virtual-ptz`、`skill-plate-forensics`：作为 `evidence-package-builder` 的增强工具。
- `crowd-density-analysis`、`anomaly-behavior-detection`、`fire-smoke-detection`、`urban-violation-detector`、`full-factor-traffic-stats`：作为 `scenario-event-recognition` 的专项后端能力。
- `multi-camera-reid`、`digital-twin-mapping`：作为 `multi-source-correlation` 的空间轨迹增强能力。
- `semantic-video-search`：只在没有结构化索引时作为兜底，不应替代 `structured-video-query`。
- `video-surveillance-analysis`：不再作为主入口，保留为兼容旧演示。
