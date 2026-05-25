# Custom Video Monitoring Skills

本目录是一组面向“监控视频分析”的自定义 skills。它们不是一个单体技能，而是一组可独立路由、独立执行、可按链路组合的原子能力。

每个 skill 目录通常包含：

- `SKILL.md`：给 Agent 使用的技能说明和执行约束。
- `router_card.json`：给 SkillRouter 使用的独立路由卡。
- `scripts/run.py` 或专用脚本：该 skill 的实际执行入口。

## 总体作用

这组 skills 用于从已存在的视频文件开始，完成抽帧、LLM 视觉分析、可选的目标检测/跟踪/ROI 辅助、事件候选生成、去重、证据生成、隐私打码和人工复核分流。

事件检测的默认链路应优先走抽帧视觉分析，而不是直接依赖目标检测框或规则脚本：

```text
analyze-video 或 frame-sampling
  -> LLM 按时间顺序查看抽帧图片
  -> 生成 visual_observations / visual_timeline
  -> spatial-occupancy-event / temporal-persistence-event / density-aggregation-event / object-composition-event
  -> event-template-mapping
  -> duplicate-event-merge
  -> evidence-snapshot
  -> privacy-masking
  -> video-segment-extraction
  -> human-review-routing
```

已有可靠结构化检测结果时，仍可以使用规则/几何辅助链路：

```text
frame-sampling
  -> camera-health-check
  -> object-detection
  -> object-tracking
  -> roi-mapping
  -> spatial-occupancy-event / temporal-persistence-event / density-aggregation-event / object-composition-event
  -> event-template-mapping
  -> event-rule-engine
  -> duplicate-event-merge
  -> evidence-snapshot
  -> privacy-masking
  -> video-segment-extraction
  -> human-review-routing
```

`analyze-video` 是偏取证回溯和事件检测的高层视频分析 skill，适合对本地视频做场景切分、关键帧提取、逐帧视觉审核和时间线分析。对打架、车祸、摔倒、聚集、拥堵、烟火、入侵、占道等事件，Agent 应先查看抽帧图片，再决定事件类型、时间范围、证据帧和置信度。其他 skills 可作为结构化流水线或辅助判断使用。

## Skill 列表

| Skill | 作用 |
| --- | --- |
| `analyze-video` | 对本地视频做场景检测、关键帧提取、章节切分、逐帧视觉审核、事件时间线和取证回溯。 |
| `video-stream-ingestion` | 仅规范化本地视频文件，输出 `raw_segment_uri` 和视频会话元数据；不处理实时视频流。 |
| `frame-sampling` | 从视频片段或已有帧记录中生成标准 Frame schema。 |
| `camera-health-check` | 检查摄像头画面质量，例如黑屏、模糊、遮挡、冻结、不可用。 |
| `object-detection` | 对抽样帧做人车物等目标检测，输出 Detection schema。 |
| `object-tracking` | 对检测结果做跨帧关联，输出 Track schema、轨迹和持续时间。 |
| `roi-mapping` | 将检测目标或轨迹匹配到摄像头 ROI 多边形。 |
| `spatial-occupancy-event` | 基于抽帧视觉证据判断目标是否占用指定 ROI 区域，可选使用 ROI 几何结果辅助。 |
| `temporal-persistence-event` | 基于抽帧时间线判断目标、动作或状态是否持续足够长时间。 |
| `density-aggregation-event` | 基于抽帧视觉证据判断是否存在数量过多、密度过高、排队、拥堵或聚集。 |
| `object-composition-event` | 基于抽帧视觉证据判断多目标组合关系，例如接触、碰撞、人车物组合、距离关系、分组关系。 |
| `event-template-mapping` | 将视觉观察或方法级结果映射为最终 Event Candidate 语义。 |
| `event-rule-engine` | 编排抽帧视觉分析和事件模板映射；已有结构化输入时也可执行规则引擎。 |
| `duplicate-event-merge` | 合并同摄像头、同类型、同 ROI 或时间窗口重叠的重复事件。 |
| `evidence-snapshot` | 为事件生成证据截图、哈希和 Evidence schema。 |
| `video-segment-extraction` | 按事件时间从原始视频中截取证据短片段。 |
| `privacy-masking` | 对证据图片中的人脸、车牌、手机号、门牌等敏感区域打码。 |
| `human-review-routing` | 判断事件是否需要进入人工复核队列。 |
| `ffmpeg-utils` | 提供底层视频剪辑、截帧等 FFmpeg 工具能力。 |

## 实现原则

这些 skills 采用“原子脚本 + 结构化 JSON”的方式实现：

1. 每个 skill 优先运行自己的脚本入口。
2. 脚本通过命令行参数读取输入 JSON、配置文件和输出路径。
3. 输出统一为 JSON，包含 `skill`、`version`、`status`、`confidence` 和 `data`。
4. 原子 skill 不直接依赖共享 registry，也不隐式调用其他 skill。
5. 上游缺失时，应由 Agent 按 `SKILL.md` 中的链路显式调用上游 skill。
6. 路由层按每个 skill 的 `router_card.json` 单独检索，不把整套链路当成一张卡。

这种设计的目的：

- 方便单独测试每个环节。
- 方便 SkillRouter 精准命中具体能力。
- 避免一个大 skill 把所有视频任务都吸走。
- 让中间产物可审计、可复用、可回放。

## 路由卡说明

每个 skill 都应该有自己的 `router_card.json`：

```text
skills/custom/<skill-name>/router_card.json
```

路由卡只描述当前 skill 自己：

- `identity`：skill id、名称和描述。
- `scope`：适用场景、任务类型、输入输出类型。
- `routing`：正向触发词、负向触发词、关键词和路由文本。
- `body`：对应 `SKILL.md` 内容。
- `execution`：需要的工具和允许的文件类型。
- `routing_policy`：优先级、冲突组和让渡条件。
- `source` / `embedding`：来源、hash 和索引元数据。

不要把所有视频 skills 写进一张路由卡。正确做法是每个 skill 一张卡，入库后由 SkillRouter 在候选集中做 top-k 检索。

## 数据流

核心数据对象：

- `raw_segment_uri`：原始视频片段路径或视频源规范化结果。
- `frames`：抽样帧列表，包含 `frame_id`、`camera_id`、`timestamp`、`image_uri` 等。
- `detections`：帧级目标检测结果，包含 label、confidence、bbox。
- `tracks`：跨帧目标轨迹，包含 track id、持续时间、最后位置、运动状态。
- `roi_matches`：目标或轨迹与 ROI 的匹配结果。
- `method_outputs`：空间、时间、密度、多目标组合等方法级事件结果。
- `events`：候选事件。
- `evidence`：证据截图、哈希、隐私处理状态。
- `review`：人工复核分流结果。

## Sandbox 依赖配置

### 最小依赖

如果只运行不涉及视频读写、图像处理、真实检测的 JSON 规则类 skills，最小依赖是：

```yaml
runtime:
  python: ">=3.10"

system_packages:
  - bash

python_packages:
  - PyYAML
```

`PyYAML` 是可选依赖。只有读取 `.yaml` / `.yml` 配置时才需要；如果所有输入配置都是 JSON，可以不装。

### 视频处理依赖

运行 `analyze-video`、`video-segment-extraction`、`ffmpeg-utils`，以及需要探测本地视频元数据的规范化入口时建议配置：

```yaml
system_packages:
  - bash
  - ffmpeg
```

其中 `ffprobe` 通常随 `ffmpeg` 一起安装，用于读取视频宽高、时长、码流等元数据。

### 图像处理依赖

运行 `frame-sampling`、`evidence-snapshot`、`privacy-masking` 的真实文件处理能力时需要 OpenCV：

```text
opencv-python>=4.8
```

如果 sandbox 是无 GUI 环境，通常可以使用：

```text
opencv-python-headless>=4.8
```

### 目标检测依赖

`object-detection` 支持 mock 后端和真实 YOLO 后端。真实 YOLO 后端需要：

```text
ultralytics>=8.0
```

`ultralytics` 会带入 PyTorch 相关依赖，sandbox 镜像需要考虑模型下载、CPU/GPU、缓存目录和网络权限。如果只是做链路测试，可以使用 mock provider，不需要安装 `ultralytics`。

### 推荐 requirements

当前目录已有：

```text
opencv-python
ultralytics
```

建议补充：

```text
PyYAML>=6.0
```

如果使用无 GUI sandbox，可以把 `opencv-python` 换成：

```text
opencv-python-headless
```

## 推荐 Sandbox 示例

偏完整的视频监控分析环境：

```yaml
runtime:
  python: ">=3.10"

system_packages:
  - bash
  - ffmpeg

python_packages:
  - PyYAML>=6.0
  - opencv-python-headless>=4.8
  - ultralytics>=8.0
```

偏轻量的规则和后处理环境：

```yaml
runtime:
  python: ">=3.10"

system_packages:
  - bash

python_packages:
  - PyYAML>=6.0
```

## 运行示例

所有 `scripts/run.py` 都支持统一的 JSON 入口：

```bash
python skills/custom/<skill-name>/scripts/run.py \
  --input /mnt/user-data/inputs/<skill-input>.json \
  --config skills/custom/configs/deerflow_config.json \
  --output /mnt/user-data/outputs/<skill-result>.json
```

命令行上的细粒度参数会覆盖 `--input` JSON 中的同名字段。这样 Agent 可以稳定地按路由卡调用已有脚本，而不需要在 skill 失败后临时拼装新脚本。

抽帧并做 LLM 视觉事件分析的推荐起点：

```bash
python skills/custom/analyze-video/scripts/extract_frames.py \
  /mnt/user-data/uploads/input.mp4 \
  --output-dir /mnt/user-data/outputs/video_review
```

也可以用 JSON 调用：

```bash
python skills/custom/analyze-video/scripts/extract_frames.py \
  --input /mnt/user-data/inputs/analyze_video_input.json
```

执行后读取 `/mnt/user-data/outputs/video_review/metadata.json`，再按时间顺序查看输出帧图片。事件检测报告应基于可见帧证据给出 `event_type`、`time_range`、`evidence_frame_ids`、`reason`、`confidence` 和 `requires_review`。

单独运行目标检测：

```bash
python skills/custom/object-detection/scripts/run.py \
  --frames-json /mnt/user-data/outputs/frames.json \
  --labels person,car \
  --provider mock \
  --output /mnt/user-data/outputs/detections.json
```

单独运行 ROI 匹配：

```bash
python skills/custom/roi-mapping/scripts/run.py \
  --tracks-json /mnt/user-data/outputs/tracks.json \
  --camera-id CAM_DEERFLOW_001 \
  --config skills/custom/configs/deerflow_config.json \
  --output /mnt/user-data/outputs/roi_matches.json
```

已有可靠 `frames`、`detections`、`tracks`、`roi_matches` 时，单独运行事件规则引擎：

```bash
python skills/custom/event-rule-engine/scripts/run.py \
  --frames-json /mnt/user-data/outputs/frames.json \
  --detections-json /mnt/user-data/outputs/detections.json \
  --tracks-json /mnt/user-data/outputs/tracks.json \
  --roi-matches-json /mnt/user-data/outputs/roi_matches.json \
  --camera-id CAM_DEERFLOW_001 \
  --config skills/custom/configs/deerflow_config.json \
  --output /mnt/user-data/outputs/events.json
```

## 开发约束

- 不要在 Agent 推理中直接读取完整视频或大文件内容。
- 不要绕过已有脚本手写一套同功能逻辑。
- 不要让方法级 skill 直接生成最终业务事件或证据。
- 不要让证据类 skill 反向执行检测、跟踪或事件规则。
- 修改某个 skill 时，同步检查它的 `SKILL.md`、`router_card.json` 和脚本参数是否一致。
- 新增 skill 时，应新增对应目录、`SKILL.md`、脚本入口和独立 `router_card.json`。

## 常见问题

`ffprobe` 找不到：

安装 `ffmpeg`，并确认 `ffmpeg` 和 `ffprobe` 都在 PATH 中。

`PyYAML is required to read YAML files`：

安装 `PyYAML`，或者把配置改成 JSON。

OpenCV 读取图片或视频失败：

确认输入路径在 sandbox 内可访问；无 GUI 环境优先使用 `opencv-python-headless`。

YOLO 依赖缺失：

使用 mock provider 做链路测试，或在 sandbox 中安装 `ultralytics` 并准备模型文件/缓存。

路由命中不准：

检查对应 skill 的 `router_card.json`，重点看 `routing_text`、`positive_triggers`、`negative_triggers`、`keywords` 和 `anti_keywords`。
