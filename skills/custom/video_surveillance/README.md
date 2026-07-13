# Custom Video Monitoring Skills

本目录是一组面向“监控视频分析”的 DeerFlow custom skills。当前架构已经收敛为：

```text
视频事件检测：single-video-event-analysis
目标检测：object-detection
视频/证据工具：抽帧、剪辑、打码、复核、去重
```

核心原则：

- **YOLO 只做目标检测**：输出 `label`、`confidence`、`bbox`，不判断打架、摔倒、车祸、拥堵、烟火、入侵等事件。
- **事件检测统一入口**：所有视频事件语义判断都走 `single-video-event-analysis`，先抽帧，再由 LLM 按时间顺序审查可见证据。
- **旧规则事件 skills 已移除**：不再使用按 ROI/轨迹/密度/组合规则硬判事件的 skill。

## Recommended Event Flow

```text
single-video-event-analysis
  -> extract timestamped frames
  -> build review_manifest.json
  -> LLM reviews frames chronologically
  -> visual_timeline + event_candidates
  -> evidence-snapshot / video-segment-extraction / privacy-masking
  -> human-review-routing
```

目标检测、跟踪和 ROI 匹配可以作为对象分析或辅助上下文使用，但不能作为事件结论来源：

```text
frame-sampling
  -> object-detection
  -> object-tracking
  -> roi-mapping
```

视频库检索和证据归档使用 Elasticsearch 作为后端：

```text
batch-video-ingestion
  -> video-search
  -> object-statistics
  -> evidence-package-generation
```

## Skill List

| Skill | Role |
| --- | --- |
| `single-video-event-analysis` | 统一的视频事件分析入口。输入本地视频，抽帧并生成 LLM 审帧 manifest，最终输出视觉时间线和事件候选。 |
| `analyze-video` | 视频抽帧和取证准备工具，使用 FFmpeg 做场景检测、粗采样、密集采样和 `metadata.json`。 |
| `video-stream-ingestion` | 规范化本地视频文件，输出 `raw_segment_uri` 和视频会话元数据。 |
| `frame-sampling` | 从视频片段或已有帧记录中生成标准 Frame schema。 |
| `camera-health-check` | 检查摄像头画面质量，例如黑屏、模糊、遮挡、冻结、不可用。 |
| `object-detection` | YOLO 目标检测，只输出人车物等对象的 Detection schema。 |
| `object-tracking` | 对检测结果做跨帧关联，输出 Track schema、轨迹和持续时间；不判断事件。 |
| `roi-mapping` | 将检测目标或轨迹匹配到摄像头 ROI 多边形；只做几何匹配。 |
| `duplicate-event-merge` | 合并重复事件候选，适合单视频或批量视频分析后的去重。 |
| `evidence-snapshot` | 为事件生成证据截图、哈希和 Evidence schema。 |
| `video-segment-extraction` | 按事件时间从原始视频中截取证据短片段。 |
| `privacy-masking` | 对证据图片中的人脸、车牌、手机号、门牌等敏感区域打码。 |
| `human-review-routing` | 判断事件是否需要进入人工复核队列。 |
| `ffmpeg-utils` | 提供底层视频剪辑、截帧等 FFmpeg 工具能力。 |
| `batch-video-ingestion` | 批量视频入库，将本地视频元数据、目标摘要和可检索文档写入 Elasticsearch。 |
| `video-search` | 从视频库按关键词、摄像头、时间、目标类别和可选预计算向量检索视频。 |
| `video-embedding-index` | 为视频库文档生成 embedding，并写入个人专属向量索引，默认 `huangxiao-video-library-vector-v1`。 |
| `object-statistics` | 基于视频库记录或检测/跟踪 JSON 统计目标数量、类别、摄像头、时间和运动状态。 |
| `evidence-package-generation` | 根据视频库记录、事件或检索结果生成证据包 manifest、截图和短视频片段。 |

## Data Objects

- `raw_segment_uri`：原始视频片段路径或视频源规范化结果。
- `frames`：抽样帧列表，包含 `frame_id`、`camera_id`、`timestamp`、`image_uri` 等。
- `review_manifest`：给 LLM 审帧使用的帧清单、目标事件和输出要求。
- `visual_timeline`：LLM 基于可见帧写出的时间线观察。
- `events`：LLM 基于可见证据生成的候选事件。
- `detections`：YOLO 帧级目标检测结果，只包含对象框和置信度。
- `tracks`：跨帧目标轨迹，供目标统计或检索使用。
- `roi_matches`：目标或轨迹与 ROI 的几何匹配结果。
- `evidence`：证据截图、哈希、隐私处理状态。
- `review`：人工复核分流结果。
- `video_library_document`：写入 Elasticsearch 的视频库文档，包含 `video_id`、`camera_id`、元数据、目标摘要和检索文本。
- `evidence_package`：证据包 manifest，包含 package_id、items、evidence artifacts 和哈希。

## Dependencies

### Minimal

```yaml
runtime:
  python: ">=3.10"

system_packages:
  - bash

python_packages:
  - PyYAML>=6.0
```

### Video Processing

`single-video-event-analysis`、`analyze-video`、`video-segment-extraction`、`ffmpeg-utils` 需要：

```yaml
system_packages:
  - bash
  - ffmpeg
```

### Image Processing

`frame-sampling`、`evidence-snapshot`、`privacy-masking` 需要 OpenCV：

```text
opencv-python-headless>=4.8
```

### Object Detection

`object-detection` 的真实 YOLO 后端需要：

```text
ultralytics>=8.0
```

`object-detection` 不再提供 mock 模式；链路测试也必须使用 `--provider ultralytics` 和真实帧图片。

### Video Library Elasticsearch

`batch-video-ingestion`、`video-search`、`object-statistics` 和 `evidence-package-generation` 通过标准库 HTTP 访问 Elasticsearch，不新增 Python 依赖。运行环境需要提供：

```bash
ES_URL=http://localhost:3128
ES_USERNAME=citybrain-street
ES_PASSWORD=123456
```

默认普通视频库索引名为 `citybrain-video-library`。个人向量索引建议使用 `huangxiao-video-library-vector-v1`，避免影响共享数据。`video-embedding-index` 可从普通视频库读取文档，生成向量后写入个人向量索引。

StreetModel 视频级向量接入使用 `Qwen3-VL-Embedding-2B`，默认写入 2048 维字段 `video_vector-Qwen3-VL-Embedding-2B_urban_governance`。视频文档中的本地路径必须能映射到 GPU 节点可访问的共享目录，默认从 `/data/deerflow/videos` 映射到 `/nfsdat2/home/xhuangslm/shared_videos`。

## Example Commands

统一事件分析准备：

```bash
python skills/custom/single-video-event-analysis/scripts/run.py \
  --input skills/custom/examples/single_video_analysis_input.json \
  --config skills/custom/configs/deerflow_config.json \
  --output /mnt/data/video-monitoring-runs/run_001/prepared_manifest.json
```

脚本会生成 `review_manifest.json`。Agent/LLM 必须查看 manifest 中的帧图，再输出 `visual_timeline` 和 `events`。

单独运行 YOLO 目标检测：

```bash
python skills/custom/object-detection/scripts/run.py \
  --frames-json /mnt/data/video-monitoring-runs/run_001/frames.json \
  --labels person,car,bus,truck,motorcycle,bicycle \
  --provider ultralytics \
  --config skills/custom/configs/deerflow_config.json \
  --output /mnt/data/video-monitoring-runs/run_001/detections.json
```

生成 StreetModel 视频级向量并写入个人 ES 索引：

```bash
python skills/custom/video-embedding-index/scripts/run.py \
  --source-index citybrain-video-library \
  --target-index huangxiao-video-library-vector-v1 \
  --owner huangxiao \
  --embedding-provider streetmodel \
  --config config.yaml \
  --output /tmp/video_embedding_index.json
```

直接对一个 StreetModel 可访问的视频路径生成向量并写入个人 ES 索引：

```bash
python skills/custom/video-embedding-index/scripts/run.py \
  --embedding-provider streetmodel \
  --video-id camera01_0001 \
  --video-uri /nfsdat2/home/xhuangslm/shared_videos/camera01/0001.mp4 \
  --video-vector-output /tmp/camera01_0001_video_vector.json \
  --config config.yaml \
  --output /tmp/camera01_0001_video_embedding.json
```

生成文本 query 向量并检索已入库视频向量：

```bash
python skills/custom/video-embedding-index/scripts/run.py \
  --embedding-provider streetmodel \
  --query "夜晚路口有很多车辆经过" \
  --query-vector-output /tmp/video_query_vector.json \
  --config config.yaml \
  --output /tmp/video_query_embedding.json

python skills/custom/video-search/scripts/run.py \
  --index huangxiao-video-library-vector-v1 \
  --query "夜晚路口有很多车辆经过" \
  --query-vector-json /tmp/video_query_vector.json \
  --config config.yaml \
  --output /tmp/video_search.json
```

## Removed Event Rule Skills

旧的规则事件类 skills 已删除，不再作为路由目标或链路组件。如果需要判断事件，请使用 `single-video-event-analysis`。
