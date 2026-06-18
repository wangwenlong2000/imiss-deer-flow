# 已测试视频样例

这份文件只记录已有测试产物支撑过的样例，方便后续复测和排查。

## 样例总览

| 样例 | 路径 | 已测场景 | 需要数据 | 依赖/MCP/模型要求 | 关键结论 | 主要产物 |
| --- | --- | --- | --- | --- | --- | --- |
| 交通监控 `Trafic.mp4` | `datasets/Vedio-demo/Trafic.mp4`；sandbox: `/mnt/datasets/Vedio-demo/Trafic.mp4` | 视频规范化、抽帧、FFmpeg 截帧/剪辑、摄像头健康、证据截图、隐私打码、视频入库、搜索、统计、向量索引基础链路 | `video_path`、`camera_id=CAM_DEERFLOW_001`、可选 manifest、ES 索引 | `ffmpeg/ffprobe`、OpenCV、YOLO、ES；StreetModel 视频 embedding 需要模型服务和共享目录 | 多条普通视频处理和 ES 链路通过；StreetModel 直接视频 embedding 曾因模型侧读不到本地路径失败 | `outputs/skill-tests/20260526-123218`、`outputs/skill-tests/20260602-001804`、`outputs/skill-tests/20260602-002415`、`outputs/skill-tests/20260601-173823-streetmodel-retry` |
| 打架视频 `fight.mp4` | `datasets/Vedio-demo/fight.mp4`；sandbox: `/mnt/datasets/Vedio-demo/fight.mp4` | 单视频事件分析 | `video_path`、`camera_id=CAM_DEERFLOW_001`、`target_events=fight,fall_down,other_abnormal_event`、输出目录 | `ffmpeg/ffprobe`；需要支持视觉的 LLM；不需要 MCP | 检出 `fight`：约 `0:00:30` 到 `0:00:55`，置信度 `0.95`，严重程度 `high`，`requires_review=true` | `outputs/skill-tests/20260526-123218/single-video-event-analysis` |
| ShanghaiTech `07_007.avi` | `datasets/shanghaitech-agent-eval/07_007.avi`；sandbox: `/mnt/datasets/shanghaitech-agent-eval/07_007.avi` | ES 入库、视频搜索、目标统计、证据包、deterministic 向量索引、StreetModel 文本 query embedding、StreetModel 视频 embedding 失败路径 | manifest、`camera_id=CAM_SHANGHAITECH_07_007`、`video_id=shanghaitech-07-007-agent-test`、ES 源/向量索引 | `ffmpeg/ffprobe`、OpenCV、YOLO、ES；StreetModel 文本向量需要模型服务；视频向量还需要共享目录/MCP | Agent 链路 `7/7` 通过，功能 `6/7` 成功；检测到 `person`，`total_objects=4`，`total_tracks=2`；视频 embedding 主要卡在共享目录/模型侧路径可见性 | `outputs/skill-tests/20260602-010655-shanghaitech-es`、`outputs/skill-tests/20260602-011116-shanghaitech-es`、`outputs/skill-tests/20260602-141734-shanghaitech-es` |

## 依赖清单

| 依赖类别 | 需要安装/准备 | 用到的样例/场景 | 是否 MCP/模型相关 |
| --- | --- | --- | --- |
| Python 基础 | Python 3.10+，`PyYAML>=6.0` | 所有 custom skill | 否 |
| 视频处理 | `ffmpeg`、`ffprobe` | `Trafic.mp4` 抽帧/剪辑/规范化，`fight.mp4` 事件抽帧，`07_007.avi` 入库和证据包 | 否；StreetModel frame-proxy 也需要 |
| OpenCV | `opencv-python` 或 `opencv-python-headless` | `Trafic.mp4` 抽帧、证据截图、隐私打码；`07_007.avi` 入库对象检测前后处理 | 否 |
| YOLO | `ultralytics`，`skills/custom/models/yolov8n.pt` | `Trafic.mp4` 和 `07_007.avi` 的 `object_detection` 入库/目标摘要 | 否 |
| Elasticsearch | `ES_URL`、`ES_USERNAME`、`ES_PASSWORD` 或配置文件 | `Trafic.mp4`、`07_007.avi` 的入库、搜索、统计、证据包、向量索引 | 否；语义向量检索也依赖 ES |
| 视觉 LLM | DeerFlow 模型需 `supports_vision: true`，Agent 能查看抽帧图片 | `fight.mp4` 的最终事件判断；后续 `Trafic.mp4` 事件分析也需要 | 不是 MCP，但属于模型能力 |
| StreetModel 模型服务 | `Qwen3-VL-Embedding-2B`、`streetmodel_embedding.base_url`、2048 维向量字段 | `07_007.avi` 文本 query embedding 成功；`Trafic.mp4`/`07_007.avi` 视频 embedding 风险验证 | 是，模型服务相关 |
| StreetModel MCP | `streetmodel-video-embedding`，工具 `streetmodel_health`、`streetmodel_embed_video`、`streetmodel_embed_query` | 视频向量和文本 query 向量推荐优先走 MCP | 是，MCP 相关 |
| 共享视频目录 | `deerflow_path_prefix` 与 `streetmodel_path_prefix` 同时可访问或可映射 | StreetModel 视频 embedding | 是，视频向量必需 |

仓库内 custom skill 依赖：

```bash
pip install -r skills/custom/requirements.txt
```

包含：

```text
opencv-python
ultralytics
PyYAML>=6.0
sentence-transformers
```

系统包：

```bash
ffmpeg
ffprobe
```

## MCP/模型专项标注

| 样例/任务 | MCP 工具 | 模型/服务 | 当前测试状态 | 额外条件 |
| --- | --- | --- | --- | --- |
| `fight.mp4` 事件分析 | 不需要 | 视觉 LLM | 已成功检出 `fight` | 需要读取抽帧图片，不能只凭文件名判断 |
| `Trafic.mp4` 普通入库/搜索 | 不需要 | ES、YOLO | 已有成功产物 | `object_detection` 模式需要 YOLO；`metadata_only` 不需要 YOLO |
| `Trafic.mp4` StreetModel 视频向量 | 可用 `streetmodel_embed_video` | StreetModel/Qwen3-VL | 已触达失败路径 | 视频路径必须对 GPU/StreetModel 节点可见 |
| `07_007.avi` ES 真链路 | 不需要 | ES、YOLO | `batch/search/statistics/package` 成功 | 使用个人测试索引 `huangxiao-shanghaitech-es-agent-test` |
| `07_007.avi` StreetModel 文本 query embedding | 可用 `streetmodel_embed_query` | `Qwen3-VL-Embedding-2B` | 成功，2048 维 | 可将 query vector 给 `video-search` |
| `07_007.avi` StreetModel 视频 embedding | 可用 `streetmodel_embed_video` | `Qwen3-VL-Embedding-2B` | Agent 链路通过，功能失败 | 需要修复 `/data/deerflow/videos` 权限或准备共享路径 |

## 详细记录

### `Trafic.mp4`

基础视频工具链：

- `video-stream-ingestion`：成功读取本地视频，输出 `raw_segment_uri=/mnt/datasets/Vedio-demo/Trafic.mp4`。
- `analyze-video`、`frame-sampling`：成功抽帧/生成审查素材。
- `ffmpeg-utils`、`video-segment-extraction`：成功截帧和生成片段。
- `camera-health-check`、`evidence-snapshot`、`privacy-masking`：成功跑通质量、截图、打码链路。

视频库链路：

- `batch-video-ingestion`：以 `video_id=video-traffic-agent-test` 入库成功。
- `video-search`：按 `Trafic`、`CAM_DEERFLOW_001` 命中记录。
- `object-statistics`：普通统计链路通过。
- `video-embedding-index` deterministic/hash 类基础向量写入通过。

失败/风险记录：

- StreetModel 直接视频 embedding 失败过，错误为模型服务侧找不到视频文件。原因不是 skill 没调用到，而是传给 StreetModel 的路径不是 GPU/模型节点可见路径。

### `fight.mp4`

已测 `single-video-event-analysis`：

- 视频信息：`duration_seconds=254.82`，`fps=30.0`，分辨率 `1728x1080`。
- 时间线关键帧：
  - `0:00:30`：出现骚动和人员聚集。
  - `0:00:40`：明显肢体冲突，有人被推倒。
  - `0:00:50`：多人参与打斗，桌椅翻倒，地面散落物品。
  - `0:00:55`：冲突持续。
- 事件结果：
  - `event_type=fight`
  - `start_time=0:00:30.000`
  - `end_time=0:00:55.000`
  - `confidence=0.95`
  - `severity=high`
  - `requires_review=true`

### `07_007.avi`

样例元数据：

- 来源：`/home/huangxiao/City_brain/shanghaitech/shanghaitech/training/videos/07_007.avi`
- 仓库内副本：`datasets/shanghaitech-agent-eval/07_007.avi`
- sandbox 路径：`/mnt/datasets/shanghaitech-agent-eval/07_007.avi`
- SHA256：`6c7f67d1353ce7d6b914b4ff0809397c9c9348cab376e868a39f726f2220cba1`
- 大小：`805556` bytes

已测结果：

- `batch-video-ingestion-shanghaitech`：成功入库 1 条，`video_id=shanghaitech-07-007-agent-test`，检测到 `person`。
- `video-search-shanghaitech`：成功命中 1 条，`query_mode=keyword_filter`。
- `object-statistics-shanghaitech`：`total_objects=4`，`total_tracks=2`，`person=4`。
- `evidence-package-generation-shanghaitech`：成功生成 package manifest、snapshot、clip。
- `video-embedding-index-deterministic-shanghaitech`：成功写入 1 条 1024 维 deterministic/hash 向量。
- `video-embedding-index-streetmodel-query`：成功生成 `Qwen3-VL-Embedding-2B` 文本 query embedding，维度 `2048`。
- `video-embedding-index-streetmodel-video`：Agent 链路通过，但功能失败；主要错误是 `VIDEO_COPY_TO_SHARED_FAILED` 或 StreetModel/GPU 节点不可见复制后的视频路径。

## 可直接复测的问题

```text
请用 single-video-event-analysis 分析 /mnt/datasets/Vedio-demo/fight.mp4 是否有打架、摔倒或其他异常事件，并输出时间线、事件候选、证据帧和是否需要人工复核。
```

```text
请把 /mnt/datasets/Vedio-demo/Trafic.mp4 以 camera_id=CAM_DEERFLOW_001 入库到 citybrain-video-library，然后按 Trafic 和 person/car 进行检索。
```

```text
请用 /mnt/datasets/shanghaitech-agent-eval/07_007.avi 测试 batch-video-ingestion、video-search、object-statistics 和 evidence-package-generation 的真链路。
```

```text
请对 ShanghaiTech 07_007 样例生成 StreetModel 文本 query embedding，并说明是否能进行视频 embedding；如果失败，请报告共享目录或路径映射问题。
```
