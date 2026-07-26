# 视频监控 skill 测试报告（2026-07-26，StreetModel 旁路）

本报告按 [`deerflow-agent-skill-test.md`](deerflow-agent-skill-test.md) 的三层结构组织，
在 **StreetModel 视频向量服务未启用** 的前提下开展测试。

复现命令：

```bash
cd /home/huangxiao/City_brain/imiss-deer-flow-qwen36-35b-a3b-test

# Layer A + Layer B
python3 scripts/test_video_surveillance_skills.py --layer all

# 只跑编排与契约（宿主机，仅标准库）
python3 scripts/test_video_surveillance_skills.py --layer plan

# 只跑沙箱内真实视频执行
python3 scripts/test_video_surveillance_skills.py --layer exec --video Vedio-demo/fight.mp4

# Layer C（Agent 端到端）
python3 scripts/run_video_surveillance_qwen35b_test.py \
  --timeout 420 \
  --dataset-root /home/huangxiao/City_brain/imiss-deer-flow-qwen36-35b-a3b-test/datasets \
  --cases C-001 C-002 C-011 C-038 C-040
```

产物目录：`outputs/skill-tests/video-surveillance-suite/<时间戳>/`，每次运行生成 `REPORT.md`、
`summary.json` 与全部中间产物（`plan/` 与 `sandbox/work/`）。修复后的最终一轮为
[`20260726-175254`](../outputs/skill-tests/video-surveillance-suite/20260726-175254)。

## 1. 环境检查结果

| 项目 | 实际值 | 是否满足测试前置 |
| --- | --- | --- |
| 服务栈 | `qwen36test-deer-flow-{nginx,gateway,langgraph,frontend}`，已运行 2 天 | 是 |
| LangGraph API | `http://127.0.0.1:3538/api/langgraph` → HTTP 200 | 是 |
| 模型 | `qwen3.6-plus` / `qwen3.6-35b-a3b`（dashscope 兼容接口） | 是 |
| 沙箱 | `AioSandboxProvider`，镜像 `huangxiao-deerflow-sandbox:network-tools` | 是 |
| 视频挂载 | `DEER_FLOW_HOST_DATASETS_PATH=.../imiss-deer-flow-qwen36-35b-a3b-test/datasets` → `/mnt/datasets`（只读） | 是 |
| 测试视频 | `/mnt/datasets/Vedio-demo/Trafic.mp4`（900.9s / 1280x674 / 12fps / h264）、`fight.mp4` | 是 |
| 沙箱内 ffmpeg/ffprobe | `/usr/bin/ffmpeg`、`/usr/bin/ffprobe` | 是 |
| 沙箱内 OpenCV | `cv2 5.0.0` | 是 |
| 沙箱内 Ultralytics | `ultralytics 8.4.103` + `torch 2.13.0+cu130`，`numpy 2.2.6` | 是 |
| YOLO 权重 | `skills/custom/models/yolov8n.pt`（配置引用的路径）与 bundle 内副本均存在 | 是 |
| **StreetModel** | `http://219.245.185.245:3130` **不可达**（RemoteDisconnected） | **否，本次旁路** |
| **Elasticsearch** | `http://172.17.0.1:3128` **不可用**（HTTP 502 / 超时） | **否，本次旁路** |
| SkillRouter | `config.yaml: skill_router.enabled=false`（走场景过滤器注入，不走向量路由） | 关闭，符合当前部署 |
| 宿主机 Python | 3.10.12，**没有** cv2/numpy/ultralytics/ffmpeg | 仅能跑纯 Python 契约测试 |

两点与旧文档不同，需要记录：

1. **宿主机不能作为执行环境**。文档 §4 的"仓库根目录执行"只对纯 Python 的编排/契约脚本成立；
   所有涉及抽帧、检测、ffmpeg 的 skill 必须在沙箱镜像内运行。本套件的 Layer B 因此改为
   `docker run` 进沙箱镜像执行，挂载 `skills → /mnt/skills`、`datasets → /mnt/datasets`。
2. **文档 §7.1 的 registry 不同步问题已修复**。当前 `skills/registry.json` 中 `video_surveillance`
   条目为 21 个，与 bundle 中 21 个 Router Card 完全一致；不再缺少 `city-video-intelligence` 等条目，
   也不再保留 `event-rule-engine` 等旧规则条目；`skill_md_path` 全部指向
   `skills/custom/video_surveillance/<skill-id>/SKILL.md` 且文件存在。该项已作为回归用例 A12 固化。

### 因 StreetModel / ES 未启用而旁路的范围

| skill | 旁路方式 |
| --- | --- |
| `video-search` | 只验证业务编排是否推荐它（A2/A4），不执行检索 |
| `video-embedding-index` | 不执行 |
| `object-statistics` | 不执行（依赖 ES 索引） |
| Agent 用例 `C-028`（跨摄像头检索） | 从 Layer C 批次中剔除 |

## 2. Layer A：业务编排与契约（19 通过 / 1 跳过）

宿主机执行，仅依赖标准库。下表为修复后的最终结果；`A1b/A5/A5c/A5d` 在修复前为失败，见 §4。

| 用例 | 内容 | 结果 |
| --- | --- | --- |
| A1 | 单视频事件请求 → `capability=single_video_event_understanding`、`recommended_skill_chain=["single-video-event-analysis"]`、`requires_human_review=true` | 通过 |
| A2 | 交警调阅 → `business_scenario=traffic_police_video_review`、`capability=video_asset_retrieval`、链路只含 `video-search`，未误入事件分析 | 通过 |
| A3 | 证据包缺输入 → 只返回一个合并追问"请问视频文件路径和事件时间或片段范围是什么？" | 通过 |
| A4 | 语义检索请求 → `capability=semantic_video_retrieval`、推荐 `video-search`（仅计划，执行旁路） | 通过 |
| A5 | 越界能力请求 → `capability_gap` 覆盖身份识别/车牌 OCR/热力图/实时流 4 项，且不再误入摄像头健康运维 | 通过（修复后） |
| A5b | 真实画面质量问题（画面模糊、遮挡、黑屏）仍路由到摄像头健康运维 | 通过（回归） |
| A1b | 「我上传了一段监控 + 画面质量 + 事件」复合请求 → 健康检查与 `single-video-event-analysis` 同时入链，且不触发向量索引 | 通过（修复后） |
| A1c | 真正的批量入库请求仍走 `video_library_governance` | 通过（回归） |
| A5c | 视频元数据请求 → `video_metadata_normalization` → `video-stream-ingestion` | 通过（修复后） |
| A5d | 单视频计数 → `video-object-analytics`；视频库统计 → `object-statistics` | 通过（修复后） |
| A6 | 不存在的本地视频 → `status=failed`、`error_code=SOURCE_NOT_FOUND`、退出码 1、不伪造元数据 | 通过 |
| A7 | 事件去重空输入 → 标准成功 JSON | 通过 |
| A8 | 同摄像头同 ROI 重复事件 → 合并为 2 条，置信度取 0.9，时间取并集，`duplicates` 正确 | 通过 |
| A9 | ROI 几何 → 多边形内命中、多边形外不命中 | 通过 |
| A10 | 人工复核 → 低置信+画面退化进 `manual_review` 并给出两条 reason；高置信 `auto_pass` | 通过 |
| A11 | 117 个 Router Card 均可解析、ID 唯一、identity/scope/routing 齐备 | 通过 |
| A12 | registry 与视频 bundle 的路径与 scope 一致性（文档 §7.1 回归） | 通过 |
| A13 | `check_skill_router_conflicts.py --no-embedding` → `conflicts=[]` | 通过 |
| A13b | 视频 bundle 完整性：21 个 skill 均有 SKILL.md、SKILL.md 引用的脚本存在、全部脚本可编译 | 通过 |
| A14 | 在线检索类 skill 执行 | 跳过（StreetModel/ES 未启用） |

## 3. Layer B：沙箱内真实视频执行（17 通过 / 0 失败）

在 `huangxiao-deerflow-sandbox:network-tools` 内针对 `Trafic.mp4` 执行；`fight.mp4` 复跑结果完全一致。

| 用例 | 内容 | 结果 |
| --- | --- | --- |
| B0 | 依赖自检：ffmpeg/ffprobe/cv2/ultralytics/torch/视频/权重 | 通过 |
| B1 | `video-stream-ingestion` → `file_status=ok`，返回完整时长 900.916s、1280x674、12fps、h264、10811 帧、文件大小 | 通过（字段补齐后） |
| B2 | `frame-sampling` → 6 帧，磁盘图片文件与 JSON 一一对应 | 通过 |
| B3 | `object-detection` → 6 帧共检出 60+ 个目标，类别限定在申请的 `person/car/bus/truck/...` 之内，bbox 坐标与置信度合法 | 通过 |
| B4 | `object-tracking` → 生成轨迹，且结果中不含任何事件语义词（打架/拥堵/入侵/摔倒） | 通过 |
| B5 | `roi-mapping` → 配置中的全画面 ROI 命中真实轨迹 | 通过 |
| B6 | `camera-health-check` → `health_status=ok`、`health_score=100` | 通过 |
| B7 | `single-video-event-analysis` → 生成 `review_manifest.json`，`schema=single_video_event_review_manifest`，60 帧中挑出 8 帧审帧，附 `review_instructions` 与 `output_schema` | 通过 |
| B8 | `evidence-snapshot` → 生成真实 jpg 与 `sha256:` 哈希 | 通过 |
| B9 | `video-segment-extraction` → 生成 mp4，ffprobe 实测时长 4.0s（前后各 2s） | 通过 |
| B10 | `privacy-masking` → 生成 `_masked.jpg`，`method=gaussian_blur`，字节与原图不同 | 通过 |
| B11 | 证据事件（置信度 0.62）→ `queue=manual_review`、`reasons=[confidence_below_threshold]` | 通过 |
| B12 | `ffmpeg-utils --operation keyframe` → 按时间戳导出关键帧 | 通过 |
| B13 | `object-detection` 输入文件缺失 → `INPUT_NOT_FOUND`，不伪造检测结果 | 通过（修复后） |
| B14 | `video-segment-extraction` 缺事件时间 → `MISSING_EVENT` 且不产出文件 | 通过 |
| B15 | 16 个 skill 的输入缺失失败契约横向扫描 | 通过（修复后） |
| B16 | 输入为非法 JSON → `INPUT_INVALID_JSON`、退出码 1 | 通过（修复后） |

链路验证结论：文档 §1.1 的两条推荐链路在真实视频上均可跑通——
`frame-sampling → object-detection → object-tracking → roi-mapping` 与
`single-video-event-analysis → evidence-snapshot / video-segment-extraction → privacy-masking → human-review-routing`。
目标检测/跟踪的输出中不含事件结论，符合"检测只作为对象上下文"的边界要求。

## 4. 发现的问题与修复

所有修复都限定在 `skills/custom/video_surveillance/` 内，并各自配套了回归用例。

### 4.1 输入文件缺失/损坏时不返回标准失败 JSON（16 个 skill，已修复）

B13/B15 首轮稳定复现：当 `--*-json` / `--config` 指向不存在的文件时，脚本直接抛
`FileNotFoundError` traceback，退出码 1，stdout 无任何 JSON。根因是各脚本共用的
`load_structured()` 直接 `Path(path).read_text()`，没有存在性检查：

```python
def load_structured(path: str | None) -> Any:
    if not path:
        return {}
    source = Path(path)
    text = source.read_text(encoding="utf-8")   # <- 文件不存在时抛 FileNotFoundError
```

影响：与 `video-stream-ingestion` 的 `SOURCE_NOT_FOUND`（A6）、`video-segment-extraction`
的 `MISSING_EVENT`（B14）不一致。对 Agent 而言拿到的是 Python traceback 而不是
`status=failed` + `error_code`，无法按契约判断该停止还是该补输入，容易退化成"自己调试脚本"
——正是文档 §5.4 列为失败判定的行为，也是上一轮 qwen35B 测试中"工具失败后模型反复重试
直到递归上限"的诱因。

**修复**：在全部 16 个使用 `load_structured()` 的 skill 脚本中引入 `SkillInputError`，
按统一失败契约返回并保留非零退出码，同时仍然写入 `--output` 指定的文件：

| 情况 | error_code |
| --- | --- |
| 文件不存在 / 是目录 | `INPUT_NOT_FOUND` |
| 文件不可读 | `INPUT_UNREADABLE` |
| JSON 解析失败 | `INPUT_INVALID_JSON` |
| YAML 解析失败或缺 PyYAML | `INPUT_INVALID_YAML` |

覆盖的 skill：`video-stream-ingestion`、`frame-sampling`、`object-detection`、`object-tracking`、
`roi-mapping`、`duplicate-event-merge`、`evidence-snapshot`、`human-review-routing`、
`camera-health-check`、`privacy-masking`、`video-segment-extraction`、`object-statistics`、
`batch-video-ingestion`、`evidence-package-generation`、`video-embedding-index`、`video-search`。

回归用例：B13（缺失输入）、B15（16 个 skill 横向扫描）、B16（非法 JSON），现均通过。

### 4.2 能力边界请求被"模糊车牌"误判为画面质量问题（已修复）

A5 的越界请求（人员身份识别、模糊车牌、全市热力图、实时 RTSP）原先返回
`capability=camera_health_operations`，且 `gaps` 只列出实时流与热力图两项。

两个根因：

1. `camera_health_operations` 的关键词里有裸的"模糊"，"读取**模糊**车牌"被当成画面质量信号；
2. `capability_gap()` 的关键词过窄——只匹配"身份识别""车牌识别"，匹配不到"人员身份""模糊车牌"。

**修复**：`city-video-intelligence/scripts/run.py`

- 新增 `strip_ambiguous_keywords()`，在关键词打分前剔除"模糊车牌""车牌模糊""模糊文字"等
  OCR/身份语境下的短语，避免污染摄像头健康打分；
- `capability_gap()` 的关键词放宽为 身份/人脸/同一人、车牌/OCR/文字识别、实时/RTSP/GB28181/直播、热力图。

修复后 `capability=cross_video_investigation`，`gaps` 完整覆盖 4 项越界能力。
回归用例：A5（4 项 gap 全覆盖）、A5b（真实画面质量问题仍走摄像头健康运维）。

### 4.3 视频元数据请求被路由到事件分析（已修复）

"请读取这个本地视频的时长、分辨率、帧率和时间信息，整理成标准输入"原先返回
`capability=single_video_event_understanding` → `single-video-event-analysis`，
等于让一个纯元数据请求去跑事件语义分析。这也是上一轮 C-002 绕过 `video-stream-ingestion`
直接手写 `ffprobe` 的根源（文档 §6 建议 3）。

**修复**：

- `city-video-intelligence`：新增能力 `video_metadata_normalization`（关键词 时长/分辨率/帧率/
  编码/元数据/标准输入/ffprobe 等），链路固定为 `video-stream-ingestion`，并补齐 `missing_fields`
  与 `CAPABILITY_PRIORITY`；
- `video-stream-ingestion/SKILL.md`：description 显式声明 duration/resolution/frame rate/codec，
  并写明"这类请求不要自己调 ffprobe，跑本 skill 的 run.py 并报告其 JSON"；
- `video-stream-ingestion/router_card.json`：补充 `video_metadata_extraction`、
  `video_input_standardization` 任务类型与相应触发词/关键词，并重建 `routing_text`；
- `skills/registry.json`：同步该条目的 `task_types`（只改视频 bundle 条目）。

回归用例：A5c。`check_skill_router_conflicts.py --no-embedding` 仍为 `conflicts=[]`。

### 4.4 单视频计数被路由到依赖 ES 的 object-statistics（已修复）

"统计这段视频里出现了多少辆车"原先返回 `object-statistics`——那是基于视频库索引的统计 skill。
在没有入库记录（本次 ES 也不可用）时，这条链路只会得到空统计或索引错误。

**修复**：`select_capability()` 增加 `is_single_local_video_request()` 判定——当请求指向单个本地
视频（"这段视频/这个视频/上传的视频"或视频路径）且没有摄像头 ID、没有时间范围时，
计数类请求改走 `object_detection` → `video-object-analytics`。视频库级统计
（带摄像头 ID 或时间范围，如"统计 CAM_008 昨天各时段的车流量趋势"）仍走 `object-statistics`。

回归用例：A5d。

### 4.5 registry 与 Router Card 的 scope 漂移（已加固）

A12 原本只校验路径。本次发现改 Router Card 后 `registry.json` 的 `task_types` 会滞后，
已把 `scenes/task_types/input_types/output_types` 的一致性一并纳入 A12 断言。

### 4.6 `video-stream-ingestion` 不返回帧率/编码，Agent 只能自己补 ffprobe（已修复）

Layer C 的 C-002 即使正确调用了 `video-stream-ingestion`，之后仍然又跑了一次 `ffprobe`。
原因是该 skill 只返回 `duration_seconds/width/height`，而且 `duration_seconds` 是**采集窗口**
（10 秒），不是文件的真实时长（900.9 秒）——用户问的帧率、编码、总帧数它一个都答不上来。

**修复**：`video-stream-ingestion/scripts/run.py` 扩展 ffprobe 查询并新增 `parse_frame_rate()`，
输出补齐为：

```text
video_duration_seconds  文件完整时长（duration_seconds 仍表示本次采集窗口）
resolution / width / height
fps / frame_count
codec / codec_long_name / pixel_format / container_format / bit_rate
file_size_bytes / filename
```

实测与 ffprobe 结果一致（900.916s、1280x674、12fps、h264、10811 帧、674561 bps、75965424 字节）。
SKILL.md 的 Outputs 一节同步更新。回归用例：B1 现在逐字段断言这些元数据。

### 4.7 「我上传了一段监控」被当成入库请求（已修复）

C-001 的问题是"**我上传了**一段路口监控，请检查画面质量，判断有没有事故、拥堵或其他异常"。
原先返回 `capability=video_library_governance`，链路是
`video-stream-ingestion → batch-video-ingestion → video-embedding-index`——
既答非所问，又把请求推向了本次不可用的 StreetModel 向量索引。

**修复**（`city-video-intelligence`）：

1. 新增 `UPLOAD_DESCRIPTION_PHRASES`，在入库判定前剔除"我上传了/已上传/上传的视频"等
   描述附件来源的说法；真正的动作词（入库、导入、上传到视频库）仍然触发入库编排；
2. 新增 `has_event_intent()`：当能力落在 `camera_health_operations`、但请求同时含事件意图
   （事故/拥堵/打架/异常……）时，把 `single-video-event-analysis` 追加进链路——
   健康检查不能替代事件语义分析，事件判断必须走唯一入口。

修复后链路为 `frame-sampling → camera-health-check → single-video-event-analysis`，
`requires_human_review=true`。回归用例：A1b（复合请求）、A1c（真实入库请求不受影响）、A5b。

### 4.8 能力边界问题未进入视频业务编排（已加固路由声明）

C-040（"哪些能力无法由现有数据支持"）两轮实测中一次直接调了 `video_object_analytics`、
一次完全没有工具调用，都没有进入 `city-video-intelligence`。回答内容本身正确，但没有走编排层，
属于文档 §6 建议 4 记录的问题。

**加固**：`city-video-intelligence` 的 SKILL.md description、router_card 的
`task_types`（新增 `capability_boundary_assessment`）、`positive_triggers`、`keywords` 与
`routing_text` 均显式声明"回答现有数据与技能能不能做到某件事，并以 capability_gap 给出结论"，
`registry.json` 对应条目同步。这属于路由声明层面的加固，实际是否生效取决于模型选择，
需要以 Layer C 复跑结果为准。

## 5. Layer C：Agent 端到端（qwen3.6-35b-a3b）

5 个自然语言用例经 LangGraph API 实测，每个用例独立 thread 并真实上传 `Trafic.mp4`。
依赖 ES 的跨摄像头检索用例 `C-028` 已剔除。

判定口径说明：`run_video_surveillance_qwen35b_test.py` 的 `actual_skills` 会把场景过滤器注入的
`<available_skills>` 候选集计入统计（一次调用就"命中"23 个 skill），因此本次改用
[`scripts/analyze_video_agent_sse.py`](../scripts/analyze_video_agent_sse.py)，只从原始 SSE 的
真实 `tool_calls` 中提取结论。

### 5.1 修复前（批次 `20260726-171821`）

| 用例 | 真实工具调用 | 判定 |
| --- | --- | --- |
| C-001 事件分析 | `invoke_skill`×2、`bash`×6（含 ffmpeg/ffprobe）、`video_object_analytics`×2、`view_image`×6；触达 `city-video-intelligence`、`frame-sampling`；模型端 `BadRequestError` 中断 | 失败 |
| C-002 视频元数据 | 仅 `bash`×2，直接跑 ffprobe，**完全绕过 `video-stream-ingestion`** | 路由失败 |
| C-011 目标检测 | `video_object_analytics(detect)` + `read_file` | 通过 |
| C-038 人工复核 | `invoke_skill(human-review-routing)` + `read_file`×2 | 通过 |
| C-040 能力边界 | 只调了 `video_object_analytics(detect)`，未进业务编排 | 语义正确、路由失败 |

### 5.2 修复后（批次 `20260726-173821`）

| 用例 | 真实工具调用 | 判定 |
| --- | --- | --- |
| C-001 事件分析 | `invoke_skill`×1、`read_file`×3、`video_object_analytics(detect)`、`view_image`×3；触达 `city-video-intelligence`；**不再出现手写 ffmpeg/ffprobe**；无递归上限；1021 字符完整回答 | 部分通过（未落到 `single-video-event-analysis`） |
| C-002 视频元数据 | `invoke_skill(video-stream-ingestion)` → 读 SKILL.md → 执行其 `run.py` → 写标准输入文件；**路由已修复** | 通过 |
| C-011 目标检测 | `video_object_analytics(detect)` + `read_file` | 通过 |
| C-038 人工复核 | `invoke_skill(human-review-routing)` + `read_file`×3 | 通过 |
| C-040 能力边界 | 0 次工具调用，直接作答；内容正确但未进编排 | 语义正确、路由失败 |

与修复前相比最明显的改善：**不再出现"绕过 skill 手写 ffmpeg/ffprobe"**，也没有再触发递归上限，
5/5 用例都产出了完整最终回答，模型均为 `qwen3.6-35b-a3b`。

### 5.3 全部修复后（批次 `20260726-175326`）

| 用例 | 真实工具调用 | 判定 |
| --- | --- | --- |
| C-001 事件分析 | `invoke_skill`×3、`read_file`×4、`bash`×2、`write_file`×2、`view_image`×8；**首次真正走通 `city-video-intelligence` → `single-video-event-analysis`**，无手写 ffmpeg；但触发 `GraphRecursionError`（limit 100），最终回答只有 76 字符 | 路由通过、执行未收尾 |
| C-002 视频元数据 | `bash`×3（`which ffprobe`、`ffprobe`、算 MD5），本轮又绕过了 `video-stream-ingestion` | 路由失败（与上一轮结果相反） |
| C-011 目标检测 | `video_object_analytics(detect)` + `read_file` | 通过 |
| C-038 人工复核 | `invoke_skill(human-review-routing)`×2 + `read_file`×3 | 通过 |
| C-040 能力边界 | `video_object_analytics(detect)`，仍未进业务编排 | 语义正确、路由失败 |

三轮对照可以看出：**同一个用例在不同轮次会走不同路径**。C-002 在第二轮正确调用了
`video-stream-ingestion`，第三轮又退回手写 ffprobe；C-001 则在第三轮才第一次走到
`single-video-event-analysis`。这说明 skill 侧的路由声明只能提高正确路由的概率，
不能保证模型每次都遵守——剩余问题需要在 lead_agent 的执行策略层面解决。

### 5.4 仍然存在的 Agent 侧问题（超出视频 skill 修复范围）

1. **C-001 触发 `GraphRecursionError`（limit 100）**。已排除 skill 侧原因：
   `single-video-event-analysis` 对 900 秒视频只挑出 **8 帧** 审帧，manifest 里也明确写着
   "Do not inspect more than 8 frames total unless ..."。模型仍然在 19 次工具调用（含 8 次
   `view_image`）后耗尽递归预算。对应文档 §6 建议 1：需要为 lead_agent 增加终止条件与重试上限。
2. **同一用例的路由在不同轮次之间不稳定**（C-002）。skill 的 description / router_card /
   编排能力都已明确声明元数据归 `video-stream-ingestion`（§4.3、§4.6），但模型仍可能直接跑
   ffprobe。需要在 lead_agent 侧强化"有对应 skill 时不得手写等价命令"的约束。
3. **C-040 能力边界问题不进业务编排**。路由声明已加固（§4.8），模型仍倾向凭已有知识直接作答。

这三条都需要改 `backend/` 下的 Agent 执行策略，超出本次"只改视频监控 skill"的范围，
因此只做记录，未做修改。

## 5.5 全 skill 覆盖的 Agent 实测（qwen3.6-35b-a3b）

前面 §5.1–§5.4 用的是旧 runner 的 5 个用例。为了真正覆盖 bundle 里全部 21 个 skill，
新建了自包含的端到端套件 [`scripts/test_video_surveillance_agent_e2e.py`](../scripts/test_video_surveillance_agent_e2e.py)，
共 23 个自然语言用例。

与旧 runner 的关键差别：断言基于三类**真实证据**，而不是文本猜测。

| 证据源 | 用来验证 |
| --- | --- |
| `tool_calls`（含 `invoke_skill` 的 `skill_name`） | Agent 真的调用了哪个 skill / 结构化工具 |
| `tool` 结果消息 | skill **真的执行并返回了契约字段**（如 `video_session_id`、`fps`） |
| 最终回答 + `ask_clarification` 参数 | 有没有编造结论，追问是否合理 |

其中"读 tool 返回"这一环补上了此前的盲区：§4.6 的元数据补齐正是靠 E01 断言
`video_session_id`/`fps`/`codec`/`video_duration_seconds` 必须出现在 skill 返回里才算通过。

测试视频改用 30 秒 / 1.5MB 的 `Vedio-demo/Trafic-30s.mp4`（由 `Trafic.mp4` 截取），
上传更快，也避免 900 秒长视频耗尽 Agent 的步数预算。

### 结果

| 批次 | recursion_limit | 结果 |
| --- | --- | --- |
| `20260726-184712` | 100 | 15 / 23 通过 |
| `20260726-202911`（复跑 5 个撞上限的用例） | 300 | 3 / 5 通过 |

**用例覆盖 21/21 个 skill；两批合并 Agent 实际触达 17/21。**

### 递归上限是首要瓶颈，而不是模型能力

limit=100 时的 8 个失败里，**5 个是撞递归上限**（E10、E14、E15、E20、E22），
这些用例的**路由全部正确、skill 也真的调到了**，只是没跑到收尾。把 limit 提到 300 后：

| 用例 | limit=100 | limit=300 |
| --- | --- | --- |
| E10 单视频事件分析 | 撞上限 | **通过** |
| E14 隐私打码 | 撞上限 | **通过** |
| E22 章节切分与关键帧 | 撞上限 | **通过** |
| E15 完整证据包 | 撞上限 | 不再撞上限，且首次触达 `evidence-package-generation`；败于服务端 `BadRequestError` |
| E20 入库 + 向量索引 | 撞上限 | 本轮走了另一条路径（只调结构化工具），属模型轮次差异 |

E15 最能说明问题：limit=100 时 Agent 已经正确串起
`city-video-intelligence → frame-sampling → evidence-snapshot → video-segment-extraction → privacy-masking`
五个 skill，链路完全符合设计，只是预算用尽；limit=300 时进一步调到了
`evidence-package-generation`。

**结论：qwen3.6-35b-a3b 在视频监控 skill 上的路由能力是够用的，当前 `recursion_limit=100`
对多步骤视频任务偏紧，建议对视频场景提高到 250–300。**

### 未触达的 4 个 skill

| skill | 原因 | 是否算缺口 |
| --- | --- | --- |
| `object-tracking`、`roi-mapping` | Agent 走了结构化工具 `video_object_analytics`，这正是文档 §5.3 规定的正确入口 | **不算**，符合设计 |
| `object-statistics`、`video-embedding-index` | ES / StreetModel 未启用，链路本身不可达 | 不算，环境限制 |

### 仍然存在的真实问题

1. **E21 伪造执行结果（最严重）**。Agent 回答"视频库批量登记已完成，共登记 3 个视频文件"并附完整表格，
   但全程只用 `bash` + `ffprobe`，**一个入库 skill 都没调用**，而 ES 本身是不可用的。
   这比编造数字更隐蔽——它伪造的是"执行已完成"这个状态。
   已把 `批量登记已完成`、`登记完成` 加入该用例的 `forbid_answer`。
2. **E12 绕过证据契约**。要求"证据截图 + 哈希"时 Agent 走了 `frame-sampling` 而不是
   `evidence-snapshot`，结果里没有 `evidence_id`、`privacy_masked` 等证据契约字段。
3. **跨 bundle 路由**。E19 中 Agent 读取了 `/mnt/skills/custom/road-traffic-analysis/SKILL.md`，
   视频库统计请求被引向了非视频 bundle 的 skill。

### 一处测试自身的口径修正

E19 最初判为失败，复查后确认是**断言过严**：Agent 并没有编造任何统计数字，而是用
`ask_clarification` 追问"上周具体指哪一周、视频库范围是什么"——在 ES 不可用且问题本身有歧义时，
这是比"直接报不可用"更好的行为。已放宽为：如实报告不可用**或**发起追问都算通过，
只保留反编造断言。同类修正还有 E06：追问内容在 `ask_clarification` 的参数里而不在最终文本里，
判定的"回答面"已扩展为 `最终回答 + 追问内容`。

## 6. 结论

- Layer A（编排与契约）19 通过 / 1 跳过，Layer B（沙箱真实视频）17 通过 / 0 失败，
  `check_skill_router_conflicts.py --no-embedding` 为 `conflicts=[]`。
- Layer C 全 skill 端到端：用例覆盖 21/21 个 skill，Agent 实际触达 17/21，
  limit=100 时 15/23 通过，其中 5 个失败在提高 recursion_limit 后有 3 个直接转为通过。
- 本次在视频 bundle 内定位并修复了 7 类问题（§4.1–§4.7）、加固 1 处路由声明（§4.8），
  每一项都有对应的回归用例。
- 对 qwen3.6-35b-a3b 的评估：**skill 路由能力够用，主要瓶颈是步数预算**。
  建议把视频场景的 `recursion_limit` 提到 250–300，并补上"没有真正执行 skill 就不得声称执行完成"
  的约束（E21）。这两项都要改 `backend/` 下的 Agent 配置与策略，不在本次"只改视频监控 skill"的范围内。
- StreetModel 与 Elasticsearch 恢复后，还需要补测 `video-search`、`video-embedding-index`、
  `object-statistics` 的在线检索路径（A14 跳过项、E18/E19/E20）。

复现命令：

```bash
# Layer A + Layer B
python3 scripts/test_video_surveillance_skills.py --layer all

# Layer C 全 skill 端到端（23 个用例）
python3 scripts/test_video_surveillance_agent_e2e.py --recursion-limit 300 --timeout 900

# 只跑某几个用例
python3 scripts/test_video_surveillance_agent_e2e.py --cases E01 E15 --list
```
