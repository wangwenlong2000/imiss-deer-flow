# 视频监控 Skill 路由测试问题

以下题干均从普通用户角度编写，不直接指定 skill。实际测试时只发送“用户问题”，括号中的内容仅用于核对预期路由。

机器可执行的版本在 [`scripts/video_routing_questions_cases.json`](../scripts/video_routing_questions_cases.json)，
并由 `test_video_surveillance_skills.py` 的 **A17** 用例在每次回归时固化。

## 判定口径

期望值不再是“必须命中某一个 skill”，而是三元组：

| 字段 | 含义 |
| --- | --- |
| `expected_capability` | `city-video-intelligence` 必须选中的能力 |
| `expect_any` | 推荐链路命中其中**任意一个**即算路由正确（容纳设计上等价的多条链路） |
| `forbid` | 命中其中任意一个即算路由**错误** |

改成集合判定的原因：同一个请求经常有多条同样正确的链路。例如“检测每一帧的人和车”，
直接调结构化工具 `video-object-analytics` 与调 `object-detection` 是等价的，
旧口径只认后者，会把正确行为判成失败。

## 题目

1. 用户问题：请分析这段监控视频，找出其中的关键场景和异常事件，按时间线还原事件经过，并生成一份复核报告。
   预期能力：`single_video_event_understanding`；命中其一：`city-video-intelligence` / `single-video-event-analysis`；禁止：`video-search`

2. 用户问题：请检查这批摄像头最近的视频质量，找出黑屏、花屏、模糊、过曝、断流和时间戳异常的设备。
   预期能力：`camera_health_operations`；命中：`camera-health-check`

3. 用户问题：这批摄像头上报了很多相似告警，请根据时间、地点、目标和证据相似度合并重复事件，并保留合并依据。
   预期能力：`event_deduplication`；命中：`duplicate-event-merge`

4. 用户问题：请从这段视频中截取能证明车辆闯入限制区域的关键画面，保留时间戳、摄像头编号和原始文件信息，导出证据图片。
   预期能力：`evidence_snapshot`；命中：`evidence-snapshot`

5. 用户问题：请把这段视频转换成统一格式，提取关键帧，并截取 00:12 到 00:28 的片段保存下来。
   预期能力：`media_transcoding`；命中其一：`ffmpeg-utils` / `video-segment-extraction`

6. 用户问题：请按每 2 秒从视频中抽取一帧，并为每帧生成包含时间戳、来源和图像尺寸的结构化记录。
   预期能力：`frame_sampling`；命中：`frame-sampling`；禁止：`video-search`

7. 用户问题：请根据告警置信度、证据完整性和潜在影响，把事件分成自动通过、需要人工复核和优先复核三类。
   预期能力：`review_triage`；命中：`human-review-routing`

8. 用户问题：请识别视频里的人员和车辆，持续记录它们的位置和运动轨迹，并输出对象类别、时间和轨迹数据。
   预期能力：`object_tracking`；命中其一：`video-object-analytics` / `object-detection` / `object-tracking`

9. 用户问题：请检测视频每一帧中的人、车、自行车和摩托车，返回每个目标的边界框、类别和置信度。
   预期能力：`object_detection`；命中其一：`video-object-analytics` / `object-detection`

10. 用户问题：请持续跟踪画面中的每辆车，生成它们的进入时间、离开时间、运动轨迹和行驶方向。
    预期能力：`object_tracking`；命中其一：`video-object-analytics` / `object-tracking`

11. 用户问题：请对视频中的人脸、车牌和其他敏感区域进行模糊处理，输出一份可以公开使用的脱敏视频。
    预期能力：`privacy_protection`；命中其一：`video-privacy-masking` / `privacy-masking`
    额外断言：**不得**报出身份识别或 OCR 能力缺口——遮蔽一个区域不需要认出里面是谁、车牌号是多少。

12. 用户问题：请在画面中标出门口和停车区两个区域，并分别统计进入、离开和停留的目标数量。
    预期能力：`roi_zone_statistics`；命中其一：`roi-transit-statistics` / `roi-mapping`；禁止：`object-statistics`
    禁止原因：`object-statistics` 依赖 Elasticsearch 视频库索引，单视频场景下只会拿到空统计或索引错误。

13. 用户问题：请从 8 小时监控录像中提取包含异常行为的多个短视频片段，每段保留前后缓冲时间并附上事件说明。
    预期能力：`evidence_preservation`；命中：`video-segment-extraction`

14. 用户问题：请接入这组 RTSP 摄像头地址，规范化视频源信息，记录摄像头会话信息，并报告无法连接或频繁断开的源。
    预期能力：`video_metadata_normalization`；命中其一：`video-stream-ingestion` / `city-video-intelligence`
    额外断言：**必须**报出 `Real-time stream` 能力缺口。`video-stream-ingestion` 只支持 `--source-type local_file`，
    RTSP 实时接流是明确的能力边界，正确行为是照实说明，而不是假装能接流。

## 测试建议

- 每次只发送一条题干，不要把预期路由一起发送。
- 先用没有上传文件的题目测试意图路由，再补充视频、图片测试实际执行。
- `object-detection`、`object-tracking` 和 `video-object-analytics` 容易相互混淆，建议分别单独测试；
  但按上面的集合口径，三者互相替代不算错。
- Agent 端到端复跑请用 `--recursion-limit 1000`，与前端 [`hooks.ts`](../frontend/src/core/threads/hooks.ts) 的真实取值一致。
  用 100 会让多步骤视频任务撞 `GraphRecursionError`，那是测试口径问题，不是产品缺陷。

## 变更记录

2026-07-26（第二次）：按当前架构重订全部 14 条期望路由，并改为 `expected_capability` +
`expect_any` + `forbid` 的集合判定。同批修复了 `city-video-intelligence` 的两个结构性问题：
关键词等权计数导致泛化词压过专用词、以及抽帧/转码/去重/证据截图/脱敏/复核分流/区域统计
七类能力在编排层完全没有入口。修复前 14 题只有 3 条路由正确，修复后 14/14。
同时新增 `video-privacy-masking`（输出脱敏视频）与 `roi-transit-statistics`（区域进出停留计数）
两个 skill，补上第 11、12 题此前无人承接的能力缺口。

2026-07-26（第一次）：移除 6 条预期路由指向已删除 skill 的题目（`density-aggregation-event`、`event-rule-engine`、
`event-template-mapping`、`object-composition-event`、`spatial-occupancy-event`、`temporal-persistence-event`）。
这些按密度、规则、目标组合、ROI 占用和时间持续性直接判定业务事件的旧 skill 已不在可执行 bundle 中，
新架构下事件语义统一由 `city-video-intelligence` -> `single-video-event-analysis` 承担。
