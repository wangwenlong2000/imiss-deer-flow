# 视频监控 Agent 端到端测试结果

模型：`qwen3.6-35b-a3b`　assistant：`aca2c42a-4860-40ab-8cdd-d33e72175d81`　recursion_limit：`100`

| 用例 | 覆盖的修复 | 结果 | 实际触达 skill | 工具 | 失败原因 |
| --- | --- | --- | --- | --- | --- |
| E01 | 4.3 + 4.6 视频元数据路由与字段补齐 | pass | video-stream-ingestion | bash, invoke_skill, ls, read_file |  |
| E02 | 4.7 上传描述 + 画面质量 + 事件复合链路 | pass | city-video-intelligence, single-video-event-analysis, video-object-analytics | bash, invoke_skill, read_file, video_object_analytics, view_image, write_file |  |
| E03 | 4.4 单视频计数走 video-object-analytics 而非 ES 统计 | pass | video-object-analytics | read_file, video_object_analytics |  |
| E04 | 4.2 + 4.8 能力边界与 capability_gap | pass | video-object-analytics | bash, video_object_analytics |  |
| E05 | 4.1 失败契约：视频不存在时必须报错而不是编造事件 | pass | city-video-intelligence, single-video-event-analysis | bash, invoke_skill, read_file |  |
| E06 | 证据包缺少关键输入时只做一次合并追问，不编造 artifact | pass | 无 | ask_clarification |  |
| E07 | 人工复核规则咨询走 human-review-routing | pass | human-review-routing | invoke_skill, ls, read_file |  |
| E08 | 目标检测与跟踪走结构化工具，且不输出事件结论 | pass | video-object-analytics | read_file, video_object_analytics |  |
| E09 | 抽帧：只准备审查素材，不下事件结论 | pass | frame-sampling | bash, invoke_skill, present_files, read_file |  |
| E10 | 单视频事件语义分析唯一入口 | fail | city-video-intelligence, single-video-event-analysis | bash, invoke_skill, read_file, view_image, write_file | 触发 LangGraph 递归上限 |
| E11 | 摄像头健康检查 | pass | camera-health-check, frame-sampling | bash, invoke_skill, read_file, view_image |  |
| E12 | 证据截图与完整性哈希 | fail | frame-sampling | bash, invoke_skill, read_file, view_image | 期望至少调用其中之一 ['evidence-snapshot', 'ffmpeg-utils', 'city-video-intelligence']，实际: ['frame-sampling'] |
| E13 | 事件片段截取 | pass | video-segment-extraction | bash, invoke_skill, ls, present_files, read_file |  |
| E14 | 隐私打码 | fail | frame-sampling, object-detection, privacy-masking | bash, invoke_skill, read_file, view_image | 触发 LangGraph 递归上限 |
| E15 | 输入齐全时生成证据包 | fail | city-video-intelligence, evidence-snapshot, frame-sampling, privacy-masking, video-segment-extraction | bash, invoke_skill, read_file | 触发 LangGraph 递归上限 |
| E16 | ROI 区域判定 | pass | video-object-analytics | read_file, video_object_analytics |  |
| E17 | 事件去重合并 | pass | duplicate-event-merge | bash, invoke_skill, read_file |  |
| E18 | ES 不可用时如实报告，不伪造检索命中 | pass | city-video-intelligence, video-search | bash, invoke_skill, read_file |  |
| E19 | 视频库统计在 ES 不可用时如实报告 | fail | 无 | ask_clarification, bash, read_file | 期望至少调用其中之一 ['object-statistics', 'city-video-intelligence']，实际: 无；最终回答未命中任一关键词 ['无法', '不可用', '没有', '失败', '缺少', '索引'] |
| E20 | StreetModel 不可用时入库与向量索引如实报告 | fail | batch-video-ingestion, city-video-intelligence, video-stream-ingestion | bash, invoke_skill, read_file, write_file | 触发 LangGraph 递归上限 |
| E21 | 批量登记只记元数据，不做内容检测 | fail | 无 | bash, read_file | 期望至少调用其中之一 ['batch-video-ingestion', 'video-stream-ingestion', 'city-video-intelligence']，实际: 无 |
| E22 | 长录像按场景切章节并抽关键帧 | fail | analyze-video, city-video-intelligence | bash, invoke_skill, read_file, view_image | 触发 LangGraph 递归上限 |
| E23 | 原子 ffmpeg 操作走 skill 而不是手写命令 | pass | ffmpeg-utils | bash, invoke_skill, read_file, view_image |  |

## Skill 覆盖矩阵

「用例覆盖」= 有用例针对该 skill；「实际触达」= Agent 在某个用例里真的调用了它。

| skill | 用例覆盖 | 实际触达 | 相关用例 |
| --- | --- | --- | --- |
| `analyze-video` | ✓ E22 | ✓ E22 | E22 |
| `batch-video-ingestion` | ✓ E20, E21 | ✓ E20 | E20, E21 |
| `camera-health-check` | ✓ E02, E11 | ✓ E11 | E02, E11 |
| `city-video-intelligence` | ✓ E02, E04, E10 | ✓ E02, E05, E10, E15, E18, E20, E22 | E02, E04, E05, E10, E15, E18, E20, E22 |
| `duplicate-event-merge` | ✓ E17 | ✓ E17 | E17 |
| `evidence-package-generation` | ✓ E06, E15 | ✗ | E06, E15 |
| `evidence-snapshot` | ✓ E12 | ✓ E15 | E12, E15 |
| `ffmpeg-utils` | ✓ E12, E23 | ✓ E23 | E12, E23 |
| `frame-sampling` | ✓ E09 | ✓ E09, E11, E12, E14, E15 | E09, E11, E12, E14, E15 |
| `human-review-routing` | ✓ E07 | ✓ E07 | E07 |
| `object-detection` | ✓ E03 | ✓ E14 | E03, E14 |
| `object-statistics` | ✓ E19 | ✗ | E19 |
| `object-tracking` | ✓ E08 | ✗ | E08 |
| `privacy-masking` | ✓ E14 | ✓ E14, E15 | E14, E15 |
| `roi-mapping` | ✓ E16 | ✗ | E16 |
| `single-video-event-analysis` | ✓ E02, E10 | ✓ E02, E05, E10 | E02, E05, E10 |
| `video-embedding-index` | ✓ E20 | ✗ | E20 |
| `video-object-analytics` | ✓ E03, E08 | ✓ E02, E03, E04, E08, E16 | E02, E03, E04, E08, E16 |
| `video-search` | ✓ E18 | ✓ E18 | E18 |
| `video-segment-extraction` | ✓ E13 | ✓ E13, E15 | E13, E15 |
| `video-stream-ingestion` | ✓ E01, E05 | ✓ E01, E20 | E01, E05, E20 |

用例覆盖 21/21 个 skill；Agent 实际触达 16/21 个。

每个用例目录下保留 `turn-1.sse`（原始流）、`tool-calls.json`（真实调用与返回）、`agent_final.md`（最终回答）与 `validation.json`（判定明细）。
