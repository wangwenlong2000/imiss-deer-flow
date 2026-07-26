# 视频监控 Agent 端到端测试结果

模型：`qwen3.6-35b-a3b`　assistant：`c083dac2-93bc-4497-969a-c777808a2d27`　recursion_limit：`300`

| 用例 | 覆盖的修复 | 结果 | 实际触达 skill | 工具 | 失败原因 |
| --- | --- | --- | --- | --- | --- |
| E10 | 单视频事件语义分析唯一入口 | pass | city-video-intelligence, single-video-event-analysis | bash, invoke_skill, read_file, view_image, write_file |  |
| E14 | 隐私打码 | pass | frame-sampling, privacy-masking, video-object-analytics | bash, invoke_skill, present_files, read_file, video_object_analytics |  |
| E15 | 输入齐全时生成证据包 | fail | city-video-intelligence, evidence-package-generation, frame-sampling, privacy-masking, video-segment-extraction, video-stream-ingestion | bash, invoke_skill, read_file, view_image | 运行出现错误事件: {"error": "BadRequestError", "message": "An internal error occurred"} |
| E20 | StreetModel 不可用时入库与向量索引如实报告 | fail | video-object-analytics | bash, video_object_analytics | 期望至少调用其中之一 ['video-embedding-index', 'batch-video-ingestion', 'video-stream-ingestion', 'city-video-intelligence']，实际: ['video-object-analytics']；最终回答未命中任一关键词 ['无法', '不可用', '失败', ' |
| E22 | 长录像按场景切章节并抽关键帧 | pass | analyze-video | bash, invoke_skill, read_file, view_image |  |

## Skill 覆盖矩阵

「用例覆盖」= 有用例针对该 skill；「实际触达」= Agent 在某个用例里真的调用了它。

| skill | 用例覆盖 | 实际触达 | 相关用例 |
| --- | --- | --- | --- |
| `analyze-video` | ✓ E22 | ✓ E22 | E22 |
| `batch-video-ingestion` | ✓ E20 | ✗ | E20 |
| `camera-health-check` | — | ✗ | — |
| `city-video-intelligence` | ✓ E10 | ✓ E10, E15 | E10, E15 |
| `duplicate-event-merge` | — | ✗ | — |
| `evidence-package-generation` | ✓ E15 | ✓ E15 | E15 |
| `evidence-snapshot` | — | ✗ | — |
| `ffmpeg-utils` | — | ✗ | — |
| `frame-sampling` | — | ✓ E14, E15 | E14, E15 |
| `human-review-routing` | — | ✗ | — |
| `object-detection` | — | ✗ | — |
| `object-statistics` | — | ✗ | — |
| `object-tracking` | — | ✗ | — |
| `privacy-masking` | ✓ E14 | ✓ E14, E15 | E14, E15 |
| `roi-mapping` | — | ✗ | — |
| `single-video-event-analysis` | ✓ E10 | ✓ E10 | E10 |
| `video-embedding-index` | ✓ E20 | ✗ | E20 |
| `video-object-analytics` | — | ✓ E14, E20 | E14, E20 |
| `video-search` | — | ✗ | — |
| `video-segment-extraction` | — | ✓ E15 | E15 |
| `video-stream-ingestion` | — | ✓ E15 | E15 |

用例覆盖 7/21 个 skill；Agent 实际触达 9/21 个。

每个用例目录下保留 `turn-1.sse`（原始流）、`tool-calls.json`（真实调用与返回）、`agent_final.md`（最终回答）与 `validation.json`（判定明细）。
