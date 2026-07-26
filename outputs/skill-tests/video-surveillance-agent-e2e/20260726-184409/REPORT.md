# 视频监控 Agent 端到端测试结果

模型：`qwen3.6-35b-a3b`　assistant：`ba902b78-5bc2-401e-9316-6eef89fa3f39`　recursion_limit：`100`

| 用例 | 覆盖的修复 | 结果 | 实际触达 skill | 工具 | 失败原因 |
| --- | --- | --- | --- | --- | --- |
| E17 | 事件去重合并 | pass | duplicate-event-merge | bash, invoke_skill, read_file |  |

## Skill 覆盖矩阵

「用例覆盖」= 有用例针对该 skill；「实际触达」= Agent 在某个用例里真的调用了它。

| skill | 用例覆盖 | 实际触达 | 相关用例 |
| --- | --- | --- | --- |
| `analyze-video` | — | ✗ | — |
| `batch-video-ingestion` | — | ✗ | — |
| `camera-health-check` | — | ✗ | — |
| `city-video-intelligence` | — | ✗ | — |
| `duplicate-event-merge` | ✓ E17 | ✓ E17 | E17 |
| `evidence-package-generation` | — | ✗ | — |
| `evidence-snapshot` | — | ✗ | — |
| `ffmpeg-utils` | — | ✗ | — |
| `frame-sampling` | — | ✗ | — |
| `human-review-routing` | — | ✗ | — |
| `object-detection` | — | ✗ | — |
| `object-statistics` | — | ✗ | — |
| `object-tracking` | — | ✗ | — |
| `privacy-masking` | — | ✗ | — |
| `roi-mapping` | — | ✗ | — |
| `single-video-event-analysis` | — | ✗ | — |
| `video-embedding-index` | — | ✗ | — |
| `video-object-analytics` | — | ✗ | — |
| `video-search` | — | ✗ | — |
| `video-segment-extraction` | — | ✗ | — |
| `video-stream-ingestion` | — | ✗ | — |

用例覆盖 1/21 个 skill；Agent 实际触达 1/21 个。

每个用例目录下保留 `turn-1.sse`（原始流）、`tool-calls.json`（真实调用与返回）、`agent_final.md`（最终回答）与 `validation.json`（判定明细）。
