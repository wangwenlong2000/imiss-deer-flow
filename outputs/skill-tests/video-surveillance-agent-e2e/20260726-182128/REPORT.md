# 视频监控 Agent 端到端测试结果

模型：`qwen3.6-35b-a3b`　assistant：`3f01ecaa-340b-44d0-a35a-08dd83c89724`　recursion_limit：`100`

| 用例 | 覆盖的修复 | 结果 | 实际触达 skill | 工具 | 失败原因 |
| --- | --- | --- | --- | --- | --- |
| E01 | 4.3 + 4.6 视频元数据路由与字段补齐 | pass | video-stream-ingestion | bash, invoke_skill, read_file |  |
| E02 | 4.7 上传描述 + 画面质量 + 事件复合链路 | fail | city-video-intelligence, frame-sampling | bash, invoke_skill, read_file, view_image | 运行出现错误事件: {"error": "BadRequestError", "message": "An internal error occurred"}；期望至少调用其中之一 ['single-video-event-analysis', 'camera-health-check']，实际: ['city-video-intelligence', 'f |
| E03 | 4.4 单视频计数走 video-object-analytics 而非 ES 统计 | pass | video-object-analytics | invoke_skill, read_file, video_object_analytics |  |
| E04 | 4.2 + 4.8 能力边界与 capability_gap | pass | video-object-analytics | video_object_analytics |  |
| E05 | 4.1 失败契约：视频不存在时必须报错而不是编造事件 | pass | 无 | ask_clarification, bash |  |
| E06 | 证据包缺少关键输入时只做一次合并追问，不编造 artifact | pass | 无 | ask_clarification, ls |  |
| E07 | 人工复核规则咨询走 human-review-routing | pass | human-review-routing | invoke_skill, read_file |  |
| E08 | 目标检测与跟踪走结构化工具，且不输出事件结论 | pass | video-object-analytics | read_file, video_object_analytics |  |

每个用例目录下保留 `turn-1.sse`（原始流）、`tool-calls.json`（真实调用与返回）、`agent_final.md`（最终回答）与 `validation.json`（判定明细）。
