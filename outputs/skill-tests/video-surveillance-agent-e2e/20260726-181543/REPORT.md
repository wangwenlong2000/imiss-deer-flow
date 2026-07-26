# 视频监控 Agent 端到端测试结果

模型：`qwen3.6-35b-a3b`　assistant：`fbee207f-2aea-4e88-b348-25d8878d102a`　recursion_limit：`100`

| 用例 | 覆盖的修复 | 结果 | 实际触达 skill | 工具 | 失败原因 |
| --- | --- | --- | --- | --- | --- |
| E06 | 证据包缺少关键输入时只做一次合并追问，不编造 artifact | fail | 无 | ask_clarification, bash | 最终回答未命中任一关键词 ['请问', '需要', '提供', '哪个', '路径'] |

每个用例目录下保留 `turn-1.sse`（原始流）、`tool-calls.json`（真实调用与返回）、`agent_final.md`（最终回答）与 `validation.json`（判定明细）。
