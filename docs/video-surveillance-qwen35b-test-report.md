# qwen3.6-35b-a3b 视频监控小规模实测报告

测试日期：2026-07-23

## 1. 结论

本报告记录两次批次：首次批次只在问题中写入宿主机视频路径，且结束时删除了临时 assistant；修正版批次通过 Gateway 真正上传视频、为每个用例创建独立且保留的 thread。修正版已经解决历史记录和附件绑定问题，但视频监控 Agent 仍未端到端通过：复杂视频任务会触发 LangGraph 递归上限，对象分析工具依赖失败后模型会重复尝试，视频元数据请求绕过了目标 skill，能力边界问题也没有进入视频场景路由。

自动脚本统计为 1/5 通过；按“实际执行目标 skill 工作流并得到完整结果”的严格口径，本批次没有完整通过的用例。C-038 的路由和回答内容正确，但只完成了 skill 准备和文件读取，没有执行复核脚本，因此标记为“部分通过”。

## 2. 测试环境

| 项目 | 实际值 |
| --- | --- |
| 模型 | `qwen3.6-35b-a3b` |
| thinking | `false` |
| Agent | `lead_agent` 临时 qwen35B assistant |
| API | `http://127.0.0.1:3538/api/langgraph` |
| 测试视频 | `/mnt/datasets/Vedio-demo/Trafic.mp4` |
| 视频挂载 | `imiss-deer-flow-main/datasets` -> `/mnt/datasets` |
| LangGraph recursion limit | `100` |

模型选择由临时 assistant 配置完成，并在 API 运行日志及每个 SSE 的 `response_metadata.model_name` 中核验为 qwen35B。运行请求只传 `context.thread_id`，避免新架构的 `configurable` 与 `context` 冲突。

复现命令：

```bash
cd /home/huangxiao/City_brain/imiss-deer-flow-qwen36-35b-a3b-test
python scripts/run_video_surveillance_qwen35b_test.py --timeout 420
```

完整原始产物：[`outputs/skill-tests/video-surveillance-qwen35b/20260723-171819`](../outputs/skill-tests/video-surveillance-qwen35b/20260723-171819)

## 3. 首次批次结果（历史记录）

| 用例 | 预期 | 实际调用/结果 | 判定 |
| --- | --- | --- | --- |
| C-001 画质、事故、拥堵和证据 | `city-video-intelligence` -> `single-video-event-analysis` | 完成编排、抽帧、健康检查；健康检查返回 `ok`，随后只准备事件分析并写入输入文件，最终触发 `GraphRecursionError` | 失败 |
| C-002 视频元数据标准化 | `video-stream-ingestion` | 直接调用 `ffprobe` 并写出 metadata JSON，回答内容完整，但没有调用目标 skill | 路由失败 |
| C-011 行人和车辆检测 | `video-object-analytics` -> `object-detection` | 调用结构化工具；工具两次返回 `video analytics command returned no result`，模型随后尝试安装 `ultralytics`，仍未完成并触发递归 | 失败 |
| C-038 人工复核条件 | `human-review-routing` | 成功准备 skill 并读取 skill 与脚本，回答的四类复核条件正确；没有观察到实际脚本执行 | 部分通过 |
| C-040 能力边界 | `city-video-intelligence` | 没有进入视频 skill 路由，也没有工具调用；最终回答正确指出身份识别、模糊车牌、全市热力图和 RTSP 均缺少必要数据或基础设施 | 语义通过，路由失败 |

所有用例的模型均为 `qwen3.6-35b-a3b`，没有模型错配。每个用例的原始 SSE、最终回答和验证文件都保存在批次目录下，例如 [`C-011/turn-1.sse`](../outputs/skill-tests/video-surveillance-qwen35b/20260723-171819/C-011/turn-1.sse) 和 [`C-011/agent_final.md`](../outputs/skill-tests/video-surveillance-qwen35b/20260723-171819/C-011/agent_final.md)。

## 4. 修正版复测结果

修正版批次目录：[`outputs/skill-tests/video-surveillance-qwen35b/20260723-175833`](../outputs/skill-tests/video-surveillance-qwen35b/20260723-175833)

| 项目 | 结果 |
| --- | --- |
| qwen35B assistant | `ce846f38-6ee4-4cf5-b574-ea77091f23ad`，保留 |
| 独立 thread | 5 个，均可通过 thread search 找到 |
| 实际上传视频 | 4 个 `Trafic.mp4`，每个视频用例单独上传到自己的 thread |
| 无视频用例 | C-038，纯人工复核规则咨询，按设计不上传视频 |
| 模型匹配 | 5/5 为 `qwen3.6-35b-a3b` |
| 自动判定 | 1/5；与首次批次一致，主要问题仍是路由和工具执行 |

可直接打开的修正版历史：

| 用例 | Thread |
| --- | --- |
| C-001 | `http://127.0.0.1:3538/workspace/chats/a6f644b3-e95f-46d3-9d11-46f89061ab5a` |
| C-002 | `http://127.0.0.1:3538/workspace/chats/806db398-4a90-4b37-bad1-5e1ccdf2b12a` |
| C-011 | `http://127.0.0.1:3538/workspace/chats/59e3d0df-6938-4ef5-acc1-9ce422b61d3b` |
| C-038 | `http://127.0.0.1:3538/workspace/chats/0f6d818b-a346-421b-9590-b1613cdb4457` |
| C-040 | `http://127.0.0.1:3538/workspace/chats/52ca731a-8962-4587-9189-33c3b57c1c22` |

修正版每个视频用例的 `upload-response.json` 均返回 `success=true`，SSE 中出现 `<uploaded_files>` 和 `/mnt/user-data/uploads/Trafic.mp4`。C-002 虽然仍错误绕过 `video-stream-ingestion`，但最终读取的是上传文件路径，不再是宿主机路径字符串。

## 5. 关键证据

### C-001

- `city-video-intelligence` 的 prepare 成功。
- `frame-sampling` 实际抽帧成功。
- `camera-health-check` 实际返回 `health_status=ok`、`health_score=100`。
- `single-video-event-analysis` 只完成 prepare；最终回答停在“现在创建输入 JSON 文件并执行事件分析”。
- LangGraph 日志显示达到 recursion limit 100，没有停止条件。

### C-011

- Agent 确实调用了 `video_object_analytics`，不是手写底层检测命令。
- 工具返回：`status=failed`、`error_code=RuntimeError`、`video analytics command returned no result`。
- Sandbox 中存在抽帧和检测产物目录，但工具没有返回可用的结构化结果；之后模型尝试 `pip install ultralytics opencv-python-headless`，并发现当前 Python 没有 `ultralytics`。
- 该用例最终同样因递归上限失败。

### C-002

- 实际工具链是 `which ffprobe`、`ffprobe ...` 和写入 `video_metadata.json`。
- 没有 `invoke_skill(video-stream-ingestion)`，说明自然语言路由没有遵循新架构规定的“先业务编排、再原子技能”链路。

## 6. 问题与建议

1. 为 `lead_agent` 增加工具失败后的终止状态和重试上限，避免 `IntentRecognitionMiddleware`/Agent 在同一任务上循环到 100 步。
2. 修复 `video-object-analytics` 的运行依赖和结果返回契约。工具失败时应返回明确的可操作错误，Agent 应停止重复安装或重复调用。
3. 将“读取视频时长、分辨率、帧率、时间信息”明确映射到 `city-video-intelligence` + `video-stream-ingestion`，禁止自然语言 Agent 直接用 bash 替代目标 skill。
4. 对能力边界问题补充 `capability_inventory` 到 `video_surveillance` 的路由规则，使 C-040 进入业务编排后再给出数据边界结论。
5. 修正测试脚本的技能统计：场景过滤器注入的 `<available_skills>` 只能作为候选集，不能计为实际执行；实际技能应只从 `invoke_skill`、结构化工具调用和成功结果中提取。

本次未修改已有 registry 或业务实现，只保存了测试脚本、原始结果和报告，便于修复后重跑同一批次。
