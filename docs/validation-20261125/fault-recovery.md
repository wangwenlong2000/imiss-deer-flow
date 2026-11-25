# OneCity 任务取消与故障恢复验证

2026-11-25 使用最终源码、临时 SQLite 账号和真实 OneCity → Gateway → LangGraph HTTP 链路完成 16 组故障恢复检查，并复跑 10 组多用户 HTTP 联调。故障调度器设置为 2 个执行任务、3 个运行或排队任务、单用户 1 个任务。结果及源码 SHA-256 记录在 [fault-recovery.json](fault-recovery.json) 和 [onecity-http.json](onecity-http.json)。

## 已完成修复

OneCity 新增本人任务状态查询、取消与流恢复入口。每次请求都会重新验证当前平台账号、会话归属以及 DeerFlow 持久化线程映射，Gateway 再次验证该线程的 IAM 权限。

| OneCity 接口 | 请求参数与行为 |
| --- | --- |
| `GET /query/deerflow/status` | `conversation_id`、`thread_id`、`run_id`，读取该任务真实状态 |
| `POST /query/deerflow/cancel` | JSON 包含上述三个 ID，等待取消确认并返回原生状态 |
| `GET /query/deerflow/resume` | 上述三个 ID 及原 `query`，通过原生 `join_stream` 恢复同一任务 |

恢复接口只接受当前会话最新任务，旧任务遇到后续轮次时返回 409。新任务元数据包含原始问题的 SHA-256，恢复问题必须与已受理问题一致。历史任务缺少该字段时，只接受当前线程最后一条实际用户问题，内部规划消息不参与身份匹配。读取快照前后及流结束时都重新确认最新任务，避免恢复期间新一轮任务开始导致跨轮展示。运行中恢复会加入原任务，已完成任务恢复会读取其最终线程快照。恢复连接关闭后任务继续执行。原生任务列表检查确认重连前后只有一个任务。

Mobile 输出终态保留 `failed`、`incomplete` 与 `cancelled`，OneCity 对应发送 `finished=false`。模型调用返回 HTTP 503 时，页面流中出现错误并以失败结束。纯 JSON 接口也保留失败结果。取消等待原生任务确认，仍为 `pending`、`running` 或未知状态时返回 503，客户端保留任务控制。重复取消已经结束的本人任务保持幂等。

流式终态兜底现在先查询本轮准确的 `run_id`，只有原生状态为 `success` 才使用最终 checkpoint 合成完成事件。新增回归确认 `pending`、`running`、`error`、`timeout` 与 `interrupted` 都不能通过该兜底提前结束任务。关闭下游流时会等待生产协程退出，清理补充状态查询任务。

## 检查结果

| 场景 | 结果 |
| --- | --- |
| 跨账号取消任务 | 返回 404 |
| 本人取消运行中任务 | 原生状态为 `interrupted`，流以未成功结束 |
| 取消后重新提交 | 同一用户可完成新任务 |
| 重复取消 | 幂等，返回原终态 |
| 浏览器流断开 | 原任务继续执行 |
| 修改恢复问题 | 返回 409，与受理问题不一致 |
| 跨账号恢复 | 返回 404 |
| 重连正在运行的任务 | 加入原任务，任务总数保持 1 |
| 断线期间完成后的恢复 | 恢复本轮最终答案 |
| 无效任务 ID | 返回 400 |
| 旧任务遇到更新轮次 | 返回 409 |
| 新轮任务运行中恢复旧任务 | 返回 409 |
| 取消排队任务 | 任务未进入 running，取消确认且容量释放 |
| 模型调用 HTTP 503 | 失败流包含错误，`finished=false` |
| 模型失败后重新提交 | 后续任务成功 |
| 失败调用重试次数 | 故障端点准确收到 1 次调用 |

实际故障调用使用真实 `ChatOpenAI.ainvoke` 访问本地 OpenAI 兼容 HTTP 端点，该端点返回 503，模型客户端设置 `max_retries=0`。其他图任务使用确定性的短等待和回答，验证范围覆盖真实调度、传输、认证、终态和容量恢复。外部业务模型的吞吐与成功率由真实业务负载报告记录。

最终定向回归另有后端 Mobile/IAM 相关 84 项、OneCity 认证与访问相关 83 项通过。后端覆盖 legacy 问题校验、加载 checkpoint 期间出现新任务、取消状态尚未确认等边界。修改的后端和验证脚本通过 Ruff 检查。

## 复验

使用已有 DeerFlow 后端环境，并确保其中可导入 Flask、Flask-Login、Peewee 和 Loguru。

```bash
python scripts/verify_onecity_fault_recovery.py \
  --output /tmp/onecity-fault-recovery.json

# backend/ 下运行
python -m pytest -q tests/test_mobile_terminal_status.py \
  tests/test_mobile_run_status.py tests/test_mobile_concurrency_errors.py \
  tests/test_mobile_compliance_forwarding.py \
  tests/test_mobile_conversation_ownership.py tests/test_api_access_control.py
```

脚本创建临时账号目录和环回地址服务，结束时终止自身图服务并移除临时运行状态。原始账号、密码、令牌和原始事件不会写入结果 JSON。
