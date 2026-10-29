# 意图识别优化与真实模型评测（2026-10-22）

> 此页保留前一版合并调用优化的实验记录。当前已进一步改为十类语义识别；最新原题、合成回归与澄清结果见 [最新报告](intent-semantic-routing-20261022.md)。

后续对同学提供的实际案例检查已发现候选过滤覆盖缺口，见 [28 条原始案例检查](provided-scenarios-intent-check-20261022.md)。新案例经用户授权后已完成真实 Qwen 评测，仅 9/28（32.14%）匹配同学提供的预期场景，确认了当前硬候选过滤的覆盖不足。本文的 96/96 合成用例结果不能推广到这些新案例。

## 已完成

业务识别公共入口已改为一次结构化调用，同时输出任务、场景、参数和执行规划。保留 OneCityAgent 的 `planning_steps` 和既有返回协议；普通单场景、多场景请求通常只调用一次模型，格式或领域校验不通过时最多再修复一次。模型继续固定为 `Qwen3.6-35B-A3B`，关闭思考。

原版代码保存在 [本地备份清单](../scripts/intent_files_backup/20261022/manifest.json)，备份没有复制凭证。原实现也保留在 `routing/intent/classifier.py`，便于独立复现旧版和回滚；业务中间件通过 `routing.intent` 公共导出使用新的 `structured.py`。

## 先测基线，再改代码

1. 冻结 64 条主评测：32 条开发、32 条验证，覆盖 16 个现有候选场景、跨场景任务、未知领域、闲聊/能力查询、追问和补参。
2. 使用既有 DashScope 服务真实调用 Qwen，原版主评测得到 **62/64（96.88%）**。原版已包含本次会话前面完成的关闭思考开关修复。
3. 开发集上迭代结构化输出和参数保留，随后检查验证集。
4. 增加并冻结 20 条难例，检查否定目标、背景领域词、提示词干扰、复杂报告和多目标覆盖。原版 **12/20**。初版新流程场景正确，但长报告曾因无依据的表格场景回退而丢失规划，因此补上了生成前候选证据过滤。该组结果参与了改进，不能再称为独立测试集。
5. 最后冻结 12 条额外上下文验收用例，包含省略对象、新目标切换、纯续写和跨领域依赖。它们没有用于最终提示词调参。原版 **11/12**，新版 **12/12**。
6. 冻结最终源码，重新跑完整 96 条得到下表，并执行 223 项针对性代码回归。

所有问题和标签均为本次分析预设的合成用例，没有真实用户敏感数据，没有业务方独立标注。评测只运行对话轮次和意图识别，不执行技能或业务工具，也不发送真实 OneCityAgent 任务。

## 结果

| 指标 | 原版 | 新版 |
| --- | ---: | ---: |
| 主评测 64 条准确率 | 62/64，96.88% | 64/64，100% |
| 难例 20 条准确率 | 12/20，60% | 20/20，100% |
| 新增上下文验收 12 条 | 11/12，91.67% | 12/12，100% |
| 综合 96 条准确率 | 85/96，88.54% | 96/96，100% |
| 综合场景 micro-F1 | 93.68% | 100% |
| 平均模型调用次数 | 4.46 | 1.04 |
| 模型调用总数 | 428 | 100 |
| 平均识别耗时 | 3.28 秒 | 1.45 秒 |
| 识别耗时中位数 | 3.71 秒 | 1.52 秒 |
| 识别耗时 P95 | 5.29 秒 | 2.77 秒 |
| 显式标注参数检查 | 7/7 | 7/7 |
| 业务请求生成且转发真实规划 | 63/68 | 68/68 |
| 输入 token 总量 | 460229 | 71351 |
| 输出 token 总量 | 21718 | 15249 |

这轮平均耗时降低约 **55.9%**、调用数降低约 **76.6%**。普通单场景和多场景新版都为一次识别调用；需要先判断对话关系的新任务通常是“对话分类一次 + 业务识别一次”，所以整体平均不等于一。闲聊等控制请求可以不调用模型。

准确率口径是 **意图类型 + 场景集合完全匹配 + 指定用例的对话行为匹配**。多选一个无关场景或漏掉一个场景都算错；超时/异常保留在分母中。参数和规划单列，不能把“场景选对”当成全部输出正确。

平均耗时为单条识别流程耗时，不含并发排队、技能执行和最终回答；模型调用计数是逻辑调用，不是 SDK 重试的物理网络请求数。两版使用同一模型服务、关闭思考、并发度 2；原版维持配置中的 temperature=0.7 和 max_tokens=8192，新版识别绑定 temperature=0、max_tokens=3072。不同运行时段的服务负载、缓存和采样会影响时延，P95 来自本轮小样本，不能当作生产 SLA。

## 改动与准确率保护

- **一次输出完整结构。** 合并切分、场景、抽参、初步规划；去掉独立问题改写，单任务保留完整原文，多任务也保留全局原始请求。
- **先过滤，再校验。** 生成前按原有领域锚点筛选候选；返回后检查场景白名单、原始输入领域证据、非空任务和规划、参数名白名单。通用的分析、图表、风险和报告不自动增加专业场景。
- **区分背景和实际目标。** 提示词要求仅选择本轮实际处理目标，明确“查网络/交通法规”不等于还要分析网络包或道路交通。
- **对话控制保留。** 继续使用原有 pending_action、追问和补参解析。新业务任务明确指向上文时，在同一次识别中携带最多 2000 字符的上一轮用户任务来补全对象，不自动继承上一轮场景，不用模型生成的改写文本伪造领域证据。
- **有界修复。** 输出非法时只增加一次修复，服务调用异常直接回退规则，不再启动旧版多调用链。回退通过 `structured_fallback_*` 标记并记录日志。回退是降级处理，不能保证参数和规划完整；本轮最终 96 条没有触发回退。
- **修复合法 JSON 被破坏的问题。** 旧解析器无条件替换单引号，导致包含 `'purchase'` 等文本的合法 JSON 失败，甚至丢掉规划。现在先按标准 JSON 解析，失败后才尝试旧格式兼容。
- **异步调用不阻塞事件循环。** `abefore_agent` 中的同步对话模型调用通过 `asyncio.to_thread` 执行，保留已有行为。

原版的典型错误包括：把“只查询网络安全法律，不检查 PCAP”判成法律加网络分析；把包含领域词的普通翻译任务判成专业任务；对新的代码任务额外沿用之前的合同场景。逐条结果见下方文件。

## OneCityAgent 兼容性

仍使用 `intent_context.planning_steps` → `todo_routing_guidance` 的“任务规划”段落 → mobile adapter 的 `planner/task_planning` 分段 → `role=规划智能体`。JSON 的 `ordered_segments` 和 SSE 的 `segment` 协议保持原样。

最终 68 条新业务请求都生成了非空执行步骤，并逐条验证这些步骤完整进入规划分段；没有用“任务切分”回退冒充规划。该检查证明后端适配链兼容，不代表平台前端联调已经完成。规划的业务质量还需要真实业务样本复核，不能用分段存在率代替内容质量。

## 文件与复现

- [主评测用例](../scripts/intent_eval_cases.json)、[难例](../scripts/intent_eval_challenge_cases.json)、[额外上下文验收](../scripts/intent_eval_holdout_cases.json)
- [评测脚本](../scripts/eval_intent_recognition.py)
- [汇总指标与数据哈希](intent-recognition-eval-20261022/summary.json)
- [原版逐条结果](intent-recognition-eval-20261022/baseline.jsonl)、[新版逐条结果](intent-recognition-eval-20261022/optimized.jsonl)
- [原版备份清单](../scripts/intent_files_backup/20261022/manifest.json)、[最终候选源码哈希](../scripts/intent_files_backup/20261022/final-candidate-manifest.json)

备份、开发轮次记录及完整模型响应在本地 gitignored 目录 `scripts/intent_files_backup/20261022/` 和 `backend/.deer-flow/experiments/intent-20261022/`。可复现脚本、用例、压缩后的逐条结果和文档纳入代码变更。脚本拒绝覆盖已有结果文件，每次复测请使用新输出路径。

在配置好相同模型凭证的后端 Python 环境中执行：

```bash
python ../scripts/eval_intent_recognition.py --output /tmp/intent-new.jsonl
python ../scripts/eval_intent_recognition.py --cases ../scripts/intent_eval_challenge_cases.json --output /tmp/intent-challenge-new.jsonl
python ../scripts/eval_intent_recognition.py --cases ../scripts/intent_eval_holdout_cases.json --output /tmp/intent-context-new.jsonl
```

复测原版加参数 `--classifier-file ../scripts/intent_files_backup/20261022/baseline/backend/packages/harness/deerflow/routing/intent/classifier.py`。它直接加载备份分类器，不需要覆盖当前业务源码。完整 96 条合并集保存在本地备份的 `final-cases.json`，也可由上述三个用例文件的 `cases` 数组拼接得到。

运行回归：

```bash
python -m pytest tests/test_structured_intent.py tests/test_eval_intent_recognition.py tests/test_intent_classifier.py tests/test_intent_recognition_middleware.py tests/test_dialogue_action_flow.py tests/test_scene_skill_filter_middleware.py tests/test_scene_skill_routing.py tests/test_mobile_compliance_forwarding.py tests/test_compliance_intent_inputgate.py tests/test_lead_agent_model_resolution.py tests/test_model_factory.py tests/test_routing_hidden_steps_refresh.py tests/test_compliance_gates.py -q
```

## 生效、回滚和后续验证

没有修改模型配置、场景定义、合规决策逻辑或移动端协议。长驻 LangGraph/Gateway 进程需要重新加载源码；本次没有手动重启服务或发起完整业务执行。

若需恢复原识别流程，可从备份恢复 `routing/intent/__init__.py`、`routing/intent/classifier.py` 和 `agents/middlewares/intent_recognition_middleware.py`，然后重启后端。不要恢复本任务无关的用户改动，也不需要恢复或复制模型凭证。只切回原公共导出也能停用结构化业务识别。

当前证据支持“在本轮用例中更快且没有识别退化”，不支持生产请求必然 100% 正确。尤其参数仅有 7 个显式检查项，缺少完整参数标注集。下一步应使用脱敏的真实业务请求独立标注场景、参数和规划，扩大跨轮对话、隐含领域表达、长输入及异常服务测试；切换本地推理部署后也要用同一组用例重测。
