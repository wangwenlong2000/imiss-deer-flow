# 按阶段控制思考模式（2026-10-27）

当前策略是：意图识别、对话关系判断和标题等辅助任务关闭思考；业务执行默认开启，保留用户显式关闭的选项。模型继续固定为 Qwen3.6-35B-A3B，十类场景保持原配置。

## 核对与修改

主业务链路原本就使用 `thinking_enabled=True` 作为默认值；意图中间件原本就单独创建 `thinking_enabled=False` 的模型实例。前端 Flash 模式会显式关闭业务思考，Thinking、Pro、Ultra 开启。之前的完整流程测试请求明确传了 false，不能把那批测试的非思考状态概括为所有用户请求的状态。

本次实际修改的是子任务链路：

- `task_tool.py` 将父任务已解析的 `metadata.thinking_enabled` 传给执行器；元数据缺失时读取 configurable，最后采用默认 true。
- `SubagentExecutor` 继承该开关，所选模型不支持思考时保持关闭，替换原先无条件关闭的行为。
- 这不会自动启动子任务，也没有增加“判断复杂程度”的模型调用。普通业务是否开启仍由现有模式设置决定。

当前 `config.yaml` 已配置 `supports_thinking: true`、开启时的 `enable_thinking: true` 和 `thinking_budget: 2048`，本次没有修改配置文件、采样参数或模型名称。关闭思考时不发送该预算。

修改前备份位于 [备份清单](../scripts/intent_files_backup/20261027-stage-thinking-25759e4c/manifest.json)。生产文件和本地配置已归档；意图中间件测试文件的原始内容另存于同目录 `additional-test-before.tar.gz`，来源和哈希记录在补充清单中。主归档权限 0600、目录 0700。

## 验证

相关测试分开运行，避免现有子任务测试模块的隔离 mocks 影响其他模块：

| 检查 | 结果 |
| --- | --- |
| 模型工厂、主业务默认值与显式关闭、意图固定关闭、任务参数传递 | [49 项通过](stage-thinking-eval-20261027/unit-main.log) |
| 子任务执行器继承、模型能力保护及原有执行路径 | [25 项通过](stage-thinking-eval-20261027/unit-subagent.log) |
| 框架依赖边界与子任务超时配置 | [39 项通过](stage-thinking-eval-20261027/unit-boundary.log) |
| 修改文件静态检查 | [Ruff 通过](stage-thinking-eval-20261027/ruff.log) |

共 113 项通过。有一条现有 `datetime.utcnow()` 弃用提示，与本次思考开关无关。

另通过当前配置向 DashScope 发出两条自建合成问题，记录真实 HTTP 请求的白名单字段，未保存凭证、请求头或思考正文，结果见 [provider-live.json](stage-thinking-eval-20261027/provider-live.json)：

- 意图识别：`enable_thinking=false`，无 `thinking_budget`，一次结构化调用正确识别为道路交通。
- 业务推理：`enable_thinking=true`，`thinking_budget=2048`；服务端返回 **1297 个 reasoning tokens**。合成合作效率题的最终结果为乙单独 30 天、甲离开后还需 22.5 天，与预期一致。

这证明实际接口使用了不同的思考设置，不代表开启思考后的全过程准确率已经完成对照验证。两条问题不同，耗时不能拿来比较思考模式的性能。开启思考的质量收益、业务总时延和本地部署性能仍需用同组完整任务评估。

后台空闲时已重启图服务，使子任务设置继承生效。现有浏览器若保存了 Flash 模式，仍会关闭业务思考；选择 Thinking、Pro 或 Ultra 即开启后续业务思考，意图识别继续关闭。

后续已进行[六组开启思考的完整流程测试](full-flow-thinking-service-check-20261027.md)，发现了计算门禁与当前接口工具选择的兼容缺陷及其他交付问题。上文113项测试与单次接口验证没有覆盖这些全过程故障，不能当作流程整体可靠性的证明。
