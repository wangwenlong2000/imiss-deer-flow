# `compliance_detectors` 集成说明

本文说明 workspace 顶层 `compliance_detectors` 中的三个检测器如何接入
DeerFlow 合规链路。设计遵循《合规检测实现总览与异构检测器接入指南》：检测器只做
识别，所有技术实现收敛为 `DetectionHit`，检测输入保持冻结，场景和处置仍由
`SceneResolver`、`IntentGuard` 与 `PolicyMatrix` 负责。

## 1. 接入结果

| 交付实现 | DeerFlow detector_id | 违规类型 | 闸门 | 适配器 |
|---|---|---|---|---|
| `HardcodedCredDetector` | `entropy_hardcoded_cred` | `hardcoded_cred` | InputGate | inprocess |
| `TextIdDetector` | `ner_text_id` | `text_id` | InputGate / ContextGate / OutputGate | inprocess |
| `ConfidentialDetector` | `llm_confidential` | `confidential` | InputGate / ContextGate / OutputGate | inprocess |

三个 detector_id 沿用 `config/compliance/detectors.yaml` 中预留的 ID。其本地算法
均为纯标准库规则实现；`ner_text_id` 与 `llm_confidential` 在中心配置中启用了
主进程边界复核，复用 `config.yaml` 的聊天模型。所有三个检测器已经显式启用；整个
合规系统是否运行仍由 `config.yaml` 的 `compliance.enabled` 控制。

`hardcoded_cred` 按契约硬约束只声明 InputGate。`text_id` 与 `confidential` 的规则
不依赖单一数据类型，因此 `data_types: null`，避免 `DetectionUnit.data_type` 缺失时
产生静默漏检。具体处置没有写入 manifest 或检测器代码，仍使用现有
`config/compliance/policy_matrix.yaml`。

## 2. 文件布局

```text
backend/packages/harness/deerflow/compliance/detectors/
├── delivered_rules/
│   ├── adapter.py                 # 唯一的双向契约转换层
│   ├── configured_llm.py          # DeerFlow 已配置模型的边界复核适配器
│   └── vendor/                    # compliance_detectors 算法快照
├── entropy_hardcoded_cred/
│   ├── detector.py
│   └── manifest.yaml
├── ner_text_id/
│   ├── detector.py
│   └── manifest.yaml
└── llm_confidential/
    ├── detector.py
    └── manifest.yaml
```

原始 `base.py`、`hardcoded_cred.py`、`text_id.py`、`confidential.py` 独立保存在
`vendor/`。导入时仅统一为仓库 LF 行尾；原始 CRLF 文件的 SHA-256 记录在
`vendor/SHA256SUMS`。这样可以区分“交付算法”和“DeerFlow 适配逻辑”，后续升级时
也能明确复核算法漂移。

## 3. 输入转换

交付检测器原来消费 normalized JSONL 行；DeerFlow 只允许检测器读取冻结的
`DetectionUnit`。`unit_to_delivery_sample()` 进行以下映射：

| DetectionUnit | 交付 normalized row |
|---|---|
| `unit_id` | `sample_id`、`data_id` |
| `gate` | `gate`、`trigger_gate` |
| `data_type` | `data_type` |
| 按顺序排列的 `text_items[].text` | 以换行连接的 `content_text` |
| `field_items[].path/value` | 保留点号路径的 `features` |

转换结果不包含 `is_positive`、`final_violation_type`、期望动作、数据集切分或样本
标签，防止评测信息进入线上判断。`raw`、Agent state、消息历史、权限对象和场景提示
也不会传给交付算法。

## 4. 输出转换

交付结果仅在 `is_hit=true` 时转换为一个 `DetectionHit`：

- `violation_type` 由各 wrapper 固定，不能由交付结果注入其他类型；
- `confidence` 被限制到 `[0, 1]`，再映射为合法 severity；
- `matched_rules` 被限制数量和长度，作为稳定 `reason_code` 与有限 evidence；
- `suggested_action` 被明确丢弃，不能越过 Policy Matrix 决定拒答或脱敏；
- `content_text` 的有效 start/end 或可复现字面量转换为 `char_span`；
- 结构化字段转换为精确 `field_path`；
- RiskLocation 文本和 evidence 均有长度/数量上限，不复制整篇敏感文档。

每个命中仍会经过 in-process adapter 的 `validate_hit()`，Registry 和 Engine 还会
再次按 manifest 检查违规类型与闸门声明。某个检测器异常时，Engine 会把错误写入
diagnostics，其他检测器继续运行。

## 5. 主进程 LLM 边界复核

交付包原有 `external/llm_adapter.py` 自行读取 `COMPLIANCE_LLM_*` 环境变量并写磁盘
缓存。集成后不再建立第二套模型配置，而由 `configured_llm.py` 惰性调用：

```python
create_chat_model(name=model_name, thinking_enabled=False, timeout=7)
```

这里的 `model_name` 不再来自 detector 静态参数，而是 Agent 构建时已经解析完成的
当前对话模型。Lead Agent 把同一个模型名绑定到 InputGate、ContextGate 和 OutputGate，
Engine 再通过 `DetectionRequest.model_name`、`DetectContext.model_name` 传给检测器；
subprocess/HTTP adapter 的 `ctx` JSON 也包含该字段。因此用户在对话中选择哪个模型，
合规 LLM 复核就使用哪个模型，不会再因空值回退到 `config.yaml` 的第一个模型。

模型在 Registry 扫描和 detector setup 阶段都不会创建，只有本地规则把样本判定为
边界情况时才首次初始化并调用。若独立测试或异构调用没有提供
`ctx.model_name`，则跳过 LLM 复核并保留本地规则结果：

- `text_id`：规则分数处于 4–8，或存在需要复核的完整人名候选；
- `confidential`：出现弱信号但尚未形成本地强组合；
- `hardcoded_cred`：始终不调用 LLM，避免把潜在凭证发送给模型。

主进程复核的边界控制：

- 输入正文最多 2500 字符，features 最多 2500 字符；
- 输出最多 512 token，temperature 绑定为 0；
- 单次模型 transport timeout 为 7 秒，两个可复核 detector 的最坏总等待不超过约
  14 秒，三闸门总预算相应设为 15 秒；
- 只解析 JSON object，confidence 限制在 `[0,1]`，风险片段限制数量和长度；
- 不缓存 prompt/response，不在日志写入 prompt、响应或原始敏感正文；
- 调用、解析或模型初始化失败时记录不含 payload 的 warning，并按交付逻辑回退本地
  规则结果。

由于这两个 detector 现在可能访问模型，manifest 的 `cost_hint` 已从 `light` 调整为
`medium`。这仍是同步主进程调用，会增加边界样本的端到端延迟；生产发布应结合实际
模型延迟、并发和数据出域要求复核配置。

## 6. 配置与运行

三个 detector 的开关位于 `config/compliance/detectors.yaml`：

```yaml
- { id: entropy_hardcoded_cred, enabled: true }
- id: ner_text_id
  enabled: true
  params:
    use_llm: true
    llm_max_content_chars: 2500
    llm_max_output_tokens: 512
    llm_timeout_seconds: 7
- id: llm_confidential
  enabled: true
  params:
    use_llm: true
    llm_max_content_chars: 2500
    llm_max_output_tokens: 512
    llm_timeout_seconds: 7
```

manifest 是能力声明的唯一事实来源，中心配置只负责开关和参数覆盖。关闭单个检测器
只需将对应 `enabled` 改为 false；无需修改 Engine、Router、Policy 或中间件。

严格注册和定向回归：

```powershell
cd backend
$env:PYTHONPATH = ".;packages/harness"
python -m pytest tests/test_compliance_delivered_detectors.py -q
python -m pytest tests/test_compliance_*.py -q
```

定向测试覆盖：三个实现满足 `Detector` 协议、strict Registry 可发现且已启用、
hardcoded 正例与占位符反例、text_id 实体关系正例与匿名化近负例、confidential
结构化组合与公开汇总反例、精确 RiskLocation、Engine 到既有策略矩阵的端到端处置，
对话模型名在三道闸和异构 JSON 契约中的透传、模型惰性选择、JSON 结果转换，以及
模型缺失/失败时不泄露 payload 的本地规则回退。

## 7. 本次验证记录

当前 workspace（2026-08-05）的验证结果：

- 新增定向测试：`14 passed`；
- 不依赖 Agent Web 运行时的合规核心回归：`319 passed, 37 skipped, 1 deselected`；
- 原始 `compliance_detectors/tests`：`16 passed`；
- 四个 vendor 文件去除 CRLF/LF 与末尾换行差异后，内容逐文件一致；
- Python AST 解析与 `git diff --check` 通过。

本机未安装 `langchain`、`langgraph`、`fastapi` 与 `ruff`，因此真实模型工厂和 9 个依赖完整 Agent
运行时的合规测试文件无法在收集阶段导入，另有 1 个文件内用例因 `langgraph` 缺失而
排除，ruff 也未能执行。这些是环境依赖限制，不是测试断言失败；在安装项目锁定依赖
的 CI/开发环境中仍应补跑完整 `python -m pytest tests/test_compliance_*.py` 与
`make lint`。

LLM 路径通过不依赖网络的 fake configured model 验证了对话模型一致性、prompt 调用、
JSON 解析、命中转换、失败回退和日志脱敏；本次没有向真实配置模型发送测试数据。

在规则预热 10 次后各测 100 次，当前机器的本地规则正例延迟如下（未包含边界 LLM
网络调用，仅供回归参考，不作为生产 SLA）：

| 路径 | P50 | P95 | max |
|---|---:|---:|---:|
| `entropy_hardcoded_cred` | 0.0275 ms | 0.0372 ms | 0.1053 ms |
| `ner_text_id` | 0.1851 ms | 0.1970 ms | 0.2496 ms |
| `llm_confidential` 本地规则 | 0.0619 ms | 0.0657 ms | 0.0970 ms |
| InputGate 多类型 Engine（4 hits） | 0.5293 ms | 0.6347 ms | 0.9444 ms |
| InputGate Engine 负例（0 hits） | 0.2650 ms | 0.3383 ms | 0.3824 ms |

由于完整流式中间件依赖不可用，本次没有重测 `compliance_retract` 事件延迟；本次也未
改动输出闸门或 SSE 实现。发布前应在完整运行时继续执行现有严格流式与 retract 测试。

## 8. 请求链路

```text
Input/Context/Output normalizer
  -> frozen DetectionUnit
  -> Registry + Router (manifest gate/data_type/cost)
  -> delivered_rules.adapter
  -> vendored compliance_detectors rule
     -> boundary only: configured_llm -> config.yaml model -> JSON review
  -> bounded DetectionHit
  -> IntentGuard (InputGate only)
  -> SceneResolver + PolicyMatrix
  -> action / audit / SSE metadata
```

本次接入没有修改 Engine、Router、场景解析、策略矩阵、审计或三闸门挂载位置，因而
保留了现有的失败隔离、输出撤回、持久化前处置和多标签累加行为。
