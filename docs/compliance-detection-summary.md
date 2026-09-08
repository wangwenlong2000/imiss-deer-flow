# 违规检测实现总结

## 一、整体概述

当前项目中的违规检测已经实现为一套完整的合规检测子系统，而不是单独的关键词判断函数。整体设计是：

> 一个统一检测引擎 + 一套可插拔检测器 + 一套违规处置矩阵 + 三道检测闸门。

当前已经接入的实际检测器是 **model_tfidf_knn**，覆盖：

- 第 8 类：视频监控点位元数据泄露（video_meta_leak）
- 第 9 类：模型输出再识别风险（re_identify）
- 第 10 类：城市治理领域专有敏感信息（domain）

第 1–7 类已经预留了统一接入结构，但目前在配置中关闭，尚未实现具体检测器。

完整检测链路：

    用户输入 / 上传文件 / 工具结果 / 模型输出
                    │
                    ▼
              Normalizer
          转换为 DetectionUnit
                    │
                    ▼
            ComplianceEngine
      路由 → 检测 → 意图 → 场景 → 矩阵 → 动作 → 审计
                    │
          ┌─────────┼─────────┐
          ▼         ▼         ▼
      InputGate ContextGate OutputGate

核心原则是“识别”和“处置”分离：

- 检测器只负责判断检测到了什么风险。
- 检测器不负责决定拒答、脱敏还是放行。
- 具体处置由“违规类型 × 检测闸门 × 使用场景”矩阵决定。

## 二、文件结构

### 1. 核心合规模块

目录：backend/packages/harness/deerflow/compliance/

    compliance/
    ├── __init__.py
    ├── contract.py                  检测器公共契约
    ├── types.py                     引擎内部数据结构
    ├── engine.py                    统一检测引擎
    ├── registry.py                  检测器发现和注册
    ├── router.py                    候选检测器路由
    ├── policy.py                    违规处置矩阵
    ├── scene.py                     场景解析
    ├── intent.py                    意图和权限处理
    ├── actions.py                   动作执行
    ├── audit.py                     JSONL 审计
    ├── preflight.py                 启动自检
    ├── runtime.py                   运行时辅助方法
    │
    ├── normalizers/
    │   ├── base.py
    │   ├── user_input.py            用户问题和上传文件
    │   ├── skill_result.py          工具和技能结果
    │   └── llm_output.py            模型输出
    │
    ├── adapters/
    │   ├── inprocess.py             进程内 Python 检测器
    │   ├── subprocess_cli.py        子进程检测器
    │   ├── http_service.py          HTTP 检测服务
    │   └── serde.py                 JSON 序列化
    │
    └── detectors/
        └── model_tfidf_knn/
            ├── manifest.yaml
            ├── detector.py
            ├── row_builder.py
            └── vendor/model_detectors/

### 2. 中间件

目录：backend/packages/harness/deerflow/agents/middlewares/

    compliance_input_gate_middleware.py
    compliance_context_gate_middleware.py
    compliance_output_gate_middleware.py
    tool_error_handling_middleware.py

### 3. 配置文件

    config.example.yaml
    config/compliance/
    ├── detectors.yaml
    └── policy_matrix.yaml

关键文件：

- config.example.yaml
- config/compliance/detectors.yaml
- config/compliance/policy_matrix.yaml

### 4. 模型、数据集和验证脚本

    models/compliance/
    └── ml_detector_0624_fresh.json

    datasets/compliance/normalized/0624_supported_split/
    ├── train.jsonl
    ├── test.jsonl
    ├── all_supported_deduplicated.jsonl
    └── split_summary.json

    scripts/
    ├── prepare_compliance_assets.py
    ├── run_compliance_model_eval.py
    ├── run_compliance_gate_eval.py
    ├── verify_content_text_rule.py
    └── export_detector_schema.py

models/compliance/ 被 .gitignore 忽略，模型权重不会随 Git 提交，需要通过资源准备脚本解压。

## 三、三道检测闸门

| 闸门 | 执行时机 | 检测对象 | 当前主要用途 |
|---|---|---|---|
| InputGate | Agent 开始执行前 | 用户问题、上传文件 | 防止危险请求或敏感文件进入系统 |
| ContextGate | 工具调用完成、模型读取前 | SkillResult、检索证据 | 防止敏感检索结果进入模型上下文 |
| OutputGate | 模型生成后 | AIMessage 最终回答 | 防止模型输出泄露敏感信息 |

### 1. InputGate

实现文件：backend/packages/harness/deerflow/agents/middlewares/compliance_input_gate_middleware.py

用户问题通过 before_agent 检测。

InputGate 的重要规则是：

> 敏感实体命中不等于违规，必须结合高风险意图判断。

例如“请帮我把这批手机号脱敏”和“请提取所有手机号并公开发布”可能包含相同的敏感实体，但第二个请求具有明显的高风险意图。

InputGate 会读取 IntentRecognitionMiddleware 产生的意图信息：

    IntentInfo(
        intent="...",
        scene_hint="...",
        high_risk=True,
        confidence=0.95,
    )

因此 InputGate 必须挂载在意图识别之后，具体顺序在 backend/packages/harness/deerflow/agents/lead_agent/agent.py 中控制。

如果检测结果包含 refuse，InputGate 会直接返回 AIMessage，并跳转到结束节点，从而终止本轮 Agent 执行。该消息同时携带 `response_metadata.compliance`（包括闸门、场景、违规类型、处置动作、依据和审计编号），因此前端会显示结构化合规卡片，刷新页面后仍可恢复；由于模型未运行，`transient_exposure_possible` 为 `false`。

#### 上传文件检测

上传文件是在上传接口中调用 scan_upload_paths，而不是由 Agent 中间件自动完成。

调用位置：backend/app/gateway/routers/uploads.py

    文件上传
      ↓
    文件保存
      ↓
    合规检测
      ↓
    检测通过：返回上传成功
    检测拒绝：返回上传失败

当前只读取文本类文件：

    .txt .md .csv .tsv .json .jsonl .yaml .yml
    .log .py .sql .xml .html

单个文件最多读取 200000 字节。二进制文件目前会跳过，需要后续专门的文件检测器。

### 2. ContextGate

实现文件：backend/packages/harness/deerflow/agents/middlewares/compliance_context_gate_middleware.py

ContextGate 包装所有工具调用：

    wrap_tool_call(...)
    awrap_tool_call(...)

只有返回值是 ToolMessage，且内容能够解析为 SkillResult 时才会进行检测。普通工具文本、控制流对象等会原样放行。

SkillResult 的主要结构：

    {
      "schema_version": "1.0",
      "skill_name": "some_skill",
      "result": {
        "display_text": "展示给模型的文本",
        "evidence": [
          {
            "evidence_id": "e-001",
            "description": "证据描述",
            "data": {
              "camera_id": "CAM-001"
            },
            "metadata": {
              "score": 0.95
            }
          }
        ]
      }
    }

每条 evidence 会被转换为一个 DetectionUnit：

- description、title、字符串类型的 data 转为 TextItem。
- 字典或列表类型的 data 转为 FieldItem。
- metadata 转为 FieldItem。
- 嵌套结构使用点号路径表示，例如 data.camera_id、metadata.score。

ContextGate 支持：

- budget_ms：检测总时间预算。
- max_units：最多检测多少个证据单元。
- 按证据 score 从高到低优先检测。
- 超过限制时记录 warning，不能静默丢弃证据。

如果命中 refuse，会保留 SkillResult 外层结构，但清空 evidence、findings，并替换 display_text。如果命中 desensitize、rewrite 或 aggregate，则只修改风险字段或风险文本。

### 3. OutputGate

实现文件：backend/packages/harness/deerflow/agents/middlewares/compliance_output_gate_middleware.py

OutputGate 实现两层保护：

    第一层：wrap_model_call 中直接净化模型响应
    第二层：after_model 作为兜底检查

主要流程：

    模型返回 AIMessage
          ↓
    提取可见文本
          ↓
    跳过带 tool_calls 的中间消息
          ↓
    调用 ComplianceEngine
          ↓
    无风险：原样返回
    有风险：执行处置动作
          ↓
    发出 compliance_retract 事件
          ↓
    返回同 ID 的安全版 AIMessage

同一个消息的 ID 会被保留，使 LangGraph reducer 替换原消息，而不是追加新消息。安全版消息的 response_metadata.compliance 会包含：

    {
      "gate": "OutputGate",
      "actions": ["warn", "manual_review"],
      "violation_types": ["re_identify"],
      "audit_ref": "audit-xxxxxxxx",
      "basis": ["个人信息保护法 §73(4) 去标识化/匿名化"],
      "retracted": true
    }

OutputGate 使用 compliance_retract 自定义事件，事件中包含 message_id、violation_types、action、replacement、notice、audit_ref 和 basis。

前端收到事件后会立即替换页面上的原始回答，随后用后端返回的安全版消息更新正常消息流。刷新页面后仍然只显示安全版本。

当前限制：scan_increment() 接口已经实现，可以按 interval_chars 做增量检测，但当前仓库中没有发现生产流式层实际调用它的代码。因此目前生产链路主要依赖完整模型消息返回后的检测和撤回。

## 四、核心数据契约

定义文件：backend/packages/harness/deerflow/compliance/contract.py

该文件只依赖 Python 标准库，检测器只需要依赖它。

### 1. DetectionUnit

检测输入被统一为：

    DetectionUnit(
        unit_id="...",
        gate="ContextGate",
        data_type="surveillance",
        text_items=(...),
        field_items=(...),
        raw={...},
    )

其中：

- TextItem：自然语言文本。
- FieldItem：结构化字段。
- raw：只读的来源信息。
- gate：当前检测闸门。
- data_type：数据类型，用于检测器路由。

### 2. DetectionHit

检测器只能返回 DetectionHit：

    DetectionHit(
        detector_id="model_tfidf_knn",
        violation_type="re_identify",
        confidence=0.91,
        severity="high",
        risk_locations=(...),
        reason_code="ml_tfidf_knn_classifier",
        evidence={...},
        basis=(),
    )

| 字段 | 作用 |
|---|---|
| detector_id | 产生结果的检测器 |
| violation_type | 违规类型 |
| confidence | 模型置信度，范围 0–1 |
| severity | 风险等级 |
| risk_locations | 风险位置，例如字段路径、字符区间 |
| reason_code | 检测原因编码 |
| evidence | 审计、解释和调试数据 |
| basis | 检测器提供的法规依据 |

当前模型检测器不填写 basis，因为模型只能判断“像哪类风险”，不能直接判断违反哪条法律。法规依据由策略矩阵统一补充。

### 3. 契约校验

所有检测器结果都会被校验：

- 是否为合法 DetectionHit。
- violation_type 是否属于十类违规。
- severity 是否合法。
- confidence 是否在 0–1。
- detector_id 是否为空。
- 风险位置类型是否合法。

非法结果不会进入策略矩阵。

## 五、ComplianceEngine 执行流程

核心文件：backend/packages/harness/deerflow/compliance/engine.py

引擎每次请求执行以下步骤：

### 1. 规范化

规范化由调用方完成，引擎接收统一的 DetectionUnit。

### 2. 路由

根据以下条件选择检测器：

- 当前闸门。
- 数据类型。
- 检测器 manifest 中声明的能力。
- 检测器开销等级。

轻量检测器优先执行，保证 ContextGate 的性能。

### 3. 执行检测

引擎会：

- 对每个 DetectionUnit 调用候选检测器。
- 将 unit 的 raw 数据复制为只读对象。
- 检查整体预算。
- 记录每个检测器耗时。
- 捕获单个检测器异常。
- 删除检测器在错误闸门上产生的结果。

单个检测器失败不会导致整个合规系统失败。

### 4. 意图过滤

IntentGuard 只对 InputGate 生效：

- 高风险意图：保留命中。
- 低风险意图：普通敏感实体命中会被降级。
- hardcoded_cred、illegal_content、political 三类底线风险不会被普通意图放行。
- 没有意图信息时不会静默放行，而是保留命中并记录 warning。

### 5. 场景和矩阵

场景解析由 SceneResolver 完成。当前一期 SceneResolver 总是返回 None，因此统一使用 _unknown。

### 6. 动作和审计

引擎根据违规类型、闸门和场景查询矩阵，合并动作、补充法规依据，并将命中结果写入审计日志。

## 六、检测器注册和解耦

注册实现文件：backend/packages/harness/deerflow/compliance/registry.py

系统会扫描：

    backend/packages/harness/deerflow/compliance/detectors/*/manifest.yaml

检测器能力写在自己的 manifest.yaml 中，中心配置只负责开关和参数覆盖。

当前模型检测器的能力声明：

    contract_version: "1.0"
    detector_id: model_tfidf_knn
    adapter: inprocess
    entry: deerflow.compliance.detectors.model_tfidf_knn.detector:ModelTfidfKnnDetector

    violation_types:
      - video_meta_leak
      - re_identify
      - domain

    gates:
      video_meta_leak: [ContextGate, OutputGate]
      re_identify: [OutputGate]
      domain: [ContextGate, OutputGate]

    data_types: null
    cost_hint: light

注册时会检查契约版本、违规类型、闸门、开销等级、参数和检测器 ID。heavy 检测器如果声明挂载到 ContextGate，会在启动时被拒绝。

支持三种检测器适配方式：

1. inprocess：适合纯 Python、轻量、无复杂依赖的检测器。
2. subprocess_cli：适合依赖 PyTorch、OpenCV、spaCy 等重量依赖的检测器。
3. http_service：适合独立 GPU 服务、已有检测服务或需要独立扩缩容的模型。

## 七、当前模型检测器

目录：backend/packages/harness/deerflow/compliance/detectors/model_tfidf_knn/

### 1. 模型方法

当前模型使用：

    字符 n-gram + Token n-gram
            ↓
          TF-IDF
            ↓
           kNN
            ↓
         余弦相似度

主要特点：

- k=1。
- 纯 Python 标准库。
- 不依赖 scikit-learn、PyTorch 等第三方机器学习库。
- 支持 none、video_meta_leak、re_identify、domain 四类标签。

特征包括字符 2–5 gram、token unigram/bigram、字段路径、URL、IPv4、时间日期、经纬度、长数字、长十六进制字符串、路径关系、字段数量、文本长度和数字比例。

相关文件：

- detector.py
- row_builder.py
- vendor/model_detectors/feature_extractor.py
- vendor/model_detectors/tfidf_knn.py

### 2. 输入特征构造

检测器会把 DetectionUnit 转成：

    {
      "data_type": "surveillance",
      "gate": "ContextGate",
      "content_text": "...",
      "features": {
        "data.camera_id": "CAM-001",
        "metadata.score": 0.95,
        "evidence-001": "..."
      }
    }

构造规则：

1. FieldItem 使用字段路径作为 key。
2. TextItem 使用 source 作为 key。
3. 字段先拼接，文本后拼接。
4. 重复 key 会追加 item_id，避免覆盖。

### 3. content_text 规则

实现文件：backend/packages/harness/deerflow/compliance/detectors/model_tfidf_knn/row_builder.py

当前使用 0624 规范：

- 如果只有一个字段，并且字段名是 content，直接使用原始值。
- 其他情况按照插入顺序拼接：

    key1: value1
    key2: value2
    key3: value3

该规则已经通过 282 条数据回归验证：content_text 精确匹配 282/282，重建后的模型预测结果与原始结果一致。

验证命令：

    make compliance-verify-content-text

### 4. 置信度和相似度

配置默认值：

    min_confidence: 0.35
    min_similarity: 0.20

处理逻辑：

- 模型预测为 none：不产生检测命中。
- 非 none 但置信度低于 min_confidence：降为 none。
- 最近邻相似度低于 min_similarity：不丢弃命中，只将严重度降为 low。
- 低相似度命中进入告警和人工复核流程，避免否定语句误判时直接拒答。

严重度映射：

    re_identify:
      confidence >= 0.8 → high
      否则 → medium

    其他类型：
      confidence >= 0.9 → high
      confidence >= 0.6 → medium
      否则 → low

### 5. 模型性能

项目文档记录的离线结果：

    训练样本：226
    测试样本：56
    accuracy：0.9821
    macro_f1：0.9859
    单条推理：约 13ms
    模型加载：约 0.2s

模型按绝对路径缓存，每个模型文件只加载一次，多个检测器实例共享同一个只读模型。

## 八、策略矩阵

策略矩阵文件：config/compliance/policy_matrix.yaml

系统支持十类违规：

| 编号 | 类型 |
|---|---|
| 1 | struct_id：结构化标识符泄露 |
| 2 | geo_loc：精确地理位置泄露 |
| 3 | hardcoded_cred：硬编码凭证泄露 |
| 4 | illegal_content：违法有害内容 |
| 5 | political：政治敏感内容 |
| 6 | text_id：自由文本标识符泄露 |
| 7 | confidential：商业秘密与内部敏感 |
| 8 | video_meta_leak：视频监控点位元数据 |
| 9 | re_identify：模型输出再识别风险 |
| 10 | domain：城市治理领域专有敏感信息 |

支持五类场景：

    self_use
    internal_org
    cross_org
    public_release
    research_anon

一期没有真实的使用权限和公开范围信息，所有请求默认进入 _unknown 场景。

### 一期 _unknown 处置规则

底线三类：

    hardcoded_cred
    illegal_content
    political

默认处置：

    [refuse, warn]

其他七类默认处置：

    [warn, manual_review]

也就是保留内容、追加合规提示、写入审计并进入人工复核，不自动脱敏或拒答。

### 当前模型检测器覆盖范围

| 类型 | InputGate | ContextGate | OutputGate |
|---|---|---|---|
| video_meta_leak | 不适用 | 检测 | 检测 |
| re_identify | 不适用 | 不适用 | 检测 |
| domain | 不适用 | 检测 | 检测 |

re_identify 被硬性限制为只允许在 OutputGate 检测。

## 九、动作执行方式

实现文件：backend/packages/harness/deerflow/compliance/actions.py

支持动作：

    allow
    warn
    report
    role_check
    manual_review
    aggregate
    desensitize
    rewrite
    block_storage
    refuse

### warn

只告警，不改变正文。OutputGate 会追加包含风险类型、处置动作、法规依据和审计编号的合规提示。

### desensitize

根据 RiskLocation 对风险字段或文本进行掩码。例如：

    camera_id = CAM-001

可能变为：

    camera_id = *******

如果无法定位具体风险位置，不会原样放行，而是保守地降级为拒绝。

### rewrite

一期没有再次调用 LLM 进行语义重写，而是掩码风险字段并添加“合规改写”提示。

### aggregate

将明细内容改成统计性结果，并添加聚合提示。

### refuse

使用固定拒答文案：

    【合规拦截】该内容包含合规风险，已被安全策略拦截，无法展示。
    如需进一步处理，请联系数据合规负责人并说明业务场景。

动作合并时按照强度排序，强动作不会被弱动作覆盖。例如 [warn, rewrite, refuse] 最终执行 refuse。

## 十、审计日志

实现文件：backend/packages/harness/deerflow/compliance/audit.py

默认配置：

    audit:
      enabled: true
      path: backend/.deer-flow/compliance/audit
      retain_days: 90

日志按 UTC 日期保存为 JSONL：

    compliance-20260730.jsonl

每次命中会记录：

- audit_ref。
- 请求 ID、线程 ID。
- 闸门和场景。
- 最终动作和每种违规对应的动作。
- 法规依据。
- 检测器、置信度、严重度。
- 风险位置。
- 模型证据和最近邻相似度。
- 检测耗时、异常和截断信息。

证据文本默认限制在 2000 字符以内，避免审计日志成为违规原文的第二份完整副本。

## 十一、故障处理

### 1. 检测器故障

单个检测器失败不会影响其他检测器。系统会记录 detector_errors 和 warning，然后继续执行其他检测器。

### 2. fail_mode

每个闸门都支持：

    fail_mode: closed

或：

    fail_mode: open

closed 表示检测失败时阻断内容；open 表示检测失败时允许内容继续，但会明确记录“内容未经检查”。默认使用 closed。

### 3. 启动自检

实现文件：backend/packages/harness/deerflow/compliance/preflight.py

启动自检会构建引擎、加载策略矩阵、检查检测器注册，并用无害文本对每个启用的闸门执行一次检测，从而提前触发模型惰性加载。

如果资源缺失：

- strict_startup: false：记录严重日志，但不挂载合规闸门。
- strict_startup: true：直接拒绝启动。

## 十二、如何启用和运行

### 1. 准备模型资源

    make compliance-assets

该命令会校验交付包的 SHA256SUMS，复制 vendor 推理代码和模型，并准备合规数据集。

模型最终应位于：

    models/compliance/ml_detector_0624_fresh.json

当前工作区中已经有数据集，但 models/compliance/ 下没有实际模型文件，需要先准备模型交付压缩包或单独提供模型权重。

### 2. 生成配置

    make config

编辑项目根目录的 config.yaml：

    compliance:
      enabled: true

      gates:
        input:
          enabled: true
          fail_mode: closed

        context:
          enabled: true
          fail_mode: closed
          budget_ms: 400
          max_units: 32

        output:
          enabled: true
          fail_mode: closed
          mode: stream_retract
          incremental_scan:
            enabled: true
            interval_chars: 200

      detectors_config_path: "config/compliance/detectors.yaml"
      policy_matrix_path: "config/compliance/policy_matrix.yaml"

      scene:
        resolver: null
        fallback_key: _unknown

      audit:
        enabled: true
        path: "backend/.deer-flow/compliance/audit"
        retain_days: 90

### 3. 确认检测器开关

config/compliance/detectors.yaml 当前配置：

    detectors:
      - id: model_tfidf_knn
        enabled: true
        params: {}

第 1–7 类当前都是 enabled: false。

### 4. 重启服务

修改 YAML 后需要重启后端或 LangGraph 服务，配置不会自动热加载。Docker 环境还需要确认挂载：

    - ../models:/app/models
    - ../config:/app/config

### 5. 验证命令

    # 验证 content_text 拼接规则
    make compliance-verify-content-text

    # 验证模型离线指标
    make compliance-eval-model

    # 通过完整 ComplianceEngine 做端到端评测
    make compliance-eval-gates

    # 导出检测器 JSON Schema
    make compliance-export-schema

## 十三、如何新增检测器

新增检测器不需要修改 engine.py、router.py 或 policy.py。

### 1. 创建目录

    backend/packages/harness/deerflow/compliance/detectors/regex_struct_id/

### 2. 添加 manifest

    contract_version: "1.0"
    detector_id: regex_struct_id
    adapter: inprocess
    entry: deerflow.compliance.detectors.regex_struct_id.detector:StructIdDetector

    violation_types:
      - struct_id

    gates:
      struct_id:
        - InputGate
        - ContextGate
        - OutputGate

    data_types: null
    cost_hint: light

    default_params:
      pattern: "..."

### 3. 实现 Detector 协议

    from deerflow.compliance.contract import (
        DetectContext,
        DetectionHit,
        DetectionUnit,
        RiskLocation,
    )

    class StructIdDetector:
        detector_id = "regex_struct_id"

        def setup(self, params):
            self.pattern = params["pattern"]

        def detect(self, unit: DetectionUnit, ctx: DetectContext):
            hits = []

            for item in unit.text_items:
                if self.pattern in item.text:
                    hits.append(
                        DetectionHit(
                            detector_id=self.detector_id,
                            violation_type="struct_id",
                            confidence=0.95,
                            severity="high",
                            risk_locations=(
                                RiskLocation(
                                    kind="field_path",
                                    locator=item.source,
                                    text=self.pattern,
                                ),
                            ),
                            reason_code="regex_match",
                        )
                    )

            return hits

检测器只应依赖 deerflow.compliance.contract，不要依赖引擎内部实现、Agent 状态、消息对象或 sandbox。

### 4. 配置开关并添加测试

    detectors:
      - id: regex_struct_id
        enabled: true

测试至少应覆盖正常命中、不命中、闸门限制、风险位置、参数覆盖和检测器异常。

## 十四、当前限制

1. 合规总开关默认关闭：compliance.enabled: false。
2. 模型权重不在 Git 中，必须通过资源包准备。
3. 场景解析尚未接入，所有请求走 _unknown。
4. 第 1–7 类只有注册和矩阵骨架，尚无实际检测器。
5. scan_increment() 已实现，但尚未接入生产流式调用方。
6. rewrite 当前是掩码加提示，不是 LLM 语义重写。
7. role_check 尚未接入真实权限系统，会降级为告警和审计。
8. 二进制文件尚未进行通用内容扫描。
9. 进程内模型检测主要依赖引擎整体时间预算，不支持强制中断。

## 十五、测试覆盖

后端测试集中在 backend/tests/：

- test_compliance_contract.py：契约和数据校验。
- test_compliance_engine.py：引擎编排、预算和故障隔离。
- test_compliance_registry.py：检测器发现和注册。
- test_compliance_policy_matrix.py：矩阵完整性和动作合并。
- test_compliance_normalizers.py：三类输入规范化。
- test_compliance_model_detector.py：TF-IDF+kNN 检测器。
- test_compliance_gates.py：三道闸门行为。
- test_compliance_output_retract.py：输出撤回和持久化。
- test_compliance_output_gate_isolation.py：违规原文隔离。
- test_compliance_integration.py：真实模型和完整链路。
- test_compliance_preflight.py：启动自检。
- test_compliance_audit_eval.py：审计日志。
- test_compliance_decoupling.py：检测器解耦约束。

前端测试位于 frontend/src/core/threads/compliance.test.ts，主要验证撤回事件、消息合并、合规元数据和线程隔离。

当前宿主环境是 Python 3.10，项目要求 Python 3.12 以上，且宿主没有安装 pytest，因此本次未能在当前环境实际执行测试。
