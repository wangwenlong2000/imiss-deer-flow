# 代码到业务 DAG 分析流水线技能说明

## 1. 技能定位

`code-business-dag-analysis-pipeline` 是一个顺序编排技能，用来把 Python 代码静态分析为业务级 DAG，并进一步诊断流水线类型、缺失模块和逻辑异常。

它不是单一算法模块，而是把以下能力固定串联：

```text
code-to-ast-new
  -> code-splitter-adapter（Python only）
  -> dataflow-extractor
  -> code-semantic-labeler
  -> pipeline-graph-builder
  -> pipeline-reasoner
  -> analysis-report
```

## 2. 技能存在性确认

已在当前仓库中确认以下技能存在：

| 顺序 | 技能名 | 路径 | 确认结果 |
| --- | --- | --- | --- |
| 1 | `code-to-ast-new` | `skills/public/code-to-ast-new` | 存在 |
| 2 | `code-splitter-adapter` | `skills/public/code-splitter-adapter` | 存在 |
| 3 | `dataflow-extractor` | `skills/public/dataflow-extractor` | 存在 |
| 4 | `code-semantic-labeler` | `skills/public/code-semantic-labeler` | 存在 |
| 5 | `pipeline-graph-builder` | `skills/public/pipeline-graph-builder`，后端 `deerflow.pipeline.PipelineGraphBuilder` | 存在 |
| 6 | `pipeline-reasoner` | `skills/public/pipeline-reasoner`，后端 `deerflow.pipeline.CloudLLMReasoner` | 存在 |

当前流水线技能目录中新增了联动脚本：

```text
skills/public/code-business-dag-analysis-pipeline/scripts/run_pipeline.py
```

## 3. 输入

最小输入：

```json
{
  "language": "python",
  "raw_code": "Python 源码字符串"
}
```

建议附加输入：

```json
{
  "project_name": "业务或项目名称",
  "entrypoint": "入口文件或入口函数",
  "report_language": "zh-CN"
}
```

## 4. 输出

流水线最终输出一个 JSON 对象，核心字段如下：

- `status`：`success` 或 `error`。
- `steps`：每个技能步骤的执行状态。
- `chunks`：Python 代码切分结果。
- `calls`：调用图关系。
- `dataflow`：数据流关系。
- `labeled_nodes`：代码节点到业务语义类型的映射。
- `pipeline`：业务 DAG，包含 `nodes` 和 `edges`。
- `diagnosis`：`pipeline-reasoner` 的严格 JSON 诊断结果。
- `report`：中文 Markdown 分析报告。

`diagnosis` 必须保持如下形态：

```json
{
  "pipeline_type": "unknown",
  "missing": [],
  "anomalies": []
}
```

## 5. 各步骤契约

### 5.1 code-to-ast-new

职责：解析 Python AST，识别函数、类、调用表达式、赋值和导入等结构。

输出应至少支持提取调用关系：

```json
{
  "calls": [
    {"caller": "main", "callee": "model.predict"}
  ]
}
```

### 5.2 code-splitter-adapter

职责：只使用 Python 适配路径，把源码切成后续技能可消费的 chunks。

输出：

```json
{
  "chunks": [
    {"name": "main", "type": "function", "code": "def main(): ..."}
  ]
}
```

### 5.3 dataflow-extractor

职责：提取变量、函数返回值、模型输出、存储输入之间的数据流。

输出：

```json
{
  "dataflow": [
    ["frame", "model.predict"],
    ["results", "storage.save"]
  ]
}
```

### 5.4 code-semantic-labeler

职责：把代码节点映射为业务阶段，例如 `Camera`、`Decode`、`Detection`、`Tracking`、`Storage`。

输出：

```json
{
  "labeled_nodes": [
    {"name": "cv2.VideoCapture", "type": "Camera"},
    {"name": "model.predict", "type": "Detection"}
  ]
}
```

集成提醒：如果该模块通过 `_init_llm` 读取本地配置，不能依赖不稳定的当前工作目录。建议把相对路径改为基于模块文件路径或显式配置根目录解析。

### 5.5 pipeline-graph-builder

职责：融合 `calls`、`dataflow` 和 `labeled_nodes`，输出业务级 DAG。

输出：

```json
{
  "pipeline": {
    "nodes": ["Camera", "Detection", "Storage"],
    "edges": [["Camera", "Detection"], ["Detection", "Storage"]]
  }
}
```

### 5.6 pipeline-reasoner

职责：使用 `CloudLLMReasoner` 的本地图推理规则诊断业务 DAG。

输出：

```json
{
  "pipeline_type": "视频监控与存储",
  "missing": ["Decode", "Preprocess"],
  "anomalies": ["逻辑断层: Camera 与 Detection 之间缺少 Decode 解码模块"]
}
```

### 5.7 analysis-report

职责：把中间产物和诊断结果转成面向工程人员的中文报告。报告不应替代 JSON，只作为解释层。

## 6. 联动运行方式

直接运行完整流水线：

```powershell
python skills\public\code-business-dag-analysis-pipeline\scripts\run_pipeline.py `
  --file path\to\input.py `
  --output result.json
```

脚本会按固定顺序执行所有技能，并输出统一 JSON。若 `code-splitter-adapter` 所需的 LangChain 切分依赖不可用，会自动降级为单 chunk，保证后续 AST、数据流、语义标注、构图和推理仍可继续。

## 7. 报告生成脚本

本技能提供 `scripts/render_report.py`。它从流水线结果 JSON 读取数据，并输出 Markdown 报告。

示例：

```powershell
python skills\public\code-business-dag-analysis-pipeline\scripts\render_report.py `
  --input result.json `
  --output report.md
```

如果不传 `--output`，报告会输出到标准输出。

## 8. 常见失败处理

- 非 Python 输入：返回 `status=error`，`failed_step=language-check`。
- AST 解析失败：返回 `failed_step=code-to-ast-new`，报告为空。
- 语义标注失败：保留已经得到的 `chunks`、`calls`、`dataflow`，并在 `anomalies` 中说明流水线未完成。
- DAG 为空：`pipeline_type` 应为 `unknown`，`anomalies` 应提示空流水线或无可诊断节点。

## 9. 最佳实践

- 优先保留每个步骤的中间产物，便于调试标注错误和构图错误。
- 不要把变量名直接当业务节点展示，业务节点应来自 `labeled_nodes[].type`。
- 对外 API 应消费严格 JSON；报告只用于展示、审查和人工沟通。
- 单元测试中使用 mock labeler 和 mock extractor，避免测试依赖真实 LLM。
