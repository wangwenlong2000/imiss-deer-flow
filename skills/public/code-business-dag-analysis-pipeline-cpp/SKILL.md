---
name: code-business-dag-analysis-pipeline-cpp
description: >-
  C++ 代码到业务 DAG 的顺序编排流水线。该技能是从
  code-business-dag-analysis-pipeline 复制出来的 C++ 专用版本，原 Python
  流水线保持不变。用于分析 C++ 源码、生成调用关系、简单数据流、业务语义标签、
  业务 DAG、诊断 JSON 和独立 Markdown 报告。
---

# Code Business DAG Analysis Pipeline for C++

## 目标

把一段 C++ 业务代码静态分析为业务级 DAG，并生成：

1. 严格 JSON：包含 `pipeline_type`、`missing`、`anomalies`，以及各步骤中间产物。
2. 独立 Markdown 报告：通过 `report_path` 返回路径。
3. Mermaid 可视化图：报告中使用 `pipeline.nodes` 和 `pipeline.edges` 渲染。

## 固定执行顺序

1. `code-to-ast-new`
   - 调用 C++ AST 解析能力。
   - 若当前环境缺少 `tree-sitter-cpp`，降级为内置轻量静态扫描。
2. `code-splitter-adapter`
   - 使用 `language="cpp"`。
   - 若切分依赖不可用，降级为单 chunk。
3. `cpp-dataflow-extractor`
   - 复制版本内置步骤。
   - 提取简单赋值、函数返回值绑定和调用顺序数据流。
4. `code-semantic-labeler`
   - 使用 `language="cpp"`。
   - 补充 C++、OpenCV、STL、存储与输出相关语义规则。
5. `pipeline-graph-builder`
   - 复用 `deerflow.pipeline.PipelineGraphBuilder`。
6. `pipeline-reasoner`
   - 复用 `deerflow.pipeline.CloudLLMReasoner` 的本地图推理规则。
7. `analysis-report`
   - 输出 C++ 专用 Markdown 报告。

## 可执行入口

```powershell
python skills\public\code-business-dag-analysis-pipeline-cpp\scripts\run_pipeline.py `
  --file path\to\input.cpp `
  --output result.json `
  --report-output report.md
```

## 输出契约

成功时返回：

```json
{
  "status": "success",
  "language": "cpp",
  "steps": [],
  "chunks": [],
  "calls": [],
  "dataflow": [],
  "labeled_nodes": [],
  "pipeline": {
    "nodes": [],
    "edges": []
  },
  "diagnosis": {
    "pipeline_type": "unknown",
    "missing": [],
    "anomalies": []
  },
  "report_path": "report.md",
  "report_format": "markdown"
}
```

失败时返回 `status="error"`，并通过 `failed_step` 标明失败步骤。

## 支持边界

- 支持 C++ 源码静态分析，不执行代码。
- 适合识别常见业务链路，例如 OpenCV 视频采集、检测、跟踪、存储、输出，以及 STL 数据处理。
- 降级扫描不是完整 C++ 编译器，无法准确处理宏展开、模板特化、复杂重载、跨文件符号解析。
- 如果需要更高准确率，建议安装 `tree-sitter-cpp`，或后续接入 clang tooling。
