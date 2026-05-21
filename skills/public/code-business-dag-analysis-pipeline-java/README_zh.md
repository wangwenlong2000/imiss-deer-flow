# Java 代码到业务 DAG 分析流水线技能说明

## 1. 技能定位

`code-business-dag-analysis-pipeline-java` 是从 `code-business-dag-analysis-pipeline` 复制出来的 Java 专用流水线。原 Python 流水线目录不做修改。

该技能把 Java 源码静态分析为业务级 DAG，并进一步诊断流水线类型、缺失模块和逻辑异常。它不是单一算法模块，而是固定编排以下能力：

```text
code-to-ast-new
  -> code-splitter-adapter（java）
  -> java-dataflow-extractor
  -> code-semantic-labeler
  -> pipeline-graph-builder
  -> pipeline-reasoner
  -> analysis-report
```

## 2. 输入

最小输入：

```json
{
  "language": "java",
  "raw_code": "Java 源码字符串"
}
```

也可以直接通过脚本读取 `.java` 文件。

## 3. 输出

最终输出一个 JSON 对象，核心字段如下：

- `status`：`success` 或 `error`。
- `language`：固定为 `java`。
- `steps`：每个步骤的执行状态。
- `chunks`：Java 代码切分结果。
- `calls`：方法调用顺序关系。
- `dataflow`：简单数据流关系。
- `labeled_nodes`：代码节点到业务语义类型的映射。
- `pipeline`：业务 DAG，包含 `nodes` 和 `edges`。
- `diagnosis`：`pipeline-reasoner` 的严格 JSON 诊断结果。
- `report_path`：独立 Markdown 报告路径。
- `report_format`：固定为 `markdown`。

## 4. Java 适配点

### 4.1 code-to-ast-new

优先调用 `parse_java`。如果当前解析器返回未实现或错误，则降级为本技能内置的 `regex-fallback`，保留语言、源码和警告信息。

### 4.2 code-splitter-adapter

使用 `language="java"` 调用切分器。若依赖不可用，则返回单 chunk，保证后续步骤继续执行。

### 4.3 java-dataflow-extractor

复制版本新增的轻量步骤，提取形如：

```java
Detections detections = detector.detect(frame);
```

对应的数据流：

```json
["detector.detect", "detections"]
```

### 4.4 code-semantic-labeler

在通用语义标注器基础上补充 Java 规则，例如：

- `VideoCapture`、`camera.read` -> `Camera`
- `decodeFrame` -> `Decode`
- `Imgproc.resize`、`preprocess` -> `Preprocess`
- `detector.detect`、`model.predict` -> `Detection`
- `tracker.update` -> `Tracking`
- `repository.save`、`Files.write`、`writer.write` -> `Storage`
- `System.out.println` -> `Output`

### 4.5 pipeline-graph-builder 与 pipeline-reasoner

继续复用后端 `deerflow.pipeline.PipelineGraphBuilder` 和 `CloudLLMReasoner`。构图时优先使用方法调用顺序和语义标签，避免轻量数据流中间变量造成额外噪声边。

## 5. 运行方式

```powershell
python skills\public\code-business-dag-analysis-pipeline-java\scripts\run_pipeline.py `
  --file path\to\input.java `
  --output result.json `
  --report-output report.md
```

## 6. 验证建议

```powershell
python skills\public\code-business-dag-analysis-pipeline-java\scripts\run_pipeline.py `
  --file skills\public\code-business-dag-analysis-pipeline-java\evals\fixtures\video_pipeline_missing_decode.java `
  --output .tmp\java_pipeline_result.json `
  --report-output .tmp\java_pipeline_report.md
```

## 7. 支持边界

- 本技能只做静态分析，不执行用户 Java 代码。
- 内置降级扫描不等同于 Java 编译器。
- 对反射、动态代理、注解生成代码、复杂泛型、跨文件符号解析的识别能力有限。
- 后续如需更精确，可接入 JavaParser、Eclipse JDT 或 tree-sitter-java。
