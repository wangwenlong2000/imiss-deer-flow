# C++ 代码到业务 DAG 分析流水线

`code-business-dag-analysis-pipeline-cpp` 是从 `code-business-dag-analysis-pipeline` 复制出来的 C++ 专用版本。原 Python 流水线目录保持不变。

## 处理链路

```text
code-to-ast-new（C++）
  -> code-splitter-adapter（cpp）
  -> cpp-dataflow-extractor（复制版本内置）
  -> code-semantic-labeler（language=cpp）
  -> pipeline-graph-builder
  -> pipeline-reasoner
  -> analysis-report
```

## 输入

支持 C++ 源码字符串或 `.cpp/.cc/.cxx/.hpp` 等文件内容。入口脚本会做轻量 C++ 特征检查，例如 `#include`、`std::`、`::`、`int main(`、`class`、`struct`、`cv::` 等。

## 输出

输出仍保持与原 Python 流水线兼容的 JSON 结构：

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
  "report_path": "",
  "report_format": "markdown"
}
```

## 运行方式

```powershell
python skills\public\code-business-dag-analysis-pipeline-cpp\scripts\run_pipeline.py `
  --file path\to\input.cpp `
  --output result.json `
  --report-output report.md
```

也可以直接传入源码字符串：

```powershell
python skills\public\code-business-dag-analysis-pipeline-cpp\scripts\run_pipeline.py `
  --code "#include <iostream>`nint main() { std::cout << ""hi""; return 0; }"
```

## C++ 支持说明

- AST：优先复用 `code-to-ast-new/scripts/convert.py` 的 `parse_cpp`。如果环境缺少 `tree-sitter-cpp`，会降级为复制版本内置的轻量静态扫描。
- 切分：调用 `code-splitter-adapter`，传入 `language="cpp"`。缺少 LangChain 切分依赖时降级为单 chunk。
- 调用关系：复制版本内置 C++ 调用序列提取，会识别普通函数、命名空间函数、成员方法、`std::cout`、`std::ofstream` 等常见节点。
- 数据流：复制版本内置简单 C++ 数据流提取，覆盖赋值、函数返回值绑定，以及按调用顺序形成的业务流边。
- 语义标注：复用 `code-semantic-labeler`，但传入 `language="cpp"`，并补充 C++/OpenCV/STL 常见规则。
- 构图与推理：继续复用 `PipelineGraphBuilder` 和 `CloudLLMReasoner`，因此输出格式与 Python 版本保持兼容。

## 局限

当前 C++ 版本是静态分析适配层，不编译、不执行源码。缺少 `tree-sitter-cpp` 时，降级扫描无法覆盖模板元编程、宏展开、复杂重载解析、跨文件符号解析等高级场景。对于生产级 C++ 项目，建议后续接入 clang tooling 或完整 tree-sitter 环境。
