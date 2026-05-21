---
name: code-io-extraction
description: >-
  使用 DeerFlow CodeASTProcessor 从 Python 函数或代码片段中静态提取输入变量和输出变量，形成组件接口描述。
---

# IO 提取

## 目标

该 Skill 使用 `deerflow.utils.code_ast_processor.CodeASTProcessor.extract_io` 从 Python 代码中提取输入和输出变量。它基于 AST 静态分析，不运行代码，适合为组件文档、示例生成和 Agent 工具描述提供接口信息。

## 🎯 核心功能

从函数或代码片段中静态识别输入变量和输出变量。

## 🔧 依赖说明

- 依赖 Python 标准库 `ast`。
- 不依赖沙箱。
- 不依赖大模型 API。
- 不执行用户代码。

## 模块位置

```text
backend/packages/harness/deerflow/utils/code_ast_processor.py
```

推荐导入方式：

```python
from deerflow.utils.code_ast_processor import CodeASTProcessor
```

## 📥 输入

```json
{
  "code": "str"
}
```

## 📤 输出

```json
{
  "input": ["price", "quantity"],
  "output": ["total"]
}
```

## 💻 独立调用示例

```python
from deerflow.utils.code_ast_processor import CodeASTProcessor

processor = CodeASTProcessor()

io_result = processor.extract_io(
    """
def calculate_total(price: float, quantity: int) -> float:
    total = price * quantity
    return total
"""
)

print(io_result)
```

## 使用建议

- 对裸代码片段建议先调用 `code-normalization`，再进行 IO 提取。
- 该 Skill 返回变量名级别的接口描述，不负责推断复杂类型、默认值或运行时 shape。
- IO 结果可直接传入 `component-docstring-generation` 和 `component-example-generation`。
