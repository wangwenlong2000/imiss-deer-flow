---
name: code-format-validation
description: >-
  使用 DeerFlow CodeASTProcessor 对 Python 代码片段进行轻量级格式验证，确认代码语法合法并包含至少一个顶层函数或类定义。
---

# 代码格式验证

## 目标

该 Skill 使用 `deerflow.utils.code_ast_processor.CodeASTProcessor.validate_format` 对 Python 代码做静态格式验证。它只解析 AST，不执行用户代码，适合作为代码处理流水线的第一道质量门。

## 🎯 核心功能

判断输入代码是否是合法 Python，并且是否包含至少一个顶层函数或类定义。

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

成功：

```json
{
  "valid": true,
  "error": ""
}
```

失败：

```json
{
  "valid": false,
  "error": "SyntaxError: invalid syntax at line 1"
}
```

## 💻 独立调用示例

```python
from deerflow.utils.code_ast_processor import CodeASTProcessor

processor = CodeASTProcessor()

result = processor.validate_format(
    """
def add(a: int, b: int) -> int:
    return a + b
"""
)

print(result)
```

## 使用建议

- 在进入语义路由、IO 提取和 LLM 生成前先调用该 Skill。
- 如果验证失败但代码语法仍可解析为普通片段，可继续调用 `code-normalization` Skill 尝试标准化。
- 该 Skill 不负责代码风格美化，只负责结构有效性判断。
