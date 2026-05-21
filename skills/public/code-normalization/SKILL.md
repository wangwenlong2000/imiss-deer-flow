---
name: code-normalization
description: >-
  使用 DeerFlow CodeASTProcessor 将裸 Python 代码片段标准化为函数定义，为 IO 提取、语义路由和组件封装提供稳定输入。
---

# 代码标准化

## 目标

该 Skill 使用 `deerflow.utils.code_ast_processor.CodeASTProcessor.normalize_code` 将裸代码片段包装为标准 Python 函数。如果输入已经包含顶层函数或类定义，则尽量保持原有定义结构。

## 🎯 核心功能

将裸代码片段包装成标准 Python 函数，便于后续静态分析和组件封装。

## 🔧 依赖说明

- 依赖 Python 标准库 `ast` 和 `textwrap`。
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
  "code": "str",
  "function_name": "str"
}
```

## 📤 输出

```json
{
  "normalized_code": "def generated_function(...):\n    ..."
}
```

## 💻 独立调用示例

```python
from deerflow.utils.code_ast_processor import CodeASTProcessor

processor = CodeASTProcessor()

normalized_code = processor.normalize_code(
    "total = price * quantity",
    function_name="calculate_total",
)

print(normalized_code)
```

## 使用建议

- 当 `code-format-validation` 返回 `valid=false` 且错误原因是缺少函数或类定义时，优先尝试该 Skill。
- 标准化后的代码应再次进入格式验证，确保后续组件封装输入稳定。
- 函数名建议由上层 Agent 或业务侧传入具有语义的信息；缺省可使用 `generated_function`。
