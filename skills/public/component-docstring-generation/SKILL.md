---
name: component-docstring-generation
description: >-
  使用 DeerFlow ComponentPackager 基于标准化函数、语义意图和 IO 信息生成 Google Style Python Docstring。
---

# Docstring 生成

## 目标

该 Skill 使用 `deerflow.pipeline.ComponentPackager.generate_docstring` 为标准化后的 Python 函数生成 Google Style Docstring。它属于生成式增强能力，适合在组件发布、Agent 工具描述和自动文档构建前调用。

## 🎯 核心功能

基于标准化函数、语义意图和 IO 信息生成 Google Style Python Docstring。

## 🔧 依赖说明

- 依赖 OpenAI-compatible Chat Completions API。
- 默认客户端来自 `openai.OpenAI`。
- 可通过 `client` 注入 Mock Client、本地网关或私有模型网关。
- 不依赖沙箱。
- 不执行用户代码。

## 模块位置

```text
backend/packages/harness/deerflow/pipeline/component_packager.py
```

推荐导入方式：

```python
from deerflow.pipeline import ComponentPackager
```

## 📥 输入

```json
{
  "normalized_function_code": "str",
  "intent": "str",
  "io_extraction": {
    "input": ["price", "quantity"],
    "output": ["total"]
  }
}
```

## 📤 输出

```json
{
  "docstring": "Calculate the total price.\n\nArgs:\n    price: Unit price.\n    quantity: Number of items.\n\nReturns:\n    The total price."
}
```

## 💻 独立调用示例

```python
import json
from types import SimpleNamespace

from deerflow.pipeline import ComponentPackager


class FakeCompletions:
    def create(self, **kwargs):
        content = {
            "docstring": (
                "Calculate a total value.\n\n"
                "Args:\n"
                "    price: Unit price.\n"
                "    quantity: Number of items.\n\n"
                "Returns:\n"
                "    The calculated total."
            )
        }
        return SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content=json.dumps(content)))]
        )


client = SimpleNamespace(chat=SimpleNamespace(completions=FakeCompletions()))
packager = ComponentPackager(client=client, model="mock-model")

docstring = packager.generate_docstring(
    normalized_function_code=(
        "def calculate_total(price: float, quantity: int) -> float:\n"
        "    return price * quantity\n"
    ),
    intent="Calculate total price",
    io_extraction={"input": ["price", "quantity"], "output": ["total"]},
)

print(docstring)
```

## 使用建议

- 传入的函数代码应先经过 `code-format-validation` 和 `code-normalization`。
- IO 信息应来自 `code-io-extraction`，避免模型凭空猜测参数。
- 生产环境建议配置超时、重试和 JSON 响应校验。
