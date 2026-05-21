---
name: component-example-generation
description: >-
  使用 DeerFlow ComponentPackager 为组件类生成可直接运行的 Python main 示例，帮助开发者快速理解组件调用方式。
---

# Example 生成

## 目标

该 Skill 使用 `deerflow.pipeline.ComponentPackager.generate_usage_example` 为生成后的组件类创建可直接运行的 `if __name__ == "__main__":` 使用示例。

## 🎯 核心功能

为组件类生成可直接运行的 Python 使用示例。

## 🔧 依赖说明

- 依赖 OpenAI-compatible Chat Completions API。
- 可通过 `client` 注入测试替身或私有 LLM 网关。
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
  },
  "class_name": "CalculateTotal_Component",
  "function_name": "calculate_total"
}
```

## 📤 输出

```json
{
  "usage_example": "if __name__ == \"__main__\":\n    result = CalculateTotal_Component.run(10.0, 3)\n    print(result)"
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
            "usage_example": (
                'if __name__ == "__main__":\n'
                "    result = CalculateTotal_Component.run(10.0, 3)\n"
                "    print(result)"
            )
        }
        return SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content=json.dumps(content)))]
        )


client = SimpleNamespace(chat=SimpleNamespace(completions=FakeCompletions()))
packager = ComponentPackager(client=client, model="mock-model")

example = packager.generate_usage_example(
    normalized_function_code=(
        "def calculate_total(price: float, quantity: int) -> float:\n"
        "    return price * quantity\n"
    ),
    intent="Calculate total price",
    io_extraction={"input": ["price", "quantity"], "output": ["total"]},
    class_name="CalculateTotal_Component",
    function_name="calculate_total",
)

print(example)
```

## 使用建议

- Example 必须以 `if __name__ == "__main__":` 开头，便于追加到组件文件底部。
- 示例不应导入当前组件，因为它会被追加到同一个模块内。
- 对外发布前建议运行静态检查，确认示例中的类名和方法名与实际组件一致。
