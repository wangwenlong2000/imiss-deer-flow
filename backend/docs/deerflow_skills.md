# DeerFlow 代码处理流水线 Skills 指南

> 面向 DeerFlow Agentic RAG 系统的代码理解、归一化、语义路由与组件化生成技能说明。

## 🌐 Overview

DeerFlow 的代码处理流水线采用 **轻量级、无沙箱、静态分析优先、LLM 辅助生成** 的设计理念。它不会执行用户提交的代码，而是通过 Python 标准库 `ast` 对代码结构做安全的静态解析，再结合语义路由和大模型生成能力，将零散代码片段加工成可复用、可导入、带文档和示例的组件代码。

整体链路如下：

```text
Code Snippet
  -> Format Validation
  -> Normalization
  -> Intent & Tagging
  -> IO Extraction
  -> Doc Generation
  -> Example Generation
  -> Component Packaging
```

这套能力被设计成 **插件化 / 技能化** 架构。每个 Skill 都是一个可独立调用的 Python 类或方法组合，既可以由 `CodeProcessingPipeline` 串联成端到端流水线，也可以被大模型 Agent、人类开发者、测试脚本或其他工具单独调用。

核心模块：

- `CodeASTProcessor`: 负责代码格式验证、标准化和 IO 提取。
- `CodeSemanticRouter`: 负责意图识别和 Metadata 标签标注。
- `ComponentPackager`: 负责 Docstring、Example 和组件包装代码生成。
- `CodeProcessingPipeline`: 负责把上述能力按固定生命周期编排成完整处理流程。

## 🧩 Standalone Skills Reference

下面的 8 个 Skill 均可脱离完整 Pipeline 独立运行。

## Skill 1: 代码格式验证

**对应实现:** `CodeASTProcessor.validate_format`

### 🎯 核心功能

判断输入代码是否是合法 Python，并且是否包含至少一个顶层函数或类定义。

### 🔧 依赖说明

- 依赖 Python 标准库 `ast`。
- 不依赖沙箱。
- 不依赖大模型 API。
- 不执行用户代码。

### 📥 输入

```json
{
  "code": "str"
}
```

### 📤 输出

```json
{
  "valid": true,
  "error": ""
}
```

失败示例：

```json
{
  "valid": false,
  "error": "SyntaxError: invalid syntax at line 1"
}
```

### 💻 独立调用示例

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

## Skill 2: 代码标准化

**对应实现:** `CodeASTProcessor.normalize_code`

### 🎯 核心功能

将裸代码片段包装成标准 Python 函数，便于后续路由、IO 提取和组件封装。

### 🔧 依赖说明

- 依赖 Python 标准库 `ast` 和 `textwrap`。
- 不依赖沙箱。
- 不依赖大模型 API。
- 不执行用户代码。

### 📥 输入

```json
{
  "code": "str",
  "function_name": "str"
}
```

### 📤 输出

```json
{
  "normalized_code": "def generated_function(...):\n    ..."
}
```

### 💻 独立调用示例

```python
from deerflow.utils.code_ast_processor import CodeASTProcessor

processor = CodeASTProcessor()

normalized_code = processor.normalize_code(
    "total = price * quantity",
    function_name="calculate_total",
)

print(normalized_code)
```

## Skill 3: IO 提取

**对应实现:** `CodeASTProcessor.extract_io`

### 🎯 核心功能

从函数或代码片段中静态识别输入变量和输出变量。

### 🔧 依赖说明

- 依赖 Python 标准库 `ast`。
- 不依赖沙箱。
- 不依赖大模型 API。
- 不执行用户代码。

### 📥 输入

```json
{
  "code": "str"
}
```

### 📤 输出

```json
{
  "input": ["price", "quantity"],
  "output": ["total"]
}
```

### 💻 独立调用示例

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

## Skill 4: 语义意图识别

**对应实现:** `CodeSemanticRouter.route`

### 🎯 核心功能

识别代码片段的语义意图，例如 HTTP 请求、数据处理、图像处理或硬件 IO。

### 🔧 依赖说明

- 默认使用轻量规则字典进行匹配。
- 可选依赖 `sentence-transformers` 作为 embedding fallback。
- 可选接入本地或开源 LLM 作为结果渲染器。
- 不依赖沙箱。
- 不执行用户代码。

### 📥 输入

```json
{
  "code": "str"
}
```

### 📤 输出

```json
{
  "intent": "HTTP + JSON fetch",
  "tags": ["network", "http", "api", "json"]
}
```

### 💻 独立调用示例

```python
from deerflow.utils.code_semantic_router import CodeSemanticRouter

router = CodeSemanticRouter(enable_embedding=False)

result = router.route(
    """
import requests

payload = requests.get(url).json()
"""
)

print(result)
```

## Skill 5: Metadata 标签标注

**对应实现:** `CodeSemanticRouter.route`

### 🎯 核心功能

为代码片段生成可检索、可过滤、可路由的标签集合。

### 🔧 依赖说明

- 规则层不需要额外依赖。
- Embedding fallback 需要 `sentence-transformers`。
- 可选 LLM renderer 需要开发者注入 `llm_generator`。
- 不依赖沙箱。
- 不执行用户代码。

### 📥 输入

```json
{
  "code": "str",
  "enable_embedding": "bool"
}
```

### 📤 输出

```json
{
  "intent": "Data Processing",
  "tags": ["data", "dataframe", "etl"]
}
```

### 💻 独立调用示例

```python
from deerflow.utils.code_semantic_router import CodeSemanticRouter

router = CodeSemanticRouter(enable_embedding=False)

metadata = router.route(
    """
import pandas as pd

df = pd.read_csv(path).dropna()
"""
)

print(metadata["tags"])
```

## Skill 6: Docstring 生成

**对应实现:** `ComponentPackager.generate_docstring`

### 🎯 核心功能

基于标准化函数、语义意图和 IO 信息生成 Google Style Python Docstring。

### 🔧 依赖说明

- 依赖 OpenAI-compatible Chat Completions API。
- 默认客户端来自 `openai.OpenAI`。
- 可通过 `client` 注入 Mock Client、本地网关或私有模型网关。
- 不依赖沙箱。
- 不执行用户代码。

### 📥 输入

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

### 📤 输出

```json
{
  "docstring": "Calculate the total price.\n\nArgs:\n    price: Unit price.\n    quantity: Number of items.\n\nReturns:\n    The total price."
}
```

### 💻 独立调用示例

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

## Skill 7: Example 生成

**对应实现:** `ComponentPackager.generate_usage_example`

### 🎯 核心功能

为生成后的组件类创建可直接运行的 `if __name__ == "__main__":` 使用示例。

### 🔧 依赖说明

- 依赖 OpenAI-compatible Chat Completions API。
- 可通过 `client` 注入测试替身或私有 LLM 网关。
- 不依赖沙箱。
- 不执行用户代码。

### 📥 输入

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

### 📤 输出

```json
{
  "usage_example": "if __name__ == \"__main__\":\n    result = CalculateTotal_Component.run(10.0, 3)\n    print(result)"
}
```

### 💻 独立调用示例

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

## Skill 8: 端到端编排

**对应实现:** `CodeProcessingPipeline.process_snippet`

### 🎯 核心功能

按固定生命周期把格式验证、标准化、语义路由、IO 提取、文档生成、示例生成和组件封装串联成统一处理流程。

### 🔧 依赖说明

- 依赖注入 `CodeASTProcessor`、`CodeSemanticRouter`、`ComponentPackager`。
- 静态阶段不依赖大模型。
- 文档和示例生成阶段通常依赖 OpenAI-compatible LLM API。
- 不依赖沙箱。
- 不执行用户代码。

### 📥 输入

```json
{
  "raw_code": "str"
}
```

### 📤 输出

成功：

```json
{
  "status": "success",
  "metadata": {
    "intent": "HTTP + JSON fetch",
    "tags": ["network", "http", "api", "json"]
  },
  "interface": {
    "input": ["url"],
    "output": ["payload"]
  },
  "final_component_code": "class GeneratedFunction_Component:\n    ..."
}
```

失败：

```json
{
  "status": "error",
  "step": "intent_and_tagging",
  "error": "router unavailable",
  "metadata": {
    "intent": "unknown",
    "tags": []
  },
  "interface": {
    "input": [],
    "output": []
  },
  "final_component_code": ""
}
```

### 💻 独立调用示例

```python
import json
from types import SimpleNamespace

from deerflow.pipeline import CodeProcessingPipeline, ComponentPackager
from deerflow.utils.code_ast_processor import CodeASTProcessor
from deerflow.utils.code_semantic_router import CodeSemanticRouter


class FakeCompletions:
    def __init__(self):
        self.calls = 0

    def create(self, **kwargs):
        self.calls += 1
        if self.calls == 1:
            content = {
                "docstring": (
                    "Fetch JSON payload from a URL.\n\n"
                    "Args:\n"
                    "    url: HTTP endpoint URL.\n\n"
                    "Returns:\n"
                    "    Parsed JSON payload."
                )
            }
        else:
            content = {
                "usage_example": (
                    'if __name__ == "__main__":\n'
                    '    payload = GeneratedFunction_Component.run("https://example.com/api")\n'
                    "    print(payload)"
                )
            }
        return SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content=json.dumps(content)))]
        )


client = SimpleNamespace(chat=SimpleNamespace(completions=FakeCompletions()))

pipeline = CodeProcessingPipeline(
    ast_processor=CodeASTProcessor(),
    semantic_router=CodeSemanticRouter(enable_embedding=False),
    component_packager=ComponentPackager(client=client, model="mock-model"),
)

result = pipeline.process_snippet(
    """
import requests

payload = requests.get(url).json()
"""
)

print(result["status"])
print(result["metadata"])
print(result["interface"])
```

## ✅ Best Practices

### 1. 像乐高一样组合技能

推荐在应用层显式注入各个 Skill。这样可以在测试环境替换 LLM Client，在生产环境接入真实模型网关，也可以为不同租户切换不同语义路由策略。

```python
from deerflow.pipeline import CodeProcessingPipeline, ComponentPackager
from deerflow.utils.code_ast_processor import CodeASTProcessor
from deerflow.utils.code_semantic_router import CodeSemanticRouter


ast_processor = CodeASTProcessor()
semantic_router = CodeSemanticRouter(enable_embedding=False)
component_packager = ComponentPackager(
    api_key="YOUR_API_KEY",
    base_url="https://api.openai.com/v1",
    model="gpt-4o-mini",
)

pipeline = CodeProcessingPipeline(
    ast_processor=ast_processor,
    semantic_router=semantic_router,
    component_packager=component_packager,
)

result = pipeline.process_snippet(
    """
def calculate_total(price: float, quantity: int) -> float:
    return price * quantity
"""
)

if result["status"] == "success":
    print(result["final_component_code"])
else:
    print(f"Pipeline failed at {result['step']}: {result['error']}")
```

### 2. 优先静态分析，谨慎使用生成式步骤

- 格式验证、标准化、IO 提取应优先使用 `CodeASTProcessor`，因为它 deterministic、低成本、无网络依赖。
- 语义路由建议先启用规则层，只有当业务需要更高召回时再启用 `sentence-transformers`。
- Docstring 和 Example 属于生成式增强能力，应做好超时、重试、审计和成本控制。

### 3. Pipeline 应返回结果，不应抛穿业务边界

`CodeProcessingPipeline.process_snippet` 会把异常收束为统一 JSON 载体。上层 Agent 或 API 服务应根据 `status` 字段判断后续动作，而不是依赖异常控制主流程。

```json
{
  "status": "error",
  "step": "doc_generation",
  "error": "LLM request timeout",
  "metadata": {
    "intent": "unknown",
    "tags": []
  },
  "interface": {
    "input": [],
    "output": []
  },
  "final_component_code": ""
}
```

### 4. 测试时注入 Mock Client

`ComponentPackager` 支持传入 `client`，因此测试不需要访问外部网络，也不需要真实 API Key。建议在单元测试中固定 LLM 返回值，以验证组件渲染结构、Docstring 插入和 Example 拼接逻辑。

### 5. 不把代码执行作为默认能力

DeerFlow 代码处理流水线的核心假设是 **理解和封装代码，而不是运行代码**。如果未来需要执行生成组件，应通过独立沙箱能力承接，并把执行权限、资源限制和审计日志作为单独安全边界设计。
