---
name: code-processing-pipeline
description: >-
  使用 DeerFlow CodeProcessingPipeline 将 CodeASTProcessor、CodeSemanticRouter 和 ComponentPackager 组合成端到端代码处理流水线。
---

# 代码处理流水线

## 目标

该 Skill 使用 `deerflow.pipeline.CodeProcessingPipeline.process_snippet` 按固定生命周期串联多个独立技能，最终生成统一 JSON 载体和可复用组件代码。

## 🌐 架构概览

DeerFlow 代码处理流水线采用轻量级、无沙箱、静态分析优先、LLM 辅助生成的设计。它不执行用户代码，而是通过 AST 静态解析、语义路由和生成式封装，将代码片段加工为带 Metadata、接口描述、Docstring 和 Example 的组件。

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

## 🎯 核心功能

将格式验证、标准化、语义路由、IO 提取、文档生成、示例生成和组件封装串联成统一处理流程。

## 🔧 依赖说明

- 依赖注入 `CodeASTProcessor`、`CodeSemanticRouter`、`ComponentPackager`。
- 静态分析阶段不依赖大模型。
- 文档和示例生成阶段通常依赖 OpenAI-compatible LLM API。
- 不依赖沙箱。
- 不执行用户代码。

## 模块位置

```text
backend/packages/harness/deerflow/pipeline/code_processing_pipeline.py
```

推荐导入方式：

```python
from deerflow.pipeline import CodeProcessingPipeline
```

## 📥 输入

```json
{
  "raw_code": "str"
}
```

## 📤 输出

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

## 💻 独立调用示例

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

## ✅ 最佳实践

- 通过依赖注入组合各个 Skill，便于测试、替换模型和接入私有网关。
- 上层服务应根据 `status` 判断处理结果，不应依赖异常穿透业务边界。
- 如果只需要某个中间能力，优先直接调用对应独立 Skill，避免不必要的 LLM 成本。
- 生产环境应为 LLM 步骤设置超时、重试、审计日志和成本控制。
