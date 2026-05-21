---
name: code-intent-routing
description: >-
  使用 DeerFlow CodeSemanticRouter 对 Python 代码片段进行语义意图识别，识别 HTTP、数据处理、图像处理、硬件 IO 等任务类型。
---

# 语义意图识别

## 目标

该 Skill 使用 `deerflow.utils.code_semantic_router.CodeSemanticRouter.route` 识别代码片段的高层语义意图。它优先使用轻量规则字典，规则未命中时可选使用 embedding fallback。

## 🎯 核心功能

识别代码片段的语义意图，例如 HTTP 请求、数据处理、图像处理或硬件 IO。

## 🔧 依赖说明

- 规则层不需要额外依赖。
- 可选依赖 `sentence-transformers` 作为 embedding fallback。
- 可选接入本地或开源 LLM 作为结果渲染器。
- 不依赖沙箱。
- 不执行用户代码。

## 模块位置

```text
backend/packages/harness/deerflow/utils/code_semantic_router.py
```

推荐导入方式：

```python
from deerflow.utils.code_semantic_router import CodeSemanticRouter
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
  "intent": "HTTP + JSON fetch",
  "tags": ["network", "http", "api", "json"]
}
```

## 💻 独立调用示例

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

## 使用建议

- 对生产链路建议先使用 `enable_embedding=False` 的规则层，获得稳定、低成本结果。
- 当规则覆盖不足时，再安装 `sentence-transformers` 并启用 embedding fallback。
- 如果使用本地 LLM 渲染器，应确保返回结构始终是 `{"intent": str, "tags": list[str]}`。
