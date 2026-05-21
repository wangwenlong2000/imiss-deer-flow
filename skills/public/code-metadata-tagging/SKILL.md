---
name: code-metadata-tagging
description: >-
  使用 DeerFlow CodeSemanticRouter 为 Python 代码片段生成可检索、可过滤、可路由的 Metadata 标签集合。
---

# Metadata 标签标注

## 目标

该 Skill 使用 `deerflow.utils.code_semantic_router.CodeSemanticRouter.route` 生成代码元数据标签。标签可用于 Agent 工具选择、RAG 检索过滤、组件分类、审计统计和可视化展示。

## 🎯 核心功能

为代码片段生成可检索、可过滤、可路由的标签集合。

## 🔧 依赖说明

- 规则层不需要额外依赖。
- Embedding fallback 需要 `sentence-transformers`。
- 可选 LLM renderer 需要开发者注入 `llm_generator`。
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
  "code": "str",
  "enable_embedding": "bool"
}
```

## 📤 输出

```json
{
  "intent": "Data Processing",
  "tags": ["data", "dataframe", "etl"]
}
```

## 💻 独立调用示例

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

## 使用建议

- 标签应被视为 Metadata，不应替代完整代码审查或安全判断。
- 建议将 `intent` 和 `tags` 一起存储，便于后续检索和排序。
- 如果标签用于自动路由到高风险执行环境，应增加人工确认或策略校验。
