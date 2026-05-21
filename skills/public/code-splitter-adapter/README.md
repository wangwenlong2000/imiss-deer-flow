# 代码切分工具适配器

一个使用适配器模式和工厂模式封装 LangChain 和 LlamaIndex 中的代码切分工具的 DeerFlow 技能。

## 功能

- **统一接口**：提供统一的 `split_code` 方法，屏蔽底层实现差异
- **多策略支持**：支持 LangChain 和 LlamaIndex 两种切分策略
- **多语言支持**：支持多种编程语言的代码切分
- **错误处理**：对 LlamaIndex 解析失败的情况提供友好的错误信息
- **元数据支持**：支持附加元数据到切分后的代码片段

## 架构设计

- **抽象基类**：`BaseCodeSplitter` 定义统一的接口
- **适配器**：
  - `LangChainCodeSplitterAdapter`：封装 LangChain 的代码切分工具
  - `LlamaIndexASTSplitterAdapter`：封装 LlamaIndex 的 AST 代码切分工具
- **工厂函数**：`get_code_splitter` 根据策略返回对应的适配器实例

## 使用方法

### 编程接口

```python
from code_splitter import get_code_splitter

# 使用 LangChain 策略
langchain_splitter = get_code_splitter(strategy="langchain", chunk_size=1000, chunk_overlap=200)
result = langchain_splitter.split_code(
    code="def foo():\n    pass\n\nclass Bar:\n    pass",
    language="python",
    metadata={"source": "utils.py"}
)

# 使用 LlamaIndex 策略
llamaindex_splitter = get_code_splitter(strategy="llamaindex")
result = llamaindex_splitter.split_code(
    code="def foo():\n    pass\n\nclass Bar:\n    pass",
    language="python",
    metadata={"source": "utils.py"}
)
```

### 命令行使用

```bash
# 切分代码字符串
python scripts/split.py \
  --strategy langchain \
  --code "def foo():\n    pass\n\nclass Bar:\n    pass" \
  --language python \
  --metadata '{"source": "utils.py"}'

# 切分代码文件
python scripts/split.py \
  --strategy llamaindex \
  --file input.py \
  --language python \
  --output output.json

# 使用自定义切分参数
python scripts/split.py \
  --strategy langchain \
  --file input.py \
  --language python \
  --chunk-size 2000 \
  --chunk-overlap 300 \
  --output output.json
```

## 参数

| 参数 | 描述 |
|------|------|
| `strategy` | 切分策略，可选值为 "langchain" 或 "llamaindex" |
| `code` | 要切分的代码字符串 |
| `file` | 包含要切分的代码的文件路径 |
| `language` | 代码的编程语言 |
| `metadata` | 附加的元数据，JSON 格式 |
| `chunk-size` | 切分后的代码块大小（仅 LangChain 策略） |
| `chunk-overlap` | 代码块之间的重叠大小（仅 LangChain 策略） |
| `output` | 保存输出结果的文件路径 |

## 支持的编程语言

### LangChain 支持的语言

- python
- java
- javascript
- typescript
- html
- css
- markdown
- json
- xml
- sql
- rust
- go
- cpp
- c++ (alias for cpp)
- c
- php
- ruby
- swift
- kotlin

### LlamaIndex 支持的语言

- python
- javascript
- typescript
- java
- c
- cpp
- rust
- go
- ruby
- php
- swift
- kotlin

## 输出格式

无论使用哪种切分策略，返回的结果格式都是一致的：

```json
[
    {"content": "def foo():\n    pass", "metadata": {"source": "utils.py", "chunk_index": 0}},
    {"content": "class Bar:\n    pass", "metadata": {"source": "utils.py", "chunk_index": 1}}
]
```

## 依赖

- Python 3.7+
- langchain-text-splitters (使用 LangChain 策略时)
- llama-index (使用 LlamaIndex 策略时)
- tree-sitter (使用 LlamaIndex 策略时，用于 AST 解析)

## 安装依赖

```bash
# 安装 LangChain 依赖
pip install langchain-text-splitters

# 安装 LlamaIndex 依赖
pip install llama-index

# 安装 tree-sitter 语言包（根据需要）
pip install tree-sitter-python tree-sitter-javascript # 示例
```

## 注意事项

- 使用 LlamaIndex 策略时，需要安装对应的 tree-sitter 语言包
- 对于复杂的代码，不同的切分策略可能会产生不同的切分结果
- 切分参数的选择会影响切分质量，建议根据具体场景调整
- LlamaIndex 策略在解析失败时会提供友好的错误信息

## 集成到 DeerFlow

1. 将此技能添加到 DeerFlow 的技能列表中
2. 在需要切分代码的地方调用此技能
3. 选择适合的切分策略和参数

## 示例

### 切分 Python 代码

**输入：**
```python
def calculate_factorial(n):
    """计算阶乘"""
    if n == 0:
        return 1
    else:
        return n * calculate_factorial(n-1)

def fibonacci(n):
    """计算斐波那契数列"""
    if n <= 1:
        return n
    else:
        return fibonacci(n-1) + fibonacci(n-2)

# 测试
print(calculate_factorial(5))
print(fibonacci(10))
```

**命令：**
```bash
python scripts/split.py \
  --strategy langchain \
  --file example.py \
  --language python \
  --metadata '{"source": "example.py"}'
```

**输出：**
```json
[
  {
    "content": "def calculate_factorial(n):\n    \"\"\"计算阶乘\"\"\"\n    if n == 0:\n        return 1\n    else:\n        return n * calculate_factorial(n-1)",
    "metadata": {
      "source": "example.py",
      "chunk_index": 0
    }
  },
  {
    "content": "def fibonacci(n):\n    \"\"\"计算斐波那契数列\"\"\"\n    if n <= 1:\n        return n\n    else:\n        return fibonacci(n-1) + fibonacci(n-2)",
    "metadata": {
      "source": "example.py",
      "chunk_index": 1
    }
  },
  {
    "content": "# 测试\nprint(calculate_factorial(5))\nprint(fibonacci(10))",
    "metadata": {
      "source": "example.py",
      "chunk_index": 2
    }
  }
]
```

## 贡献

欢迎贡献！请随时提交 Pull Request。
