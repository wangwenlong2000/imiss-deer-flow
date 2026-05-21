---
name: call-graph-extractor
description: 从 Python 代码片段中静态提取函数调用关系，为 Agent 提供代码执行流的上下文。
---

# 调用图提取器技能

## 概述

此技能使用 Python 原生的 `ast` 模块，从代码片段中静态提取函数调用关系，为 Agent 提供代码执行流的上下文。它能够识别函数调用和方法调用，并记录调用者和被调用者之间的关系。

## 核心功能

- **AST 解析**：使用 Python 原生的 `ast` 模块解析代码
- **调用关系提取**：提取函数调用和方法调用的关系
- **调用者跟踪**：记录当前代码处于哪个函数定义内
- **异常处理**：处理代码语法错误，确保工作流不会崩溃
- **轻量级实现**：不依赖外部库，使用纯 Python 实现

## 工作流程

### 步骤 1：接收代码输入

用户提供包含函数调用的 Python 代码片段。

### 步骤 2：解析代码

使用 `ast.parse()` 将代码解析为抽象语法树。

### 步骤 3：遍历 AST

使用 `CallVisitor` 遍历 AST，提取函数调用关系。

### 步骤 4：生成调用图

根据提取的调用关系，生成调用图数据。

### 步骤 5：输出结果

以 JSON 格式输出调用图数据。

## 使用示例

### 基本使用

```python
from call_graph_extractor import ASTPythonCallGraphExtractor

extractor = ASTPythonCallGraphExtractor()
code = """
def model():
    tracker()

cv2.VideoCapture()
model()
"""
result = extractor.extract(code)
print(result)
```

**输出：**
```json
{
  "calls": [
    {"caller": "main", "callee": "cv2.VideoCapture"},
    {"caller": "main", "callee": "model"},
    {"caller": "model", "callee": "tracker"}
  ]
}
```

### 命令行使用

```bash
python /mnt/skills/public/call-graph-extractor/scripts/extract.py \
  --code "def model():\n    tracker()\n\ncv2.VideoCapture()\nmodel()"
```

## 参数

| 参数 | 描述 |
|------|------|
| `--code` | 要分析的 Python 代码字符串 |
| `--file` | 包含要分析的代码的文件路径 |
| `--output` | 保存输出结果的文件路径 |

## 支持的调用类型

- **简单函数调用**：如 `func()`
- **属性方法调用**：如 `cv2.VideoCapture()` 或 `self.method()`
- **嵌套调用**：如 `func1(func2())`
- **下标访问调用**：如 `obj[0]()`

## 输出格式

输出是一个包含调用关系的 JSON 字典：

```json
{
  "calls": [
    {"caller": "main", "callee": "cv2.VideoCapture"},
    {"caller": "main", "callee": "model"},
    {"caller": "model", "callee": "tracker"}
  ]
}
```

## 依赖

- Python 3.7+
- 标准库 `ast`
- 标准库 `abc`

## 注意事项

- 此技能使用静态分析，只能提取显式的函数调用关系
- 对于动态调用（如 `getattr(obj, "method")()`），可能无法正确识别
- 对于导入的模块和函数，会记录完整的名称（如 `cv2.VideoCapture`）
- 如果代码存在语法错误，会返回空的调用记录，而不是崩溃

## 示例

### 示例 1：简单函数调用

**输入：**
```python
def greet():
    print("Hello")

greet()
```

**输出：**
```json
{
  "calls": [
    {"caller": "greet", "callee": "print"},
    {"caller": "main", "callee": "greet"}
  ]
}
```

### 示例 2：方法调用

**输入：**
```python
class MyClass:
    def method(self):
        self.helper()
    
    def helper(self):
        pass

obj = MyClass()
obj.method()
```

**输出：**
```json
{
  "calls": [
    {"caller": "main", "callee": "MyClass"},
    {"caller": "main", "callee": "obj.method"},
    {"caller": "method", "callee": "self.helper"}
  ]
}
```

### 示例 3：嵌套调用

**输入：**
```python
def add(a, b):
    return a + b

def multiply(x, y):
    return x * y

result = multiply(add(1, 2), add(3, 4))
print(result)
```

**输出：**
```json
{
  "calls": [
    {"caller": "main", "callee": "multiply"},
    {"caller": "main", "callee": "add"},
    {"caller": "main", "callee": "add"},
    {"caller": "main", "callee": "print"}
  ]
}
```

## 集成到 DeerFlow

1. 将此技能添加到 DeerFlow 的技能列表中
2. 在需要分析代码执行流的地方调用此技能
3. 使用提取的调用图为 Agent 提供代码执行流的上下文
