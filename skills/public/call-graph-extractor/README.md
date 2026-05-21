# 调用图提取器

一个使用 Python 原生的 `ast` 模块从代码片段中静态提取函数调用关系的 DeerFlow 技能。

## 功能

- **AST 解析**：使用 Python 原生的 `ast` 模块解析代码
- **调用关系提取**：提取函数调用和方法调用的关系
- **调用者跟踪**：记录当前代码处于哪个函数定义内
- **异常处理**：处理代码语法错误，确保工作流不会崩溃
- **轻量级实现**：不依赖外部库，使用纯 Python 实现

## 架构设计

- **抽象基类**：`BaseCallGraphExtractor` 定义统一的接口
- **具体实现**：`ASTPythonCallGraphExtractor` 使用 AST 解析实现调用图提取
- **AST 访问器**：`CallVisitor` 继承自 `ast.NodeVisitor`，用于遍历 AST 并提取调用关系

## 使用方法

### 编程接口

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

### 命令行使用

```bash
# 分析代码字符串
python scripts/extract.py \
  --code "def model():\n    tracker()\n\ncv2.VideoCapture()\nmodel()"

# 分析代码文件
python scripts/extract.py \
  --file input.py \
  --output output.json
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

### 示例 4：处理语法错误

**输入：**
```python
def missing_parenthesis:
    print("Hello")
```

**输出：**
```json
{
  "calls": []
}
```

## 集成到 DeerFlow

1. 将此技能添加到 DeerFlow 的技能列表中
2. 在需要分析代码执行流的地方调用此技能
3. 使用提取的调用图为 Agent 提供代码执行流的上下文

## 贡献

欢迎贡献！请随时提交 Pull Request。
