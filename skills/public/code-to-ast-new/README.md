# AST 代码树结构生成技能

一个将输入的代码转化为 AST（抽象语法树）结构的 DeerFlow 技能。

## 功能

- **多语言支持**：支持 Python、JavaScript、Java、C++ 等多种编程语言
- **AST 生成**：将代码转化为抽象语法树结构
- **JSON 输出**：以 JSON 格式输出 AST 结构
- **详细分析**：生成详细的 AST 节点和属性
- **文件支持**：支持直接代码输入和文件输入

## 使用方法

### 基本使用

```bash
# 解析 Python 代码
python scripts/convert.py --language python --code "def hello():\n    print('Hello, world!')"

# 解析 JavaScript 代码
python scripts/convert.py --language javascript --code "function hello() {\n  console.log('Hello, world!');\n}"

# 解析代码文件
python scripts/convert.py --language python --file input.py --output ast.json

# 自定义 JSON 缩进
python scripts/convert.py --language python --code "def hello():\n    print('Hello, world!')" --indent 4
```

### 参数

| 参数 | 是否必需 | 描述 |
|------|----------|------|
| `--language` | 是 | 编程语言（python, javascript, java, cpp） |
| `--code` | 要么 `--code` 要么 `--file` | 直接代码输入 |
| `--file` | 要么 `--code` 要么 `--file` | 代码文件路径 |
| `--output` | 否 | 保存 AST 输出的路径 |
| `--indent` | 否 | JSON 输出的缩进空格数（默认：2） |

## 支持的语言

- **Python**：使用内置的 `ast` 模块
- **JavaScript**：使用 `esprima` 库
- **Java**：使用占位实现（需要 `javaparser`）
- **C++**：使用占位实现（需要 `clang`）

## 示例

### Python 代码 AST 示例

**输入：**
```python
def hello():
    print('Hello, world!')
```

**输出：**
```json
{
  "type": "Module",
  "body": [
    {
      "type": "FunctionDef",
      "name": "hello",
      "args": {
        "type": "arguments",
        "posonlyargs": [],
        "args": [],
        "kwonlyargs": [],
        "kw_defaults": [],
        "defaults": []
      },
      "body": [
        {
          "type": "Expr",
          "value": {
            "type": "Call",
            "func": {
              "type": "Name",
              "id": "print",
              "ctx": {
                "type": "Load"
              }
            },
            "args": [
              {
                "type": "Constant",
                "value": "Hello, world!",
                "kind": null
              }
            ],
            "keywords": []
          }
        }
      ],
      "decorator_list": [],
      "returns": null
    }
  ],
  "type_ignores": []
}
```

### JavaScript 代码 AST 示例

**输入：**
```javascript
function hello() {
  console.log('Hello, world!');
}
```

**输出：**
```json
{
  "type": "Program",
  "body": [
    {
      "type": "FunctionDeclaration",
      "id": {
        "type": "Identifier",
        "name": "hello"
      },
      "params": [],
      "body": {
        "type": "BlockStatement",
        "body": [
          {
            "type": "ExpressionStatement",
            "expression": {
              "type": "CallExpression",
              "callee": {
                "type": "MemberExpression",
                "object": {
                  "type": "Identifier",
                  "name": "console"
                },
                "property": {
                  "type": "Identifier",
                  "name": "log"
                },
                "computed": false
              },
              "arguments": [
                {
                  "type": "Literal",
                  "value": "Hello, world!",
                  "raw": "'Hello, world!'"
                }
              ]
            }
          }
        ]
      },
      "generator": false,
      "async": false
    }
  ],
  "sourceType": "script"
}
```

## 依赖

- Python 3.7+
- 对于 JavaScript：`esprima` 库

## 安装依赖

```bash
# 安装 JavaScript 解析依赖
pip install esprima
```

## 注意事项

- AST 生成的质量取决于代码的语法正确性
- 非常大的代码文件可能需要更长的处理时间
- 不同语言的 AST 结构可能有所不同
- 对于最佳结果，提供语法正确的代码
- JavaScript AST 生成需要安装 `esprima` 库
- Java 和 C++ 的 AST 生成功能尚未完全实现

## 贡献

欢迎贡献！请随时提交 Pull Request。
