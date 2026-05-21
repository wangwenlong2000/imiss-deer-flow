# C++ AST 提取器

一个使用 tree-sitter 将 C++ 代码转换成 AST 结构的 DeerFlow 技能。

## 功能

- **AST 解析**: 使用 tree-sitter 解析 C++ 代码，生成抽象语法树
- **节点过滤**: 只处理命名节点，过滤匿名节点（如括号、分号、空白等）
- **函数提取**: 从 AST 中提取所有函数定义和声明
- **类提取**: 从 AST 中提取所有类和结构体定义
- **变量提取**: 从 AST 中提取所有变量声明
- **回退机制**: 当 tree-sitter 不可用时，使用模拟解析器

## 架构设计

- **抽象基类**: `BaseCppParser` 定义统一的解析接口
- **具体实现**:
  - `TreeSitterCppParser`: 使用 tree-sitter 的真实解析器
  - `MockCppParser`: 模拟解析器，当 tree-sitter 不可用时使用
- **提取器**: `CppAstExtractor` 提供统一的接口

## 使用方法

### 编程接口

```python
from cpp_to_ast import CppAstExtractor

# 创建提取器
extractor = CppAstExtractor()

# C++ 代码
cpp_code = """
#include <iostream>

class Camera {
public:
    void capture();
};

void detect() {
    Camera cam;
    cam.capture();
}

int main() {
    detect();
    return 0;
}
"""

# 提取 AST
result = extractor.extract(cpp_code)

# 输出结果
print(result["ast"])
print(result["functions"])
print(result["classes"])
print(result["variables"])
```

### 命令行使用

```bash
# 分析代码字符串
python scripts/convert.py \
  --code "#include <iostream>\n\nclass Camera {\npublic:\n    void capture();\n};\n\nint main() {\n    return 0;\n}"

# 分析代码文件
python scripts/convert.py \
  --file input.cpp \
  --output output.json

# 只提取函数
python scripts/convert.py \
  --file input.cpp \
  --functions

# 只提取类
python scripts/convert.py \
  --file input.cpp \
  --classes

# 只提取变量
python scripts/convert.py \
  --file input.cpp \
  --variables
```

## 核心代码

### CST 转 AST

```python
def _cst_to_ast_dict(self, node) -> Dict[str, Any]:
    """
    将 tree-sitter CST 节点递归转换为干净的 JSON 结构
    """
    result = {
        "type": node.type,
        "start_point": node.start_point,
        "end_point": node.end_point,
        "text": node.text.decode("utf-8") if node.text else None,
    }

    if node.children:
        children = []
        for child in node.children:
            if child.is_named:  # 关键过滤条件
                children.append(self._cst_to_ast_dict(child))
        if children:
            result["children"] = children

    return result
```

## 输出格式

```json
{
  "ast": {
    "type": "translation_unit",
    "start_point": [0, 0],
    "end_point": [12, 1],
    "text": "...",
    "children": [...]
  },
  "functions": [
    {
      "type": "function_definition",
      "name": "detect",
      "start_point": [6, 0],
      "end_point": [9, 1],
      "text": "void detect() {...}"
    }
  ],
  "classes": [
    {
      "type": "class_specifier",
      "name": "Camera",
      "start_point": [2, 0],
      "end_point": [5, 1],
      "text": "class Camera {...}"
    }
  ],
  "variables": [
    {
      "type": "declaration",
      "declared_type": "Camera",
      "name": "cam",
      "start_point": [7, 4],
      "end_point": [7, 12],
      "text": "Camera cam;"
    }
  ]
}
```

## 依赖

- Python 3.7+
- tree-sitter
- tree-sitter-cpp

## 安装依赖

```bash
pip install tree-sitter
```

注意：需要单独安装 tree-sitter-cpp 语言库。

## 注意事项

- 需要提前安装 tree-sitter 和 tree-sitter-cpp
- 如果 tree-sitter 不可用，会自动使用模拟解析器
- 模拟解析器的功能有限，建议安装 tree-sitter 以获得更好的解析效果
- AST 输出只包含命名节点，匿名节点（如括号、分号）会被过滤

## 扩展指南

### 添加新的提取功能

1. 在 `TreeSitterCppParser` 类中添加新的提取方法
2. 在 `CppAstExtractor.extract` 方法中调用新方法
3. 更新命令行工具支持新的提取选项

### 支持其他语言

1. 创建新的语言解析器类，继承自 `BaseCppParser`
2. 修改 `CppAstExtractor` 以支持多种语言

## 集成到 DeerFlow

1. 将此技能添加到 DeerFlow 的技能列表中
2. 在需要 C++ 代码分析的地方调用此技能
3. 使用提取结果为 Agent 提供代码的语义上下文

## 贡献

欢迎贡献！请随时提交 Pull Request。
