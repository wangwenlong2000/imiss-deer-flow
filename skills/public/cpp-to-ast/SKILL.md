# C++ AST 提取器

## 基本信息
- **名称**: C++ AST 提取器
- **描述**: 使用 tree-sitter 将 C++ 代码转换成 AST 结构，并提取函数、类和变量信息。
- **版本**: 1.0.0
- **标签**: C++, AST, 代码分析, tree-sitter

## 功能
- **AST 解析**: 使用 tree-sitter 解析 C++ 代码，生成抽象语法树
- **节点过滤**: 只处理命名节点，过滤匿名节点（如括号、分号、空白等）
- **函数提取**: 从 AST 中提取所有函数定义和声明
- **类提取**: 从 AST 中提取所有类和结构体定义
- **变量提取**: 从 AST 中提取所有变量声明
- **回退机制**: 当 tree-sitter 不可用时，使用模拟解析器

## 工作流程
1. **代码输入**: 接收 C++ 代码字符串或文件路径
2. **AST 解析**: 使用 tree-sitter 解析代码生成 CST
3. **节点转换**: 将 CST 节点转换为干净的 JSON 结构
4. **信息提取**: 从 AST 中提取函数、类和变量信息
5. **结果输出**: 返回包含 AST 和提取信息的 JSON 结构

## 参数

### 输入参数
| 参数名 | 类型 | 描述 | 是否必填 | 默认值 |
|-------|------|------|---------|-------|
| code | string | 要分析的 C++ 代码字符串 | 是 | 无 |
| file | string | 包含要分析的代码的文件路径 | 否 | 无 |

### 输出参数
| 参数名 | 类型 | 描述 |
|-------|------|------|
| ast | object | AST 结构，包含节点类型、位置和文本信息 |
| functions | array | 提取的函数列表 |
| classes | array | 提取的类和结构体列表 |
| variables | array | 提取的变量列表 |

## 命令行使用

### 分析代码
```bash
python scripts/convert.py \
  --code "#include <iostream>\n\nclass Camera {\npublic:\n    void capture();\n};\n\nint main() {\n    return 0;\n}"
```

### 分析代码文件
```bash
python scripts/convert.py \
  --file input.cpp \
  --output output.json
```

### 只提取函数
```bash
python scripts/convert.py \
  --file input.cpp \
  --functions
```

### 只提取类
```bash
python scripts/convert.py \
  --file input.cpp \
  --classes
```

### 只提取变量
```bash
python scripts/convert.py \
  --file input.cpp \
  --variables
```

## 输出格式

### 完整输出
```json
{
  "ast": {
    "type": "translation_unit",
    "start_point": [0, 0],
    "end_point": [12, 1],
    "text": "#include <iostream>\n...",
    "children": [...]
  },
  "functions": [
    {
      "type": "function_definition",
      "name": "detect",
      "start_point": [12, 0],
      "end_point": [16, 1],
      "text": "void detect() {\n    Camera cam;\n    cam.capture();\n}"
    }
  ],
  "classes": [
    {
      "type": "class_specifier",
      "name": "Camera",
      "start_point": [4, 0],
      "end_point": [7, 1],
      "text": "class Camera {\npublic:\n    void capture();\n};"
    }
  ],
  "variables": [
    {
      "type": "declaration",
      "declared_type": "Camera",
      "name": "cam",
      "start_point": [13, 4],
      "end_point": [13, 12],
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

## 示例

### 输入示例
```cpp
#include <iostream>
#include <vector>

class Camera {
public:
    void capture();
    void process();
};

struct Point {
    int x;
    int y;
};

void detect() {
    Camera cam;
    cam.capture();
    cam.process();
}

int main() {
    detect();
    return 0;
}
```

### 输出示例
```json
{
  "ast": {
    "type": "translation_unit",
    "start_point": [0, 0],
    "end_point": [22, 1],
    "text": "#include <iostream>\n#include <vector>\n\nclass Camera {\npublic:\n    void capture();\n    void process();\n};\n\nstruct Point {\n    int x;\n    int y;\n};\n\nvoid detect() {\n    Camera cam;\n    cam.capture();\n    cam.process();\n}\n\nint main() {\n    detect();\n    return 0;\n}",
    "children": [...]
  },
  "functions": [
    {
      "type": "function_definition",
      "name": "detect",
      "start_point": [15, 0],
      "end_point": [19, 1],
      "text": "void detect() {\n    Camera cam;\n    cam.capture();\n    cam.process();\n}"
    },
    {
      "type": "function_definition",
      "name": "main",
      "start_point": [21, 0],
      "end_point": [24, 1],
      "text": "int main() {\n    detect();\n    return 0;\n}"
    }
  ],
  "classes": [
    {
      "type": "class_specifier",
      "name": "Camera",
      "start_point": [3, 0],
      "end_point": [8, 1],
      "text": "class Camera {\npublic:\n    void capture();\n    void process();\n};"
    },
    {
      "type": "struct_specifier",
      "name": "Point",
      "start_point": [10, 0],
      "end_point": [14, 1],
      "text": "struct Point {\n    int x;\n    int y;\n};"
    }
  ],
  "variables": [
    {
      "type": "declaration",
      "declared_type": "Camera",
      "name": "cam",
      "start_point": [16, 4],
      "end_point": [16, 12],
      "text": "Camera cam;"
    }
  ]
}
```
