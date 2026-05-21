#!/usr/bin/env python3
"""
将输入的代码转化为 AST（抽象语法树）结构。
支持 Python、JavaScript、Java（占位）、C++（tree-sitter）。
"""

import argparse
import json
import os
import ast
import sys
from typing import Dict, Any, Optional, List

try:
    import esprima
    has_esprima = True
except ImportError:
    has_esprima = False

try:
    import tree_sitter
    from tree_sitter import Language, Parser
    import tree_sitter_cpp
    has_tree_sitter_cpp = True
except ImportError:
    has_tree_sitter_cpp = False


# ============================================================
# Python 解析
# ============================================================

def ast_to_dict(node):
    """将 AST 节点转换为字典"""
    if isinstance(node, ast.AST):
        result = {}
        result['type'] = node.__class__.__name__
        for field, value in ast.iter_fields(node):
            if isinstance(value, list):
                result[field] = [ast_to_dict(item) for item in value]
            elif isinstance(value, ast.AST):
                result[field] = ast_to_dict(value)
            else:
                result[field] = value
        return result
    else:
        return node

def parse_python(code):
    """解析 Python 代码并生成 AST"""
    try:
        tree = ast.parse(code)
        return ast_to_dict(tree)
    except SyntaxError as e:
        return {"error": f"语法错误: {str(e)}"}

def parse_javascript(code):
    """解析 JavaScript 代码并生成 AST"""
    if not has_esprima:
        return {"error": "需要安装 esprima 库: pip install esprima"}
    try:
        tree = esprima.parseScript(code)
        return tree.toDict()
    except Exception as e:
        return {"error": f"解析错误: {str(e)}"}

def parse_java(code):
    """解析 Java 代码并生成 AST"""
    return {"error": "Java 解析功能尚未实现，需要 javaparser"}


# ============================================================
# C++ 解析（tree-sitter）
# ============================================================

if has_tree_sitter_cpp:
    class TreeSitterCppParser:
        """
        使用 tree-sitter 实现的 C++ 解析器。
        """

        def __init__(self):
            """初始化 tree-sitter C++ 解析器。"""
            self.parser = None
            self._init_parser()

        def _init_parser(self) -> None:
            """初始化 tree-sitter 解析器（tree-sitter 0.25.x API）。"""
            try:
                cpp_language = Language(tree_sitter_cpp.language())
                self.parser = Parser(cpp_language)
            except Exception as e:
                print(f"警告：无法加载 tree-sitter-cpp 语言库: {e}", file=sys.stderr)

        def parse(self, code: str) -> Dict[str, Any]:
            """解析 C++ 代码并生成 AST。"""
            if self.parser is None:
                return {"error": "tree-sitter 解析器未初始化"}
            try:
                tree = self.parser.parse(bytes(code, "utf-8"))
                root_node = tree.root_node
                return self._cst_to_ast_dict(root_node)
            except Exception as e:
                return {"error": f"解析错误: {str(e)}"}

        def _cst_to_ast_dict(self, node) -> Dict[str, Any]:
            """将 tree-sitter CST 节点递归转换为干净的 JSON 结构。"""
            result = {
                "type": node.type,
                "start_point": node.start_point,
                "end_point": node.end_point,
                "text": node.text.decode("utf-8") if node.text else None,
            }
            if node.children:
                children = []
                for child in node.children:
                    if child.is_named:
                        children.append(self._cst_to_ast_dict(child))
                if children:
                    result["children"] = children
            return result

        def extract_functions(self, ast: Dict[str, Any]) -> List[Dict[str, Any]]:
            """从 AST 中提取所有函数定义。"""
            functions = []

            def traverse(node, parent_type: str = ""):
                if isinstance(node, dict):
                    node_type = node.get("type", "")
                    if node_type in ["function_definition", "function_declaration"]:
                        func_info = {
                            "type": node_type,
                            "name": self._extract_function_name(node),
                            "start_point": node.get("start_point"),
                            "end_point": node.get("end_point"),
                            "text": node.get("text"),
                        }
                        functions.append(func_info)
                    elif node_type in ("declaration", "field_declaration"):
                        if self._has_function_declarator(node):
                            func_info = {
                                "type": "function_declaration",
                                "name": self._extract_function_name(node),
                                "start_point": node.get("start_point"),
                                "end_point": node.get("end_point"),
                                "text": node.get("text"),
                            }
                            functions.append(func_info)

                    children = node.get("children", [])
                    for child in children:
                        traverse(child, node_type)

            traverse(ast)
            return functions

        def _has_function_declarator(self, node: Dict[str, Any]) -> bool:
            """递归检查节点中是否包含 function_declarator。"""
            if node.get("type") == "function_declarator":
                return True
            for child in node.get("children", []):
                if self._has_function_declarator(child):
                    return True
            return False

        def _extract_function_name(self, func_node: Dict[str, Any]) -> Optional[str]:
            """从函数定义节点中提取函数名。"""
            children = func_node.get("children", [])
            for child in children:
                if child.get("type") == "identifier":
                    return child.get("text")
                if child.get("type") == "declarator":
                    return self._find_identifier(child)
                if child.get("type") == "function_declarator":
                    return self._extract_from_func_declarator(child)
                if child.get("type") in ("reference_declarator", "pointer_declarator"):
                    for subchild in child.get("children", []):
                        if subchild.get("type") == "function_declarator":
                            return self._extract_from_func_declarator(subchild)
                        if subchild.get("type") == "identifier":
                            return subchild.get("text")
                    return None
            return None

        def _extract_from_func_declarator(self, node: Dict[str, Any]) -> Optional[str]:
            """从 function_declarator 节点中提取函数名。"""
            for subchild in node.get("children", []):
                sub_type = subchild.get("type", "")
                if sub_type == "operator_name":
                    return subchild.get("text")
                if sub_type == "destructor_name":
                    return subchild.get("text")
                if sub_type in ("identifier", "field_identifier"):
                    return subchild.get("text")
                if sub_type == "qualified_identifier":
                    result = self._find_identifier(subchild)
                    if result:
                        return result
                if sub_type == "declarator":
                    result = self._find_identifier(subchild)
                    if result:
                        return result
            return None

        def _find_identifier(self, node: Dict[str, Any]) -> Optional[str]:
            """在节点中查找标识符。"""
            if node.get("type") in ("identifier", "field_identifier"):
                return node.get("text")
            if node.get("type") == "parameter_list":
                return None
            children = node.get("children", [])
            for child in children:
                result = self._find_identifier(child)
                if result:
                    return result
            return None

        def extract_classes(self, ast: Dict[str, Any]) -> List[Dict[str, Any]]:
            """从 AST 中提取所有类定义。"""
            classes = []

            def traverse(node):
                if isinstance(node, dict):
                    node_type = node.get("type", "")
                    if node_type in ["class_specifier", "struct_specifier"]:
                        class_info = {
                            "type": node_type,
                            "name": self._extract_class_name(node),
                            "start_point": node.get("start_point"),
                            "end_point": node.get("end_point"),
                            "text": node.get("text"),
                        }
                        classes.append(class_info)
                    children = node.get("children", [])
                    for child in children:
                        traverse(child)

            traverse(ast)
            return classes

        def _extract_class_name(self, class_node: Dict[str, Any]) -> Optional[str]:
            """从类定义节点中提取类名。"""
            children = class_node.get("children", [])
            for child in children:
                if child.get("type") == "type_identifier":
                    return child.get("text")
            return None

        def extract_variables(self, ast: Dict[str, Any]) -> List[Dict[str, Any]]:
            """从 AST 中提取所有变量声明。"""
            variables = []

            def traverse(node, parent_type: str = ""):
                if isinstance(node, dict):
                    node_type = node.get("type", "")
                    if node_type in ("declaration", "field_declaration", "parameter_declaration"):
                        declared_type = self._extract_declared_type(node)
                        names = self._extract_variable_names(node)
                        for name in names:
                            var_info = {
                                "type": node_type,
                                "declared_type": declared_type,
                                "name": name,
                                "start_point": node.get("start_point"),
                                "end_point": node.get("end_point"),
                                "text": node.get("text"),
                            }
                            variables.append(var_info)
                    children = node.get("children", [])
                    for child in children:
                        traverse(child, node_type)

            traverse(ast)
            return variables

        def _extract_variable_names(self, decl_node: Dict[str, Any]) -> List[str]:
            """从声明节点中提取所有变量名（支持多重声明 int a, b;）。"""
            names = []
            children = decl_node.get("children", [])
            for child in children:
                child_type = child.get("type", "")
                if child_type == "init_declarator":
                    name = self._find_identifier(child)
                    if name:
                        names.append(name)
                elif child_type == "identifier":
                    name = child.get("text")
                    if name:
                        names.append(name)
                elif child_type == "field_identifier":
                    name = child.get("text")
                    if name:
                        names.append(name)
                elif child_type in ("reference_declarator", "pointer_declarator"):
                    name = self._find_identifier(child)
                    if name:
                        names.append(name)
            return names

        def _extract_declared_type(self, decl_node: Dict[str, Any]) -> Optional[str]:
            """从声明节点中提取声明类型。"""
            children = decl_node.get("children", [])
            for child in children:
                child_type = child.get("type", "")
                if child_type in ("type_specifier", "primitive_type", "sized_type_specifier", "type_identifier"):
                    return child.get("text")
                if child_type == "template_type":
                    return child.get("text")
                if child_type == "qualified_identifier":
                    return child.get("text")
            return None


def parse_cpp(code):
    """解析 C++ 代码并生成 AST"""
    if not has_tree_sitter_cpp:
        return {"error": "需要安装 tree-sitter 和 tree-sitter-cpp: pip install tree-sitter tree-sitter-cpp"}
    try:
        parser = TreeSitterCppParser()
        return parser.parse(code)
    except Exception as e:
        return {"error": f"C++ 解析错误: {str(e)}"}


# ============================================================
# 主函数
# ============================================================

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="代码转 AST 工具")
    parser.add_argument("--language", required=True, choices=["python", "javascript", "java", "cpp"],
                        help="编程语言")
    parser.add_argument("--code", help="直接代码输入")
    parser.add_argument("--file", help="代码文件路径")
    parser.add_argument("--output", help="保存 AST 输出的路径")
    parser.add_argument("--indent", type=int, default=2, help="JSON 输出的缩进空格数")

    args = parser.parse_args()

    if args.code:
        code = args.code
    elif args.file:
        if not os.path.exists(args.file):
            print(json.dumps({"error": f"文件不存在: {args.file}"}, indent=args.indent))
            return
        with open(args.file, "r", encoding="utf-8") as f:
            code = f.read()
    else:
        print(json.dumps({"error": "必须提供 --code 或 --file 参数"}, indent=args.indent))
        return

    if args.language == "python":
        ast_tree = parse_python(code)
    elif args.language == "javascript":
        ast_tree = parse_javascript(code)
    elif args.language == "java":
        ast_tree = parse_java(code)
    elif args.language == "cpp":
        ast_tree = parse_cpp(code)
    else:
        ast_tree = {"error": f"不支持的语言: {args.language}"}

    if args.output:
        os.makedirs(os.path.dirname(args.output), exist_ok=True)
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(ast_tree, f, indent=args.indent, ensure_ascii=False)
        print(f"AST 已保存到 {args.output}")
    else:
        print(json.dumps(ast_tree, indent=args.indent, ensure_ascii=False))


if __name__ == "__main__":
    main()
