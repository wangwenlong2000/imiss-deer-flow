#!/usr/bin/env python3
"""
使用 tree-sitter 将 C++ 代码转换成 AST 结构。
"""

import abc
import json
import os
import sys
from typing import Dict, Any, Optional, List

try:
    import tree_sitter
    from tree_sitter import Language, Parser
    import tree_sitter_cpp
    has_tree_sitter = True
except ImportError:
    has_tree_sitter = False


class BaseCppParser(abc.ABC):
    """
    C++ 解析器的抽象基类。
    """

    @abc.abstractmethod
    def parse(self, code: str) -> Dict[str, Any]:
        """
        解析 C++ 代码并生成 AST。

        参数:
            code (str): 要解析的 C++ 代码。

        返回:
            Dict[str, Any]: AST 结构。
        """
        pass


class TreeSitterCppParser(BaseCppParser):
    """
    使用 tree-sitter 实现的 C++ 解析器。
    """

    def __init__(self):
        """
        初始化 tree-sitter C++ 解析器。
        """
        self.parser = None
        self._init_parser()

    def _init_parser(self) -> None:
        """
        初始化 tree-sitter 解析器（tree-sitter 0.25.x API）。
        """
        if not has_tree_sitter:
            print("警告：tree-sitter 未安装，请安装 tree-sitter 和 tree-sitter-cpp")
            return

        try:
            # tree-sitter 0.25.x API：Language() 接受 PyCapsule 对象
            cpp_language = Language(tree_sitter_cpp.language())
            # 方法1：传递给 Parser 构造函数
            self.parser = Parser(cpp_language)
            print("Tree-sitter C++ parser initialized successfully")
        except Exception as e:
            print(f"警告：无法加载 tree-sitter-cpp 语言库: {e}")
            print("请确保已安装 tree-sitter-cpp (pip install tree-sitter-cpp)")
            return

    def parse(self, code: str) -> Dict[str, Any]:
        """
        解析 C++ 代码并生成 AST。

        参数:
            code (str): 要解析的 C++ 代码。

        返回:
            Dict[str, Any]: AST 结构。
        """
        if self.parser is None:
            return {"error": "tree-sitter 解析器未初始化"}

        try:
            tree = self.parser.parse(bytes(code, "utf-8"))
            root_node = tree.root_node
            return self._cst_to_ast_dict(root_node)
        except Exception as e:
            return {"error": f"解析错误: {str(e)}"}

    def _cst_to_ast_dict(self, node) -> Dict[str, Any]:
        """
        将 tree-sitter CST 节点递归转换为干净的 JSON 结构。

        参数:
            node: tree-sitter 节点。

        返回:
            Dict[str, Any]: 干净的 AST 结构。
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
                if child.is_named:
                    children.append(self._cst_to_ast_dict(child))
            if children:
                result["children"] = children

        return result

    def extract_functions(self, ast: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        从 AST 中提取所有函数定义。

        参数:
            ast (Dict[str, Any]): AST 结构。

        返回:
            List[Dict[str, Any]]: 函数定义列表。
        """
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

                children = node.get("children", [])
                for child in children:
                    traverse(child, node_type)

        traverse(ast)
        return functions

    def _extract_function_name(self, func_node: Dict[str, Any]) -> Optional[str]:
        """
        从函数定义节点中提取函数名。

        参数:
            func_node (Dict[str, Any]): 函数定义节点。

        返回:
            Optional[str]: 函数名。
        """
        children = func_node.get("children", [])
        for child in children:
            if child.get("type") == "identifier":
                return child.get("text")
            elif child.get("type") == "declarator":
                # 递归查找标识符
                return self._find_identifier(child)
        return None

    def _find_identifier(self, node: Dict[str, Any]) -> Optional[str]:
        """
        在节点中查找标识符。

        参数:
            node (Dict[str, Any]): 节点。

        返回:
            Optional[str]: 标识符文本。
        """
        if node.get("type") == "identifier":
            return node.get("text")
        children = node.get("children", [])
        for child in children:
            result = self._find_identifier(child)
            if result:
                return result
        return None

    def extract_classes(self, ast: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        从 AST 中提取所有类定义。

        参数:
            ast (Dict[str, Any]): AST 结构。

        返回:
            List[Dict[str, Any]]: 类定义列表。
        """
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
        """
        从类定义节点中提取类名。

        参数:
            class_node (Dict[str, Any]): 类定义节点。

        返回:
            Optional[str]: 类名。
        """
        children = class_node.get("children", [])
        for child in children:
            if child.get("type") == "type_identifier":
                return child.get("text")
        return None

    def extract_variables(self, ast: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        从 AST 中提取所有变量声明。

        参数:
            ast (Dict[str, Any]): AST 结构。

        返回:
            List[Dict[str, Any]]: 变量声明列表。
        """
        variables = []

        def traverse(node, parent_type: str = ""):
            if isinstance(node, dict):
                node_type = node.get("type", "")

                if node_type == "declaration":
                    var_info = {
                        "type": node_type,
                        "declared_type": self._extract_declared_type(node),
                        "name": self._extract_variable_name(node),
                        "start_point": node.get("start_point"),
                        "end_point": node.get("end_point"),
                        "text": node.get("text"),
                    }
                    if var_info["name"]:
                        variables.append(var_info)

                children = node.get("children", [])
                for child in children:
                    traverse(child, node_type)

        traverse(ast)
        return variables

    def _extract_declared_type(self, decl_node: Dict[str, Any]) -> Optional[str]:
        """
        从声明节点中提取声明类型。

        参数:
            decl_node (Dict[str, Any]): 声明节点。

        返回:
            Optional[str]: 声明类型。
        """
        children = decl_node.get("children", [])
        for child in children:
            if child.get("type") in ["type_specifier", "primitive_type"]:
                return child.get("text")
            if child.get("type") == "template_type":
                return child.get("text")   # ← 新增分支
        return None

    def _extract_variable_name(self, decl_node: Dict[str, Any]) -> Optional[str]:
        """
        从声明节点中提取变量名。

        参数:
            decl_node (Dict[str, Any]): 声明节点。

        返回:
            Optional[str]: 变量名。
        """
        children = decl_node.get("children", [])
        for child in children:
            if child.get("type") == "init_declarator":
                # 查找声明符中的标识符
                return self._find_identifier(child)
            elif child.get("type") == "identifier":
                return child.get("text")
            elif child.get("type") == "reference_declarator":
                return self._find_identifier(child)  # ← 新增分支
        return None


class MockCppParser(BaseCppParser):
    """
    模拟的 C++ 解析器，当 tree-sitter 不可用时使用。
    """

    def parse(self, code: str) -> Dict[str, Any]:
        """
        解析 C++ 代码并生成模拟的 AST。

        参数:
            code (str): 要解析的 C++ 代码。

        返回:
            Dict[str, Any]: 模拟的 AST 结构。
        """
        result = {
            "type": "translation_unit",
            "start_point": (0, 0),
            "end_point": (len(code.splitlines()), 0),
            "text": code,
            "children": []
        }

        lines = code.splitlines()
        for i, line in enumerate(lines):
            line = line.strip()

            if line.startswith("class ") or line.startswith("struct "):
                class_name = line.split()[1].replace("{", "").replace(";", "")
                result["children"].append({
                    "type": "class_specifier" if line.startswith("class") else "struct_specifier",
                    "start_point": (i, 0),
                    "end_point": (i, len(line)),
                    "text": line,
                    "children": []
                })
            elif "(" in line and ")" in line and ";" not in line.split(")")[-1]:
                func_name = line.split()[1].split("(")[0]
                result["children"].append({
                    "type": "function_definition",
                    "start_point": (i, 0),
                    "end_point": (i, len(line)),
                    "text": line,
                    "children": []
                })
            elif ";" in line and ("=" in line or line.count(" ") > 0):
                parts = line.split(";")[0].split()
                if len(parts) >= 2:
                    var_type = parts[0]
                    var_name = parts[1].split("=")[0]
                    result["children"].append({
                        "type": "declaration",
                        "start_point": (i, 0),
                        "end_point": (i, len(line)),
                        "text": line,
                        "children": []
                    })

        return result


class CppAstExtractor:
    """
    C++ AST 提取器，提供统一的接口。
    """

    def __init__(self):
        """
        初始化 C++ AST 提取器。
        """
        self.parser = TreeSitterCppParser() if has_tree_sitter else MockCppParser()

    def extract(self, code: str) -> Dict[str, Any]:
        """
        提取 C++ 代码的 AST。

        参数:
            code (str): 要分析的 C++ 代码。

        返回:
            Dict[str, Any]: 包含 AST 和提取信息的字典。
        """
        ast = self.parser.parse(code)

        if "error" in ast:
            return {"ast": ast, "functions": [], "classes": [], "variables": []}

        functions = self.parser.extract_functions(ast) if has_tree_sitter else []
        classes = self.parser.extract_classes(ast) if has_tree_sitter else []
        variables = self.parser.extract_variables(ast) if has_tree_sitter else []

        return {
            "ast": ast,
            "functions": functions,
            "classes": classes,
            "variables": variables
        }


# 确保 tree-sitter 相关导入在使用时才执行
if __name__ == "__main__":
    # 示例代码
    cpp_code = """
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
"""

    extractor = CppAstExtractor()
    result = extractor.extract(cpp_code)
    print(json.dumps(result, ensure_ascii=False, indent=2))
