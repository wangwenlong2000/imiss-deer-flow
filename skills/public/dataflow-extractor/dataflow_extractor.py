#!/usr/bin/env python3
"""
数据流提取器模块，用于从 Python 代码片段中静态提取变量和模块之间的数据流转关系。
"""

import abc
import ast
from typing import Dict, List, Any


class BaseDataflowExtractor(abc.ABC):
    """
    数据流提取器的抽象基类。
    """
    
    @abc.abstractmethod
    def extract(self, code: str) -> dict:
        """
        从代码中提取数据流关系。
        
        参数:
            code (str): 要分析的 Python 代码字符串。
        
        返回:
            dict: 包含数据流关系的字典，格式为 {"dataflow": [["Source", "Destination"]]}
        """
        pass


class ASTDataflowExtractor(BaseDataflowExtractor):
    """
    使用 AST 解析的数据流提取器。
    """
    
    def extract(self, code: str) -> dict:
        """
        从 Python 代码中提取数据流关系。
        
        参数:
            code (str): 要分析的 Python 代码字符串。
        
        返回:
            dict: 包含数据流关系的字典，格式为 {"dataflow": [["Source", "Destination"]]}
        """
        try:
            # 解析代码为 AST
            tree = ast.parse(code)
            
            # 创建访问器实例
            visitor = DataflowVisitor()
            
            # 遍历 AST
            visitor.visit(tree)
            
            # 返回结果
            return {
                "dataflow": visitor.dataflow
            }
        except SyntaxError:
            # 如果代码存在语法错误，返回空的数据流列表
            return {
                "dataflow": []
            }


class DataflowVisitor(ast.NodeVisitor):
    """
    AST 访问器，用于提取数据流关系。
    """
    
    def __init__(self):
        """
        初始化 DataflowVisitor。
        """
        # 存储数据流关系的列表
        self.dataflow: List[List[str]] = []
    
    def visit_Assign(self, node: ast.Assign) -> None:
        """
        访问赋值节点。
        
        参数:
            node (ast.Assign): 赋值节点。
        """
        # 遍历所有目标变量
        for target in node.targets:
            # 提取目标变量名称
            target_name = self._extract_name(target)
            if not target_name:
                continue
            
            # 提取右值的数据源
            sources = self._extract_sources(node.value)
            for source in sources:
                # 添加数据流关系
                self.dataflow.append([source, target_name])
        
        # 继续访问其他节点
        self.generic_visit(node)
    
    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        """
        访问带类型注解的赋值节点。
        
        参数:
            node (ast.AnnAssign): 带类型注解的赋值节点。
        """
        # 提取目标变量名称
        target_name = self._extract_name(node.target)
        if not target_name:
            return
        
        # 提取右值的数据源
        sources = self._extract_sources(node.value)
        for source in sources:
            # 添加数据流关系
            self.dataflow.append([source, target_name])
        
        # 继续访问其他节点
        self.generic_visit(node)
    
    def _extract_name(self, node: ast.AST) -> str:
        """
        从节点中提取变量名称。
        
        参数:
            node (ast.AST): AST 节点。
        
        返回:
            str: 变量名称，如果无法提取则返回空字符串。
        """
        if isinstance(node, ast.Name):
            # 简单变量名，如 a
            return node.id
        elif isinstance(node, ast.Attribute):
            # 属性访问，如 obj.attr
            # 递归提取对象名称
            value = self._extract_name(node.value)
            if value:
                return f"{value}.{node.attr}"
            return node.attr
        elif isinstance(node, ast.Subscript):
            # 下标访问，如 obj[index]
            # 提取对象名称
            value = self._extract_name(node.value)
            if value:
                return value
            return ""
        elif isinstance(node, ast.Tuple) or isinstance(node, ast.List):
            # 解包赋值，如 a, b = c, d
            # 这里只处理简单的解包，返回第一个变量名
            if node.elts:
                return self._extract_name(node.elts[0])
            return ""
        else:
            # 其他类型的节点，无法提取
            return ""
    
    def _extract_sources(self, node: ast.AST) -> List[str]:
        """
        从节点中提取数据源。
        
        参数:
            node (ast.AST): AST 节点。
        
        返回:
            List[str]: 数据源名称列表。
        """
        sources = []
        
        if isinstance(node, ast.Name):
            # 规则1：右值是一个变量（Name），提取变量名
            sources.append(node.id)
        elif isinstance(node, ast.Call):
            # 规则2：右值是一个函数调用（Call），提取函数名或对象
            # 提取被调用者
            callee = self._extract_callee(node.func)
            if callee:
                sources.append(callee)
            
            # 提取函数参数中的变量
            for arg in node.args:
                arg_sources = self._extract_sources(arg)
                sources.extend(arg_sources)
            
            # 提取关键字参数中的变量
            for kw in node.keywords:
                kw_sources = self._extract_sources(kw.value)
                sources.extend(kw_sources)
        elif isinstance(node, ast.Attribute):
            # 属性访问，如 obj.attr
            # 提取对象名称
            value = self._extract_name(node.value)
            if value:
                sources.append(value)
        elif isinstance(node, ast.BinOp):
            # 二元操作，如 a + b
            # 提取左右操作数中的变量
            left_sources = self._extract_sources(node.left)
            right_sources = self._extract_sources(node.right)
            sources.extend(left_sources)
            sources.extend(right_sources)
        elif isinstance(node, ast.UnaryOp):
            # 一元操作，如 -a
            # 提取操作数中的变量
            operand_sources = self._extract_sources(node.operand)
            sources.extend(operand_sources)
        elif isinstance(node, ast.Tuple) or isinstance(node, ast.List):
            # 元组或列表，如 (a, b) 或 [a, b]
            # 提取元素中的变量
            for elt in node.elts:
                elt_sources = self._extract_sources(elt)
                sources.extend(elt_sources)
        elif isinstance(node, ast.Dict):
            # 字典，如 {"a": b, "c": d}
            # 提取值中的变量
            for value in node.values:
                value_sources = self._extract_sources(value)
                sources.extend(value_sources)
        # 规则3：忽略纯字面量（常量、数字、字符串）的赋值
        # 这里不处理 ast.Constant、ast.Num、ast.Str 等字面量节点
        
        # 去重
        return list(set(sources))
    
    def _extract_callee(self, node: ast.AST) -> str:
        """
        提取函数调用的被调用者名称。
        
        参数:
            node (ast.AST): 被调用者的 AST 节点。
        
        返回:
            str: 被调用者的名称，如果无法提取则返回空字符串。
        """
        if isinstance(node, ast.Name):
            # 简单函数调用，如 func()
            return node.id
        elif isinstance(node, ast.Attribute):
            # 属性方法调用，如 obj.method()
            # 递归提取对象名称
            value = self._extract_name(node.value)
            if value:
                return value  # 优先提取对象名称，如 tracker.update 提取 tracker
            return node.attr
        elif isinstance(node, ast.Subscript):
            # 下标访问调用，如 obj[0]()
            # 提取对象名称
            value = self._extract_name(node.value)
            if value:
                return value
            return ""
        else:
            # 其他类型的节点，无法提取
            return ""
