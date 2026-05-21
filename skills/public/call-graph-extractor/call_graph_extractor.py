#!/usr/bin/env python3
"""
调用图提取器模块，用于从 Python 代码片段中静态提取函数调用关系。
"""

import abc
import ast
from typing import Dict, List, Any


class BaseCallGraphExtractor(abc.ABC):
    """
    调用图提取器的抽象基类。
    """
    
    @abc.abstractmethod
    def extract(self, code: str) -> dict:
        """
        从代码中提取调用图。
        
        参数:
            code (str): 要分析的 Python 代码字符串。
        
        返回:
            dict: 包含调用关系的字典，格式为 {"calls": [{"caller": str, "callee": str}]}
        """
        pass


class ASTPythonCallGraphExtractor(BaseCallGraphExtractor):
    """
    使用 AST 解析的 Python 调用图提取器。
    """
    
    def extract(self, code: str) -> dict:
        """
        从 Python 代码中提取调用图。
        
        参数:
            code (str): 要分析的 Python 代码字符串。
        
        返回:
            dict: 包含调用关系的字典，格式为 {"calls": [{"caller": str, "callee": str}]}
        """
        try:
            # 解析代码为 AST
            tree = ast.parse(code)
            
            # 创建访问器实例
            visitor = CallVisitor()
            
            # 遍历 AST
            visitor.visit(tree)
            
            # 返回结果
            return {
                "calls": visitor.calls
            }
        except SyntaxError:
            # 如果代码存在语法错误，返回空的调用记录
            return {
                "calls": []
            }


class CallVisitor(ast.NodeVisitor):
    """
    AST 访问器，用于提取函数调用关系。
    """
    
    def __init__(self):
        """
        初始化 CallVisitor。
        """
        # 存储调用关系的列表
        self.calls: List[Dict[str, str]] = []
        # 存储当前函数栈，用于跟踪当前的调用者
        self.function_stack: List[str] = ["main"]  # 初始为全局作用域
    
    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        """
        访问函数定义节点。
        
        参数:
            node (ast.FunctionDef): 函数定义节点。
        """
        # 将当前函数名压入栈中
        self.function_stack.append(node.name)
        
        # 继续访问函数体
        self.generic_visit(node)
        
        # 函数访问结束，从栈中弹出
        self.function_stack.pop()
    
    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        """
        访问异步函数定义节点。
        
        参数:
            node (ast.AsyncFunctionDef): 异步函数定义节点。
        """
        # 处理方式与普通函数类似
        self.function_stack.append(node.name)
        self.generic_visit(node)
        self.function_stack.pop()
    
    def visit_Call(self, node: ast.Call) -> None:
        """
        访问函数调用节点。
        
        参数:
            node (ast.Call): 函数调用节点。
        """
        # 获取当前的调用者（栈顶元素）
        caller = self.function_stack[-1]
        
        # 解析被调用者
        callee = self._parse_callee(node.func)
        
        # 如果成功解析出被调用者，添加到调用关系列表
        if callee:
            self.calls.append({
                "caller": caller,
                "callee": callee
            })
        
        # 继续访问其他节点
        self.generic_visit(node)
    
    def _parse_callee(self, node: ast.AST) -> str:
        """
        解析被调用者的名称。
        
        参数:
            node (ast.AST): 被调用者的 AST 节点。
        
        返回:
            str: 被调用者的名称，如果无法解析则返回空字符串。
        """
        if isinstance(node, ast.Name):
            # 简单函数调用，如 func()
            return node.id
        elif isinstance(node, ast.Attribute):
            # 属性方法调用，如 cv2.VideoCapture() 或 self.method()
            # 递归解析属性链
            value = self._parse_callee(node.value)
            if value:
                return f"{value}.{node.attr}"
            return node.attr
        elif isinstance(node, ast.Subscript):
            # 处理下标访问，如 obj[0]()
            value = self._parse_callee(node.value)
            if value:
                return value
            return ""
        elif isinstance(node, ast.BinOp):
            # 处理二元操作，如 a + b()
            left = self._parse_callee(node.left)
            right = self._parse_callee(node.right)
            if left:
                return left
            if right:
                return right
            return ""
        elif isinstance(node, ast.UnaryOp):
            # 处理一元操作，如 -func()
            return self._parse_callee(node.operand)
        else:
            # 其他类型的节点，无法解析
            return ""
