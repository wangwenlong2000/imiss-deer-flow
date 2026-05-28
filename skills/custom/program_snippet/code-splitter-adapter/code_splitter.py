#!/usr/bin/env python3
"""
代码切分工具适配器模块，使用适配器模式和工厂模式封装不同的代码切分策略。
"""

import abc
from typing import Dict, List, Any


def _normalize_language(language: str) -> str:
    aliases = {
        "c++": "cpp",
        "cc": "cpp",
        "cxx": "cpp",
        "h++": "cpp",
        "hpp": "cpp",
        "hxx": "cpp",
        "js": "javascript",
        "ts": "typescript",
    }
    normalized = language.strip().lower()
    return aliases.get(normalized, normalized)


def _build_langchain_language_map(Language) -> Dict[str, Any]:
    # LangChain's Language enum changes across versions. Build this map
    # defensively so unsupported enum members do not break supported languages.
    language_attrs = {
        "python": ("PYTHON",),
        "java": ("JAVA",),
        "javascript": ("JS", "JAVASCRIPT"),
        "typescript": ("TS", "TYPESCRIPT", "JS", "JAVASCRIPT"),
        "html": ("HTML",),
        "css": ("CSS",),
        "markdown": ("MARKDOWN",),
        "json": ("JSON",),
        "xml": ("XML",),
        "sql": ("SQL",),
        "rust": ("RUST",),
        "go": ("GO",),
        "cpp": ("CPP",),
        "c": ("C",),
        "php": ("PHP",),
        "ruby": ("RUBY",),
        "swift": ("SWIFT",),
        "kotlin": ("KOTLIN", "JAVA"),
    }

    language_map = {}
    for language, attrs in language_attrs.items():
        for attr in attrs:
            if hasattr(Language, attr):
                language_map[language] = getattr(Language, attr)
                break
    return language_map


class BaseCodeSplitter(abc.ABC):
    """
    代码切分工具的抽象基类。
    """
    
    @abc.abstractmethod
    def split_code(self, code: str, language: str, metadata: dict = None) -> List[Dict[str, Any]]:
        """
        将代码切分为多个片段。
        
        参数:
            code (str): 要切分的代码字符串。
            language (str): 代码的编程语言。
            metadata (dict, optional): 附加的元数据。
        
        返回:
            List[Dict[str, Any]]: 切分后的代码片段列表，每个片段包含 content 和 metadata 字段。
        """
        pass


class LangChainCodeSplitterAdapter(BaseCodeSplitter):
    """
    LangChain 代码切分工具的适配器。
    """
    
    def __init__(self, **kwargs):
        """
        初始化 LangChain 代码切分器适配器。
        
        参数:
            **kwargs: 传递给 RecursiveCharacterTextSplitter 的额外参数。
        """
        self.kwargs = kwargs
    
    def split_code(self, code: str, language: str, metadata: dict = None) -> List[Dict[str, Any]]:
        """
        使用 LangChain 的 RecursiveCharacterTextSplitter 切分代码。
        
        参数:
            code (str): 要切分的代码字符串。
            language (str): 代码的编程语言。
            metadata (dict, optional): 附加的元数据。
        
        返回:
            List[Dict[str, Any]]: 切分后的代码片段列表。
        """
        try:
            from langchain_text_splitters import RecursiveCharacterTextSplitter
            from langchain_text_splitters import Language
        except ImportError:
            raise ImportError("langchain_text_splitters 模块未安装，请运行 'pip install langchain-text-splitters'")
        
        # 语言映射
        language_map = _build_langchain_language_map(Language)
        
        # 获取对应的 Language 枚举值
        lang_enum = language_map.get(_normalize_language(language))
        if not lang_enum:
            raise ValueError(f"不支持的语言: {language}")
        
        # 创建切分器
        splitter = RecursiveCharacterTextSplitter.from_language(
            language=lang_enum,
            **self.kwargs
        )
        
        # 切分代码
        splits = splitter.split_text(code)
        
        # 构建返回结果
        result = []
        for i, content in enumerate(splits):
            split_metadata = metadata.copy() if metadata else {}
            split_metadata['chunk_index'] = i
            result.append({
                'content': content,
                'metadata': split_metadata
            })
        
        return result


class LlamaIndexASTSplitterAdapter(BaseCodeSplitter):
    """
    LlamaIndex AST 代码切分工具的适配器。
    """
    
    def __init__(self, **kwargs):
        """
        初始化 LlamaIndex AST 代码切分器适配器。
        
        参数:
            **kwargs: 传递给 CodeSplitter 的额外参数。
        """
        self.kwargs = kwargs
    
    def split_code(self, code: str, language: str, metadata: dict = None) -> List[Dict[str, Any]]:
        """
        使用 LlamaIndex 的 CodeSplitter 切分代码。
        
        参数:
            code (str): 要切分的代码字符串。
            language (str): 代码的编程语言。
            metadata (dict, optional): 附加的元数据。
        
        返回:
            List[Dict[str, Any]]: 切分后的代码片段列表。
        """
        try:
            from llama_index.core.node_parser import CodeSplitter
        except ImportError:
            raise ImportError("llama_index 模块未安装，请运行 'pip install llama-index'")
        
        try:
            # 创建切分器
            splitter = CodeSplitter(
                language=_normalize_language(language),
                **self.kwargs
            )
            
            # 切分代码
            nodes = splitter.get_nodes_from_documents([code])
            
            # 构建返回结果
            result = []
            for i, node in enumerate(nodes):
                split_metadata = metadata.copy() if metadata else {}
                split_metadata['chunk_index'] = i
                # 从 node 中获取内容，具体属性可能因 LlamaIndex 版本而异
                content = getattr(node, 'text', getattr(node, 'content', str(node)))
                result.append({
                    'content': content,
                    'metadata': split_metadata
                })
            
            return result
        except Exception as e:
            # 捕获解析失败的异常，抛出友好的 RuntimeError
            raise RuntimeError(f"代码解析失败: {str(e)}. 可能是因为缺乏对应的 tree-sitter 语言包或代码语法错误。")


def get_code_splitter(strategy: str = "langchain", **kwargs) -> BaseCodeSplitter:
    """
    工厂函数，根据策略返回对应的代码切分器实例。
    
    参数:
        strategy (str): 切分策略，可选值为 "langchain" 或 "llamaindex"。
        **kwargs: 传递给切分器的额外参数。
    
    返回:
        BaseCodeSplitter: 代码切分器实例。
    """
    if strategy.lower() == "langchain":
        return LangChainCodeSplitterAdapter(**kwargs)
    elif strategy.lower() == "llamaindex":
        return LlamaIndexASTSplitterAdapter(**kwargs)
    else:
        raise ValueError(f"不支持的切分策略: {strategy}。可选值为 'langchain' 或 'llamaindex'。")
