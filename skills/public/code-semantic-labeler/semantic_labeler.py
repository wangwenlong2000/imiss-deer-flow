#!/usr/bin/env python3
"""
代码语义标注器模块，使用责任链模式实现代码语义标注。
"""

import abc
import ast
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any

_SKILL_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SKILL_DIR.parents[2]
_DEFAULT_CONFIG_PATH = _REPO_ROOT / "config.yaml"
if "DEER_FLOW_CONFIG_PATH" not in os.environ and _DEFAULT_CONFIG_PATH.exists():
    os.environ["DEER_FLOW_CONFIG_PATH"] = str(_DEFAULT_CONFIG_PATH)


# 添加 backend/packages/harness 到路径，以便导入 deerflow.models
_backend_path = os.path.join(os.path.dirname(__file__), "..", "..", "..", "backend", "packages", "harness")
if _backend_path not in sys.path:
    sys.path.insert(0, _backend_path)


class BaseLabeler(abc.ABC):
    """
    语义标注器的抽象基类。
    """

    @abc.abstractmethod
    def label(self, node_name: str, **context) -> Optional[str]:
        """
        为节点添加语义标签。

        参数:
            node_name (str): 节点名称。
            **context: 额外上下文信息（如 code, language, node_info, full_ast 等）。

        返回:
            Optional[str]: 语义标签，如果无法标注则返回 None。
        """
        pass


class RuleBasedLabeler(BaseLabeler):
    """
    基于规则的语义标注器。
    """

    def __init__(self, ontology_rules: Dict[str, str]):
        """
        初始化基于规则的语义标注器。

        参数:
            ontology_rules (Dict[str, str]): 本体规则字典，键为模式，值为标签。
        """
        self.ontology_rules = ontology_rules

    def label(self, node_name: str, **context) -> Optional[str]:
        """
        基于规则为节点添加语义标签。

        参数:
            node_name (str): 节点名称。
            **context: 额外上下文信息（规则标注器忽略）。

        返回:
            Optional[str]: 语义标签，如果无法标注则返回 None。
        """
        if node_name in self.ontology_rules:
            return self.ontology_rules[node_name]

        for pattern, label in self.ontology_rules.items():
            if node_name.startswith(pattern):
                return label

        return None


class LLMFallbackLabeler(BaseLabeler):
    """
    LLM 兜底语义标注器（增强版：支持代码上下文和 AST 上下文）。
    """

    # 有效标签集合
    VALID_LABELS = {
        "Camera", "Detection", "Tracking", "Database",
        "ImageProcessing", "DataProcessing", "MachineLearning",
        "Network", "FileIO", "General"
    }

    def __init__(self):
        """
        初始化 LLM 兜底语义标注器。
        """
        self.llm = None
        self._init_llm()

    def _init_llm(self):
        try:
            from deerflow.models import create_chat_model
            # 以脚本自身位置为锚点，计算到 config.yaml 的相对路径
            script_dir = os.path.dirname(os.path.abspath(__file__))
            self.llm = create_chat_model(thinking_enabled=False)
        except Exception as e:
            print(f"警告：创建 DeerFlow 模型时出错: {e}")

    def label(self, node_name: str, **context) -> Optional[str]:
        """
        使用 LLM 为节点添加语义标签（增强版）。

        参数:
            node_name (str): 节点名称。
            **context: 额外上下文信息，支持：
                - code (str): 原始代码全文
                - language (str): 编程语言
                - node_info (dict): 当前节点的详细信息（含 code_snippet, line_number, parent_type 等）
                - full_ast (dict): 完整 AST 字典

        返回:
            Optional[str]: 语义标签，如果无法标注则返回 None。
        """
        return self._call_llm_api(node_name, context)

    def _build_llm_prompt(self, name: str, code_context: str = "", ast_context: str = "") -> str:
        """
        构建增强 LLM Prompt，包含节点名称、代码上下文和 AST 上下文。
        """
        cat_str = ", ".join(sorted(self.VALID_LABELS))

        # 检测是否为 C++ 代码（用于代码块标记）
        code_marker = "cpp" if "#include" in code_context else "python"

        prompt = f"""你是一个代码语义标注专家。请根据以下信息判断代码节点属于哪个业务类别。

### 待标注的节点
节点名称: {name}

### 代码上下文
```{code_marker}
{code_context if code_context else "（无代码上下文）"}
```

### AST 上下文
{ast_context if ast_context else "（无 AST 上下文）"}

### 可选类别
{cat_str}

请只输出一个最合适的类别名称，不要输出其他内容。如果无法判断，请输出"General"。"""
        return prompt

    def _build_ast_context(self, node_info: dict) -> str:
        """
        从 AST 节点信息构建可读的 AST 上下文字符串。
        """
        parts = []
        if "parent_type" in node_info and node_info["parent_type"]:
            parts.append(f"- 父节点类型: {node_info['parent_type']}")
        if "node_type" in node_info and node_info["node_type"]:
            parts.append(f"- 节点类型: {node_info['node_type']}")
        if "line_number" in node_info:
            parts.append(f"- 所在行号: {node_info['line_number']}")
        if "parent_name" in node_info and node_info["parent_name"]:
            parts.append(f"- 父节点名称: {node_info['parent_name']}")
        return "\n".join(parts) if parts else ""

    def _call_llm_api(self, name: str, context: dict = None) -> Optional[str]:
        """
        调用 LLM API 为节点添加语义标签（增强版）。

        参数:
            name (str): 节点名称。
            context (dict, optional): 上下文信息，包含 code, language, node_info, full_ast 等。

        返回:
            Optional[str]: 语义标签，如果无法标注则返回 None。
        """
        if self.llm is None:
            return self._fallback_label(name)

        if context is None:
            context = {}

        # 提取上下文信息
        code = context.get("code", "")
        language = context.get("language", "python")
        node_info = context.get("node_info", {})
        full_ast = context.get("full_ast", {})

        # 构建代码上下文：优先使用行级代码片段，其次使用全文
        code_context = node_info.get("code_snippet", "")
        if not code_context and code:
            # 如果没有行级片段，使用完整代码的前500字符作为上下文
            code_context = code[:500]

        # 构建 AST 上下文
        ast_context = self._build_ast_context(node_info)

        # 构建增强 prompt
        prompt = self._build_llm_prompt(name, code_context, ast_context)

        try:
            response = self.llm.invoke(prompt)
            label = response.content.strip()

            if label in self.VALID_LABELS:
                return label
            return "General"

        except Exception as e:
            print(f"LLM API 调用失败: {e}")
            return self._fallback_label(name)

    def _fallback_label(self, name: str) -> Optional[str]:
        """
        回退标签方法，当 LLM 不可用时使用。

        参数:
            name (str): 节点名称。

        返回:
            Optional[str]: 语义标签。
        """
        mock_labels = {
            "cv2.VideoCapture": "Camera",
            "model.predict": "Detection",
            "tracker.update": "Tracking",
            "db.save": "Database",
            "cv2.imread": "ImageProcessing",
            "cv2.imwrite": "ImageProcessing",
            "cv2.cvtColor": "ImageProcessing",
            "numpy.array": "DataProcessing",
            "pandas.DataFrame": "DataProcessing",
            # C++ STL 容器
            "std::vector": "DataProcessing",
            "std::map": "DataProcessing",
            "std::unordered_map": "DataProcessing",
            "std::set": "DataProcessing",
            "std::unordered_set": "DataProcessing",
            "std::list": "DataProcessing",
            "std::deque": "DataProcessing",
            "std::queue": "DataProcessing",
            "std::stack": "DataProcessing",
            "std::pair": "DataProcessing",
            "std::tuple": "DataProcessing",
            "std::array": "DataProcessing",
            "std::forward_list": "DataProcessing",
            "std::priority_queue": "DataProcessing",
            # C++ STL 算法
            "std::sort": "Algorithms",
            "std::find": "Algorithms",
            "std::binary_search": "Algorithms",
            "std::accumulate": "Algorithms",
            "std::count": "Algorithms",
            "std::copy": "Algorithms",
            "std::transform": "Algorithms",
            "std::for_each": "Algorithms",
            "std::remove": "Algorithms",
            "std::replace": "Algorithms",
            "std::unique": "Algorithms",
            "std::reverse": "Algorithms",
            "std::merge": "Algorithms",
            "std::lower_bound": "Algorithms",
            "std::upper_bound": "Algorithms",
            # C++ I/O
            "std::cout": "IOOperation",
            "std::cin": "IOOperation",
            "std::cerr": "IOOperation",
            "std::clog": "IOOperation",
            "std::fstream": "FileIO",
            "std::ifstream": "FileIO",
            "std::ofstream": "FileIO",
            "std::stringstream": "DataProcessing",
            # C++ 字符串
            "std::string": "DataProcessing",
            "std::to_string": "DataProcessing",
            "std::stoi": "DataProcessing",
            "std::stod": "DataProcessing",
            # C++ 并发
            "std::thread": "Concurrency",
            "std::mutex": "Concurrency",
            "std::lock_guard": "Concurrency",
            "std::unique_lock": "Concurrency",
            "std::atomic": "Concurrency",
            "std::future": "Concurrency",
            "std::promise": "Concurrency",
            "std::async": "Concurrency",
            # C++ 智能指针
            "std::unique_ptr": "MemoryManagement",
            "std::shared_ptr": "MemoryManagement",
            "std::weak_ptr": "MemoryManagement",
            "std::make_unique": "MemoryManagement",
            "std::make_shared": "MemoryManagement",
        }

        if name in mock_labels:
            return mock_labels[name]

        for pattern, label in mock_labels.items():
            if pattern in name:
                return label

        return "General"


class SemanticLabelingPipeline:
    """
    语义标注管道，使用责任链模式管理多个标注器。
    """

    def __init__(self, labelers: List[BaseLabeler]):
        """
        初始化语义标注管道。

        参数:
            labelers (List[BaseLabeler]): 标注器列表。
        """
        self.labelers = labelers

    def process_nodes(
        self,
        nodes: List[Dict[str, Any]],
        code: str = "",
        language: str = "python",
        full_ast: dict = None
    ) -> List[Dict[str, Any]]:
        """
        批量处理节点列表，为每个节点添加语义标签（增强版：传递上下文给标注器）。

        参数:
            nodes (List[Dict[str, Any]]): 节点字典列表，每个字典应包含 "name" 字段，
                                         以及可选的 code_snippet, line_number, parent_type, node_type 等。
            code (str): 原始代码全文。
            language (str): 编程语言（"python" 或 "cpp"），默认为 "python"。
            full_ast (dict, optional): 完整 AST 字典。

        返回:
            List[Dict[str, Any]]: 带有语义标签的节点字典列表。
        """
        if full_ast is None:
            full_ast = {}

        labeled_nodes = []

        for node in nodes:
            node_name = node.get("name")
            if not node_name:
                continue

            label = None
            for labeler in self.labelers:
                # 传递增强上下文给每个标注器
                label = labeler.label(
                    node_name,
                    code=code,
                    language=language,
                    node_info=node,
                    full_ast=full_ast
                )
                if label:
                    break

            labeled_node = node.copy()
            labeled_node["type"] = label or "Unknown"
            labeled_nodes.append(labeled_node)

        return labeled_nodes


class CodeSummarizer:
    """
    代码总结器，使用 LLM 对代码进行总结。
    """

    def __init__(self):
        """
        初始化代码总结器。
        """
        self.llm = None
        self._init_llm()

    def _init_llm(self):
        try:
            from deerflow.models import create_chat_model
            # 以脚本自身位置为锚点，计算到 config.yaml 的相对路径
            script_dir = os.path.dirname(os.path.abspath(__file__))
            self.llm = create_chat_model(thinking_enabled=False)
        except Exception as e:
            print(f"警告：创建 DeerFlow 模型时出错: {e}")


    def summarize(self, code: str) -> str:
        """
        对代码进行总结。

        参数:
            code (str): 要总结的代码。

        返回:
            str: 代码总结。
        """
        return self._call_llm_api(code)

    def _call_llm_api(self, code: str) -> str:
        """
        调用 LLM API 对代码进行总结。

        参数:
            code (str): 要总结的代码。

        返回:
            str: 代码总结。
        """
        if self.llm is None:
            return self._fallback_summarize(code)

        try:
            # 检测代码语言，用于 LLM prompt 中的代码块标记
            is_cpp = "#include" in code and ("std::" in code or "int main" in code)
            code_marker = "cpp" if is_cpp else "python"
            prompt = f"""你是一个代码分析专家。请用简短的中文（不超过50字）总结以下代码的功能。

代码:
```{code_marker}
{code}
```

请只输出总结内容，不要输出其他内容。"""

            response = self.llm.invoke(prompt)
            summary = response.content.strip()
            return summary

        except Exception as e:
            print(f"LLM API 调用失败: {e}")
            return self._fallback_summarize(code)

    def _fallback_summarize(self, code: str) -> str:
        """
        回退总结方法，当 LLM 不可用时使用。

        参数:
            code (str): 要总结的代码。

        返回:
            str: 代码总结。
        """
        if "cv2" in code or "VideoCapture" in code or "imread" in code:
            return "这是一段包含视频流读取和图像处理的 Python 代码。"
        elif "model.predict" in code or "Detection" in code:
            return "这是一段包含目标检测功能的 Python 代码。"
        elif "tracker" in code:
            return "这是一段包含目标跟踪功能的 Python 代码。"
        elif "db" in code or "Database" in code:
            return "这是一段包含数据库操作的 Python 代码。"
        elif "numpy" in code or "pandas" in code:
            return "这是一段包含数据处理功能的 Python 代码。"
        elif "tensorflow" in code or "pytorch" in code:
            return "这是一段包含机器学习功能的 Python 代码。"
        elif "#include" in code and ("std::" in code or "int main" in code):
            if "std::thread" in code or "std::mutex" in code or "std::future" in code:
                return "这是一段包含 C++ 并发编程的代码。"
            elif "std::vector" in code or "std::map" in code or "std::unordered_map" in code:
                return "这是一段包含 C++ STL 容器使用的代码。"
            elif "std::unique_ptr" in code or "std::shared_ptr" in code:
                return "这是一段包含 C++ 智能指针内存管理的代码。"
            elif "std::cout" in code or "std::cin" in code or "std::ifstream" in code or "std::ofstream" in code:
                return "这是一段包含 C++ I/O 操作的代码。"
            elif "class " in code:
                return "这是一段包含 C++ 面向对象编程的代码。"
            elif "std::sort" in code or "std::find" in code or "std::binary_search" in code:
                return "这是一段包含 C++ STL 算法使用的代码。"
            else:
                return "这是一段 C++ 代码。"
        else:
            return "这是一段代码。"


class DeerFlowOrchestrator:
    """
    DeerFlow 工作流引擎，统一的对外接口类。
    """

    def __init__(self, semantic_pipeline: SemanticLabelingPipeline, code_summarizer: CodeSummarizer):
        """
        初始化 DeerFlow 工作流引擎。

        参数:
            semantic_pipeline (SemanticLabelingPipeline): 语义标注管道。
            code_summarizer (CodeSummarizer): 代码总结器。
        """
        self.semantic_pipeline = semantic_pipeline
        self.code_summarizer = code_summarizer

    def process_raw_code(self, raw_code: str, language: str = "python") -> Dict[str, Any]:
        """
        处理原始代码，执行完整的语义标注流程。

        参数:
            raw_code (str): 原始代码。
            language (str): 编程语言（"python" 或 "cpp"），默认为 "python"。

        返回:
            Dict[str, Any]: 处理结果，包含状态、管道步骤和标注后的节点。
        """
        try:
            # 步骤 1: 代码切分 (模拟)
            split_result = self._mock_code_split(raw_code)

            # 步骤 2: AST 分析，提取核心变量和函数调用图
            ast_result = self._real_ast_extract(raw_code, language=language)

            # 步骤 3: 语义标注
            nodes = self._extract_nodes_from_ast_result(ast_result)
            labeled_nodes = self.semantic_pipeline.process_nodes(
                nodes,
                code=raw_code,
                language=language,
                full_ast=ast_result.get("ast", {})
            )

            # 步骤 4: 代码总结
            code_summary = self.code_summarizer.summarize(raw_code)

            result = {
                "status": "success",
                "pipeline_steps": ["split", "ast_extract", "semantic_label", "code_summary"],
                "labeled_nodes": labeled_nodes,
                "code_summary": code_summary
            }

            return result
        except Exception as e:
            return {
                "status": "error",
                "message": str(e),
                "pipeline_steps": [],
                "labeled_nodes": [],
                "code_summary": ""
            }

    def _mock_code_split(self, code: str) -> Dict[str, Any]:
        return {
            "chunks": [{
                "content": code,
                "metadata": {"chunk_index": 0}
            }]
        }

    def _real_ast_extract(self, code: str, language: str = "python") -> Dict[str, Any]:
        try:
            _script_dir = os.path.dirname(os.path.abspath(__file__))
            _ast_path = os.path.abspath(os.path.join(_script_dir, "..", "code-to-ast-new", "scripts"))
            if _ast_path not in sys.path:
                sys.path.insert(0, _ast_path)
            from convert import parse_python, parse_cpp

            if language == "cpp":
                ast_dict = parse_cpp(code)
            else:
                ast_dict = parse_python(code)

            nodes = self._extract_nodes_from_ast(ast_dict, code=code, language=language)

            return {
                "ast": ast_dict,
                "nodes": nodes
            }
        except ImportError:
            return self._mock_ast_extract(code)
        except Exception as e:
            return self._mock_ast_extract(code)

    def _mock_ast_extract(self, code: str) -> Dict[str, Any]:
        mock_nodes = []
        if "cv2.VideoCapture" in code:
            mock_nodes.append({"name": "cv2.VideoCapture"})
        if "model.predict" in code:
            mock_nodes.append({"name": "model.predict"})
        if "tracker.update" in code:
            mock_nodes.append({"name": "tracker.update"})
        if "db.save" in code:
            mock_nodes.append({"name": "db.save"})
        if "cv2.imread" in code:
            mock_nodes.append({"name": "cv2.imread"})
        if "cv2.imwrite" in code:
            mock_nodes.append({"name": "cv2.imwrite"})
        return {
            "ast": {},
            "nodes": mock_nodes
        }

    def _extract_nodes_from_ast(self, ast_dict: Dict[str, Any], code: str = "", language: str = "python") -> List[Dict[str, Any]]:
        if language == "cpp":
            return self._extract_nodes_from_cpp_ast(ast_dict, code=code)
        return self._extract_nodes_from_python_ast(ast_dict, code=code)

    def _extract_nodes_from_python_ast(self, ast_dict: Dict[str, Any], code: str = "") -> List[Dict[str, Any]]:
        nodes = []
        code_lines = code.split("\n") if code else []

        def _get_line_code(line_num: int) -> str:
            if 0 <= line_num < len(code_lines):
                return code_lines[line_num].strip()
            return ""

        def traverse(node, parent_type: str = "", parent_name: str = "", depth: int = 0):
            if depth > 20:
                return
            if isinstance(node, dict):
                node_type = node.get("type", "")
                if node_type == "Call":
                    func = node.get("func")
                    if func:
                        func_name = ""
                        if func.get("type") == "Name":
                            func_name = func.get("id", "")
                        elif func.get("type") == "Attribute":
                            value = func.get("value", {})
                            attr = func.get("attr", "")
                            value_name = ""
                            if value.get("type") == "Name":
                                value_name = value.get("id", "")
                            elif value.get("type") == "Attribute":
                                value_name = self._extract_node_name(value)
                            if value_name:
                                func_name = f"{value_name}.{attr}"
                            else:
                                func_name = attr

                        if func_name:
                            line_num = func.get("lineno", node.get("lineno", 0))
                            if isinstance(line_num, int) and line_num > 0:
                                code_snippet = _get_line_code(line_num - 1)
                            else:
                                code_snippet = ""

                            node_info = {
                                "name": func_name,
                                "code_snippet": code_snippet,
                                "node_type": "Call",
                                "parent_type": parent_type,
                                "parent_name": parent_name,
                                "line_number": line_num if isinstance(line_num, int) else 0,
                            }
                            nodes.append(node_info)

                for key, value in node.items():
                    if key == "type":
                        continue
                    current_parent_type = node_type if node_type else parent_type
                    current_parent_name = node.get("id", node.get("attr", parent_name))
                    if isinstance(value, (dict, list)):
                        traverse(value, current_parent_type, str(current_parent_name), depth + 1)
            elif isinstance(node, list):
                for item in node:
                    traverse(item, parent_type, parent_name, depth + 1)

        traverse(ast_dict)
        return nodes

    def _extract_node_name(self, node: Dict[str, Any]) -> str:
        if node.get("type") == "Name":
            return node.get("id", "")
        elif node.get("type") == "Attribute":
            value = node.get("value")
            attr = node.get("attr")
            if value and attr:
                value_name = self._extract_node_name(value)
                if value_name:
                    return f"{value_name}.{attr}"
                return attr
        return ""

    def _extract_nodes_from_cpp_ast(self, ast_dict: Dict[str, Any], code: str = "") -> List[Dict[str, Any]]:
        nodes = []
        seen = set()
        code_lines = code.split("\n") if code else []
        tree = ast_dict.get("full_tree", ast_dict)

        def _get_line_code(line_index: int) -> str:
            if 0 <= line_index < len(code_lines):
                return code_lines[line_index].strip()
            return ""

        def _extract_call_name(node: Dict[str, Any]) -> Optional[str]:
            children = node.get("children", [])
            for child in children:
                child_type = child.get("type", "")
                if child_type == "identifier":
                    return child.get("text", "")
                elif child_type == "field_expression" or child_type == "pointer_expression":
                    grand_children = child.get("children", [])
                    parts = []
                    for gc in grand_children:
                        if gc.get("type") == "identifier":
                            parts.append(gc.get("text", ""))
                        elif gc.get("type") in ("field_identifier",):
                            parts.append(gc.get("text", ""))
                        elif gc.get("type") == "->" or gc.get("type") == "." or gc.get("type") == "*" or gc.get("type") == "&":
                            continue
                        elif gc.get("type") == "argument_list":
                            continue
                        else:
                            text = _extract_call_name(gc)
                            if text:
                                parts.append(text)
                    if parts:
                        return ".".join(parts)
                elif child_type == "qualified_identifier":
                    return child.get("text", "")
                elif child_type == "template_function":
                    func_children = child.get("children", [])
                    for fc in func_children:
                        if fc.get("type") == "identifier":
                            return fc.get("text", "")
                    return child.get("text", "")
                elif child_type == "argument_list":
                    continue
            return None

        def traverse(node, parent_type: str = "", parent_name: str = "", depth: int = 0):
            if depth > 30:
                return
            if not isinstance(node, dict):
                return
            node_type = node.get("type", "")
            if node_type == "call_expression":
                name = _extract_call_name(node)
                if name and name not in seen:
                    seen.add(name)
                    start_position = node.get("start_position", {})
                    line_num = start_position.get("row", -1) if isinstance(start_position, dict) else -1
                    code_snippet = ""
                    if line_num >= 0:
                        code_snippet = _get_line_code(line_num)
                    node_info = {
                        "name": name,
                        "code_snippet": code_snippet,
                        "node_type": "call_expression",
                        "parent_type": parent_type,
                        "parent_name": parent_name,
                        "line_number": line_num + 1 if line_num >= 0 else 0,
                    }
                    nodes.append(node_info)
            children = node.get("children", [])
            if isinstance(children, list):
                for child in children:
                    traverse(child, node_type, child.get("text", ""), depth + 1)

        traverse(tree)
        return nodes

    def _extract_nodes_from_ast_result(self, ast_result: Dict[str, Any]) -> List[Dict[str, Any]]:
        return ast_result.get("nodes", [])


class OntologyManager:
    def __init__(self, rules_file: str = None):
        self.rules_file = rules_file
        self.rules = {}
        if rules_file and os.path.exists(rules_file):
            self.load_rules()

    def load_rules(self) -> None:
        try:
            with open(self.rules_file, "r", encoding="utf-8") as f:
                self.rules = json.load(f)
        except Exception as e:
            print(f"加载规则文件失败: {e}")

    def save_rules(self) -> None:
        if self.rules_file:
            try:
                os.makedirs(os.path.dirname(self.rules_file), exist_ok=True)
                with open(self.rules_file, "w", encoding="utf-8") as f:
                    json.dump(self.rules, f, ensure_ascii=False, indent=2)
            except Exception as e:
                print(f"保存规则文件失败: {e}")

    def add_rule(self, pattern: str, label: str) -> None:
        self.rules[pattern] = label

    def remove_rule(self, pattern: str) -> None:
        if pattern in self.rules:
            del self.rules[pattern]

    def get_rules(self) -> Dict[str, str]:
        return self.rules

    def update_rules(self, new_rules: Dict[str, str]) -> None:
        self.rules.update(new_rules)
