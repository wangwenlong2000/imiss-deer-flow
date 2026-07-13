"""Local reasoning for business pipeline DAG diagnostics."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, Final, TypedDict

import networkx as nx


class PipelineDiagnosis(TypedDict):
    """Structured diagnosis returned by the pipeline reasoner."""

    pipeline_type: str
    missing: list[str]
    anomalies: list[str]


class CloudLLMReasoner:
    """Diagnose a business pipeline DAG with DeerFlow's built-in reasoning rules.

    The class keeps the original CloudLLMReasoner name for compatibility with
    Skill 6 callers, but it no longer requires an OpenAI-compatible API key.
    """

    SYSTEM_PROMPT: Final[str] = (
        "你是智慧城市代码流分析专家。你的任务是分析业务组件 DAG，推断业务分类 "
        "pipeline_type，找出缺失的关键模块 missing，并诊断逻辑断层、反向依赖、"
        "不合理数据流等异常 anomalies。你是一个 API 节点，严禁输出任何解释性文字，"
        "必须返回一个纯粹的 JSON 对象，不包含 markdown 格式符。JSON 对象必须严格包含 "
        "pipeline_type, missing, anomalies 三个字段，其中 missing 和 anomalies 必须是字符串数组。"
    )

    _STAGE_ORDER: Final[dict[str, int]] = {
        "source": 0,
        "decode": 1,
        "preprocess": 2,
        "detection": 3,
        "tracking": 4,
        "recognition": 5,
        "analysis": 6,
        "alert": 7,
        "storage": 8,
        "output": 9,
    }

    _ALIASES: Final[dict[str, tuple[str, ...]]] = {
        "source": ("camera", "video", "stream", "capture", "sensor", "source", "摄像", "相机", "视频", "采集"),
        "decode": ("decode", "decoder", "demux", "解码", "抽帧"),
        "preprocess": ("preprocess", "resize", "normalize", "transform", "filter", "预处理", "归一化"),
        "detection": ("detect", "detection", "detector", "yolo", "识别", "检测", "目标检测"),
        "tracking": ("track", "tracking", "tracker", "跟踪"),
        "recognition": ("recognition", "recognize", "plate", "ocr", "reid", "人脸", "车牌", "识别"),
        "analysis": ("analysis", "analytics", "reason", "rule", "event", "分析", "研判", "事件"),
        "alert": ("alert", "alarm", "notify", "warning", "告警", "报警", "通知"),
        "storage": ("storage", "store", "save", "database", "db", "s3", "oss", "存储", "入库", "数据库"),
        "output": ("output", "render", "dashboard", "report", "visual", "输出", "展示", "报表"),
    }

    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str | None = None,
        model: str = "deerflow-local-reasoner",
        timeout: float | None = None,
        client: Any | None = None,
    ) -> None:
        """Initialize the local reasoner.

        Deprecated cloud-client parameters are accepted and ignored so existing
        callers can switch to the local reasoning path without code changes.
        """

        self.model = model
        self.client = client
        self.api_key = api_key
        self.base_url = base_url
        self.timeout = timeout

    def _serialize_for_llm(self, graph: nx.DiGraph) -> str:
        """Serialize only business node types and topological edges."""

        ordered_nodes = self._ordered_nodes(graph)
        node_types = [self._node_type(graph, node) for node in ordered_nodes]
        edge_paths = [
            f"{self._node_type(graph, source)} -> {self._node_type(graph, target)}"
            for source, target in graph.edges
        ]

        return "\n".join(
            [
                "NodeTypes:",
                *node_types,
                "Edges:",
                *edge_paths,
            ]
        )

    def reason(self, graph: nx.DiGraph) -> PipelineDiagnosis:
        """Analyze the DAG locally and return the Skill 6 diagnosis format."""

        node_types = [self._node_type(graph, node) for node in self._ordered_nodes(graph)]
        stage_by_node = {node: self._stage_for_type(self._node_type(graph, node)) for node in graph.nodes}
        present_stages = {stage for stage in stage_by_node.values() if stage is not None}

        missing = self._infer_missing_modules(present_stages)
        anomalies = self._infer_anomalies(graph, stage_by_node, present_stages)

        return {
            "pipeline_type": self._infer_pipeline_type(node_types, present_stages),
            "missing": missing,
            "anomalies": anomalies,
        }

    def _ordered_nodes(self, graph: nx.DiGraph) -> list[Any]:
        if nx.is_directed_acyclic_graph(graph):
            return list(nx.topological_sort(graph))
        return list(graph.nodes)

    def _node_type(self, graph: nx.DiGraph, node: Any) -> str:
        node_data = graph.nodes[node]
        semantic_type = node_data.get("type") if isinstance(node_data, Mapping) else None
        if isinstance(semantic_type, str) and semantic_type:
            return semantic_type
        return str(node)

    def _stage_for_type(self, node_type: str) -> str | None:
        normalized = node_type.casefold()
        for stage, aliases in self._ALIASES.items():
            if any(alias.casefold() in normalized for alias in aliases):
                return stage
        return None

    def _infer_pipeline_type(self, node_types: Iterable[str], present_stages: set[str]) -> str:
        joined = " ".join(node_types).casefold()

        if any(keyword in joined for keyword in ("traffic", "vehicle", "plate", "车流", "车辆", "车牌", "交通")):
            return "交通监控与存储" if "storage" in present_stages else "交通监控"
        if any(keyword in joined for keyword in ("face", "person", "pedestrian", "人脸", "行人", "人员")):
            return "安防人员识别与告警" if "alert" in present_stages else "安防人员识别"
        if {"source", "detection", "alert"}.issubset(present_stages):
            return "视频监控与告警"
        if {"source", "detection", "storage"}.issubset(present_stages):
            return "视频监控与存储"
        if {"source", "detection"}.issubset(present_stages):
            return "视频目标检测流水线"
        if "storage" in present_stages:
            return "数据采集与存储"
        return "unknown"

    def _infer_missing_modules(self, present_stages: set[str]) -> list[str]:
        missing: list[str] = []

        if "source" in present_stages and "detection" in present_stages:
            if "decode" not in present_stages:
                missing.append("Decode")
            if "preprocess" not in present_stages:
                missing.append("Preprocess")

        if "detection" in present_stages and "tracking" not in present_stages:
            missing.append("Tracking")

        if ({"source", "detection"} & present_stages) and "storage" not in present_stages:
            missing.append("Storage")

        return missing

    def _infer_anomalies(
        self,
        graph: nx.DiGraph,
        stage_by_node: Mapping[Any, str | None],
        present_stages: set[str],
    ) -> list[str]:
        anomalies: list[str] = []

        if graph.number_of_nodes() == 0:
            return ["空流水线: 未发现可诊断的业务节点"]

        if not nx.is_directed_acyclic_graph(graph):
            cycle_descriptions = [" -> ".join(str(node) for node in cycle) for cycle in nx.simple_cycles(graph)]
            anomalies.extend(f"循环依赖: {cycle}" for cycle in cycle_descriptions[:3])

        for source, target in graph.edges:
            source_stage = stage_by_node.get(source)
            target_stage = stage_by_node.get(target)
            if source_stage is None or target_stage is None:
                continue
            if self._STAGE_ORDER[source_stage] > self._STAGE_ORDER[target_stage]:
                anomalies.append(
                    f"反向依赖: {self._node_type(graph, source)} 不应流向 {self._node_type(graph, target)}"
                )

        source_nodes = [node for node, stage in stage_by_node.items() if stage == "source"]
        detection_nodes = [node for node, stage in stage_by_node.items() if stage == "detection"]
        storage_nodes = [node for node, stage in stage_by_node.items() if stage == "storage"]

        if source_nodes and detection_nodes and not self._has_any_path(graph, source_nodes, detection_nodes):
            anomalies.append("逻辑断层: Camera 与 Detection 之间没有数据流转路径")

        if "source" in present_stages and "detection" in present_stages and "decode" not in present_stages:
            anomalies.append("逻辑断层: Camera 与 Detection 之间缺少 Decode 解码模块")

        if detection_nodes and storage_nodes and not self._has_any_path(graph, detection_nodes, storage_nodes):
            anomalies.append("逻辑断层: Detection 与 Storage 之间没有结果落库路径")

        return self._deduplicate(anomalies)

    def _has_any_path(self, graph: nx.DiGraph, sources: Iterable[Any], targets: Iterable[Any]) -> bool:
        target_list = list(targets)
        for source in sources:
            for target in target_list:
                if source == target or nx.has_path(graph, source, target):
                    return True
        return False

    def _deduplicate(self, values: Iterable[str]) -> list[str]:
        seen: set[str] = set()
        result: list[str] = []
        for value in values:
            if value not in seen:
                seen.add(value)
                result.append(value)
        return result
