"""Build business-level DAGs from structural and semantic code analysis output."""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from typing import Any, Literal, TypedDict

import networkx as nx

logger = logging.getLogger(__name__)

UnknownPolicy = Literal["drop", "unknown"]


class PipelineJson(TypedDict):
    """Exported pipeline JSON consumed by downstream DeerFlow components."""

    pipeline: dict[str, list[str] | list[list[str]]]


class PipelineGraphBuilder:
    """Fuse call graph, dataflow, and semantic labels into a business-level DAG."""

    def __init__(
        self,
        *,
        unknown_policy: UnknownPolicy = "drop",
        unknown_type: str = "Unknown",
        break_cycles: bool = True,
    ) -> None:
        if unknown_policy not in {"drop", "unknown"}:
            raise ValueError("unknown_policy must be either 'drop' or 'unknown'")
        self.unknown_policy = unknown_policy
        self.unknown_type = unknown_type
        self.break_cycles = break_cycles

    def build_dag(
        self,
        calls: list[Any],
        dataflow: list[Any],
        labeled_nodes: list[Mapping[str, Any]],
    ) -> PipelineJson:
        """Build and export a DAG in ``{"pipeline": {"nodes": ..., "edges": ...}}`` format.

        ``calls`` supports dict edges such as ``{"caller": "...", "callee": "..."}`` and
        ``dataflow`` supports two-item list/tuple edges such as ``["frame", "model.predict"]``.
        """

        type_lookup = self._build_type_lookup(labeled_nodes)
        graph = nx.DiGraph()

        # 第一阶段：严格按照“代码名称 -> 语义 Type”的查找表转换边。
        # networkx.DiGraph 的边集合天然是去重的：重复调用 add_edge("Camera", "Detection")
        # 不会产生两条重复边，只会保留一条有向边，因此这里不需要额外维护 set。
        for source_name, target_name in self._iter_edges(calls, preferred_keys=("caller", "callee")):
            self._add_semantic_edge(graph, source_name, target_name, type_lookup)

        for source_name, target_name in self._iter_edges(dataflow, preferred_keys=("source", "target")):
            self._add_semantic_edge(graph, source_name, target_name, type_lookup)

        # 第二阶段：补齐常见流水线抽取输出里的“变量中转”缺口。
        # 例如 dataflow 为 ["frame", "model.predict"]，而 frame 没有语义标签时，
        # 仅靠严格映射会丢失 Camera -> Detection。这里使用调用/数据流出现顺序抽取已标注操作，
        # 生成相邻业务组件链路，既兼容样例，也避免把 Unknown 变量暴露为业务节点。
        if self.unknown_policy == "drop":
            for source_type, target_type in self._infer_ordered_semantic_edges(calls, dataflow, type_lookup):
                graph.add_edge(source_type, target_type)

        # 自环表示同一个业务组件内部的调用或数据移动，不属于业务级 DAG 的跨组件依赖。
        # networkx.selfloop_edges 可以稳定找出所有 source == target 的边，再统一删除。
        graph.remove_edges_from(nx.selfloop_edges(graph))

        if self.break_cycles and not nx.is_directed_acyclic_graph(graph):
            self._break_cycles(graph)

        return self._export_graph(graph)

    def _build_type_lookup(self, labeled_nodes: list[Mapping[str, Any]]) -> dict[str, str]:
        lookup: dict[str, str] = {}
        for node in labeled_nodes:
            name = node.get("name")
            semantic_type = node.get("type")
            if isinstance(name, str) and name and isinstance(semantic_type, str) and semantic_type:
                lookup[name] = semantic_type
        return lookup

    def _iter_edges(
        self,
        raw_edges: list[Any],
        *,
        preferred_keys: tuple[str, str],
    ) -> list[tuple[str, str]]:
        edges: list[tuple[str, str]] = []
        source_key, target_key = preferred_keys
        fallback_source_keys = (source_key, "caller", "from", "src", "source")
        fallback_target_keys = (target_key, "callee", "to", "dst", "target")

        for edge in raw_edges:
            source: Any = None
            target: Any = None
            if isinstance(edge, Mapping):
                source = self._first_present(edge, fallback_source_keys)
                target = self._first_present(edge, fallback_target_keys)
            elif self._is_pair(edge):
                source, target = edge[0], edge[1]

            if isinstance(source, str) and isinstance(target, str) and source and target:
                edges.append((source, target))
        return edges

    def _first_present(self, edge: Mapping[str, Any], keys: Sequence[str]) -> Any:
        for key in keys:
            if key in edge:
                return edge[key]
        return None

    def _is_pair(self, value: Any) -> bool:
        return isinstance(value, Sequence) and not isinstance(value, str | bytes) and len(value) >= 2

    def _add_semantic_edge(
        self,
        graph: nx.DiGraph,
        source_name: str,
        target_name: str,
        type_lookup: Mapping[str, str],
    ) -> None:
        source_type = type_lookup.get(source_name)
        target_type = type_lookup.get(target_name)

        if source_type is None or target_type is None:
            if self.unknown_policy == "drop":
                return
            source_type = source_type or self.unknown_type
            target_type = target_type or self.unknown_type

        graph.add_edge(source_type, target_type)

    def _infer_ordered_semantic_edges(
        self,
        calls: list[Any],
        dataflow: list[Any],
        type_lookup: Mapping[str, str],
    ) -> list[tuple[str, str]]:
        ordered_types: list[str] = []
        for source_name, target_name in [
            *self._iter_edges(calls, preferred_keys=("caller", "callee")),
            *self._iter_edges(dataflow, preferred_keys=("source", "target")),
        ]:
            for code_name in (source_name, target_name):
                semantic_type = type_lookup.get(code_name)
                if semantic_type is not None and (not ordered_types or ordered_types[-1] != semantic_type):
                    ordered_types.append(semantic_type)

        return list(zip(ordered_types, ordered_types[1:], strict=False))

    def _break_cycles(self, graph: nx.DiGraph) -> None:
        while not nx.is_directed_acyclic_graph(graph):
            cycle = nx.find_cycle(graph, orientation="original")
            source, target = cycle[-1][0], cycle[-1][1]
            logger.warning("Pipeline graph contains a cycle; removing edge %s -> %s to enforce DAG.", source, target)
            graph.remove_edge(source, target)

    def _export_graph(self, graph: nx.DiGraph) -> PipelineJson:
        if nx.is_directed_acyclic_graph(graph):
            nodes = list(nx.topological_sort(graph))
        else:
            nodes = list(graph.nodes)
        edges = [[str(source), str(target)] for source, target in graph.edges]
        return {"pipeline": {"nodes": [str(node) for node in nodes], "edges": edges}}
