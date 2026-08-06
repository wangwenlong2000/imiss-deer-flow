# -*- coding: utf-8 -*-

from dataclasses import dataclass, asdict
from typing import Dict, List

from yigraph_template_registry import QUERY_TEMPLATES, QUERY_MODIFIERS


@dataclass(frozen=True)
class QueryCapability:
    name: str
    description: str
    method: str
    required_params: List[str]
    optional_params: List[str]
    default_explanation: str


DEFAULT_EXPLANATIONS: Dict[str, str] = {
    "node_lookup": "用于查单个号码/设备/节点的基础画像与属性信息。",
    "relationship_filter": "用于按边属性过滤关系，例如金额、时间、时长、方向等。",
    "aggregation_query": "用于做分组统计、TopK 排名、数量/金额聚合分析。",
    "neighbor_query": "用于看某个节点周围 1~k 跳邻居，适合局部关系探索。",
    "path_query": "用于分析两个节点怎么连起来、是否存在路径、最短路径是什么。",
    "common_neighbor": "用于找两个节点的共同联系人、共同对端、共同邻居。",
    "subgraph": "用于围绕中心节点或边条件抽取局部子图。",
    "subgraph_by_nodes": "用于指定一组节点，抽取它们之间的内部关系子图。"
}

SKILL_OPERATOR_ALIGNMENT: Dict[str, List[str]] = {
    "association-path-analysis": [
        "path_query",
        "common_neighbor",
        "relationship_filter"
    ],
    "subgraph-extraction-analysis": [
        "subgraph",
        "neighbor_query"
    ],
    "overlap-analysis": [
        "common_neighbor",
        "subgraph_by_nodes"
    ],
    "graph-operator": [
        "node_lookup",
        "neighbor_query",
        "path_query",
        "common_neighbor",
        "subgraph",
        "subgraph_by_nodes",
        "aggregation_query",
        "relationship_filter"
    ]
}


def build_capability_catalog() -> Dict[str, QueryCapability]:
    catalog: Dict[str, QueryCapability] = {}
    for name, meta in QUERY_TEMPLATES.items():
        catalog[name] = QueryCapability(
            name=name,
            description=meta.get("description", ""),
            method=meta.get("method", ""),
            required_params=list(meta.get("required_params", [])),
            optional_params=list(meta.get("optional_params", [])),
            default_explanation=DEFAULT_EXPLANATIONS.get(name, "")
        )
    return catalog


def get_query_capability(name: str) -> Dict:
    catalog = build_capability_catalog()
    if name not in catalog:
        return {}
    return asdict(catalog[name])


def get_skill_operator_alignment(skill_name: str) -> List[str]:
    return SKILL_OPERATOR_ALIGNMENT.get(skill_name, [])


def format_capabilities_markdown() -> str:
    catalog = build_capability_catalog()
    lines: List[str] = []
    lines.append("# YiGraph 风格 Query Capabilities")
    lines.append("")
    for name, cap in catalog.items():
        lines.append(f"## {name}")
        lines.append(f"- description: {cap.description}")
        lines.append(f"- method: {cap.method}")
        lines.append(f"- required_params: {cap.required_params}")
        lines.append(f"- optional_params: {cap.optional_params}")
        lines.append(f"- default_explanation: {cap.default_explanation}")
        lines.append("")
    lines.append("## 通用修饰符")
    for name, meta in QUERY_MODIFIERS.items():
        lines.append(f"- {name}: {meta}")
    return "\n".join(lines)


if __name__ == "__main__":
    print(format_capabilities_markdown())
