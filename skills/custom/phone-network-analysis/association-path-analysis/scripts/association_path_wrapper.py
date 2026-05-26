#!/usr/bin/env python3
import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

CURRENT_DIR = Path(__file__).resolve().parent
SKILL_DIR = CURRENT_DIR.parent
PHONE_ANALYSIS_DIR = SKILL_DIR.parent
COMMON_DIR = PHONE_ANALYSIS_DIR / "common"
if str(COMMON_DIR) not in sys.path:
    sys.path.insert(0, str(COMMON_DIR))

from composite_path_engine import CompositePathEngine  # type: ignore
from yigraph_adapter import run_graph_path_query  # type: ignore
try:
    from yigraph_query_capabilities import get_skill_operator_alignment  # type: ignore
except Exception:
    get_skill_operator_alignment = None

try:
    import networkx as nx  # type: ignore
except Exception:
    nx = None

DEFAULT_CALL_GRAPH_PATH = "/mnt/datasets/phone-network/processed/unified/call_edges.csv"
DEFAULT_DEVICE_GRAPH_PATH = "/mnt/datasets/phone-network/processed/graph_views/unified/edges_phone_imei.parquet"
DEFAULT_SOURCE_COL = "src_user_id"
DEFAULT_TARGET_COL = "dst_counterparty_id"
DEFAULT_DEVICE_SOURCE_COL = "user_id"
DEFAULT_DEVICE_TARGET_COL = "imei"


def resolve_data_path(raw_path: str) -> str:
    candidates = [Path(raw_path)]
    text = str(raw_path)
    mappings = [
        ("/mnt/datasets/", "/workspace/imiss-deer-flow-main/datasets/"),
        ("/workspace/imiss-deer-flow-main/datasets/", "/mnt/datasets/"),
        ("/mnt/user-data/workspace/imiss-deer-flow-main/datasets/", "/mnt/datasets/"),
        ("/mnt/user-data/workspace/imiss-deer-flow-main/", "/workspace/imiss-deer-flow-main/"),
    ]
    for src, dst in mappings:
        if text.startswith(src):
            candidates.append(Path(text.replace(src, dst, 1)))
    for p in candidates:
        if p.exists():
            return str(p)
    return str(candidates[0])


def ensure_output_dir() -> Path:
    candidates = [
        Path("/mnt/user-data/outputs"),
        Path("/workspace/imiss-deer-flow-main/outputs"),
        Path.cwd() / "outputs",
    ]
    for p in candidates:
        try:
            p.mkdir(parents=True, exist_ok=True)
            return p
        except Exception:
            continue
    fallback = Path("/tmp/association-path-analysis-outputs")
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback


def short_id(value: str, head: int = 12) -> str:
    if not value:
        return ""
    return value if len(value) <= head else value[:head] + "..."


def short_id_fixed(value: str, head: int = 8) -> str:
    if not value:
        return "unknown"
    return value[:head]


def sanitize_filename(text: str) -> str:
    text = re.sub(r"[^0-9A-Za-z_.\-\u4e00-\u9fff]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "report"


def unique_keep_order(items: List[str]) -> List[str]:
    seen = set()
    out = []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


def relation_zh(rel: str) -> str:
    mapping = {
        "call": "通话",
        "shared_device": "共享设备",
        "common_counterparty": "共同对端",
    }
    return mapping.get(rel, rel)


def extract_step_evidence_preview(
    step: Dict[str, Any],
    pair_relation_signals: Dict[str, Any],
    phone_a: str,
    phone_b: str,
    preview_limit: int = 10,
) -> List[str]:
    relation = step.get("relation")
    frm = step.get("from")
    to = step.get("to")
    preview: List[str] = []

    if relation == "common_counterparty":
        if {frm, to} == {phone_a, phone_b}:
            preview = list(pair_relation_signals.get("common_counterparties_preview", []))
        elif step.get("evidence"):
            preview = [step["evidence"]]
    elif relation == "shared_device":
        if {frm, to} == {phone_a, phone_b}:
            preview = list(pair_relation_signals.get("shared_devices_preview", []))
        elif step.get("evidence"):
            preview = [step["evidence"]]
    elif relation == "call":
        if step.get("evidence"):
            preview = [step["evidence"]]

    return unique_keep_order(preview)[:preview_limit]


def normalize_candidate_paths(comp: Dict[str, Any]) -> List[Dict[str, Any]]:
    candidates = list(comp.get("candidate_paths", []))
    if candidates:
        return candidates
    if comp.get("path_found"):
        return [
            {
                "path_nodes": list(comp.get("path_nodes", [])),
                "path_steps": list(comp.get("path_steps", [])),
                "path_length": comp.get("path_length"),
                "relation_sequence": list(comp.get("relation_sequence", [])),
                "score": comp.get("score", 0.0),
            }
        ]
    return []


def enrich_candidate_paths(
    candidates: List[Dict[str, Any]],
    pair_relation_signals: Dict[str, Any],
    phone_a: str,
    phone_b: str,
) -> List[Dict[str, Any]]:
    enriched: List[Dict[str, Any]] = []
    for idx, cand in enumerate(candidates, start=1):
        item = dict(cand)
        item["candidate_rank"] = idx
        item["path_nodes"] = list(item.get("path_nodes", []))
        item["path_steps"] = list(item.get("path_steps", []))
        item["relation_sequence"] = list(item.get("relation_sequence", []))
        item["path_length"] = item.get("path_length", max(0, len(item["path_nodes"]) - 1))
        item["explicit_middle_nodes"] = item["path_nodes"][1:-1]

        new_steps = []
        for step in item["path_steps"]:
            step2 = dict(step)
            ev_preview = extract_step_evidence_preview(step2, pair_relation_signals, phone_a, phone_b, preview_limit=10)
            step2["relation_zh"] = relation_zh(step2.get("relation", ""))
            step2["evidence_nodes_preview"] = ev_preview
            if ev_preview and not step2.get("evidence_count"):
                step2["evidence_count"] = len(ev_preview)
            new_steps.append(step2)
        item["path_steps"] = new_steps
        enriched.append(item)
    return enriched


def build_bridge_node_ranking(candidate_paths: List[Dict[str, Any]], top_n: int = 10) -> List[Dict[str, Any]]:
    if not candidate_paths:
        return []

    frequency = Counter()
    role_map: Dict[str, set] = defaultdict(set)
    graph = nx.Graph() if nx is not None else None

    for cand in candidate_paths:
        nodes = list(cand.get("path_nodes", []))
        steps = list(cand.get("path_steps", []))

        for node in nodes[1:-1]:
            frequency[node] += 1
            role_map[node].add("explicit_path_node")

        if graph is not None:
            for i in range(len(nodes) - 1):
                graph.add_edge(nodes[i], nodes[i + 1])

        for step in steps:
            frm = step.get("from")
            to = step.get("to")
            evidence_nodes = list(step.get("evidence_nodes_preview", []))
            rel = step.get("relation", "")
            for ev in evidence_nodes:
                frequency[ev] += 1
                role_map[ev].add(f"evidence:{rel}")
                if graph is not None and frm and to:
                    graph.add_edge(frm, ev)
                    graph.add_edge(ev, to)

    if not frequency:
        return []

    betweenness: Dict[str, float] = {}
    degree_map: Dict[str, int] = {}
    if graph is not None and graph.number_of_nodes() > 0:
        try:
            betweenness = nx.betweenness_centrality(graph)  # type: ignore
        except Exception:
            betweenness = {}
        degree_map = dict(graph.degree())

    ranking = []
    for node, freq in frequency.items():
        degree = degree_map.get(node, 0)
        btw = float(betweenness.get(node, 0.0))
        score = round(freq * 10 + degree + btw * 5, 6)
        ranking.append(
            {
                "node": node,
                "node_preview": short_id(node),
                "path_frequency": freq,
                "local_degree": degree,
                "local_betweenness_centrality": btw,
                "roles": sorted(role_map.get(node, set())),
                "score": score,
            }
        )

    ranking.sort(
        key=lambda x: (
            x["score"],
            x["path_frequency"],
            x["local_betweenness_centrality"],
            x["local_degree"],
        ),
        reverse=True,
    )
    return ranking[:top_n]


def summarize_candidate_paths(candidate_paths: List[Dict[str, Any]], top_k: int = 3) -> List[Dict[str, Any]]:
    out = []
    for cand in candidate_paths[:top_k]:
        evidence_preview: List[str] = []
        for step in cand.get("path_steps", []):
            evidence_preview.extend(step.get("evidence_nodes_preview", []))
        out.append(
            {
                "candidate_rank": cand.get("candidate_rank"),
                "path_length": cand.get("path_length"),
                "relation_sequence": cand.get("relation_sequence", []),
                "score": cand.get("score"),
                "path_nodes_preview": [short_id(x) for x in cand.get("path_nodes", [])],
                "explicit_middle_nodes_preview": [short_id(x) for x in cand.get("explicit_middle_nodes", [])[:5]],
                "evidence_nodes_preview": [short_id(x) for x in unique_keep_order(evidence_preview)[:5]],
            }
        )
    return out


def build_composite_human_summary(best_path: Dict[str, Any]) -> str:
    if not best_path:
        return "在当前复合关系图设置下，没有找到两号码之间的复合路径。"

    path_steps = list(best_path.get("path_steps", []))
    path_length = best_path.get("path_length")

    if len(path_steps) == 1:
        step = path_steps[0]
        relation = step.get("relation", "unknown")
        evidence_count = step.get("evidence_count", 0) or len(step.get("evidence_nodes_preview", []))

        if relation == "common_counterparty":
            return (
                f"在复合关系图中，这两个号码之间存在一条由共同对端派生出的关系边，"
                f"证据是两者共享 {evidence_count} 个共同对端。"
                f"这不代表两者存在直接通话，而是说明它们处于同一个联系圈中。"
            )
        if relation == "shared_device":
            return (
                f"在复合关系图中，这两个号码之间存在一条由共享设备派生出的关系边，"
                f"证据是两者存在 {evidence_count} 个共享设备线索。"
                f"这不代表两者直接通话，但说明它们可能存在设备共用关系。"
            )
        if relation == "call":
            return "在复合关系图中，这两个号码之间存在一条原始通话边，说明两者之间存在直接通联。"
        return (
            f"在复合关系图中，这两个号码之间存在一条派生关系边：{relation}。"
            f"这属于间接关系，不等同于直接通话。"
        )

    if path_length is not None:
        relation_seq = " -> ".join(best_path.get("relation_sequence", []))
        if relation_seq:
            return f"在复合关系图中，找到了一条长度为 {path_length} 的混合关系路径，关系序列为：{relation_seq}。"
        return f"在复合关系图中，找到了一条长度为 {path_length} 的混合关系路径。"

    return "在复合关系图中找到了间接关系路径，但暂未生成完整解释。"


def build_investigation_next_steps(
    pair_relation_signals: Dict[str, Any],
    direct_call_analysis: Optional[Dict[str, Any]],
    composite_analysis: Optional[Dict[str, Any]],
) -> List[str]:
    suggestions: List[str] = []

    if pair_relation_signals.get("shared_device_count", 0) > 0:
        suggestions.append("优先检查共享设备对应的所有关联号码，判断是否存在一机多号或设备复用。")
    if pair_relation_signals.get("common_counterparty_count", 0) > 0:
        suggestions.append("继续核查共同对端号码，识别哪些共同对端更像关键中介节点。")
    if direct_call_analysis and not direct_call_analysis.get("path_found"):
        suggestions.append("由于有向通话图中不可达，建议重点关注间接关系与复合路径证据。")
    if composite_analysis and composite_analysis.get("path_found"):
        bridge_ranking = composite_analysis.get("bridge_node_ranking", [])
        if bridge_ranking:
            suggestions.append("优先查询 Top 桥接节点画像，并围绕这些节点抽取 1-2 跳局部子图继续核查。")
        suggestions.append("对最优复合路径中的证据节点逐个做号码画像、邻居展开和共享设备复查。")
    else:
        suggestions.append("若仍未找到复合路径，可适度放宽 max_hops 或提高 per_relation_limit 后重试。")
    return unique_keep_order(suggestions)


def render_markdown_report(payload: Dict[str, Any]) -> str:
    result = payload.get("result", {})
    pair = result.get("pair_relation_signals", {})
    direct = result.get("direct_call_analysis") or {}
    comp = result.get("composite_analysis") or {}

    lines: List[str] = []
    lines.append("# 两号码联合关联分析报告")
    lines.append("")
    lines.append("## 一、分析对象")
    lines.append("")
    lines.append(f"- 号码A：`{payload.get('input_summary', {}).get('phone_a', '')}`")
    lines.append(f"- 号码B：`{payload.get('input_summary', {}).get('phone_b', '')}`")
    lines.append("")
    lines.append("## 二、直接配对信号")
    lines.append("")
    lines.append(f"- A 是否直接打给 B：**{'是' if pair.get('a_calls_b') else '否'}**")
    lines.append(f"- B 是否直接打给 A：**{'是' if pair.get('b_calls_a') else '否'}**")
    lines.append(f"- 共享设备数：**{pair.get('shared_device_count', 0)}**")
    lines.append(f"- 共同对端数：**{pair.get('common_counterparty_count', 0)}**")
    lines.append("")

    if direct:
        lines.append("## 三、有向通话路径分析")
        lines.append("")
        if direct.get("path_found"):
            lines.append("- 路径存在：**是**")
            lines.append(f"- 路径长度：**{direct.get('path_length')}**")
            lines.append("- 路径节点序列：")
            for idx, node in enumerate(direct.get("path_nodes", []) or direct.get("path", []), start=1):
                lines.append(f"  {idx}. `{node}`")
        else:
            lines.append("- 路径存在：**否**")
            lines.append("- 说明：在有向通话图中未找到 A 到 B 的可达路径。")
        if direct.get("human_summary"):
            lines.append(f"- 中文解释：{direct.get('human_summary')}")
        lines.append("")

    if comp:
        lines.append("## 四、最优复合路径分析")
        lines.append("")
        if comp.get("path_found"):
            best = comp.get("best_path") or comp
            lines.append("- 路径存在：**是**")
            lines.append(f"- 路径长度：**{best.get('path_length')}**")
            lines.append(f"- 关系序列：**{' -> '.join(best.get('relation_sequence', []))}**")
            lines.append(f"- 路径评分：**{best.get('score', '')}**")
            lines.append("")
            lines.append("### 4.1 路径步骤")
            for i, step in enumerate(best.get("path_steps", []), start=1):
                lines.append(
                    f"{i}. `{short_id(step.get('from',''))}` -> `{short_id(step.get('to',''))}`，关系：**{step.get('relation_zh', step.get('relation'))}**"
                )
                ev_preview = step.get("evidence_nodes_preview", [])
                if ev_preview:
                    lines.append(f"   - 证据节点预览：{', '.join([f'`{x}`' for x in ev_preview[:5]])}")
                if step.get("evidence_count") is not None:
                    lines.append(f"   - 证据数量：{step.get('evidence_count')}")
            lines.append("")

            candidate_paths = comp.get("candidate_paths_summary", [])
            if candidate_paths:
                lines.append("### 4.2 Top-K 候选路径")
                for cand in candidate_paths:
                    lines.append(
                        f"- 候选 {cand.get('candidate_rank')}：长度 {cand.get('path_length')}，关系序列 `{' -> '.join(cand.get('relation_sequence', []))}`，评分 {cand.get('score')}"
                    )
                lines.append("")

            bridge_ranking = comp.get("bridge_node_ranking", [])
            if bridge_ranking:
                lines.append("### 4.3 桥接点 / 关键证据节点排序")
                for idx, node in enumerate(bridge_ranking[:10], start=1):
                    lines.append(
                        f"{idx}. `{node.get('node')}` | 频次={node.get('path_frequency')} | 度={node.get('local_degree')} | 介数={round(node.get('local_betweenness_centrality', 0.0), 6)} | 分数={node.get('score')}"
                    )
                lines.append("")
            if comp.get("human_summary"):
                lines.append("### 4.4 中文解释")
                lines.append(f"- {comp.get('human_summary')}")
                lines.append("")
        else:
            lines.append("- 未找到复合关系路径。")
            lines.append("")

    lines.append("## 五、下一步调查建议")
    lines.append("")
    for idx, item in enumerate(result.get("investigation_next_steps", []), start=1):
        lines.append(f"{idx}. {item}")
    lines.append("")

    lines.append("## 六、结论")
    lines.append("")
    if comp and comp.get("human_summary"):
        lines.append(comp.get("human_summary"))
    elif direct and direct.get("human_summary"):
        lines.append(direct.get("human_summary"))
    else:
        lines.append(result.get("pair_signal_summary", "未生成结论。"))
    lines.append("")
    return "\n".join(lines)


def write_report_markdown(payload: Dict[str, Any]) -> str:
    out_dir = ensure_output_dir()
    mode = payload.get("input_summary", {}).get("analysis_mode", "both")
    phone_a = payload.get("input_summary", {}).get("phone_a", "A")
    phone_b = payload.get("input_summary", {}).get("phone_b", "B")
    filename = f"association_path_report_{short_id_fixed(phone_a)}_{short_id_fixed(phone_b)}_{mode}.md"
    filename = sanitize_filename(filename)
    report_path = out_dir / filename
    report_content = render_markdown_report(payload)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(report_content, encoding="utf-8")
    if not report_path.exists():
        raise RuntimeError(f"report not created: {report_path}")
    return str(report_path)


def call_direct_adapter(
    phone_a: str,
    phone_b: str,
    graph_path: str,
    source_col: str,
    target_col: str,
    directed: bool,
) -> Dict[str, Any]:
    try:
        return run_graph_path_query(
            phone_a=phone_a,
            phone_b=phone_b,
            directed=directed,
            graph_path=graph_path,
            graph_format="csv",
            source_col=source_col,
            target_col=target_col,
        )
    except TypeError:
        return run_graph_path_query(phone_a=phone_a, phone_b=phone_b, directed=directed)


def main() -> None:
    parser = argparse.ArgumentParser(description="YiGraph 风格两号码关联路径分析")
    parser.add_argument("--phone-a", required=True)
    parser.add_argument("--phone-b", required=True)
    parser.add_argument("--analysis-mode", default="both", choices=["direct_call", "composite", "both"])
    parser.add_argument("--call-graph-path", default=DEFAULT_CALL_GRAPH_PATH)
    parser.add_argument("--device-graph-path", default=DEFAULT_DEVICE_GRAPH_PATH)
    parser.add_argument("--source-col", default=DEFAULT_SOURCE_COL)
    parser.add_argument("--target-col", default=DEFAULT_TARGET_COL)
    parser.add_argument("--device-source-col", default=DEFAULT_DEVICE_SOURCE_COL)
    parser.add_argument("--device-target-col", default=DEFAULT_DEVICE_TARGET_COL)
    parser.add_argument("--directed-call", action="store_true", default=True)
    parser.add_argument("--max-hops", type=int, default=3)
    parser.add_argument("--top-k", type=int, default=1)
    parser.add_argument("--per-relation-limit", type=int, default=20)
    parser.add_argument("--max-expand-nodes", type=int, default=500)
    parser.add_argument("--min-common-counterparty", type=int, default=2)
    parser.add_argument("--strategy", default="balanced")
    args = parser.parse_args()

    call_graph_path = resolve_data_path(args.call_graph_path)
    device_graph_path = resolve_data_path(args.device_graph_path)

    engine = CompositePathEngine(call_graph_path=call_graph_path, device_graph_path=device_graph_path)
    pair_relation_signals = engine.get_pair_signals(args.phone_a, args.phone_b, preview_limit=10)
    pair_relation_signals["pair_signal_summary"] = (
        f"共同对端 {pair_relation_signals.get('common_counterparty_count', 0)} 个。"
        if pair_relation_signals.get("common_counterparty_count", 0) > 0
        else "未发现明显直接配对信号。"
    )

    result: Dict[str, Any] = {"pair_relation_signals": pair_relation_signals}
    notes: List[str] = []

    if args.analysis_mode in ("direct_call", "both"):
        direct_resp = call_direct_adapter(
            args.phone_a,
            args.phone_b,
            call_graph_path,
            args.source_col,
            args.target_col,
            args.directed_call,
        )
        direct_result = direct_resp.get("result", {}) if direct_resp.get("ok") else direct_resp
        result["direct_call_analysis"] = direct_result
        if not direct_result.get("human_summary"):
            if direct_result.get("path_found"):
                direct_result["human_summary"] = f"在有向通话图里，找到了一条长度为 {direct_result.get('path_length')} 的可达路径。"
            else:
                direct_result["human_summary"] = "在有向通话图里，没有找到从号码A到号码B的可达路径。"

    if args.analysis_mode in ("composite", "both"):
        composite = engine.find_composite_path(
            source_phone=args.phone_a,
            target_phone=args.phone_b,
            max_hops=args.max_hops,
            directed_call=args.directed_call,
            enable_call=True,
            enable_shared_device=True,
            enable_common_counterparty=True,
            max_expand_nodes=args.max_expand_nodes,
            per_relation_limit=args.per_relation_limit,
            top_k=args.top_k,
            strategy=args.strategy,
            min_common_counterparty=args.min_common_counterparty,
        )
        candidate_paths = normalize_candidate_paths(composite)
        candidate_paths = enrich_candidate_paths(candidate_paths, pair_relation_signals, args.phone_a, args.phone_b)
        bridge_ranking = build_bridge_node_ranking(candidate_paths, top_n=10)
        candidate_summary = summarize_candidate_paths(candidate_paths, top_k=args.top_k)

        best_path = dict(candidate_paths[0]) if candidate_paths else dict(composite.get("best_path", {}))
        if candidate_paths and not composite.get("best_path"):
            composite["best_path"] = best_path

        composite["candidate_paths"] = candidate_paths
        composite["candidate_paths_summary"] = candidate_summary
        composite["bridge_node_ranking"] = bridge_ranking
        composite["path_found"] = bool(candidate_paths)

        if candidate_paths:
            composite["path_nodes"] = best_path.get("path_nodes", [])
            composite["path_steps"] = best_path.get("path_steps", [])
            composite["path_length"] = best_path.get("path_length")
            composite["relation_sequence"] = best_path.get("relation_sequence", [])
            composite["score"] = best_path.get("score")
            composite["human_summary"] = build_composite_human_summary(best_path)
        else:
            composite["human_summary"] = "在当前复合关系图设置下，没有找到两号码之间的复合路径。"

        result["composite_analysis"] = composite

    result["investigation_next_steps"] = build_investigation_next_steps(
        pair_relation_signals,
        result.get("direct_call_analysis"),
        result.get("composite_analysis"),
    )

    if result.get("direct_call_analysis", {}).get("path_found"):
        result["recommended_view"] = "direct_call_analysis"
    elif result.get("composite_analysis", {}).get("path_found"):
        result["recommended_view"] = "composite_analysis"
    else:
        result["recommended_view"] = "none"

    result["pair_signal_summary"] = pair_relation_signals.get("pair_signal_summary", "")
    if get_skill_operator_alignment is not None:
        try:
            result["query_operator_alignment"] = get_skill_operator_alignment("association-path-analysis")
        except Exception:
            pass

    payload: Dict[str, Any] = {
        "ok": True,
        "skill": "association-path-analysis",
        "query_type": "path_query",
        "input_summary": {
            "phone_a": args.phone_a,
            "phone_b": args.phone_b,
            "analysis_mode": args.analysis_mode,
            "call_graph_path": call_graph_path,
            "device_graph_path": device_graph_path,
            "graph_format": "csv",
            "source_col": args.source_col,
            "target_col": args.target_col,
            "directed_call": args.directed_call,
            "max_hops": args.max_hops,
            "per_relation_limit": args.per_relation_limit,
            "max_expand_nodes": args.max_expand_nodes,
            "top_k": args.top_k,
            "strategy": args.strategy,
            "min_common_counterparty": args.min_common_counterparty,
        },
        "result": result,
        "notes": notes,
    }

    final_report_path = write_report_markdown(payload)
    payload["report_path"] = final_report_path
    payload["artifacts"] = [
        {
            "type": "markdown_report",
            "path": final_report_path,
            "title": Path(final_report_path).name,
        }
    ]
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
