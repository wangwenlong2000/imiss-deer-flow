#!/usr/bin/env python3
import argparse
import json
import math
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

import pandas as pd

CURRENT_DIR = Path(__file__).resolve().parent
SKILL_DIR = CURRENT_DIR.parent
PHONE_ANALYSIS_DIR = SKILL_DIR.parent
COMMON_DIR = PHONE_ANALYSIS_DIR / "common"
if str(COMMON_DIR) not in sys.path:
    sys.path.insert(0, str(COMMON_DIR))

try:
    from yigraph_query_capabilities import get_query_capability, get_skill_operator_alignment  # type: ignore
except Exception:
    get_query_capability = None
    get_skill_operator_alignment = None

DEFAULT_CALL_GRAPH_PATH = "/mnt/datasets/phone-network/processed/unified/call_edges.csv"
DEFAULT_DEVICE_GRAPH_PATH = "/mnt/datasets/phone-network/processed/graph_views/unified/edges_phone_imei.parquet"
DEFAULT_SOURCE_COL = "src_user_id"
DEFAULT_TARGET_COL = "dst_counterparty_id"
DEFAULT_DEVICE_SOURCE_COL = "user_id"
DEFAULT_DEVICE_TARGET_COL = "imei"
CSV_READ_KWARGS = {"low_memory": False}


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
    fallback = Path("/tmp/overlap-analysis-outputs")
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback


def sanitize_filename(text: str) -> str:
    text = re.sub(r"[^0-9A-Za-z_.\-\u4e00-\u9fff]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "report"


def short_id(value: str, head: int = 12) -> str:
    return value if len(value) <= head else value[:head] + "..."


def short_id_fixed(value: str, head: int = 8) -> str:
    if not value:
        return "unknown"
    return value[:head]


def normalize_series(series: pd.Series) -> pd.Series:
    return series.astype("string").fillna("").str.strip()


def jaccard_score(a: Set[str], b: Set[str]) -> float:
    union = a | b
    if not union:
        return 0.0
    return len(a & b) / len(union)


def overlap_ratio(common: Set[str], base: Set[str]) -> float:
    if not base:
        return 0.0
    return len(common) / len(base)


def relation_strength_level(shared_device_count: int, common_counterparty_count: int, cp_jaccard: float, dev_jaccard: float) -> str:
    if shared_device_count >= 1 and dev_jaccard >= 0.2:
        return "strong"
    if shared_device_count >= 1:
        return "strong"
    if common_counterparty_count >= 5:
        return "strong"
    if common_counterparty_count >= 3 or cp_jaccard >= 0.1:
        return "medium"
    if common_counterparty_count >= 1:
        return "weak"
    return "none"


def read_call_edges(path: str, source_col: str, target_col: str) -> pd.DataFrame:
    df = pd.read_csv(path, usecols=[source_col, target_col], **CSV_READ_KWARGS)
    df[source_col] = normalize_series(df[source_col])
    df[target_col] = normalize_series(df[target_col])
    return df[(df[source_col] != "") & (df[target_col] != "")].copy()


def read_device_edges(path: str, source_col: str, target_col: str) -> pd.DataFrame:
    if path.lower().endswith(".parquet"):
        df = pd.read_parquet(path, columns=[source_col, target_col])
    else:
        df = pd.read_csv(path, usecols=[source_col, target_col], **CSV_READ_KWARGS)
    df[source_col] = normalize_series(df[source_col])
    df[target_col] = normalize_series(df[target_col])
    return df[(df[source_col] != "") & (df[target_col] != "")].copy()


def collect_counterparties(df: pd.DataFrame, phone: str, source_col: str, target_col: str) -> Set[str]:
    out_neighbors = set(df.loc[df[source_col] == phone, target_col].astype(str).tolist())
    in_neighbors = set(df.loc[df[target_col] == phone, source_col].astype(str).tolist())
    return {x for x in (out_neighbors | in_neighbors) if x and x != phone}


def collect_devices(df: pd.DataFrame, phone: str, source_col: str, target_col: str) -> Set[str]:
    return set(df.loc[df[source_col] == phone, target_col].astype(str).tolist())


def direct_call_flags(df: pd.DataFrame, phone_a: str, phone_b: str, source_col: str, target_col: str) -> Tuple[bool, bool]:
    a_calls_b = bool(((df[source_col] == phone_a) & (df[target_col] == phone_b)).any())
    b_calls_a = bool(((df[source_col] == phone_b) & (df[target_col] == phone_a)).any())
    return a_calls_b, b_calls_a


def score_common_counterparties(common_nodes: Set[str], call_df: pd.DataFrame, source_col: str, target_col: str) -> List[Dict[str, Any]]:
    if not common_nodes:
        return []
    rows = []
    for node in common_nodes:
        local = call_df[(call_df[source_col] == node) | (call_df[target_col] == node)]
        local_degree = len(set(local[source_col].astype(str).tolist()) | set(local[target_col].astype(str).tolist())) - 1
        call_frequency = len(local)
        score = round(math.log1p(call_frequency) + math.log1p(max(local_degree, 0)), 6)
        rows.append(
            {
                "node": node,
                "node_preview": short_id(node),
                "call_frequency": int(call_frequency),
                "local_degree": int(max(local_degree, 0)),
                "score": score,
                "role": "common_counterparty",
            }
        )
    rows.sort(key=lambda x: (-x["score"], -x["call_frequency"], x["node"]))
    for idx, row in enumerate(rows, start=1):
        row["rank"] = idx
    return rows


def score_shared_devices(common_devices: Set[str], device_df: pd.DataFrame, source_col: str, target_col: str) -> List[Dict[str, Any]]:
    if not common_devices:
        return []
    rows = []
    for dev in common_devices:
        shared_phones = set(device_df.loc[device_df[target_col] == dev, source_col].astype(str).tolist())
        shared_phone_count = len(shared_phones)
        score = round(math.log1p(shared_phone_count) * 5, 6)
        rows.append(
            {
                "device": dev,
                "device_preview": short_id(dev),
                "shared_phone_count": int(shared_phone_count),
                "shared_phones_preview": [short_id(x) for x in sorted(shared_phones)[:10]],
                "score": score,
                "role": "shared_device",
            }
        )
    rows.sort(key=lambda x: (-x["score"], -x["shared_phone_count"], x["device"]))
    for idx, row in enumerate(rows, start=1):
        row["rank"] = idx
    return rows


def next_steps(
    level: str,
    shared_device_count: int,
    common_counterparty_count: int,
    top_common_counterparties: List[Dict[str, Any]],
) -> List[str]:
    focus_nodes = [x["node_preview"] for x in top_common_counterparties[:2]]

    steps: List[str] = []
    if focus_nodes:
        steps.append(f"优先核查共同对端 Top 节点：{', '.join(focus_nodes)}。")
    if shared_device_count > 0:
        steps.append("优先核查共享设备：检查这些设备是否被多个号码复用，是否存在养号、设备池或团伙共用现象。")
    if common_counterparty_count > 0:
        steps.append("建议以 Top 共同对端为中心，继续调用 subgraph-extraction-analysis 抽取 1-2 跳局部关系圈。")
    if level in {"medium", "strong"}:
        steps.append("若要判断两个号码之间是否存在路径型间接关系，继续调用 association-path-analysis 做路径型联合核查。")
    else:
        steps.append("若当前重叠证据较弱，可继续调用 association-path-analysis 检查是否仍存在间接路径关系。")
    steps.append("若共享设备线索后续出现，再重点排查共用设备对应的其他号码画像与邻居结构。")
    return steps


def build_recommended_followups(top_common_counterparties: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    reason = "围绕 Top 共同对端继续下钻局部关系圈。"
    if top_common_counterparties:
        reason = f"围绕 Top 共同对端 {', '.join([x['node_preview'] for x in top_common_counterparties[:2]])} 继续下钻局部关系圈。"
    return [
        {
            "skill": "subgraph-extraction-analysis",
            "reason": reason,
        },
        {
            "skill": "association-path-analysis",
            "reason": "继续分析两个号码是否存在路径型间接关系。",
        },
    ]


def build_markdown_report(result: Dict[str, Any], report_path: Path) -> None:
    r = result["result"]
    lines: List[str] = []
    lines.append("# 号码重叠关系分析报告")
    lines.append("")
    lines.append(f"- 号码 A：`{r['phone_a']}`")
    lines.append(f"- 号码 B：`{r['phone_b']}`")
    lines.append("")
    lines.append("## 1. 直接配对信号")
    lines.append("")
    lines.append(f"- A→B 直接通话：{'是' if r['direct_signals']['a_calls_b'] else '否'}")
    lines.append(f"- B→A 直接通话：{'是' if r['direct_signals']['b_calls_a'] else '否'}")
    lines.append("")
    lines.append("## 2. 共同对端重叠")
    lines.append("")
    cp = r["common_counterparty_analysis"]
    lines.append(f"- A 对端数：{cp['phone_a_counterparty_count']}")
    lines.append(f"- B 对端数：{cp['phone_b_counterparty_count']}")
    lines.append(f"- 共同对端数：{cp['common_counterparty_count']}")
    lines.append(f"- Jaccard 重叠率：{cp['jaccard']:.4f}")
    lines.append(f"- A 口径重叠率：{cp['ratio_vs_a']:.4f}")
    lines.append(f"- B 口径重叠率：{cp['ratio_vs_b']:.4f}")
    lines.append("")
    if cp["top_common_counterparties"]:
        lines.append("### Top 共同对端证据")
        lines.append("")
        lines.append("| 排名 | 节点预览 | 局部度数 | 通话记录数 | 评分 |")
        lines.append("|---|---|---:|---:|---:|")
        for row in cp["top_common_counterparties"]:
            lines.append(f"| {row['rank']} | `{row['node_preview']}` | {row['local_degree']} | {row['call_frequency']} | {row['score']:.2f} |")
        lines.append("")
    lines.append("## 3. 共享设备重叠")
    lines.append("")
    dev = r["shared_device_analysis"]
    lines.append(f"- A 设备数：{dev['phone_a_device_count']}")
    lines.append(f"- B 设备数：{dev['phone_b_device_count']}")
    lines.append(f"- 共享设备数：{dev['shared_device_count']}")
    lines.append(f"- 设备 Jaccard 重叠率：{dev['jaccard']:.4f}")
    lines.append("")
    if dev["top_shared_devices"]:
        lines.append("### Top 共享设备证据")
        lines.append("")
        lines.append("| 排名 | 设备预览 | 共享号码数 | 评分 |")
        lines.append("|---|---|---:|---:|")
        for row in dev["top_shared_devices"]:
            lines.append(f"| {row['rank']} | `{row['device_preview']}` | {row['shared_phone_count']} | {row['score']:.2f} |")
        lines.append("")
    lines.append("## 4. 综合判断")
    lines.append("")
    lines.append(f"- 重叠强度等级：**{r['overall_level']}**")
    lines.append(f"- 一句话结论：{r['human_summary']}")
    lines.append("")
    lines.append("## 5. 下一步调查建议")
    lines.append("")
    for idx, item in enumerate(r["investigation_next_steps"], start=1):
        lines.append(f"{idx}. {item}")
    lines.append("")
    followups = r.get("recommended_followups", [])
    if followups:
        lines.append("## 6. 推荐联动 Skill")
        lines.append("")
        for item in followups:
            lines.append(f"- `{item['skill']}`：{item['reason']}")
        lines.append("")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(lines), encoding="utf-8")
    if not report_path.exists():
        raise RuntimeError(f"report not created: {report_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="YiGraph-style overlap analysis for two phone numbers")
    parser.add_argument("--phone-a", required=True)
    parser.add_argument("--phone-b", required=True)
    parser.add_argument("--call-graph-path", default=DEFAULT_CALL_GRAPH_PATH)
    parser.add_argument("--device-graph-path", default=DEFAULT_DEVICE_GRAPH_PATH)
    parser.add_argument("--graph-format", default="csv")
    parser.add_argument("--source-col", default=DEFAULT_SOURCE_COL)
    parser.add_argument("--target-col", default=DEFAULT_TARGET_COL)
    parser.add_argument("--device-source-col", default=DEFAULT_DEVICE_SOURCE_COL)
    parser.add_argument("--device-target-col", default=DEFAULT_DEVICE_TARGET_COL)
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--min-common-counterparty", type=int, default=1)
    args = parser.parse_args()

    phone_a = args.phone_a.strip()
    phone_b = args.phone_b.strip()
    call_graph_path = resolve_data_path(args.call_graph_path)
    device_graph_path = resolve_data_path(args.device_graph_path)

    call_df = read_call_edges(call_graph_path, args.source_col, args.target_col)
    device_df = read_device_edges(device_graph_path, args.device_source_col, args.device_target_col)

    a_calls_b, b_calls_a = direct_call_flags(call_df, phone_a, phone_b, args.source_col, args.target_col)
    cp_a = collect_counterparties(call_df, phone_a, args.source_col, args.target_col)
    cp_b = collect_counterparties(call_df, phone_b, args.source_col, args.target_col)
    common_cp = cp_a & cp_b
    dev_a = collect_devices(device_df, phone_a, args.device_source_col, args.device_target_col)
    dev_b = collect_devices(device_df, phone_b, args.device_source_col, args.device_target_col)
    common_dev = dev_a & dev_b

    cp_jaccard = jaccard_score(cp_a, cp_b)
    dev_jaccard = jaccard_score(dev_a, dev_b)
    cp_ranked = score_common_counterparties(common_cp, call_df, args.source_col, args.target_col)
    dev_ranked = score_shared_devices(common_dev, device_df, args.device_source_col, args.device_target_col)
    level = relation_strength_level(len(common_dev), len(common_cp), cp_jaccard, dev_jaccard)
    human_summary = (
        f"两个号码的重叠强度等级为 {level}。"
        f" 共同对端 {len(common_cp)} 个，共享设备 {len(common_dev)} 个。"
        f" 这说明两者在同一联系圈中的重叠程度为 {'较强' if level == 'strong' else '中等' if level == 'medium' else '较弱' if level == 'weak' else '暂无明显'}。"
    )
    top_common_counterparties = cp_ranked[: max(1, args.top_k)]
    next_actions = next_steps(level, len(common_dev), len(common_cp), top_common_counterparties)
    recommended_followups = build_recommended_followups(top_common_counterparties)

    result_payload: Dict[str, Any] = {
        "phone_a": phone_a,
        "phone_b": phone_b,
        "direct_signals": {"a_calls_b": a_calls_b, "b_calls_a": b_calls_a},
        "common_counterparty_analysis": {
            "phone_a_counterparty_count": len(cp_a),
            "phone_b_counterparty_count": len(cp_b),
            "common_counterparty_count": len(common_cp),
            "jaccard": cp_jaccard,
            "ratio_vs_a": overlap_ratio(common_cp, cp_a),
            "ratio_vs_b": overlap_ratio(common_cp, cp_b),
            "top_common_counterparties": top_common_counterparties,
            "passed_min_threshold": len(common_cp) >= args.min_common_counterparty,
        },
        "shared_device_analysis": {
            "phone_a_device_count": len(dev_a),
            "phone_b_device_count": len(dev_b),
            "shared_device_count": len(common_dev),
            "jaccard": dev_jaccard,
            "ratio_vs_a": overlap_ratio(common_dev, dev_a),
            "ratio_vs_b": overlap_ratio(common_dev, dev_b),
            "top_shared_devices": dev_ranked[: max(1, args.top_k)],
        },
        "overall_level": level,
        "human_summary": human_summary,
        "investigation_next_steps": next_actions,
        "recommended_followups": recommended_followups,
    }

    query_cap_common_neighbor = get_query_capability("common_neighbor") if get_query_capability else None
    query_cap_relationship_filter = get_query_capability("relationship_filter") if get_query_capability else None
    if get_skill_operator_alignment:
        try:
            skill_alignment = get_skill_operator_alignment("overlap-analysis")
        except Exception:
            try:
                skill_alignment = get_skill_operator_alignment("association-path-analysis")
            except Exception:
                skill_alignment = []
    else:
        skill_alignment = []

    output_dir = ensure_output_dir()
    report_name = f"overlap_report_{short_id_fixed(phone_a)}_{short_id_fixed(phone_b)}.md"
    report_name = sanitize_filename(report_name)
    report_path = output_dir / report_name

    final_output: Dict[str, Any] = {
        "ok": True,
        "skill": "overlap-analysis",
        "query_type": "common_neighbor",
        "input_summary": {
            "phone_a": phone_a,
            "phone_b": phone_b,
            "call_graph_path": call_graph_path,
            "device_graph_path": device_graph_path,
            "graph_format": args.graph_format,
            "source_col": args.source_col,
            "target_col": args.target_col,
            "device_source_col": args.device_source_col,
            "device_target_col": args.device_target_col,
            "top_k": args.top_k,
            "min_common_counterparty": args.min_common_counterparty,
        },
        "result": result_payload,
        "notes": [],
        "yigraph_meta": {
            "common_neighbor_capability": query_cap_common_neighbor,
            "relationship_filter_capability": query_cap_relationship_filter,
            "related_skill_alignment_reference": skill_alignment,
            "explanation": "overlap-analysis 主要复用 YiGraph 的 common_neighbor + relationship_filter + aggregation_query 风格。",
        },
    }

    build_markdown_report(final_output, report_path)
    final_report_path = str(report_path)
    final_output["report_path"] = final_report_path
    final_output["artifacts"] = [
        {
            "type": "markdown_report",
            "path": final_report_path,
            "title": Path(final_report_path).name,
        }
    ]
    print(json.dumps(final_output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
