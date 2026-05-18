#!/usr/bin/env python3
"""single-number-analysis

完整的单号码综合分析 skill。

本版本重点修复三类问题：
1. 前端路由容易误走 subgraph-extraction-analysis
2. 边数统计口径不统一
3. 报告结构仍偏“子图抽取”而非“单号码主分析”

当前版本特性：
- 支持 mixed / call_only / device_only 三种分析模式
- 支持通话图有向/无向观察
- 明确区分“原始记录数/关系投影次数/图去重边数”
- 输出单号码主分析报告，而不是子图报告
- 返回稳定的 report_path / report_exists / artifacts，便于前端展示附件
"""

from __future__ import annotations

import argparse
import json
import math
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import networkx as nx
import pandas as pd

DEFAULT_ROOT = Path("/workspace/imiss-deer-flow-main")
DEFAULT_USER_NODE_PATH = DEFAULT_ROOT / "datasets/phone-network/processed/unified/user_nodes.csv"
DEFAULT_CALL_GRAPH_PATH = DEFAULT_ROOT / "datasets/phone-network/processed/unified/call_edges.csv"
DEFAULT_DEVICE_GRAPH_PATH = DEFAULT_ROOT / "datasets/phone-network/processed/graph_views/unified/edges_phone_imei.parquet"
DEFAULT_OUTPUT_DIR = Path("/mnt/user-data/outputs")
DEFAULT_DATASET_ROOT_CANDIDATES = [
    DEFAULT_ROOT / "datasets/phone-network",
    Path("/mnt/datasets/phone-network"),
    Path("/workspace/imiss-deer-flow-main/datasets/phone-network"),
]


def short_id(value: Optional[str], n: int = 12) -> str:
    if not value:
        return ""
    return value if len(value) <= n else f"{value[:n]}..."


def safe_read_table(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"file_not_found: {path}")
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix == ".parquet":
        return pd.read_parquet(path)
    raise ValueError(f"unsupported_file_type: {path}")


def resolve_dataset_paths(dataset_root: Optional[str], dataset: str) -> Tuple[Path, Path, Path]:
    """Resolve standard phone-network graph files from dataset-root + dataset.

    Explicit file path arguments in main() still take priority; this helper only
    provides the standard three-file fallback for processed datasets.
    """
    candidates: List[Path] = []
    if dataset_root:
        candidates.append(Path(dataset_root).expanduser())
    candidates.extend(DEFAULT_DATASET_ROOT_CANDIDATES)

    checked: List[str] = []
    for root in candidates:
        user_node_path = root / "processed" / dataset / "user_nodes.csv"
        call_graph_path = root / "processed" / dataset / "call_edges.csv"
        parquet_device_path = root / "processed" / "graph_views" / dataset / "edges_phone_imei.parquet"
        csv_device_path = root / "processed" / "graph_views" / dataset / "edges_phone_imei.csv"
        device_graph_path = parquet_device_path if parquet_device_path.exists() else csv_device_path

        checked.append(
            f"{root}: user={user_node_path.exists()}, call={call_graph_path.exists()}, device={device_graph_path.exists()}"
        )
        if user_node_path.exists() and call_graph_path.exists() and device_graph_path.exists():
            return user_node_path, call_graph_path, device_graph_path

    raise FileNotFoundError(
        f"cannot_resolve_dataset_paths: dataset={dataset}, checked={checked}"
    )


class PhoneDataContext:
    def __init__(
        self,
        user_node_path: Path,
        call_graph_path: Path,
        device_graph_path: Path,
        source_col: str,
        target_col: str,
        device_source_col: str,
        device_target_col: str,
    ) -> None:
        self.user_node_path = user_node_path
        self.call_graph_path = call_graph_path
        self.device_graph_path = device_graph_path
        self.source_col = source_col
        self.target_col = target_col
        self.device_source_col = device_source_col
        self.device_target_col = device_target_col

        self.user_df = safe_read_table(user_node_path)
        self.call_df = safe_read_table(call_graph_path)
        self.device_df = safe_read_table(device_graph_path)

        self._normalize_frames()
        self._build_indexes()

    def _normalize_frames(self) -> None:
        required_call_cols = {self.source_col, self.target_col}
        if not required_call_cols.issubset(set(self.call_df.columns)):
            raise ValueError(
                f"call_graph_missing_columns: need {sorted(required_call_cols)}, got {list(self.call_df.columns)}"
            )

        required_device_cols = {self.device_source_col, self.device_target_col}
        if not required_device_cols.issubset(set(self.device_df.columns)):
            raise ValueError(
                f"device_graph_missing_columns: need {sorted(required_device_cols)}, got {list(self.device_df.columns)}"
            )

        self.call_df[self.source_col] = self.call_df[self.source_col].astype(str)
        self.call_df[self.target_col] = self.call_df[self.target_col].astype(str)
        self.device_df[self.device_source_col] = self.device_df[self.device_source_col].astype(str)
        self.device_df[self.device_target_col] = self.device_df[self.device_target_col].astype(str)

        if "user_id" in self.user_df.columns:
            self.user_df["user_id"] = self.user_df["user_id"].astype(str)

    def _build_indexes(self) -> None:
        self.out_neighbors: Dict[str, Set[str]] = defaultdict(set)
        self.in_neighbors: Dict[str, Set[str]] = defaultdict(set)
        self.undirected_neighbors: Dict[str, Set[str]] = defaultdict(set)
        self.pair_call_freq: Counter[Tuple[str, str]] = Counter()

        for row in self.call_df[[self.source_col, self.target_col]].itertuples(index=False):
            src = getattr(row, self.source_col)
            dst = getattr(row, self.target_col)
            self.out_neighbors[src].add(dst)
            self.in_neighbors[dst].add(src)
            self.undirected_neighbors[src].add(dst)
            self.undirected_neighbors[dst].add(src)
            self.pair_call_freq[(src, dst)] += 1

        self.phone_to_devices: Dict[str, Set[str]] = defaultdict(set)
        self.device_to_phones: Dict[str, Set[str]] = defaultdict(set)
        for row in self.device_df[[self.device_source_col, self.device_target_col]].itertuples(index=False):
            phone = getattr(row, self.device_source_col)
            device = getattr(row, self.device_target_col)
            self.phone_to_devices[phone].add(device)
            self.device_to_phones[device].add(phone)

        self.user_index: Dict[str, Dict[str, object]] = {}
        if "user_id" in self.user_df.columns:
            for rec in self.user_df.to_dict(orient="records"):
                self.user_index[str(rec.get("user_id"))] = rec

    def get_profile(self, phone_id: str) -> Dict[str, object]:
        attrs = self.user_index.get(phone_id, {})
        call_record_count = self.pair_call_freq_total(phone_id)
        counterparties = self.undirected_neighbors.get(phone_id, set())
        devices = self.phone_to_devices.get(phone_id, set())
        return {
            "phone_id": phone_id,
            "node_found": bool(attrs),
            "raw_node_attrs": attrs,
            "call_record_count": int(call_record_count),
            "counterparty_count": len(counterparties),
            "sample_counterparties": sorted(list(counterparties))[:10],
            "device_count": len(devices),
            "sample_devices": sorted(list(devices))[:10],
        }

    def pair_call_freq_total(self, phone_id: str) -> int:
        total = 0
        for (src, dst), cnt in self.pair_call_freq.items():
            if src == phone_id or dst == phone_id:
                total += cnt
        return int(total)

    def phone_call_frequency(self, a: str, b: str) -> int:
        return int(self.pair_call_freq.get((a, b), 0) + self.pair_call_freq.get((b, a), 0))

    def device_peers(self, phone_id: str) -> Set[str]:
        peers: Set[str] = set()
        for device in self.phone_to_devices.get(phone_id, set()):
            peers.update(self.device_to_phones.get(device, set()))
        peers.discard(phone_id)
        return peers

    def shared_device_details(self, phone_id: str) -> List[Dict[str, object]]:
        rows: List[Dict[str, object]] = []
        for device in sorted(self.phone_to_devices.get(phone_id, set())):
            phones = sorted(list(self.device_to_phones.get(device, set()) - {phone_id}))
            if not phones:
                continue
            rows.append(
                {
                    "device_id": device,
                    "device_preview": short_id(device),
                    "shared_phone_count": len(phones),
                    "shared_phones_preview": [short_id(x) for x in phones[:5]],
                    "shared_phone_ids": phones,
                }
            )
        rows.sort(key=lambda x: (-int(x["shared_phone_count"]), str(x["device_id"])))
        return rows


class SingleNumberAnalyzer:
    def __init__(self, ctx: PhoneDataContext) -> None:
        self.ctx = ctx

    def collect_candidate_nodes(
        self,
        phone_id: str,
        hops: int,
        analysis_mode: str,
        directed_call: bool,
    ) -> Set[str]:
        if hops < 1:
            return {phone_id}

        visited: Set[str] = {phone_id}
        frontier: Set[str] = {phone_id}

        for _ in range(hops):
            next_frontier: Set[str] = set()
            for node in frontier:
                if analysis_mode in {"mixed", "call_only"}:
                    if directed_call:
                        call_next = set(self.ctx.out_neighbors.get(node, set())) | set(self.ctx.in_neighbors.get(node, set()))
                    else:
                        call_next = set(self.ctx.undirected_neighbors.get(node, set()))
                    next_frontier.update(call_next)
                if analysis_mode in {"mixed", "device_only"}:
                    next_frontier.update(self.ctx.device_peers(node))
            next_frontier -= visited
            visited.update(next_frontier)
            frontier = next_frontier
            if not frontier:
                break

        return visited

    def call_edge_stats(self, nodes: Set[str], directed_call: bool) -> Dict[str, object]:
        if not nodes:
            return {
                "raw_record_count": 0,
                "unique_graph_edge_count": 0,
                "undirected_merged_edge_count": 0,
                "preview": [],
            }

        raw_record_count = 0
        unique_pairs: Set[Tuple[str, str]] = set()
        undirected_pairs: Set[Tuple[str, str]] = set()
        preview: List[Tuple[str, str]] = []

        for row in self.ctx.call_df[[self.ctx.source_col, self.ctx.target_col]].itertuples(index=False):
            src = getattr(row, self.ctx.source_col)
            dst = getattr(row, self.ctx.target_col)
            if src in nodes and dst in nodes:
                raw_record_count += 1
                pair = (src, dst) if directed_call else tuple(sorted((src, dst)))
                unique_pairs.add(pair)
                undirected_pairs.add(tuple(sorted((src, dst))))
                if len(preview) < 20:
                    preview.append((short_id(src), short_id(dst)))

        return {
            "raw_record_count": raw_record_count,
            "unique_graph_edge_count": len(unique_pairs),
            "undirected_merged_edge_count": len(undirected_pairs),
            "preview": preview,
        }

    def device_edge_stats(self, nodes: Set[str]) -> Dict[str, object]:
        if not nodes:
            return {
                "pair_incidence_count": 0,
                "unique_graph_edge_count": 0,
                "preview": [],
            }

        pair_incidence_count = 0
        unique_pairs: Set[Tuple[str, str]] = set()
        preview: List[Tuple[str, str]] = []

        for device, phones in self.ctx.device_to_phones.items():
            overlap = sorted(list(set(phones) & nodes))
            m = len(overlap)
            if m < 2:
                continue
            pair_incidence_count += m * (m - 1) // 2
            for i in range(m):
                for j in range(i + 1, m):
                    pair = tuple(sorted((overlap[i], overlap[j])))
                    if pair not in unique_pairs and len(preview) < 20:
                        preview.append((short_id(pair[0]), short_id(pair[1])))
                    unique_pairs.add(pair)

        return {
            "pair_incidence_count": pair_incidence_count,
            "unique_graph_edge_count": len(unique_pairs),
            "unique_pairs": unique_pairs,
            "preview": preview,
        }

    def relation_union_edge_count(
        self,
        call_stats: Dict[str, object],
        device_stats: Dict[str, object],
        include_call: bool,
        include_device: bool,
    ) -> int:
        merged_pairs: Set[Tuple[str, str]] = set()
        if include_call:
            # For single-number main report, the merged structural graph uses undirected relation pairs.
            # This avoids “总边数”和“分关系边数”完全不同口径时造成误解。
            # directed_call 的方向信息会单独保留在 call_edge_stats 里。
            # 这里只负责“结构圈规模展示”。
            merged_pairs_count = int(call_stats.get("undirected_merged_edge_count", 0))
            # no direct pair list available; use count only below when there is no device relation
            if not include_device:
                return merged_pairs_count
        if include_device:
            merged_pairs.update(device_stats.get("unique_pairs", set()))
        # 如果包含通话关系且也包含设备关系，则重新构建通话无向 pair 集合。
        if include_call and include_device:
            # recover from preview-less count by scanning already filtered nodes not available here;
            # caller should avoid this path for exact union calculation.
            # this method is only a fallback and is not used in final report for mixed mode.
            pass
        return len(merged_pairs)

    def build_priority_scores(
        self,
        phone_id: str,
        candidate_nodes: Set[str],
        analysis_mode: str,
    ) -> Dict[str, Dict[str, object]]:
        direct_call_neighbors = set(self.ctx.undirected_neighbors.get(phone_id, set()))
        device_peers = self.ctx.device_peers(phone_id)
        phone_devices = self.ctx.phone_to_devices.get(phone_id, set())

        candidate_call_degree_counter: Counter[str] = Counter()
        candidate_device_degree_counter: Counter[str] = Counter()

        if analysis_mode in {"mixed", "call_only"}:
            for row in self.ctx.call_df[[self.ctx.source_col, self.ctx.target_col]].itertuples(index=False):
                src = getattr(row, self.ctx.source_col)
                dst = getattr(row, self.ctx.target_col)
                if src in candidate_nodes and dst in candidate_nodes:
                    candidate_call_degree_counter[src] += 1
                    candidate_call_degree_counter[dst] += 1

        if analysis_mode in {"mixed", "device_only"}:
            for device, phones in self.ctx.device_to_phones.items():
                overlap = sorted(list(set(phones) & candidate_nodes))
                if len(overlap) < 2:
                    continue
                for i in range(len(overlap)):
                    for j in range(i + 1, len(overlap)):
                        a, b = overlap[i], overlap[j]
                        candidate_device_degree_counter[a] += 1
                        candidate_device_degree_counter[b] += 1

        score_map: Dict[str, Dict[str, object]] = {}
        for node in candidate_nodes:
            if node == phone_id:
                continue

            shared_device_ids = sorted(list(self.ctx.phone_to_devices.get(node, set()) & phone_devices))
            shared_device_strength = 0.0
            shared_device_peer_total = 0
            for dev in shared_device_ids:
                peer_count = max(len(self.ctx.device_to_phones.get(dev, set())) - 1, 0)
                shared_device_peer_total += peer_count
                shared_device_strength += math.log1p(peer_count)

            call_freq = self.ctx.phone_call_frequency(phone_id, node)
            direct_call_flag = node in direct_call_neighbors
            device_flag = node in device_peers
            multi_line_flag = direct_call_flag and device_flag
            candidate_call_degree = int(candidate_call_degree_counter.get(node, 0))
            candidate_device_degree = int(candidate_device_degree_counter.get(node, 0))
            candidate_relation_degree = candidate_call_degree + candidate_device_degree

            score = 0.0
            score += 5.0 if device_flag else 0.0
            score += 4.0 if direct_call_flag else 0.0
            score += 3.0 if multi_line_flag else 0.0
            score += 2.5 * shared_device_strength
            score += 1.8 * math.log1p(call_freq)
            score += 1.2 * math.log1p(candidate_relation_degree)
            score += 1.5 if len(shared_device_ids) >= 2 else 0.0

            reasons: List[str] = []
            if direct_call_flag:
                reasons.append("与中心号码存在直接通话关系")
            if device_flag:
                reasons.append("与中心号码存在共享设备关系")
            if multi_line_flag:
                reasons.append("同时命中通话线索和设备线索")
            if shared_device_peer_total > 0:
                reasons.append(f"共享设备共牵出 {shared_device_peer_total} 个关联号码")
            if call_freq > 0:
                reasons.append(f"与中心号码通话频次为 {call_freq}")
            if candidate_relation_degree > 0:
                reasons.append(f"在候选关系圈中的候选局部度为 {candidate_relation_degree}")
            if not reasons:
                reasons.append("在候选关系圈中出现，但当前证据较弱")

            score_map[node] = {
                "node": node,
                "node_preview": short_id(node),
                "score": round(score, 6),
                "direct_call_related": direct_call_flag,
                "shared_device_related": device_flag,
                "shared_device_count_with_center": len(shared_device_ids),
                "shared_device_ids_preview": [short_id(x) for x in shared_device_ids[:5]],
                "shared_device_peer_total": shared_device_peer_total,
                "call_frequency_with_center": call_freq,
                "candidate_call_degree": candidate_call_degree,
                "candidate_device_degree": candidate_device_degree,
                "candidate_relation_degree": candidate_relation_degree,
                "reasons": reasons,
            }
        return score_map

    def retain_priority_subgraph(
        self,
        phone_id: str,
        candidate_nodes: Set[str],
        max_nodes: int,
        score_map: Dict[str, Dict[str, object]],
        analysis_mode: str,
    ) -> Dict[str, object]:
        kept_nodes: List[str] = [phone_id]
        ranked_nodes = sorted(
            [x for x in candidate_nodes if x != phone_id],
            key=lambda x: (-float(score_map.get(x, {}).get("score", 0.0)), str(x)),
        )
        if max_nodes > 1:
            kept_nodes.extend(ranked_nodes[: max_nodes - 1])
        kept_set = set(kept_nodes)

        # 角色识别统一在“无向关系图”上做，避免 directed_call 时桥接点/局部度解释混乱。
        role_graph = nx.Graph()
        role_graph.add_nodes_from(kept_set)

        if analysis_mode in {"mixed", "call_only"}:
            for row in self.ctx.call_df[[self.ctx.source_col, self.ctx.target_col]].itertuples(index=False):
                src = getattr(row, self.ctx.source_col)
                dst = getattr(row, self.ctx.target_col)
                if src in kept_set and dst in kept_set:
                    role_graph.add_edge(src, dst)

        device_pairs: Set[Tuple[str, str]] = set()
        if analysis_mode in {"mixed", "device_only"}:
            for device, phones in self.ctx.device_to_phones.items():
                overlap = sorted(list(set(phones) & kept_set))
                if len(overlap) < 2:
                    continue
                for i in range(len(overlap)):
                    for j in range(i + 1, len(overlap)):
                        a, b = overlap[i], overlap[j]
                        pair = tuple(sorted((a, b)))
                        device_pairs.add(pair)
                        role_graph.add_edge(a, b)

        try:
            if role_graph.number_of_nodes() <= 500:
                betweenness = nx.betweenness_centrality(role_graph)
            else:
                betweenness = {n: 0.0 for n in role_graph.nodes()}
        except Exception:
            betweenness = {n: 0.0 for n in role_graph.nodes()}

        role_rows: List[Dict[str, object]] = []
        for node in kept_set:
            if node == phone_id:
                continue
            entry = {
                "node": node,
                "node_preview": short_id(node),
                "retained_local_degree": int(role_graph.degree(node)) if node in role_graph else 0,
                "local_betweenness_centrality": round(float(betweenness.get(node, 0.0)), 6),
                "shared_device_related": bool(score_map.get(node, {}).get("shared_device_related", False)),
                "suspicious_score": round(float(score_map.get(node, {}).get("score", 0.0)), 6),
            }
            role_rows.append(entry)

        local_hubs = sorted(
            role_rows,
            key=lambda x: (-int(x["retained_local_degree"]), -float(x["suspicious_score"]), str(x["node"])),
        )[:10]
        bridge_nodes = sorted(
            role_rows,
            key=lambda x: (
                -float(x["local_betweenness_centrality"]),
                -int(x["retained_local_degree"]),
                -float(x["suspicious_score"]),
                str(x["node"]),
            ),
        )[:10]

        return {
            "kept_nodes": kept_nodes,
            "kept_set": kept_set,
            "role_graph": role_graph,
            "local_hubs": local_hubs,
            "bridge_nodes": bridge_nodes,
            "actual_num_nodes": len(kept_set),
            "actual_unique_relation_edge_count": int(role_graph.number_of_edges()),
            "actual_device_unique_pair_count": len(device_pairs),
            "truncated": len(candidate_nodes) > len(kept_set),
        }

    def recommend_drilldown_seeds(self, suspicious_nodes: List[Dict[str, object]]) -> List[Dict[str, object]]:
        seeds: List[Dict[str, object]] = []
        for idx, item in enumerate(suspicious_nodes[:3], start=1):
            reasons = list(item.get("reasons", []))[:3]
            seeds.append(
                {
                    "rank": idx,
                    "node": item["node"],
                    "node_preview": item["node_preview"],
                    "score": item["score"],
                    "why_recommended": reasons,
                    "followup_skills": [
                        "association-path-analysis",
                        "overlap-analysis",
                        "subgraph-extraction-analysis",
                    ],
                }
            )
        return seeds

    def _build_candidate_call_undirected_pairs(self, nodes: Set[str]) -> Set[Tuple[str, str]]:
        pairs: Set[Tuple[str, str]] = set()
        for row in self.ctx.call_df[[self.ctx.source_col, self.ctx.target_col]].itertuples(index=False):
            src = getattr(row, self.ctx.source_col)
            dst = getattr(row, self.ctx.target_col)
            if src in nodes and dst in nodes:
                pairs.add(tuple(sorted((src, dst))))
        return pairs

    def run(
        self,
        phone_id: str,
        hops: int,
        max_nodes: int,
        top_k: int,
        analysis_mode: str,
        directed_call: bool,
        output_dir: Path,
    ) -> Dict[str, object]:
        include_call = analysis_mode in {"mixed", "call_only"}
        include_device = analysis_mode in {"mixed", "device_only"}

        profile = self.ctx.get_profile(phone_id)
        candidate_nodes = self.collect_candidate_nodes(
            phone_id=phone_id,
            hops=hops,
            analysis_mode=analysis_mode,
            directed_call=directed_call,
        )

        candidate_call_stats = self.call_edge_stats(candidate_nodes, directed_call) if include_call else {
            "raw_record_count": 0,
            "unique_graph_edge_count": 0,
            "undirected_merged_edge_count": 0,
            "preview": [],
        }
        candidate_device_stats = self.device_edge_stats(candidate_nodes) if include_device else {
            "pair_incidence_count": 0,
            "unique_graph_edge_count": 0,
            "unique_pairs": set(),
            "preview": [],
        }
        candidate_call_undirected_pairs = self._build_candidate_call_undirected_pairs(candidate_nodes) if include_call else set()
        candidate_merged_relation_edge_count = len(candidate_call_undirected_pairs | set(candidate_device_stats.get("unique_pairs", set())))

        score_map = self.build_priority_scores(
            phone_id=phone_id,
            candidate_nodes=candidate_nodes,
            analysis_mode=analysis_mode,
        )

        retained = self.retain_priority_subgraph(
            phone_id=phone_id,
            candidate_nodes=candidate_nodes,
            max_nodes=max_nodes,
            score_map=score_map,
            analysis_mode=analysis_mode,
        )
        kept_set = retained["kept_set"]

        retained_call_stats = self.call_edge_stats(kept_set, directed_call) if include_call else {
            "raw_record_count": 0,
            "unique_graph_edge_count": 0,
            "undirected_merged_edge_count": 0,
            "preview": [],
        }
        retained_device_stats = self.device_edge_stats(kept_set) if include_device else {
            "pair_incidence_count": 0,
            "unique_graph_edge_count": 0,
            "unique_pairs": set(),
            "preview": [],
        }
        retained_call_undirected_pairs = self._build_candidate_call_undirected_pairs(kept_set) if include_call else set()
        retained_merged_relation_edge_count = len(retained_call_undirected_pairs | set(retained_device_stats.get("unique_pairs", set())))

        suspicious_nodes = sorted(
            [v for v in score_map.values() if v["node"] in kept_set],
            key=lambda x: (-float(x["score"]), str(x["node"])),
        )[:top_k]

        # 补充 retained 图上的度，方便解释为什么它们被留下。
        retained_degree_lookup = {x["node"]: int(x["retained_local_degree"]) for x in retained["local_hubs"] + retained["bridge_nodes"]}
        for item in suspicious_nodes:
            item["retained_local_degree"] = int(retained["role_graph"].degree(item["node"])) if item["node"] in retained["role_graph"] else 0

        shared_devices = self.ctx.shared_device_details(phone_id)[:10]
        used_for_scoring = analysis_mode in {"mixed", "device_only"}
        drilldown_seeds = self.recommend_drilldown_seeds(suspicious_nodes)

        mode_zh = {
            "mixed": "混合关系",
            "call_only": "仅通话关系",
            "device_only": "仅共享设备关系",
        }.get(analysis_mode, analysis_mode)

        risk_summary_parts: List[str] = []
        if retained["truncated"]:
            risk_summary_parts.append(
                f"原始候选关系圈过大（{len(candidate_nodes)} 个节点），已按优先级保留 {retained['actual_num_nodes']} 个关键节点。"
            )
        if shared_devices:
            risk_summary_parts.append(
                f"中心号码关联 {len(shared_devices)} 条共享设备线索，其中最强设备可牵出 {shared_devices[0]['shared_phone_count']} 个关联号码。"
            )
        if suspicious_nodes:
            risk_summary_parts.append(
                f"当前最值得优先核查的节点是 {suspicious_nodes[0]['node_preview']}，综合可疑分为 {suspicious_nodes[0]['score']:.2f}。"
            )
        if not risk_summary_parts:
            risk_summary_parts.append("当前未发现特别突出的局部异常节点，但仍建议结合其他关系视角继续核查。")

        result = {
            "phone_profile": profile,
            "analysis_view": {
                "analysis_mode": analysis_mode,
                "analysis_mode_zh": mode_zh,
                "directed_call": directed_call,
                "hops": hops,
                "max_nodes": max_nodes,
                "top_k": top_k,
            },
            "call_relation_analysis": {
                "used_for_scoring": include_call,
                "directed_call": directed_call,
                "candidate_call_raw_record_count": int(candidate_call_stats["raw_record_count"]),
                "candidate_call_unique_graph_edge_count": int(candidate_call_stats["unique_graph_edge_count"]),
                "candidate_call_undirected_merged_edge_count": int(candidate_call_stats["undirected_merged_edge_count"]),
                "retained_call_raw_record_count": int(retained_call_stats["raw_record_count"]),
                "retained_call_unique_graph_edge_count": int(retained_call_stats["unique_graph_edge_count"]),
                "retained_call_undirected_merged_edge_count": int(retained_call_stats["undirected_merged_edge_count"]),
                "call_edges_preview": retained_call_stats["preview"],
            },
            "shared_device_analysis": {
                "device_count": len(self.ctx.phone_to_devices.get(phone_id, set())),
                "shared_device_count": len(shared_devices),
                "shared_devices_preview": shared_devices,
                "used_for_scoring": used_for_scoring,
                "candidate_device_pair_incidence_count": int(candidate_device_stats["pair_incidence_count"]),
                "candidate_device_unique_graph_edge_count": int(candidate_device_stats["unique_graph_edge_count"]),
                "retained_device_pair_incidence_count": int(retained_device_stats["pair_incidence_count"]),
                "retained_device_unique_graph_edge_count": int(retained_device_stats["unique_graph_edge_count"]),
            },
            "subgraph_analysis": {
                "center_node": phone_id,
                "candidate_num_nodes_before_truncation": len(candidate_nodes),
                "candidate_unique_relation_edge_count_before_truncation": int(candidate_merged_relation_edge_count),
                "candidate_call_edge_count_before_truncation": int(candidate_call_stats["raw_record_count"]),
                "candidate_device_edge_count_before_truncation": int(candidate_device_stats["pair_incidence_count"]),
                "actual_num_nodes": int(retained["actual_num_nodes"]),
                "actual_unique_relation_edge_count": int(retained_merged_relation_edge_count),
                "actual_call_edge_count": int(retained_call_stats["raw_record_count"]),
                "actual_device_edge_count": int(retained_device_stats["pair_incidence_count"]),
                "truncated": bool(retained["truncated"]),
                "kept_nodes_preview": [short_id(x) for x in retained["kept_nodes"][:20]],
            },
            "top_suspicious_nodes": suspicious_nodes,
            "key_roles": {
                "center_node": phone_id,
                "center_preview": short_id(phone_id),
                "local_hubs": retained["local_hubs"],
                "bridge_nodes": retained["bridge_nodes"],
            },
            "drilldown_seeds": drilldown_seeds,
            "human_summary": " ".join(risk_summary_parts),
            "investigation_next_steps": [
                "优先对 Top 可疑节点做单号码画像复查，确认其标签、对端规模和设备关系。",
                "对推荐下钻节点继续调用 association-path-analysis，判断其与其他可疑节点的路径关系。",
                "对推荐下钻节点继续调用 overlap-analysis，判断是否处于同一联系圈。",
                "如需更完整视图，可调大 max_nodes 或切换 analysis_mode 重新抽取。",
            ],
            "recommended_followups": [
                {
                    "skill": "association-path-analysis",
                    "reason": "适合继续分析中心号码与某个可疑节点之间的路径关系。",
                },
                {
                    "skill": "overlap-analysis",
                    "reason": "适合判断中心号码与某个邻居是否位于同一联系圈。",
                },
                {
                    "skill": "subgraph-extraction-analysis",
                    "reason": "适合围绕某个下钻节点继续抽取更小范围局部图。",
                },
            ],
        }

        output_dir.mkdir(parents=True, exist_ok=True)
        report_name = f"single_number_report_{phone_id[:8]}_{analysis_mode}_h{hops}.md"
        report_path = output_dir / report_name
        report_text = self.build_markdown_report(result)
        report_path.write_text(report_text, encoding="utf-8")

        payload = {
            "ok": True,
            "skill": "single-number-analysis",
            "query_type": "single_number_analysis",
            "input_summary": {
                "phone_id": phone_id,
                "hops": hops,
                "max_nodes": max_nodes,
                "top_k": top_k,
                "analysis_mode": analysis_mode,
                "directed_call": directed_call,
                "user_node_path": str(self.ctx.user_node_path),
                "call_graph_path": str(self.ctx.call_graph_path),
                "device_graph_path": str(self.ctx.device_graph_path),
            },
            "result": result,
            "notes": [
                "该技能定位为完整的单号码分析 skill，而不是单纯子图抽取子技能。",
                "当前版本已修正前端容易误走 subgraph-extraction-analysis 的说明问题。",
                "当前版本已统一边数统计口径，区分原始记录数、设备关系投影次数和图去重边数。",
                "当前版本已把报告结构改为单号码主分析，不再以子图摘要为叙事中心。",
                "若后续需要时间趋势分析，应继续补充时间维度基础算子。",
            ],
            "report_path": str(report_path),
            "report_exists": report_path.exists(),
            "artifacts": [
                {
                    "type": "markdown",
                    "path": str(report_path),
                    "title": report_name,
                }
            ],
        }
        return payload

    def build_markdown_report(self, result: Dict[str, object]) -> str:
        profile = result["phone_profile"]
        analysis_view = result["analysis_view"]
        call_analysis = result["call_relation_analysis"]
        device_analysis = result["shared_device_analysis"]
        subgraph = result["subgraph_analysis"]
        suspicious_nodes = result["top_suspicious_nodes"]
        key_roles = result["key_roles"]
        drilldown = result["drilldown_seeds"]
        next_steps = result["investigation_next_steps"]

        attrs = profile.get("raw_node_attrs", {}) or {}
        province = attrs.get("province") or "未知"
        label = attrs.get("label") if attrs.get("label") is not None else "未知"
        sub_label = attrs.get("sub_label") or "未知"

        lines: List[str] = []
        lines.append(f"# 单号码综合分析报告：{short_id(str(profile['phone_id']), 16)}")
        lines.append("")
        lines.append("## 1. 分析对象与模式")
        lines.append(f"- 号码ID：`{profile['phone_id']}`")
        lines.append(f"- 分析模式：{analysis_view['analysis_mode_zh']}")
        lines.append(f"- 通话方向设置：{'有向' if analysis_view['directed_call'] else '无向'}")
        lines.append(f"- 关系圈跳数：{analysis_view['hops']}")
        lines.append(f"- 最大保留节点数：{analysis_view['max_nodes']}")
        lines.append(f"- Top 可疑节点数：{analysis_view['top_k']}")
        lines.append("")

        lines.append("## 2. 号码画像")
        lines.append(f"- 是否命中画像：{profile['node_found']}")
        lines.append(f"- 省份：{province}")
        lines.append(f"- 标签：{label}")
        lines.append(f"- 子标签：{sub_label}")
        lines.append(f"- 通话记录数：{profile['call_record_count']}")
        lines.append(f"- 对端数量：{profile['counterparty_count']}")
        lines.append(f"- 设备数量：{profile['device_count']}")
        lines.append("")

        lines.append("## 3. 风险摘要")
        lines.append(f"- {result['human_summary']}")
        lines.append("")

        lines.append("## 4. 通话关系分析")
        lines.append(f"- 本轮是否参与打分：{'是' if call_analysis['used_for_scoring'] else '否'}")
        lines.append(f"- 候选范围内原始通话记录数：{call_analysis['candidate_call_raw_record_count']}")
        lines.append(f"- 候选范围内图去重通话边数：{call_analysis['candidate_call_unique_graph_edge_count']}")
        lines.append(f"- 保留范围内原始通话记录数：{call_analysis['retained_call_raw_record_count']}")
        lines.append(f"- 保留范围内图去重通话边数：{call_analysis['retained_call_unique_graph_edge_count']}")
        if call_analysis.get("call_edges_preview"):
            preview_text = "; ".join([f"{a} -> {b}" for a, b in call_analysis["call_edges_preview"][:10]])
            lines.append(f"- 通话边预览：{preview_text}")
        lines.append("")

        lines.append("## 5. 共享设备分析")
        lines.append(f"- 本轮是否参与打分：{'是' if device_analysis['used_for_scoring'] else '否'}")
        lines.append(f"- 中心号码设备总数：{device_analysis['device_count']}")
        lines.append(f"- 可形成共享设备线索的设备数：{device_analysis['shared_device_count']}")
        lines.append(f"- 候选范围内设备关系投影次数：{device_analysis['candidate_device_pair_incidence_count']}")
        lines.append(f"- 候选范围内图去重设备边数：{device_analysis['candidate_device_unique_graph_edge_count']}")
        lines.append(f"- 保留范围内设备关系投影次数：{device_analysis['retained_device_pair_incidence_count']}")
        lines.append(f"- 保留范围内图去重设备边数：{device_analysis['retained_device_unique_graph_edge_count']}")
        if device_analysis.get("shared_devices_preview"):
            lines.append("- 关键共享设备线索：")
            for item in device_analysis["shared_devices_preview"][:10]:
                lines.append(
                    f"  - 设备 `{item['device_preview']}` | 关联号码数={item['shared_phone_count']} | 共享号码预览={item['shared_phones_preview']}"
                )
        else:
            lines.append("- 当前没有形成可解释的共享设备线索。")
        lines.append("")

        lines.append("## 6. 关系圈规模与统计口径")
        lines.append(f"- 候选关系圈节点数（截断前）：{subgraph['candidate_num_nodes_before_truncation']}")
        lines.append(f"- 候选关系圈结构边数（用于展示）：{subgraph['candidate_unique_relation_edge_count_before_truncation']}")
        lines.append(f"- 实际保留节点数：{subgraph['actual_num_nodes']}")
        lines.append(f"- 实际保留结构边数（用于展示）：{subgraph['actual_unique_relation_edge_count']}")
        lines.append(f"- 是否截断：{subgraph['truncated']}")
        lines.append(f"- 保留节点预览：{', '.join(subgraph['kept_nodes_preview']) if subgraph['kept_nodes_preview'] else '无'}")
        lines.append("")
        lines.append("### 统计口径说明")
        lines.append("- 原始通话记录数：按原始通话边表逐条统计，重复通话会重复计数。")
        lines.append("- 设备关系投影次数：同一台设备关联多个号码时，会投影成号码对；不同设备可重复贡献多次。")
        lines.append("- 图去重边数：用于关系图展示和结构规模描述；同一对号码只算一条结构边。")
        lines.append("")

        lines.append("## 7. Top 可疑节点排名")
        if suspicious_nodes:
            for idx, item in enumerate(suspicious_nodes, start=1):
                lines.append(f"### {idx}. {item['node_preview']}")
                lines.append(f"- 综合可疑分：{item['score']:.2f}")
                lines.append(f"- 与中心号码直接通话：{'是' if item['direct_call_related'] else '否'}")
                lines.append(f"- 与中心号码共享设备：{'是' if item['shared_device_related'] else '否'}")
                lines.append(f"- 与中心号码通话频次：{item['call_frequency_with_center']}")
                lines.append(f"- 与中心号码共享设备数：{item['shared_device_count_with_center']}")
                lines.append(f"- 候选通话局部度：{item['candidate_call_degree']}")
                lines.append(f"- 候选设备局部度：{item['candidate_device_degree']}")
                lines.append(f"- 保留图局部度：{item['retained_local_degree']}")
                lines.append(f"- 主要原因：{'；'.join(item['reasons'])}")
        else:
            lines.append("- 当前未识别到明显可疑节点。")
        lines.append("")

        lines.append("## 8. 桥接点 / 枢纽点")
        lines.append("### 8.1 局部 Hub")
        for item in key_roles.get("local_hubs", []):
            lines.append(
                f"- `{item['node_preview']}` | 保留图局部度={item['retained_local_degree']} | betweenness={item['local_betweenness_centrality']} | 可疑分={item['suspicious_score']:.2f}"
            )
        lines.append("### 8.2 局部桥接点")
        for item in key_roles.get("bridge_nodes", []):
            lines.append(
                f"- `{item['node_preview']}` | betweenness={item['local_betweenness_centrality']} | 保留图局部度={item['retained_local_degree']} | 可疑分={item['suspicious_score']:.2f}"
            )
        lines.append("")

        lines.append("## 9. 推荐二次下钻路径")
        if drilldown:
            for item in drilldown:
                lines.append(f"### Seed {item['rank']}: {item['node_preview']}")
                lines.append(f"- 推荐分：{item['score']:.2f}")
                lines.append(f"- 推荐原因：{'；'.join(item['why_recommended'])}")
                lines.append(f"- 建议联动技能：{', '.join(item['followup_skills'])}")
        else:
            lines.append("- 暂无推荐二次下钻节点。")
        lines.append("")

        lines.append("## 10. 下一步调查建议")
        for step in next_steps:
            lines.append(f"- {step}")
        lines.append("")

        return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run single-number-analysis skill")
    parser.add_argument("--phone-id", required=True, help="target phone id")
    parser.add_argument("--hops", type=int, default=2, help="relation hops")
    parser.add_argument("--max-nodes", type=int, default=200, help="max retained nodes")
    parser.add_argument("--top-k", type=int, default=10, help="top suspicious nodes to show")
    parser.add_argument(
        "--analysis-mode",
        choices=["mixed", "call_only", "device_only"],
        default="mixed",
        help="relation view mode",
    )
    parser.add_argument(
        "--directed-call",
        action="store_true",
        help="treat call graph as directed when analysis_mode contains call relation",
    )
    parser.add_argument("--user-node-path", default=None)
    parser.add_argument("--call-graph-path", default=None)
    parser.add_argument("--device-graph-path", default=None)
    parser.add_argument("--dataset-root", default=None)
    parser.add_argument("--dataset", default="unified")
    parser.add_argument(
        "--artifact-mode",
        choices=["full", "essential", "markdown_only"],
        default="full",
    )
    parser.add_argument("--source-col", default="src_user_id")
    parser.add_argument("--target-col", default="dst_counterparty_id")
    parser.add_argument("--device-source-col", default="user_id")
    parser.add_argument("--device-target-col", default="imei")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        auto_user_node_path, auto_call_graph_path, auto_device_graph_path = resolve_dataset_paths(
            args.dataset_root,
            args.dataset,
        )

        user_node_path = Path(args.user_node_path) if args.user_node_path else auto_user_node_path
        call_graph_path = Path(args.call_graph_path) if args.call_graph_path else auto_call_graph_path
        device_graph_path = Path(args.device_graph_path) if args.device_graph_path else auto_device_graph_path

        ctx = PhoneDataContext(
            user_node_path=user_node_path,
            call_graph_path=call_graph_path,
            device_graph_path=device_graph_path,
            source_col=args.source_col,
            target_col=args.target_col,
            device_source_col=args.device_source_col,
            device_target_col=args.device_target_col,
        )
        analyzer = SingleNumberAnalyzer(ctx)
        result = analyzer.run(
            phone_id=args.phone_id,
            hops=args.hops,
            max_nodes=args.max_nodes,
            top_k=args.top_k,
            analysis_mode=args.analysis_mode,
            directed_call=args.directed_call,
            output_dir=Path(args.output_dir),
        )
        result.setdefault("input_summary", {}).update(
            {
                "dataset_root": str(Path(args.dataset_root).expanduser()) if args.dataset_root else None,
                "dataset": args.dataset,
                "artifact_mode": args.artifact_mode,
            }
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
    except Exception as exc:
        payload = {
            "ok": False,
            "skill": "single-number-analysis",
            "error": str(exc),
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        raise


if __name__ == "__main__":
    main()
