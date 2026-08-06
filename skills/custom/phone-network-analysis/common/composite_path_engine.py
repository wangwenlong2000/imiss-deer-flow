# -*- coding: utf-8 -*-

from __future__ import annotations

import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb
import networkx as nx

from yigraph_graph_algorithms import bridge_node_ranking


class CompositePathEngine:
    """
    面向电话网络数据的复合路径分析引擎

    当前联合三类关系：
    1. call（通话边）
    2. shared_device（共享设备）
    3. common_counterparty（共同对端）

    本版本重点修复：
    - 先做“直接复合关系短路”
    - 命中 direct composite edge 时不再进入 BFS
    - 给重查询加缓存，避免反复扫大表
    """

    def __init__(
        self,
        call_graph_path: str,
        device_graph_path: str,
        source_col: str = "src_user_id",
        target_col: str = "dst_counterparty_id",
        device_source_col: str = "user_id",
        device_target_col: str = "imei",
    ) -> None:
        self.call_graph_path = str(call_graph_path)
        self.device_graph_path = str(device_graph_path)
        self.source_col = source_col
        self.target_col = target_col
        self.device_source_col = device_source_col
        self.device_target_col = device_target_col

        self.conn = duckdb.connect(database=":memory:")
        self._setup_views()

        # 缓存
        self._pair_signal_cache: Dict[Tuple[str, str, int], Dict[str, Any]] = {}
        self._call_neighbor_cache: Dict[Tuple[str, bool, int], List[Dict[str, Any]]] = {}
        self._shared_device_neighbor_cache: Dict[Tuple[str, int], List[Dict[str, Any]]] = {}
        self._common_counterparty_neighbor_cache: Dict[Tuple[str, int, int], List[Dict[str, Any]]] = {}
        self._direct_call_count_cache: Dict[Tuple[str, str], int] = {}

    # ------------------------------------------------------------------
    # 初始化
    # ------------------------------------------------------------------
    def _setup_views(self) -> None:
        call_path = self.call_graph_path.replace("'", "''")
        device_path = self.device_graph_path.replace("'", "''")

        self.conn.execute("DROP VIEW IF EXISTS call_edges")
        self.conn.execute("DROP VIEW IF EXISTS device_edges")

        if self.call_graph_path.endswith(".csv"):
            self.conn.execute(
                f"""
                CREATE VIEW call_edges AS
                SELECT *
                FROM read_csv_auto(
                    '{call_path}',
                    HEADER=TRUE,
                    ALL_VARCHAR=TRUE
                )
                """
            )
        elif self.call_graph_path.endswith(".parquet"):
            self.conn.execute(
                f"""
                CREATE VIEW call_edges AS
                SELECT *
                FROM read_parquet('{call_path}')
                """
            )
        else:
            raise ValueError(f"不支持的通话图格式: {self.call_graph_path}")

        if self.device_graph_path.endswith(".csv"):
            self.conn.execute(
                f"""
                CREATE VIEW device_edges AS
                SELECT *
                FROM read_csv_auto(
                    '{device_path}',
                    HEADER=TRUE,
                    ALL_VARCHAR=TRUE
                )
                """
            )
        elif self.device_graph_path.endswith(".parquet"):
            self.conn.execute(
                f"""
                CREATE VIEW device_edges AS
                SELECT *
                FROM read_parquet('{device_path}')
                """
            )
        else:
            raise ValueError(f"不支持的设备图格式: {self.device_graph_path}")

    def _fetch_rows(self, sql: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
        params = params or []
        df = self.conn.execute(sql, params).df()
        if df.empty:
            return []
        return df.to_dict(orient="records")

    def _fetch_one_value(self, sql: str, params: Optional[List[Any]] = None, default: Any = None) -> Any:
        rows = self._fetch_rows(sql, params or [])
        if not rows:
            return default
        row = rows[0]
        if not row:
            return default
        return row[list(row.keys())[0]]

    # ------------------------------------------------------------------
    # 配对信号
    # ------------------------------------------------------------------
    def _direct_call_count(self, phone_a: str, phone_b: str) -> int:
        key = (phone_a, phone_b)
        if key in self._direct_call_count_cache:
            return self._direct_call_count_cache[key]

        sql = f"""
        SELECT COUNT(*) AS cnt
        FROM call_edges
        WHERE {self.source_col} = ?
          AND {self.target_col} = ?
        """
        value = int(self._fetch_one_value(sql, [phone_a, phone_b], default=0) or 0)
        self._direct_call_count_cache[key] = value
        return value

    def _get_shared_devices_between_pair(
        self,
        phone_a: str,
        phone_b: str,
        preview_limit: int = 10,
    ) -> Tuple[int, List[str]]:
        count_sql = f"""
        WITH a AS (
            SELECT DISTINCT {self.device_target_col} AS device_id
            FROM device_edges
            WHERE {self.device_source_col} = ?
              AND {self.device_target_col} IS NOT NULL
        ),
        b AS (
            SELECT DISTINCT {self.device_target_col} AS device_id
            FROM device_edges
            WHERE {self.device_source_col} = ?
              AND {self.device_target_col} IS NOT NULL
        )
        SELECT COUNT(*) AS cnt
        FROM (
            SELECT device_id FROM a
            INTERSECT
            SELECT device_id FROM b
        )
        """
        preview_sql = f"""
        WITH a AS (
            SELECT DISTINCT {self.device_target_col} AS device_id
            FROM device_edges
            WHERE {self.device_source_col} = ?
              AND {self.device_target_col} IS NOT NULL
        ),
        b AS (
            SELECT DISTINCT {self.device_target_col} AS device_id
            FROM device_edges
            WHERE {self.device_source_col} = ?
              AND {self.device_target_col} IS NOT NULL
        )
        SELECT device_id
        FROM (
            SELECT device_id FROM a
            INTERSECT
            SELECT device_id FROM b
        )
        LIMIT ?
        """
        count_value = int(self._fetch_one_value(count_sql, [phone_a, phone_b], default=0) or 0)
        rows = self._fetch_rows(preview_sql, [phone_a, phone_b, preview_limit])
        preview = [str(r["device_id"]) for r in rows]
        return count_value, preview

    def _get_common_counterparties_between_pair(
        self,
        phone_a: str,
        phone_b: str,
        preview_limit: int = 10,
    ) -> Tuple[int, List[str]]:
        count_sql = f"""
        WITH a AS (
            SELECT DISTINCT {self.target_col} AS cp
            FROM call_edges
            WHERE {self.source_col} = ?
              AND {self.target_col} IS NOT NULL
        ),
        b AS (
            SELECT DISTINCT {self.target_col} AS cp
            FROM call_edges
            WHERE {self.source_col} = ?
              AND {self.target_col} IS NOT NULL
        )
        SELECT COUNT(*) AS cnt
        FROM (
            SELECT cp FROM a
            INTERSECT
            SELECT cp FROM b
        )
        """
        preview_sql = f"""
        WITH a AS (
            SELECT DISTINCT {self.target_col} AS cp
            FROM call_edges
            WHERE {self.source_col} = ?
              AND {self.target_col} IS NOT NULL
        ),
        b AS (
            SELECT DISTINCT {self.target_col} AS cp
            FROM call_edges
            WHERE {self.source_col} = ?
              AND {self.target_col} IS NOT NULL
        )
        SELECT cp
        FROM (
            SELECT cp FROM a
            INTERSECT
            SELECT cp FROM b
        )
        LIMIT ?
        """
        count_value = int(self._fetch_one_value(count_sql, [phone_a, phone_b], default=0) or 0)
        rows = self._fetch_rows(preview_sql, [phone_a, phone_b, preview_limit])
        preview = [str(r["cp"]) for r in rows]
        return count_value, preview

    def get_pair_signals(
        self,
        phone_a: str,
        phone_b: str,
        preview_limit: int = 10,
    ) -> Dict[str, Any]:
        cache_key = (phone_a, phone_b, preview_limit)
        if cache_key in self._pair_signal_cache:
            return self._pair_signal_cache[cache_key]

        a_calls_b = self._direct_call_count(phone_a, phone_b) > 0
        b_calls_a = self._direct_call_count(phone_b, phone_a) > 0

        shared_device_count, shared_devices_preview = self._get_shared_devices_between_pair(
            phone_a, phone_b, preview_limit=preview_limit
        )
        common_counterparty_count, common_counterparties_preview = self._get_common_counterparties_between_pair(
            phone_a, phone_b, preview_limit=preview_limit
        )

        summary_parts: List[str] = []
        if a_calls_b:
            summary_parts.append("A直接呼叫B")
        if b_calls_a:
            summary_parts.append("B直接呼叫A")
        if shared_device_count > 0:
            summary_parts.append(f"共享设备 {shared_device_count} 个")
        if common_counterparty_count > 0:
            summary_parts.append(f"共同对端 {common_counterparty_count} 个")
        if not summary_parts:
            summary_parts.append("未发现直接配对信号")

        result = {
            "a_calls_b": a_calls_b,
            "b_calls_a": b_calls_a,
            "shared_device_count": shared_device_count,
            "shared_devices_preview": shared_devices_preview,
            "common_counterparty_count": common_counterparty_count,
            "common_counterparties_preview": common_counterparties_preview,
            "pair_signal_summary": "；".join(summary_parts) + "。",
        }
        self._pair_signal_cache[cache_key] = result
        return result

    # ------------------------------------------------------------------
    # 关系权重
    # ------------------------------------------------------------------
    def _relation_weight(self, relation: str) -> float:
        mapping = {
            "call": 5.0,
            "shared_device": 4.0,
            "common_counterparty": 2.0,
        }
        return mapping.get(relation, 1.0)

    def _score_path(self, path_steps: List[Dict[str, Any]]) -> float:
        if not path_steps:
            return 0.0

        total = 0.0
        for step in path_steps:
            relation = str(step.get("relation", ""))
            evidence_count = max(int(step.get("evidence_count") or 1), 1)
            total += self._relation_weight(relation) * math.log1p(evidence_count)

        total -= 0.3 * len(path_steps)
        return total

    def _build_candidate_payload(self, path_nodes: List[str], path_steps: List[Dict[str, Any]]) -> Dict[str, Any]:
        return {
            "path_nodes": path_nodes,
            "path_steps": path_steps,
            "path_length": len(path_steps),
            "relation_sequence": [str(s["relation"]) for s in path_steps],
            "score": self._score_path(path_steps),
        }

    def _sort_candidate_paths(self, candidate_paths: List[Dict[str, Any]], strategy: str = "balanced") -> List[Dict[str, Any]]:
        strategy = (strategy or "balanced").strip().lower()

        if strategy == "shortest":
            return sorted(candidate_paths, key=lambda x: (x["path_length"], -x["score"]))
        if strategy == "strongest":
            return sorted(candidate_paths, key=lambda x: (-x["score"], x["path_length"]))
        return sorted(candidate_paths, key=lambda x: (-x["score"], x["path_length"]))

    # ------------------------------------------------------------------
    # 直接复合关系短路（关键修复）
    # ------------------------------------------------------------------
    def _build_direct_pair_candidates(
        self,
        phone_a: str,
        phone_b: str,
        pair_signals: Dict[str, Any],
        directed_call: bool = True,
        min_common_counterparty: int = 2,
    ) -> List[Dict[str, Any]]:
        candidates: List[Dict[str, Any]] = []

        # call
        if directed_call:
            count_ab = self._direct_call_count(phone_a, phone_b)
            if count_ab > 0:
                step = {
                    "from": phone_a,
                    "to": phone_b,
                    "relation": "call",
                    "direction": "directed",
                    "evidence": None,
                    "evidence_count": count_ab,
                }
                candidates.append(self._build_candidate_payload([phone_a, phone_b], [step]))
        else:
            count_ab = self._direct_call_count(phone_a, phone_b)
            count_ba = self._direct_call_count(phone_b, phone_a)
            total = count_ab + count_ba
            if total > 0:
                step = {
                    "from": phone_a,
                    "to": phone_b,
                    "relation": "call",
                    "direction": "undirected",
                    "evidence": None,
                    "evidence_count": total,
                }
                candidates.append(self._build_candidate_payload([phone_a, phone_b], [step]))

        # shared_device
        if pair_signals.get("shared_device_count", 0) > 0:
            preview = pair_signals.get("shared_devices_preview", [])
            step = {
                "from": phone_a,
                "to": phone_b,
                "relation": "shared_device",
                "direction": "derived_undirected",
                "evidence": preview[0] if preview else None,
                "evidence_count": int(pair_signals["shared_device_count"]),
            }
            candidates.append(self._build_candidate_payload([phone_a, phone_b], [step]))

        # common_counterparty
        if pair_signals.get("common_counterparty_count", 0) >= min_common_counterparty:
            preview = pair_signals.get("common_counterparties_preview", [])
            step = {
                "from": phone_a,
                "to": phone_b,
                "relation": "common_counterparty",
                "direction": "derived_undirected",
                "evidence": preview[0] if preview else None,
                "evidence_count": int(pair_signals["common_counterparty_count"]),
            }
            candidates.append(self._build_candidate_payload([phone_a, phone_b], [step]))

        return candidates

    # ------------------------------------------------------------------
    # 邻居扩展
    # ------------------------------------------------------------------
    def _get_call_neighbors(
        self,
        phone: str,
        directed_call: bool = True,
        per_relation_limit: int = 20,
    ) -> List[Dict[str, Any]]:
        cache_key = (phone, directed_call, per_relation_limit)
        if cache_key in self._call_neighbor_cache:
            return self._call_neighbor_cache[cache_key]

        if directed_call:
            sql = f"""
            SELECT
                {self.target_col} AS neighbor,
                COUNT(*) AS evidence_count
            FROM call_edges
            WHERE {self.source_col} = ?
              AND {self.target_col} IS NOT NULL
            GROUP BY {self.target_col}
            ORDER BY evidence_count DESC
            LIMIT ?
            """
            rows = self._fetch_rows(sql, [phone, per_relation_limit])
            results = [
                {
                    "to": str(r["neighbor"]),
                    "relation": "call",
                    "direction": "directed",
                    "evidence": None,
                    "evidence_count": int(r["evidence_count"] or 1),
                }
                for r in rows
                if r["neighbor"] is not None
            ]
        else:
            sql = f"""
            WITH outgoing AS (
                SELECT {self.target_col} AS neighbor, COUNT(*) AS cnt
                FROM call_edges
                WHERE {self.source_col} = ?
                  AND {self.target_col} IS NOT NULL
                GROUP BY {self.target_col}
            ),
            incoming AS (
                SELECT {self.source_col} AS neighbor, COUNT(*) AS cnt
                FROM call_edges
                WHERE {self.target_col} = ?
                  AND {self.source_col} IS NOT NULL
                GROUP BY {self.source_col}
            ),
            merged AS (
                SELECT neighbor, SUM(cnt) AS evidence_count
                FROM (
                    SELECT * FROM outgoing
                    UNION ALL
                    SELECT * FROM incoming
                )
                GROUP BY neighbor
            )
            SELECT neighbor, evidence_count
            FROM merged
            ORDER BY evidence_count DESC
            LIMIT ?
            """
            rows = self._fetch_rows(sql, [phone, phone, per_relation_limit])
            results = [
                {
                    "to": str(r["neighbor"]),
                    "relation": "call",
                    "direction": "undirected",
                    "evidence": None,
                    "evidence_count": int(r["evidence_count"] or 1),
                }
                for r in rows
                if r["neighbor"] is not None
            ]

        self._call_neighbor_cache[cache_key] = results
        return results

    def _get_shared_device_neighbors(
        self,
        phone: str,
        per_relation_limit: int = 20,
    ) -> List[Dict[str, Any]]:
        cache_key = (phone, per_relation_limit)
        if cache_key in self._shared_device_neighbor_cache:
            return self._shared_device_neighbor_cache[cache_key]

        sql = f"""
        WITH my_devices AS (
            SELECT DISTINCT {self.device_target_col} AS device_id
            FROM device_edges
            WHERE {self.device_source_col} = ?
              AND {self.device_target_col} IS NOT NULL
        ),
        matched AS (
            SELECT
                {self.device_source_col} AS neighbor,
                COUNT(DISTINCT {self.device_target_col}) AS shared_device_count,
                MIN({self.device_target_col}) AS sample_device
            FROM device_edges
            WHERE {self.device_target_col} IN (SELECT device_id FROM my_devices)
              AND {self.device_source_col} <> ?
            GROUP BY {self.device_source_col}
            ORDER BY shared_device_count DESC
            LIMIT ?
        )
        SELECT *
        FROM matched
        """
        rows = self._fetch_rows(sql, [phone, phone, per_relation_limit])
        results = [
            {
                "to": str(r["neighbor"]),
                "relation": "shared_device",
                "direction": "derived_undirected",
                "evidence": r["sample_device"],
                "evidence_count": int(r["shared_device_count"] or 1),
            }
            for r in rows
            if r["neighbor"] is not None
        ]
        self._shared_device_neighbor_cache[cache_key] = results
        return results

    def _get_common_counterparty_neighbors(
        self,
        phone: str,
        min_common_counterparty: int = 2,
        per_relation_limit: int = 20,
    ) -> List[Dict[str, Any]]:
        cache_key = (phone, min_common_counterparty, per_relation_limit)
        if cache_key in self._common_counterparty_neighbor_cache:
            return self._common_counterparty_neighbor_cache[cache_key]

        sql = f"""
        WITH my_cp AS (
            SELECT DISTINCT {self.target_col} AS cp
            FROM call_edges
            WHERE {self.source_col} = ?
              AND {self.target_col} IS NOT NULL
        ),
        matched AS (
            SELECT
                {self.source_col} AS neighbor,
                COUNT(DISTINCT {self.target_col}) AS common_count,
                MIN({self.target_col}) AS sample_cp
            FROM call_edges
            WHERE {self.target_col} IN (SELECT cp FROM my_cp)
              AND {self.source_col} <> ?
            GROUP BY {self.source_col}
            HAVING COUNT(DISTINCT {self.target_col}) >= ?
            ORDER BY common_count DESC
            LIMIT ?
        )
        SELECT *
        FROM matched
        """
        rows = self._fetch_rows(sql, [phone, phone, min_common_counterparty, per_relation_limit])
        results = [
            {
                "to": str(r["neighbor"]),
                "relation": "common_counterparty",
                "direction": "derived_undirected",
                "evidence": r["sample_cp"],
                "evidence_count": int(r["common_count"] or 1),
            }
            for r in rows
            if r["neighbor"] is not None
        ]
        self._common_counterparty_neighbor_cache[cache_key] = results
        return results

    def _dedup_best_steps(self, steps: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        best: Dict[str, Dict[str, Any]] = {}
        for step in steps:
            node = str(step["to"])
            score = self._relation_weight(step["relation"]) * math.log1p(max(int(step["evidence_count"] or 1), 1))
            if node not in best:
                best[node] = step
            else:
                old = best[node]
                old_score = self._relation_weight(old["relation"]) * math.log1p(max(int(old["evidence_count"] or 1), 1))
                if score > old_score:
                    best[node] = step
        return list(best.values())

    def _expand_node(
        self,
        current_node: str,
        directed_call: bool = True,
        enable_call: bool = True,
        enable_shared_device: bool = True,
        enable_common_counterparty: bool = True,
        per_relation_limit: int = 20,
        min_common_counterparty: int = 2,
    ) -> List[Dict[str, Any]]:
        steps: List[Dict[str, Any]] = []

        if enable_call:
            steps.extend(self._get_call_neighbors(current_node, directed_call, per_relation_limit))
        if enable_shared_device:
            steps.extend(self._get_shared_device_neighbors(current_node, per_relation_limit))
        if enable_common_counterparty:
            steps.extend(self._get_common_counterparty_neighbors(current_node, min_common_counterparty, per_relation_limit))

        return self._dedup_best_steps(steps)

    # ------------------------------------------------------------------
    # 桥接排序与建议
    # ------------------------------------------------------------------
    def _build_bridge_ranking(self, candidate_paths: List[Dict[str, Any]], top_k: int = 10) -> List[Dict[str, Any]]:
        small_graph = nx.Graph()
        for item in candidate_paths:
            nodes = item.get("path_nodes", [])
            if len(nodes) < 2:
                continue
            for i in range(len(nodes) - 1):
                small_graph.add_edge(nodes[i], nodes[i + 1])

        if small_graph.number_of_nodes() == 0:
            return []

        return bridge_node_ranking(
            graph=small_graph,
            candidate_paths=candidate_paths,
            local_radius=2,
            max_local_nodes=300,
            top_k=top_k,
        )

    def _build_investigation_next_steps(
        self,
        pair_signals: Dict[str, Any],
        best_path: Optional[Dict[str, Any]],
        bridge_ranking: List[Dict[str, Any]],
    ) -> List[str]:
        steps: List[str] = []

        if best_path and best_path.get("path_length", 0) >= 2:
            steps.append("对桥接号码做 query_phone_node，查看其画像、标签、通话量和设备情况。")
            steps.append("围绕桥接号码做 1~2 跳 subgraph_extract，检查是否形成局部高密度关系圈。")

        if pair_signals.get("common_counterparty_count", 0) > 0:
            steps.append("继续核查共同对端号码，判断这些共同对端是否为关键中介节点或批量接触目标。")

        if pair_signals.get("shared_device_count", 0) > 0:
            steps.append("继续核查共享设备对应的其它号码，判断是否存在设备复用或团伙共用设备。")

        if bridge_ranking:
            steps.append("优先排查桥接点排名靠前的节点，这些节点更可能是局部网络中的关键通道。")

        if not steps:
            steps.append("当前缺少明显强信号，建议先分别做单号码画像和局部子图分析。")

        return steps

    # ------------------------------------------------------------------
    # 主函数：复合路径
    # ------------------------------------------------------------------
    def find_composite_path(
        self,
        source_phone: str,
        target_phone: str,
        max_hops: int = 3,
        directed_call: bool = True,
        enable_call: bool = True,
        enable_shared_device: bool = True,
        enable_common_counterparty: bool = True,
        per_relation_limit: int = 20,
        max_expand_nodes: int = 500,
        top_k: int = 1,
        strategy: str = "balanced",
        min_common_counterparty: int = 2,
    ) -> Dict[str, Any]:
        if source_phone == target_phone:
            best_path = self._build_candidate_payload([source_phone], [])
            return {
                "path_found": True,
                "candidate_paths": [best_path],
                "best_path": best_path,
                "path_nodes": [source_phone],
                "path_steps": [],
                "path_length": 0,
                "relation_sequence": [],
                "searched_nodes": 1,
                "truncated": False,
                "bridge_node_ranking": [],
                "human_summary": "起点和终点是同一个号码，无需搜索路径。",
                "investigation_next_steps": ["无需做关联路径分析，可直接做单号码画像分析。"],
            }

        # 关键修复：先看直接复合关系
        pair_signals = self.get_pair_signals(source_phone, target_phone, preview_limit=10)
        direct_candidates = self._build_direct_pair_candidates(
            phone_a=source_phone,
            phone_b=target_phone,
            pair_signals=pair_signals,
            directed_call=directed_call,
            min_common_counterparty=min_common_counterparty,
        )

        if direct_candidates:
            direct_candidates = self._sort_candidate_paths(direct_candidates, strategy=strategy)
            direct_candidates = direct_candidates[:top_k]
            best_path = direct_candidates[0]

            return {
                "path_found": True,
                "candidate_paths": direct_candidates,
                "best_path": best_path,
                "path_nodes": best_path["path_nodes"],
                "path_steps": best_path["path_steps"],
                "path_length": best_path["path_length"],
                "relation_sequence": best_path["relation_sequence"],
                "searched_nodes": 1,
                "truncated": False,
                "bridge_node_ranking": [],
                "human_summary": f"在复合关系图里，两个号码之间存在直接关系边：{' -> '.join(best_path['relation_sequence'])}。",
                "investigation_next_steps": self._build_investigation_next_steps(
                    pair_signals=pair_signals,
                    best_path=best_path,
                    bridge_ranking=[],
                ),
            }

        # 若没有直接关系，再进入 BFS
        from collections import deque

        queue = deque()
        queue.append((source_phone, [source_phone], []))

        best_depth: Dict[str, int] = {source_phone: 0}
        candidate_paths: List[Dict[str, Any]] = []
        seen_signatures = set()

        searched_nodes = 0
        truncated = False

        while queue:
            current_node, path_nodes, path_steps = queue.popleft()
            current_depth = len(path_steps)

            if current_depth >= max_hops:
                continue

            searched_nodes += 1
            if searched_nodes > max_expand_nodes:
                truncated = True
                break

            next_steps = self._expand_node(
                current_node=current_node,
                directed_call=directed_call,
                enable_call=enable_call,
                enable_shared_device=enable_shared_device,
                enable_common_counterparty=enable_common_counterparty,
                per_relation_limit=per_relation_limit,
                min_common_counterparty=min_common_counterparty,
            )

            for step in next_steps:
                next_node = str(step["to"])
                if next_node in path_nodes:
                    continue

                new_nodes = path_nodes + [next_node]
                new_steps = path_steps + [step]

                if next_node == target_phone:
                    payload = self._build_candidate_payload(new_nodes, new_steps)
                    sig = tuple(payload["path_nodes"])
                    if sig not in seen_signatures:
                        seen_signatures.add(sig)
                        candidate_paths.append(payload)
                    continue

                old_depth = best_depth.get(next_node)
                new_depth = len(new_steps)
                if old_depth is None or new_depth <= old_depth:
                    best_depth[next_node] = new_depth
                    queue.append((next_node, new_nodes, new_steps))

        if not candidate_paths:
            return {
                "path_found": False,
                "candidate_paths": [],
                "best_path": None,
                "path_nodes": [],
                "path_steps": [],
                "path_length": None,
                "relation_sequence": [],
                "searched_nodes": searched_nodes,
                "truncated": truncated,
                "bridge_node_ranking": [],
                "human_summary": "在复合关系图里，没有找到满足条件的路径。",
                "investigation_next_steps": [
                    "建议改成无向通话视角再试一次。",
                    "建议继续做单号码画像和局部子图分析。"
                ],
            }

        candidate_paths = self._sort_candidate_paths(candidate_paths, strategy=strategy)
        candidate_paths = candidate_paths[:top_k]
        best_path = candidate_paths[0]
        bridge_rank = self._build_bridge_ranking(candidate_paths, top_k=10)

        relation_chain = " -> ".join(best_path.get("relation_sequence", [])) or "无"
        human_summary = (
            f"在复合关系图里，找到了一条长度为 {best_path['path_length']} 的混合关系路径。"
            f"这条路径的关系序列是：{relation_chain}。"
        )

        return {
            "path_found": True,
            "candidate_paths": candidate_paths,
            "best_path": best_path,
            "path_nodes": best_path["path_nodes"],
            "path_steps": best_path["path_steps"],
            "path_length": best_path["path_length"],
            "relation_sequence": best_path["relation_sequence"],
            "searched_nodes": searched_nodes,
            "truncated": truncated,
            "bridge_node_ranking": bridge_rank,
            "human_summary": human_summary,
            "investigation_next_steps": self._build_investigation_next_steps(
                pair_signals=pair_signals,
                best_path=best_path,
                bridge_ranking=bridge_rank,
            ),
        }


if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser(description="CompositePathEngine standalone test entry")

    parser.add_argument("--phone-a", required=True, help="号码 A")
    parser.add_argument("--phone-b", required=True, help="号码 B")

    parser.add_argument(
        "--call-graph-path",
        default="/workspace/imiss-deer-flow-main/datasets/phone-network/processed/unified/call_edges.csv",
        help="通话边文件路径",
    )
    parser.add_argument(
        "--device-graph-path",
        default="/workspace/imiss-deer-flow-main/datasets/phone-network/processed/graph_views/unified/edges_phone_imei.parquet",
        help="号码-设备关系文件路径",
    )

    parser.add_argument("--source-col", default="src_user_id", help="通话图源列")
    parser.add_argument("--target-col", default="dst_counterparty_id", help="通话图目标列")
    parser.add_argument("--device-source-col", default="user_id", help="设备图源列")
    parser.add_argument("--device-target-col", default="imei", help="设备图目标列")

    parser.add_argument("--max-hops", type=int, default=3, help="最大跳数")
    parser.add_argument("--per-relation-limit", type=int, default=20, help="每种关系的扩展邻居数上限")
    parser.add_argument("--max-expand-nodes", type=int, default=500, help="最大展开节点数")
    parser.add_argument("--top-k", type=int, default=1, help="返回候选路径数")
    parser.add_argument(
        "--strategy",
        choices=["shortest", "strongest", "balanced"],
        default="balanced",
        help="路径排序策略",
    )
    parser.add_argument("--min-common-counterparty", type=int, default=2, help="共同对端关系生效阈值")

    parser.add_argument("--undirected-call", action="store_true", help="把通话关系按无向处理")
    parser.add_argument("--disable-call", action="store_true", help="禁用通话关系")
    parser.add_argument("--disable-shared-device", action="store_true", help="禁用共享设备关系")
    parser.add_argument("--disable-common-counterparty", action="store_true", help="禁用共同对端关系")

    args = parser.parse_args()

    directed_call = not args.undirected_call

    engine = CompositePathEngine(
        call_graph_path=args.call_graph_path,
        device_graph_path=args.device_graph_path,
        source_col=args.source_col,
        target_col=args.target_col,
        device_source_col=args.device_source_col,
        device_target_col=args.device_target_col,
    )

    pair_signals = engine.get_pair_signals(
        phone_a=args.phone_a,
        phone_b=args.phone_b,
        preview_limit=10,
    )

    composite_result = engine.find_composite_path(
        source_phone=args.phone_a,
        target_phone=args.phone_b,
        max_hops=args.max_hops,
        directed_call=directed_call,
        enable_call=not args.disable_call,
        enable_shared_device=not args.disable_shared_device,
        enable_common_counterparty=not args.disable_common_counterparty,
        per_relation_limit=args.per_relation_limit,
        max_expand_nodes=args.max_expand_nodes,
        top_k=args.top_k,
        strategy=args.strategy,
        min_common_counterparty=args.min_common_counterparty,
    )

    output = {
        "pair_signals": pair_signals,
        "composite_result": composite_result,
    }

    print(json.dumps(output, ensure_ascii=False, indent=2))