#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from collections import deque
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb


CURRENT_FILE = Path(__file__).resolve()


def _unique_paths(items: List[Any]) -> List[Path]:
    result = []
    seen = set()
    for item in items:
        if not item:
            continue
        p = Path(item).expanduser()
        s = str(p)
        if s in seen:
            continue
        seen.add(s)
        result.append(p)
    return result


def resolve_call_graph_path(user_path: Optional[str] = None) -> str:
    candidates = _unique_paths([
        user_path,
        os.environ.get("PHONE_NETWORK_CALL_GRAPH"),
        "/mnt/datasets/phone-network/processed/unified/call_edges.csv",
        CURRENT_FILE.parents[3] / "datasets/phone-network/processed/unified/call_edges.csv",
        "/workspace/imiss-deer-flow-main/datasets/phone-network/processed/unified/call_edges.csv",
    ])
    for p in candidates:
        if p.exists():
            return str(p)
    return "/mnt/datasets/phone-network/processed/unified/call_edges.csv"


def resolve_device_graph_path(user_path: Optional[str] = None) -> str:
    candidates = _unique_paths([
        user_path,
        os.environ.get("PHONE_NETWORK_DEVICE_GRAPH"),
        "/mnt/datasets/phone-network/processed/graph_views/unified/edges_phone_imei.parquet",
        CURRENT_FILE.parents[3] / "datasets/phone-network/processed/graph_views/unified/edges_phone_imei.parquet",
        "/workspace/imiss-deer-flow-main/datasets/phone-network/processed/graph_views/unified/edges_phone_imei.parquet",
    ])
    for p in candidates:
        if p.exists():
            return str(p)
    return "/mnt/datasets/phone-network/processed/graph_views/unified/edges_phone_imei.parquet"


def _escape_sql_path(path: str) -> str:
    return path.replace("'", "''")


class CompositePathEngine:
    """
    用 DuckDB 按需查询电话网络数据，构建“关系投影图”的局部邻接，
    从而支持：
    1. directed call path
    2. shared_device relation
    3. common_counterparty relation
    的复合路径搜索。
    """

    def __init__(
        self,
        call_graph_path: Optional[str] = None,
        device_graph_path: Optional[str] = None,
        per_relation_limit: int = 100,
    ):
        self.call_graph_path = resolve_call_graph_path(call_graph_path)
        self.device_graph_path = resolve_device_graph_path(device_graph_path)
        self.per_relation_limit = per_relation_limit
        self.con = duckdb.connect(database=":memory:")
        self._init_views()

    def _init_views(self):
        call_path = _escape_sql_path(self.call_graph_path)
        device_path = _escape_sql_path(self.device_graph_path)

        self.con.execute(f"""
            CREATE OR REPLACE VIEW call_edges AS
            SELECT
                CAST(src_user_id AS VARCHAR) AS src_user_id,
                CAST(dst_counterparty_id AS VARCHAR) AS dst_counterparty_id
            FROM read_csv_auto('{call_path}', all_varchar=true, ignore_errors=true)
            WHERE src_user_id IS NOT NULL
              AND dst_counterparty_id IS NOT NULL
              AND src_user_id <> ''
              AND dst_counterparty_id <> '';
        """)

        self.con.execute(f"""
            CREATE OR REPLACE VIEW device_edges AS
            SELECT
                CAST(user_id AS VARCHAR) AS user_id,
                CAST(imei AS VARCHAR) AS imei
            FROM read_parquet('{device_path}')
            WHERE user_id IS NOT NULL
              AND imei IS NOT NULL
              AND user_id <> ''
              AND imei <> '';
        """)

    def get_pair_signals(
        self,
        phone_a: str,
        phone_b: str,
        preview_limit: int = 10,
    ) -> Dict[str, Any]:
        a_calls_b = self.con.execute("""
            SELECT COUNT(*) > 0
            FROM call_edges
            WHERE src_user_id = ? AND dst_counterparty_id = ?
        """, [phone_a, phone_b]).fetchone()[0]

        b_calls_a = self.con.execute("""
            SELECT COUNT(*) > 0
            FROM call_edges
            WHERE src_user_id = ? AND dst_counterparty_id = ?
        """, [phone_b, phone_a]).fetchone()[0]

        shared_devices = self.con.execute("""
            WITH a_dev AS (
                SELECT DISTINCT imei
                FROM device_edges
                WHERE user_id = ?
            ),
            b_dev AS (
                SELECT DISTINCT imei
                FROM device_edges
                WHERE user_id = ?
            )
            SELECT imei
            FROM a_dev
            INTERSECT
            SELECT imei
            FROM b_dev
            LIMIT ?
        """, [phone_a, phone_b, preview_limit]).fetchall()

        shared_device_count = self.con.execute("""
            WITH a_dev AS (
                SELECT DISTINCT imei
                FROM device_edges
                WHERE user_id = ?
            ),
            b_dev AS (
                SELECT DISTINCT imei
                FROM device_edges
                WHERE user_id = ?
            )
            SELECT COUNT(*)
            FROM (
                SELECT imei
                FROM a_dev
                INTERSECT
                SELECT imei
                FROM b_dev
            )
        """, [phone_a, phone_b]).fetchone()[0]

        common_counterparties = self.con.execute("""
            WITH a_cp AS (
                SELECT DISTINCT dst_counterparty_id AS cp
                FROM call_edges
                WHERE src_user_id = ?
            ),
            b_cp AS (
                SELECT DISTINCT dst_counterparty_id AS cp
                FROM call_edges
                WHERE src_user_id = ?
            )
            SELECT cp
            FROM a_cp
            INTERSECT
            SELECT cp
            FROM b_cp
            LIMIT ?
        """, [phone_a, phone_b, preview_limit]).fetchall()

        common_counterparty_count = self.con.execute("""
            WITH a_cp AS (
                SELECT DISTINCT dst_counterparty_id AS cp
                FROM call_edges
                WHERE src_user_id = ?
            ),
            b_cp AS (
                SELECT DISTINCT dst_counterparty_id AS cp
                FROM call_edges
                WHERE src_user_id = ?
            )
            SELECT COUNT(*)
            FROM (
                SELECT cp
                FROM a_cp
                INTERSECT
                SELECT cp
                FROM b_cp
            )
        """, [phone_a, phone_b]).fetchone()[0]

        return {
            "a_calls_b": bool(a_calls_b),
            "b_calls_a": bool(b_calls_a),
            "shared_device_count": int(shared_device_count),
            "shared_devices_preview": [x[0] for x in shared_devices],
            "common_counterparty_count": int(common_counterparty_count),
            "common_counterparties_preview": [x[0] for x in common_counterparties],
        }

    def _call_neighbors(self, node: str, directed_call: bool = True) -> List[Dict[str, Any]]:
        rows = self.con.execute("""
            SELECT DISTINCT dst_counterparty_id
            FROM call_edges
            WHERE src_user_id = ?
            LIMIT ?
        """, [node, self.per_relation_limit]).fetchall()

        neighbors = []
        for row in rows:
            nbr = row[0]
            neighbors.append({
                "neighbor": nbr,
                "relation": "call",
                "direction": "out",
                "evidence": None,
                "evidence_count": 1,
            })

        if not directed_call:
            rows_in = self.con.execute("""
                SELECT DISTINCT src_user_id
                FROM call_edges
                WHERE dst_counterparty_id = ?
                LIMIT ?
            """, [node, self.per_relation_limit]).fetchall()

            for row in rows_in:
                nbr = row[0]
                neighbors.append({
                    "neighbor": nbr,
                    "relation": "call",
                    "direction": "in",
                    "evidence": None,
                    "evidence_count": 1,
                })

        return neighbors

    def _shared_device_neighbors(self, node: str) -> List[Dict[str, Any]]:
        rows = self.con.execute("""
            WITH my_devices AS (
                SELECT DISTINCT imei
                FROM device_edges
                WHERE user_id = ?
            )
            SELECT
                user_id AS neighbor,
                MIN(imei) AS one_device,
                COUNT(DISTINCT imei) AS shared_device_count
            FROM device_edges
            WHERE imei IN (SELECT imei FROM my_devices)
              AND user_id <> ?
            GROUP BY user_id
            ORDER BY shared_device_count DESC, neighbor
            LIMIT ?
        """, [node, node, self.per_relation_limit]).fetchall()

        neighbors = []
        for row in rows:
            neighbors.append({
                "neighbor": row[0],
                "relation": "shared_device",
                "direction": "derived_undirected",
                "evidence": row[1],
                "evidence_count": int(row[2]),
            })
        return neighbors

    def _common_counterparty_neighbors(self, node: str) -> List[Dict[str, Any]]:
        rows = self.con.execute("""
            WITH my_cp AS (
                SELECT DISTINCT dst_counterparty_id AS cp
                FROM call_edges
                WHERE src_user_id = ?
            )
            SELECT
                src_user_id AS neighbor,
                MIN(dst_counterparty_id) AS one_counterparty,
                COUNT(DISTINCT dst_counterparty_id) AS shared_counterparty_count
            FROM call_edges
            WHERE dst_counterparty_id IN (SELECT cp FROM my_cp)
              AND src_user_id <> ?
            GROUP BY src_user_id
            ORDER BY shared_counterparty_count DESC, neighbor
            LIMIT ?
        """, [node, node, self.per_relation_limit]).fetchall()

        neighbors = []
        for row in rows:
            neighbors.append({
                "neighbor": row[0],
                "relation": "common_counterparty",
                "direction": "derived_undirected",
                "evidence": row[1],
                "evidence_count": int(row[2]),
            })
        return neighbors

    def get_composite_neighbors(
        self,
        node: str,
        directed_call: bool = True,
        enable_call: bool = True,
        enable_shared_device: bool = True,
        enable_common_counterparty: bool = True,
    ) -> List[Dict[str, Any]]:
        neighbors = []

        # 关系优先级：先直接通话，再共享设备，再共同对端
        if enable_call:
            neighbors.extend(self._call_neighbors(node, directed_call=directed_call))
        if enable_shared_device:
            neighbors.extend(self._shared_device_neighbors(node))
        if enable_common_counterparty:
            neighbors.extend(self._common_counterparty_neighbors(node))

        dedup = {}
        for item in neighbors:
            key = (item["neighbor"], item["relation"], item["direction"])
            if key not in dedup:
                dedup[key] = item
            else:
                if item["evidence_count"] > dedup[key]["evidence_count"]:
                    dedup[key] = item

        return list(dedup.values())

    def find_composite_path(
        self,
        source: str,
        target: str,
        max_hops: int = 4,
        directed_call: bool = True,
        enable_call: bool = True,
        enable_shared_device: bool = True,
        enable_common_counterparty: bool = True,
        max_expand_nodes: int = 5000,
    ) -> Dict[str, Any]:
        if source == target:
            return {
                "path_found": True,
                "path_nodes": [source],
                "path_steps": [],
                "path_length": 0,
                "relation_sequence": [],
                "searched_nodes": 1,
            }

        queue = deque()
        queue.append({
            "node": source,
            "path_nodes": [source],
            "path_steps": [],
        })

        best_depth = {source: 0}
        expanded = 0

        while queue:
            state = queue.popleft()
            current = state["node"]
            depth = len(state["path_steps"])

            if depth >= max_hops:
                continue

            if expanded >= max_expand_nodes:
                break

            expanded += 1

            neighbors = self.get_composite_neighbors(
                current,
                directed_call=directed_call,
                enable_call=enable_call,
                enable_shared_device=enable_shared_device,
                enable_common_counterparty=enable_common_counterparty,
            )

            for nbr in neighbors:
                next_node = nbr["neighbor"]
                next_depth = depth + 1

                if next_node in state["path_nodes"]:
                    continue

                step = {
                    "from": current,
                    "to": next_node,
                    "relation": nbr["relation"],
                    "direction": nbr["direction"],
                    "evidence": nbr["evidence"],
                    "evidence_count": nbr["evidence_count"],
                }

                next_path_nodes = state["path_nodes"] + [next_node]
                next_path_steps = state["path_steps"] + [step]

                if next_node == target:
                    return {
                        "path_found": True,
                        "path_nodes": next_path_nodes,
                        "path_steps": next_path_steps,
                        "path_length": len(next_path_steps),
                        "relation_sequence": [x["relation"] for x in next_path_steps],
                        "searched_nodes": expanded,
                    }

                if next_node not in best_depth or next_depth < best_depth[next_node]:
                    best_depth[next_node] = next_depth
                    queue.append({
                        "node": next_node,
                        "path_nodes": next_path_nodes,
                        "path_steps": next_path_steps,
                    })

        return {
            "path_found": False,
            "path_nodes": [],
            "path_steps": [],
            "path_length": None,
            "relation_sequence": [],
            "searched_nodes": expanded,
        }
