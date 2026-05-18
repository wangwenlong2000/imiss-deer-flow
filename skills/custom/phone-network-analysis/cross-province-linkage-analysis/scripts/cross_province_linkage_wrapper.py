#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
cross-province-linkage-analysis

Identify cross-province linkage evidence in unified phone-network data:
shared devices, common counterparties, direct cross-province calls, strong
cross-province object pairs, and representative linkage paths.
"""

from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb
import pandas as pd

SCRIPT_VERSION = "cross-province-linkage-analysis-release-v1.0"
DEFAULT_DATASET = "unified"
DEFAULT_PROVINCE_A = "sichuan"
DEFAULT_PROVINCE_B = "shaanxi"


def preview_id(value: Any, n: int = 12) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    s = str(value)
    return s if len(s) <= n else s[:n] + "..."


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or pd.isna(value):
            return default
        return int(value)
    except Exception:
        return default


def safe_float(value: Any, default: float = 0.0, ndigits: int = 6) -> float:
    try:
        if value is None or pd.isna(value):
            return default
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return default
        return round(f, ndigits)
    except Exception:
        return default


def pct(n: Any, d: Any, ndigits: int = 4) -> float:
    n = safe_float(n, 0.0, 10)
    d = safe_float(d, 0.0, 10)
    if d == 0:
        return 0.0
    return round(n / d, ndigits)


def json_safe(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {str(k): json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [json_safe(v) for v in obj]
    if isinstance(obj, tuple):
        return [json_safe(v) for v in obj]
    if isinstance(obj, pd.DataFrame):
        return json_safe(obj.to_dict(orient="records"))
    if isinstance(obj, pd.Series):
        return json_safe(obj.to_dict())
    if isinstance(obj, pd.Timestamp):
        return obj.strftime("%Y-%m-%d %H:%M:%S")
    try:
        if pd.isna(obj):
            return None
    except Exception:
        pass
    if hasattr(obj, "item"):
        try:
            return obj.item()
        except Exception:
            pass
    return obj


def dataframe_records(df: pd.DataFrame, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    if df is None or df.empty:
        return []
    if limit is not None:
        df = df.head(limit)
    return json_safe(df.to_dict(orient="records"))


def ensure_output_dir(path: str) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def find_dataset_root(user_root: Optional[str]) -> Path:
    candidates: List[Path] = []
    if user_root:
        candidates.append(Path(user_root))
    env_root = os.environ.get("PHONE_NETWORK_DATASET_ROOT")
    if env_root:
        candidates.append(Path(env_root))
    candidates.extend([
        Path("/workspace/imiss-deer-flow-main/datasets/phone-network"),
        Path("/mnt/datasets/phone-network"),
        Path("/mnt/data/phone-network"),
    ])
    for c in candidates:
        if c.exists():
            return c
    return candidates[0]


def resolve_paths(dataset_root: Path, dataset: str) -> Dict[str, Optional[Path]]:
    processed = dataset_root / "processed"
    user_nodes = processed / dataset / "user_nodes.csv"
    call_edges = processed / dataset / "call_edges.csv"
    device_parquet = processed / "graph_views" / dataset / "edges_phone_imei.parquet"
    device_csv = processed / "graph_views" / dataset / "edges_phone_imei.csv"
    return {
        "user_nodes": user_nodes if user_nodes.exists() else None,
        "call_edges": call_edges if call_edges.exists() else None,
        "device_edges": device_parquet if device_parquet.exists() else (device_csv if device_csv.exists() else None),
    }


def sql_path(path: Path) -> str:
    return str(path).replace("'", "''")


def relation_expr(path: Path) -> str:
    p = sql_path(path)
    if path.suffix.lower() == ".parquet":
        return f"read_parquet('{p}')"
    return f"read_csv_auto('{p}', ignore_errors=true)"


def get_columns_for_file(conn: duckdb.DuckDBPyConnection, path: Path) -> List[str]:
    p = sql_path(path)
    if path.suffix.lower() == ".parquet":
        q = f"DESCRIBE SELECT * FROM read_parquet('{p}')"
    else:
        q = f"DESCRIBE SELECT * FROM read_csv_auto('{p}', ignore_errors=true)"
    return [str(x) for x in conn.execute(q).fetchdf()["column_name"].tolist()]


def choose_col(columns: List[str], candidates: List[str]) -> Optional[str]:
    lower_to_orig = {c.lower(): c for c in columns}
    for cand in candidates:
        if cand.lower() in lower_to_orig:
            return lower_to_orig[cand.lower()]
    return None


def setup_views(conn: duckdb.DuckDBPyConnection, paths: Dict[str, Optional[Path]]) -> Dict[str, Any]:
    meta: Dict[str, Any] = {"missing": [], "warnings": [], "columns": {}}
    if not paths.get("user_nodes"):
        meta["missing"].append("user_nodes")
        return meta
    if not paths.get("call_edges"):
        meta["missing"].append("call_edges")
    if not paths.get("device_edges"):
        meta["warnings"].append("device_edges missing; cross-province shared device evidence unavailable")

    node_cols = get_columns_for_file(conn, paths["user_nodes"])
    user_col = choose_col(node_cols, ["user_id", "phone_id", "phone", "src_user_id"])
    province_col = choose_col(node_cols, ["province", "prov", "area", "region"])
    label_col = choose_col(node_cols, ["label", "risk_label", "is_risk"])
    sub_label_col = choose_col(node_cols, ["sub_label", "sublabel", "type", "category"])
    meta["columns"]["nodes"] = {
        "user_col": user_col,
        "province_col": province_col,
        "label_col": label_col,
        "sub_label_col": sub_label_col,
    }
    if not user_col:
        meta["missing"].append("node_id_column")
        return meta
    if not province_col:
        meta["missing"].append("province_column")
        return meta

    n_path = sql_path(paths["user_nodes"])
    label_expr = f"CAST(COALESCE(CAST({label_col} AS VARCHAR), 'unknown') AS VARCHAR)" if label_col else "'unknown'"
    sub_expr = f"LOWER(COALESCE(CAST({sub_label_col} AS VARCHAR), 'unknown'))" if sub_label_col else "'unknown'"
    conn.execute(f"""
        CREATE OR REPLACE VIEW nodes AS
        SELECT
            CAST({user_col} AS VARCHAR) AS user_id,
            LOWER(COALESCE(CAST({province_col} AS VARCHAR), 'unknown')) AS province,
            {label_expr} AS label,
            {sub_expr} AS sub_label,
            CASE
                WHEN TRY_CAST({label_expr} AS INTEGER) = 1 OR LOWER({sub_expr}) IN ('risk','purefraud','fraud','mutation') THEN 1
                ELSE 0
            END AS is_risk
        FROM read_csv_auto('{n_path}', ignore_errors=true)
        WHERE {user_col} IS NOT NULL
    """)

    if paths.get("call_edges"):
        call_cols = get_columns_for_file(conn, paths["call_edges"])
        src_col = choose_col(call_cols, ["src_user_id", "source", "src", "caller", "user_id", "phone_id"])
        dst_col = choose_col(call_cols, ["dst_counterparty_id", "target", "dst", "callee", "counterparty_id", "peer_id"])
        time_col = choose_col(call_cols, ["event_time", "call_time", "start_time", "timestamp", "time"])
        hour_col = choose_col(call_cols, ["event_hour", "hour", "call_hour"])
        weight_col = choose_col(call_cols, ["call_count", "weight", "record_count", "cnt"])
        meta["columns"]["calls"] = {
            "src_col": src_col,
            "dst_col": dst_col,
            "time_col": time_col,
            "hour_col": hour_col,
            "weight_col": weight_col,
        }
        if src_col and dst_col:
            c_expr = relation_expr(paths["call_edges"])
            event_time_expr = f"TRY_CAST({time_col} AS TIMESTAMP)" if time_col else "NULL"
            if time_col and hour_col:
                hour_expr = f"CASE WHEN TRY_CAST({time_col} AS TIMESTAMP) IS NOT NULL THEN EXTRACT('hour' FROM TRY_CAST({time_col} AS TIMESTAMP)) ELSE TRY_CAST({hour_col} AS DOUBLE) END"
            elif time_col:
                hour_expr = f"CASE WHEN TRY_CAST({time_col} AS TIMESTAMP) IS NOT NULL THEN EXTRACT('hour' FROM TRY_CAST({time_col} AS TIMESTAMP)) ELSE NULL END"
            elif hour_col:
                hour_expr = f"TRY_CAST({hour_col} AS DOUBLE)"
            else:
                hour_expr = "NULL"
            weight_expr = f"COALESCE(TRY_CAST({weight_col} AS DOUBLE), 1.0)" if weight_col else "1.0"
            conn.execute(f"""
                CREATE OR REPLACE VIEW calls AS
                SELECT
                    CAST({src_col} AS VARCHAR) AS src_user_id,
                    CAST({dst_col} AS VARCHAR) AS dst_counterparty_id,
                    {event_time_expr} AS event_time,
                    {hour_expr} AS hour_value,
                    {weight_expr} AS call_weight
                FROM {c_expr}
                WHERE {src_col} IS NOT NULL AND {dst_col} IS NOT NULL
            """)
        else:
            meta["missing"].append("call_src_or_dst_column")
    else:
        meta["missing"].append("call_edges")

    if paths.get("device_edges"):
        dev_cols = get_columns_for_file(conn, paths["device_edges"])
        d_user_col = choose_col(dev_cols, ["user_id", "phone_id", "src_user_id", "phone"])
        device_col = choose_col(dev_cols, ["imei", "device_id", "imsi", "terminal_id"])
        meta["columns"]["device"] = {"user_col": d_user_col, "device_col": device_col}
        if d_user_col and device_col:
            d_expr = relation_expr(paths["device_edges"])
            conn.execute(f"""
                CREATE OR REPLACE VIEW device_edges AS
                SELECT DISTINCT
                    CAST({d_user_col} AS VARCHAR) AS user_id,
                    CAST({device_col} AS VARCHAR) AS device_id
                FROM {d_expr}
                WHERE {d_user_col} IS NOT NULL AND {device_col} IS NOT NULL
            """)
        else:
            meta["warnings"].append("device user/device column missing; shared-device evidence unavailable")
    return meta


def df_one(conn: duckdb.DuckDBPyConnection, query: str) -> pd.DataFrame:
    return conn.execute(query).fetchdf()


def province_overview(conn: duckdb.DuckDBPyConnection, pa: str, pb: str) -> pd.DataFrame:
    return df_one(conn, f"""
        SELECT
            province,
            COUNT(*) AS node_rows,
            COUNT(DISTINCT user_id) AS distinct_users,
            SUM(is_risk) AS risk_users,
            COUNT(*) - SUM(is_risk) AS non_risk_users,
            CASE WHEN COUNT(DISTINCT user_id)=0 THEN 0 ELSE SUM(is_risk)::DOUBLE / COUNT(DISTINCT user_id) END AS risk_ratio,
            SUM(CASE WHEN sub_label='risk' THEN 1 ELSE 0 END) AS risk_sub_label_users,
            SUM(CASE WHEN sub_label='purefraud' THEN 1 ELSE 0 END) AS purefraud_users,
            SUM(CASE WHEN sub_label='mutation' THEN 1 ELSE 0 END) AS mutation_users
        FROM nodes
        WHERE province IN ('{pa}', '{pb}')
        GROUP BY province
        ORDER BY province
    """)


def cross_shared_devices(conn: duckdb.DuckDBPyConnection, pa: str, pb: str, top_k: int, min_shared_phone_per_province: int) -> pd.DataFrame:
    try:
        return df_one(conn, f"""
            WITH dn AS (
                SELECT d.device_id, d.user_id, n.province, n.is_risk
                FROM device_edges d
                JOIN nodes n ON d.user_id = n.user_id
                WHERE n.province IN ('{pa}', '{pb}')
            )
            SELECT
                device_id,
                COUNT(DISTINCT CASE WHEN province='{pa}' THEN user_id END) AS {pa}_phone_count,
                COUNT(DISTINCT CASE WHEN province='{pb}' THEN user_id END) AS {pb}_phone_count,
                COUNT(DISTINCT user_id) AS total_phone_count,
                SUM(CASE WHEN province='{pa}' THEN is_risk ELSE 0 END) AS {pa}_risk_count,
                SUM(CASE WHEN province='{pb}' THEN is_risk ELSE 0 END) AS {pb}_risk_count,
                SUM(is_risk) AS total_risk_count,
                STRING_AGG(DISTINCT CASE WHEN province='{pa}' THEN SUBSTRING(user_id,1,12) || '...' END, ', ') AS {pa}_phones_preview,
                STRING_AGG(DISTINCT CASE WHEN province='{pb}' THEN SUBSTRING(user_id,1,12) || '...' END, ', ') AS {pb}_phones_preview
            FROM dn
            GROUP BY device_id
            HAVING COUNT(DISTINCT CASE WHEN province='{pa}' THEN user_id END) >= {min_shared_phone_per_province}
               AND COUNT(DISTINCT CASE WHEN province='{pb}' THEN user_id END) >= {min_shared_phone_per_province}
            ORDER BY total_phone_count DESC, total_risk_count DESC, {pa}_phone_count DESC, {pb}_phone_count DESC
            LIMIT {max(top_k * 5, top_k)}
        """)
    except Exception:
        return pd.DataFrame()


def cross_common_counterparties(conn: duckdb.DuckDBPyConnection, pa: str, pb: str, top_k: int, min_common_sources: int, max_hub_degree: int) -> pd.DataFrame:
    try:
        return df_one(conn, f"""
            WITH cn AS (
                SELECT c.dst_counterparty_id AS counterparty_id, c.src_user_id, n.province, n.is_risk, c.call_weight
                FROM calls c
                JOIN nodes n ON c.src_user_id = n.user_id
                WHERE n.province IN ('{pa}', '{pb}')
            ), agg AS (
                SELECT
                    counterparty_id,
                    COUNT(DISTINCT CASE WHEN province='{pa}' THEN src_user_id END) AS {pa}_source_count,
                    COUNT(DISTINCT CASE WHEN province='{pb}' THEN src_user_id END) AS {pb}_source_count,
                    COUNT(DISTINCT src_user_id) AS total_source_count,
                    SUM(CASE WHEN province='{pa}' THEN call_weight ELSE 0 END) AS {pa}_call_count,
                    SUM(CASE WHEN province='{pb}' THEN call_weight ELSE 0 END) AS {pb}_call_count,
                    SUM(call_weight) AS total_call_count,
                    SUM(CASE WHEN province='{pa}' THEN is_risk ELSE 0 END) AS {pa}_risk_source_count,
                    SUM(CASE WHEN province='{pb}' THEN is_risk ELSE 0 END) AS {pb}_risk_source_count,
                    STRING_AGG(DISTINCT CASE WHEN province='{pa}' THEN SUBSTRING(src_user_id,1,12) || '...' END, ', ') AS {pa}_sources_preview,
                    STRING_AGG(DISTINCT CASE WHEN province='{pb}' THEN SUBSTRING(src_user_id,1,12) || '...' END, ', ') AS {pb}_sources_preview
                FROM cn
                GROUP BY counterparty_id
            )
            SELECT
                *,
                CASE WHEN total_source_count > {max_hub_degree} THEN 1 ELSE 0 END AS public_hub_flag,
                ROUND(LOG(1 + {pa}_source_count) * LOG(1 + {pb}_source_count) * LOG(1 + total_call_count), 6) AS linkage_score
            FROM agg
            WHERE {pa}_source_count >= {min_common_sources}
              AND {pb}_source_count >= {min_common_sources}
            ORDER BY public_hub_flag ASC, linkage_score DESC, total_source_count DESC
            LIMIT {max(top_k * 5, top_k)}
        """)
    except Exception:
        return pd.DataFrame()


def direct_cross_calls(conn: duckdb.DuckDBPyConnection, pa: str, pb: str, top_k: int) -> pd.DataFrame:
    try:
        return df_one(conn, f"""
            WITH ce AS (
                SELECT
                    c.src_user_id,
                    c.dst_counterparty_id,
                    ns.province AS src_province,
                    nd.province AS dst_province,
                    SUM(c.call_weight) AS call_count
                FROM calls c
                JOIN nodes ns ON c.src_user_id = ns.user_id
                JOIN nodes nd ON c.dst_counterparty_id = nd.user_id
                WHERE ns.province IN ('{pa}', '{pb}')
                  AND nd.province IN ('{pa}', '{pb}')
                  AND ns.province <> nd.province
                GROUP BY c.src_user_id, c.dst_counterparty_id, ns.province, nd.province
            )
            SELECT * FROM ce
            ORDER BY call_count DESC
            LIMIT {max(top_k * 5, top_k)}
        """)
    except Exception:
        return pd.DataFrame()


def build_pair_evidence(
    conn: duckdb.DuckDBPyConnection,
    pa: str,
    pb: str,
    top_k: int,
    min_common_sources: int,
    max_pair_common_hub_degree: int,
) -> pd.DataFrame:
    shared_pairs = pd.DataFrame()
    common_pairs = pd.DataFrame()
    direct_pairs = pd.DataFrame()

    try:
        shared_pairs = df_one(conn, f"""
            WITH x AS (
                SELECT
                    da.user_id AS phone_a,
                    db.user_id AS phone_b,
                    da.device_id
                FROM device_edges da
                JOIN nodes na ON da.user_id = na.user_id AND na.province='{pa}'
                JOIN device_edges db ON da.device_id = db.device_id
                JOIN nodes nb ON db.user_id = nb.user_id AND nb.province='{pb}'
                WHERE da.user_id <> db.user_id
            )
            SELECT
                phone_a,
                phone_b,
                COUNT(DISTINCT device_id) AS shared_device_count,
                STRING_AGG(DISTINCT SUBSTRING(device_id,1,12) || '...', ', ') AS shared_devices_preview
            FROM x
            GROUP BY phone_a, phone_b
            ORDER BY shared_device_count DESC
            LIMIT {max(top_k * 200, top_k)}
        """)
    except Exception:
        pass

    try:
        common_pairs = df_one(conn, f"""
            WITH sc AS (
                SELECT
                    c.src_user_id,
                    c.dst_counterparty_id AS counterparty_id,
                    n.province,
                    SUM(c.call_weight) AS call_weight
                FROM calls c
                JOIN nodes n ON c.src_user_id = n.user_id
                WHERE n.province IN ('{pa}', '{pb}')
                GROUP BY c.src_user_id, c.dst_counterparty_id, n.province
            ), cc AS (
                SELECT
                    counterparty_id,
                    COUNT(DISTINCT CASE WHEN province='{pa}' THEN src_user_id END) AS a_cnt,
                    COUNT(DISTINCT CASE WHEN province='{pb}' THEN src_user_id END) AS b_cnt,
                    COUNT(DISTINCT src_user_id) AS total_cnt
                FROM sc
                GROUP BY counterparty_id
                HAVING COUNT(DISTINCT CASE WHEN province='{pa}' THEN src_user_id END) >= {min_common_sources}
                   AND COUNT(DISTINCT CASE WHEN province='{pb}' THEN src_user_id END) >= {min_common_sources}
                   AND COUNT(DISTINCT src_user_id) <= {max_pair_common_hub_degree}
            ), cp AS (
                SELECT
                    ca.src_user_id AS phone_a,
                    cb.src_user_id AS phone_b,
                    ca.counterparty_id AS counterparty_id,
                    SUM(ca.call_weight + cb.call_weight) AS pair_call_weight
                FROM sc ca
                JOIN sc cb ON ca.counterparty_id = cb.counterparty_id
                JOIN cc ON ca.counterparty_id = cc.counterparty_id
                WHERE ca.province='{pa}' AND cb.province='{pb}' AND ca.src_user_id <> cb.src_user_id
                GROUP BY ca.src_user_id, cb.src_user_id, ca.counterparty_id
            )
            SELECT
                phone_a,
                phone_b,
                COUNT(DISTINCT counterparty_id) AS common_counterparty_count,
                SUM(pair_call_weight) AS common_counterparty_call_weight,
                STRING_AGG(DISTINCT SUBSTRING(counterparty_id,1,12) || '...', ', ') AS common_counterparties_preview
            FROM cp
            GROUP BY phone_a, phone_b
            ORDER BY common_counterparty_count DESC, common_counterparty_call_weight DESC
            LIMIT {max(top_k * 500, top_k)}
        """)
    except Exception:
        pass

    try:
        direct_pairs = df_one(conn, f"""
            WITH d AS (
                SELECT
                    CASE WHEN ns.province='{pa}' THEN c.src_user_id ELSE c.dst_counterparty_id END AS phone_a,
                    CASE WHEN ns.province='{pa}' THEN c.dst_counterparty_id ELSE c.src_user_id END AS phone_b,
                    SUM(CASE WHEN ns.province='{pa}' THEN c.call_weight ELSE 0 END) AS a_to_b_call_count,
                    SUM(CASE WHEN ns.province='{pb}' THEN c.call_weight ELSE 0 END) AS b_to_a_call_count,
                    SUM(c.call_weight) AS direct_call_count
                FROM calls c
                JOIN nodes ns ON c.src_user_id = ns.user_id
                JOIN nodes nd ON c.dst_counterparty_id = nd.user_id
                WHERE ns.province IN ('{pa}', '{pb}')
                  AND nd.province IN ('{pa}', '{pb}')
                  AND ns.province <> nd.province
                GROUP BY 1, 2
            )
            SELECT * FROM d
            ORDER BY direct_call_count DESC
            LIMIT {max(top_k * 200, top_k)}
        """)
    except Exception:
        pass

    # Merge evidence tables in pandas.
    base = None
    for df in [shared_pairs, common_pairs, direct_pairs]:
        if df is None or df.empty:
            continue
        if base is None:
            base = df.copy()
        else:
            base = base.merge(df, how="outer", on=["phone_a", "phone_b"])
    if base is None:
        return pd.DataFrame(columns=[
            "phone_a", "phone_b", "phone_a_preview", "phone_b_preview", "shared_device_count",
            "common_counterparty_count", "direct_call_count", "linkage_score", "relation_types",
        ])

    for col in ["shared_device_count", "common_counterparty_count", "common_counterparty_call_weight", "a_to_b_call_count", "b_to_a_call_count", "direct_call_count"]:
        if col not in base.columns:
            base[col] = 0
        base[col] = pd.to_numeric(base[col], errors="coerce").fillna(0)
    for col in ["shared_devices_preview", "common_counterparties_preview"]:
        if col not in base.columns:
            base[col] = ""
        base[col] = base[col].fillna("")
    base["phone_a_preview"] = base["phone_a"].map(preview_id)
    base["phone_b_preview"] = base["phone_b"].map(preview_id)

    def rel_types(row: pd.Series) -> str:
        rels = []
        if row.get("shared_device_count", 0) > 0:
            rels.append("shared_device")
        if row.get("common_counterparty_count", 0) > 0:
            rels.append("common_counterparty")
        if row.get("direct_call_count", 0) > 0:
            rels.append("direct_call")
        return "+".join(rels) if rels else "unknown"

    base["relation_types"] = base.apply(rel_types, axis=1)
    base["linkage_score"] = (
        base["shared_device_count"] * 12.0
        + base["common_counterparty_count"] * 4.0
        + base["direct_call_count"].clip(upper=1000).map(lambda x: math.log1p(x) * 2.0)
        + base["common_counterparty_call_weight"].fillna(0).clip(upper=5000).map(lambda x: math.log1p(x))
    ).round(6)
    return base.sort_values(["linkage_score", "shared_device_count", "common_counterparty_count", "direct_call_count"], ascending=False).head(top_k).reset_index(drop=True)


def build_linkage_paths(pairs: pd.DataFrame, devices: pd.DataFrame, counterparties: pd.DataFrame, top_k: int) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    if pairs is not None and not pairs.empty:
        for _, r in pairs.head(top_k).iterrows():
            if safe_int(r.get("shared_device_count")) > 0:
                rows.append({
                    "path_type": "shared_device_path",
                    "path_zh": "跨省共享设备路径",
                    "source_phone_preview": r.get("phone_a_preview", preview_id(r.get("phone_a"))),
                    "target_phone_preview": r.get("phone_b_preview", preview_id(r.get("phone_b"))),
                    "evidence_preview": str(r.get("shared_devices_preview", "")).split(", ")[0],
                    "path_expression": f"{r.get('phone_a_preview', '')} --共享设备--> {str(r.get('shared_devices_preview', '')).split(', ')[0]} --挂载--> {r.get('phone_b_preview', '')}",
                    "score": safe_float(r.get("linkage_score")),
                })
            if safe_int(r.get("common_counterparty_count")) > 0:
                rows.append({
                    "path_type": "common_counterparty_path",
                    "path_zh": "跨省共同对端路径",
                    "source_phone_preview": r.get("phone_a_preview", preview_id(r.get("phone_a"))),
                    "target_phone_preview": r.get("phone_b_preview", preview_id(r.get("phone_b"))),
                    "evidence_preview": str(r.get("common_counterparties_preview", "")).split(", ")[0],
                    "path_expression": f"{r.get('phone_a_preview', '')} --共同联系--> {str(r.get('common_counterparties_preview', '')).split(', ')[0]} <--共同联系-- {r.get('phone_b_preview', '')}",
                    "score": safe_float(r.get("linkage_score")),
                })
            if safe_float(r.get("direct_call_count")) > 0:
                rows.append({
                    "path_type": "direct_cross_call_path",
                    "path_zh": "跨省直接通话路径",
                    "source_phone_preview": r.get("phone_a_preview", preview_id(r.get("phone_a"))),
                    "target_phone_preview": r.get("phone_b_preview", preview_id(r.get("phone_b"))),
                    "evidence_preview": f"direct_call={safe_float(r.get('direct_call_count'))}",
                    "path_expression": f"{r.get('phone_a_preview', '')} <--> {r.get('phone_b_preview', '')}",
                    "score": safe_float(r.get("linkage_score")),
                })
    # Fallback representative device/counterparty paths when no pairs are returned.
    if not rows and devices is not None and not devices.empty:
        for _, r in devices.head(top_k).iterrows():
            rows.append({
                "path_type": "shared_device_pool",
                "path_zh": "跨省共享设备池",
                "source_phone_preview": str(r.get(f"{DEFAULT_PROVINCE_A}_phones_preview", "")).split(", ")[0],
                "target_phone_preview": str(r.get(f"{DEFAULT_PROVINCE_B}_phones_preview", "")).split(", ")[0],
                "evidence_preview": preview_id(r.get("device_id")),
                "path_expression": "两省号码共同挂载同一设备",
                "score": safe_float(r.get("total_phone_count")),
            })
    if not rows and counterparties is not None and not counterparties.empty:
        for _, r in counterparties.head(top_k).iterrows():
            rows.append({
                "path_type": "common_counterparty_pool",
                "path_zh": "跨省共同对端池",
                "source_phone_preview": str(r.get(f"{DEFAULT_PROVINCE_A}_sources_preview", "")).split(", ")[0],
                "target_phone_preview": str(r.get(f"{DEFAULT_PROVINCE_B}_sources_preview", "")).split(", ")[0],
                "evidence_preview": preview_id(r.get("counterparty_id")),
                "path_expression": "两省号码共同联系同一对端",
                "score": safe_float(r.get("linkage_score")),
            })
    return pd.DataFrame(rows).head(top_k)


def build_bridge_objects(pairs: pd.DataFrame, top_k: int) -> pd.DataFrame:
    if pairs is None or pairs.empty:
        return pd.DataFrame(columns=["node_preview", "province", "bridge_role", "evidence_count", "score"])
    rows = []
    for side, province in [("phone_a", DEFAULT_PROVINCE_A), ("phone_b", DEFAULT_PROVINCE_B)]:
        g = pairs.groupby(side).agg(
            evidence_count=("relation_types", "count"),
            total_score=("linkage_score", "sum"),
            shared_device_links=("shared_device_count", "sum"),
            common_counterparty_links=("common_counterparty_count", "sum"),
            direct_call_links=("direct_call_count", "sum"),
        ).reset_index().rename(columns={side: "node"})
        g["province"] = province
        rows.append(g)
    out = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    if out.empty:
        return out
    out["node_preview"] = out["node"].map(preview_id)
    out["bridge_role"] = out.apply(lambda r: "multi_evidence_bridge" if (r["shared_device_links"] > 0 and r["common_counterparty_links"] > 0) else "cross_province_bridge", axis=1)
    out["score"] = (out["total_score"] + out["evidence_count"] * 2).round(6)
    cols = ["node", "node_preview", "province", "bridge_role", "evidence_count", "shared_device_links", "common_counterparty_links", "direct_call_links", "score"]
    return out[cols].sort_values("score", ascending=False).head(top_k)


def metric_summary(pa: str, pb: str, devices: pd.DataFrame, counterparties: pd.DataFrame, pairs: pd.DataFrame, direct: pd.DataFrame) -> Dict[str, Any]:
    return {
        "cross_shared_device_count": safe_int(len(devices)),
        "cross_common_counterparty_count": safe_int(len(counterparties)),
        "strong_cross_pair_count_returned": safe_int(len(pairs)),
        "direct_cross_call_pair_count_returned": safe_int(len(direct)),
        "top_shared_device": preview_id(devices.iloc[0]["device_id"]) if devices is not None and not devices.empty else "",
        "top_common_counterparty": preview_id(counterparties.iloc[0]["counterparty_id"]) if counterparties is not None and not counterparties.empty else "",
        "top_pair": (
            f"{preview_id(pairs.iloc[0]['phone_a'])} <-> {preview_id(pairs.iloc[0]['phone_b'])}"
            if pairs is not None and not pairs.empty else ""
        ),
        "province_a": pa,
        "province_b": pb,
    }


def write_csv(df: pd.DataFrame, path: Path) -> None:
    if df is None:
        df = pd.DataFrame()
    df.to_csv(path, index=False, encoding="utf-8-sig")


def write_excel(path: Path, sheets: Dict[str, pd.DataFrame]) -> None:
    try:
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            for name, df in sheets.items():
                safe_name = name[:31]
                (df if df is not None else pd.DataFrame()).to_excel(writer, index=False, sheet_name=safe_name)
    except Exception:
        # Fallback: create a small csv-like placeholder with xlsx suffix not desired, so write text marker.
        path.write_text("Excel generation failed; see CSV evidence files.\n", encoding="utf-8")


def md_table(df: pd.DataFrame, cols: Optional[List[str]] = None, max_rows: int = 10) -> str:
    if df is None or df.empty:
        return "暂无数据。\n"
    sub = df.copy()
    if cols:
        sub = sub[[c for c in cols if c in sub.columns]]
    sub = sub.head(max_rows)
    if sub.empty:
        return "暂无数据。\n"
    headers = [str(c) for c in sub.columns]
    lines = []
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for _, row in sub.iterrows():
        vals = []
        for c in sub.columns:
            v = row[c]
            if isinstance(v, float):
                v = round(v, 4)
            vals.append(str(v).replace("|", "/"))
        lines.append("| " + " | ".join(vals) + " |")
    return "\n".join(lines) + "\n"


def generate_reports(
    output_dir: Path,
    dataset: str,
    pa: str,
    pb: str,
    overview: pd.DataFrame,
    devices: pd.DataFrame,
    counterparties: pd.DataFrame,
    pairs: pd.DataFrame,
    direct: pd.DataFrame,
    paths: pd.DataFrame,
    bridges: pd.DataFrame,
    summary: Dict[str, Any],
) -> Tuple[Path, Path]:
    report = output_dir / f"cross_province_linkage_{dataset}.md"
    presentation = output_dir / f"cross_province_linkage_{dataset}_presentation.md"
    m = summary.get("result", {}).get("linkage_summary", {})
    time_note = summary.get("scope_note", "")

    lines = []
    lines.append("# 跨省联动分析报告\n")
    lines.append("## 一、分析口径\n")
    lines.append(f"- 数据集：`{dataset}`\n")
    lines.append(f"- 对比省份：`{pa}` 与 `{pb}`\n")
    lines.append("- 分析对象：基于 unified 统一索引识别跨省共享设备、跨省共同对端、跨省强关联对象和代表性关系链路。\n")
    lines.append("- 重要边界：本报告输出的是跨省关联线索，不等于已经确认存在真实案件联动或犯罪事实。\n")
    if time_note:
        lines.append(f"- 时间/数据口径提醒：{time_note}\n")
    lines.append("\n## 二、总体结果\n")
    lines.append(f"- 跨省共享设备数量：**{m.get('cross_shared_device_count', 0)}**\n")
    lines.append(f"- 跨省共同对端数量：**{m.get('cross_common_counterparty_count', 0)}**\n")
    lines.append(f"- 返回的强关联号码对：**{m.get('strong_cross_pair_count_returned', 0)}**\n")
    lines.append(f"- 返回的直接跨省通话号码对：**{m.get('direct_cross_call_pair_count_returned', 0)}**\n")

    lines.append("\n## 三、省份对象概览\n")
    lines.append(md_table(overview, max_rows=10))
    lines.append("\n## 四、跨省共享设备证据\n")
    lines.append(md_table(devices, cols=["device_id", f"{pa}_phone_count", f"{pb}_phone_count", "total_phone_count", "total_risk_count"], max_rows=10))
    lines.append("\n## 五、跨省共同对端证据\n")
    lines.append(md_table(counterparties, cols=["counterparty_id", f"{pa}_source_count", f"{pb}_source_count", "total_source_count", "total_call_count", "public_hub_flag", "linkage_score"], max_rows=10))
    lines.append("\n## 六、跨省强关联号码对\n")
    lines.append(md_table(pairs, cols=["phone_a_preview", "phone_b_preview", "relation_types", "shared_device_count", "common_counterparty_count", "direct_call_count", "linkage_score"], max_rows=10))
    lines.append("\n## 七、重点桥接对象\n")
    lines.append(md_table(bridges, cols=["node_preview", "province", "bridge_role", "evidence_count", "score"], max_rows=10))
    lines.append("\n## 八、代表性跨省关系链路\n")
    lines.append(md_table(paths, cols=["path_zh", "source_phone_preview", "target_phone_preview", "evidence_preview", "path_expression", "score"], max_rows=10))
    lines.append("\n## 九、后续建议\n")
    lines.append("1. 对 Top 跨省共享设备调用 `shared-device-analysis`，确认设备池规模和挂载号码画像。\n")
    lines.append("2. 对 Top 跨省号码对调用 `association-path-analysis` 和 `overlap-analysis`，复核路径与同圈关系。\n")
    lines.append("3. 对桥接对象调用 `single-number-analysis` 或 `risk-evidence-pack`，补全单号证据包。\n")
    lines.append("4. 如跨省共享设备或共同对端集中度较高，可进一步调用 `gang-cluster-analysis` 识别疑似团伙结构。\n")
    lines.append("\n## 十、生成文件\n")
    for art in summary.get("artifacts", []):
        lines.append(f"- `{art.get('title')}`\n")
    report.write_text("".join(lines), encoding="utf-8")

    pl = []
    pl.append("# 跨省联动研判摘要\n")
    pl.append("## 核心结论\n")
    pl.append(f"基于 `{dataset}` 统一索引，本次识别 `{pa}` 与 `{pb}` 之间的跨省关联线索。\n\n")
    pl.append(f"- 跨省共享设备：**{m.get('cross_shared_device_count', 0)}** 个\n")
    pl.append(f"- 跨省共同对端：**{m.get('cross_common_counterparty_count', 0)}** 个\n")
    pl.append(f"- 强关联号码对：**{m.get('strong_cross_pair_count_returned', 0)}** 对\n")
    if m.get("top_shared_device"):
        pl.append(f"- 最显著共享设备：`{m.get('top_shared_device')}`\n")
    if m.get("top_pair"):
        pl.append(f"- 最显著跨省号码对：`{m.get('top_pair')}`\n")
    pl.append("\n## 重点证据\n")
    pl.append("### 共享设备\n")
    pl.append(md_table(devices, cols=["device_id", f"{pa}_phone_count", f"{pb}_phone_count", "total_phone_count", "total_risk_count"], max_rows=5))
    pl.append("\n### 共同对端\n")
    pl.append(md_table(counterparties, cols=["counterparty_id", f"{pa}_source_count", f"{pb}_source_count", "total_source_count", "total_call_count", "public_hub_flag"], max_rows=5))
    pl.append("\n### 强关联对象\n")
    pl.append(md_table(pairs, cols=["phone_a_preview", "phone_b_preview", "relation_types", "linkage_score"], max_rows=5))
    pl.append("\n## 研判边界\n")
    pl.append("本报告用于发现跨省关联线索，不直接给出案件定性；后续需结合单号证据包、共享设备深挖和路径复核继续确认。\n")
    pl.append("\n## 建议下一步\n")
    pl.append("优先围绕 Top 共享设备、Top 强关联号码对和桥接对象继续下钻。\n")
    presentation.write_text("".join(pl), encoding="utf-8")
    return report, presentation


def build_artifacts(mode: str, files: Dict[str, Path]) -> List[Dict[str, str]]:
    # Always generate all files, but expose only requested artifacts to frontend.
    if mode == "markdown_only":
        keys = ["report_md", "presentation_md"]
    elif mode == "essential":
        keys = ["report_md", "presentation_md", "summary_json", "evidence_xlsx"]
    else:
        keys = list(files.keys())
    out = []
    for k in keys:
        p = files.get(k)
        if not p:
            continue
        typ = "markdown_report" if p.suffix.lower() == ".md" else ("json" if p.suffix.lower() == ".json" else ("xlsx" if p.suffix.lower() == ".xlsx" else "csv"))
        out.append({"type": typ, "path": str(p), "title": p.name})
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Cross-province linkage analysis for phone-network data")
    parser.add_argument("--dataset-root", default=None)
    parser.add_argument("--dataset", default=DEFAULT_DATASET)
    parser.add_argument("--province-a", default=DEFAULT_PROVINCE_A)
    parser.add_argument("--province-b", default=DEFAULT_PROVINCE_B)
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--min-shared-phone-per-province", type=int, default=1)
    parser.add_argument("--min-common-sources", type=int, default=2)
    parser.add_argument("--max-hub-degree", type=int, default=500)
    parser.add_argument("--max-pair-common-hub-degree", type=int, default=200)
    parser.add_argument("--artifact-mode", choices=["full", "essential", "markdown_only"], default="full")
    parser.add_argument("--output-dir", default="/mnt/user-data/outputs")
    args = parser.parse_args()

    pa = args.province_a.lower()
    pb = args.province_b.lower()
    dataset_root = find_dataset_root(args.dataset_root)
    output_dir = ensure_output_dir(args.output_dir)
    paths = resolve_paths(dataset_root, args.dataset)
    conn = duckdb.connect(database=":memory:")
    setup = setup_views(conn, paths)

    base_result: Dict[str, Any] = {
        "ok": False,
        "skill": "cross-province-linkage-analysis",
        "query_type": "cross_province_linkage",
        "script_version": SCRIPT_VERSION,
        "dataset": args.dataset,
        "dataset_root": str(dataset_root),
        "analysis_scope": "cross_province_linkage_not_region_comparison",
        "province_a": pa,
        "province_b": pb,
        "artifact_mode": args.artifact_mode,
        "setup_meta": setup,
        "base_operator_alignment": {
            "province_scope_filter": "node_lookup + province_filter",
            "cross_shared_device": "query_shared_device + aggregation_query",
            "cross_common_counterparty": "common_neighbor + relationship_filter + aggregation_query",
            "direct_cross_call": "relationship_filter(src_province != dst_province)",
            "strong_pair_ranking": "aggregation_query + scoring_layer",
            "linkage_path_summary": "path_query style evidence reconstruction",
        },
    }

    if setup.get("missing"):
        base_result.update({
            "ok": True,
            "status": "missing_required_data",
            "notes": ["缺少必要数据或字段，无法完成跨省联动分析。"],
            "artifacts": [],
        })
        print(json.dumps(json_safe(base_result), ensure_ascii=False, indent=2))
        return

    overview = province_overview(conn, pa, pb)
    devices = cross_shared_devices(conn, pa, pb, args.top_k, args.min_shared_phone_per_province)
    counterparties = cross_common_counterparties(conn, pa, pb, args.top_k, args.min_common_sources, args.max_hub_degree)
    direct = direct_cross_calls(conn, pa, pb, args.top_k)
    pairs = build_pair_evidence(conn, pa, pb, args.top_k, args.min_common_sources, args.max_pair_common_hub_degree)
    paths_df = build_linkage_paths(pairs, devices, counterparties, args.top_k)
    bridges = build_bridge_objects(pairs, args.top_k)

    linkage_summary = metric_summary(pa, pb, devices, counterparties, pairs, direct)
    scope_note = "本 skill 识别的是跨省关联线索，不等同于案件定性；它和 sichuan-shaanxi-comparison 的区别是：本 skill 追踪跨省联动关系，后者比较两地差异。"

    prefix = f"cross_province_linkage_{args.dataset}"
    files = {
        "overview_csv": output_dir / f"{prefix}_province_overview.csv",
        "cross_shared_devices_csv": output_dir / f"{prefix}_cross_shared_devices.csv",
        "cross_common_counterparties_csv": output_dir / f"{prefix}_cross_common_counterparties.csv",
        "direct_cross_calls_csv": output_dir / f"{prefix}_direct_cross_calls.csv",
        "strong_pairs_csv": output_dir / f"{prefix}_strong_pairs.csv",
        "bridge_objects_csv": output_dir / f"{prefix}_bridge_objects.csv",
        "linkage_paths_csv": output_dir / f"{prefix}_linkage_paths.csv",
        "summary_json": output_dir / f"{prefix}_summary.json",
        "evidence_xlsx": output_dir / f"{prefix}_evidence.xlsx",
    }

    # Pre-summary artifacts are added after report paths are known.
    for key, df in [
        ("overview_csv", overview),
        ("cross_shared_devices_csv", devices),
        ("cross_common_counterparties_csv", counterparties),
        ("direct_cross_calls_csv", direct),
        ("strong_pairs_csv", pairs),
        ("bridge_objects_csv", bridges),
        ("linkage_paths_csv", paths_df),
    ]:
        write_csv(df, files[key])

    result = dict(base_result)
    result.update({
        "ok": True,
        "status": "ok",
        "scope_note": scope_note,
        "input_summary": {
            "dataset_root": str(dataset_root),
            "dataset": args.dataset,
            "province_a": pa,
            "province_b": pb,
            "top_k": args.top_k,
            "min_shared_phone_per_province": args.min_shared_phone_per_province,
            "min_common_sources": args.min_common_sources,
            "max_hub_degree": args.max_hub_degree,
            "max_pair_common_hub_degree": args.max_pair_common_hub_degree,
            "paths": {k: str(v) if v else None for k, v in paths.items()},
        },
        "result": {
            "linkage_summary": linkage_summary,
            "province_overview": dataframe_records(overview),
            "top_cross_shared_devices": dataframe_records(devices, args.top_k),
            "top_cross_common_counterparties": dataframe_records(counterparties, args.top_k),
            "top_strong_cross_pairs": dataframe_records(pairs, args.top_k),
            "top_bridge_objects": dataframe_records(bridges, args.top_k),
            "representative_linkage_paths": dataframe_records(paths_df, args.top_k),
        },
        "top_signal_summary": [
            f"识别跨省共享设备 {linkage_summary.get('cross_shared_device_count', 0)} 个。",
            f"识别跨省共同对端 {linkage_summary.get('cross_common_counterparty_count', 0)} 个。",
            f"返回强关联跨省号码对 {linkage_summary.get('strong_cross_pair_count_returned', 0)} 对。",
        ],
        "next_step_suggestions": [
            "对 Top 跨省共享设备调用 shared-device-analysis 继续下钻。",
            "对 Top 跨省号码对调用 association-path-analysis / overlap-analysis 复核关系路径。",
            "对桥接对象调用 risk-evidence-pack 生成单号证据包。",
            "若跨省线索集中，可调用 gang-cluster-analysis 做团伙结构识别。",
        ],
    })

    # Need artifacts in summary before markdown file list; create temporary report without artifacts then update.
    result["artifacts"] = []
    report_md, presentation_md = generate_reports(output_dir, args.dataset, pa, pb, overview, devices, counterparties, pairs, direct, paths_df, bridges, result)
    files = {"report_md": report_md, "presentation_md": presentation_md, **files}
    result["artifacts"] = build_artifacts(args.artifact_mode, files)
    result["files"] = {k: str(v) for k, v in files.items()}
    result["report_path"] = str(report_md)

    # Rewrite markdown with final artifact list.
    generate_reports(output_dir, args.dataset, pa, pb, overview, devices, counterparties, pairs, direct, paths_df, bridges, result)

    write_excel(files["evidence_xlsx"], {
        "province_overview": overview,
        "cross_shared_devices": devices,
        "cross_common_counterparties": counterparties,
        "direct_cross_calls": direct,
        "strong_pairs": pairs,
        "bridge_objects": bridges,
        "linkage_paths": paths_df,
    })
    files["summary_json"].write_text(json.dumps(json_safe(result), ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(json_safe(result), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
