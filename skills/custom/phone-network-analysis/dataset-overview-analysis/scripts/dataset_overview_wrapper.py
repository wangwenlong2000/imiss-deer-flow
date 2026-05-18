#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
dataset-overview-analysis

面向电话网络数据开展总体概览分析，统计对象规模、风险对象分布、关系规模、
时间覆盖、共享设备规模和可分析能力范围，用于回答“数据里有什么、能做什么”。

This script is designed to run inside the DeerFlow sandbox and uses the already
preprocessed phone-network graph tables under datasets/phone-network.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb
import pandas as pd

SCRIPT_VERSION = "dataset-overview-analysis-release-v1.1"
SKILL_NAME = "dataset-overview-analysis"
QUERY_TYPE = "dataset_overview"

DEFAULT_DATASET = "unified"
DEFAULT_OUTPUT_DIR = "/mnt/user-data/outputs"


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def safe_preview(value: Any, n: int = 12) -> str:
    if value is None:
        return ""
    s = str(value)
    return s if len(s) <= n else s[:n] + "..."


def to_jsonable(obj: Any) -> Any:
    """Convert pandas/numpy values to JSON-safe Python objects."""
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
        return obj
    if isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    if hasattr(obj, "item"):
        try:
            return to_jsonable(obj.item())
        except Exception:
            pass
    if isinstance(obj, dict):
        return {str(k): to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [to_jsonable(x) for x in obj]
    if isinstance(obj, pd.DataFrame):
        return [to_jsonable(x) for x in obj.to_dict(orient="records")]
    if isinstance(obj, pd.Series):
        return to_jsonable(obj.to_dict())
    return str(obj)


def ensure_dir(path: str | Path) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def resolve_output_dir(output_dir: Optional[str]) -> Path:
    candidates = []
    if output_dir:
        candidates.append(Path(output_dir))
    candidates.append(Path(DEFAULT_OUTPUT_DIR))
    candidates.append(Path.cwd() / "outputs")
    for p in candidates:
        try:
            p.mkdir(parents=True, exist_ok=True)
            test_file = p / ".write_test"
            test_file.write_text("ok", encoding="utf-8")
            test_file.unlink(missing_ok=True)
            return p
        except Exception:
            continue
    raise RuntimeError("无法创建输出目录。")


def qident(col: str) -> str:
    return '"' + col.replace('"', '""') + '"'


def sql_string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def run_df(conn: duckdb.DuckDBPyConnection, sql: str) -> pd.DataFrame:
    try:
        return conn.execute(sql).fetchdf()
    except Exception as exc:
        return pd.DataFrame([{"error": str(exc), "sql_preview": sql[:300]}])


def run_scalar(conn: duckdb.DuckDBPyConnection, sql: str, default: Any = None) -> Any:
    try:
        row = conn.execute(sql).fetchone()
        return row[0] if row else default
    except Exception:
        return default


def read_text_lines(path: Path, limit: int = 5) -> List[str]:
    try:
        lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        return lines[:limit]
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Dataset path resolution
# ---------------------------------------------------------------------------

def find_repo_root(start: Optional[Path] = None) -> Optional[Path]:
    cur = (start or Path(__file__).resolve()).resolve()
    for p in [cur] + list(cur.parents):
        if (p / "datasets" / "phone-network").exists():
            return p
    common = [
        Path("/workspace/imiss-deer-flow-main"),
        Path("/mnt/skills"),
        Path.cwd(),
    ]
    for p in common:
        if (p / "datasets" / "phone-network").exists():
            return p
    return None


def resolve_dataset_root(dataset_root: Optional[str]) -> Path:
    if dataset_root:
        p = Path(dataset_root)
        if p.exists():
            return p.resolve()
        raise FileNotFoundError(f"指定的 dataset-root 不存在：{dataset_root}")

    repo = find_repo_root()
    if repo is not None:
        candidate = repo / "datasets" / "phone-network"
        if candidate.exists():
            return candidate.resolve()

    common = [
        Path("/workspace/imiss-deer-flow-main/datasets/phone-network"),
        Path("/mnt/data/phone-network"),
        Path("/mnt/user-data/datasets/phone-network"),
    ]
    for p in common:
        if p.exists():
            return p.resolve()
    raise FileNotFoundError("无法找到电话网络数据集根目录，请通过 --dataset-root 指定。")


def resolve_dataset_paths(dataset_root: Path, dataset: str) -> Dict[str, Optional[Path]]:
    processed = dataset_root / "processed"
    graph_views = processed / "graph_views"

    user_candidates = [
        processed / dataset / "user_nodes.csv",
        processed / "unified" / "user_nodes.csv" if dataset != "unified" else processed / dataset / "user_nodes.csv",
        dataset_root / "user_nodes.csv",
    ]
    call_candidates = [
        processed / dataset / "call_edges.csv",
        processed / "unified" / "call_edges.csv" if dataset != "unified" else processed / dataset / "call_edges.csv",
        dataset_root / "call_edges.csv",
    ]
    device_candidates = [
        graph_views / dataset / "edges_phone_imei.parquet",
        graph_views / dataset / "edges_phone_imei.csv",
        graph_views / "unified" / "edges_phone_imei.parquet" if dataset != "unified" else graph_views / dataset / "edges_phone_imei.parquet",
        graph_views / "unified" / "edges_phone_imei.csv" if dataset != "unified" else graph_views / dataset / "edges_phone_imei.csv",
        processed / dataset / "edges_phone_imei.parquet",
        processed / dataset / "edges_phone_imei.csv",
    ]

    def first_exists(cands: List[Path]) -> Optional[Path]:
        seen = set()
        for p in cands:
            if p in seen:
                continue
            seen.add(p)
            if p and p.exists():
                return p.resolve()
        return None

    return {
        "user_nodes": first_exists(user_candidates),
        "call_edges": first_exists(call_candidates),
        "device_edges": first_exists(device_candidates),
    }


# ---------------------------------------------------------------------------
# DuckDB setup and schema detection
# ---------------------------------------------------------------------------

def create_view_from_file(conn: duckdb.DuckDBPyConnection, view_name: str, path: Path) -> None:
    path_lit = sql_string_literal(str(path))
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        conn.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_parquet({path_lit})")
    else:
        conn.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_csv_auto({path_lit}, header=true, ignore_errors=true)")


def get_columns(conn: duckdb.DuckDBPyConnection, view_name: str) -> List[str]:
    try:
        df = conn.execute(f"DESCRIBE SELECT * FROM {view_name}").fetchdf()
        return [str(x) for x in df["column_name"].tolist()]
    except Exception:
        return []


def pick_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    lower_map = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in lower_map:
            return lower_map[cand.lower()]
    return None


def setup_views(conn: duckdb.DuckDBPyConnection, paths: Dict[str, Optional[Path]]) -> Dict[str, Any]:
    meta: Dict[str, Any] = {"available_views": {}, "columns": {}, "picked_columns": {}}

    if paths.get("user_nodes"):
        create_view_from_file(conn, "user_nodes", paths["user_nodes"])
        cols = get_columns(conn, "user_nodes")
        meta["available_views"]["user_nodes"] = True
        meta["columns"]["user_nodes"] = cols
        meta["picked_columns"]["user_id"] = pick_col(cols, ["user_id", "phone_id", "msisdn", "id", "node_id"])
        meta["picked_columns"]["label"] = pick_col(cols, ["label", "risk_label", "is_risk"])
        meta["picked_columns"]["sub_label"] = pick_col(cols, ["sub_label", "subtype", "risk_type", "category"])
        meta["picked_columns"]["province"] = pick_col(cols, ["province", "prov", "region"])
        meta["picked_columns"]["city"] = pick_col(cols, ["city", "county"])
    else:
        meta["available_views"]["user_nodes"] = False

    if paths.get("call_edges"):
        create_view_from_file(conn, "raw_call_edges", paths["call_edges"])
        cols = get_columns(conn, "raw_call_edges")
        meta["available_views"]["call_edges"] = True
        meta["columns"]["call_edges"] = cols
        src = pick_col(cols, ["src_user_id", "source", "source_id", "user_id", "caller", "caller_id", "phone_id"])
        dst = pick_col(cols, ["dst_counterparty_id", "target", "target_id", "counterparty_id", "callee", "callee_id", "dst_user_id"])
        time_col = pick_col(cols, ["event_time", "call_time", "start_time", "time", "timestamp", "datetime", "date_time"])
        hour_col = pick_col(cols, ["hour", "event_hour", "call_hour", "start_hour"])
        duration_col = pick_col(cols, ["duration", "call_duration", "duration_sec", "duration_seconds", "call_duration_sec"])
        count_col = pick_col(cols, ["call_count", "count", "cnt", "records"])
        province_col = pick_col(cols, ["province", "prov", "region"])

        meta["picked_columns"].update({
            "call_src": src,
            "call_dst": dst,
            "event_time": time_col,
            "hour_col": hour_col,
            "duration_col": duration_col,
            "count_col": count_col,
            "call_province": province_col,
        })

        src_expr = f"CAST({qident(src)} AS VARCHAR)" if src else "CAST(NULL AS VARCHAR)"
        dst_expr = f"CAST({qident(dst)} AS VARCHAR)" if dst else "CAST(NULL AS VARCHAR)"
        if time_col:
            ts_expr = f"TRY_CAST({qident(time_col)} AS TIMESTAMP)"
        else:
            ts_expr = "CAST(NULL AS TIMESTAMP)"
        if time_col and hour_col:
            hour_expr = f"COALESCE(EXTRACT('hour' FROM TRY_CAST({qident(time_col)} AS TIMESTAMP)), TRY_CAST({qident(hour_col)} AS DOUBLE))"
        elif time_col:
            hour_expr = f"EXTRACT('hour' FROM TRY_CAST({qident(time_col)} AS TIMESTAMP))"
        elif hour_col:
            hour_expr = f"TRY_CAST({qident(hour_col)} AS DOUBLE)"
        else:
            hour_expr = "CAST(NULL AS DOUBLE)"
        if count_col:
            weight_expr = f"COALESCE(TRY_CAST({qident(count_col)} AS DOUBLE), 1.0)"
        elif duration_col:
            # For overview purposes duration is kept as a secondary weighted signal.
            weight_expr = f"COALESCE(TRY_CAST({qident(duration_col)} AS DOUBLE), 1.0)"
        else:
            weight_expr = "1.0"
        province_expr = f"CAST({qident(province_col)} AS VARCHAR)" if province_col else "CAST(NULL AS VARCHAR)"
        conn.execute(f"""
            CREATE OR REPLACE VIEW call_edges AS
            SELECT
                {src_expr} AS source_id,
                {dst_expr} AS target_id,
                {ts_expr} AS event_ts,
                CAST({hour_expr} AS DOUBLE) AS hour_value,
                CAST({weight_expr} AS DOUBLE) AS weight_value,
                {province_expr} AS province
            FROM raw_call_edges
        """)
    else:
        meta["available_views"]["call_edges"] = False

    if paths.get("device_edges"):
        create_view_from_file(conn, "raw_device_edges", paths["device_edges"])
        cols = get_columns(conn, "raw_device_edges")
        meta["available_views"]["device_edges"] = True
        meta["columns"]["device_edges"] = cols
        phone_col = pick_col(cols, ["user_id", "phone_id", "src_user_id", "msisdn", "id"])
        device_col = pick_col(cols, ["imei", "device_id", "imei_id", "terminal_id", "equipment_id"])
        meta["picked_columns"].update({"device_phone": phone_col, "device_id": device_col})
        phone_expr = f"CAST({qident(phone_col)} AS VARCHAR)" if phone_col else "CAST(NULL AS VARCHAR)"
        dev_expr = f"CAST({qident(device_col)} AS VARCHAR)" if device_col else "CAST(NULL AS VARCHAR)"
        conn.execute(f"""
            CREATE OR REPLACE VIEW device_edges AS
            SELECT {phone_expr} AS phone_id, {dev_expr} AS device_id
            FROM raw_device_edges
            WHERE {phone_expr} IS NOT NULL AND {dev_expr} IS NOT NULL
        """)
    else:
        meta["available_views"]["device_edges"] = False

    return meta


# ---------------------------------------------------------------------------
# Analysis functions
# ---------------------------------------------------------------------------

def build_node_summary(conn: duckdb.DuckDBPyConnection, meta: Dict[str, Any], top_k: int) -> Dict[str, Any]:
    if not meta["available_views"].get("user_nodes"):
        return {"available": False, "notes": ["未找到 user_nodes 节点表。"]}
    pc = meta["picked_columns"]
    uid = pc.get("user_id")
    label = pc.get("label")
    sub_label = pc.get("sub_label")
    province = pc.get("province")

    total = int(run_scalar(conn, "SELECT COUNT(*) FROM user_nodes", 0) or 0)
    distinct_users = int(run_scalar(conn, f"SELECT COUNT(DISTINCT {qident(uid)}) FROM user_nodes" if uid else "SELECT COUNT(*) FROM user_nodes", 0) or 0)
    duplicate_users = max(total - distinct_users, 0)

    risk_count = None
    risk_ratio = None
    if label:
        risk_count = int(run_scalar(conn, f"SELECT COUNT(*) FROM user_nodes WHERE TRY_CAST({qident(label)} AS DOUBLE) = 1", 0) or 0)
        risk_ratio = round(risk_count / total, 6) if total else None

    label_df = pd.DataFrame()
    if label:
        label_df = run_df(conn, f"""
            SELECT CAST({qident(label)} AS VARCHAR) AS label, COUNT(*) AS count,
                   ROUND(COUNT(*) * 100.0 / NULLIF((SELECT COUNT(*) FROM user_nodes), 0), 4) AS pct
            FROM user_nodes
            GROUP BY 1
            ORDER BY count DESC
            LIMIT {top_k}
        """)
    sub_label_df = pd.DataFrame()
    if sub_label:
        sub_label_df = run_df(conn, f"""
            SELECT COALESCE(CAST({qident(sub_label)} AS VARCHAR), 'NULL') AS sub_label, COUNT(*) AS count,
                   ROUND(COUNT(*) * 100.0 / NULLIF((SELECT COUNT(*) FROM user_nodes), 0), 4) AS pct
            FROM user_nodes
            GROUP BY 1
            ORDER BY count DESC
            LIMIT {top_k}
        """)
    province_df = pd.DataFrame()
    if province:
        province_df = run_df(conn, f"""
            SELECT COALESCE(CAST({qident(province)} AS VARCHAR), 'NULL') AS province, COUNT(*) AS count,
                   ROUND(COUNT(*) * 100.0 / NULLIF((SELECT COUNT(*) FROM user_nodes), 0), 4) AS pct
            FROM user_nodes
            GROUP BY 1
            ORDER BY count DESC
            LIMIT {top_k}
        """)

    return {
        "available": True,
        "total_rows": total,
        "distinct_users": distinct_users,
        "duplicate_user_rows": duplicate_users,
        "risk_count": risk_count,
        "risk_ratio": risk_ratio,
        "label_distribution": label_df.to_dict(orient="records"),
        "sub_label_distribution": sub_label_df.to_dict(orient="records"),
        "province_distribution": province_df.to_dict(orient="records"),
    }


def build_call_summary(conn: duckdb.DuckDBPyConnection, meta: Dict[str, Any], top_k: int) -> Dict[str, Any]:
    if not meta["available_views"].get("call_edges"):
        return {"available": False, "notes": ["未找到 call_edges 通话关系表。"]}

    total = int(run_scalar(conn, "SELECT COUNT(*) FROM call_edges", 0) or 0)
    unique_sources = int(run_scalar(conn, "SELECT COUNT(DISTINCT source_id) FROM call_edges WHERE source_id IS NOT NULL", 0) or 0)
    unique_targets = int(run_scalar(conn, "SELECT COUNT(DISTINCT target_id) FROM call_edges WHERE target_id IS NOT NULL", 0) or 0)
    min_ts = run_scalar(conn, "SELECT MIN(event_ts) FROM call_edges WHERE event_ts IS NOT NULL")
    max_ts = run_scalar(conn, "SELECT MAX(event_ts) FROM call_edges WHERE event_ts IS NOT NULL")
    time_available = min_ts is not None and max_ts is not None

    night_count = int(run_scalar(conn, "SELECT COUNT(*) FROM call_edges WHERE hour_value IS NOT NULL AND (hour_value >= 22 OR hour_value < 6)", 0) or 0)
    hour_known = int(run_scalar(conn, "SELECT COUNT(*) FROM call_edges WHERE hour_value IS NOT NULL", 0) or 0)
    night_ratio = round(night_count / hour_known, 6) if hour_known else None

    daily_df = pd.DataFrame()
    if time_available:
        daily_df = run_df(conn, f"""
            SELECT CAST(event_ts AS DATE) AS event_date,
                   COUNT(*) AS record_count,
                   SUM(weight_value) AS weighted_count,
                   COUNT(DISTINCT source_id) AS active_callers,
                   COUNT(DISTINCT target_id) AS counterparty_count,
                   ROUND(AVG(CASE WHEN hour_value IS NOT NULL AND (hour_value >= 22 OR hour_value < 6) THEN 1.0 ELSE 0.0 END), 6) AS night_ratio
            FROM call_edges
            WHERE event_ts IS NOT NULL
            GROUP BY 1
            ORDER BY event_date
        """)
    daily_stats = {}
    if not daily_df.empty and "record_count" in daily_df.columns:
        active_days = int(len(daily_df))
        calendar_span_days = None
        active_day_ratio = None
        try:
            min_day = pd.to_datetime(daily_df["event_date"]).min()
            max_day = pd.to_datetime(daily_df["event_date"]).max()
            calendar_span_days = int((max_day.normalize() - min_day.normalize()).days) + 1
            active_day_ratio = round(active_days / calendar_span_days, 6) if calendar_span_days else None
        except Exception:
            calendar_span_days = None
            active_day_ratio = None
        daily_stats = {
            "days": active_days,
            "active_days": active_days,
            "calendar_span_days": calendar_span_days,
            "active_day_ratio": active_day_ratio,
            "avg_daily_records": round(float(daily_df["record_count"].mean()), 4),
            "max_daily_records": int(daily_df["record_count"].max()),
            "min_daily_records": int(daily_df["record_count"].min()),
        }

    hourly_df = pd.DataFrame()
    if hour_known:
        hourly_df = run_df(conn, f"""
            SELECT CAST(hour_value AS INTEGER) AS hour, COUNT(*) AS record_count, SUM(weight_value) AS weighted_count,
                   COUNT(DISTINCT source_id) AS active_callers
            FROM call_edges
            WHERE hour_value IS NOT NULL
            GROUP BY 1
            ORDER BY record_count DESC, hour
            LIMIT {top_k}
        """)

    top_callers_df = run_df(conn, f"""
        SELECT source_id AS node_id, SUBSTRING(source_id, 1, 12) || '...' AS node_preview,
               COUNT(*) AS record_count, SUM(weight_value) AS weighted_count,
               COUNT(DISTINCT target_id) AS counterparty_count,
               ROUND(AVG(CASE WHEN hour_value IS NOT NULL AND (hour_value >= 22 OR hour_value < 6) THEN 1.0 ELSE 0.0 END), 6) AS night_ratio
        FROM call_edges
        WHERE source_id IS NOT NULL
        GROUP BY 1
        ORDER BY record_count DESC
        LIMIT {top_k}
    """)

    top_targets_df = run_df(conn, f"""
        SELECT target_id AS counterparty_id, SUBSTRING(target_id, 1, 12) || '...' AS counterparty_preview,
               COUNT(*) AS record_count, SUM(weight_value) AS weighted_count,
               COUNT(DISTINCT source_id) AS source_count,
               ROUND(AVG(CASE WHEN hour_value IS NOT NULL AND (hour_value >= 22 OR hour_value < 6) THEN 1.0 ELSE 0.0 END), 6) AS night_ratio
        FROM call_edges
        WHERE target_id IS NOT NULL
        GROUP BY 1
        ORDER BY record_count DESC
        LIMIT {top_k}
    """)

    return {
        "available": True,
        "record_count": total,
        "unique_call_sources": unique_sources,
        "unique_counterparties": unique_targets,
        "time_available": bool(time_available),
        "time_min": str(min_ts) if min_ts is not None else None,
        "time_max": str(max_ts) if max_ts is not None else None,
        "hour_known_records": hour_known,
        "night_record_count": night_count,
        "night_ratio": night_ratio,
        "daily_stats": daily_stats,
        "daily_overview": daily_df.to_dict(orient="records") if not daily_df.empty else [],
        "hourly_top": hourly_df.to_dict(orient="records") if not hourly_df.empty else [],
        "top_callers": top_callers_df.to_dict(orient="records"),
        "top_counterparties": top_targets_df.to_dict(orient="records"),
    }


def build_device_summary(conn: duckdb.DuckDBPyConnection, meta: Dict[str, Any], top_k: int) -> Dict[str, Any]:
    if not meta["available_views"].get("device_edges"):
        return {"available": False, "notes": ["未找到 edges_phone_imei 设备关系表。"]}

    edge_count = int(run_scalar(conn, "SELECT COUNT(*) FROM device_edges", 0) or 0)
    phone_count = int(run_scalar(conn, "SELECT COUNT(DISTINCT phone_id) FROM device_edges", 0) or 0)
    device_count = int(run_scalar(conn, "SELECT COUNT(DISTINCT device_id) FROM device_edges", 0) or 0)

    shared_device_count = int(run_scalar(conn, """
        SELECT COUNT(*) FROM (
            SELECT device_id, COUNT(DISTINCT phone_id) AS phone_count
            FROM device_edges
            GROUP BY device_id
            HAVING COUNT(DISTINCT phone_id) >= 2
        )
    """, 0) or 0)
    device_5_count = int(run_scalar(conn, """
        SELECT COUNT(*) FROM (
            SELECT device_id, COUNT(DISTINCT phone_id) AS phone_count
            FROM device_edges
            GROUP BY device_id
            HAVING COUNT(DISTINCT phone_id) >= 5
        )
    """, 0) or 0)
    device_10_count = int(run_scalar(conn, """
        SELECT COUNT(*) FROM (
            SELECT device_id, COUNT(DISTINCT phone_id) AS phone_count
            FROM device_edges
            GROUP BY device_id
            HAVING COUNT(DISTINCT phone_id) >= 10
        )
    """, 0) or 0)

    label_col = meta["picked_columns"].get("label")
    uid_col = meta["picked_columns"].get("user_id")
    if meta["available_views"].get("user_nodes") and label_col and uid_col:
        top_devices_sql = f"""
            SELECT d.device_id,
                   SUBSTRING(d.device_id, 1, 12) || '...' AS device_preview,
                   COUNT(DISTINCT d.phone_id) AS phone_count,
                   COUNT(DISTINCT CASE WHEN TRY_CAST(u.{qident(label_col)} AS DOUBLE) = 1 THEN d.phone_id END) AS risk_phone_count,
                   STRING_AGG(SUBSTRING(d.phone_id, 1, 12) || '...', ', ' ORDER BY d.phone_id) AS phone_preview
            FROM (
                SELECT DISTINCT device_id, phone_id FROM device_edges
            ) d
            LEFT JOIN user_nodes u ON CAST(u.{qident(uid_col)} AS VARCHAR) = d.phone_id
            GROUP BY d.device_id
            ORDER BY phone_count DESC, risk_phone_count DESC
            LIMIT {top_k}
        """
    else:
        top_devices_sql = f"""
            SELECT device_id,
                   SUBSTRING(device_id, 1, 12) || '...' AS device_preview,
                   COUNT(DISTINCT phone_id) AS phone_count,
                   NULL AS risk_phone_count,
                   STRING_AGG(SUBSTRING(phone_id, 1, 12) || '...', ', ' ORDER BY phone_id) AS phone_preview
            FROM (SELECT DISTINCT device_id, phone_id FROM device_edges)
            GROUP BY device_id
            ORDER BY phone_count DESC
            LIMIT {top_k}
        """
    top_devices_df = run_df(conn, top_devices_sql)

    top_phone_device_df = run_df(conn, f"""
        SELECT phone_id,
               SUBSTRING(phone_id, 1, 12) || '...' AS phone_preview,
               COUNT(DISTINCT device_id) AS device_count
        FROM device_edges
        GROUP BY phone_id
        ORDER BY device_count DESC
        LIMIT {top_k}
    """)

    return {
        "available": True,
        "edge_count": edge_count,
        "phones_with_device": phone_count,
        "device_count": device_count,
        "shared_device_count": shared_device_count,
        "device_with_5plus_phone_count": device_5_count,
        "device_with_10plus_phone_count": device_10_count,
        "top_shared_devices": top_devices_df.to_dict(orient="records"),
        "top_phone_device_counts": top_phone_device_df.to_dict(orient="records"),
    }


def build_quality_summary(paths: Dict[str, Optional[Path]], meta: Dict[str, Any], node_summary: Dict[str, Any], call_summary: Dict[str, Any], device_summary: Dict[str, Any]) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    def add(item: str, status: str, detail: str):
        rows.append({"item": item, "status": status, "detail": detail})

    add("user_nodes_file", "ok" if paths.get("user_nodes") else "missing", str(paths.get("user_nodes") or "未找到"))
    add("call_edges_file", "ok" if paths.get("call_edges") else "missing", str(paths.get("call_edges") or "未找到"))
    add("device_edges_file", "ok" if paths.get("device_edges") else "missing", str(paths.get("device_edges") or "未找到"))

    pc = meta.get("picked_columns", {})
    add("node_id_column", "ok" if pc.get("user_id") else "missing", str(pc.get("user_id") or "无法识别"))
    add("call_source_target_columns", "ok" if pc.get("call_src") and pc.get("call_dst") else "missing", f"src={pc.get('call_src')}, dst={pc.get('call_dst')}")
    add("time_column", "ok" if pc.get("event_time") or pc.get("hour_col") else "partial", f"event_time={pc.get('event_time')}, hour={pc.get('hour_col')}")
    add("device_columns", "ok" if pc.get("device_phone") and pc.get("device_id") else "missing", f"phone={pc.get('device_phone')}, device={pc.get('device_id')}")

    if node_summary.get("available"):
        dup = node_summary.get("duplicate_user_rows", 0)
        add("duplicate_user_rows", "ok" if dup == 0 else "warning", f"duplicate_rows={dup}")
    if call_summary.get("available"):
        add("call_time_coverage", "ok" if call_summary.get("time_available") else "partial", f"{call_summary.get('time_min')} ~ {call_summary.get('time_max')}")
    if device_summary.get("available"):
        add("shared_device_signal", "ok" if device_summary.get("shared_device_count", 0) > 0 else "weak", f"shared_device_count={device_summary.get('shared_device_count')}")

    missing_count = sum(1 for r in rows if r["status"] == "missing")
    warning_count = sum(1 for r in rows if r["status"] in ("warning", "partial", "weak"))
    return {"rows": rows, "missing_count": missing_count, "warning_count": warning_count}


def build_capability_summary(meta: Dict[str, Any], node_summary: Dict[str, Any], call_summary: Dict[str, Any], device_summary: Dict[str, Any]) -> Dict[str, Any]:
    caps: List[Dict[str, Any]] = []
    def cap(name: str, available: bool, basis: str, recommended_skill: str, note: str):
        caps.append({"capability": name, "available": bool(available), "basis": basis, "recommended_skill": recommended_skill, "note": note})

    node_available = node_summary.get("available", False)
    call_available = call_summary.get("available", False)
    device_available = device_summary.get("available", False)
    time_available = call_summary.get("time_available", False)
    label_available = bool(meta.get("picked_columns", {}).get("label"))
    province_available = bool(meta.get("picked_columns", {}).get("province"))

    cap("单号码画像与局部关系分析", node_available and call_available, "user_nodes + call_edges", "single-number-analysis", "可分析单号画像、联系人广度和局部邻居。")
    cap("TopN 高风险发现", node_available and call_available, "user_nodes + call_edges + label/device(optional)", "topn-high-risk-discovery", "可基于标签、通话广度、设备等信号排序。")
    cap("共享设备分析", device_available, "edges_phone_imei", "shared-device-analysis", "可分析共用设备、设备池和同设备扩散。")
    cap("两号码路径分析", call_available or device_available, "call_edges/device_edges/common_counterparty", "association-path-analysis", "可做通话路径、共同对端和共享设备复合路径。")
    cap("两号码重叠分析", call_available or device_available, "neighbor/common_neighbor/device", "overlap-analysis", "可比较共同对端和设备重叠。")
    cap("局部子图抽取", call_available or device_available, "neighbor_query + subgraph_extract", "subgraph-extraction-analysis", "可围绕单个号码抽取 1-2 跳局部关系圈。")
    cap("群体风险分析", call_available or device_available, "subgraph_by_nodes + aggregation", "group-risk-analysis", "可对号码集合归纳群体特征。")
    cap("团伙簇发现", call_available or device_available, "relation graph + clustering evidence", "gang-cluster-analysis", "可基于号码对、共享设备、共同对端识别团伙簇。")
    cap("条件筛选", node_available and (call_available or device_available), "node/filter/aggregation", "condition-based-screening", "可按标签、省份、夜间行为、联系人广度、共享设备筛目标。")
    cap("风险证据包", node_available and (call_available or device_available), "profile + relation evidence", "risk-evidence-pack", "可对单个号码汇总风险证据。")
    cap("时间序列异常分析", call_available and time_available, "call_edges.event_time/hour", "time-series-anomaly-analysis", "可分析阶段性活跃上升、夜间变化和异常日期。")
    cap("数据总体概览", node_available or call_available or device_available, "node/relation/device/time aggregation", "dataset-overview-analysis", "可做总体分布、数据质量和可分析能力概览。")

    available_count = sum(1 for c in caps if c["available"])
    return {"capabilities": caps, "available_count": available_count, "total_count": len(caps)}


# ---------------------------------------------------------------------------
# Output construction
# ---------------------------------------------------------------------------

def build_tables(node_summary: Dict[str, Any], call_summary: Dict[str, Any], device_summary: Dict[str, Any], quality: Dict[str, Any], capabilities: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
    overview_rows: List[Dict[str, Any]] = []
    def add(section: str, metric: str, value: Any, explanation: str = ""):
        overview_rows.append({"section": section, "metric": metric, "value": value, "explanation": explanation})

    add("node", "total_rows", node_summary.get("total_rows"), "节点表总行数")
    add("node", "distinct_users", node_summary.get("distinct_users"), "唯一号码数")
    add("node", "risk_count", node_summary.get("risk_count"), "label=1 的风险对象数")
    add("node", "risk_ratio", node_summary.get("risk_ratio"), "风险对象占比")
    add("call", "record_count", call_summary.get("record_count"), "通话关系记录数")
    add("call", "unique_call_sources", call_summary.get("unique_call_sources"), "作为主叫/源号码出现的唯一数量")
    add("call", "unique_counterparties", call_summary.get("unique_counterparties"), "对端唯一数量")
    add("call", "time_min", call_summary.get("time_min"), "最早通话时间")
    add("call", "time_max", call_summary.get("time_max"), "最晚通话时间")
    add("call", "night_ratio", call_summary.get("night_ratio"), "夜间记录占比，按可识别小时记录计算")
    add("call", "active_days", (call_summary.get("daily_stats") or {}).get("active_days"), "有通话记录的日期数量")
    add("call", "calendar_span_days", (call_summary.get("daily_stats") or {}).get("calendar_span_days"), "最早到最晚通话日期之间的日历跨度")
    add("call", "active_day_ratio", (call_summary.get("daily_stats") or {}).get("active_day_ratio"), "有记录日期 / 日历跨度")
    add("device", "edge_count", device_summary.get("edge_count"), "号码-设备关系记录数")
    add("device", "device_count", device_summary.get("device_count"), "唯一设备数")
    add("device", "shared_device_count", device_summary.get("shared_device_count"), "至少关联2个号码的设备数")
    add("device", "device_with_5plus_phone_count", device_summary.get("device_with_5plus_phone_count"), "至少关联5个号码的设备数")
    add("capability", "available_count", capabilities.get("available_count"), "当前可用分析能力数量")
    add("quality", "missing_count", quality.get("missing_count"), "缺失关键项数量")
    add("quality", "warning_count", quality.get("warning_count"), "警告或部分可用项数量")

    tables: Dict[str, pd.DataFrame] = {
        "overview_counts": pd.DataFrame(overview_rows),
        "label_distribution": pd.DataFrame(node_summary.get("label_distribution", [])),
        "sub_label_distribution": pd.DataFrame(node_summary.get("sub_label_distribution", [])),
        "province_distribution": pd.DataFrame(node_summary.get("province_distribution", [])),
        "daily_overview": pd.DataFrame(call_summary.get("daily_overview", [])),
        "hourly_top": pd.DataFrame(call_summary.get("hourly_top", [])),
        "top_callers": pd.DataFrame(call_summary.get("top_callers", [])),
        "top_counterparties": pd.DataFrame(call_summary.get("top_counterparties", [])),
        "top_shared_devices": pd.DataFrame(device_summary.get("top_shared_devices", [])),
        "top_phone_device_counts": pd.DataFrame(device_summary.get("top_phone_device_counts", [])),
        "data_quality": pd.DataFrame(quality.get("rows", [])),
        "available_capabilities": pd.DataFrame(capabilities.get("capabilities", [])),
    }
    return tables


def dataframe_to_md_list(df: pd.DataFrame, columns: List[str], limit: int, format_row) -> List[str]:
    if df is None or df.empty:
        return ["- 暂无可展示记录。"]
    rows = []
    for _, row in df.head(limit).iterrows():
        rows.append(format_row(row))
    return rows


def render_report(dataset: str, dataset_root: Path, paths: Dict[str, Optional[Path]], node_summary: Dict[str, Any], call_summary: Dict[str, Any], device_summary: Dict[str, Any], quality: Dict[str, Any], capabilities: Dict[str, Any], tables: Dict[str, pd.DataFrame], output_files: Dict[str, str], top_k: int) -> str:
    lines: List[str] = []
    lines.append(f"# 电话网络数据集总体概览：{dataset}")
    lines.append("")
    lines.append("## 一、核心结论")
    lines.append("")
    lines.append(f"- 数据集：`{dataset}`")
    lines.append(f"- 数据根目录：`{dataset_root}`")
    lines.append(f"- 节点表：`{paths.get('user_nodes') or '未找到'}`")
    lines.append(f"- 通话边表：`{paths.get('call_edges') or '未找到'}`")
    lines.append(f"- 设备边表：`{paths.get('device_edges') or '未找到'}`")
    if node_summary.get("available"):
        risk_text = "未知"
        if node_summary.get("risk_count") is not None:
            risk_text = f"{node_summary.get('risk_count')} 个，占比 {round((node_summary.get('risk_ratio') or 0) * 100, 4)}%"
        lines.append(f"- 对象规模：唯一号码 `{node_summary.get('distinct_users')}` 个；风险对象：{risk_text}。")
    if call_summary.get("available"):
        lines.append(f"- 通话关系规模：记录 `{call_summary.get('record_count')}` 条；源号码 `{call_summary.get('unique_call_sources')}` 个；对端 `{call_summary.get('unique_counterparties')}` 个。")
        if call_summary.get("time_available"):
            lines.append(f"- 时间覆盖：`{call_summary.get('time_min')}` ~ `{call_summary.get('time_max')}`；夜间记录占比 `{call_summary.get('night_ratio')}`。")
    if device_summary.get("available"):
        lines.append(f"- 设备关系规模：号码-设备关系 `{device_summary.get('edge_count')}` 条，唯一设备 `{device_summary.get('device_count')}` 台，共享设备 `{device_summary.get('shared_device_count')}` 台。")
    lines.append(f"- 可用分析能力：{capabilities.get('available_count')}/{capabilities.get('total_count')} 项。")
    if quality.get("missing_count", 0) or quality.get("warning_count", 0):
        lines.append(f"- 数据质量提示：缺失项 `{quality.get('missing_count')}` 个，警告/部分可用项 `{quality.get('warning_count')}` 个。")
    else:
        lines.append("- 数据质量提示：关键文件和关键字段均已识别。")

    lines.append("")
    lines.append("## 二、对象规模与风险分布")
    lines.append("")
    if node_summary.get("available"):
        lines.append(f"- 节点表总行数：`{node_summary.get('total_rows')}`")
        lines.append(f"- 唯一号码数：`{node_summary.get('distinct_users')}`")
        lines.append(f"- 重复号码行数：`{node_summary.get('duplicate_user_rows')}`")
        if node_summary.get("risk_count") is not None:
            lines.append(f"- 风险对象数：`{node_summary.get('risk_count')}`，风险占比：`{node_summary.get('risk_ratio')}`")
        prov_df = tables.get("province_distribution", pd.DataFrame())
        if prov_df is not None and not prov_df.empty:
            lines.append("- 省份 Top 分布：")
            for _, row in prov_df.head(min(5, top_k)).iterrows():
                lines.append(f"  - `{row.get('province')}`：{row.get('count')} 个，占比 {row.get('pct')}%")
        sub_df = tables.get("sub_label_distribution", pd.DataFrame())
        if sub_df is not None and not sub_df.empty:
            lines.append("- sub_label Top 分布：")
            for _, row in sub_df.head(min(5, top_k)).iterrows():
                lines.append(f"  - `{row.get('sub_label')}`：{row.get('count')} 个，占比 {row.get('pct')}%")
    else:
        lines.append("- 未找到节点表，无法统计对象规模和风险分布。")

    lines.append("")
    lines.append("## 三、通话关系规模与时间覆盖")
    lines.append("")
    if call_summary.get("available"):
        lines.append(f"- 通话记录数：`{call_summary.get('record_count')}`")
        lines.append(f"- 源号码数：`{call_summary.get('unique_call_sources')}`")
        lines.append(f"- 对端数：`{call_summary.get('unique_counterparties')}`")
        if call_summary.get("time_available"):
            lines.append(f"- 时间范围：`{call_summary.get('time_min')}` ~ `{call_summary.get('time_max')}`")
            ds = call_summary.get("daily_stats", {}) or {}
            if ds:
                span_text = f"；原始日历跨度 `{ds.get('calendar_span_days')}` 天" if ds.get('calendar_span_days') else ""
                ratio_text = f"；活跃日覆盖率 `{ds.get('active_day_ratio')}`" if ds.get('active_day_ratio') is not None else ""
                lines.append(f"- 日级覆盖：有记录日期 `{ds.get('active_days', ds.get('days'))}` 天{span_text}{ratio_text}；有记录日期日均记录 `{ds.get('avg_daily_records')}`；单日最高 `{ds.get('max_daily_records')}`。")
                lines.append("- 说明：这里的有记录日期不等于连续日历天数；用于判断数据活跃覆盖，不代表每天都有通话记录。")
        else:
            lines.append("- 未识别到可用时间字段，时间序列类分析能力会受限。")
        lines.append(f"- 可识别小时记录数：`{call_summary.get('hour_known_records')}`；夜间记录数：`{call_summary.get('night_record_count')}`；夜间占比：`{call_summary.get('night_ratio')}`")
    else:
        lines.append("- 未找到通话边表，无法统计通话关系规模。")

    lines.append("")
    lines.append("## 四、共享设备关系概览")
    lines.append("")
    if device_summary.get("available"):
        lines.append(f"- 号码-设备关系数：`{device_summary.get('edge_count')}`")
        lines.append(f"- 关联设备的号码数：`{device_summary.get('phones_with_device')}`")
        lines.append(f"- 唯一设备数：`{device_summary.get('device_count')}`")
        lines.append(f"- 共享设备数（至少2个号码）：`{device_summary.get('shared_device_count')}`")
        lines.append(f"- 关联5个及以上号码的设备数：`{device_summary.get('device_with_5plus_phone_count')}`")
        lines.append(f"- 关联10个及以上号码的设备数：`{device_summary.get('device_with_10plus_phone_count')}`")
        dev_df = tables.get("top_shared_devices", pd.DataFrame())
        if dev_df is not None and not dev_df.empty:
            lines.append("- Top 共享设备样例：")
            for _, row in dev_df.head(min(5, top_k)).iterrows():
                lines.append(f"  - 设备 `{row.get('device_preview')}` | 挂载号码={row.get('phone_count')} | 风险号码={row.get('risk_phone_count')}")
    else:
        lines.append("- 未找到设备边表，无法分析共享设备关系。")

    lines.append("")
    lines.append("## 五、重点活跃对象与公共对端样例")
    lines.append("")
    caller_df = tables.get("top_callers", pd.DataFrame())
    if caller_df is not None and not caller_df.empty:
        lines.append("- Top 活跃源号码：")
        for _, row in caller_df.head(min(5, top_k)).iterrows():
            lines.append(f"  - 号码 `{row.get('node_preview')}` | 记录数={row.get('record_count')} | 联系人={row.get('counterparty_count')} | 夜间占比={row.get('night_ratio')}")
    target_df = tables.get("top_counterparties", pd.DataFrame())
    if target_df is not None and not target_df.empty:
        lines.append("- Top 公共对端：")
        for _, row in target_df.head(min(5, top_k)).iterrows():
            lines.append(f"  - 对端 `{row.get('counterparty_preview')}` | 记录数={row.get('record_count')} | 连接源号码数={row.get('source_count')} | 夜间占比={row.get('night_ratio')}")

    lines.append("")
    lines.append("## 六、可分析能力范围")
    lines.append("")
    cap_df = tables.get("available_capabilities", pd.DataFrame())
    if cap_df is not None and not cap_df.empty:
        for _, row in cap_df.iterrows():
            status = "可用" if row.get("available") else "受限"
            lines.append(f"- `{row.get('recommended_skill')}`：{status}。{row.get('note')}")

    lines.append("")
    lines.append("## 七、数据质量检查")
    lines.append("")
    qdf = tables.get("data_quality", pd.DataFrame())
    if qdf is not None and not qdf.empty:
        for _, row in qdf.iterrows():
            lines.append(f"- `{row.get('item')}`：{row.get('status')}；{row.get('detail')}")

    lines.append("")
    lines.append("## 八、建议的后续分析入口")
    lines.append("")
    lines.append("- 想先了解单个号码：使用 `single-number-analysis` 或 `risk-evidence-pack`。")
    lines.append("- 想找重点对象名单：使用 `topn-high-risk-discovery` 或 `condition-based-screening`。")
    lines.append("- 想看共用设备：使用 `shared-device-analysis`。")
    lines.append("- 想分析两个号码关系：使用 `association-path-analysis` 或 `overlap-analysis`。")
    lines.append("- 想分析一组号码：使用 `group-risk-analysis` 或 `gang-cluster-analysis`。")
    lines.append("- 想分析阶段性变化：使用 `time-series-anomaly-analysis`。")

    lines.append("")
    lines.append("## 九、基础算子对齐")
    lines.append("")
    lines.append("- 对象规模与标签分布 = `node_lookup + aggregation_query`")
    lines.append("- 关系规模与公共对端 = `relationship_filter + aggregation_query`")
    lines.append("- 共享设备概览 = `query_shared_device + aggregation_query`")
    lines.append("- 时间覆盖与小时分布 = `relationship_filter(time window/hour) + aggregation_query`")
    lines.append("- 可分析能力范围 = 基于现有基础算子能力映射生成")

    lines.append("")
    lines.append("## 十、生成文件")
    lines.append("")
    for key, path in output_files.items():
        lines.append(f"- `{key}`：`{Path(path).name}`")
    lines.append("")
    return "\n".join(lines)



def render_presentation_report(dataset: str, dataset_root: Path, node_summary: Dict[str, Any], call_summary: Dict[str, Any], device_summary: Dict[str, Any], quality: Dict[str, Any], capabilities: Dict[str, Any], tables: Dict[str, pd.DataFrame]) -> str:
    """Render a concise presentation-oriented overview report.

    This report is intentionally less technical than the full markdown report. It avoids
    subjective uncomputed scores and highlights only evidence that comes from the data.
    """
    node_count = node_summary.get("distinct_users")
    risk_count = node_summary.get("risk_count")
    risk_pct = round((node_summary.get("risk_ratio") or 0) * 100, 2) if node_summary.get("risk_ratio") is not None else None
    record_count = call_summary.get("record_count")
    counterparty_count = call_summary.get("unique_counterparties")
    ds = call_summary.get("daily_stats", {}) or {}
    device_count = device_summary.get("device_count")
    shared_device_count = device_summary.get("shared_device_count")
    device_5 = device_summary.get("device_with_5plus_phone_count")
    device_10 = device_summary.get("device_with_10plus_phone_count")
    time_min = call_summary.get("time_min")
    time_max = call_summary.get("time_max")
    active_days = ds.get("active_days", ds.get("days"))
    calendar_span = ds.get("calendar_span_days")
    night_pct = round((call_summary.get("night_ratio") or 0) * 100, 2) if call_summary.get("night_ratio") is not None else None

    lines: List[str] = []
    lines.append(f"# 电话网络数据总览报告：{dataset}")
    lines.append("")
    lines.append("> 面向演示开场和分析入口，用于回答“数据里有什么、能做什么”。")
    lines.append("")
    lines.append("## 一、数据里有什么")
    lines.append("")
    lines.append("| 维度 | 规模 | 说明 |")
    lines.append("|---|---:|---|")
    lines.append(f"| 号码对象 | {node_count if node_count is not None else '未知'} | 风险对象 {risk_count if risk_count is not None else '未知'} 个，占比 {risk_pct if risk_pct is not None else '未知'}% |")
    lines.append(f"| 通话关系 | {record_count if record_count is not None else '未知'} | 覆盖对端 {counterparty_count if counterparty_count is not None else '未知'} 个 |")
    lines.append(f"| 共享设备 | {shared_device_count if shared_device_count is not None else '未知'} | 唯一设备 {device_count if device_count is not None else '未知'} 台，5+挂载设备 {device_5 if device_5 is not None else '未知'} 台，10+挂载设备 {device_10 if device_10 is not None else '未知'} 台 |")
    if time_min and time_max:
        lines.append(f"| 时间覆盖 | {active_days if active_days is not None else '未知'} 个有记录日期 | 原始时间范围 {str(time_min)[:10]} ~ {str(time_max)[:10]}，日历跨度 {calendar_span if calendar_span is not None else '未知'} 天 |")
    lines.append("")

    lines.append("## 二、风险对象分布")
    lines.append("")
    sub_df = tables.get("sub_label_distribution", pd.DataFrame())
    if sub_df is not None and not sub_df.empty:
        lines.append("| sub_label | 数量 | 占比 |")
        lines.append("|---|---:|---:|")
        for _, row in sub_df.iterrows():
            lines.append(f"| {row.get('sub_label')} | {row.get('count')} | {row.get('pct')}% |")
    prov_df = tables.get("province_distribution", pd.DataFrame())
    if prov_df is not None and not prov_df.empty:
        lines.append("")
        lines.append("省份分布：")
        for _, row in prov_df.iterrows():
            lines.append(f"- {row.get('province')}：{row.get('count')} 个，占比 {row.get('pct')}%")
    lines.append("")

    lines.append("## 三、主要行为与设备信号")
    lines.append("")
    if call_summary.get("available"):
        lines.append(f"- 日均通话记录：{(call_summary.get('daily_stats') or {}).get('avg_daily_records')}（按有记录日期统计）")
        lines.append(f"- 单日最高通话记录：{(call_summary.get('daily_stats') or {}).get('max_daily_records')}")
        lines.append(f"- 夜间通话记录：{call_summary.get('night_record_count')}，占比 {night_pct}%")
        lines.append("- 时间覆盖说明：当前数据存在跨年时间范围，但实际有记录日期不是连续全量日历覆盖。")
    dev_df = tables.get("top_shared_devices", pd.DataFrame())
    if dev_df is not None and not dev_df.empty:
        lines.append("")
        lines.append("Top 共享设备样例：")
        for _, row in dev_df.head(5).iterrows():
            risk_cnt = row.get("risk_phone_count")
            phone_cnt = row.get("phone_count")
            risk_rate = None
            try:
                risk_rate = round(float(risk_cnt) / float(phone_cnt) * 100, 2) if phone_cnt else None
            except Exception:
                risk_rate = None
            extra = f"，风险率 {risk_rate}%" if risk_rate is not None else ""
            lines.append(f"- 设备 {row.get('device_preview')}：挂载号码 {phone_cnt} 个，风险号码 {risk_cnt}{extra}")
    lines.append("")

    lines.append("## 四、能做什么")
    lines.append("")
    cap_df = tables.get("available_capabilities", pd.DataFrame())
    if cap_df is not None and not cap_df.empty:
        lines.append(f"当前可用分析能力：{capabilities.get('available_count')}/{capabilities.get('total_count')} 项。")
        lines.append("")
        lines.append("| 能力 | 推荐 skill | 状态 |")
        lines.append("|---|---|---|")
        for _, row in cap_df.iterrows():
            status = "可用" if bool(row.get("available")) else "受限"
            lines.append(f"| {row.get('capability')} | {row.get('recommended_skill')} | {status} |")
    lines.append("")

    lines.append("## 五、推荐分析链路")
    lines.append("")
    lines.append("1. 先用 `dataset-overview-analysis` 查看数据规模、质量和可用能力。")
    lines.append("2. 用 `topn-high-risk-discovery` 或 `condition-based-screening` 发现重点对象。")
    lines.append("3. 用 `risk-evidence-pack` 或 `single-number-analysis` 对重点号码生成证据。")
    lines.append("4. 如果关注设备池或团伙结构，继续使用 `shared-device-analysis`、`group-risk-analysis`、`gang-cluster-analysis`。")
    lines.append("5. 如果关注阶段性变化，继续使用 `time-series-anomaly-analysis`。")
    lines.append("")

    lines.append("## 六、数据质量提示")
    lines.append("")
    lines.append(f"- 缺失关键项：{quality.get('missing_count')} 个。")
    lines.append(f"- 警告或部分可用项：{quality.get('warning_count')} 个。")
    if node_summary.get("duplicate_user_rows"):
        lines.append(f"- 节点表存在 {node_summary.get('duplicate_user_rows')} 行重复号码记录，已作为质量提示保留。")
    lines.append("")
    lines.append("## 七、交付文件")
    lines.append("")
    lines.append("- 技术报告：`dataset_overview_<dataset>.md`")
    lines.append("- 演示报告：`dataset_overview_<dataset>_presentation.md`")
    lines.append("- 结构化摘要：`dataset_overview_<dataset>_summary.json`")
    lines.append("- 证据工作簿：`dataset_overview_<dataset>_evidence.xlsx`")
    lines.append("- 明细表：`dataset_overview_<dataset>_*.csv`")
    lines.append("")
    return "\n".join(lines)

def write_outputs(output_dir: Path, dataset: str, report_md: str, presentation_md: str, tables: Dict[str, pd.DataFrame], summary: Dict[str, Any]) -> Dict[str, str]:
    prefix = f"dataset_overview_{dataset}"
    files: Dict[str, str] = {}

    report_path = output_dir / f"{prefix}.md"
    report_path.write_text(report_md, encoding="utf-8")
    files["report_md"] = str(report_path)

    presentation_path = output_dir / f"{prefix}_presentation.md"
    presentation_path.write_text(presentation_md, encoding="utf-8")
    files["presentation_md"] = str(presentation_path)

    csv_map = {
        "overview_counts": "overview_counts",
        "label_distribution": "label_distribution",
        "sub_label_distribution": "sub_label_distribution",
        "province_distribution": "province_distribution",
        "daily_overview": "daily_overview",
        "hourly_top": "hourly_top",
        "top_callers": "top_callers",
        "top_counterparties": "top_counterparties",
        "top_shared_devices": "top_shared_devices",
        "top_phone_device_counts": "top_phone_device_counts",
        "data_quality": "data_quality",
        "available_capabilities": "available_capabilities",
    }
    for key, table_key in csv_map.items():
        df = tables.get(table_key, pd.DataFrame())
        out = output_dir / f"{prefix}_{key}.csv"
        df.to_csv(out, index=False, encoding="utf-8-sig")
        files[f"{key}_csv"] = str(out)

    summary_path = output_dir / f"{prefix}_summary.json"
    summary_path.write_text(json.dumps(to_jsonable(summary), ensure_ascii=False, indent=2), encoding="utf-8")
    files["summary_json"] = str(summary_path)

    xlsx_path = output_dir / f"{prefix}_evidence.xlsx"
    try:
        with pd.ExcelWriter(xlsx_path) as writer:
            for sheet_name, df in tables.items():
                safe_sheet = sheet_name[:31]
                if df is None or df.empty:
                    pd.DataFrame([{"empty": True}]).to_excel(writer, sheet_name=safe_sheet, index=False)
                else:
                    df.to_excel(writer, sheet_name=safe_sheet, index=False)
        files["evidence_xlsx"] = str(xlsx_path)
    except Exception as exc:
        files["evidence_xlsx_error"] = str(exc)

    return files


def build_artifacts(files: Dict[str, str]) -> List[Dict[str, str]]:
    artifacts: List[Dict[str, str]] = []
    for key, path in files.items():
        if key.endswith("_error"):
            continue
        suffix = Path(path).suffix.lower()
        if suffix == ".md":
            typ = "markdown_report"
        elif suffix == ".csv":
            typ = "csv"
        elif suffix == ".json":
            typ = "json"
        elif suffix == ".xlsx":
            typ = "xlsx"
        else:
            typ = "file"
        artifacts.append({"type": typ, "path": path, "title": Path(path).name})
    return artifacts


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="电话网络数据集总体概览分析")
    parser.add_argument("--dataset-root", default=None, help="电话网络数据集根目录。未指定时自动查找 /workspace/imiss-deer-flow-main/datasets/phone-network。")
    parser.add_argument("--dataset", default=DEFAULT_DATASET, help="数据集视图名称，默认 unified。")
    parser.add_argument("--top-k", type=int, default=20, help="各类 Top 列表展示数量。")
    parser.add_argument("--output-dir", default=None, help="输出目录，默认 /mnt/user-data/outputs。")
    parser.add_argument("--pretty", action="store_true", help="保留参数，用于兼容。")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = resolve_output_dir(args.output_dir)
    result: Dict[str, Any]

    try:
        dataset_root = resolve_dataset_root(args.dataset_root)
        paths = resolve_dataset_paths(dataset_root, args.dataset)

        conn = duckdb.connect(database=":memory:")
        meta = setup_views(conn, paths)

        node_summary = build_node_summary(conn, meta, args.top_k)
        call_summary = build_call_summary(conn, meta, args.top_k)
        device_summary = build_device_summary(conn, meta, args.top_k)
        quality = build_quality_summary(paths, meta, node_summary, call_summary, device_summary)
        capabilities = build_capability_summary(meta, node_summary, call_summary, device_summary)
        tables = build_tables(node_summary, call_summary, device_summary, quality, capabilities)

        # Build summary before report files are known.
        status = "ok"
        if quality.get("missing_count", 0) >= 2:
            status = "partial"

        summary = {
            "ok": True,
            "skill": SKILL_NAME,
            "query_type": QUERY_TYPE,
            "script_version": SCRIPT_VERSION,
            "dataset": args.dataset,
            "dataset_root": str(dataset_root),
            "status": status,
            "input_summary": {
                "dataset_root": str(dataset_root),
                "dataset": args.dataset,
                "top_k": args.top_k,
                "paths": {k: str(v) if v else None for k, v in paths.items()},
            },
            "result": {
                "node_summary": node_summary,
                "call_summary": {k: v for k, v in call_summary.items() if k not in ("daily_overview", "top_callers", "top_counterparties", "hourly_top")},
                "device_summary": {k: v for k, v in device_summary.items() if k not in ("top_shared_devices", "top_phone_device_counts")},
                "quality_summary": {"missing_count": quality.get("missing_count"), "warning_count": quality.get("warning_count")},
                "capability_summary": {"available_count": capabilities.get("available_count"), "total_count": capabilities.get("total_count")},
                "top_signal_summary": [],
                "recommended_next_steps": [],
            },
            "base_operator_alignment": {
                "node_distribution": "node_lookup + aggregation_query",
                "relation_scale": "relationship_filter + aggregation_query",
                "device_overview": "query_shared_device + aggregation_query",
                "time_coverage": "relationship_filter(time window/hour) + aggregation_query",
                "capability_mapping": "base operator registry + skill routing layer",
            },
            "notes": [],
        }

        top_signal_summary: List[str] = []
        if node_summary.get("available"):
            top_signal_summary.append(f"唯一号码 {node_summary.get('distinct_users')} 个，风险对象 {node_summary.get('risk_count')} 个。")
        if call_summary.get("available"):
            top_signal_summary.append(f"通话关系 {call_summary.get('record_count')} 条，覆盖源号码 {call_summary.get('unique_call_sources')} 个。")
        if device_summary.get("available"):
            top_signal_summary.append(f"共享设备 {device_summary.get('shared_device_count')} 台，其中关联5个及以上号码的设备 {device_summary.get('device_with_5plus_phone_count')} 台。")
        top_signal_summary.append(f"当前可用分析能力 {capabilities.get('available_count')}/{capabilities.get('total_count')} 项。")
        summary["result"]["top_signal_summary"] = top_signal_summary
        summary["result"]["recommended_next_steps"] = [
            "先用 topn-high-risk-discovery 或 condition-based-screening 发现重点对象。",
            "再用 risk-evidence-pack / single-number-analysis 对重点号码生成证据包。",
            "若关注设备池或群体结构，继续使用 shared-device-analysis、group-risk-analysis、gang-cluster-analysis。",
            "若关注阶段性变化，继续使用 time-series-anomaly-analysis。",
        ]

        # Render report first without files, write later, then update report not necessary.
        tmp_files = {
            "report_md": f"dataset_overview_{args.dataset}.md",
            "presentation_md": f"dataset_overview_{args.dataset}_presentation.md",
            "summary_json": f"dataset_overview_{args.dataset}_summary.json",
            "evidence_xlsx": f"dataset_overview_{args.dataset}_evidence.xlsx",
            "overview_counts_csv": f"dataset_overview_{args.dataset}_overview_counts.csv",
            "available_capabilities_csv": f"dataset_overview_{args.dataset}_available_capabilities.csv",
        }
        report = render_report(args.dataset, dataset_root, paths, node_summary, call_summary, device_summary, quality, capabilities, tables, tmp_files, args.top_k)
        presentation_report = render_presentation_report(args.dataset, dataset_root, node_summary, call_summary, device_summary, quality, capabilities, tables)
        files = write_outputs(output_dir, args.dataset, report, presentation_report, tables, summary)
        summary["artifacts"] = build_artifacts(files)
        summary["report_path"] = files.get("report_md")
        summary["files"] = files

        # Re-write summary with artifact paths.
        if files.get("summary_json"):
            Path(files["summary_json"]).write_text(json.dumps(to_jsonable(summary), ensure_ascii=False, indent=2), encoding="utf-8")

        print(json.dumps(to_jsonable(summary), ensure_ascii=False, indent=2))

    except Exception as exc:
        error_summary = {
            "ok": False,
            "skill": SKILL_NAME,
            "query_type": QUERY_TYPE,
            "script_version": SCRIPT_VERSION,
            "status": "error",
            "error": str(exc),
            "notes": ["数据集概览分析执行失败，请检查 dataset-root、数据文件路径和依赖环境。"],
        }
        print(json.dumps(to_jsonable(error_summary), ensure_ascii=False, indent=2))
        sys.exit(1)


if __name__ == "__main__":
    main()
