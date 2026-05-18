#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Dataset quality and linkability diagnostic for phone-network graph datasets.

This script reads a processed phone-network dataset and answers:
- Is the dataset graph-ready?
- Which downstream skills can be safely used?
- Are the identity namespaces linkable across provinces/data sources?
- What files/columns/quality issues are missing?

It is intentionally read-only: it never modifies source data.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

try:
    import duckdb  # type: ignore
except Exception:  # pragma: no cover
    duckdb = None

SCRIPT_VERSION = "dataset-quality-and-linkability-diagnostic-release-v1.2"
DEFAULT_OUTPUT_DIR = Path("/mnt/user-data/outputs")

PHONE_COLS = ["user_id", "phone_id", "phone", "mobile", "phone_number", "手机号", "用户号码", "号码"]
CALL_SRC_COLS = ["src_user_id", "source", "src", "caller", "主叫", "主叫号码", "phone", "user_id"]
CALL_DST_COLS = ["dst_counterparty_id", "target", "dst", "callee", "被叫", "被叫号码", "counterparty", "peer"]
DEVICE_USER_COLS = ["user_id", "src_id", "phone", "mobile", "phone_number", "手机号", "用户号码"]
DEVICE_COLS = ["imei", "device_id", "dst_id", "device", "terminal_id", "设备号", "终端", "终端号"]
PROVINCE_COLS = ["province", "归属省", "省份", "归属地省", "area_province"]
TIME_COLS = ["event_time", "call_time", "通话时间", "start_datetime", "timestamp", "time"]
DATE_COLS = ["event_date", "call_date", "日期"]
HOUR_COLS = ["event_hour", "call_hour", "hour", "小时"]
LABEL_COLS = ["label", "风险标签", "risk_label"]
SUB_LABEL_COLS = ["sub_label", "风险类型", "risk_type"]


def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        if isinstance(x, float) and math.isnan(x):
            return default
        return float(x)
    except Exception:
        return default


def safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(x)
    except Exception:
        return default


def json_safe(obj: Any) -> Any:
    """Convert pandas/numpy/duckdb-returned scalar objects to JSON-safe values."""
    if isinstance(obj, dict):
        return {str(k): json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [json_safe(v) for v in obj]
    if isinstance(obj, tuple):
        return [json_safe(v) for v in obj]
    if isinstance(obj, (datetime, date)):
        return obj.isoformat(sep=" ") if isinstance(obj, datetime) else obj.isoformat()
    if isinstance(obj, Path):
        return str(obj)
    # pandas Timestamp / NaT and numpy scalar compatibility
    if hasattr(obj, "isoformat") and obj.__class__.__name__ in {"Timestamp", "NaTType"}:
        try:
            if str(obj) == "NaT":
                return None
            return obj.isoformat()
        except Exception:
            pass
    if hasattr(obj, "item"):
        try:
            return json_safe(obj.item())
        except Exception:
            pass
    if isinstance(obj, float) and math.isnan(obj):
        return None
    # Final guard: if stdlib json still cannot encode it, stringify instead of crashing.
    try:
        json.dumps(obj, ensure_ascii=False)
        return obj
    except TypeError:
        return str(obj)


def q(path: Path) -> str:
    return str(path).replace("'", "''")


def first_existing(columns: Sequence[str], candidates: Sequence[str]) -> Optional[str]:
    lower = {c.lower(): c for c in columns}
    for cand in candidates:
        if cand in columns:
            return cand
        if cand.lower() in lower:
            return lower[cand.lower()]
    return None


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


class DuckHelper:
    def __init__(self) -> None:
        if duckdb is None:
            raise RuntimeError("duckdb is required for this diagnostic script")
        self.conn = duckdb.connect(database=":memory:")

    def register_file(self, view: str, path: Path) -> bool:
        if not path.exists():
            return False
        suffix = path.suffix.lower()
        if suffix == ".csv":
            sql = f"CREATE OR REPLACE VIEW {view} AS SELECT * FROM read_csv_auto('{q(path)}', header=true, ignore_errors=true, all_varchar=true)"
        elif suffix == ".parquet":
            sql = f"CREATE OR REPLACE VIEW {view} AS SELECT * FROM read_parquet('{q(path)}')"
        elif suffix in {".json", ".jsonl"}:
            sql = f"CREATE OR REPLACE VIEW {view} AS SELECT * FROM read_json_auto('{q(path)}')"
        else:
            return False
        self.conn.execute(sql)
        return True

    def cols(self, view: str) -> List[str]:
        try:
            rows = self.conn.execute(f"DESCRIBE SELECT * FROM {view}").fetchall()
            return [str(r[0]) for r in rows]
        except Exception:
            return []

    def scalar(self, sql: str, default: Any = None) -> Any:
        try:
            row = self.conn.execute(sql).fetchone()
            return row[0] if row else default
        except Exception:
            return default

    def rows(self, sql: str) -> List[Dict[str, Any]]:
        try:
            df = self.conn.execute(sql).fetchdf()
            return df.to_dict(orient="records")
        except Exception:
            return []


def resolve_paths(dataset_root: Path, dataset: str) -> Dict[str, Path]:
    processed = dataset_root / "processed" / dataset
    graph_view = dataset_root / "processed" / "graph_views" / dataset
    dev_parquet = graph_view / "edges_phone_imei.parquet"
    dev_csv = graph_view / "edges_phone_imei.csv"
    return {
        "processed_dir": processed,
        "graph_view_dir": graph_view,
        "user_nodes": processed / "user_nodes.csv",
        "call_edges": processed / "call_edges.csv",
        "device_edges": dev_parquet if dev_parquet.exists() else dev_csv,
        "device_edges_parquet": dev_parquet,
        "device_edges_csv": dev_csv,
    }


def make_expr_col(col: Optional[str]) -> str:
    if not col:
        return "NULL"
    return f'CAST("{col}" AS VARCHAR)'


def register_views(dh: DuckHelper, paths: Dict[str, Path]) -> Dict[str, Any]:
    info: Dict[str, Any] = {
        "registered": {},
        "columns": {},
        "exists": {},
    }
    for key, view in [("user_nodes", "users"), ("call_edges", "calls"), ("device_edges", "devices")]:
        exists = paths[key].exists()
        info["exists"][key] = exists
        if exists:
            ok = dh.register_file(view, paths[key])
            info["registered"][key] = ok
            info["columns"][key] = dh.cols(view) if ok else []
        else:
            info["registered"][key] = False
            info["columns"][key] = []
    return info


def detect_schema(columns: Dict[str, List[str]]) -> Dict[str, Any]:
    u_cols = columns.get("user_nodes", [])
    c_cols = columns.get("call_edges", [])
    d_cols = columns.get("device_edges", [])
    return {
        "users": {
            "user_id": first_existing(u_cols, PHONE_COLS),
            "province": first_existing(u_cols, PROVINCE_COLS),
            "label": first_existing(u_cols, LABEL_COLS),
            "sub_label": first_existing(u_cols, SUB_LABEL_COLS),
        },
        "calls": {
            "src_user_id": first_existing(c_cols, CALL_SRC_COLS),
            "dst_counterparty_id": first_existing(c_cols, CALL_DST_COLS),
            "province": first_existing(c_cols, PROVINCE_COLS),
            "event_time": first_existing(c_cols, TIME_COLS),
            "event_date": first_existing(c_cols, DATE_COLS),
            "event_hour": first_existing(c_cols, HOUR_COLS),
            "duration": first_existing(c_cols, ["duration", "duration_sec", "call_duration", "通话时长", "seconds"]),
            "imei": first_existing(c_cols, DEVICE_COLS),
        },
        "devices": {
            "user_id": first_existing(d_cols, DEVICE_USER_COLS),
            "imei": first_existing(d_cols, DEVICE_COLS),
        },
    }


def summarize_tables(dh: DuckHelper, reg: Dict[str, Any], schema: Dict[str, Any]) -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        "user_rows": 0,
        "distinct_users": 0,
        "call_rows": 0,
        "device_edges": 0,
        "distinct_devices": 0,
        "province_count": 0,
        "provinces": [],
        "time_coverage": {},
    }
    if reg["registered"].get("user_nodes"):
        uid = schema["users"].get("user_id")
        prov = schema["users"].get("province")
        summary["user_rows"] = safe_int(dh.scalar("SELECT COUNT(*) FROM users", 0))
        if uid:
            summary["distinct_users"] = safe_int(dh.scalar(f'SELECT COUNT(DISTINCT "{uid}") FROM users', 0))
        if prov:
            rows = dh.rows(f'SELECT CAST("{prov}" AS VARCHAR) AS province, COUNT(*) AS row_count, COUNT(DISTINCT {make_expr_col(uid)}) AS distinct_user_count FROM users GROUP BY 1 ORDER BY row_count DESC')
            summary["province_distribution"] = rows
            summary["provinces"] = [r.get("province") for r in rows if r.get("province") is not None]
            summary["province_count"] = len(summary["provinces"])
    if reg["registered"].get("call_edges"):
        summary["call_rows"] = safe_int(dh.scalar("SELECT COUNT(*) FROM calls", 0))
        time_col = schema["calls"].get("event_time")
        date_col = schema["calls"].get("event_date")
        hour_col = schema["calls"].get("event_hour")
        if time_col:
            summary["time_coverage"] = {
                "time_col": time_col,
                "min_time": dh.scalar(f'SELECT MIN(TRY_CAST("{time_col}" AS TIMESTAMP)) FROM calls'),
                "max_time": dh.scalar(f'SELECT MAX(TRY_CAST("{time_col}" AS TIMESTAMP)) FROM calls'),
                "valid_time_rows": safe_int(dh.scalar(f'SELECT COUNT(*) FROM calls WHERE TRY_CAST("{time_col}" AS TIMESTAMP) IS NOT NULL', 0)),
            }
        elif date_col:
            summary["time_coverage"] = {
                "date_col": date_col,
                "min_date": dh.scalar(f'SELECT MIN("{date_col}") FROM calls'),
                "max_date": dh.scalar(f'SELECT MAX("{date_col}") FROM calls'),
            }
        summary["has_hour_column"] = bool(hour_col)
    if reg["registered"].get("device_edges"):
        imei = schema["devices"].get("imei")
        summary["device_edges"] = safe_int(dh.scalar("SELECT COUNT(*) FROM devices", 0))
        if imei:
            summary["distinct_devices"] = safe_int(dh.scalar(f'SELECT COUNT(DISTINCT "{imei}") FROM devices', 0))
    return summary


def data_quality(dh: DuckHelper, reg: Dict[str, Any], schema: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    def add(item: str, status: str, detail: str, severity: str = "info") -> None:
        out.append({"item": item, "status": status, "severity": severity, "detail": detail})

    for key in ["user_nodes", "call_edges", "device_edges"]:
        add(f"file_{key}", "pass" if reg["exists"].get(key) else "fail", "存在" if reg["exists"].get(key) else "缺失", "error" if not reg["exists"].get(key) else "info")

    uid = schema["users"].get("user_id")
    csrc = schema["calls"].get("src_user_id")
    cdst = schema["calls"].get("dst_counterparty_id")
    duid = schema["devices"].get("user_id")
    dimei = schema["devices"].get("imei")
    prov = schema["users"].get("province")

    add("user_id_column", "pass" if uid else "fail", uid or "未识别", "error" if not uid else "info")
    add("call_src_column", "pass" if csrc else "warn", csrc or "未识别", "warn" if not csrc else "info")
    add("call_dst_column", "pass" if cdst else "warn", cdst or "未识别", "warn" if not cdst else "info")
    add("device_user_column", "pass" if duid else "warn", duid or "未识别", "warn" if not duid else "info")
    add("device_imei_column", "pass" if dimei else "warn", dimei or "未识别", "warn" if not dimei else "info")
    add("province_column", "pass" if prov else "warn", prov or "未识别", "warn" if not prov else "info")

    if reg["registered"].get("user_nodes") and uid:
        duplicates = safe_int(dh.scalar(f'SELECT COUNT(*) - COUNT(DISTINCT "{uid}") FROM users', 0))
        add("duplicate_user_rows", "pass" if duplicates == 0 else "warn", str(duplicates), "warn" if duplicates else "info")
    if reg["registered"].get("call_edges") and csrc and uid and reg["registered"].get("user_nodes"):
        missing_src = safe_int(dh.scalar(f'SELECT COUNT(*) FROM calls c LEFT JOIN users u ON CAST(c."{csrc}" AS VARCHAR)=CAST(u."{uid}" AS VARCHAR) WHERE u."{uid}" IS NULL', 0))
        add("call_src_not_in_user_nodes", "pass" if missing_src == 0 else "warn", str(missing_src), "warn" if missing_src else "info")
    if reg["registered"].get("device_edges") and duid and uid and reg["registered"].get("user_nodes"):
        missing_dev_user = safe_int(dh.scalar(f'SELECT COUNT(*) FROM devices d LEFT JOIN users u ON CAST(d."{duid}" AS VARCHAR)=CAST(u."{uid}" AS VARCHAR) WHERE u."{uid}" IS NULL', 0))
        add("device_user_not_in_user_nodes", "pass" if missing_dev_user == 0 else "warn", str(missing_dev_user), "warn" if missing_dev_user else "info")
    return out


def id_format_rows(dh: DuckHelper, field_name: str, sql_source: str, id_col: str, prov_col: str, top_k: int) -> List[Dict[str, Any]]:
    # all_varchar views make LENGTH and regexp reliable; for parquet cast explicitly.
    sql = f"""
    WITH base AS (
      SELECT CAST({id_col} AS VARCHAR) AS id_value, CAST({prov_col} AS VARCHAR) AS province
      FROM {sql_source}
      WHERE {id_col} IS NOT NULL AND CAST({id_col} AS VARCHAR) <> '' AND {prov_col} IS NOT NULL
    ), agg AS (
      SELECT province,
             COUNT(*) AS row_count,
             COUNT(DISTINCT id_value) AS distinct_id_count,
             MIN(LENGTH(id_value)) AS min_len,
             MAX(LENGTH(id_value)) AS max_len,
             AVG(LENGTH(id_value)) AS avg_len,
             100.0 * SUM(CASE WHEN regexp_matches(id_value, '^[0-9a-fA-F]+$') THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS hex_row_pct
      FROM base GROUP BY province
    )
    SELECT '{field_name}' AS field, * FROM agg ORDER BY province
    """
    rows = dh.rows(sql)
    # Add length distribution in a separate lighter query per province.
    for r in rows:
        province = str(r.get("province"))
        dist_sql = f"""
        WITH base AS (
          SELECT CAST({id_col} AS VARCHAR) AS id_value, CAST({prov_col} AS VARCHAR) AS province
          FROM {sql_source}
          WHERE {id_col} IS NOT NULL AND CAST({id_col} AS VARCHAR) <> '' AND {prov_col} IS NOT NULL
        )
        SELECT LENGTH(id_value) AS length, COUNT(DISTINCT id_value) AS distinct_id_count
        FROM base WHERE province = '{province.replace("'", "''")}'
        GROUP BY 1 ORDER BY distinct_id_count DESC LIMIT {top_k}
        """
        r["length_distribution"] = dh.rows(dist_sql)
        r["avg_len"] = round(safe_float(r.get("avg_len")), 2)
        r["hex_row_pct"] = round(safe_float(r.get("hex_row_pct")), 2)
    return rows


def identity_linkability(dh: DuckHelper, reg: Dict[str, Any], schema: Dict[str, Any], province_a: Optional[str], province_b: Optional[str], top_k: int) -> Dict[str, Any]:
    users_ok = reg["registered"].get("user_nodes")
    calls_ok = reg["registered"].get("call_edges")
    devices_ok = reg["registered"].get("device_edges")
    uid = schema["users"].get("user_id")
    uprov = schema["users"].get("province")
    csrc = schema["calls"].get("src_user_id")
    cdst = schema["calls"].get("dst_counterparty_id")
    cprov = schema["calls"].get("province")
    duid = schema["devices"].get("user_id")
    dimei = schema["devices"].get("imei")
    result: Dict[str, Any] = {
        "enabled": False,
        "province_a": province_a,
        "province_b": province_b,
        "linkability_level": "not_applicable_single_or_unknown_province",
        "linkability_score": 0,
        "conclusion_zh": "当前数据无法进行跨省/跨来源实体可联动性判断。",
        "identity_overlaps": {},
        "format_diagnostics": [],
        "warnings": [],
        "samples": {},
    }
    if not (users_ok and uid and uprov):
        result["warnings"].append("缺少用户节点省份或用户ID字段，无法诊断跨省实体命名空间。")
        return result
    provinces = [r["province"] for r in dh.rows(f'SELECT DISTINCT CAST("{uprov}" AS VARCHAR) AS province FROM users WHERE "{uprov}" IS NOT NULL ORDER BY 1')]
    result["available_provinces"] = provinces
    if not province_a or not province_b:
        if len(provinces) >= 2:
            province_a, province_b = provinces[0], provinces[1]
            result["province_a"] = province_a
            result["province_b"] = province_b
        else:
            result["warnings"].append("省份数量不足 2 个，不需要做跨省可联动性判断。")
            return result
    if province_a not in provinces or province_b not in provinces:
        result["warnings"].append(f"指定省份不存在：{province_a}, {province_b}；可用省份：{provinces}")
        return result

    result["enabled"] = True
    a = str(province_a).replace("'", "''")
    b = str(province_b).replace("'", "''")

    # Format diagnostics.
    result["format_diagnostics"].extend(id_format_rows(dh, "user_id", "users", f'"{uid}"', f'"{uprov}"', top_k))
    if devices_ok and duid and dimei:
        src = f'(SELECT d."{dimei}" AS id_value, u."{uprov}" AS province FROM devices d JOIN users u ON CAST(d."{duid}" AS VARCHAR)=CAST(u."{uid}" AS VARCHAR))'
        result["format_diagnostics"].extend(id_format_rows(dh, "imei", src, "id_value", "province", top_k))
    if calls_ok and cdst:
        if cprov:
            src = "calls"
            prov_expr = f'"{cprov}"'
        elif csrc:
            src = f'(SELECT c."{cdst}" AS dst_id, u."{uprov}" AS province FROM calls c JOIN users u ON CAST(c."{csrc}" AS VARCHAR)=CAST(u."{uid}" AS VARCHAR))'
            prov_expr = "province"
            result["format_diagnostics"].extend(id_format_rows(dh, "dst_counterparty_id", src, "dst_id", prov_expr, top_k))
            src = None  # type: ignore
        if cprov:
            result["format_diagnostics"].extend(id_format_rows(dh, "dst_counterparty_id", src, f'"{cdst}"', prov_expr, top_k))

    overlaps: Dict[str, int] = {}
    overlaps["user_id_overlap_count"] = safe_int(dh.scalar(f"""
        WITH a AS (SELECT DISTINCT CAST("{uid}" AS VARCHAR) AS id FROM users WHERE CAST("{uprov}" AS VARCHAR)='{a}'),
             b AS (SELECT DISTINCT CAST("{uid}" AS VARCHAR) AS id FROM users WHERE CAST("{uprov}" AS VARCHAR)='{b}')
        SELECT COUNT(*) FROM (SELECT id FROM a INTERSECT SELECT id FROM b)
    """, 0))
    if devices_ok and duid and dimei:
        overlaps["imei_overlap_count"] = safe_int(dh.scalar(f"""
            WITH devprov AS (
              SELECT DISTINCT CAST(d."{dimei}" AS VARCHAR) AS imei, CAST(u."{uprov}" AS VARCHAR) AS province
              FROM devices d JOIN users u ON CAST(d."{duid}" AS VARCHAR)=CAST(u."{uid}" AS VARCHAR)
              WHERE d."{dimei}" IS NOT NULL
            ), a AS (SELECT imei FROM devprov WHERE province='{a}'), b AS (SELECT imei FROM devprov WHERE province='{b}')
            SELECT COUNT(*) FROM (SELECT imei FROM a INTERSECT SELECT imei FROM b)
        """, 0))
        result["samples"]["cross_shared_devices"] = dh.rows(f"""
            WITH devprov AS (
              SELECT CAST(d."{dimei}" AS VARCHAR) AS imei, CAST(u."{uprov}" AS VARCHAR) AS province, COUNT(DISTINCT CAST(d."{duid}" AS VARCHAR)) AS phone_count
              FROM devices d JOIN users u ON CAST(d."{duid}" AS VARCHAR)=CAST(u."{uid}" AS VARCHAR)
              WHERE d."{dimei}" IS NOT NULL GROUP BY 1,2
            ), agg AS (
              SELECT imei,
                     SUM(CASE WHEN province='{a}' THEN phone_count ELSE 0 END) AS a_phone_count,
                     SUM(CASE WHEN province='{b}' THEN phone_count ELSE 0 END) AS b_phone_count
              FROM devprov GROUP BY 1
            ) SELECT imei, a_phone_count, b_phone_count, a_phone_count+b_phone_count AS total_phone_count
              FROM agg WHERE a_phone_count>0 AND b_phone_count>0 ORDER BY total_phone_count DESC LIMIT {top_k}
        """)
    else:
        overlaps["imei_overlap_count"] = 0
    if calls_ok and cdst:
        if cprov:
            overlaps["counterparty_overlap_count"] = safe_int(dh.scalar(f"""
                WITH a AS (SELECT DISTINCT CAST("{cdst}" AS VARCHAR) AS id FROM calls WHERE CAST("{cprov}" AS VARCHAR)='{a}' AND "{cdst}" IS NOT NULL),
                     b AS (SELECT DISTINCT CAST("{cdst}" AS VARCHAR) AS id FROM calls WHERE CAST("{cprov}" AS VARCHAR)='{b}' AND "{cdst}" IS NOT NULL)
                SELECT COUNT(*) FROM (SELECT id FROM a INTERSECT SELECT id FROM b)
            """, 0))
        elif csrc:
            overlaps["counterparty_overlap_count"] = safe_int(dh.scalar(f"""
                WITH cp AS (
                    SELECT DISTINCT CAST(c."{cdst}" AS VARCHAR) AS id, CAST(u."{uprov}" AS VARCHAR) AS province
                    FROM calls c JOIN users u ON CAST(c."{csrc}" AS VARCHAR)=CAST(u."{uid}" AS VARCHAR)
                    WHERE c."{cdst}" IS NOT NULL
                ), a AS (SELECT id FROM cp WHERE province='{a}'), b AS (SELECT id FROM cp WHERE province='{b}')
                SELECT COUNT(*) FROM (SELECT id FROM a INTERSECT SELECT id FROM b)
            """, 0))
        else:
            overlaps["counterparty_overlap_count"] = 0
    else:
        overlaps["counterparty_overlap_count"] = 0
    if calls_ok and csrc and cdst:
        overlaps["direct_cross_call_pair_count"] = safe_int(dh.scalar(f"""
            WITH pairs AS (
              SELECT DISTINCT CAST(c."{csrc}" AS VARCHAR) AS src, CAST(c."{cdst}" AS VARCHAR) AS dst
              FROM calls c
            )
            SELECT COUNT(*)
            FROM pairs p
            JOIN users su ON p.src=CAST(su."{uid}" AS VARCHAR)
            JOIN users tu ON p.dst=CAST(tu."{uid}" AS VARCHAR)
            WHERE (CAST(su."{uprov}" AS VARCHAR)='{a}' AND CAST(tu."{uprov}" AS VARCHAR)='{b}')
               OR (CAST(su."{uprov}" AS VARCHAR)='{b}' AND CAST(tu."{uprov}" AS VARCHAR)='{a}')
        """, 0))
    else:
        overlaps["direct_cross_call_pair_count"] = 0

    result["identity_overlaps"] = overlaps

    # Determine visible namespace differences from format diagnostics.
    format_warnings: List[str] = []
    by_field: Dict[str, Dict[str, Tuple[int, int]]] = {}
    for row in result["format_diagnostics"]:
        field = row.get("field")
        prov = row.get("province")
        if field and prov:
            by_field.setdefault(str(field), {})[str(prov)] = (safe_int(row.get("min_len")), safe_int(row.get("max_len")))
    for field, mp in by_field.items():
        if province_a in mp and province_b in mp and mp[str(province_a)] != mp[str(province_b)]:
            format_warnings.append(f"{field} 两省ID长度分布不一致：{province_a}={mp[str(province_a)]}, {province_b}={mp[str(province_b)]}")
    result["format_warnings"] = format_warnings

    signal_count = sum(1 for k in ["imei_overlap_count", "counterparty_overlap_count", "direct_cross_call_pair_count"] if overlaps.get(k, 0) > 0)
    if signal_count >= 2:
        level, score = "linkable_with_multiple_cross_province_signals", 3
        conclusion = "当前数据存在多类跨省 ID 相等证据，可支持基于当前脱敏ID的跨省联动线索识别。"
    elif signal_count == 1:
        level, score = "partially_linkable_with_one_cross_province_signal", 2
        conclusion = "当前数据存在一类跨省 ID 相等证据，可做有限跨省线索分析，但需要说明证据边界。"
    elif format_warnings:
        level, score = "not_linkable_due_to_visible_namespace_difference", 0
        conclusion = "当前数据未发现跨省ID相等证据，且两省ID格式存在可见差异；不建议据此判断真实跨省联动不存在。"
    else:
        level, score = "no_cross_province_overlap_found_namespace_unknown", 1
        conclusion = "当前数据未发现跨省ID相等证据；若两省脱敏命名空间一致，可解释为未检出，否则仍需映射表确认。"
    result["linkability_level"] = level
    result["linkability_score"] = score
    result["conclusion_zh"] = conclusion
    if format_warnings:
        result["warnings"].extend(format_warnings)
    return result


def build_capability_matrix(summary: Dict[str, Any], reg: Dict[str, Any], schema: Dict[str, Any], quality: List[Dict[str, Any]], linkability: Dict[str, Any]) -> List[Dict[str, Any]]:
    users = summary.get("distinct_users", 0) > 0
    calls = summary.get("call_rows", 0) > 0
    devices = summary.get("device_edges", 0) > 0
    labels = bool(schema["users"].get("label") or schema["users"].get("sub_label"))
    province = bool(schema["users"].get("province"))
    time_ok = bool(schema["calls"].get("event_time") or schema["calls"].get("event_date") or schema["calls"].get("event_hour"))
    cross_link_score = safe_int(linkability.get("linkability_score", 0)) if linkability.get("enabled") else 0

    def row(skill: str, status: str, reason: str, next_step: str) -> Dict[str, Any]:
        return {"skill": skill, "support_status": status, "reason_zh": reason, "recommended_next_step_zh": next_step}

    matrix: List[Dict[str, Any]] = []
    matrix.append(row("dataset-overview-analysis", "supported" if (users or calls or devices) else "not_supported", "可读取至少一种标准图表。" if (users or calls or devices) else "未发现可分析的标准图表。", "先做数据总体概览。"))
    matrix.append(row("topn-high-risk-discovery", "supported" if users and (calls or devices) else "partial" if users else "not_supported", "有用户节点和关系边，可进行风险排序。" if users and (calls or devices) else "缺少关系边或用户节点，排序证据不足。", "若缺少关系边，先补充通话或设备数据。"))
    matrix.append(row("single-number-analysis", "supported" if users and (calls or devices) else "not_supported", "有号码节点和至少一种关系边。" if users and (calls or devices) else "缺少号码节点或关系边。", "选择一个号码做单号画像。"))
    matrix.append(row("association-path-analysis", "supported" if calls or devices else "not_supported", "存在通话边或设备边，可做路径/复合关系分析。" if calls or devices else "没有关系边，无法分析路径。", "输入两个号码做路径核查。"))
    matrix.append(row("overlap-analysis", "supported" if calls or devices else "not_supported", "存在共同对端或共享设备的基础边。" if calls or devices else "没有通话/设备边，无法计算重叠。", "输入两个号码核查共同邻居/设备。"))
    matrix.append(row("subgraph-extraction-analysis", "supported" if users and (calls or devices) else "not_supported", "可围绕号码抽取局部关系图。" if users and (calls or devices) else "缺少节点或边，无法抽取局部子图。", "选择重点号码抽取 1-2 跳子图。"))
    matrix.append(row("shared-device-analysis", "supported" if devices else "not_supported", "存在设备边，可分析共享设备。" if devices else "缺少 edges_phone_imei，无法分析共享设备。", "围绕号码或设备做共享设备分析。"))
    matrix.append(row("group-risk-analysis", "supported" if users and (calls or devices) else "not_supported", "可对号码集合聚合统计风险模式。" if users and (calls or devices) else "缺少号码集合或关系边。", "输入号码集合做群体分析。"))
    matrix.append(row("gang-cluster-analysis", "supported" if users and (devices or calls) else "not_supported", "可基于共享设备/共同对端/邻居重叠识别候选簇。" if users and (devices or calls) else "缺少成簇关系证据。", "输入候选号码集合做团伙簇识别。"))
    matrix.append(row("condition-based-screening", "supported" if users else "not_supported", "有用户节点，可按标签/省份/指标筛选；若有边可筛更多行为条件。" if users else "缺少用户节点，无法筛选目标。", "按省份、标签、夜间行为、共享设备等条件筛选。"))
    matrix.append(row("risk-evidence-pack", "supported" if users and (calls or devices) else "not_supported", "可聚合画像、通话、设备等证据。" if users and (calls or devices) else "缺少证据来源。", "为重点号码生成证据包。"))
    matrix.append(row("time-series-anomaly-analysis", "supported" if calls and time_ok else "not_supported", "通话边包含时间字段，可做趋势/夜间/异常日期分析。" if calls and time_ok else "缺少通话时间字段。", "围绕号码或群体做时间异常分析。"))
    matrix.append(row("sichuan-shaanxi-comparison", "supported" if province and summary.get("province_count", 0) >= 2 else "not_supported", "至少包含两个省份，可做地域对比。" if province and summary.get("province_count", 0) >= 2 else "省份字段不足或只有单一省份。", "做四川/陕西或其他省份的统计对比。"))
    matrix.append(row("cross-province-linkage-analysis", "supported" if cross_link_score >= 2 else "not_supported" if linkability.get("enabled") else "not_applicable", "存在跨省ID相等证据，可做有限跨省联动。" if cross_link_score >= 2 else linkability.get("conclusion_zh", "当前数据不适合跨省同实体联动分析。"), "若不支持，先补统一哈希规则/映射表，或仅做地域对比。"))
    matrix.append(row("dataset-onboarding-graph-preprocess", "not_needed_for_processed_dataset", "当前对象是已处理后的标准图数据；该 skill 用于原始上传数据接入。", "如果上传的是原始 CSV/Excel，先调用 dataset-onboarding-graph-preprocess。"))
    return matrix


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    # Flatten nested objects as JSON strings.
    keys: List[str] = []
    for r in rows:
        for k in r.keys():
            if k not in keys:
                keys.append(k)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        for r in rows:
            rr = {}
            for k in keys:
                v = r.get(k)
                rr[k] = json.dumps(v, ensure_ascii=False) if isinstance(v, (list, dict)) else v
            w.writerow(rr)



def read_any_table_for_fallback(path: Path, columns_hint: Optional[List[str]] = None):
    import pandas as pd
    if not path.exists():
        return None
    try:
        if path.suffix.lower() == ".csv":
            return pd.read_csv(path, dtype=str, encoding="utf-8-sig", usecols=columns_hint if columns_hint else None)
        if path.suffix.lower() == ".parquet":
            return pd.read_parquet(path, columns=columns_hint if columns_hint else None)
        if path.suffix.lower() in {".json", ".jsonl"}:
            return pd.read_json(path)
    except Exception:
        try:
            if path.suffix.lower() == ".csv":
                return pd.read_csv(path, dtype=str, usecols=columns_hint if columns_hint else None)
        except Exception:
            return None
    return None


def pandas_diagnose(dataset_root: Path, dataset: str, province_a: Optional[str], province_b: Optional[str], top_k: int):
    """Fallback implementation when duckdb is unavailable. It is optimized for small/medium datasets.
    In the project docker image, duckdb is normally available and the SQL path will be used.
    """
    import pandas as pd
    paths = resolve_paths(dataset_root, dataset)
    users = read_any_table_for_fallback(paths["user_nodes"])
    calls = read_any_table_for_fallback(paths["call_edges"])
    devices = read_any_table_for_fallback(paths["device_edges"])
    reg = {
        "exists": {"user_nodes": paths["user_nodes"].exists(), "call_edges": paths["call_edges"].exists(), "device_edges": paths["device_edges"].exists()},
        "registered": {"user_nodes": users is not None, "call_edges": calls is not None, "device_edges": devices is not None},
        "columns": {"user_nodes": list(users.columns) if users is not None else [], "call_edges": list(calls.columns) if calls is not None else [], "device_edges": list(devices.columns) if devices is not None else []},
    }
    schema = detect_schema(reg["columns"])
    summary = {"user_rows": 0, "distinct_users": 0, "call_rows": 0, "device_edges": 0, "distinct_devices": 0, "province_count": 0, "provinces": [], "time_coverage": {}}
    quality=[]
    def add(item,status,detail,severity="info"):
        quality.append({"item":item,"status":status,"severity":severity,"detail":str(detail)})
    for key in ["user_nodes","call_edges","device_edges"]:
        add(f"file_{key}", "pass" if reg["exists"].get(key) else "fail", "存在" if reg["exists"].get(key) else "缺失", "error" if not reg["exists"].get(key) else "info")
    uid=schema["users"].get("user_id"); uprov=schema["users"].get("province")
    csrc=schema["calls"].get("src_user_id"); cdst=schema["calls"].get("dst_counterparty_id"); cprov=schema["calls"].get("province")
    duid=schema["devices"].get("user_id"); dimei=schema["devices"].get("imei")
    add("user_id_column", "pass" if uid else "fail", uid or "未识别", "error" if not uid else "info")
    add("call_src_column", "pass" if csrc else "warn", csrc or "未识别", "warn" if not csrc else "info")
    add("call_dst_column", "pass" if cdst else "warn", cdst or "未识别", "warn" if not cdst else "info")
    add("device_user_column", "pass" if duid else "warn", duid or "未识别", "warn" if not duid else "info")
    add("device_imei_column", "pass" if dimei else "warn", dimei or "未识别", "warn" if not dimei else "info")
    add("province_column", "pass" if uprov else "warn", uprov or "未识别", "warn" if not uprov else "info")
    if users is not None and uid:
        summary["user_rows"]=int(len(users)); summary["distinct_users"]=int(users[uid].nunique(dropna=True))
        dup=int(len(users)-users[uid].nunique(dropna=True)); add("duplicate_user_rows", "pass" if dup==0 else "warn", dup, "warn" if dup else "info")
        if uprov:
            dist=users.groupby(uprov, dropna=False).agg(row_count=(uid,"size"), distinct_user_count=(uid,"nunique")).reset_index().rename(columns={uprov:"province"})
            summary["province_distribution"]=dist.to_dict("records"); summary["provinces"]=[str(x) for x in dist["province"].dropna().tolist()]; summary["province_count"]=len(summary["provinces"])
    if calls is not None:
        summary["call_rows"]=int(len(calls))
        t=schema["calls"].get("event_time"); d=schema["calls"].get("event_date"); h=schema["calls"].get("event_hour")
        if t:
            tt=pd.to_datetime(calls[t], errors="coerce")
            summary["time_coverage"]={"time_col":t,"min_time":str(tt.min()) if tt.notna().any() else None,"max_time":str(tt.max()) if tt.notna().any() else None,"valid_time_rows":int(tt.notna().sum())}
        elif d:
            summary["time_coverage"]={"date_col":d,"min_date":str(calls[d].min()),"max_date":str(calls[d].max())}
        summary["has_hour_column"]=bool(h)
    if devices is not None:
        summary["device_edges"]=int(len(devices)); summary["distinct_devices"]=int(devices[dimei].nunique(dropna=True)) if dimei else 0
    # Linkability fallback
    link={"enabled":False,"province_a":province_a,"province_b":province_b,"linkability_level":"not_applicable_single_or_unknown_province","linkability_score":0,"conclusion_zh":"当前数据无法进行跨省/跨来源实体可联动性判断。","identity_overlaps":{},"format_diagnostics":[],"warnings":[],"samples":{}}
    if users is not None and uid and uprov and summary.get("province_count",0)>=2:
        provinces=summary.get("provinces",[]); province_a=province_a or provinces[0]; province_b=province_b or provinces[1]
        link.update({"enabled":True,"province_a":province_a,"province_b":province_b,"available_provinces":provinces})
        def fmt_rows(field, df, idc, pc):
            outs=[]
            if df is None or not idc or not pc or idc not in df.columns or pc not in df.columns: return outs
            tmp=df[[idc,pc]].dropna(); tmp[idc]=tmp[idc].astype(str); tmp=tmp[tmp[idc] != ""]
            for prov, g in tmp.groupby(pc):
                lens=g[idc].str.len()
                hexpct=float(g[idc].str.match(r'^[0-9a-fA-F]+$').mean()*100) if len(g) else 0
                dist=lens.value_counts().reset_index(); dist.columns=['length','distinct_id_count']
                outs.append({"field":field,"province":str(prov),"row_count":int(len(g)),"distinct_id_count":int(g[idc].nunique()),"min_len":int(lens.min()),"max_len":int(lens.max()),"avg_len":round(float(lens.mean()),2),"hex_row_pct":round(hexpct,2),"length_distribution":dist.head(top_k).to_dict('records')})
            return outs
        link["format_diagnostics"].extend(fmt_rows("user_id", users, uid, uprov))
        user_sets={p:set(users.loc[users[uprov].astype(str)==str(p), uid].astype(str).dropna()) for p in [province_a,province_b]}
        user_ov=len(user_sets.get(province_a,set()) & user_sets.get(province_b,set()))
        imei_ov=0; cp_ov=0; direct=0
        if devices is not None and duid and dimei:
            devprov=devices.merge(users[[uid,uprov]], left_on=duid, right_on=uid, how='inner')
            link["format_diagnostics"].extend(fmt_rows("imei", devprov, dimei, uprov))
            sets={p:set(devprov.loc[devprov[uprov].astype(str)==str(p), dimei].astype(str).dropna()) for p in [province_a,province_b]}
            imei_ov=len(sets.get(province_a,set()) & sets.get(province_b,set()))
        if calls is not None and cdst:
            if cprov:
                cp_source=calls; pc=cprov
            elif csrc:
                cp_source=calls.merge(users[[uid,uprov]], left_on=csrc, right_on=uid, how='inner'); pc=uprov
            else:
                cp_source=None; pc=None
            if cp_source is not None and pc:
                link["format_diagnostics"].extend(fmt_rows("dst_counterparty_id", cp_source, cdst, pc))
                sets={p:set(cp_source.loc[cp_source[pc].astype(str)==str(p), cdst].astype(str).dropna()) for p in [province_a,province_b]}
                cp_ov=len(sets.get(province_a,set()) & sets.get(province_b,set()))
            if csrc and cdst:
                provmap=users.set_index(uid)[uprov].astype(str).to_dict(); tmp=calls[[csrc,cdst]].dropna();
                srcp=tmp[csrc].astype(str).map(provmap); dstp=tmp[cdst].astype(str).map(provmap)
                direct=int((((srcp==str(province_a)) & (dstp==str(province_b))) | ((srcp==str(province_b)) & (dstp==str(province_a)))).sum())
        overlaps={"user_id_overlap_count":user_ov,"imei_overlap_count":imei_ov,"counterparty_overlap_count":cp_ov,"direct_cross_call_pair_count":direct}
        link["identity_overlaps"]=overlaps
        by={}
        for r in link["format_diagnostics"]:
            by.setdefault(r['field'],{})[r['province']]=(r['min_len'],r['max_len'])
        fw=[]
        for f,mp in by.items():
            if str(province_a) in mp and str(province_b) in mp and mp[str(province_a)] != mp[str(province_b)]: fw.append(f"{f} 两省ID长度分布不一致：{province_a}={mp[str(province_a)]}, {province_b}={mp[str(province_b)]}")
        sig=sum(1 for k in ['imei_overlap_count','counterparty_overlap_count','direct_cross_call_pair_count'] if overlaps.get(k,0)>0)
        if sig>=2: lvl,score,conc='linkable_with_multiple_cross_province_signals',3,'当前数据存在多类跨省 ID 相等证据，可支持基于当前脱敏ID的跨省联动线索识别。'
        elif sig==1: lvl,score,conc='partially_linkable_with_one_cross_province_signal',2,'当前数据存在一类跨省 ID 相等证据，可做有限跨省线索分析，但需要说明证据边界。'
        elif fw: lvl,score,conc='not_linkable_due_to_visible_namespace_difference',0,'当前数据未发现跨省ID相等证据，且两省ID格式存在可见差异；不建议据此判断真实跨省联动不存在。'
        else: lvl,score,conc='no_cross_province_overlap_found_namespace_unknown',1,'当前数据未发现跨省ID相等证据；若两省脱敏命名空间一致，可解释为未检出，否则仍需映射表确认。'
        link.update({"linkability_level":lvl,"linkability_score":score,"conclusion_zh":conc,"format_warnings":fw,"warnings":fw})
    caps=build_capability_matrix(summary, reg, schema, quality, link)
    return reg, schema, summary, quality, link, caps



def first_available_phone_id(dataset_root: Path, dataset: str, schema: Dict[str, Any], paths: Dict[str, Path]) -> str:
    """Return one sample phone id for command templates. Empty string if unavailable."""
    uid = schema.get("users", {}).get("user_id") if isinstance(schema, dict) else None
    if not uid or not paths.get("user_nodes") or not paths["user_nodes"].exists():
        return "<phone_id>"
    try:
        import pandas as pd
        df = pd.read_csv(paths["user_nodes"], dtype=str, usecols=[uid], encoding="utf-8-sig")
        vals = df[uid].dropna().astype(str)
        vals = vals[vals != ""]
        if len(vals) > 0:
            return str(vals.iloc[0])
    except Exception:
        pass
    return "<phone_id>"


def build_downstream_command_templates(dataset_root: Path, dataset: str, paths: Dict[str, Path], schema: Dict[str, Any], capability_matrix: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Generate ready-to-copy downstream commands.

    Some early wrappers do not support --dataset-root/--dataset yet, so this function
    explicitly separates dataset-mode and path-mode commands.
    """
    supported = {r.get("skill"): r.get("support_status") for r in capability_matrix}
    user_nodes = paths.get("user_nodes", dataset_root / "processed" / dataset / "user_nodes.csv")
    call_edges = paths.get("call_edges", dataset_root / "processed" / dataset / "call_edges.csv")
    device_edges = paths.get("device_edges", dataset_root / "processed" / "graph_views" / dataset / "edges_phone_imei.parquet")
    sample_phone = first_available_phone_id(dataset_root, dataset, schema, paths)

    def add(skill: str, purpose: str, execution_mode: str, command: str, note: str) -> Dict[str, Any]:
        return {
            "skill": skill,
            "purpose_zh": purpose,
            "support_status": "supported" if skill == "dataset-quality-and-linkability-diagnostic" else supported.get(skill, "unknown"),
            "execution_mode_zh": execution_mode,
            "command": command.strip(),
            "note_zh": note,
        }

    cmds: List[Dict[str, Any]] = []
    cmds.append(add(
        "dataset-quality-and-linkability-diagnostic",
        "复查当前数据集的数据质量、图结构完整性和可用 skill。",
        "dataset 模式",
        f"""
cd /workspace/imiss-deer-flow-main/skills/custom/phone-network-analysis/dataset-quality-and-linkability-diagnostic/scripts
python3 dataset_quality_linkability_diagnostic_wrapper.py \\
  --dataset-root {dataset_root} \\
  --dataset {dataset} \\
  --top-k 10 \\
  --artifact-mode essential
""",
        "本 skill 支持 --dataset-root 和 --dataset。",
    ))
    cmds.append(add(
        "dataset-overview-analysis",
        "查看数据总体规模、风险分布、关系规模和可分析能力范围。",
        "dataset 模式",
        f"""
cd /workspace/imiss-deer-flow-main/skills/custom/phone-network-analysis/dataset-overview-analysis/scripts
python3 dataset_overview_wrapper.py \\
  --dataset-root {dataset_root} \\
  --dataset {dataset} \\
  --top-k 10
""",
        "dataset-overview-analysis 支持 dataset 模式，适合作为后续分析入口。",
    ))
    cmds.append(add(
        "single-number-analysis",
        "选择一个号码做单号码画像、局部关系、共享设备和后续下钻建议。",
        "显式路径模式",
        f"""
cd /workspace/imiss-deer-flow-main/skills/custom/phone-network-analysis/single-number-analysis/scripts
python3 single_number_analysis_wrapper.py \\
  --phone-id '{sample_phone}' \\
  --hops 2 \\
  --max-nodes 200 \\
  --top-k 10 \\
  --analysis-mode mixed \\
  --user-node-path {user_nodes} \\
  --call-graph-path {call_edges} \\
  --device-graph-path {device_edges}
""",
        "当前部分早期 wrapper 不支持 --dataset-root/--dataset，因此这里给出显式路径调用。",
    ))
    cmds.append(add(
        "topn-high-risk-discovery",
        "基于当前数据生成 TopN 风险对象名单。",
        "显式路径模式",
        f"""
cd /workspace/imiss-deer-flow-main/skills/custom/phone-network-analysis/topn-high-risk-discovery/scripts
python3 topn_high_risk_discovery_wrapper.py \\
  --top-n 10 \\
  --analysis-mode mixed \\
  --candidate-scope all \\
  --user-node-path {user_nodes} \\
  --call-graph-path {call_edges} \\
  --device-graph-path {device_edges}
""",
        "topn-high-risk-discovery 当前使用显式路径模式；不要给它传 --dataset-root/--dataset/--artifact-mode。",
    ))
    cmds.append(add(
        "shared-device-analysis",
        "围绕号码或设备做共享设备关系分析。",
        "路径模式/按具体 wrapper 参数调整",
        f"""
cd /workspace/imiss-deer-flow-main/skills/custom/phone-network-analysis/shared-device-analysis/scripts
# 请将 <phone_id> 或 <device_id> 替换为要分析的对象；若该 wrapper 支持显式路径，请使用下列三件套路径：
# user_nodes: {user_nodes}
# call_edges: {call_edges}
# device_edges: {device_edges}
""",
        "不同版本 shared-device-analysis 参数可能不同；本诊断报告提供标准三件套路径，运行时按该 skill 的 SKILL.md 选择号码模式或设备模式。",
    ))
    cmds.append(add(
        "time-series-anomaly-analysis",
        "分析号码级或群体级时间趋势、夜间行为和异常突变。",
        "dataset 模式",
        f"""
cd /workspace/imiss-deer-flow-main/skills/custom/phone-network-analysis/time-series-anomaly-analysis/scripts
python3 time_series_anomaly_analysis_wrapper.py \\
  --mode phone \\
  --phone-id '{sample_phone}' \\
  --dataset-root {dataset_root} \\
  --dataset {dataset} \\
  --recent-days 7 \\
  --baseline-days 30 \\
  --top-k 10
""",
        "如果当前数据缺少时间字段，该命令不适用；请以能力矩阵判断为准。",
    ))
    return cmds

def build_report(result: Dict[str, Any]) -> str:
    summary = result.get("summary", {})
    link = result.get("linkability", {})
    caps = result.get("capability_matrix", [])
    quality = result.get("quality_checks", [])
    lines: List[str] = []
    lines.append(f"# 数据质量与可联动性诊断报告\n")
    lines.append("## 一、核心结论\n")
    lines.append(f"- 数据集：`{result.get('dataset')}`")
    lines.append(f"- 图结构是否可用：{'是' if result.get('graph_ready') else '否'}")
    lines.append(f"- 处理状态：`{result.get('status')}`")
    lines.append(f"- 主要结论：{result.get('conclusion_zh')}\n")

    lines.append("## 二、数据构成\n")
    rows = [
        ("用户节点行数", summary.get("user_rows", 0)),
        ("去重用户数", summary.get("distinct_users", 0)),
        ("通话边数", summary.get("call_rows", 0)),
        ("设备边数", summary.get("device_edges", 0)),
        ("去重设备数", summary.get("distinct_devices", 0)),
        ("省份数量", summary.get("province_count", 0)),
    ]
    lines.append("| 指标 | 数值 |\n|---|---:|")
    for k, v in rows:
        lines.append(f"| {k} | {v} |")
    lines.append("")
    if summary.get("province_distribution"):
        lines.append("### 省份分布\n")
        lines.append("| 省份 | 行数 | 去重用户数 |\n|---|---:|---:|")
        for r in summary.get("province_distribution", [])[:20]:
            lines.append(f"| {r.get('province')} | {r.get('row_count')} | {r.get('distinct_user_count')} |")
        lines.append("")

    lines.append("## 三、可用能力判断\n")
    lines.append("| skill | 支持状态 | 原因 | 建议 |\n|---|---|---|---|")
    for r in caps:
        lines.append(f"| `{r.get('skill')}` | {r.get('support_status')} | {r.get('reason_zh')} | {r.get('recommended_next_step_zh')} |")
    lines.append("")

    lines.append("## 四、下游调用命令模板\n")
    lines.append("以下模板区分两类调用方式：`dataset 模式` 表示 wrapper 支持 `--dataset-root/--dataset`；`显式路径模式` 表示需要直接传入 user_nodes/call_edges/device_edges 三个路径。")
    lines.append("")
    for r in result.get("downstream_command_templates", []):
        lines.append(f"### {r.get('skill')}\n")
        lines.append(f"- 用途：{r.get('purpose_zh')}")
        lines.append(f"- 当前能力状态：{r.get('support_status')}")
        lines.append(f"- 调用方式：{r.get('execution_mode_zh')}")
        lines.append(f"- 说明：{r.get('note_zh')}\n")
        lines.append("```bash")
        lines.append(str(r.get("command", "")).strip())
        lines.append("```\n")

    lines.append("## 五、跨省/跨来源可联动性诊断\n")
    if link.get("enabled"):
        lines.append(f"- 对比范围：`{link.get('province_a')}` vs `{link.get('province_b')}`")
        lines.append(f"- 可联动等级：`{link.get('linkability_level')}`")
        lines.append(f"- 可联动分数：{link.get('linkability_score')}/3")
        lines.append(f"- 结论：{link.get('conclusion_zh')}")
        ov = link.get("identity_overlaps", {})
        lines.append("\n| 检查项 | 结果 |\n|---|---:|")
        for k in ["user_id_overlap_count", "imei_overlap_count", "counterparty_overlap_count", "direct_cross_call_pair_count"]:
            lines.append(f"| {k} | {ov.get(k, 0)} |")
        if link.get("warnings"):
            lines.append("\n**重要提醒：**")
            for w in link.get("warnings", [])[:10]:
                lines.append(f"- {w}")
    else:
        lines.append(f"- {link.get('conclusion_zh', '当前数据不需要或不能进行跨省可联动性判断。')}")
    lines.append("")

    lines.append("## 六、数据质量检查\n")
    lines.append("| 检查项 | 状态 | 级别 | 说明 |\n|---|---|---|---|")
    for r in quality:
        lines.append(f"| {r.get('item')} | {r.get('status')} | {r.get('severity')} | {r.get('detail')} |")
    lines.append("")

    lines.append("## 七、后续建议\n")
    for item in result.get("next_steps_zh", []):
        lines.append(f"- {item}")
    lines.append("")
    return "\n".join(lines)


def select_artifacts(mode: str, paths: Dict[str, Path]) -> List[Dict[str, str]]:
    all_items = [
        ("markdown_report", paths["report_md"], paths["report_md"].name),
        ("json", paths["summary_json"], paths["summary_json"].name),
        ("csv", paths["capability_csv"], paths["capability_csv"].name),
        ("csv", paths.get("command_templates_csv", paths["capability_csv"]), paths.get("command_templates_csv", paths["capability_csv"]).name),
        ("csv", paths["quality_csv"], paths["quality_csv"].name),
        ("csv", paths["linkability_csv"], paths["linkability_csv"].name),
    ]
    if mode == "markdown_only":
        keep = [all_items[0]]
    elif mode == "essential":
        keep = all_items[:4]
    else:
        keep = all_items
    return [{"type": t, "path": str(p), "title": title} for t, p, title in keep if p.exists()]


def main() -> None:
    parser = argparse.ArgumentParser(description="Diagnose phone-network processed dataset quality and linkability.")
    parser.add_argument("--dataset-root", default="/workspace/imiss-deer-flow-main/datasets/phone-network")
    parser.add_argument("--dataset", default="unified")
    parser.add_argument("--province-a", default=None)
    parser.add_argument("--province-b", default=None)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--artifact-mode", choices=["full", "essential", "markdown_only"], default="essential")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    ensure_dir(output_dir)
    dataset_root = Path(args.dataset_root)
    dataset = args.dataset
    paths = resolve_paths(dataset_root, dataset)

    status = "ok"
    conclusion = "数据集已完成诊断。"
    graph_ready = False
    try:
        if duckdb is None:
            reg, schema, summary, quality, link, caps = pandas_diagnose(dataset_root, dataset, args.province_a, args.province_b, args.top_k)
        else:
            dh = DuckHelper()
            reg = register_views(dh, paths)
            schema = detect_schema(reg["columns"])
            summary = summarize_tables(dh, reg, schema)
            quality = data_quality(dh, reg, schema)
            link = identity_linkability(dh, reg, schema, args.province_a, args.province_b, args.top_k)
            caps = build_capability_matrix(summary, reg, schema, quality, link)
        has_users = summary.get("distinct_users", 0) > 0
        has_edges = summary.get("call_rows", 0) > 0 or summary.get("device_edges", 0) > 0
        graph_ready = bool(has_users and has_edges)
        if not has_users:
            status = "not_graph_ready_missing_user_nodes"
            conclusion = "当前数据缺少可用号码节点，不能直接进入电话网络图分析。"
        elif not has_edges:
            status = "partial_graph_only_user_nodes"
            conclusion = "当前数据有号码节点但缺少通话/设备关系边，只能做有限画像或字段统计。"
        else:
            status = "ok"
            conclusion = "当前数据具备标准图结构，可进入后续电话网络分析；具体 skill 支持情况见能力矩阵。"
    except Exception as e:
        status = "diagnostic_failed"
        conclusion = f"诊断脚本执行失败：{type(e).__name__}: {e}"
        reg = {"exists": {}, "registered": {}, "columns": {}}
        schema = {}
        summary = {}
        quality = [{"item": "diagnostic_exception", "status": "fail", "severity": "error", "detail": conclusion}]
        link = {"enabled": False, "conclusion_zh": conclusion, "linkability_score": 0, "linkability_level": "diagnostic_failed"}
        caps = []

    prefix = f"dataset_quality_linkability_{dataset}"
    out_paths = {
        "report_md": output_dir / f"{prefix}.md",
        "summary_json": output_dir / f"{prefix}_summary.json",
        "capability_csv": output_dir / f"{prefix}_capability_matrix.csv",
        "quality_csv": output_dir / f"{prefix}_quality_checks.csv",
        "linkability_csv": output_dir / f"{prefix}_linkability_diagnostics.csv",
    }
    result: Dict[str, Any] = {
        "ok": True,
        "skill": "dataset-quality-and-linkability-diagnostic",
        "query_type": "dataset_quality_linkability_diagnostic",
        "script_version": SCRIPT_VERSION,
        "status": status,
        "status_zh": "可分析" if status == "ok" else "部分可用/需处理",
        "dataset_root": str(dataset_root),
        "dataset": dataset,
        "graph_ready": graph_ready,
        "conclusion_zh": conclusion,
        "paths": {k: str(v) for k, v in paths.items()},
        "meta": reg,
        "schema_detection": schema,
        "summary": json_safe(summary),
        "quality_checks": quality,
        "linkability": json_safe(link),
        "capability_matrix": caps,
        "downstream_command_templates": build_downstream_command_templates(dataset_root, dataset, paths, schema, caps),
        "next_steps_zh": [],
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }
    if graph_ready:
        result["next_steps_zh"].append("先运行 dataset-overview-analysis 获取总体规模、风险分布和可分析范围。")
        result["next_steps_zh"].append("再根据能力矩阵选择 topn-high-risk-discovery、single-number-analysis、shared-device-analysis 等下游 skill。")
    else:
        result["next_steps_zh"].append("先补齐 user_nodes.csv、call_edges.csv 或 edges_phone_imei.parquet，或重新运行 dataset-onboarding-graph-preprocess。")
    if link.get("enabled") and safe_int(link.get("linkability_score", 0)) < 2:
        result["next_steps_zh"].append("不要直接做跨省共享设备/共同对端联动结论；如需跨省同实体分析，应补统一哈希规则、实体映射表或原始可复现脱敏流程。")

    # Keep command templates in both JSON and Markdown. A CSV copy is useful for quick inspection.
    out_paths["command_templates_csv"] = output_dir / f"{prefix}_downstream_command_templates.csv"
    write_csv(out_paths["capability_csv"], caps)
    write_csv(out_paths["command_templates_csv"], result.get("downstream_command_templates", []))
    write_csv(out_paths["quality_csv"], quality)
    link_rows = []
    if link.get("format_diagnostics"):
        link_rows.extend(link.get("format_diagnostics", []))
    else:
        link_rows.append({"field": "linkability", "province": "-", "detail": link.get("conclusion_zh")})
    write_csv(out_paths["linkability_csv"], link_rows)
    out_paths["report_md"].write_text(build_report(result), encoding="utf-8")
    result["report_path"] = str(out_paths["report_md"])
    result["output_paths"] = {k: str(v) for k, v in out_paths.items()}
    result["artifacts"] = select_artifacts(args.artifact_mode, out_paths)
    out_paths["summary_json"].write_text(json.dumps(json_safe(result), ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(json_safe(result), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
