#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
dataset-onboarding-graph-preprocess v1.3

Convert raw phone-network CSV/XLSX/Parquet/JSON files into the standard graph tables
used by phone-network-analysis skills:
  - processed/<dataset>/user_nodes.csv
  - processed/<dataset>/call_edges.csv
  - processed/graph_views/<dataset>/edges_phone_imei.parquet
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

SCRIPT_VERSION = "dataset-onboarding-graph-preprocess-release-v1.3"

USER_COLUMNS = [
    "province", "dataset_name", "user_id", "label", "sub_label", "age",
    "open_card_time", "access_mode", "monthly_fee", "monthly_flow_mb",
    "monthly_call_duration", "caller_ratio_3m", "caller_dispersion_3m",
    "cross_province_ratio_3m", "broadband_flag", "source_table",
]
CALL_COLUMNS = [
    "province", "dataset_name", "src_user_id", "dst_counterparty_id", "event_time",
    "event_date", "event_hour", "duration", "call_type", "imei", "city", "county",
    "station", "cell", "roaming_place", "counterparty_belong", "source_table",
]
DEVICE_COLUMNS = ["src_id", "dst_id", "src_type", "dst_type", "edge_type", "dataset", "user_id", "imei", "edge_count"]
AUX_USER_ATTRS = [
    "age", "open_card_time", "access_mode", "monthly_fee", "monthly_flow_mb", "monthly_call_duration",
    "caller_ratio_3m", "caller_dispersion_3m", "cross_province_ratio_3m", "broadband_flag",
]
COLUMN_SYNONYMS: Dict[str, List[str]] = {
    "phone": ["phone", "phone_id", "phone_no", "phone_number", "mobile", "mobile_no", "msisdn", "user", "user_id", "uid", "subscriber", "caller", "caller_id", "src_user_id", "src", "source", "from", "from_phone", "main_number", "主叫", "主叫号码", "手机号", "手机号码", "号码", "本机号码", "用户号码", "用户", "客户号码"],
    "counterparty": ["counterparty", "counterparty_id", "dst_counterparty_id", "callee", "callee_id", "called", "called_id", "peer", "peer_id", "target", "target_id", "dst", "to", "to_phone", "peer_number", "被叫", "被叫号码", "对端", "对端号码", "联系人", "联系人号码", "联系号码", "对方号码"],
    "device": ["imei", "device", "device_id", "terminal", "terminal_id", "terminal_no", "设备", "设备号", "设备id", "设备ID", "终端", "终端号", "终端ID", "终端id", "手机设备号", "meid", "imsi"],
    "event_time": ["event_time", "call_time", "time", "timestamp", "datetime", "start_time", "start_datetime", "通话时间", "呼叫时间", "开始时间", "开始日期", "时间"],
    "event_date": ["event_date", "date", "call_date", "日期", "通话日期"],
    "event_hour": ["event_hour", "hour", "call_hour", "小时", "时段"],
    "duration": ["duration", "duration_sec", "call_duration", "seconds", "duration_seconds", "通话时长", "时长", "秒数"],
    "call_type": ["call_type", "call_direction", "direction", "通话类型", "呼叫类型", "主被叫"],
    "province": ["province", "省份", "省", "归属省", "归属地省", "归属地", "area_province", "所在地省"],
    "city": ["city", "城市", "市", "归属市", "所在地市"],
    "county": ["county", "district", "区县", "县", "区"],
    "station": ["station", "base_station", "基站"],
    "cell": ["cell", "cell_id", "小区", "小区号"],
    "roaming_place": ["roaming_place", "roaming", "漫游地", "漫游位置"],
    "counterparty_belong": ["counterparty_belong", "peer_belong", "对端归属地", "联系人归属地"],
    "label": ["label", "is_risk", "risk_label", "标签", "风险标签", "是否风险"],
    "sub_label": ["sub_label", "subtype", "risk_type", "type_label", "子标签", "风险类型", "标签类型"],
    "age": ["age", "年龄"],
    "open_card_time": ["open_card_time", "开户时间", "开卡时间"],
    "access_mode": ["access_mode", "入网方式"],
    "monthly_fee": ["monthly_fee", "月租", "套餐费"],
    "monthly_flow_mb": ["monthly_flow_mb", "月流量", "流量"],
    "monthly_call_duration": ["monthly_call_duration", "月通话时长"],
    "caller_ratio_3m": ["caller_ratio_3m", "主叫占比"],
    "caller_dispersion_3m": ["caller_dispersion_3m", "主叫离散度"],
    "cross_province_ratio_3m": ["cross_province_ratio_3m", "跨省占比"],
    "broadband_flag": ["broadband_flag", "宽带标识"],
}
ID_HEX_RE = re.compile(r"^[0-9a-fA-F]+$")


def norm_col(name: Any) -> str:
    s = str(name).strip().lower()
    s = re.sub(r"[\s\-./\\:：()（）\[\]【】]+", "_", s)
    return s.strip("_")


def normalize_value(v: Any) -> str:
    if pd.isna(v):
        return ""
    s = str(v).strip()
    if s.lower() in {"nan", "none", "null", "nat"}:
        return ""
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]
    return s


def is_probably_hash(s: str) -> bool:
    return bool(s) and len(s) in {16, 32, 40, 64, 96, 128} and bool(ID_HEX_RE.fullmatch(s))


def hash_id(raw: Any, entity_type: str, salt: str, hash_length: int, preserve_existing_ids: bool) -> str:
    s = normalize_value(raw)
    if not s:
        return ""
    if preserve_existing_ids and is_probably_hash(s):
        return s.lower()
    material = f"{entity_type}|{s}|{salt}".encode("utf-8")
    return hashlib.sha512(material).hexdigest()[:hash_length]


def parse_label(v: Any) -> Any:
    s = normalize_value(v).lower()
    if s == "":
        return None
    if s in {"1", "true", "yes", "risk", "fraud", "purefraud", "mutation", "black", "高风险", "风险", "疑似风险"}:
        return 1
    if s in {"0", "false", "no", "normal", "white", "whitelist", "正常", "白名单", "低风险"}:
        return 0
    try:
        return int(float(s))
    except Exception:
        return s


def infer_sub_label(label_value: Any, explicit: Any = None) -> str:
    ex = normalize_value(explicit)
    if ex:
        return ex.lower()
    if label_value == 1:
        return "risk"
    if label_value == 0:
        return "normal"
    return "unknown"


def choose_col(columns: Iterable[str], semantic: str, override: Optional[str] = None) -> Optional[str]:
    cols = list(columns)
    norm_to_orig = {norm_col(c): c for c in cols}
    if override:
        if override in cols:
            return override
        n = norm_col(override)
        if n in norm_to_orig:
            return norm_to_orig[n]
    for cand in COLUMN_SYNONYMS.get(semantic, []):
        n = norm_col(cand)
        if n in norm_to_orig:
            return norm_to_orig[n]
    terms = [norm_col(x) for x in COLUMN_SYNONYMS.get(semantic, [])]
    for col in cols:
        ncol = norm_col(col)
        if any(term and (ncol == term or ncol.endswith("_" + term) or term in ncol) for term in terms):
            return col
    return None


def read_json_flexible(path: Path) -> pd.DataFrame:
    text = path.read_text(encoding="utf-8-sig")
    data = json.loads(text)
    if isinstance(data, list):
        return pd.json_normalize(data).astype(str)
    if isinstance(data, dict):
        for key in ["data", "records", "rows", "items", "result"]:
            if isinstance(data.get(key), list):
                return pd.json_normalize(data[key]).astype(str)
        return pd.json_normalize(data).astype(str)
    return pd.read_json(path, dtype=str)


def read_input_file(path: Path) -> List[Tuple[str, pd.DataFrame]]:
    suffix = path.suffix.lower()
    frames: List[Tuple[str, pd.DataFrame]] = []
    if suffix == ".csv":
        try:
            df = pd.read_csv(path, dtype=str, encoding="utf-8-sig", low_memory=False)
        except UnicodeDecodeError:
            df = pd.read_csv(path, dtype=str, encoding="gbk", low_memory=False)
        frames.append((path.name, df))
    elif suffix in {".xlsx", ".xls"}:
        xls = pd.ExcelFile(path)
        for sheet in xls.sheet_names:
            frames.append((f"{path.name}::{sheet}", pd.read_excel(path, sheet_name=sheet, dtype=str)))
    elif suffix == ".parquet":
        frames.append((path.name, pd.read_parquet(path).astype(str)))
    elif suffix == ".json":
        frames.append((path.name, read_json_flexible(path)))
    return frames


def collect_input_files(input_dir: Optional[Path], input_files: List[str]) -> List[Path]:
    files: List[Path] = []
    if input_dir and input_dir.exists():
        # 递归查找，兼容前端上传目录里再嵌套一层文件夹的情况。
        for pattern in ["*.csv", "*.xlsx", "*.xls", "*.parquet", "*.json"]:
            files.extend(sorted(input_dir.rglob(pattern)))
    for item in input_files:
        p = Path(item)
        if p.exists():
            files.append(p)
    seen, out = set(), []
    for p in files:
        rp = str(p.resolve())
        if rp not in seen:
            seen.add(rp)
            out.append(p)
    return out


def safe_to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def derive_datetime_cols(df: pd.DataFrame, time_col: Optional[str], date_col: Optional[str], hour_col: Optional[str]) -> Tuple[pd.Series, pd.Series, pd.Series, int]:
    n = len(df)
    event_time = pd.Series([None] * n, dtype="object")
    event_date = pd.Series([None] * n, dtype="object")
    event_hour = pd.Series([None] * n, dtype="object")
    invalid_time_count = 0
    if time_col and time_col in df.columns:
        raw = df[time_col].astype(str).replace({"nan": "", "None": "", "null": "", "NaT": ""})
        dt = pd.to_datetime(raw, errors="coerce")
        invalid_time_count = int(((raw != "") & dt.isna()).sum())
        event_time = dt.dt.strftime("%Y-%m-%d %H:%M:%S").where(dt.notna(), raw.where(raw != "", None))
        event_date = dt.dt.strftime("%Y-%m-%d").where(dt.notna(), None)
        event_hour = dt.dt.hour.where(dt.notna(), None)
    if date_col and date_col in df.columns:
        raw_date = df[date_col].astype(str).replace({"nan": "", "None": "", "null": "", "NaT": ""})
        dt_date = pd.to_datetime(raw_date, errors="coerce")
        date_values = dt_date.dt.strftime("%Y-%m-%d").where(dt_date.notna(), raw_date.where(raw_date != "", None))
        event_date = event_date.where(event_date.notna(), date_values)
        if event_time.isna().all():
            event_time = date_values
    if hour_col and hour_col in df.columns:
        h = safe_to_numeric(df[hour_col])
        event_hour = event_hour.where(event_hour.notna(), h.where(h.notna(), None))
    return event_time, event_date, event_hour, invalid_time_count


def detect_mapping(df: pd.DataFrame, args: argparse.Namespace) -> Dict[str, Optional[str]]:
    m: Dict[str, Optional[str]] = {
        "phone": choose_col(df.columns, "phone", args.source_col),
        "counterparty": choose_col(df.columns, "counterparty", args.target_col),
        "device": choose_col(df.columns, "device", args.device_col),
        "event_time": choose_col(df.columns, "event_time", args.time_col),
        "event_date": choose_col(df.columns, "event_date", args.date_col),
        "event_hour": choose_col(df.columns, "event_hour", args.hour_col),
        "duration": choose_col(df.columns, "duration", args.duration_col),
        "call_type": choose_col(df.columns, "call_type", args.call_type_col),
        "province": choose_col(df.columns, "province", args.province_col),
        "city": choose_col(df.columns, "city"),
        "county": choose_col(df.columns, "county"),
        "station": choose_col(df.columns, "station"),
        "cell": choose_col(df.columns, "cell"),
        "roaming_place": choose_col(df.columns, "roaming_place"),
        "counterparty_belong": choose_col(df.columns, "counterparty_belong"),
        "label": choose_col(df.columns, "label", args.label_col),
        "sub_label": choose_col(df.columns, "sub_label", args.sub_label_col),
    }
    for k in AUX_USER_ATTRS:
        m[k] = choose_col(df.columns, k)
    return m


def build_from_frame(source_name: str, df: pd.DataFrame, mapping: Dict[str, Optional[str]], args: argparse.Namespace) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    users: List[Dict[str, Any]] = []
    calls: List[Dict[str, Any]] = []
    devices: List[Dict[str, Any]] = []
    phone_col, counter_col, device_col = mapping.get("phone"), mapping.get("counterparty"), mapping.get("device")
    province_col, label_col, sub_label_col = mapping.get("province"), mapping.get("label"), mapping.get("sub_label")
    stats: Dict[str, Any] = {"source": source_name, "rows": int(len(df)), "columns": list(map(str, df.columns)), "mapping": mapping, "roles": [], "notes": [], "warnings": [], "data_quality": {"duplicate_input_rows": int(df.duplicated().sum()), "skipped_empty_phone_rows": 0, "skipped_empty_counterparty_rows": 0, "skipped_empty_device_rows": 0, "invalid_time_rows": 0}}
    if not phone_col:
        stats["warnings"].append("未识别到号码/用户列，本文件不会产生 user/call/device 输出。")
        return users, calls, devices, stats
    event_time, event_date, event_hour, invalid_time_count = derive_datetime_cols(df, mapping.get("event_time"), mapping.get("event_date"), mapping.get("event_hour"))
    stats["data_quality"]["invalid_time_rows"] = invalid_time_count
    for pos, (_, row) in enumerate(df.iterrows()):
        raw_phone = normalize_value(row.get(phone_col))
        if not raw_phone:
            stats["data_quality"]["skipped_empty_phone_rows"] += 1
            continue
        user_id = hash_id(raw_phone, "phone", args.hash_salt, args.hash_length, args.preserve_existing_ids)
        province = normalize_value(row.get(province_col)) if province_col else args.province
        province = province or args.province or "unknown"
        label_value = parse_label(row.get(label_col)) if label_col else None
        sub_label = infer_sub_label(label_value, row.get(sub_label_col) if sub_label_col else None)
        user_row: Dict[str, Any] = {"province": province, "dataset_name": args.dataset_name, "user_id": user_id, "label": label_value, "sub_label": sub_label, "source_table": source_name}
        for attr in AUX_USER_ATTRS:
            col = mapping.get(attr)
            user_row[attr] = normalize_value(row.get(col)) if col else None
        users.append(user_row)
        if counter_col:
            raw_counter = normalize_value(row.get(counter_col))
            if raw_counter:
                dst_id = hash_id(raw_counter, "phone", args.hash_salt, args.hash_length, args.preserve_existing_ids)
                imei_hashed = None
                if device_col:
                    raw_device_for_call = normalize_value(row.get(device_col))
                    if raw_device_for_call:
                        imei_hashed = hash_id(raw_device_for_call, "device", args.hash_salt, args.hash_length, args.preserve_existing_ids)
                calls.append({
                    "province": province, "dataset_name": args.dataset_name, "src_user_id": user_id, "dst_counterparty_id": dst_id,
                    "event_time": event_time.iloc[pos] if pos < len(event_time) else None,
                    "event_date": event_date.iloc[pos] if pos < len(event_date) else None,
                    "event_hour": event_hour.iloc[pos] if pos < len(event_hour) else None,
                    "duration": safe_to_numeric(pd.Series([row.get(mapping.get("duration"))])).iloc[0] if mapping.get("duration") else None,
                    "call_type": normalize_value(row.get(mapping.get("call_type"))) if mapping.get("call_type") else None,
                    "imei": imei_hashed,
                    "city": normalize_value(row.get(mapping.get("city"))) if mapping.get("city") else None,
                    "county": normalize_value(row.get(mapping.get("county"))) if mapping.get("county") else None,
                    "station": normalize_value(row.get(mapping.get("station"))) if mapping.get("station") else None,
                    "cell": normalize_value(row.get(mapping.get("cell"))) if mapping.get("cell") else None,
                    "roaming_place": normalize_value(row.get(mapping.get("roaming_place"))) if mapping.get("roaming_place") else None,
                    "counterparty_belong": normalize_value(row.get(mapping.get("counterparty_belong"))) if mapping.get("counterparty_belong") else None,
                    "source_table": source_name,
                })
            else:
                stats["data_quality"]["skipped_empty_counterparty_rows"] += 1
        if device_col:
            raw_device = normalize_value(row.get(device_col))
            if raw_device:
                imei = hash_id(raw_device, "device", args.hash_salt, args.hash_length, args.preserve_existing_ids)
                devices.append({"src_id": user_id, "dst_id": imei, "src_type": "phone", "dst_type": "imei", "edge_type": "phone_imei", "dataset": args.dataset, "user_id": user_id, "imei": imei, "edge_count": 1, "source_table": source_name})
            else:
                stats["data_quality"]["skipped_empty_device_rows"] += 1
    if users:
        stats["roles"].append("user_nodes")
    if calls:
        stats["roles"].append("call_edges")
    if devices:
        stats["roles"].append("device_edges")
    if not counter_col:
        stats["notes"].append("未识别到对端列：本文件按节点表/设备表处理，不产生通话边。")
    if not device_col:
        stats["notes"].append("未识别到设备列：本文件不产生设备边。")
    if invalid_time_count:
        stats["warnings"].append(f"存在 {invalid_time_count} 行时间无法解析，已保留原始时间并将 event_date/event_hour 置空。")
    if stats["data_quality"]["skipped_empty_phone_rows"]:
        stats["warnings"].append(f"存在 {stats['data_quality']['skipped_empty_phone_rows']} 行号码为空，已跳过。")
    return users, calls, devices, stats


def coalesce_user_rows(users_df: pd.DataFrame) -> pd.DataFrame:
    if users_df.empty:
        return pd.DataFrame(columns=USER_COLUMNS)
    for col in USER_COLUMNS:
        if col not in users_df.columns:
            users_df[col] = None
    users_df = users_df[USER_COLUMNS].copy()
    users_df["_label_sort"] = users_df["label"].apply(lambda x: 2 if str(x) == "1" else (1 if pd.notna(x) and str(x) not in {"", "None", "nan"} else 0))
    users_df = users_df.sort_values(["user_id", "_label_sort"], ascending=[True, False])
    def first_non_empty(series: pd.Series) -> Any:
        for v in series:
            if pd.notna(v) and str(v) not in {"", "None", "nan"}:
                return v
        return None
    grouped = users_df.groupby("user_id", as_index=False).agg({col: first_non_empty for col in USER_COLUMNS if col != "user_id"})
    return grouped[USER_COLUMNS]


def finalize_call_edges(calls_df: pd.DataFrame) -> pd.DataFrame:
    if calls_df.empty:
        return pd.DataFrame(columns=CALL_COLUMNS)
    for col in CALL_COLUMNS:
        if col not in calls_df.columns:
            calls_df[col] = None
    calls_df = calls_df[CALL_COLUMNS].copy()
    calls_df["duration"] = pd.to_numeric(calls_df["duration"], errors="coerce")
    calls_df["event_hour"] = pd.to_numeric(calls_df["event_hour"], errors="coerce").astype("Int64")
    return calls_df


def finalize_device_edges(devices_df: pd.DataFrame) -> pd.DataFrame:
    if devices_df.empty:
        return pd.DataFrame(columns=DEVICE_COLUMNS)
    group_cols = ["src_id", "dst_id", "src_type", "dst_type", "edge_type", "dataset", "user_id", "imei"]
    grouped = devices_df.groupby(group_cols, dropna=False, as_index=False)["edge_count"].sum()
    grouped["edge_count"] = pd.to_numeric(grouped["edge_count"], errors="coerce").fillna(1).astype(int)
    return grouped[DEVICE_COLUMNS]


def write_parquet_safe(df: pd.DataFrame, path: Path, warnings: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(path, index=False)
        return
    except Exception as e1:
        try:
            import duckdb  # type: ignore
            con = duckdb.connect(database=":memory:")
            con.register("df_to_write", df)
            safe_path = str(path).replace("'", "''")
            con.execute(f"COPY df_to_write TO '{safe_path}' (FORMAT PARQUET)")
            con.close()
            return
        except Exception as e2:
            warnings.append(f"无法写出 parquet：{e1}; duckdb fallback also failed: {e2}")
            raise


def count_nullish(df: pd.DataFrame, col: str) -> int:
    if col not in df.columns:
        return 0
    return int(df[col].isna().sum() + (df[col].astype(str).isin(["", "None", "nan", "NaT"]).sum()))


def build_quality_report(users_df: pd.DataFrame, calls_df: pd.DataFrame, devices_df: pd.DataFrame, mappings: List[Dict[str, Any]], warnings: List[str], notes: List[str]) -> pd.DataFrame:
    invalid_time = sum(int(m.get("data_quality", {}).get("invalid_time_rows", 0)) for m in mappings)
    skipped_phone = sum(int(m.get("data_quality", {}).get("skipped_empty_phone_rows", 0)) for m in mappings)
    duplicate_rows = sum(int(m.get("data_quality", {}).get("duplicate_input_rows", 0)) for m in mappings)
    rows = [
        {"check": "user_nodes_non_empty", "status": "pass" if len(users_df) > 0 else "fail", "detail": f"{len(users_df)} rows"},
        {"check": "call_edges_non_empty", "status": "pass" if len(calls_df) > 0 else "warn", "detail": f"{len(calls_df)} rows"},
        {"check": "device_edges_non_empty", "status": "pass" if len(devices_df) > 0 else "warn", "detail": f"{len(devices_df)} rows"},
        {"check": "call_src_missing", "status": "pass" if count_nullish(calls_df, "src_user_id") == 0 else "warn", "detail": str(count_nullish(calls_df, "src_user_id"))},
        {"check": "call_dst_missing", "status": "pass" if count_nullish(calls_df, "dst_counterparty_id") == 0 else "warn", "detail": str(count_nullish(calls_df, "dst_counterparty_id"))},
        {"check": "device_user_missing", "status": "pass" if count_nullish(devices_df, "user_id") == 0 else "warn", "detail": str(count_nullish(devices_df, "user_id"))},
        {"check": "device_imei_missing", "status": "pass" if count_nullish(devices_df, "imei") == 0 else "warn", "detail": str(count_nullish(devices_df, "imei"))},
        {"check": "input_files_processed", "status": "pass" if mappings else "fail", "detail": str(len(mappings))},
        {"check": "duplicate_input_rows", "status": "warn" if duplicate_rows else "pass", "detail": str(duplicate_rows)},
        {"check": "invalid_time_rows", "status": "warn" if invalid_time else "pass", "detail": str(invalid_time)},
        {"check": "skipped_empty_phone_rows", "status": "warn" if skipped_phone else "pass", "detail": str(skipped_phone)},
        {"check": "role_notes", "status": "info" if notes else "pass", "detail": " | ".join(notes[:10]) if notes else "none"},
        {"check": "warnings", "status": "warn" if warnings else "pass", "detail": " | ".join(warnings[:10]) if warnings else "none"},
    ]
    return pd.DataFrame(rows)


def make_artifacts(all_paths: Dict[str, str], artifact_mode: str) -> List[Dict[str, str]]:
    """Return artifacts that should be exposed to the frontend.

    All output files are still generated on disk. artifact_mode only controls
    which files are displayed as downloadable cards in the frontend. This avoids
    repeated and noisy attachment lists in chat.
    """
    type_map = {
        "report_md": "markdown_report",
        "summary_json": "json",
        "mapping_json": "json",
        "quality_csv": "csv",
        "user_nodes": "csv",
        "call_edges": "csv",
        "device_edges": "parquet",
    }
    zh_title = {
        "report_md": "建图预处理报告.md",
        "user_nodes": "标准用户节点表_user_nodes.csv",
        "call_edges": "标准通话边表_call_edges.csv",
        "device_edges": "标准设备边表_edges_phone_imei.parquet",
        "summary_json": "预处理摘要_preprocess_summary.json",
        "mapping_json": "字段映射_schema_mapping.json",
        "quality_csv": "质量检查_data_quality_report.csv",
    }
    if artifact_mode == "markdown_only":
        keys = ["report_md"]
    elif artifact_mode == "essential":
        # 前端常用模式：只展示报告 + 标准三件套，避免下载卡片过多。
        # summary/mapping/quality 仍会生成在输出目录，报告中会说明路径。
        keys = ["report_md", "user_nodes", "call_edges", "device_edges"]
    else:
        keys = ["report_md", "user_nodes", "call_edges", "device_edges", "summary_json", "mapping_json", "quality_csv"]
    return [
        {"type": type_map.get(k, "file"), "path": all_paths[k], "title": zh_title.get(k, Path(all_paths[k]).name)}
        for k in keys
        if k in all_paths
    ]


def write_report(report_path: Path, result: Dict[str, Any]) -> None:
    s = result["summary"]
    mappings = result["input_file_summaries"]
    qrows = result["quality_checks"]
    output_paths = result["output_paths"]
    graph_ready = bool(result.get("graph_ready"))
    lines: List[str] = []
    lines += ["# 数据接入与建图预处理报告", "", "## 一、处理结论", ""]
    lines.append(f"- **处理状态**：`{result['status']}`")
    lines.append(f"- **图结构是否可用于后续分析**：{'是' if graph_ready else '否，需要先修正输入数据或字段映射'}")
    lines.append(f"- **脚本版本**：`{result['script_version']}`")
    lines.append(f"- **数据集名称**：`{result['dataset']}`")
    lines.append(f"- **输出根目录**：`{result['output_root']}`")
    lines.append(f"- **ID 处理方式**：{'保留已有哈希ID' if result['preserve_existing_ids'] else '统一重新哈希脱敏'}，hash_length={result['hash_length']}")
    lines.append(f"- **用户节点**：{s['user_nodes']} 行，去重用户 {s['distinct_users']} 个")
    lines.append(f"- **通话边**：{s['call_edges']} 行")
    lines.append(f"- **设备边**：{s['device_edges']} 行，去重设备 {s['distinct_devices']} 个")
    lines.append(f"- **质量提醒**：warnings={s['warnings_count']}，role_notes={s.get('notes_count', 0)}")

    lines += ["", "## 二、输出说明", ""]
    lines.append("本报告只保留处理结论、字段映射和质量检查，不在正文重复展开全部附件清单，避免前端下载卡片和报告正文重复展示。")
    lines.append("标准图结构文件、字段映射和质量检查文件已生成在输出目录中；前端下载入口请以上方附件卡片为准。")
    lines.append("如只想查看报告，请在前端请求中使用 `artifact_mode=markdown_only`；如需要标准三件套，请使用 `artifact_mode=essential`。")

    lines += ["", "## 三、输入文件与字段映射", ""]
    if not mappings:
        lines.append("未发现可读取输入文件，因此没有可展示的字段映射。请检查 `--input-dir` 或 `--input-file` 是否指向真实存在的 csv/xlsx/xls/parquet/json 文件。")
        lines.append("")
    for item in mappings:
        lines.append(f"### {item['source']}")
        lines.append(f"- 原始行数：{item['rows']}")
        lines.append(f"- 识别角色：{', '.join(item.get('roles') or ['未产生标准输出'])}")
        dq = item.get("data_quality", {})
        lines.append(f"- 数据质量：重复行 {dq.get('duplicate_input_rows', 0)}，空号码跳过 {dq.get('skipped_empty_phone_rows', 0)}，空对端跳过 {dq.get('skipped_empty_counterparty_rows', 0)}，空设备跳过 {dq.get('skipped_empty_device_rows', 0)}，坏时间 {dq.get('invalid_time_rows', 0)}")
        lines += ["- 字段映射：", "", "| 标准含义 | 原始字段 |", "|---|---|"]
        non_empty = False
        for k, v in item["mapping"].items():
            if v:
                non_empty = True
                lines.append(f"| {k} | `{v}` |")
        if not non_empty:
            lines.append("| 无 | 未识别到可用字段 |")
        if item.get("notes"):
            lines.append("- 角色说明：" + "；".join(item["notes"]))
        if item.get("warnings"):
            lines.append("- 质量提醒：" + "；".join(item["warnings"]))
        lines.append("")

    lines += ["## 四、质量检查", "", "| 检查项 | 状态 | 详情 |", "|---|---|---|"]
    for row in qrows:
        lines.append(f"| {row['check']} | {row['status']} | {row['detail']} |")

    lines += ["", "## 五、后续可调用的分析技能", ""]
    if graph_ready:
        lines.append("当前输出已经按电话网络分析技能的标准 schema 生成。后续可以把 `--dataset` 设置为本次输出的数据集名称，继续调用：")
        lines.append("")
        for skill in ["dataset-overview-analysis", "single-number-analysis", "topn-high-risk-discovery", "condition-based-screening", "shared-device-analysis", "group-risk-analysis", "gang-cluster-analysis", "risk-evidence-pack", "time-series-anomaly-analysis"]:
            lines.append(f"- `{skill}`")
    else:
        lines.append("当前输入没有生成可用于后续分析的完整图结构。请优先处理质量检查中失败或警告的问题，例如补充号码列、对端列、设备列或显式指定字段映射。")

    lines += ["", "## 六、重要边界", "", "- 本 skill 负责把原始表格转换为当前项目使用的图结构表，不负责判断业务事实。", "- 如果输入数据来自不同来源，想做跨数据集同实体联动，必须使用同一哈希规则或提供实体映射表。", "- 默认会对原始号码和设备重新哈希，避免在报告和图结构中暴露原始 ID。", "- 设备表、标签表没有对端列是正常情况，会记录为角色说明，不再当作质量错误。", "- 如果没有识别到号码字段，脚本会正常输出质量报告，但不会声称建图成功。"]
    report_path.write_text("\n".join(lines), encoding="utf-8")



def build_no_input_result(args: argparse.Namespace, input_files: List[Path], processed_dir: Path, graph_view_dir: Path, outputs_dir: Path, output_root: Path) -> Dict[str, Any]:
    """Gracefully return a diagnostic result when no readable input files are found."""
    processed_dir.mkdir(parents=True, exist_ok=True)
    graph_view_dir.mkdir(parents=True, exist_ok=True)
    user_nodes_path = processed_dir / "user_nodes.csv"
    call_edges_path = processed_dir / "call_edges.csv"
    device_edges_path = graph_view_dir / "edges_phone_imei.parquet"
    mapping_path = processed_dir / "schema_mapping.json"
    quality_path = processed_dir / "data_quality_report.csv"
    summary_path = processed_dir / "preprocess_summary.json"
    report_path = outputs_dir / f"dataset_onboarding_graph_preprocess_{args.dataset}.md"

    pd.DataFrame(columns=USER_COLUMNS).to_csv(user_nodes_path, index=False, encoding="utf-8-sig")
    pd.DataFrame(columns=CALL_COLUMNS).to_csv(call_edges_path, index=False, encoding="utf-8-sig")
    write_parquet_safe(pd.DataFrame(columns=DEVICE_COLUMNS), device_edges_path, [])
    quality_rows = [
        {"check": "input_files_processed", "status": "fail", "detail": "0，未找到可读取的输入文件"},
        {"check": "user_nodes_non_empty", "status": "fail", "detail": "0 rows"},
        {"check": "call_edges_non_empty", "status": "warn", "detail": "0 rows"},
        {"check": "device_edges_non_empty", "status": "warn", "detail": "0 rows"},
        {"check": "warnings", "status": "warn", "detail": "没有找到可读取的输入文件。支持 csv/xlsx/xls/parquet/json。"},
    ]
    quality_df = pd.DataFrame(quality_rows)
    quality_df.to_csv(quality_path, index=False, encoding="utf-8-sig")
    output_paths = {"user_nodes": str(user_nodes_path), "call_edges": str(call_edges_path), "device_edges": str(device_edges_path), "mapping_json": str(mapping_path), "quality_csv": str(quality_path), "summary_json": str(summary_path), "report_md": str(report_path)}
    warnings = ["没有找到可读取的输入文件。请检查上传目录或显式指定 --input-file。支持 csv/xlsx/xls/parquet/json。"]
    summary = {"user_nodes": 0, "distinct_users": 0, "call_edges": 0, "device_edges": 0, "distinct_devices": 0, "input_files": 0, "processed_tables": 0, "warnings_count": 1, "notes_count": 0}
    result = {
        "ok": True,
        "graph_ready": False,
        "skill": "dataset-onboarding-graph-preprocess",
        "skill_zh": "电话网络数据接入与建图预处理",
        "query_type": "graph_preprocess",
        "query_type_zh": "原始数据转标准图结构",
        "script_version": SCRIPT_VERSION,
        "status": "not_graph_ready_no_readable_input_files",
        "status_zh": "没有找到可读取的输入文件",
        "dataset": args.dataset,
        "dataset_name": args.dataset_name,
        "output_root": str(output_root),
        "processed_dir": str(processed_dir),
        "graph_view_dir": str(graph_view_dir),
        "preserve_existing_ids": bool(args.preserve_existing_ids),
        "hash_length": int(args.hash_length),
        "artifact_mode": args.artifact_mode,
        "summary": summary,
        "input_file_summaries": [],
        "quality_checks": quality_rows,
        "warnings": warnings,
        "notes": [],
        "output_paths": output_paths,
        "user_message_zh": "没有找到可读取的输入文件。请确认上传文件所在目录，或使用 --input-file 指定具体文件。",
        "frontend_display_policy_zh": "前端只需要展示一次 Markdown 报告或简要摘要，不要连续重复粘贴同一份报告；下载附件以 artifacts 为准。",
        "base_operator_alignment": {
            "node_table_generation": "node_lookup schema construction",
            "call_edge_generation": "relationship_filter compatible edge table",
            "device_edge_generation": "query_shared_device compatible bipartite graph",
            "aggregation_support": "aggregation_query ready processed tables",
        },
    }
    result["artifacts"] = make_artifacts(output_paths, "markdown_only")
    result["report_path"] = str(report_path)
    mapping_path.write_text(json.dumps({"script_version": SCRIPT_VERSION, "dataset": args.dataset, "input_files": [], "file_summaries": [], "warnings": warnings, "notes": []}, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    write_report(report_path, result)
    summary_path.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return result

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dataset onboarding and graph preprocess for phone-network-analysis")
    parser.add_argument("--input-dir", type=str, default=None)
    parser.add_argument("--input-file", action="append", default=[])
    parser.add_argument("--dataset-root", "--output-root", dest="output_root", type=str, default="/workspace/imiss-deer-flow-main/datasets/phone-network")
    parser.add_argument("--dataset", type=str, default="onboarded_demo")
    parser.add_argument("--dataset-name", type=str, default=None)
    parser.add_argument("--province", type=str, default="unknown")
    parser.add_argument("--hash-salt", type=str, default="phone-network-analysis-default-salt")
    parser.add_argument("--hash-length", type=int, default=64, choices=[16, 32, 40, 64, 96, 128])
    parser.add_argument("--preserve-existing-ids", action="store_true")
    parser.add_argument("--artifact-mode", choices=["full", "essential", "markdown_only"], default="full")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--source-col", type=str, default=None)
    parser.add_argument("--target-col", type=str, default=None)
    parser.add_argument("--device-col", type=str, default=None)
    parser.add_argument("--time-col", type=str, default=None)
    parser.add_argument("--date-col", type=str, default=None)
    parser.add_argument("--hour-col", type=str, default=None)
    parser.add_argument("--duration-col", type=str, default=None)
    parser.add_argument("--call-type-col", type=str, default=None)
    parser.add_argument("--province-col", type=str, default=None)
    parser.add_argument("--label-col", type=str, default=None)
    parser.add_argument("--sub-label-col", type=str, default=None)
    args = parser.parse_args()
    if args.dataset_name is None:
        args.dataset_name = f"phone-network-{args.dataset}"
    if not args.input_dir and not args.input_file:
        parser.error("必须提供 --input-dir 或 --input-file")
    return args


def main() -> None:
    args = parse_args()
    input_files = collect_input_files(Path(args.input_dir).resolve() if args.input_dir else None, args.input_file)
    output_root = Path(args.output_root).resolve()
    processed_dir = output_root / "processed" / args.dataset
    graph_view_dir = output_root / "processed" / "graph_views" / args.dataset
    outputs_dir = Path("/mnt/user-data/outputs")
    outputs_dir.mkdir(parents=True, exist_ok=True)
    if processed_dir.exists() and any(processed_dir.iterdir()) and not args.overwrite:
        raise SystemExit(f"输出目录已存在且非空：{processed_dir}。如需覆盖，请加 --overwrite。")
    processed_dir.mkdir(parents=True, exist_ok=True)
    graph_view_dir.mkdir(parents=True, exist_ok=True)
    if not input_files:
        result = build_no_input_result(args, input_files, processed_dir, graph_view_dir, outputs_dir, output_root)
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        return

    all_users: List[Dict[str, Any]] = []
    all_calls: List[Dict[str, Any]] = []
    all_devices: List[Dict[str, Any]] = []
    file_summaries: List[Dict[str, Any]] = []
    warnings: List[str] = []
    notes: List[str] = []
    for path in input_files:
        try:
            frames = read_input_file(path)
        except Exception as exc:
            warnings.append(f"读取失败 {path}: {exc}")
            continue
        for source_name, df in frames:
            if df.empty:
                warnings.append(f"输入为空：{source_name}")
                continue
            mapping = detect_mapping(df, args)
            users, calls, devices, stats = build_from_frame(source_name, df, mapping, args)
            all_users.extend(users)
            all_calls.extend(calls)
            all_devices.extend(devices)
            file_summaries.append(stats)
            warnings.extend([f"{source_name}: {w}" for w in stats.get("warnings", [])])
            notes.extend([f"{source_name}: {n}" for n in stats.get("notes", [])])

    users_df = coalesce_user_rows(pd.DataFrame(all_users))
    calls_df = finalize_call_edges(pd.DataFrame(all_calls))
    devices_df = finalize_device_edges(pd.DataFrame(all_devices))
    user_nodes_path = processed_dir / "user_nodes.csv"
    call_edges_path = processed_dir / "call_edges.csv"
    device_edges_path = graph_view_dir / "edges_phone_imei.parquet"
    mapping_path = processed_dir / "schema_mapping.json"
    quality_path = processed_dir / "data_quality_report.csv"
    summary_path = processed_dir / "preprocess_summary.json"
    report_path = outputs_dir / f"dataset_onboarding_graph_preprocess_{args.dataset}.md"
    users_df.to_csv(user_nodes_path, index=False, encoding="utf-8-sig")
    calls_df.to_csv(call_edges_path, index=False, encoding="utf-8-sig")
    write_parquet_safe(devices_df, device_edges_path, warnings)
    quality_df = build_quality_report(users_df, calls_df, devices_df, file_summaries, warnings, notes)
    quality_df.to_csv(quality_path, index=False, encoding="utf-8-sig")
    output_paths = {"user_nodes": str(user_nodes_path), "call_edges": str(call_edges_path), "device_edges": str(device_edges_path), "mapping_json": str(mapping_path), "quality_csv": str(quality_path), "summary_json": str(summary_path), "report_md": str(report_path)}
    summary = {"user_nodes": int(len(users_df)), "distinct_users": int(users_df["user_id"].nunique()) if "user_id" in users_df.columns else 0, "call_edges": int(len(calls_df)), "device_edges": int(len(devices_df)), "distinct_devices": int(devices_df["imei"].nunique()) if "imei" in devices_df.columns else 0, "input_files": len(input_files), "processed_tables": len(file_summaries), "warnings_count": len(warnings), "notes_count": len(notes)}
    mapping_path.write_text(json.dumps({"script_version": SCRIPT_VERSION, "dataset": args.dataset, "input_files": [str(p) for p in input_files], "file_summaries": file_summaries, "warnings": warnings, "notes": notes}, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    graph_ready = bool(len(users_df) > 0 and (len(calls_df) > 0 or len(devices_df) > 0))
    if graph_ready:
        status = "ok"
        status_zh = "已成功生成标准图结构"
    elif len(users_df) > 0:
        status = "partial_nodes_only"
        status_zh = "只生成了用户节点，缺少通话边或设备边"
    else:
        status = "not_graph_ready_missing_phone_column"
        status_zh = "未识别到号码字段，无法生成可分析图结构"
    result = {
        "ok": True,
        "graph_ready": graph_ready,
        "skill": "dataset-onboarding-graph-preprocess",
        "skill_zh": "电话网络数据接入与建图预处理",
        "query_type": "graph_preprocess",
        "query_type_zh": "原始数据转标准图结构",
        "script_version": SCRIPT_VERSION,
        "status": status,
        "status_zh": status_zh,
        "dataset": args.dataset,
        "dataset_name": args.dataset_name,
        "output_root": str(output_root),
        "processed_dir": str(processed_dir),
        "graph_view_dir": str(graph_view_dir),
        "preserve_existing_ids": bool(args.preserve_existing_ids),
        "hash_length": int(args.hash_length),
        "artifact_mode": args.artifact_mode,
        "summary": summary,
        "input_file_summaries": file_summaries,
        "quality_checks": quality_df.to_dict(orient="records"),
        "warnings": warnings,
        "notes": notes,
        "output_paths": output_paths,
        "user_message_zh": status_zh + "。请查看 Markdown 报告和质量检查表。",
        "frontend_display_policy_zh": "前端只需要展示一次 Markdown 报告或简要摘要，不要连续重复粘贴同一份报告；下载附件以 artifacts 为准。",
        "base_operator_alignment": {
            "node_table_generation": "node_lookup schema construction",
            "call_edge_generation": "relationship_filter compatible edge table",
            "device_edge_generation": "query_shared_device compatible bipartite graph",
            "aggregation_support": "aggregation_query ready processed tables",
        },
    }
    result["artifacts"] = make_artifacts(output_paths, args.artifact_mode)
    result["report_path"] = str(report_path)
    write_report(report_path, result)
    summary_path.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
