#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd

SCRIPT_VERSION = "condition-based-screening-release-v3"


def split_csv_arg(value: Optional[str]) -> List[str]:
    if not value:
        return []
    return [x.strip() for x in str(value).split(",") if x.strip()]


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def safe_int(v, default: int = 0) -> int:
    try:
        if pd.isna(v):
            return default
        return int(v)
    except Exception:
        return default


def safe_float(v, default: float = 0.0) -> float:
    try:
        if pd.isna(v):
            return default
        return float(v)
    except Exception:
        return default


def preview(v: object, n: int = 12) -> str:
    if v is None:
        return ""
    s = str(v)
    return s if len(s) <= n else s[:n] + "..."


def find_repo_root() -> Path:
    script_path = Path(__file__).resolve()
    for p in [script_path.parent] + list(script_path.parents):
        if (p / "datasets" / "phone-network").exists():
            return p
    if len(script_path.parents) >= 6:
        return script_path.parents[5]
    return script_path.parent


def detect_dataset_root(candidate: Optional[str]) -> Path:
    script_dir = Path(__file__).resolve().parent
    env_root = os.getenv("PHONE_NETWORK_DATASETS_ROOT")
    repo_root = find_repo_root()
    cands: List[Path] = []
    if candidate:
        cands.append(Path(candidate).expanduser())
    if env_root:
        cands.append(Path(env_root).expanduser())
    cands.extend(
        [
            repo_root / "datasets" / "phone-network",
            script_dir / "condition_based_screening_testdata" / "datasets" / "phone-network",
            Path.cwd() / "condition_based_screening_testdata" / "datasets" / "phone-network",
            Path("datasets/phone-network"),
            Path("/workspace/imiss-deer-flow-main/datasets/phone-network"),
            Path("/mnt/datasets/phone-network"),
            Path.home() / "imiss-deer-flow-main" / "datasets" / "phone-network",
        ]
    )
    checked: List[str] = []
    seen = set()
    for c in cands:
        c = c.resolve() if not c.is_absolute() else c
        key = str(c)
        if key in seen:
            continue
        seen.add(key)
        checked.append(key)
        if c.exists():
            return c
    raise FileNotFoundError(
        "无法找到电话网络数据集根目录，请通过 --dataset-root 指定。已检查路径: "
        + " | ".join(checked)
    )


def find_existing(paths: Sequence[Path]) -> Optional[Path]:
    for p in paths:
        if p.exists():
            return p
    return None


def load_csv_or_parquet(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path, low_memory=False)


def resolve_phone_id_file(path_str: str) -> Path:
    p = Path(path_str).expanduser()
    if p.exists():
        return p
    script_dir = Path(__file__).resolve().parent
    cands = [script_dir / path_str, Path.cwd() / path_str]
    for c in cands:
        if c.exists():
            return c
    raise FileNotFoundError(f"未找到号码文件: {path_str}")


def load_call_edges(dataset_root: Path, dataset: str) -> Tuple[pd.DataFrame, Path]:
    paths = [
        dataset_root / "processed" / dataset / "call_edges.csv",
        dataset_root / "processed" / dataset / "call_edges.parquet",
        dataset_root / "processed" / "graph_views" / dataset / "edges_call.csv",
        dataset_root / "processed" / "graph_views" / dataset / "edges_call.parquet",
    ]
    path = find_existing(paths)
    if path is None:
        raise FileNotFoundError(f"未找到 {dataset} 的通话边文件")
    df = load_csv_or_parquet(path)
    rename_map = {}
    if "src_user_id" not in df.columns and "user_id" in df.columns:
        rename_map["user_id"] = "src_user_id"
    if "dst_counterparty_id" not in df.columns and "counterparty_id" in df.columns:
        rename_map["counterparty_id"] = "dst_counterparty_id"
    if rename_map:
        df = df.rename(columns=rename_map)
    need = {"src_user_id", "dst_counterparty_id"}
    if not need.issubset(df.columns):
        raise ValueError("call_edges 缺少 src_user_id / dst_counterparty_id 列")
    return df, path


def load_user_nodes(dataset_root: Path, dataset: str) -> Tuple[pd.DataFrame, Optional[Path]]:
    paths = [
        dataset_root / "processed" / dataset / "user_nodes.csv",
        dataset_root / "processed" / dataset / "user_nodes.parquet",
        dataset_root / "processed" / "graph_views" / dataset / "user_nodes.csv",
        dataset_root / "processed" / "graph_views" / dataset / "user_nodes.parquet",
    ]
    path = find_existing(paths)
    if path is None:
        return pd.DataFrame(columns=["user_id", "province", "label", "sub_label", "risk_score"]), None
    df = load_csv_or_parquet(path)
    if "phone_id" in df.columns and "user_id" not in df.columns:
        df = df.rename(columns={"phone_id": "user_id"})
    for col in ["user_id", "province", "label", "sub_label", "risk_score"]:
        if col not in df.columns:
            df[col] = None
    return df[["user_id", "province", "label", "sub_label", "risk_score"]].drop_duplicates("user_id"), path


def load_device_edges(dataset_root: Path, dataset: str, call_df: pd.DataFrame) -> Tuple[pd.DataFrame, Optional[Path]]:
    paths = [
        dataset_root / "processed" / dataset / "edges_phone_imei.csv",
        dataset_root / "processed" / dataset / "edges_phone_imei.parquet",
        dataset_root / "processed" / "graph_views" / dataset / "edges_phone_imei.csv",
        dataset_root / "processed" / "graph_views" / dataset / "edges_phone_imei.parquet",
    ]
    path = find_existing(paths)
    if path is not None:
        df = load_csv_or_parquet(path)
        if "src_user_id" in df.columns and "user_id" not in df.columns:
            df = df.rename(columns={"src_user_id": "user_id"})
        if {"user_id", "imei"}.issubset(df.columns):
            return df[["user_id", "imei"]].dropna().drop_duplicates(), path
    if {"src_user_id", "imei"}.issubset(call_df.columns):
        derived = (
            call_df[["src_user_id", "imei"]]
            .rename(columns={"src_user_id": "user_id"})
            .dropna()
            .drop_duplicates()
        )
        return derived, None
    return pd.DataFrame(columns=["user_id", "imei"]), None


def load_scope_phone_ids(args: argparse.Namespace) -> List[str]:
    phone_ids: List[str] = []
    if args.phone_ids:
        phone_ids.extend(split_csv_arg(args.phone_ids))
    if args.phone_id_file:
        resolved = resolve_phone_id_file(args.phone_id_file)
        with open(resolved, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if s:
                    phone_ids.append(s)
    if args.input_csv and args.phone_id_column:
        df = pd.read_csv(args.input_csv, low_memory=False)
        if args.phone_id_column not in df.columns:
            raise ValueError(f"输入 CSV 不包含列: {args.phone_id_column}")
        phone_ids.extend(df[args.phone_id_column].dropna().astype(str).tolist())
    out: List[str] = []
    seen = set()
    for p in phone_ids:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


def normalize_user_meta(user_df: pd.DataFrame, call_df: pd.DataFrame) -> pd.DataFrame:
    df = user_df.copy()
    for col in ["user_id", "province", "label", "sub_label", "risk_score"]:
        if col not in df.columns:
            df[col] = None
    df = df[["user_id", "province", "label", "sub_label", "risk_score"]].drop_duplicates("user_id")
    if "province" in call_df.columns:
        derived = (
            call_df[["src_user_id", "province"]]
            .dropna()
            .rename(columns={"src_user_id": "user_id"})
            .drop_duplicates("user_id")
        )
        df = df.merge(derived, on="user_id", how="outer", suffixes=("", "_from_call"))
        if "province_from_call" in df.columns:
            df["province"] = df["province"].fillna(df["province_from_call"])
            df = df.drop(columns=["province_from_call"])
    df["province"] = df["province"].fillna("unknown")
    df["label"] = df["label"].fillna(0)
    df["sub_label"] = df["sub_label"].fillna("")
    df["risk_score"] = pd.to_numeric(df["risk_score"], errors="coerce").fillna(0.0)
    return df


def build_user_metrics(call_df: pd.DataFrame, device_df: pd.DataFrame, user_meta_df: pd.DataFrame, args: argparse.Namespace) -> pd.DataFrame:
    work = call_df.copy()
    if "event_hour" not in work.columns:
        if "event_time" in work.columns:
            work["event_hour"] = pd.to_datetime(work["event_time"], errors="coerce").dt.hour
        else:
            work["event_hour"] = None

    def is_night_hour(h: object) -> bool:
        if pd.isna(h):
            return False
        hour = int(h)
        start = int(args.night_start_hour)
        end = int(args.night_end_hour)
        if start <= end:
            return start <= hour <= end
        return hour >= start or hour <= end

    work["is_night"] = work["event_hour"].apply(is_night_hour)
    grouped = work.groupby("src_user_id", dropna=True)
    metrics = pd.DataFrame({
        "user_id": list(grouped.size().index),
        "call_count": grouped.size().values,
        "unique_counterparties": grouped["dst_counterparty_id"].nunique().values,
        "night_call_count": grouped["is_night"].sum().values,
        "avg_call_duration": grouped["duration"].mean().values if "duration" in work.columns else [0.0] * len(grouped),
    })
    metrics["night_call_ratio"] = metrics.apply(
        lambda r: safe_float(r["night_call_count"]) / max(safe_int(r["call_count"]), 1), axis=1
    )

    if not device_df.empty:
        dg = device_df.groupby("imei")["user_id"].nunique().reset_index(name="device_user_count")
        dw = device_df.merge(dg, on="imei", how="left")
        dw["shared_peer_count"] = dw["device_user_count"].fillna(1).astype(int) - 1
        dw["is_shared_device"] = dw["device_user_count"].fillna(1).astype(int) > 1
        per_user = dw.groupby("user_id").agg(
            device_count=("imei", pd.Series.nunique),
            shared_device_count=("is_shared_device", "sum"),
            shared_peer_total=("shared_peer_count", "sum"),
        ).reset_index()
        metrics = metrics.merge(per_user, on="user_id", how="left")
    else:
        metrics["device_count"] = 0
        metrics["shared_device_count"] = 0
        metrics["shared_peer_total"] = 0

    metrics = metrics.merge(user_meta_df, on="user_id", how="left")
    for col in ["call_count", "unique_counterparties", "night_call_count", "device_count", "shared_device_count", "shared_peer_total"]:
        metrics[col] = pd.to_numeric(metrics[col], errors="coerce").fillna(0).astype(int)
    for col in ["avg_call_duration", "night_call_ratio", "risk_score"]:
        metrics[col] = pd.to_numeric(metrics[col], errors="coerce").fillna(0.0)
    metrics["province"] = metrics["province"].fillna("unknown")
    metrics["label"] = metrics["label"].fillna(0)
    metrics["sub_label"] = metrics["sub_label"].fillna("")
    metrics["risk_flag"] = (
        (pd.to_numeric(metrics["label"], errors="coerce").fillna(0) > 0)
        | metrics["sub_label"].astype(str).str.lower().isin({"risk", "purefraud", "fraud", "high_risk"})
    )
    return metrics


def derive_auto_threshold(series: pd.Series, quantile: float = 0.9, floor: float = 1.0) -> float:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return floor
    return max(float(s.quantile(quantile)), floor)


def apply_mode_defaults(metrics_df: pd.DataFrame, args: argparse.Namespace) -> Dict[str, float]:
    thresholds: Dict[str, float] = {}
    if metrics_df.empty:
        return thresholds
    if args.mode == "high_call_volume" and args.min_call_records is None:
        thresholds["min_call_records"] = math.ceil(derive_auto_threshold(metrics_df["call_count"], 0.9, 10))
    if args.mode == "broad_contacts" and args.min_counterparties is None:
        thresholds["min_counterparties"] = math.ceil(derive_auto_threshold(metrics_df["unique_counterparties"], 0.9, 5))
    if args.mode == "shared_device":
        if args.min_shared_device_count is None:
            thresholds["min_shared_device_count"] = 1
        if args.min_shared_peer_total is None:
            thresholds["min_shared_peer_total"] = math.ceil(derive_auto_threshold(metrics_df["shared_peer_total"], 0.75, 2))
    if args.mode == "night_abnormal":
        if args.min_night_ratio is None:
            thresholds["min_night_ratio"] = max(args.night_ratio_threshold, 0.3)
        if args.min_night_count is None:
            thresholds["min_night_count"] = max(args.night_count_threshold, 5)
    if args.mode == "mixed":
        if args.min_counterparties is None:
            thresholds["min_counterparties"] = math.ceil(derive_auto_threshold(metrics_df["unique_counterparties"], 0.8, 5))
        if args.min_shared_device_count is None:
            thresholds["min_shared_device_count"] = 1
    return thresholds


def build_condition_columns(df: pd.DataFrame, args: argparse.Namespace, auto_thresholds: Dict[str, float]) -> Tuple[pd.DataFrame, List[str], Dict[str, str]]:
    work = df.copy()
    active: List[str] = []
    desc: Dict[str, str] = {}

    def add_condition(name: str, mask: pd.Series, description: str) -> None:
        work[f"match_{name}"] = mask.fillna(False)
        active.append(name)
        desc[name] = description

    labels = split_csv_arg(args.labels)
    sub_labels = split_csv_arg(args.sub_labels)
    provinces = split_csv_arg(args.province)

    if args.risk_only:
        add_condition("risk_only", work["risk_flag"], "仅保留风险标签对象")
    if args.unlabeled_only:
        add_condition("unlabeled_only", ~work["risk_flag"], "仅保留未显式标注为风险的对象")
    if labels:
        add_condition("label", work["label"].astype(str).isin(labels), f"标签属于 {labels}")
    if sub_labels:
        add_condition("sub_label", work["sub_label"].astype(str).isin(sub_labels), f"sub_label 属于 {sub_labels}")
    if provinces:
        add_condition("province", work["province"].astype(str).isin(provinces), f"省份属于 {provinces}")
    if args.min_risk_score is not None:
        add_condition("risk_score", work["risk_score"] >= float(args.min_risk_score), f"risk_score >= {float(args.min_risk_score):.2f}")

    min_call_records = args.min_call_records if args.min_call_records is not None else auto_thresholds.get("min_call_records")
    if min_call_records is not None:
        add_condition("call_volume", work["call_count"] >= int(min_call_records), f"通话记录数 >= {int(min_call_records)}")

    min_counterparties = args.min_counterparties if args.min_counterparties is not None else auto_thresholds.get("min_counterparties")
    if min_counterparties is not None:
        add_condition("broad_contacts", work["unique_counterparties"] >= int(min_counterparties), f"联系人广度 >= {int(min_counterparties)}")

    min_shared_device_count = args.min_shared_device_count if args.min_shared_device_count is not None else auto_thresholds.get("min_shared_device_count")
    if min_shared_device_count is not None:
        add_condition("shared_device_count", work["shared_device_count"] >= int(min_shared_device_count), f"共享设备数 >= {int(min_shared_device_count)}")

    min_shared_peer_total = args.min_shared_peer_total if args.min_shared_peer_total is not None else auto_thresholds.get("min_shared_peer_total")
    if min_shared_peer_total is not None:
        add_condition("shared_peer_total", work["shared_peer_total"] >= int(min_shared_peer_total), f"共享设备牵出号码总量 >= {int(min_shared_peer_total)}")

    min_night_ratio = args.min_night_ratio if args.min_night_ratio is not None else auto_thresholds.get("min_night_ratio")
    if min_night_ratio is not None:
        add_condition("night_ratio", work["night_call_ratio"] >= float(min_night_ratio), f"夜间通话占比 >= {float(min_night_ratio):.2f}")

    min_night_count = args.min_night_count if args.min_night_count is not None else auto_thresholds.get("min_night_count")
    if min_night_count is not None:
        add_condition("night_count", work["night_call_count"] >= int(min_night_count), f"夜间通话次数 >= {int(min_night_count)}")

    return work, active, desc


def calc_match_mask(df: pd.DataFrame, active: List[str], match_mode: str) -> pd.Series:
    if not active:
        return pd.Series([True] * len(df), index=df.index)
    cols = [f"match_{x}" for x in active]
    if match_mode == "any":
        return df[cols].any(axis=1)
    return df[cols].all(axis=1)


def build_filter_chain(df: pd.DataFrame, active: List[str], match_mode: str) -> List[Dict[str, object]]:
    chain: List[Dict[str, object]] = [{"step": "initial_scope", "count": int(len(df))}]
    if not active:
        return chain
    if match_mode == "all":
        cur = df.copy()
        for name in active:
            cur = cur[cur[f"match_{name}"]]
            chain.append({"step": f"after_{name}", "count": int(len(cur))})
    else:
        for name in active:
            cnt = int(df[df[f"match_{name}"]].shape[0])
            chain.append({"step": f"hit_{name}", "count": cnt})
        union_count = int(calc_match_mask(df, active, "any").sum())
        chain.append({"step": "after_any_union", "count": union_count})
    return chain


def compute_screening_score(df: pd.DataFrame, active: List[str]) -> pd.Series:
    if df.empty:
        return pd.Series(dtype=float)
    out = pd.Series(0.0, index=df.index)
    if active:
        out += df[[f"match_{x}" for x in active]].sum(axis=1) * 10.0
    boosters = [
        ("risk_score", 5),
        ("call_count", 3),
        ("unique_counterparties", 3),
        ("night_call_ratio", 4),
        ("shared_device_count", 4),
        ("shared_peer_total", 3),
    ]
    for col, weight in boosters:
        if col in df.columns:
            s = pd.to_numeric(df[col], errors="coerce").fillna(0)
            if float(s.max()) > 0:
                out += s.rank(pct=True) * weight
    out += df.get("risk_flag", pd.Series(False, index=df.index)).astype(int) * 2.0
    return out.round(4)


def build_condition_evidence(row: pd.Series, active: List[str]) -> Tuple[List[str], str]:
    matched: List[str] = []
    pieces: List[str] = []
    for name in active:
        if not bool(row.get(f"match_{name}", False)):
            continue
        matched.append(name)
        if name == "call_volume":
            pieces.append(f"通话量={safe_int(row.get('call_count'))}")
        elif name == "broad_contacts":
            pieces.append(f"联系人广度={safe_int(row.get('unique_counterparties'))}")
        elif name == "shared_device_count":
            pieces.append(f"共享设备数={safe_int(row.get('shared_device_count'))}")
        elif name == "shared_peer_total":
            pieces.append(f"共享设备牵出号码={safe_int(row.get('shared_peer_total'))}")
        elif name == "night_ratio":
            pieces.append(f"夜间占比={safe_float(row.get('night_call_ratio')):.2f}")
        elif name == "night_count":
            pieces.append(f"夜间次数={safe_int(row.get('night_call_count'))}")
        elif name == "province":
            pieces.append(f"省份={row.get('province')}")
        elif name == "sub_label":
            pieces.append(f"sub_label={row.get('sub_label')}")
        elif name == "label":
            pieces.append(f"label={row.get('label')}")
        elif name == "risk_only":
            pieces.append("风险标签对象")
        elif name == "unlabeled_only":
            pieces.append("未标注风险对象")
        elif name == "risk_score":
            pieces.append(f"risk_score={safe_float(row.get('risk_score')):.2f}")
    return matched, "；".join(pieces)


def build_shared_device_artifact(screened_df: pd.DataFrame, device_df: pd.DataFrame, user_meta_df: pd.DataFrame, top_k: int) -> pd.DataFrame:
    cols = ["imei", "screened_member_count", "total_phone_count", "risk_phone_count", "province_count", "member_preview"]
    if screened_df.empty or device_df.empty:
        return pd.DataFrame(columns=cols)
    screen_ids = set(screened_df["user_id"].astype(str))
    dg = device_df.groupby("imei")["user_id"].nunique().reset_index(name="total_phone_count")
    meta = user_meta_df[["user_id", "province", "risk_score"]].copy()
    meta["risk_flag"] = (
        pd.to_numeric(user_meta_df.get("label", 0), errors="coerce").fillna(0) > 0
    ) | user_meta_df.get("sub_label", pd.Series("", index=user_meta_df.index)).astype(str).str.lower().isin({"risk", "purefraud", "fraud", "high_risk"})
    merged = device_df.merge(dg, on="imei", how="left").merge(meta[["user_id", "province", "risk_flag"]], on="user_id", how="left")
    screen_only = merged[merged["user_id"].astype(str).isin(screen_ids)].copy()
    if screen_only.empty:
        return pd.DataFrame(columns=cols)
    out = screen_only.groupby("imei").agg(
        screened_member_count=("user_id", pd.Series.nunique),
        total_phone_count=("total_phone_count", "max"),
        risk_phone_count=("risk_flag", "sum"),
        province_count=("province", pd.Series.nunique),
    ).reset_index()
    previews = screen_only.groupby("imei")["user_id"].apply(lambda s: ", ".join(sorted(preview(x) for x in set(s)))).reset_index(name="member_preview")
    out = out.merge(previews, on="imei", how="left")
    return out.sort_values(["screened_member_count", "total_phone_count", "imei"], ascending=[False, False, True]).head(max(top_k, 1))


def build_common_counterparty_artifact(screened_df: pd.DataFrame, call_df: pd.DataFrame, top_k: int) -> pd.DataFrame:
    cols = ["counterparty_id", "screened_member_count", "total_calls", "member_preview"]
    if screened_df.empty or call_df.empty:
        return pd.DataFrame(columns=cols)
    screen_ids = set(screened_df["user_id"].astype(str))
    scoped = call_df[call_df["src_user_id"].astype(str).isin(screen_ids)].copy()
    if scoped.empty:
        return pd.DataFrame(columns=cols)
    agg = scoped.groupby("dst_counterparty_id").agg(
        screened_member_count=("src_user_id", pd.Series.nunique),
        total_calls=("src_user_id", "size"),
    ).reset_index().rename(columns={"dst_counterparty_id": "counterparty_id"})
    agg = agg[agg["screened_member_count"] >= 2]
    if agg.empty:
        return pd.DataFrame(columns=cols)
    previews = scoped.groupby("dst_counterparty_id")["src_user_id"].apply(lambda s: ", ".join(sorted(preview(x) for x in set(s)))).reset_index(name="member_preview").rename(columns={"dst_counterparty_id": "counterparty_id"})
    agg = agg.merge(previews, on="counterparty_id", how="left")
    return agg.sort_values(["screened_member_count", "total_calls", "counterparty_id"], ascending=[False, False, True]).head(max(top_k, 1))


def explain_filter_impact(scope_before: int, scope_after: int, active: List[str], match_mode: str) -> str:
    if not active:
        return "本次没有显式条件过滤，结果更接近全量对象概览。"
    if scope_before == scope_after:
        return "当前筛选条件没有减少样本，说明输入对象在这些条件上高度同质，本次更像条件确认而不是样本缩小。"
    removed = scope_before - scope_after
    ratio = removed / max(scope_before, 1)
    return f"当前筛选条件共缩小 {removed} 个对象，收缩比例 {ratio:.2%}，筛选对样本范围产生了真实约束；匹配模式为 {match_mode}。"


def build_distribution(df: pd.DataFrame, col: str, top_k: int = 5) -> List[Dict[str, object]]:
    if df.empty or col not in df.columns:
        return []
    s = df[col].fillna("unknown").astype(str)
    vc = s.value_counts().head(max(top_k, 1))
    return [{"value": str(idx), "count": int(cnt)} for idx, cnt in vc.items()]


def build_condition_hit_stats(df: pd.DataFrame, active: List[str]) -> List[Dict[str, object]]:
    stats: List[Dict[str, object]] = []
    if df.empty:
        return stats
    for name in active:
        col = f"match_{name}"
        if col not in df.columns:
            continue
        stats.append({"condition": name, "hit_count": int(df[col].sum())})
    return stats


def build_top_signal_summary(screened_df: pd.DataFrame, device_df: pd.DataFrame, cp_df: pd.DataFrame) -> List[str]:
    out: List[str] = []
    if not screened_df.empty:
        top1 = screened_df.iloc[0]
        out.append(
            f"Top1 对象 {preview(top1.get('user_id'))}：风险分 {safe_float(top1.get('screening_score')):.2f}，命中 {safe_int(top1.get('matched_condition_count'))} 条条件。"
        )
    if not device_df.empty:
        d = device_df.iloc[0]
        out.append(
            f"最显著共享设备 {preview(d.get('imei'))}：群体内命中成员 {safe_int(d.get('screened_member_count'))} 个，共挂载 {safe_int(d.get('total_phone_count'))} 个号码。"
        )
    if not cp_df.empty:
        c = cp_df.iloc[0]
        out.append(
            f"最显著共同对端 {preview(c.get('counterparty_id'))}：被 {safe_int(c.get('screened_member_count'))} 个命中对象共同联系，累计通话 {safe_int(c.get('total_calls'))} 次。"
        )
    return out


def recommend_followup(row: pd.Series) -> str:
    recs: List[str] = []
    if safe_int(row.get("shared_device_count")) > 0:
        recs.append("shared-device-analysis")
    if safe_int(row.get("call_count")) > 0:
        recs.append("single-number-analysis")
    if safe_float(row.get("night_call_ratio")) >= 0.3:
        recs.append("time-series-anomaly-analysis")
    return ", ".join(recs) if recs else "single-number-analysis"


def build_report(summary: Dict[str, object], condition_desc: Dict[str, str], screened_df: pd.DataFrame, device_df: pd.DataFrame, cp_df: pd.DataFrame, output_files: Dict[str, Path]) -> str:
    lines: List[str] = []
    lines.append(f"# 条件筛选分析报告：{summary['group_name']}")
    lines.append("")
    lines.append("## 一、总体结论")
    lines.append("")
    lines.append(f"- 数据集：`{summary['dataset']}`")
    lines.append(f"- 候选对象数：`{summary['candidate_count']}`")
    lines.append(f"- 筛选命中数：`{summary['screened_count']}`")
    lines.append(f"- 模式：`{summary['mode']}`")
    lines.append(f"- 匹配模式：`{summary['match_mode']}`")
    lines.append(f"- 收缩比例：`{summary['shrink_ratio_pct']}`")
    lines.append(f"- 总体结论：{summary['screening_conclusion']}")
    lines.append(f"- 过滤影响：{summary['filter_impact_explanation']}")
    for item in summary.get('top_signal_summary', []):
        lines.append(f"- 关键发现：{item}")
    lines.append("")
    lines.append("## 二、启用的筛选条件")
    lines.append("")
    if condition_desc:
        for k, v in condition_desc.items():
            lines.append(f"- `{k}`：{v}")
    else:
        lines.append("- 无显式条件，当前结果更像范围概览。")
    lines.append("")
    lines.append("## 三、筛选链路")
    lines.append("")
    for step in summary["filter_chain"]:
        lines.append(f"- {step['step']}：{step['count']}")
    if summary.get('condition_hit_stats'):
        lines.append("")
        lines.append("### 条件命中统计")
        for item in summary['condition_hit_stats']:
            lines.append(f"- {item['condition']}：命中 {item['hit_count']} 个对象")
    lines.append("")
    lines.append("## 四、命中对象 Top 列表")
    lines.append("")
    if screened_df.empty:
        lines.append("- 当前条件下无命中对象。")
    else:
        for i, (_, row) in enumerate(screened_df.head(max(int(summary['top_k']), 1)).iterrows(), 1):
            lines.append(
                f"- Rank {i}: `{preview(row['user_id'])}` | score={row['screening_score']} | matched={row['matched_condition_count']} | evidence={row['evidence_summary']} | 推荐下钻={row['recommended_followup']}"
            )
    lines.append("")
    lines.append("## 五、命中对象画像概览")
    lines.append("")
    prov = summary.get('province_distribution', [])
    subs = summary.get('sub_label_distribution', [])
    if prov:
        lines.append("### 省份分布")
        for item in prov:
            lines.append(f"- {item['value']}：{item['count']} 个")
    if subs:
        lines.append("")
        lines.append("### sub_label 分布")
        for item in subs:
            lines.append(f"- {item['value']}：{item['count']} 个")
    lines.append("")
    lines.append("## 六、共享设备证据")
    lines.append("")
    if device_df.empty:
        lines.append("- 当前没有足够显著的共享设备证据。")
    else:
        for _, row in device_df.iterrows():
            lines.append(
                f"- 设备 `{preview(row['imei'])}` | 命中成员数={safe_int(row['screened_member_count'])} | 总挂载号码数={safe_int(row['total_phone_count'])} | 风险号码数={safe_int(row['risk_phone_count'])} | 成员预览={row['member_preview']}"
            )
    lines.append("")
    lines.append("## 七、共同对端证据")
    lines.append("")
    if cp_df.empty:
        lines.append("- 当前没有足够显著的共同对端证据。")
    else:
        for _, row in cp_df.iterrows():
            lines.append(
                f"- 对端 `{preview(row['counterparty_id'])}` | 共接触成员数={safe_int(row['screened_member_count'])} | 总通话量={safe_int(row['total_calls'])} | 成员预览={row['member_preview']}"
            )
    lines.append("")
    lines.append("## 八、后续建议")
    lines.append("")
    for item in summary["next_step_suggestions"]:
        lines.append(f"- {item}")
    lines.append("")
    lines.append("## 九、基础算子对齐")
    lines.append("")
    lines.append("- 条件对象筛选 = node_lookup + aggregation_query + relationship_filter")
    lines.append("- 夜间行为筛选 = time-window relationship_filter + aggregation_query")
    lines.append("- 联系人广度筛选 = neighbor_query + aggregation_query")
    lines.append("- 共享设备筛选 = query_shared_device + aggregation_query")
    lines.append("- 条件命中排序 = aggregation_query + scoring_layer")
    lines.append("")
    lines.append("## 十、生成文件")
    lines.append("")
    for key, path in output_files.items():
        lines.append(f"- `{key}`：{path.name}")
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Condition-based screening for phone-network targets")
    p.add_argument("--dataset-root", default=None)
    p.add_argument("--dataset", default="unified")
    p.add_argument("--group-name", default="condition_screening")
    p.add_argument("--phone-ids", default=None)
    p.add_argument("--phone-id-file", default=None)
    p.add_argument("--input-csv", default=None)
    p.add_argument("--phone-id-column", default=None)
    p.add_argument("--output-root", default="/mnt/user-data/outputs")
    p.add_argument("--top-k", type=int, default=10)
    p.add_argument("--mode", choices=["mixed", "night_abnormal", "broad_contacts", "shared_device", "high_call_volume", "tag_focus"], default="mixed")
    p.add_argument("--match-mode", choices=["all", "any"], default="all")

    p.add_argument("--risk-only", action="store_true")
    p.add_argument("--unlabeled-only", action="store_true")
    p.add_argument("--labels", default=None)
    p.add_argument("--sub-labels", default=None)
    p.add_argument("--province", default=None)
    p.add_argument("--min-risk-score", type=float, default=None)

    p.add_argument("--min-call-records", type=int, default=None)
    p.add_argument("--min-counterparties", type=int, default=None)
    p.add_argument("--min-shared-device-count", type=int, default=None)
    p.add_argument("--min-shared-peer-total", type=int, default=None)
    p.add_argument("--min-night-ratio", type=float, default=None)
    p.add_argument("--min-night-count", type=int, default=None)
    p.add_argument("--min-match-count", type=int, default=1)

    p.add_argument("--night-start-hour", type=int, default=22)
    p.add_argument("--night-end-hour", type=int, default=6)
    p.add_argument("--night-ratio-threshold", type=float, default=0.30)
    p.add_argument("--night-count-threshold", type=int, default=10)

    p.add_argument("--sort-by", choices=["screening_score", "call_count", "unique_counterparties", "night_call_ratio", "shared_device_count", "shared_peer_total", "risk_score"], default="screening_score")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    dataset_root = detect_dataset_root(args.dataset_root)
    out_root = Path(args.output_root)
    ensure_dir(out_root)

    call_df, call_path = load_call_edges(dataset_root, args.dataset)
    user_df, user_path = load_user_nodes(dataset_root, args.dataset)
    user_meta_df = normalize_user_meta(user_df, call_df)
    device_df, device_path = load_device_edges(dataset_root, args.dataset, call_df)
    metrics_df = build_user_metrics(call_df, device_df, user_meta_df, args)

    scope_ids = load_scope_phone_ids(args)
    missing_scope_ids: List[str] = []
    if scope_ids:
        scope_set = set(scope_ids)
        available = set(metrics_df["user_id"].astype(str))
        missing_scope_ids = sorted(scope_set - available)
        scoped_df = metrics_df[metrics_df["user_id"].astype(str).isin(scope_set)].copy()
    else:
        scoped_df = metrics_df.copy()

    candidate_count = int(len(scoped_df))
    auto_thresholds = apply_mode_defaults(scoped_df if not scoped_df.empty else metrics_df, args)
    work_df, active_conditions, condition_desc = build_condition_columns(scoped_df, args, auto_thresholds)
    filter_chain = build_filter_chain(work_df, active_conditions, args.match_mode)
    work_df["match_mask"] = calc_match_mask(work_df, active_conditions, args.match_mode)
    screened_df = work_df[work_df["match_mask"]].copy()

    if active_conditions:
        screened_df["matched_condition_count"] = screened_df[[f"match_{x}" for x in active_conditions]].sum(axis=1)
    else:
        screened_df["matched_condition_count"] = 0
    screened_df = screened_df[screened_df["matched_condition_count"] >= max(int(args.min_match_count), 1)] if active_conditions else screened_df

    matched_names_col: List[List[str]] = []
    evidence_list: List[str] = []
    for _, row in screened_df.iterrows():
        names, evidence = build_condition_evidence(row, active_conditions)
        matched_names_col.append(names)
        evidence_list.append(evidence)
    if not screened_df.empty:
        screened_df["matched_condition_names"] = ["|".join(x) for x in matched_names_col]
        screened_df["evidence_summary"] = evidence_list
        screened_df["screening_score"] = compute_screening_score(screened_df, active_conditions)
        screened_df["recommended_followup"] = screened_df.apply(recommend_followup, axis=1)
        screened_df = screened_df.sort_values([args.sort_by, "matched_condition_count", "user_id"], ascending=[False, False, True])
    else:
        screened_df["matched_condition_names"] = []
        screened_df["evidence_summary"] = []
        screened_df["screening_score"] = []
        screened_df["recommended_followup"] = []

    device_artifact_df = build_shared_device_artifact(screened_df, device_df, user_meta_df, args.top_k)
    cp_artifact_df = build_common_counterparty_artifact(screened_df, call_df, args.top_k)

    screening_conclusion = (
        f"筛选后命中 {len(screened_df)} 个对象。" if len(screened_df) > 0 else "当前条件下未筛到命中对象，可放宽阈值或改用 any 匹配模式。"
    )
    filter_impact_explanation = explain_filter_impact(candidate_count, int(len(screened_df)), active_conditions, args.match_mode)

    next_steps: List[str] = []
    if not screened_df.empty:
        top1 = preview(screened_df.iloc[0]["user_id"])
        next_steps.append(f"优先对 Top1 `{top1}` 调用 single-number-analysis 做单号深挖。")
        if not device_artifact_df.empty:
            next_steps.append("命中对象存在共享设备证据，建议继续调用 shared-device-analysis。")
        if not cp_artifact_df.empty:
            next_steps.append("命中对象存在共同对端重叠，建议结合 overlap-analysis 或 association-path-analysis 做关系复核。")
        if len(screened_df) >= 2:
            next_steps.append("可对 Top1 和 Top2 做 association-path-analysis / overlap-analysis 复核路径与同圈关系。")
    else:
        next_steps.append("建议放宽阈值，或先从 topn-high-risk-discovery / group-risk-analysis 中挑选候选对象再筛。")

    slug = f"{args.group_name}_{args.dataset}"
    targets_csv = out_root / f"condition_screening_targets_{slug}_{len(screened_df)}targets.csv"
    devices_csv = out_root / f"condition_screening_devices_{slug}_{len(device_artifact_df)}rows.csv"
    counterparts_csv = out_root / f"condition_screening_counterparts_{slug}_{len(cp_artifact_df)}rows.csv"
    summary_json = out_root / f"condition_screening_summary_{slug}.json"
    report_md = out_root / f"condition_screening_report_{slug}.md"
    evidence_xlsx = out_root / f"condition_screening_evidence_{slug}.xlsx"

    export_cols = [
        "user_id", "province", "label", "sub_label", "risk_score", "call_count", "unique_counterparties",
        "night_call_count", "night_call_ratio", "device_count", "shared_device_count", "shared_peer_total",
        "matched_condition_names", "matched_condition_count", "evidence_summary", "screening_score", "recommended_followup",
    ]
    screened_export = screened_df[[c for c in export_cols if c in screened_df.columns]].copy()
    screened_export.to_csv(targets_csv, index=False)
    device_artifact_df.to_csv(devices_csv, index=False)
    cp_artifact_df.to_csv(counterparts_csv, index=False)

    with pd.ExcelWriter(evidence_xlsx, engine="openpyxl") as writer:
        pd.DataFrame(filter_chain).to_excel(writer, sheet_name="filter_chain", index=False)
        screened_export.to_excel(writer, sheet_name="targets", index=False)
        device_artifact_df.to_excel(writer, sheet_name="devices", index=False)
        cp_artifact_df.to_excel(writer, sheet_name="counterparts", index=False)
        pd.DataFrame(build_condition_hit_stats(work_df, active_conditions)).to_excel(writer, sheet_name="condition_hits", index=False)
        pd.DataFrame(build_distribution(screened_df, "province", 10)).to_excel(writer, sheet_name="province_dist", index=False)
        pd.DataFrame(build_distribution(screened_df, "sub_label", 10)).to_excel(writer, sheet_name="sublabel_dist", index=False)

    summary: Dict[str, object] = {
        "ok": True,
        "skill": "condition-based-screening",
        "query_type": "relationship_filter",
        "script_version": SCRIPT_VERSION,
        "group_name": args.group_name,
        "dataset": args.dataset,
        "mode": args.mode,
        "match_mode": args.match_mode,
        "top_k": int(args.top_k),
        "candidate_count": candidate_count,
        "screened_count": int(len(screened_df)),
        "missing_scope_count": len(missing_scope_ids),
        "missing_scope_preview": [preview(x) for x in missing_scope_ids[:10]],
        "active_conditions": active_conditions,
        "condition_descriptions": condition_desc,
        "auto_thresholds": auto_thresholds,
        "filter_chain": filter_chain,
        "screening_conclusion": screening_conclusion,
        "filter_impact_explanation": filter_impact_explanation,
        "shrink_ratio": round((candidate_count - int(len(screened_df))) / max(candidate_count, 1), 6),
        "shrink_ratio_pct": f"{((candidate_count - int(len(screened_df))) / max(candidate_count, 1)):.2%}",
        "condition_hit_stats": build_condition_hit_stats(work_df, active_conditions),
        "province_distribution": build_distribution(screened_df, "province", 10),
        "sub_label_distribution": build_distribution(screened_df, "sub_label", 10),
        "top_signal_summary": build_top_signal_summary(screened_df, device_artifact_df, cp_artifact_df),
        "evidence_completeness": {
            "targets_available": not screened_df.empty,
            "device_evidence_available": not device_artifact_df.empty,
            "counterparty_evidence_available": not cp_artifact_df.empty,
            "filter_chain_available": True,
        },
        "data_sources": {
            "dataset_root": str(dataset_root),
            "call_edges": str(call_path),
            "user_nodes": str(user_path) if user_path else None,
            "device_edges": str(device_path) if device_path else "derived_from_call_edges",
        },
        "next_step_suggestions": next_steps,
        "base_operator_alignment": {
            "entity_scope_filter": "node_lookup + aggregation_query",
            "night_behavior_filter": "time-window relationship_filter + aggregation_query",
            "broad_contact_filter": "neighbor_query + aggregation_query",
            "shared_device_filter": "query_shared_device + aggregation_query",
            "label_province_filter": "node_lookup + relationship_filter",
            "mixed_condition_ranking": "aggregation_query + scoring_layer",
        },
    }

    output_files = {
        "report_md": report_md,
        "targets_csv": targets_csv,
        "devices_csv": devices_csv,
        "counterparts_csv": counterparts_csv,
        "summary_json": summary_json,
        "evidence_xlsx": evidence_xlsx,
    }
    report_md.write_text(build_report(summary, condition_desc, screened_df, device_artifact_df, cp_artifact_df, output_files), encoding="utf-8")
    summary["artifacts"] = [
        {"type": "markdown_report", "path": str(report_md), "title": report_md.name},
        {"type": "csv", "path": str(targets_csv), "title": targets_csv.name},
        {"type": "csv", "path": str(devices_csv), "title": devices_csv.name},
        {"type": "csv", "path": str(counterparts_csv), "title": counterparts_csv.name},
        {"type": "xlsx", "path": str(evidence_xlsx), "title": evidence_xlsx.name},
        {"type": "json", "path": str(summary_json), "title": summary_json.name},
    ]
    summary["report_path"] = str(report_md)
    summary_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
