#!/usr/bin/env python3
import argparse
import json
import math
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd

DEFAULT_DEVICE_GRAPH_PATH = "/mnt/datasets/phone-network/processed/graph_views/unified/edges_phone_imei.parquet"
DEFAULT_USER_NODE_PATH = "/mnt/datasets/phone-network/processed/unified/user_nodes.csv"
DEFAULT_DEVICE_SOURCE_COL = "user_id"
DEFAULT_DEVICE_TARGET_COL = "imei"
DEFAULT_DATASET_ROOT_CANDIDATES = [
    Path("/workspace/imiss-deer-flow-main/datasets/phone-network"),
    Path("/mnt/datasets/phone-network"),
]
CSV_READ_KWARGS = {"low_memory": False}

RISKISH_SUBLABELS = {"risk", "purefraud", "fraud", "suspect", "black", "malicious"}
SAFEISH_SUBLABELS = {"normal", "whitelist", "white", "safe"}


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


def resolve_dataset_shared_paths(dataset_root: Optional[str], dataset: str) -> Tuple[Path, Path]:
    candidates: List[Path] = []
    if dataset_root:
        candidates.append(Path(dataset_root).expanduser())
    candidates.extend(DEFAULT_DATASET_ROOT_CANDIDATES)

    checked: List[str] = []
    for root in candidates:
        user_node_path = root / "processed" / dataset / "user_nodes.csv"
        parquet_device_path = root / "processed" / "graph_views" / dataset / "edges_phone_imei.parquet"
        csv_device_path = root / "processed" / "graph_views" / dataset / "edges_phone_imei.csv"
        device_graph_path = parquet_device_path if parquet_device_path.exists() else csv_device_path
        checked.append(f"{root}: user={user_node_path.exists()}, device={device_graph_path.exists()}")
        if user_node_path.exists() and device_graph_path.exists():
            return user_node_path, device_graph_path

    raise FileNotFoundError(
        f"cannot_resolve_dataset_shared_paths: dataset={dataset}, checked={checked}"
    )


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
    fallback = Path("/tmp/shared-device-analysis-outputs")
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback


def sanitize_filename(text: str) -> str:
    text = re.sub(r"[^0-9A-Za-z_\-\u4e00-\u9fff]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "report"


def short_id(value: str, head: int = 12) -> str:
    if not value:
        return "unknown"
    return value if len(value) <= head else value[:head] + "..."


def make_output_path(output_dir: Path, basename: str, suffix: str) -> Path:
    clean = sanitize_filename(basename)
    if not suffix.startswith("."):
        suffix = "." + suffix
    return output_dir / f"{clean}{suffix}"


def normalize_series(series: pd.Series) -> pd.Series:
    return series.astype("string").fillna("").str.strip()


def read_device_edges(path: str, source_col: str, target_col: str) -> pd.DataFrame:
    if path.lower().endswith(".parquet"):
        df = pd.read_parquet(path, columns=[source_col, target_col])
    else:
        df = pd.read_csv(path, usecols=[source_col, target_col], **CSV_READ_KWARGS)
    df[source_col] = normalize_series(df[source_col])
    df[target_col] = normalize_series(df[target_col])
    df = df[(df[source_col] != "") & (df[target_col] != "")].copy()
    df = df.drop_duplicates(subset=[source_col, target_col]).reset_index(drop=True)
    return df


def read_user_nodes(path: Optional[str]) -> pd.DataFrame:
    if not path:
        return pd.DataFrame()
    p = Path(path)
    if not p.exists():
        return pd.DataFrame()
    df = pd.read_csv(p, **CSV_READ_KWARGS)
    if "user_id" not in df.columns:
        return pd.DataFrame()
    df["user_id"] = normalize_series(df["user_id"])
    df = df[df["user_id"] != ""].copy()
    keep_cols = [
        c for c in ["user_id", "province", "label", "sub_label", "dataset_name", "source_table"]
        if c in df.columns
    ]
    return df[keep_cols].drop_duplicates(subset=["user_id"])


class SharedDeviceAnalyzer:
    def __init__(
        self,
        device_graph_path: str,
        user_node_path: Optional[str],
        device_source_col: str,
        device_target_col: str,
        top_k: int,
        min_shared_phone: int,
        min_device_phone_count: int,
        risk_only: bool,
    ) -> None:
        self.device_graph_path = resolve_data_path(device_graph_path)
        self.user_node_path = resolve_data_path(user_node_path) if user_node_path else None
        self.device_source_col = device_source_col
        self.device_target_col = device_target_col
        self.top_k = max(1, top_k)
        self.min_shared_phone = max(1, min_shared_phone)
        self.min_device_phone_count = max(1, min_device_phone_count)
        self.risk_only = risk_only

        self.device_df = read_device_edges(self.device_graph_path, self.device_source_col, self.device_target_col)
        self.user_df = read_user_nodes(self.user_node_path)
        self.user_meta: Dict[str, Dict[str, Any]] = {}
        if not self.user_df.empty:
            self.user_meta = self.user_df.set_index("user_id").to_dict(orient="index")

        self.phone_to_devices: Dict[str, Set[str]] = (
            self.device_df.groupby(self.device_source_col)[self.device_target_col]
            .apply(lambda x: set(x.astype(str)))
            .to_dict()
        )
        self.device_to_phones: Dict[str, Set[str]] = (
            self.device_df.groupby(self.device_target_col)[self.device_source_col]
            .apply(lambda x: set(x.astype(str)))
            .to_dict()
        )

    def phone_meta(self, phone_id: str) -> Dict[str, Any]:
        meta = dict(self.user_meta.get(phone_id, {}))
        meta["phone_id"] = phone_id
        meta["device_count"] = len(self.phone_to_devices.get(phone_id, set()))
        return meta

    def risk_tag(self, phone_id: str) -> str:
        meta = self.user_meta.get(phone_id, {})
        label = meta.get("label")
        sub_label = str(meta.get("sub_label") or "").strip().lower()
        if str(label) == "1" or label == 1:
            return sub_label or "risk"
        if sub_label in RISKISH_SUBLABELS:
            return sub_label
        if sub_label in SAFEISH_SUBLABELS:
            return sub_label
        return sub_label or "normal_or_unlabeled"

    def is_risk_phone(self, phone_id: str) -> bool:
        tag = self.risk_tag(phone_id)
        return tag in RISKISH_SUBLABELS

    def score_related_phone(self, phone_id: str, shared_devices: Set[str]) -> float:
        device_count = len(self.phone_to_devices.get(phone_id, set()))
        risk_bonus = 10.0 if self.is_risk_phone(phone_id) else 0.0
        shared_bonus = len(shared_devices) * 6.0
        breadth_bonus = math.log1p(device_count) * 3.0
        return round(risk_bonus + shared_bonus + breadth_bonus, 6)

    def score_device(self, stats: Dict[str, Any]) -> float:
        phone_count = stats["phone_count"]
        risk_phone_count = stats["risk_phone_count"]
        risk_ratio = stats["risk_ratio"]
        province_bonus = 2.0 if stats["province_count"] > 1 else 0.0
        label_mix_bonus = 1.5 if stats["label_diversity_count"] > 1 else 0.0
        return round(
            math.log1p(phone_count) * 5.0
            + risk_phone_count * 3.0
            + risk_ratio * 8.0
            + province_bonus
            + label_mix_bonus,
            6,
        )

    def device_stats(self, device_id: str) -> Dict[str, Any]:
        phones = sorted(self.device_to_phones.get(device_id, set()))
        provinces = []
        labels = []
        risk_phone_count = 0
        for phone in phones:
            meta = self.phone_meta(phone)
            province = str(meta.get("province", "unknown") or "unknown")
            tag = self.risk_tag(phone)
            provinces.append(province)
            labels.append(tag)
            if self.is_risk_phone(phone):
                risk_phone_count += 1
        province_count = len(set(provinces))
        label_diversity_count = len(set(labels))
        risk_ratio = round(risk_phone_count / len(phones), 6) if phones else 0.0
        stats = {
            "device_id": device_id,
            "device_preview": short_id(device_id),
            "phone_count": len(phones),
            "risk_phone_count": risk_phone_count,
            "risk_ratio": risk_ratio,
            "province_count": province_count,
            "provinces": sorted(set(provinces))[:10],
            "label_diversity_count": label_diversity_count,
            "label_distribution": self._distribution(labels),
            "phones_preview": [short_id(x) for x in phones[:10]],
        }
        stats["score"] = self.score_device(stats)
        stats["focus_signal"] = self.classify_device_focus(stats)
        return stats

    def classify_device_focus(self, stats: Dict[str, Any]) -> str:
        if stats["phone_count"] >= 10 and stats["risk_ratio"] >= 0.8:
            return "high_risk_device_pool"
        if stats["phone_count"] >= 5 and stats["province_count"] > 1:
            return "cross_region_device_pool"
        if stats["phone_count"] >= 5:
            return "shared_device_pool"
        return "normal_or_weak_signal"

    def _distribution(self, values: List[str]) -> Dict[str, int]:
        result: Dict[str, int] = {}
        for v in values:
            key = v or "unknown"
            result[key] = result.get(key, 0) + 1
        return dict(sorted(result.items(), key=lambda kv: (-kv[1], kv[0])))

    def rank_related_phones(self, related_pairs: List[Tuple[str, Set[str]]]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for phone_id, shared_devices in related_pairs:
            if self.risk_only and not self.is_risk_phone(phone_id):
                continue
            meta = self.phone_meta(phone_id)
            rows.append(
                {
                    "phone_id": phone_id,
                    "phone_preview": short_id(phone_id),
                    "shared_device_count": len(shared_devices),
                    "shared_devices_preview": [short_id(x) for x in sorted(shared_devices)[:10]],
                    "device_count": meta.get("device_count", 0),
                    "province": meta.get("province", "unknown"),
                    "risk_tag": self.risk_tag(phone_id),
                    "label": meta.get("label"),
                    "sub_label": meta.get("sub_label"),
                    "score": self.score_related_phone(phone_id, shared_devices),
                }
            )
        rows.sort(key=lambda x: (-x["score"], -x["shared_device_count"], -x["device_count"], x["phone_id"]))
        for idx, row in enumerate(rows, start=1):
            row["rank"] = idx
        return rows

    def rank_devices(self, devices: Set[str], allow_single_attached: bool = True) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for dev in devices:
            stats = self.device_stats(dev)
            if (not allow_single_attached) and stats["phone_count"] < self.min_device_phone_count:
                continue
            rows.append(stats)
        rows.sort(key=lambda x: (-x["score"], -x["phone_count"], -x["risk_phone_count"], x["device_id"]))
        for idx, row in enumerate(rows, start=1):
            row["rank"] = idx
        return rows

    def analyze_pair(self, phone_a: str, phone_b: str) -> Dict[str, Any]:
        devices_a = self.phone_to_devices.get(phone_a, set())
        devices_b = self.phone_to_devices.get(phone_b, set())
        shared_devices = devices_a & devices_b
        device_ranked = self.rank_devices(shared_devices, allow_single_attached=True)

        related_phone_map: Dict[str, Set[str]] = {}
        for dev in shared_devices:
            phones = self.device_to_phones.get(dev, set())
            for other in phones:
                if other in {phone_a, phone_b}:
                    continue
                related_phone_map.setdefault(other, set()).add(dev)
        related_pairs = [
            (p, ds) for p, ds in related_phone_map.items()
            if len(ds) >= self.min_shared_phone
        ]
        related_ranked = self.rank_related_phones(related_pairs)

        max_attached = max((row["phone_count"] for row in device_ranked), default=0)
        if len(shared_devices) == 0:
            strength = "none"
        elif len(shared_devices) >= 3 or max_attached >= 10:
            strength = "strong"
        else:
            strength = "medium"

        evidence_package = {
            "phone_a_device_count": len(devices_a),
            "phone_b_device_count": len(devices_b),
            "shared_device_count": len(shared_devices),
            "max_shared_device_attached_phone_count": max_attached,
            "shared_device_focus_signals": [row["focus_signal"] for row in device_ranked[: min(3, len(device_ranked))]],
            "related_phone_count": len(related_ranked),
            "risk_related_phone_count": sum(1 for x in related_ranked if self.is_risk_phone(x["phone_id"])),
        }

        return {
            "analysis_mode": "pair",
            "phone_a": self.phone_meta(phone_a),
            "phone_b": self.phone_meta(phone_b),
            "shared_device_count": len(shared_devices),
            "shared_devices": device_ranked[: self.top_k],
            "same_device_related_phone_count": len(related_ranked),
            "same_device_related_phones": related_ranked[: self.top_k],
            "relation_strength": strength,
            "evidence_package": evidence_package,
            "human_summary": (
                f"这两个号码共用 {len(shared_devices)} 台设备，关系强度为 {strength}。"
                if shared_devices else
                "这两个号码没有发现共用设备。"
            ),
            "investigation_next_steps": self.next_steps_pair(len(shared_devices), related_ranked, device_ranked),
        }

    def analyze_phone(self, phone_id: str) -> Dict[str, Any]:
        devices = self.phone_to_devices.get(phone_id, set())
        ranked_devices = self.rank_devices(devices, allow_single_attached=True)
        shared_devices = [x for x in ranked_devices if x["phone_count"] >= self.min_device_phone_count]

        related_map: Dict[str, Set[str]] = {}
        for dev in devices:
            if self.device_stats(dev)["phone_count"] < self.min_device_phone_count:
                continue
            phones = self.device_to_phones.get(dev, set())
            if len(phones) <= 1:
                continue
            for other in phones:
                if other == phone_id:
                    continue
                related_map.setdefault(other, set()).add(dev)
        related_pairs = [(p, ds) for p, ds in related_map.items() if len(ds) >= self.min_shared_phone]
        ranked_related = self.rank_related_phones(related_pairs)
        suspicious_cluster = self.infer_cluster(shared_devices, ranked_related)

        strongest_device = shared_devices[0] if shared_devices else None
        evidence_package = {
            "strongest_shared_device_id": strongest_device["device_id"] if strongest_device else None,
            "strongest_shared_device_peer_count": max((x["phone_count"] - 1 for x in shared_devices), default=0),
            "shared_device_pool_count": sum(1 for x in shared_devices if x["phone_count"] >= 5),
            "cross_province_shared_device_count": sum(1 for x in shared_devices if x["province_count"] > 1),
            "mixed_label_shared_device_count": sum(1 for x in shared_devices if x["label_diversity_count"] > 1),
            "risk_related_phone_count": sum(1 for x in ranked_related if self.is_risk_phone(x["phone_id"])),
            "unlabeled_related_phone_count": sum(1 for x in ranked_related if not self.is_risk_phone(x["phone_id"])),
        }

        return {
            "analysis_mode": "phone",
            "phone_profile": self.phone_meta(phone_id),
            "device_count": len(devices),
            "shared_device_count": len(shared_devices),
            "top_devices": shared_devices[: self.top_k] if shared_devices else ranked_devices[: self.top_k],
            "same_device_related_phone_count": len(ranked_related),
            "top_same_device_related_phones": ranked_related[: self.top_k],
            "suspicious_cluster_signal": suspicious_cluster,
            "evidence_package": evidence_package,
            "human_summary": (
                f"该号码关联 {len(devices)} 台设备，其中 {len(shared_devices)} 台存在共享迹象。"
                if devices else "该号码当前没有设备记录。"
            ),
            "investigation_next_steps": self.next_steps_phone(shared_devices, ranked_related),
        }

    def analyze_device(self, device_id: str) -> Dict[str, Any]:
        phones = sorted(self.device_to_phones.get(device_id, set()))
        phone_rows = []
        for phone_id in phones:
            if self.risk_only and not self.is_risk_phone(phone_id):
                continue
            meta = self.phone_meta(phone_id)
            phone_rows.append(
                {
                    "phone_id": phone_id,
                    "phone_preview": short_id(phone_id),
                    "province": meta.get("province", "unknown"),
                    "risk_tag": self.risk_tag(phone_id),
                    "label": meta.get("label"),
                    "sub_label": meta.get("sub_label"),
                    "device_count": meta.get("device_count", 0),
                    "score": self.score_related_phone(phone_id, {device_id}),
                }
            )
        phone_rows.sort(key=lambda x: (-x["score"], -x["device_count"], x["phone_id"]))
        for idx, row in enumerate(phone_rows, start=1):
            row["rank"] = idx
        risk_phone_count = sum(1 for row in phone_rows if self.is_risk_phone(row["phone_id"]))
        provinces = sorted({row["province"] for row in phone_rows})
        risk_ratio = round(risk_phone_count / len(phone_rows), 6) if phone_rows else 0.0
        if len(phone_rows) >= 10 or risk_ratio >= 0.8:
            cluster_level = "strong"
        elif len(phone_rows) >= 3:
            cluster_level = "medium"
        else:
            cluster_level = "weak"
        evidence_package = {
            "risk_phone_ratio": risk_ratio,
            "province_count": len(provinces),
            "provinces": provinces[:10],
            "label_distribution": self._distribution([row["risk_tag"] for row in phone_rows]),
            "focus_signal": (
                "high_risk_device_pool" if risk_ratio >= 0.8 and len(phone_rows) >= 5
                else "cross_region_device_pool" if len(provinces) > 1 and len(phone_rows) >= 5
                else "shared_device_pool" if len(phone_rows) >= 5
                else "weak_device_signal"
            ),
        }
        return {
            "analysis_mode": "device",
            "device_id": device_id,
            "phone_count": len(phone_rows),
            "risk_phone_count": risk_phone_count,
            "cluster_level": cluster_level,
            "linked_phones": phone_rows[: self.top_k],
            "evidence_package": evidence_package,
            "human_summary": f"该设备关联 {len(phone_rows)} 个号码，其中 {risk_phone_count} 个已带风险标签。",
            "investigation_next_steps": self.next_steps_device(len(phone_rows), risk_phone_count, evidence_package),
        }

    def infer_cluster(self, shared_devices: List[Dict[str, Any]], ranked_related: List[Dict[str, Any]]) -> Dict[str, Any]:
        device_pool = sum(1 for x in shared_devices if x["phone_count"] >= 5)
        risk_related = sum(1 for x in ranked_related if self.is_risk_phone(x["phone_id"]))
        cross_region = sum(1 for x in shared_devices if x["province_count"] > 1)
        label_mixed = sum(1 for x in shared_devices if x["label_diversity_count"] > 1)
        level = "low"
        if device_pool >= 2 or risk_related >= 3 or cross_region >= 1:
            level = "high"
        elif device_pool >= 1 or risk_related >= 1 or len(ranked_related) >= 5:
            level = "medium"
        return {
            "level": level,
            "device_pool_count": device_pool,
            "risk_related_phone_count": risk_related,
            "cross_region_shared_device_count": cross_region,
            "mixed_label_shared_device_count": label_mixed,
            "explanation": (
                "存在明显的共享设备扩散现象，疑似设备池或团伙共用。" if level == "high"
                else "存在一定共享设备扩散迹象，建议继续核查。" if level == "medium"
                else "当前共享设备线索较弱。"
            ),
        }

    def next_steps_pair(
        self,
        shared_device_count: int,
        ranked_related: List[Dict[str, Any]],
        device_ranked: List[Dict[str, Any]],
    ) -> List[str]:
        steps: List[str] = []
        if shared_device_count > 0:
            steps.append("优先核查共用设备，确认是否为真实复用终端、养号设备池或团伙共用设备。")
            if device_ranked:
                steps.append(f"优先检查最强共用设备 {device_ranked[0]['device_preview']}，其挂载号码数为 {device_ranked[0]['phone_count']}。")
        if ranked_related:
            steps.append(f"继续核查同设备关联号码 Top 节点，例如 {ranked_related[0]['phone_preview']}。")
        else:
            steps.append("当前未发现额外的同设备关联号码，可改做路径分析或重叠分析。")
        steps.append("如需确认两号码是否位于同一联系圈，继续调用 overlap-analysis。")
        steps.append("如需确认两号码与关联号码的连接路径，继续调用 association-path-analysis。")
        return steps

    def next_steps_phone(self, shared_devices: List[Dict[str, Any]], ranked_related: List[Dict[str, Any]]) -> List[str]:
        steps: List[str] = []
        if shared_devices:
            steps.append("优先查询共享程度最高的设备，确认这些设备下是否聚集了大量高风险或异常号码。")
            steps.append("建议先核查共享设备池，再联动单号码分析确认关键关联号码的画像与历史行为。")
        if ranked_related:
            steps.append(f"优先对同设备关联号码 Top 节点 {ranked_related[0]['phone_preview']} 做单号码复查。")
        steps.append("围绕关键共享设备节点继续抽取 1-2 跳局部子图，识别更大的设备扩散关系圈。")
        steps.append("结合 association-path-analysis 和 overlap-analysis 对高优先级关联号码继续下钻。")
        return steps

    def next_steps_device(self, phone_count: int, risk_phone_count: int, evidence_package: Dict[str, Any]) -> List[str]:
        steps: List[str] = []
        if phone_count >= 5:
            steps.append("该设备下号码较多，建议视为重点设备池线索继续核查。")
        if risk_phone_count > 0:
            steps.append("优先复查已带风险标签的关联号码，确认该设备是否贯穿多个风险对象。")
        if evidence_package.get("province_count", 1) > 1:
            steps.append("该设备关联号码跨省分布，建议检查是否存在跨地域复用或批量养号。")
        steps.append("可对该设备下任意两个高优先号码调用 association-path-analysis，确认是否还存在通话或共同对端证据。")
        return steps

    def csv_dataframe(self, mode: str, result: Dict[str, Any]) -> pd.DataFrame:
        if mode == "pair":
            shared_devices = pd.DataFrame(result.get("shared_devices", []))
            if not shared_devices.empty:
                shared_devices.insert(0, "section", "shared_devices")
            else:
                shared_devices = pd.DataFrame(columns=[
                    "section", "device_id", "device_preview", "phone_count", "risk_phone_count",
                    "risk_ratio", "province_count", "label_diversity_count", "focus_signal", "score", "rank",
                ])
            related = pd.DataFrame(result.get("same_device_related_phones", []))
            if not related.empty:
                related.insert(0, "section", "same_device_related_phones")
            else:
                related = pd.DataFrame(columns=[
                    "section", "phone_id", "phone_preview", "shared_device_count", "device_count",
                    "province", "risk_tag", "score", "rank",
                ])
            return pd.concat([shared_devices, related], ignore_index=True, sort=False)
        if mode == "phone":
            devices = pd.DataFrame(result.get("top_devices", []))
            if not devices.empty:
                devices.insert(0, "section", "top_devices")
            else:
                devices = pd.DataFrame(columns=[
                    "section", "device_id", "device_preview", "phone_count", "risk_phone_count", "risk_ratio",
                    "province_count", "label_diversity_count", "focus_signal", "score", "rank",
                ])
            related = pd.DataFrame(result.get("top_same_device_related_phones", []))
            if not related.empty:
                related.insert(0, "section", "top_same_device_related_phones")
            else:
                related = pd.DataFrame(columns=[
                    "section", "phone_id", "phone_preview", "shared_device_count", "device_count",
                    "province", "risk_tag", "score", "rank",
                ])
            return pd.concat([devices, related], ignore_index=True, sort=False)
        linked = pd.DataFrame(result.get("linked_phones", []))
        if not linked.empty:
            linked.insert(0, "section", "linked_phones")
        else:
            linked = pd.DataFrame(columns=[
                "section", "phone_id", "phone_preview", "province", "risk_tag", "device_count", "score", "rank"
            ])
        return linked

    def write_csv(self, mode: str, result: Dict[str, Any], output_dir: Path) -> Path:
        df = self.csv_dataframe(mode, result)
        if mode == "pair":
            name = f"shared_device_pair_{short_id(result['phone_a']['phone_id'], 8).replace('...', '')}_{short_id(result['phone_b']['phone_id'], 8).replace('...', '')}"
        elif mode == "phone":
            name = f"shared_device_phone_{short_id(result['phone_profile']['phone_id'], 8).replace('...', '')}"
        else:
            name = f"shared_device_device_{short_id(result['device_id'], 8).replace('...', '')}"
        path = make_output_path(output_dir, name, '.csv')
        df.to_csv(path, index=False, encoding="utf-8-sig")
        return path

    def write_markdown(self, mode: str, result: Dict[str, Any], output_dir: Path) -> Path:
        if mode == "pair":
            report_name = f"shared_device_pair_report_{short_id(result['phone_a']['phone_id'], 8).replace('...', '')}_{short_id(result['phone_b']['phone_id'], 8).replace('...', '')}"
            lines: List[str] = [
                "# 共用设备关系分析报告",
                "",
                f"- 号码A：`{result['phone_a']['phone_id']}`",
                f"- 号码B：`{result['phone_b']['phone_id']}`",
                "",
                "## 1. 核心结论",
                f"- 共用设备数：{result['shared_device_count']}",
                f"- 关系强度：{result['relation_strength']}",
                f"- 简要说明：{result['human_summary']}",
                "",
                "## 2. 证据包摘要",
                f"- 号码A设备数：{result['evidence_package']['phone_a_device_count']}",
                f"- 号码B设备数：{result['evidence_package']['phone_b_device_count']}",
                f"- 最大共用设备挂载号码数：{result['evidence_package']['max_shared_device_attached_phone_count']}",
                f"- 同设备牵出号码数：{result['evidence_package']['related_phone_count']}",
                f"- 风险同设备号码数：{result['evidence_package']['risk_related_phone_count']}",
                "",
                "## 3. 共用设备明细",
            ]
            if result['shared_devices']:
                for row in result['shared_devices']:
                    lines.append(
                        f"- 设备 `{row['device_preview']}` | 挂载号码数={row['phone_count']} | 风险号码数={row['risk_phone_count']} | 风险占比={row['risk_ratio']} | 重点信号={row['focus_signal']} | 评分={row['score']}"
                    )
            else:
                lines.append("- 未发现共用设备。")
            lines += ["", "## 4. 同设备关联号码"]
            if result['same_device_related_phones']:
                for row in result['same_device_related_phones']:
                    lines.append(
                        f"- `{row['phone_preview']}` | 共用设备数={row['shared_device_count']} | 设备总数={row['device_count']} | 风险标签={row['risk_tag']} | 省份={row['province']} | 评分={row['score']}"
                    )
            else:
                lines.append("- 暂无额外同设备关联号码。")
            lines += ["", "## 5. 下一步建议"]
            for idx, step in enumerate(result['investigation_next_steps'], start=1):
                lines.append(f"{idx}. {step}")
        elif mode == "phone":
            report_name = f"shared_device_phone_report_{short_id(result['phone_profile']['phone_id'], 8).replace('...', '')}"
            lines = [
                "# 单号码共享设备分析报告",
                "",
                f"- 号码：`{result['phone_profile']['phone_id']}`",
                f"- 省份：{result['phone_profile'].get('province', 'unknown')}",
                f"- 标签：{self.risk_tag(result['phone_profile']['phone_id'])}",
                "",
                "## 1. 核心结论",
                f"- 设备总数：{result['device_count']}",
                f"- 存在共享迹象的设备数：{result['shared_device_count']}",
                f"- 共享设备团伙信号：{result['suspicious_cluster_signal']['level']}",
                f"- 说明：{result['human_summary']}",
                "",
                "## 2. 证据包摘要",
                f"- 最强共享设备额外牵出号码数：{result['evidence_package']['strongest_shared_device_peer_count']}",
                f"- 共享设备池数量（挂载>=5）：{result['evidence_package']['shared_device_pool_count']}",
                f"- 跨省共享设备数量：{result['evidence_package']['cross_province_shared_device_count']}",
                f"- 混合标签共享设备数量：{result['evidence_package']['mixed_label_shared_device_count']}",
                f"- 风险同设备号码数：{result['evidence_package']['risk_related_phone_count']}",
                f"- 非风险/未标注同设备号码数：{result['evidence_package']['unlabeled_related_phone_count']}",
                f"- 团伙信号说明：{result['suspicious_cluster_signal']['explanation']}",
                "",
                "## 3. Top 共享设备",
            ]
            if result['top_devices']:
                for row in result['top_devices']:
                    lines.append(
                        f"- 设备 `{row['device_preview']}` | 挂载号码数={row['phone_count']} | 风险号码数={row['risk_phone_count']} | 风险占比={row['risk_ratio']} | 省份数={row['province_count']} | 标签多样性={row['label_diversity_count']} | 重点信号={row['focus_signal']} | 评分={row['score']}"
                    )
            else:
                lines.append("- 暂无设备记录。")
            lines += ["", "## 4. Top 同设备关联号码"]
            if result['top_same_device_related_phones']:
                for row in result['top_same_device_related_phones']:
                    lines.append(
                        f"- `{row['phone_preview']}` | 共用设备数={row['shared_device_count']} | 风险标签={row['risk_tag']} | 省份={row['province']} | 设备总数={row['device_count']} | 评分={row['score']}"
                    )
            else:
                lines.append("- 暂无同设备关联号码。")
            lines += ["", "## 5. 下一步建议"]
            for idx, step in enumerate(result['investigation_next_steps'], start=1):
                lines.append(f"{idx}. {step}")
        else:
            report_name = f"shared_device_device_report_{short_id(result['device_id'], 8).replace('...', '')}"
            lines = [
                "# 设备关联号码分析报告",
                "",
                f"- 设备ID：`{result['device_id']}`",
                "",
                "## 1. 核心结论",
                f"- 关联号码数：{result['phone_count']}",
                f"- 风险号码数：{result['risk_phone_count']}",
                f"- 聚集强度：{result['cluster_level']}",
                f"- 简要说明：{result['human_summary']}",
                "",
                "## 2. 证据包摘要",
                f"- 风险号码占比：{result['evidence_package']['risk_phone_ratio']}",
                f"- 省份数：{result['evidence_package']['province_count']}",
                f"- 重点信号：{result['evidence_package']['focus_signal']}",
                f"- 标签分布：{json.dumps(result['evidence_package']['label_distribution'], ensure_ascii=False)}",
                "",
                "## 3. 关联号码",
            ]
            if result['linked_phones']:
                for row in result['linked_phones']:
                    lines.append(
                        f"- `{row['phone_preview']}` | 风险标签={row['risk_tag']} | 省份={row['province']} | 设备总数={row['device_count']} | 评分={row['score']}"
                    )
            else:
                lines.append("- 暂无关联号码。")
            lines += ["", "## 4. 下一步建议"]
            for idx, step in enumerate(result['investigation_next_steps'], start=1):
                lines.append(f"{idx}. {step}")
        path = make_output_path(output_dir, report_name, '.md')
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="shared-device-analysis wrapper")
    parser.add_argument("--mode", choices=["auto", "pair", "phone", "device"], default="auto")
    parser.add_argument("--phone-a", default="")
    parser.add_argument("--phone-b", default="")
    parser.add_argument("--phone-id", default="")
    parser.add_argument("--device-id", default="")
    parser.add_argument("--device-graph-path", default=None)
    parser.add_argument("--user-node-path", default=None)
    parser.add_argument("--dataset-root", default=None)
    parser.add_argument("--dataset", default="unified")
    parser.add_argument("--artifact-mode", choices=["full", "essential", "markdown_only"], default="full")
    parser.add_argument("--device-source-col", default=DEFAULT_DEVICE_SOURCE_COL)
    parser.add_argument("--device-target-col", default=DEFAULT_DEVICE_TARGET_COL)
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--min-shared-phone", type=int, default=1, help="相关号码至少共用多少台设备才保留。")
    parser.add_argument("--min-device-phone-count", type=int, default=2, help="设备至少挂载多少号码才视为共享设备。")
    parser.add_argument("--risk-only", action="store_true", help="只保留风险标签号码。")
    return parser.parse_args()


def infer_mode(args: argparse.Namespace) -> str:
    if args.mode != "auto":
        return args.mode
    if args.phone_a and args.phone_b:
        return "pair"
    if args.phone_id:
        return "phone"
    if args.device_id:
        return "device"
    raise ValueError("必须至少提供 phone-a+phone-b、phone-id 或 device-id。")


def build_payload(mode: str, args: argparse.Namespace, analyzer: SharedDeviceAnalyzer, result: Dict[str, Any]) -> Dict[str, Any]:
    output_dir = ensure_output_dir()
    report_path = analyzer.write_markdown(mode, result, output_dir)
    csv_path = analyzer.write_csv(mode, result, output_dir)

    input_summary = {
        "mode": mode,
        "device_graph_path": analyzer.device_graph_path,
        "user_node_path": analyzer.user_node_path,
        "device_source_col": analyzer.device_source_col,
        "device_target_col": analyzer.device_target_col,
        "top_k": analyzer.top_k,
        "min_shared_phone": analyzer.min_shared_phone,
        "min_device_phone_count": analyzer.min_device_phone_count,
        "risk_only": analyzer.risk_only,
        "dataset_root": args.dataset_root,
        "dataset": args.dataset,
        "artifact_mode": args.artifact_mode,
    }
    if mode == "pair":
        input_summary.update({"phone_a": args.phone_a, "phone_b": args.phone_b})
    elif mode == "phone":
        input_summary.update({"phone_id": args.phone_id})
    else:
        input_summary.update({"device_id": args.device_id})

    payload = {
        "ok": True,
        "skill": "shared-device-analysis",
        "query_type": "shared_device_analysis",
        "input_summary": input_summary,
        "result": result,
        "notes": [
            "该技能是电话网络数据分析层技能，底层主要组合了 query_shared_device、common_device、query_phone_node 这类基础算子思路。",
            "当前重点支持三种视角：两个号码共用设备分析、单号码共享设备扩散分析、单设备关联号码分析。",
            "本版本补充了共享设备证据包、风险过滤和共享设备阈值控制，便于后续做设备池、团伙或风险群组分析。",
        ],
        "base_operator_alignment": [
            "query_shared_device",
            "common_device",
            "query_phone_node",
            "relationship_filter",
            "subgraph_by_nodes",
        ],
        "yigraph_meta": {
            "related_query_types": [
                "relationship_filter",
                "common_neighbor",
                "subgraph_by_nodes",
            ],
            "explanation": "shared-device-analysis 对应 YiGraph 的关系过滤、共同邻居和围绕关键证据节点继续下钻的分析风格。",
        },
        "report_path": str(report_path),
        "csv_path": str(csv_path),
        "artifacts": (
            [
                {"type": "markdown_report", "path": str(report_path), "title": report_path.name},
            ]
            if args.artifact_mode == "markdown_only"
            else [
                {"type": "markdown_report", "path": str(report_path), "title": report_path.name},
                {"type": "csv_list", "path": str(csv_path), "title": csv_path.name},
            ]
        ),
    }
    return payload


def main() -> None:
    args = parse_args()
    mode = infer_mode(args)
    auto_user_node_path: Optional[Path] = None
    auto_device_graph_path: Optional[Path] = None
    if not (args.user_node_path and args.device_graph_path):
        auto_user_node_path, auto_device_graph_path = resolve_dataset_shared_paths(
            args.dataset_root,
            args.dataset,
        )

    user_node_path = str(Path(args.user_node_path)) if args.user_node_path else str(auto_user_node_path)
    device_graph_path = str(Path(args.device_graph_path)) if args.device_graph_path else str(auto_device_graph_path)

    analyzer = SharedDeviceAnalyzer(
        device_graph_path=device_graph_path,
        user_node_path=user_node_path,
        device_source_col=args.device_source_col,
        device_target_col=args.device_target_col,
        top_k=args.top_k,
        min_shared_phone=args.min_shared_phone,
        min_device_phone_count=args.min_device_phone_count,
        risk_only=args.risk_only,
    )

    if mode == "pair":
        result = analyzer.analyze_pair(args.phone_a, args.phone_b)
    elif mode == "phone":
        result = analyzer.analyze_phone(args.phone_id)
    else:
        result = analyzer.analyze_device(args.device_id)

    print(json.dumps(build_payload(mode, args, analyzer, result), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
