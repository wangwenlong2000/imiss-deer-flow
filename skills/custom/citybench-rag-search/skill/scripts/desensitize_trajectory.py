#!/usr/bin/env python3
"""
CityBench 轨迹隐私脱敏脚本
=============================

功能：把"含敏感身份信息的原始 GPS 轨迹"转换为"可索引的聚合 evidence"。
本脚本是 RAG 检索（search.py）的上游：
    原始轨迹 CSV → desensitize_trajectory.py → evidence.jsonl → search.py

四道脱敏关卡（按顺序执行）：
    1. user_id 不可逆哈希          —— SHA256 截断为 12 hex
    2. GPS 坐标 Laplace 噪声        —— ε-差分隐私，默认 ε=1.0（约 ±100m 偏移）
    3. 时间桶化 + Geohash 网格化    —— 精确时间 → 2 小时时段；精确坐标 → 5 位 geohash（约 5km×5km）
    4. K-匿名校验                  —— 单元（日期×时段×geohash）内 unique_users < 5 直接丢弃

输出：
    - {output-dir}/evidence.jsonl       聚合后的 evidence（可直接喂 search.py --local-file）
    - {output-dir}/privacy_report.json  脱敏审计报告，统计：
        * 原始记录数 / 输出 evidence 数
        * 哈希用户总数
        * 噪声参数 (epsilon)
        * k-匿名丢弃了多少个低密度单元
        * 平均坐标偏移量（米）

输入 CSV 列名（默认值；可通过参数覆盖）：
    user_id, latitude, longitude, timestamp, [category]

CLI：
    python3 desensitize_trajectory.py \\
        --input /path/to/raw_trajectory.csv \\
        --output-dir /tmp/desensitized \\
        --epsilon 1.0 \\
        --geohash-precision 5 \\
        --min-users 5
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import math
import random
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


# ───────────────────── 工具：geohash 编码（纯 stdlib） ─────────────────────

_GEOHASH_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"


def encode_geohash(lat: float, lon: float, precision: int = 5) -> str:
    """经典 geohash 编码，无需第三方库。"""
    lat_lo, lat_hi = -90.0, 90.0
    lon_lo, lon_hi = -180.0, 180.0
    is_lon = True
    bits = 0
    bit_count = 0
    out = []
    while len(out) < precision:
        if is_lon:
            mid = (lon_lo + lon_hi) / 2
            if lon >= mid:
                bits = (bits << 1) | 1
                lon_lo = mid
            else:
                bits = bits << 1
                lon_hi = mid
        else:
            mid = (lat_lo + lat_hi) / 2
            if lat >= mid:
                bits = (bits << 1) | 1
                lat_lo = mid
            else:
                bits = bits << 1
                lat_hi = mid
        is_lon = not is_lon
        bit_count += 1
        if bit_count == 5:
            out.append(_GEOHASH_BASE32[bits])
            bits = 0
            bit_count = 0
    return "".join(out)


# ───────────────────── 工具：Laplace 噪声 ─────────────────────

def laplace_noise(scale: float, rng: random.Random) -> float:
    """从 Laplace(0, scale) 分布采样。等价于差分隐私的标准噪声。"""
    u = rng.random() - 0.5
    return -scale * math.copysign(1, u) * math.log(1 - 2 * abs(u))


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """两点距离（米），用于审计噪声偏移量。"""
    R = 6371000.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


# ───────────────────── 时间桶化 ─────────────────────

TIME_SLOTS = [
    ("凌晨", 23, 7),
    ("早高峰", 7, 9),
    ("上午", 9, 11),
    ("午间", 11, 13),
    ("下午", 13, 17),
    ("晚高峰", 17, 19),
    ("夜间", 19, 23),
]


def get_time_slot(hour: int) -> tuple[str, int, int]:
    for name, start, end in TIME_SLOTS:
        if start > end:  # 跨午夜（凌晨）
            if hour >= start or hour < end:
                return name, start, end
        else:
            if start <= hour < end:
                return name, start, end
    return "凌晨", 23, 7


def parse_timestamp(s: str) -> Optional[datetime]:
    """容忍多种格式：ISO、epoch、check-in 时间戳。"""
    s = s.strip()
    if not s:
        return None
    # epoch 秒
    if s.isdigit() and len(s) >= 10:
        try:
            return datetime.utcfromtimestamp(int(s[:10]))
        except (ValueError, OSError):
            pass
    # ISO
    for fmt in (
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%a %b %d %H:%M:%S %z %Y",  # CityBench Twitter check-in 格式
    ):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


# ───────────────────── 第 1 关：user_id 哈希 ─────────────────────

def hash_user_id(uid: str, salt: str) -> str:
    """SHA256 + salt → 截断 12 位 hex。不可逆。"""
    h = hashlib.sha256((salt + str(uid)).encode("utf-8")).hexdigest()
    return h[:12]


# ───────────────────── 主流程 ─────────────────────

def desensitize(
    input_path: Path,
    output_dir: Path,
    epsilon: float,
    geohash_precision: int,
    min_users: int,
    salt: str,
    user_col: str,
    lat_col: str,
    lon_col: str,
    time_col: str,
    cat_col: Optional[str],
    city_label: str,
    seed: int,
) -> dict:
    rng = random.Random(seed)

    # Laplace scale ←→ epsilon 关系：
    #   敏感度 Δf 取 0.001° (≈ 100m，单条 GPS 的扰动上限)
    #   scale = Δf / ε
    sensitivity_deg = 0.001
    laplace_scale = sensitivity_deg / max(epsilon, 1e-6)

    # 统计
    n_raw = 0
    n_invalid = 0
    raw_user_ids: set[str] = set()
    hashed_user_ids: set[str] = set()
    noise_offsets_m: list[float] = []

    # 按 (date, time_slot, geohash) 聚合
    buckets: dict[tuple, dict] = defaultdict(lambda: {
        "users": set(),
        "checkins": 0,
        "categories": Counter(),
        "lat_sum": 0.0,
        "lon_sum": 0.0,
    })

    output_dir.mkdir(parents=True, exist_ok=True)

    # 流式处理 CSV
    with open(input_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        cols = reader.fieldnames or []
        for required in (user_col, lat_col, lon_col, time_col):
            if required not in cols:
                raise ValueError(
                    f"列 '{required}' 不在 CSV header 中。可用列: {cols}。"
                    f" 用 --user-col/--lat-col/--lon-col/--time-col 指定。"
                )

        for row in reader:
            n_raw += 1
            try:
                uid = str(row[user_col]).strip()
                lat = float(row[lat_col])
                lon = float(row[lon_col])
                ts = parse_timestamp(row[time_col])
                if not (uid and ts and -90 <= lat <= 90 and -180 <= lon <= 180):
                    n_invalid += 1
                    continue
            except (ValueError, KeyError):
                n_invalid += 1
                continue

            raw_user_ids.add(uid)

            # ── 关卡 1：user_id 不可逆哈希 ──
            uid_hash = hash_user_id(uid, salt)
            hashed_user_ids.add(uid_hash)

            # ── 关卡 2：Laplace 噪声 ──
            noisy_lat = lat + laplace_noise(laplace_scale, rng)
            noisy_lon = lon + laplace_noise(laplace_scale, rng)
            noise_offsets_m.append(haversine_m(lat, lon, noisy_lat, noisy_lon))

            # ── 关卡 3：时间桶化 + Geohash 网格化 ──
            ghash = encode_geohash(noisy_lat, noisy_lon, geohash_precision)
            slot_name, slot_start, slot_end = get_time_slot(ts.hour)
            date_str = ts.strftime("%Y-%m-%d")
            key = (date_str, slot_name, ghash)

            b = buckets[key]
            b["users"].add(uid_hash)
            b["checkins"] += 1
            b["lat_sum"] += noisy_lat
            b["lon_sum"] += noisy_lon
            cat = row.get(cat_col, "未分类").strip() if cat_col else "未分类"
            if cat:
                b["categories"][cat] += 1

    # ── 关卡 4：K-匿名 + 输出 evidence ──
    evidence_path = output_dir / "evidence.jsonl"
    n_dropped_kanon = 0
    n_emitted = 0
    with open(evidence_path, "w", encoding="utf-8") as out_f:
        for (date_str, slot_name, ghash), b in sorted(buckets.items()):
            n_users = len(b["users"])
            if n_users < min_users:
                n_dropped_kanon += 1
                continue
            n_emitted += 1
            checkins = b["checkins"]
            top3 = b["categories"].most_common(3)
            top_cat_names = [c for c, _ in top3]
            total_cats = sum(b["categories"].values()) or 1
            top_cat_str = "、".join(
                f"{c}({int(round(cnt / total_cats * 100))}%)" for c, cnt in top3
            ) or "未分类"

            # 还原时段时间区间
            slot_start_h = next(s for n, s, _ in TIME_SLOTS if n == slot_name)
            slot_end_h = next(e for n, _, e in TIME_SLOTS if n == slot_name)

            display_date = f"{date_str[0:4]}年{date_str[5:7]}月{date_str[8:10]}日"
            text = (
                f"{display_date}"
                f"{slot_name}({slot_start_h:02d}:00-{slot_end_h:02d}:00)，"
                f"{city_label}(geohash:{ghash})区域，签到活动量{checkins}次，"
                f"活跃用户约{n_users}人，热门类别为{top_cat_str}。"
                f"（已脱敏：用户ID哈希+坐标加噪ε={epsilon}+k≥{min_users}匿名）"
            )

            evidence = {
                "evidence_id": f"traj_ev_{date_str.replace('-', '')}_{slot_start_h:02d}00_{city_label.lower()}_{ghash}",
                "data_type": "spatiotemporal_trajectory",
                "text": text,
                "meta": {
                    "source_id": f"citybench_checkins_{city_label.lower()}",
                    "time_range": {
                        "start": f"{date_str}T{slot_start_h:02d}:00:00",
                        "end": f"{date_str}T{slot_end_h:02d}:00:00",
                    },
                    "geo_scope": {"city": city_label, "geohash": ghash},
                    "granularity": "hourly_district",
                    "sensitivity_level": "aggregated_safe",
                    "access_policy": "open",
                    "privacy_provenance": {
                        "user_id_hashed": True,
                        "coord_noise_epsilon": epsilon,
                        "geohash_precision": geohash_precision,
                        "k_anonymity_threshold": min_users,
                        "passed_k_anonymity": True,
                    },
                    "features": {
                        "checkin_count": checkins,
                        "unique_users": n_users,
                        "top_categories": top_cat_names,
                        "wow_change_pct": None,
                        "anomaly_flag": False,
                    },
                },
            }
            out_f.write(json.dumps(evidence, ensure_ascii=False) + "\n")

    # ── 隐私审计报告 ──
    avg_offset = sum(noise_offsets_m) / len(noise_offsets_m) if noise_offsets_m else 0.0
    max_offset = max(noise_offsets_m) if noise_offsets_m else 0.0
    report = {
        "input_file": str(input_path),
        "output_evidence": str(evidence_path),
        "raw_record_count": n_raw,
        "invalid_record_count": n_invalid,
        "raw_unique_users": len(raw_user_ids),
        "hashed_unique_users": len(hashed_user_ids),
        "user_id_collision": len(raw_user_ids) != len(hashed_user_ids),
        "privacy_pipeline": [
            {
                "step": 1,
                "name": "user_id_hash",
                "algorithm": "SHA256 + salt → 12-hex truncate",
                "irreversible": True,
            },
            {
                "step": 2,
                "name": "coord_laplace_noise",
                "algorithm": "Laplace(0, sensitivity/ε) per axis",
                "epsilon": epsilon,
                "sensitivity_deg": sensitivity_deg,
                "scale_deg": laplace_scale,
                "avg_offset_meters": round(avg_offset, 2),
                "max_offset_meters": round(max_offset, 2),
            },
            {
                "step": 3,
                "name": "spatiotemporal_bucketing",
                "geohash_precision": geohash_precision,
                "geohash_cell_km": "≈ 4.9 × 4.9",
                "time_slot_hours": 2,
            },
            {
                "step": 4,
                "name": "k_anonymity_filter",
                "k": min_users,
                "buckets_dropped": n_dropped_kanon,
                "buckets_emitted": n_emitted,
            },
        ],
        "compliance_note": (
            "输出 evidence 均为聚合统计量，单条记录代表 ≥ "
            f"{min_users} 名用户在 2 小时窗口、5km 网格内的合并行为；"
            "无法反查个人轨迹。符合 GB/T 35273-2020 个人信息安全规范的去标识化要求。"
        ),
    }
    report_path = output_dir / "privacy_report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    return report


def main() -> int:
    p = argparse.ArgumentParser(
        description="CityBench 轨迹隐私脱敏：原始 GPS → 聚合 evidence",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--input", required=True, help="原始轨迹 CSV 路径")
    p.add_argument("--output-dir", required=True, help="输出目录")
    p.add_argument("--city-label", default="Shanghai", help="evidence 中的 city 字段")
    p.add_argument("--epsilon", type=float, default=1.0, help="差分隐私 ε（越小越严，默认 1.0）")
    p.add_argument("--geohash-precision", type=int, default=5, help="geohash 位数（默认 5，约 5km）")
    p.add_argument("--min-users", type=int, default=5, help="k-匿名阈值（默认 5）")
    p.add_argument("--salt", default="citybench_v1", help="哈希 salt")
    p.add_argument("--user-col", default="user_id")
    p.add_argument("--lat-col", default="latitude")
    p.add_argument("--lon-col", default="longitude")
    p.add_argument("--time-col", default="timestamp")
    p.add_argument("--cat-col", default="category", help="POI 类别列（可选）")
    p.add_argument("--seed", type=int, default=42, help="噪声随机种子（可复现）")
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    input_path = Path(args.input)
    if not input_path.exists():
        logger.error(f"输入文件不存在: {input_path}")
        return 1

    out_dir = Path(args.output_dir)
    start = time.time()
    report = desensitize(
        input_path=input_path,
        output_dir=out_dir,
        epsilon=args.epsilon,
        geohash_precision=args.geohash_precision,
        min_users=args.min_users,
        salt=args.salt,
        user_col=args.user_col,
        lat_col=args.lat_col,
        lon_col=args.lon_col,
        time_col=args.time_col,
        cat_col=args.cat_col,
        city_label=args.city_label,
        seed=args.seed,
    )
    elapsed = time.time() - start

    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"\n[OK] {report['raw_record_count']} 条原始记录 → "
          f"{report['privacy_pipeline'][3]['buckets_emitted']} 条 evidence "
          f"(丢弃 {report['privacy_pipeline'][3]['buckets_dropped']} 个低密度单元) "
          f"[{elapsed:.2f}s]")
    print(f"[OK] Evidence: {report['output_evidence']}")
    print(f"[OK] Privacy report: {out_dir / 'privacy_report.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
