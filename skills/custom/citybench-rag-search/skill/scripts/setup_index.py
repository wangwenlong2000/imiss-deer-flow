#!/usr/bin/env python3
"""
CityBench ETL 全流水线 + ES 索引构建脚本。
将原始签到 CSV 数据经过五步处理写入 Elasticsearch 混合检索索引。

步骤:
  1. 数据归档 (SHA256 校验 + 只读保护)
  2. Manifest 元数据生成
  3. 聚合脱敏 + Jinja2 文本化
  4. Evidence 证据单元标准化
  5. ES 索引构建 (IK 分词 + Embedding 向量 + Bulk 写入)

用法:
    python3 setup_citybench_index.py --raw-data-dir ~/citydata/mobility/checkin --output-dir ./data_lake --sample
    python3 setup_citybench_index.py --raw-data-dir ~/citydata/mobility/checkin --output-dir ./data_lake
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import shutil
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
from jinja2 import Template
from tqdm import tqdm

logger = logging.getLogger(__name__)

# ───────────────── Configuration Defaults ─────────────────

DEFAULTS = {
    "SAMPLE_MODE": True,
    "SAMPLE_SIZE": 1000,
    "CHUNK_SIZE": 500000,
    "GEOHASH_PRECISION": 5,
    "MIN_USER_THRESHOLD": 5,
    "ANOMALY_STD_MULTIPLIER": 2.0,
    "ES_HOST": "http://localhost:9200",
    "ES_INDEX_NAME": "citybench_evidence",
    "ES_BULK_SIZE": 500,
    "DASHSCOPE_API_KEY": "",
    "DASHSCOPE_BASE_URL": "https://dashscope.aliyuncs.com/compatible-mode/v1",
    "EMBEDDING_MODEL": "text-embedding-v3",
    "EMBEDDING_DIMENSIONS": 1024,
    "EMBEDDING_BATCH_SIZE": 10,
    "EMBEDDING_QPS_LIMIT": 10.0,
    "EMBEDDING_MAX_RETRIES": 5,
}


def cfg(key: str):
    """Read config from env, fallback to DEFAULTS."""
    val = os.getenv(key)
    if val is not None:
        default = DEFAULTS.get(key)
        if isinstance(default, bool):
            return val.lower() in ("true", "1", "yes")
        if isinstance(default, int):
            return int(val)
        if isinstance(default, float):
            return float(val)
        return val
    return DEFAULTS.get(key)


# ───────────────── Time Slot Definitions ─────────────────

TIME_SLOTS_MAP = {
    "凌晨(23:00-7:00)": (23, 7),
    "早高峰(7:00-9:00)": (7, 9),
    "上午(9:00-11:00)": (9, 11),
    "午间(11:00-13:00)": (11, 13),
    "下午(13:00-17:00)": (13, 17),
    "晚高峰(17:00-19:00)": (17, 19),
    "夜间(19:00-23:00)": (19, 23),
}


def get_time_slot(hour: int) -> str:
    if 7 <= hour < 9:
        return "早高峰(7:00-9:00)"
    elif 9 <= hour < 11:
        return "上午(9:00-11:00)"
    elif 11 <= hour < 13:
        return "午间(11:00-13:00)"
    elif 13 <= hour < 17:
        return "下午(13:00-17:00)"
    elif 17 <= hour < 19:
        return "晚高峰(17:00-19:00)"
    elif 19 <= hour < 23:
        return "夜间(19:00-23:00)"
    else:
        return "凌晨(23:00-7:00)"


def get_time_slot_range(slot_name: str) -> Tuple[int, int]:
    return TIME_SLOTS_MAP.get(slot_name, (0, 0))


# ───────────────── Geohash (pure Python fallback) ─────────────────

try:
    import geohash as gh

    def encode_geohash(lat: float, lon: float, precision: int = 5) -> str:
        return gh.encode(lat, lon, precision=precision)

except ImportError:
    # Minimal pure-Python geohash encoder
    _BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"

    def encode_geohash(lat: float, lon: float, precision: int = 5) -> str:
        lat_range, lon_range = [-90.0, 90.0], [-180.0, 180.0]
        bits, ch, even = 0, 0, True
        result = []
        while len(result) < precision:
            if even:
                mid = (lon_range[0] + lon_range[1]) / 2
                if lon >= mid:
                    ch |= 1 << (4 - bits)
                    lon_range[0] = mid
                else:
                    lon_range[1] = mid
            else:
                mid = (lat_range[0] + lat_range[1]) / 2
                if lat >= mid:
                    ch |= 1 << (4 - bits)
                    lat_range[0] = mid
                else:
                    lat_range[1] = mid
            even = not even
            bits += 1
            if bits == 5:
                result.append(_BASE32[ch])
                bits, ch = 0, 0
        return "".join(result)


# ───────────────── Jinja2 Template ─────────────────

SUMMARY_TEMPLATE = Template(
    "{{ date }}{{ time_slot }}，"
    "{{ city }}(geohash:{{ geohash }})区域，"
    "签到活动量{{ checkin_count }}次，"
    "活跃用户约{{ unique_users }}人，"
    "热门类别为{{ top_categories_text }}，"
    "{% if wow_change_pct is not none %}"
    "较前一周同时段{{ '上升' if wow_change_pct > 0 else '下降' }}{{ wow_change_pct | abs | round(1) }}%，"
    "{% else %}"
    "无环比数据（首周），"
    "{% endif %}"
    "{{ anomaly_desc }}。"
)


def format_top_categories(top_cats: list) -> str:
    parts = [f"{cat}({pct:.0f}%)" for cat, pct in top_cats]
    return "、".join(parts) if parts else "无"


def render_summary(
    date, time_slot, city, geohash, checkin_count, unique_users,
    top_categories, wow_change_pct, is_anomaly,
) -> str:
    top_categories_text = format_top_categories(top_categories)
    anomaly_desc = "存在显著异常波动" if is_anomaly else "属于正常波动"
    rendered = SUMMARY_TEMPLATE.render(
        date=date, time_slot=time_slot, city=city, geohash=geohash,
        checkin_count=checkin_count, unique_users=unique_users,
        top_categories_text=top_categories_text,
        wow_change_pct=wow_change_pct, anomaly_desc=anomaly_desc,
    )
    return rendered.replace("\n", "").strip()


def format_date_cn(date_str: str) -> str:
    parts = date_str.split("-")
    return f"{parts[0]}年{int(parts[1])}月{int(parts[2])}日"


# ───────────────── Embedding Client ─────────────────

class EmbeddingClient:
    def __init__(self):
        from openai import OpenAI

        api_key = cfg("DASHSCOPE_API_KEY")
        if not api_key:
            raise ValueError("DASHSCOPE_API_KEY not set")
        self.client = OpenAI(api_key=api_key, base_url=cfg("DASHSCOPE_BASE_URL"))
        self.model = cfg("EMBEDDING_MODEL")
        self.dimensions = cfg("EMBEDDING_DIMENSIONS")
        self.batch_size = cfg("EMBEDDING_BATCH_SIZE")
        self.max_retries = cfg("EMBEDDING_MAX_RETRIES")
        self._last_req = 0.0
        self._interval = 1.0 / cfg("EMBEDDING_QPS_LIMIT")

    def _wait(self):
        elapsed = time.time() - self._last_req
        if elapsed < self._interval:
            time.sleep(self._interval - elapsed)
        self._last_req = time.time()

    def embed_texts(self, texts: List[str]) -> List[List[float]]:
        all_emb: List[List[float]] = []
        for i in range(0, len(texts), self.batch_size):
            batch = texts[i : i + self.batch_size]
            for attempt in range(1, self.max_retries + 1):
                try:
                    self._wait()
                    resp = self.client.embeddings.create(
                        model=self.model, input=batch,
                        dimensions=self.dimensions, encoding_format="float",
                    )
                    sorted_data = sorted(resp.data, key=lambda x: x.index)
                    all_emb.extend([item.embedding for item in sorted_data])
                    break
                except Exception as e:
                    wait = min(2 ** attempt, 60)
                    logger.warning(f"Embedding fail (attempt {attempt}): {e}, retry in {wait}s")
                    if attempt == self.max_retries:
                        raise
                    time.sleep(wait)
        return all_emb


# ═══════════════════ STEP 1: Archive ═══════════════════

def compute_sha256(filepath: str) -> str:
    sha256 = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def discover_city_files(raw_dir: str) -> list[dict]:
    raw_path = Path(raw_dir)
    if not raw_path.exists():
        logger.error(f"Raw data directory not found: {raw_dir}")
        return []
    city_files = []
    for checkin_file in sorted(raw_path.glob("*_filtered_checkins.csv")):
        city_name = checkin_file.name.replace("_filtered_checkins.csv", "")
        poi_file = raw_path / f"{city_name}_filtered_pois.csv"
        if poi_file.exists():
            city_files.append({
                "city": city_name,
                "checkins": str(checkin_file),
                "pois": str(poi_file),
            })
            logger.info(f"Found city: {city_name}")
    logger.info(f"Total cities found: {len(city_files)}")
    return city_files


def run_archive(raw_dir: str, archive_dir: Path) -> str:
    logger.info("=" * 60)
    logger.info("Step 1: Archive raw data")
    logger.info("=" * 60)
    archive_dir.mkdir(parents=True, exist_ok=True)
    if archive_dir.exists():
        for fpath in archive_dir.iterdir():
            os.chmod(fpath, 0o644)
        os.chmod(archive_dir, 0o755)

    city_files = discover_city_files(raw_dir)
    if not city_files:
        raise FileNotFoundError(f"No city data found in {raw_dir}")

    checksums, copied = [], 0
    for city_info in city_files:
        for key in ("checkins", "pois"):
            src = city_info[key]
            dst = archive_dir / os.path.basename(src)
            shutil.copy2(src, dst)
            copied += 1
            fhash = compute_sha256(str(dst))
            checksums.append(f"{fhash}  {os.path.basename(src)}")
            logger.info(f"Archived: {os.path.basename(src)}")

    with open(archive_dir / "checksums.sha256", "w") as f:
        f.write("\n".join(checksums) + "\n")

    for fpath in archive_dir.iterdir():
        os.chmod(fpath, 0o444)
    os.chmod(archive_dir, 0o555)
    logger.info(f"Step 1 done: {copied} files archived")
    return str(archive_dir)


# ═══════════════════ STEP 2: Manifest ═══════════════════

def load_pois(poi_path: str) -> pd.DataFrame:
    df = pd.read_csv(poi_path, dtype={"Venue ID": str})
    df.rename(columns={
        "Venue ID": "venue_id", "Latitude": "latitude",
        "Longitude": "longitude", "Venue Category Name": "category",
        "Country Code": "country_code",
    }, inplace=True)
    return df


def run_manifest(archive_dir: Path, manifest_path: Path, sample_mode: bool, sample_size: int) -> str:
    logger.info("=" * 60)
    logger.info("Step 2: Manifest metadata generation")
    logger.info("=" * 60)
    city_pairs = []
    for cf in sorted(archive_dir.glob("*_filtered_checkins.csv")):
        city = cf.name.replace("_filtered_checkins.csv", "")
        pf = archive_dir / f"{city}_filtered_pois.csv"
        if pf.exists():
            city_pairs.append((city, str(cf), str(pf)))

    entries = []
    for city_name, checkin_path, poi_path in tqdm(city_pairs, desc="Manifest"):
        pois = load_pois(poi_path)
        kw = {"dtype": {"User ID": str, "Venue ID": str}}
        df = pd.read_csv(checkin_path, nrows=sample_size if sample_mode else None, **kw)
        df["parsed_time"] = pd.to_datetime(df["UTC Time"], format="%a %b %d %H:%M:%S %z %Y")
        merged = df.merge(pois, left_on="Venue ID", right_on="venue_id", how="inner")
        spatial_bbox = {
            "min_lat": float(merged["latitude"].min()),
            "max_lat": float(merged["latitude"].max()),
            "min_lon": float(merged["longitude"].min()),
            "max_lon": float(merged["longitude"].max()),
        } if len(merged) > 0 else {"min_lat": 0, "max_lat": 0, "min_lon": 0, "max_lon": 0}
        entries.append({
            "dataset_id": f"citybench_checkins_{city_name.lower()}",
            "city": city_name,
            "record_count": len(df),
            "time_range": {
                "start": df["parsed_time"].min().isoformat(),
                "end": df["parsed_time"].max().isoformat(),
            },
            "spatial_bbox": spatial_bbox,
            "data_type": "spatiotemporal_trajectory",
        })

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump({"datasets": entries}, f, ensure_ascii=False, indent=2)
    logger.info(f"Step 2 done: {len(entries)} cities in manifest")
    return str(manifest_path)


# ═══════════════════ STEP 3: Textualize ═══════════════════

def process_city(city_name, checkin_path, poi_path, sample_mode, sample_size, precision, min_users):
    pois = load_pois(poi_path)
    kw = {"dtype": {"User ID": str, "Venue ID": str}}
    df = pd.read_csv(checkin_path, nrows=sample_size if sample_mode else None, **kw)
    logger.info(f"  {city_name}: {len(df)} checkins, {len(pois)} POIs")

    df["utc_time"] = pd.to_datetime(df["UTC Time"], format="%a %b %d %H:%M:%S %z %Y")
    df["tz_offset_minutes"] = df["Timezone Offset"].astype(int)
    df["local_time"] = df["utc_time"] + pd.to_timedelta(df["tz_offset_minutes"], unit="m")
    df = df.merge(pois, left_on="Venue ID", right_on="venue_id", how="inner")
    if len(df) == 0:
        return []

    df["geohash"] = df.apply(lambda r: encode_geohash(r["latitude"], r["longitude"], precision), axis=1)
    df["date"] = df["local_time"].dt.date
    df["hour"] = df["local_time"].dt.hour
    df["time_slot"] = df["hour"].apply(get_time_slot)
    df["iso_year"] = df["local_time"].dt.isocalendar().year.astype(int)
    df["iso_week"] = df["local_time"].dt.isocalendar().week.astype(int)
    df["weekday"] = df["local_time"].dt.weekday

    records = []
    for (date_val, ts, gh), group in df.groupby(["date", "time_slot", "geohash"]):
        unique_users = group["User ID"].nunique()
        if unique_users < min_users:
            continue
        cat_counts = group["category"].value_counts()
        total = cat_counts.sum()
        top3 = [(cat, (cnt / total) * 100) for cat, cnt in cat_counts.head(3).items()]
        records.append({
            "city": city_name, "date": str(date_val), "time_slot": ts,
            "geohash": gh, "checkin_count": len(group), "unique_users": unique_users,
            "top_categories": top3, "top_cat_names": [c for c, _ in top3],
            "iso_year": int(group["iso_year"].iloc[0]),
            "iso_week": int(group["iso_week"].iloc[0]),
            "weekday": int(group["weekday"].iloc[0]),
        })
    logger.info(f"  {city_name}: {len(records)} aggregated records")
    return records


def compute_wow_and_anomaly(records, anomaly_std):
    if not records:
        return records
    history = defaultdict(list)
    for rec in records:
        history[(rec["geohash"], rec["time_slot"], rec["weekday"])].append(rec)
    for group_recs in history.values():
        group_recs.sort(key=lambda r: (r["iso_year"], r["iso_week"]))
        counts = [r["checkin_count"] for r in group_recs]
        for i, rec in enumerate(group_recs):
            if i == 0:
                rec["wow_change_pct"] = None
            else:
                prev = group_recs[i - 1]["checkin_count"]
                rec["wow_change_pct"] = ((rec["checkin_count"] - prev) / prev * 100) if prev > 0 else None
            if len(counts) >= 3:
                mean_val, std_val = np.mean(counts), np.std(counts)
                rec["is_anomaly"] = bool(abs(rec["checkin_count"] - mean_val) > anomaly_std * std_val) if std_val > 0 else False
            else:
                rec["is_anomaly"] = False
    return records


def textualize_records(records):
    for rec in records:
        rec["text"] = render_summary(
            date=format_date_cn(rec["date"]), time_slot=rec["time_slot"],
            city=rec["city"], geohash=rec["geohash"],
            checkin_count=rec["checkin_count"], unique_users=rec["unique_users"],
            top_categories=rec["top_categories"],
            wow_change_pct=rec.get("wow_change_pct"),
            is_anomaly=rec.get("is_anomaly", False),
        )
    return records


def run_textualize(archive_dir, agg_path, sample_mode, sample_size, precision, min_users, anomaly_std):
    logger.info("=" * 60)
    logger.info("Step 3: Aggregate + textualize")
    logger.info("=" * 60)
    city_pairs = []
    for cf in sorted(archive_dir.glob("*_filtered_checkins.csv")):
        city = cf.name.replace("_filtered_checkins.csv", "")
        pf = archive_dir / f"{city}_filtered_pois.csv"
        if pf.exists():
            city_pairs.append((city, str(cf), str(pf)))

    all_records = []
    for city_name, cp, pp in city_pairs:
        recs = process_city(city_name, cp, pp, sample_mode, sample_size, precision, min_users)
        recs = compute_wow_and_anomaly(recs, anomaly_std)
        recs = textualize_records(recs)
        all_records.extend(recs)

    agg_path.parent.mkdir(parents=True, exist_ok=True)
    with open(agg_path, "w", encoding="utf-8") as f:
        for rec in all_records:
            out = dict(rec)
            out["top_categories"] = [[c, p] for c, p in out["top_categories"]]
            f.write(json.dumps(out, ensure_ascii=False) + "\n")
    logger.info(f"Step 3 done: {len(all_records)} textualized records")
    return all_records


# ═══════════════════ STEP 4: Evidence ═══════════════════

def build_evidence_id(date_str, time_slot, city, geohash):
    date_compact = date_str.replace("-", "")
    start_hour, _ = get_time_slot_range(time_slot)
    return f"traj_ev_{date_compact}_{start_hour:02d}00_{city.lower()}_{geohash}"


def record_to_evidence(rec, archive_dir):
    city, date_str, ts, gh = rec["city"], rec["date"], rec["time_slot"], rec["geohash"]
    evidence_id = build_evidence_id(date_str, ts, city, gh)
    start_hour, end_hour = get_time_slot_range(ts)
    if start_hour < end_hour:
        time_range = {"start": f"{date_str}T{start_hour:02d}:00:00", "end": f"{date_str}T{end_hour:02d}:00:00"}
    else:
        time_range = {"start": f"{date_str}T{start_hour:02d}:00:00", "end": f"{date_str}T23:59:59"}

    top_cats = rec.get("top_categories", [])
    top_cat_names = [c[0] if isinstance(c, (list, tuple)) else c for c in top_cats]

    return {
        "evidence_id": evidence_id,
        "data_type": "spatiotemporal_trajectory",
        "text": rec["text"],
        "meta": {
            "source_id": f"citybench_checkins_{city.lower()}",
            "source_path": os.path.join(str(archive_dir), f"{city}_filtered_checkins.csv"),
            "time_range": time_range,
            "geo_scope": {"city": city, "geohash": gh},
            "granularity": "hourly_district",
            "sensitivity_level": "aggregated_safe",
            "access_policy": "open",
            "features": {
                "checkin_count": rec["checkin_count"],
                "unique_users": rec["unique_users"],
                "top_categories": top_cat_names,
                "wow_change_pct": rec.get("wow_change_pct"),
                "anomaly_flag": rec.get("is_anomaly", False),
            },
        },
    }


def run_evidence(records, evidence_path, archive_dir):
    logger.info("=" * 60)
    logger.info("Step 4: Evidence generation")
    logger.info("=" * 60)
    evidence_path.parent.mkdir(parents=True, exist_ok=True)
    with open(evidence_path, "w", encoding="utf-8") as f:
        for rec in records:
            ev = record_to_evidence(rec, archive_dir)
            f.write(json.dumps(ev, ensure_ascii=False) + "\n")
    logger.info(f"Step 4 done: {len(records)} evidence records")
    return str(evidence_path)


# ═══════════════════ STEP 5: ES Index ═══════════════════

INDEX_MAPPING = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "analysis": {
            "analyzer": {
                "ik_max_analyzer": {"type": "custom", "tokenizer": "ik_max_word"},
                "ik_smart_analyzer": {"type": "custom", "tokenizer": "ik_smart"},
            }
        },
    },
    "mappings": {
        "properties": {
            "evidence_id": {"type": "keyword"},
            "data_type": {"type": "keyword"},
            "text": {
                "type": "text",
                "analyzer": "ik_max_analyzer",
                "search_analyzer": "ik_smart_analyzer",
            },
            "text_vector": {
                "type": "dense_vector",
                "dims": DEFAULTS["EMBEDDING_DIMENSIONS"],
                "index": True,
                "similarity": "cosine",
            },
            "meta": {
                "properties": {
                    "source_id": {"type": "keyword"},
                    "source_path": {"type": "keyword"},
                    "time_range": {
                        "properties": {
                            "start": {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
                            "end": {"type": "date", "format": "strict_date_optional_time||epoch_millis"},
                        }
                    },
                    "geo_scope": {
                        "properties": {
                            "city": {"type": "keyword"},
                            "geohash": {"type": "keyword"},
                        }
                    },
                    "granularity": {"type": "keyword"},
                    "sensitivity_level": {"type": "keyword"},
                    "access_policy": {"type": "keyword"},
                    "features": {
                        "properties": {
                            "checkin_count": {"type": "integer"},
                            "unique_users": {"type": "integer"},
                            "top_categories": {"type": "keyword"},
                            "wow_change_pct": {"type": "float"},
                            "anomaly_flag": {"type": "boolean"},
                        }
                    },
                }
            },
        }
    },
}


def run_index(evidence_path):
    logger.info("=" * 60)
    logger.info("Step 5: ES index construction")
    logger.info("=" * 60)

    from elasticsearch import Elasticsearch, helpers

    es_host = cfg("ES_HOST")
    index_name = cfg("ES_INDEX_NAME")

    es = Elasticsearch(es_host, request_timeout=60, max_retries=3, retry_on_timeout=True)
    if not es.ping():
        raise ConnectionError(f"Cannot connect to ES: {es_host}")
    logger.info(f"ES connected: {es.info()['version']['number']}")

    # Update dims from env
    INDEX_MAPPING["mappings"]["properties"]["text_vector"]["dims"] = cfg("EMBEDDING_DIMENSIONS")

    # Try creating index; fallback without IK if plugin not available
    if es.indices.exists(index=index_name):
        logger.warning(f"Index {index_name} exists, rebuilding...")
        es.indices.delete(index=index_name)

    try:
        es.indices.create(index=index_name, body=INDEX_MAPPING)
        logger.info(f"Index {index_name} created with IK analyzer")
    except Exception as e:
        logger.warning(f"IK analyzer not available ({e}), falling back to standard analyzer")
        fallback = json.loads(json.dumps(INDEX_MAPPING))
        fallback["settings"]["analysis"] = {}
        fallback["mappings"]["properties"]["text"]["analyzer"] = "standard"
        fallback["mappings"]["properties"]["text"]["search_analyzer"] = "standard"
        es.indices.create(index=index_name, body=fallback)
        logger.info(f"Index {index_name} created with standard analyzer (IK fallback)")

    # Load evidence
    evidences = []
    with open(evidence_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                evidences.append(json.loads(line))
    logger.info(f"Loaded {len(evidences)} evidence records")

    if not evidences:
        logger.warning("No evidence to index")
        return

    # Batch embed
    embed_client = EmbeddingClient()
    texts = [ev["text"] for ev in evidences]
    logger.info(f"Generating embeddings for {len(texts)} texts...")
    all_embeddings = []
    bs = cfg("EMBEDDING_BATCH_SIZE")
    for i in tqdm(range(0, len(texts), bs), desc="Embedding"):
        batch = texts[i : i + bs]
        embs = embed_client.embed_texts(batch)
        all_embeddings.extend(embs)

    # Bulk index
    def gen_actions():
        for i, ev in enumerate(evidences):
            yield {
                "_index": index_name,
                "_id": ev["evidence_id"],
                "_source": {
                    "evidence_id": ev["evidence_id"],
                    "data_type": ev["data_type"],
                    "text": ev["text"],
                    "text_vector": all_embeddings[i],
                    "meta": ev["meta"],
                },
            }

    success, errors = 0, 0
    for ok, result in tqdm(
        helpers.streaming_bulk(es, gen_actions(), chunk_size=cfg("ES_BULK_SIZE"),
                               raise_on_error=False, max_retries=3),
        total=len(evidences), desc="ES bulk",
    ):
        if ok:
            success += 1
        else:
            errors += 1

    es.indices.refresh(index=index_name)
    count = es.count(index=index_name)["count"]
    logger.info(f"Step 5 done: {count} docs indexed (success={success}, errors={errors})")


# ═══════════════════ Main CLI ═══════════════════

def main() -> int:
    parser = argparse.ArgumentParser(description="CityBench ETL + ES index setup")
    parser.add_argument("--raw-data-dir", required=True, help="Directory with *_filtered_checkins.csv")
    parser.add_argument("--output-dir", required=True, help="Data lake output root")
    parser.add_argument("--sample", action="store_true", help="Enable sampling mode")
    parser.add_argument("--full", action="store_true", help="Disable sampling (full mode)")
    parser.add_argument("--sample-size", type=int, default=None, help="Rows per city in sample mode")
    parser.add_argument("--step", type=int, help="Run only this step (1-5)")
    parser.add_argument("--from-step", type=int, help="Start from this step (1-5)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    sample_mode = True
    if args.full:
        sample_mode = False
    elif args.sample:
        sample_mode = True
    else:
        sample_mode = cfg("SAMPLE_MODE")
    sample_size = args.sample_size or cfg("SAMPLE_SIZE")

    precision = cfg("GEOHASH_PRECISION")
    min_users = cfg("MIN_USER_THRESHOLD")
    anomaly_std = cfg("ANOMALY_STD_MULTIPLIER")

    version_tag = f"v1_{datetime.now().strftime('%Y%m%d')}"
    base = Path(args.output_dir) / "trajectories" / "citybench" / version_tag
    archive_dir = base / "raw"
    manifest_path = base / "manifest.json"
    agg_path = base / "aggregated" / "aggregated_textualized.jsonl"
    evidence_path = base / "evidence.jsonl"

    logger.info("╔══════════════════════════════════════════════════╗")
    logger.info("║   CityBench ETL Pipeline + ES Index Setup       ║")
    logger.info("╚══════════════════════════════════════════════════╝")
    logger.info(f"Sample mode: {'ON ({} rows/city)'.format(sample_size) if sample_mode else 'OFF (full)'}")
    logger.info(f"Output: {base}")

    start = time.time()

    if args.step:
        steps = [args.step]
    elif args.from_step:
        steps = list(range(args.from_step, 6))
    else:
        steps = [1, 2, 3, 4, 5]

    records = None

    try:
        if 1 in steps:
            run_archive(args.raw_data_dir, archive_dir)
        if 2 in steps:
            run_manifest(archive_dir, manifest_path, sample_mode, sample_size)
        if 3 in steps:
            records = run_textualize(
                archive_dir, agg_path, sample_mode, sample_size,
                precision, min_users, anomaly_std,
            )
        if 4 in steps:
            if records is None:
                # Load from intermediate file
                if agg_path.exists():
                    records = []
                    with open(agg_path, "r", encoding="utf-8") as f:
                        for line in f:
                            if line.strip():
                                rec = json.loads(line)
                                rec["top_categories"] = [(c[0], c[1]) for c in rec.get("top_categories", [])]
                                records.append(rec)
                else:
                    raise FileNotFoundError(f"No aggregated data at {agg_path}. Run step 3 first.")
            run_evidence(records, evidence_path, archive_dir)
        if 5 in steps:
            run_index(str(evidence_path))
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        return 1
    except ConnectionError as e:
        logger.error(f"Connection failed: {e}")
        return 1
    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        return 1

    elapsed = time.time() - start
    logger.info(f"Pipeline complete in {elapsed:.1f}s ({elapsed / 60:.1f}min)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
