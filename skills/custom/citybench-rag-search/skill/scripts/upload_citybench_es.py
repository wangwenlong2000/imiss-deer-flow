#!/usr/bin/env python3
"""Upload CityBench evidence JSONL to the shared Elasticsearch service.

This script intentionally uses only the Python standard library so it can run
even when the local `elasticsearch` package is broken by NumPy/pyarrow ABI
mismatches. It creates a text-searchable index and reserves an indexed
dense_vector field for later embedding backfill.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
SKILL_ROOT = SCRIPT_DIR.parent
DEFAULT_EVIDENCE = SKILL_ROOT / "data" / "sample_evidence.jsonl"
DEFAULT_LANDMARKS = SKILL_ROOT / "data" / "geohash_landmarks.json"


def _auth_header(username: str | None, password: str | None) -> dict[str, str]:
    if not username or password is None:
        return {}
    token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
    return {"Authorization": f"Basic {token}"}


class ESClient:
    def __init__(self, base_url: str, username: str | None, password: str | None, timeout: int = 60):
        self.base_url = base_url.rstrip("/")
        self.headers = _auth_header(username, password)
        self.timeout = timeout
        # Match `curl --noproxy '*'`: the campus proxy returns 502 for direct
        # ES endpoints, so this client must bypass environment proxy settings.
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def request(self, method: str, path: str, body: Any | None = None, *, ndjson: bool = False) -> Any:
        url = f"{self.base_url}/{path.lstrip('/')}"
        data = None
        headers = dict(self.headers)
        if body is not None:
            if ndjson:
                data = body.encode("utf-8") if isinstance(body, str) else body
                headers["Content-Type"] = "application/x-ndjson"
            else:
                data = json.dumps(body, ensure_ascii=False).encode("utf-8")
                headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with self.opener.open(req, timeout=self.timeout) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as e:
            raw = e.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"ES HTTP {e.code} {method} {path}: {raw}") from e


def load_landmarks(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    return {k: v for k, v in data.items() if isinstance(v, dict) and not k.startswith("_")}


def lookup_landmark(landmarks: dict[str, dict[str, Any]], city: str | None, geohash: str | None) -> dict[str, Any] | None:
    if not geohash:
        return None
    cities = [city] if city else list(landmarks)
    for c in cities:
        if not c:
            continue
        city_map = landmarks.get(c, {})
        if geohash in city_map:
            return city_map[geohash]
        for prefix, info in city_map.items():
            if isinstance(info, dict) and geohash.startswith(prefix):
                return info
    return None


def iter_evidence(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSONL at {path}:{line_no}: {exc}") from exc
    return records


def display_text(text: str, city: str | None, geohash: str | None, landmark: str | None) -> str:
    if not (city and geohash and landmark):
        return text
    return (
        text.replace(f"{city}(geohash:{geohash})区域", f"{landmark}区域")
        .replace(f"{city}（geohash:{geohash}）区域", f"{landmark}区域")
        .replace(f"geohash:{geohash}", landmark)
    )


def to_doc(ev: dict[str, Any], landmarks: dict[str, dict[str, Any]]) -> dict[str, Any]:
    meta = ev.get("meta") or {}
    geo = meta.get("geo_scope") or {}
    time_range = meta.get("time_range") or {}
    features = meta.get("features") or {}
    city = geo.get("city")
    geohash = geo.get("geohash")
    info = lookup_landmark(landmarks, city, geohash) or {}
    landmark = info.get("landmark") or geo.get("landmark")
    district = info.get("district") or geo.get("district")
    lat = info.get("lat", geo.get("lat"))
    lon = info.get("lon", geo.get("lon"))
    text = display_text(str(ev.get("text", "")), city, geohash, landmark)
    evidence_id = str(ev.get("evidence_id") or ev.get("id"))
    source_path = meta.get("source_path") or f"skills/custom/citybench-rag-search/skill/data/sample_evidence.jsonl#{evidence_id}"

    return {
        "id": evidence_id,
        "evidence_id": evidence_id,
        "title": f"CityBench {city or ''} {landmark or geohash or ''} {time_range.get('start', '')}".strip(),
        "content": text,
        "text": text,
        "data_type": ev.get("data_type", "spatiotemporal_trajectory"),
        "source_path": source_path,
        "city": city,
        "geohash": geohash,
        "landmark": landmark,
        "district": district,
        "latitude": lat,
        "longitude": lon,
        "time_start": time_range.get("start"),
        "time_end": time_range.get("end"),
        "granularity": meta.get("granularity"),
        "sensitivity_level": meta.get("sensitivity_level"),
        "access_policy": meta.get("access_policy"),
        "checkin_count": features.get("checkin_count"),
        "unique_users": features.get("unique_users"),
        "top_categories": features.get("top_categories") or [],
        "wow_change_pct": features.get("wow_change_pct"),
        "anomaly_flag": features.get("anomaly_flag"),
        "metadata": {
            "source_id": meta.get("source_id"),
            "original_text": ev.get("text"),
            "geo_scope": {**geo, "landmark": landmark, "district": district, "lat": lat, "lon": lon},
            "time_range": time_range,
            "features": features,
        },
        "meta": meta,
    }


def index_mapping(vector_dims: int, replicas: int) -> dict[str, Any]:
    return {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": replicas,
        },
        "mappings": {
            "dynamic": True,
            "properties": {
                "id": {"type": "keyword"},
                "evidence_id": {"type": "keyword"},
                "title": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                "content": {"type": "text"},
                "text": {"type": "text"},
                "data_type": {"type": "keyword"},
                "source_path": {"type": "keyword"},
                "city": {"type": "keyword"},
                "geohash": {"type": "keyword"},
                "landmark": {"type": "keyword", "fields": {"text": {"type": "text"}}},
                "district": {"type": "keyword", "fields": {"text": {"type": "text"}}},
                "latitude": {"type": "double"},
                "longitude": {"type": "double"},
                "time_start": {"type": "date"},
                "time_end": {"type": "date"},
                "granularity": {"type": "keyword"},
                "sensitivity_level": {"type": "keyword"},
                "access_policy": {"type": "keyword"},
                "checkin_count": {"type": "integer"},
                "unique_users": {"type": "integer"},
                "top_categories": {"type": "keyword", "fields": {"text": {"type": "text"}}},
                "wow_change_pct": {"type": "float"},
                "anomaly_flag": {"type": "boolean"},
                # Reserved for a later embedding backfill when the model service is reachable.
                "text_vector": {"type": "dense_vector", "dims": vector_dims, "index": True, "similarity": "cosine"},
                "metadata": {"type": "object", "enabled": True},
                "meta": {"type": "object", "enabled": True},
            }
        },
    }


def make_bulk_payload(index_name: str, docs: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    for doc in docs:
        lines.append(json.dumps({"index": {"_index": index_name, "_id": doc["id"]}}, ensure_ascii=False))
        lines.append(json.dumps(doc, ensure_ascii=False))
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Upload CityBench evidence to Elasticsearch")
    p.add_argument("--es-url", default=os.getenv("ES_URL", "http://219.245.186.96:3128"))
    p.add_argument("--es-username", default=os.getenv("ES_USERNAME", "citybrain-street"))
    p.add_argument("--es-password", default=os.getenv("ES_PASSWORD", "123456"))
    p.add_argument("--index", default=os.getenv("ES_INDEX_NAME", "citybench_evidence"))
    p.add_argument("--evidence", type=Path, default=DEFAULT_EVIDENCE)
    p.add_argument("--landmarks", type=Path, default=DEFAULT_LANDMARKS)
    p.add_argument("--output-jsonl", type=Path, default=Path("/tmp/citybench_es_docs.jsonl"))
    p.add_argument("--vector-dims", type=int, default=2048)
    p.add_argument("--replicas", type=int, default=0)
    p.add_argument("--recreate", action="store_true", help="Delete and recreate the target index")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    es = ESClient(args.es_url, args.es_username, args.es_password)
    info = es.request("GET", "/")
    print(f"[OK] Connected ES {info.get('version', {}).get('number')} at {args.es_url}")

    landmarks = load_landmarks(args.landmarks)
    evidence = iter_evidence(args.evidence)
    docs = [to_doc(ev, landmarks) for ev in evidence]
    args.output_jsonl.parent.mkdir(parents=True, exist_ok=True)
    with args.output_jsonl.open("w", encoding="utf-8") as f:
        for doc in docs:
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")
    print(f"[OK] Prepared {len(docs)} docs: {args.output_jsonl}")

    exists = False
    try:
        es.request("HEAD", f"/{args.index}")
        exists = True
    except RuntimeError:
        exists = False

    if exists and args.recreate:
        es.request("DELETE", f"/{args.index}")
        print(f"[OK] Deleted existing index: {args.index}")
        exists = False

    if not exists:
        es.request("PUT", f"/{args.index}", index_mapping(args.vector_dims, args.replicas))
        print(f"[OK] Created index: {args.index}")
    else:
        print(f"[INFO] Index exists, upserting docs: {args.index}")

    if docs:
        payload = make_bulk_payload(args.index, docs)
        result = es.request("POST", "/_bulk", payload, ndjson=True)
        if result.get("errors"):
            failures = [item for item in result.get("items", []) if "error" in item.get("index", {})]
            print(json.dumps(failures[:5], ensure_ascii=False, indent=2), file=sys.stderr)
            raise RuntimeError(f"Bulk upload had {len(failures)} failures")

    es.request("POST", f"/{args.index}/_refresh")
    time.sleep(0.2)
    count = es.request("GET", f"/{args.index}/_count")
    print(json.dumps({
        "ok": True,
        "index": args.index,
        "uploaded_docs": len(docs),
        "es_count": count.get("count"),
        "es_url": args.es_url,
        "prepared_jsonl": str(args.output_jsonl),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
