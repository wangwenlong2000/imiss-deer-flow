#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
import os
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    import yaml
except Exception:
    yaml = None

SKILL = "object-statistics"
VERSION = "1.0.0"
DEFAULT_INDEX = "citybrain-video-library"


def load_structured(path: str | None) -> Any:
    if not path:
        return {}
    source = Path(path)
    text = source.read_text(encoding="utf-8")
    if source.suffix.lower() in {".yaml", ".yml"}:
        if yaml is None:
            raise RuntimeError("PyYAML is required to read YAML files")
        return yaml.safe_load(text) or {}
    return json.loads(text) if text.strip() else {}


def load_config(path: str | None) -> dict[str, Any]:
    data = load_structured(path)
    return data if isinstance(data, dict) else {}


def load_list(path: str | None, key: str) -> list[dict[str, Any]]:
    data = load_structured(path)
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        value = data.get(key) or data.get("data", {}).get(key, [])
        return value if isinstance(value, list) else []
    return []


def input_value(input_data: dict[str, Any], *keys: str, default: Any = None) -> Any:
    nested = input_data.get("data") if isinstance(input_data.get("data"), dict) else {}
    for key in keys:
        if input_data.get(key) is not None:
            return input_data[key]
        if nested.get(key) is not None:
            return nested[key]
    return default


def success(data: dict[str, Any], confidence: float = 1.0) -> dict[str, Any]:
    return {"skill": SKILL, "version": VERSION, "status": "success", "confidence": round(confidence, 4), "data": data}


def failed(code: str, message: str, retryable: bool = False, detail: dict[str, Any] | None = None) -> dict[str, Any]:
    return {"skill": SKILL, "version": VERSION, "status": "failed", "error_code": code, "message": message, "retryable": retryable, "detail": detail or {}}


def emit(result: dict[str, Any], output: str | None) -> int:
    text = json.dumps(result, ensure_ascii=False, indent=2)
    if output:
        path = Path(output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0 if result.get("status") == "success" else 1


class EsError(RuntimeError):
    def __init__(self, code: str, message: str, retryable: bool = False, detail: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable
        self.detail = detail or {}


class EsClient:
    def __init__(self, config: dict[str, Any]):
        es_config = config.get("elasticsearch", {}) if isinstance(config.get("elasticsearch"), dict) else {}
        lib_config = config.get("video_library", {}) if isinstance(config.get("video_library"), dict) else {}
        nested_es = lib_config.get("elasticsearch", {}) if isinstance(lib_config.get("elasticsearch"), dict) else {}
        hosts = es_config.get("hosts") if isinstance(es_config.get("hosts"), list) else []
        nested_hosts = nested_es.get("hosts") if isinstance(nested_es.get("hosts"), list) else []
        self.url = (os.getenv("ES_URL") or nested_es.get("url") or (nested_hosts[0] if nested_hosts else None) or es_config.get("url") or (hosts[0] if hosts else None) or "http://localhost:3128").rstrip("/")
        self.username = os.getenv("ES_USERNAME") or nested_es.get("username") or es_config.get("username")
        self.password = os.getenv("ES_PASSWORD") or nested_es.get("password") or es_config.get("password")

    def request(self, method: str, path: str, body: Any | None = None, ok: set[int] | None = None) -> tuple[int, Any]:
        ok = ok or {200}
        req = urllib.request.Request(f"{self.url}/{path.lstrip('/')}", data=None if body is None else json.dumps(body, ensure_ascii=False).encode("utf-8"), method=method, headers={"Content-Type": "application/json"})
        if self.username and self.password:
            token = base64.b64encode(f"{self.username}:{self.password}".encode("utf-8")).decode("ascii")
            req.add_header("Authorization", f"Basic {token}")
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                text = resp.read().decode("utf-8", errors="replace")
                return resp.status, json.loads(text) if text.strip() else {}
        except urllib.error.HTTPError as exc:
            text = exc.read().decode("utf-8", errors="replace")
            try:
                payload = json.loads(text) if text.strip() else {}
            except Exception:
                payload = {"raw": text}
            raise EsError("ES_REQUEST_FAILED", f"Elasticsearch request failed: HTTP {exc.code}", exc.code >= 500, {"path": path, "response": payload})
        except urllib.error.URLError as exc:
            raise EsError("ES_CONNECTION_FAILED", f"Could not connect to Elasticsearch at {self.url}: {exc.reason}", True, {"url": self.url})

    def search(self, index: str, body: dict[str, Any]) -> list[dict[str, Any]]:
        _, payload = self.request("POST", f"{index}/_search", body, ok={200})
        return [hit.get("_source", {}) for hit in payload.get("hits", {}).get("hits", [])]


def parse_time_bucket(value: str | None) -> str:
    if not value:
        return "unknown"
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:00")
    except Exception:
        return str(value)[:13] or "unknown"


def add_detection_stats(stats: dict[str, Counter[str]], detection: dict[str, Any], fallback_camera: str | None = None) -> int:
    count = 0
    camera_id = detection.get("camera_id") or fallback_camera or "unknown"
    bucket = parse_time_bucket(detection.get("timestamp"))
    for obj in detection.get("objects", []) or []:
        label = str(obj.get("label") or "unknown")
        stats["by_label"][label] += 1
        stats["by_camera"][str(camera_id)] += 1
        stats["by_time_bucket"][bucket] += 1
        count += 1
    return count


def add_track_stats(stats: dict[str, Counter[str]], track: dict[str, Any]) -> None:
    label = str(track.get("label") or "unknown")
    camera_id = str(track.get("camera_id") or "unknown")
    movement = str(track.get("movement_state") or "unknown")
    stats["tracks_by_label"][label] += 1
    stats["by_camera"][camera_id] += 0
    stats["movement_states"][movement] += 1


def stats_from_local(detections: list[dict[str, Any]], tracks: list[dict[str, Any]]) -> dict[str, Any]:
    stats: dict[str, Counter[str]] = defaultdict(Counter)
    total = 0
    for detection in detections:
        total += add_detection_stats(stats, detection)
    for track in tracks:
        add_track_stats(stats, track)
    return format_stats(total, stats, len(tracks), "local_json")


def stats_from_docs(docs: list[dict[str, Any]]) -> dict[str, Any]:
    stats: dict[str, Counter[str]] = defaultdict(Counter)
    total = 0
    track_total = 0
    for doc in docs:
        camera_id = str(doc.get("camera_id") or "unknown")
        bucket = parse_time_bucket(doc.get("started_at"))
        object_summary = doc.get("object_summary") if isinstance(doc.get("object_summary"), dict) else {}
        counts = object_summary.get("by_label") if isinstance(object_summary.get("by_label"), dict) else {}
        for label, count in counts.items():
            value = int(count or 0)
            stats["by_label"][str(label)] += value
            stats["by_camera"][camera_id] += value
            stats["by_time_bucket"][bucket] += value
            total += value
        tracks_summary = doc.get("tracks_summary") if isinstance(doc.get("tracks_summary"), dict) else {}
        track_total += int(tracks_summary.get("total_tracks") or 0)
        movement = tracks_summary.get("movement_states") if isinstance(tracks_summary.get("movement_states"), dict) else {}
        for state, count in movement.items():
            stats["movement_states"][str(state)] += int(count or 0)
    return format_stats(total, stats, track_total, "elasticsearch")


def format_stats(total: int, stats: dict[str, Counter[str]], track_total: int, source: str) -> dict[str, Any]:
    return {
        "source": source,
        "total_objects": total,
        "total_tracks": track_total,
        "by_label": dict(stats["by_label"]),
        "by_camera": dict(stats["by_camera"]),
        "by_time_bucket": dict(stats["by_time_bucket"]),
        "movement_states": dict(stats["movement_states"]),
    }


def build_query(args: argparse.Namespace, input_data: dict[str, Any]) -> dict[str, Any]:
    filters: list[dict[str, Any]] = []
    video_id = args.video_id or input_value(input_data, "video_id")
    camera_id = args.camera_id or input_value(input_data, "camera_id")
    if video_id:
        filters.append({"term": {"video_id": video_id}})
    if camera_id:
        filters.append({"term": {"camera_id": camera_id}})
    time_range: dict[str, Any] = {}
    if args.start_time:
        time_range["gte"] = args.start_time
    if args.end_time:
        time_range["lte"] = args.end_time
    if time_range:
        filters.append({"range": {"started_at": time_range}})
    return {"size": 1000, "_source": {"excludes": ["vector"]}, "query": {"bool": {"filter": filters, "must": [{"match_all": {}}]}}}


def main() -> int:
    parser = argparse.ArgumentParser(description="Compute object statistics.")
    parser.add_argument("--input")
    parser.add_argument("--index", default=DEFAULT_INDEX)
    parser.add_argument("--video-id")
    parser.add_argument("--camera-id")
    parser.add_argument("--start-time")
    parser.add_argument("--end-time")
    parser.add_argument("--detections-json")
    parser.add_argument("--tracks-json")
    parser.add_argument("--group-by", default="label,camera,time,movement")
    parser.add_argument("--config")
    parser.add_argument("--output")
    args = parser.parse_args()
    input_data = load_structured(args.input) if args.input else {}
    input_data = input_data if isinstance(input_data, dict) else {}
    detections = load_list(args.detections_json, "detections") if args.detections_json else input_value(input_data, "detections", default=[])
    tracks = load_list(args.tracks_json, "tracks") if args.tracks_json else input_value(input_data, "tracks", default=[])
    detections = detections if isinstance(detections, list) else []
    tracks = tracks if isinstance(tracks, list) else []
    if detections or tracks:
        data = stats_from_local(detections, tracks)
        data["group_by"] = [x.strip() for x in args.group_by.split(",") if x.strip()]
        return emit(success(data), args.output)
    config = load_config(args.config)
    try:
        es = EsClient(config)
        docs = es.search(args.index, build_query(args, input_data))
    except EsError as exc:
        return emit(failed(exc.code, exc.message, exc.retryable, exc.detail), args.output)
    data = stats_from_docs(docs)
    data["group_by"] = [x.strip() for x in args.group_by.split(",") if x.strip()]
    data["index"] = args.index
    data["matched_videos"] = len(docs)
    return emit(success(data), args.output)


if __name__ == "__main__":
    raise SystemExit(main())
