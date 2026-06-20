#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from hashlib import sha1
from pathlib import Path
from typing import Any

try:
    import yaml
except Exception:
    yaml = None

SKILL = "batch-video-ingestion"
VERSION = "1.0.0"
DEFAULT_INDEX = "citybrain-video-library"
VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".mkv", ".webm"}


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


def input_value(data: dict[str, Any], *keys: str, default: Any = None) -> Any:
    nested = data.get("data") if isinstance(data.get("data"), dict) else {}
    for key in keys:
        if data.get(key) is not None:
            return data[key]
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
        self.vector_dims = int(os.getenv("VIDEO_VECTOR_DIMS") or lib_config.get("vector_dims") or 384)

    def request(self, method: str, path: str, body: Any | None = None, ok: set[int] | None = None) -> tuple[int, Any]:
        ok = ok or {200, 201}
        url = f"{self.url}/{path.lstrip('/')}"
        data = None if body is None else json.dumps(body, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
        if self.username and self.password:
            token = base64.b64encode(f"{self.username}:{self.password}".encode("utf-8")).decode("ascii")
            req.add_header("Authorization", f"Basic {token}")
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                text = resp.read().decode("utf-8", errors="replace")
                payload = json.loads(text) if text.strip() else {}
                if resp.status not in ok:
                    raise EsError("ES_REQUEST_FAILED", f"Unexpected Elasticsearch status {resp.status}", True, {"response": payload})
                return resp.status, payload
        except urllib.error.HTTPError as exc:
            text = exc.read().decode("utf-8", errors="replace")
            try:
                payload = json.loads(text) if text.strip() else {}
            except Exception:
                payload = {"raw": text}
            if exc.code in ok:
                return exc.code, payload
            raise EsError("ES_REQUEST_FAILED", f"Elasticsearch request failed: HTTP {exc.code}", exc.code >= 500, {"path": path, "response": payload})
        except urllib.error.URLError as exc:
            raise EsError("ES_CONNECTION_FAILED", f"Could not connect to Elasticsearch at {self.url}: {exc.reason}", True, {"url": self.url})

    def ensure_index(self, index: str) -> None:
        status, _ = self.request("HEAD", index, ok={200, 404})
        if status == 200:
            return
        mapping = {
            "mappings": {
                "properties": {
                    "video_id": {"type": "keyword"},
                    "camera_id": {"type": "keyword"},
                    "file_path": {"type": "keyword"},
                    "raw_segment_uri": {"type": "keyword"},
                    "filename": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "source_type": {"type": "keyword"},
                    "duration_seconds": {"type": "float"},
                    "width": {"type": "integer"},
                    "height": {"type": "integer"},
                    "started_at": {"type": "date"},
                    "ended_at": {"type": "date"},
                    "location": {"type": "object", "enabled": True},
                    "labels": {"type": "keyword"},
                    "event_types": {"type": "keyword"},
                    "ingestion_mode": {"type": "keyword"},
                    "content_detection_enabled": {"type": "boolean"},
                    "search_terms": {"type": "keyword"},
                    "path_tokens": {"type": "keyword"},
                    "content_text": {"type": "text"},
                    "object_summary": {"type": "object", "enabled": True},
                    "tracks_summary": {"type": "object", "enabled": True},
                    "metadata": {"type": "object", "enabled": True},
                    "created_at": {"type": "date"},
                    "updated_at": {"type": "date"},
                    "vector": {"type": "dense_vector", "dims": self.vector_dims, "index": True, "similarity": "cosine"}
                }
            }
        }
        self.request("PUT", index, mapping, ok={200, 201})

    def index_doc(self, index: str, doc_id: str, doc: dict[str, Any]) -> None:
        self.request("PUT", f"{index}/_doc/{doc_id}", doc, ok={200, 201})

    def refresh(self, index: str) -> None:
        self.request("POST", f"{index}/_refresh", None, ok={200})


def run_skill(skill: str, args: list[str]) -> dict[str, Any]:
    script = Path(__file__).resolve().parents[2] / skill / "scripts" / "run.py"
    cmd = [sys.executable, str(script), *args]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if "--output" in args:
        output_index = args.index("--output") + 1
        if output_index < len(args):
            output_path = Path(args[output_index])
            if output_path.exists():
                try:
                    result = json.loads(output_path.read_text(encoding="utf-8"))
                    if proc.returncode != 0 and result.get("status") == "success":
                        result["status"] = "failed"
                        result["error_code"] = "SCRIPT_FAILED"
                    return result
                except Exception:
                    pass
    text = proc.stdout.strip() or proc.stderr.strip()
    try:
        result = json.loads(text)
    except Exception:
        result = {"status": "failed", "error_code": "SCRIPT_OUTPUT_PARSE_FAILED", "message": text[-2000:], "returncode": proc.returncode}
    if proc.returncode != 0 and result.get("status") == "success":
        result["status"] = "failed"
        result["error_code"] = "SCRIPT_FAILED"
    return result


def resolve_records(input_data: dict[str, Any], manifest_json: str | None, video_dir: str | None) -> list[dict[str, Any]]:
    if manifest_json:
        manifest = load_structured(manifest_json)
    else:
        manifest = input_value(input_data, "videos", "manifest", default=[])
    if isinstance(manifest, dict):
        records = manifest.get("videos") or manifest.get("data", {}).get("videos") or []
    elif isinstance(manifest, list):
        records = manifest
    else:
        records = []
    if records:
        return [r for r in records if isinstance(r, dict)]
    directory = video_dir or input_value(input_data, "video_dir")
    if not directory:
        return []
    root = Path(directory)
    return [{"file_path": str(path), "filename": path.name} for path in sorted(root.rglob("*")) if path.suffix.lower() in VIDEO_EXTENSIONS]


def stable_video_id(record: dict[str, Any], path: str) -> str:
    if record.get("video_id") or record.get("id"):
        return str(record.get("video_id") or record.get("id"))
    return "VID_" + sha1(path.encode("utf-8")).hexdigest()[:16]


def labels_from_detections(detections: list[dict[str, Any]]) -> tuple[list[str], dict[str, int], int]:
    counts: Counter[str] = Counter()
    total = 0
    for detection in detections:
        for obj in detection.get("objects", []) or []:
            label = str(obj.get("label") or "unknown")
            counts[label] += 1
            total += 1
    return sorted(counts), dict(counts), total


def summarize_tracks(tracks: list[dict[str, Any]]) -> dict[str, Any]:
    by_label: Counter[str] = Counter()
    movement: Counter[str] = Counter()
    for track in tracks:
        by_label[str(track.get("label") or "unknown")] += 1
        movement[str(track.get("movement_state") or "unknown")] += 1
    return {"total_tracks": len(tracks), "by_label": dict(by_label), "movement_states": dict(movement), "tracks": tracks[:50]}


def writable_output_base(record: dict[str, Any], config: dict[str, Any]) -> Path:
    configured = record.get("output_dir") or config.get("output_dir")
    if configured:
        path = Path(str(configured))
        if str(path).startswith("/mnt/data") and Path("/mnt/user-data/outputs").exists():
            return Path("/mnt/user-data/outputs") / "video-library-work"
        return path
    if Path("/mnt/user-data/outputs").exists():
        return Path("/mnt/user-data/outputs") / "video-library-work"
    return Path("outputs")


def flatten_text_values(value: Any) -> list[str]:
    texts: list[str] = []
    if value is None:
        return texts
    if isinstance(value, (str, int, float)):
        text = str(value).strip()
        if text:
            texts.append(text)
    elif isinstance(value, dict):
        for child in value.values():
            texts.extend(flatten_text_values(child))
    elif isinstance(value, list):
        for child in value:
            texts.extend(flatten_text_values(child))
    return texts


def path_tokens(path: str) -> list[str]:
    source = Path(path)
    raw_parts = list(source.parts) + [source.stem, source.name]
    tokens: list[str] = []
    for part in raw_parts:
        cleaned = part.replace("_", " ").replace("-", " ").replace(".", " ")
        tokens.extend(chunk for chunk in cleaned.split() if chunk)
    seen = set()
    result = []
    for token in tokens:
        lowered = token.lower()
        if lowered not in seen:
            seen.add(lowered)
            result.append(token)
    return result[:100]


def enriched_metadata(record: dict[str, Any], file_path: str) -> dict[str, Any]:
    metadata = dict(record.get("metadata") if isinstance(record.get("metadata"), dict) else {})
    source = Path(file_path)
    metadata.setdefault("filename", record.get("filename") or source.name)
    metadata.setdefault("file_ext", source.suffix.lower())
    metadata.setdefault("source_path", file_path)
    metadata.setdefault("path_tokens", path_tokens(file_path))
    for key in ("name", "description", "owner_scope", "source_kind", "scene", "region", "address", "road", "camera_name"):
        if record.get(key) is not None and metadata.get(key) is None:
            metadata[key] = record[key]
    return metadata


def build_content_text(record: dict[str, Any], doc: dict[str, Any]) -> str:
    parts = [
        doc.get("filename"),
        doc.get("camera_id"),
        record.get("name"),
        record.get("description"),
        " ".join(doc.get("labels") or []),
        " ".join(doc.get("search_terms") or []),
        " ".join(doc.get("path_tokens") or []),
    ]
    parts.extend(flatten_text_values(doc.get("location") or {}))
    parts.extend(flatten_text_values(doc.get("metadata") or {}))
    parts.extend(flatten_text_values(record.get("tags") or []))
    seen = set()
    deduped = []
    for part in parts:
        text = str(part).strip()
        if text and text not in seen:
            seen.add(text)
            deduped.append(text)
    return " ".join(deduped)


def with_config(args: list[str], config_path: str | None) -> list[str]:
    if config_path:
        return [*args, "--config", config_path]
    return args


def resolve_analysis_mode(record: dict[str, Any], default_mode: str) -> str:
    mode = str(record.get("analysis_mode") or record.get("ingestion_mode") or default_mode or "object_detection")
    aliases = {
        "metadata": "metadata_only",
        "metadata-only": "metadata_only",
        "no_detection": "metadata_only",
        "none": "metadata_only",
        "fast": "metadata_only",
        "object": "object_detection",
        "object-analysis": "object_detection",
        "content_detection": "object_detection",
        "content-detection": "object_detection",
        "full": "object_detection",
    }
    return aliases.get(mode, mode)


def ingest_one(record: dict[str, Any], index: str, config: dict[str, Any], config_path: str | None, es: EsClient, default_analysis_mode: str) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    file_path = record.get("file_path") or record.get("video_path") or record.get("path") or record.get("virtual_path")
    if not file_path:
        return None, {"record": record, "error_code": "MISSING_VIDEO_PATH", "message": "file_path/video_path/path/virtual_path is required"}
    camera_id = str(record.get("camera_id") or input_value(config, "default_camera_id", default="CAM_DEERFLOW_001"))
    capture_seconds = record.get("capture_seconds") or config.get("default_capture_seconds") or 10
    metadata = enriched_metadata(record, str(file_path))
    started_at = record.get("started_at") or metadata.get("shooting_started_at")
    normalize_args = ["--video", str(file_path), "--camera-id", camera_id, "--source-type", str(record.get("source_type") or "local_file"), "--capture-seconds", str(int(float(capture_seconds)))]
    if started_at:
        normalize_args.extend(["--started-at", str(started_at)])
    normalize = run_skill("video-stream-ingestion", normalize_args)
    if normalize.get("status") != "success":
        return None, {"record": record, "error_code": normalize.get("error_code") or "NORMALIZE_FAILED", "message": normalize.get("message") or "video normalization failed", "detail": normalize}
    video_data = normalize.get("data", {})
    raw_uri = video_data.get("raw_segment_uri") or file_path
    analysis_mode = resolve_analysis_mode(record, default_analysis_mode)
    run_dir = writable_output_base(record, config) / "video-library" / stable_video_id(record, str(file_path))
    frames = []
    detections_result = {"status": "skipped"}
    detections: list[dict[str, Any]] = []
    tracks_result = {"status": "skipped"}
    tracks: list[dict[str, Any]] = []
    if analysis_mode == "object_detection":
        frames_args = ["--video", str(raw_uri), "--camera-id", camera_id, "--capture-seconds", str(int(float(capture_seconds))), "--interval-seconds", str(record.get("interval_seconds") or 1), "--output-dir", str(run_dir), "--output", str(run_dir / "frames.json")]
        frames_result = run_skill("frame-sampling", with_config(frames_args, config_path))
        frames = frames_result.get("data", {}).get("frames", []) if frames_result.get("status") == "success" else []
    if analysis_mode == "object_detection" and frames:
        labels = record.get("labels") or ["person", "car", "bus", "truck", "motorcycle", "bicycle"]
        detect_args = ["--frames-json", str(run_dir / "frames.json"), "--labels", ",".join(labels), "--provider", "ultralytics", "--output", str(run_dir / "detections.json")]
        detections_result = run_skill("object-detection", with_config(detect_args, config_path))
        detections = detections_result.get("data", {}).get("detections", []) if detections_result.get("status") == "success" else []
    if detections:
        tracks_result = run_skill("object-tracking", ["--detections-json", str(run_dir / "detections.json"), "--camera-id", camera_id, "--output", str(run_dir / "tracks.json")])
        tracks = tracks_result.get("data", {}).get("tracks", []) if tracks_result.get("status") == "success" else []
    labels, label_counts, total_objects = labels_from_detections(detections)
    record_labels = record.get("labels") if isinstance(record.get("labels"), list) else []
    search_terms = sorted({str(x) for x in [*record_labels, *labels, *flatten_text_values(record.get("tags") or [])] if str(x).strip()})
    file_path_tokens = path_tokens(str(file_path))
    video_id = stable_video_id(record, str(file_path))
    now = datetime.now(timezone.utc).isoformat()
    doc: dict[str, Any] = {
        "video_id": video_id,
        "camera_id": camera_id,
        "file_path": str(file_path),
        "raw_segment_uri": raw_uri,
        "filename": record.get("filename") or Path(str(file_path)).name,
        "source_type": record.get("source_type") or "local_file",
        "duration_seconds": video_data.get("duration_seconds") or metadata.get("duration_seconds"),
        "width": video_data.get("width") or metadata.get("width"),
        "height": video_data.get("height") or metadata.get("height"),
        "started_at": video_data.get("started_at") or metadata.get("shooting_started_at"),
        "ended_at": video_data.get("ended_at") or metadata.get("shooting_ended_at"),
        "location": record.get("location") or metadata.get("location") or {},
        "labels": labels,
        "event_types": record.get("event_types") or [],
        "ingestion_mode": analysis_mode,
        "content_detection_enabled": analysis_mode == "object_detection",
        "search_terms": search_terms,
        "path_tokens": file_path_tokens,
        "object_summary": {"total_objects": total_objects, "by_label": label_counts, "analysis_status": detections_result.get("status"), "detections": detections[:50]},
        "tracks_summary": summarize_tracks(tracks),
        "metadata": metadata,
        "created_at": now,
        "updated_at": now,
    }
    if isinstance(record.get("vector"), list):
        doc["vector"] = record["vector"]
    doc["content_text"] = build_content_text(record, doc)
    es.index_doc(index, video_id, doc)
    return {"video_id": video_id, "camera_id": camera_id, "filename": doc["filename"], "labels": labels, "analysis_status": doc["object_summary"]["analysis_status"], "ingestion_mode": analysis_mode, "content_detection_enabled": doc["content_detection_enabled"]}, None


def main() -> int:
    parser = argparse.ArgumentParser(description="Batch ingest videos into Elasticsearch.")
    parser.add_argument("--input")
    parser.add_argument("--manifest-json")
    parser.add_argument("--video-dir")
    parser.add_argument("--index", default=DEFAULT_INDEX)
    parser.add_argument("--config")
    parser.add_argument("--output")
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--analysis-mode", choices=["metadata_only", "object_detection"], default="metadata_only", help="metadata_only is fast and does not inspect frames; object_detection samples frames and runs YOLO/tracking, which is slower.")
    parser.add_argument("--skip-content-detection", action="store_true", help="Alias for --analysis-mode metadata_only.")
    args = parser.parse_args()
    input_data = load_structured(args.input) if args.input else {}
    input_data = input_data if isinstance(input_data, dict) else {}
    config = load_config(args.config)
    index = args.index or input_value(input_data, "index", default=DEFAULT_INDEX)
    analysis_mode = "metadata_only" if args.skip_content_detection else input_value(input_data, "analysis_mode", default=args.analysis_mode)
    records = resolve_records(input_data, args.manifest_json, args.video_dir)
    if not records:
        return emit(failed("MISSING_INPUT", "Provide --manifest-json, --video-dir, or input.videos"), args.output)
    try:
        es = EsClient(config)
        es.ensure_index(index)
    except EsError as exc:
        return emit(failed(exc.code, exc.message, exc.retryable, exc.detail), args.output)
    documents = []
    failures = []
    for record in records:
        try:
            doc, failure = ingest_one(record, index, config, args.config, es, str(analysis_mode))
            if doc:
                documents.append(doc)
            if failure:
                failures.append(failure)
        except EsError as exc:
            failures.append({"record": record, "error_code": exc.code, "message": exc.message, "detail": exc.detail})
        except Exception as exc:
            failures.append({"record": record, "error_code": "INGEST_FAILED", "message": str(exc)})
    try:
        if args.refresh:
            es.refresh(index)
    except EsError as exc:
        failures.append({"error_code": exc.code, "message": exc.message, "detail": exc.detail})
    data = {"ingested_count": len(documents), "failed_count": len(failures), "index": index, "documents": documents, "failures": failures}
    return emit(success(data, 1.0 if not failures else 0.75), args.output)


if __name__ == "__main__":
    raise SystemExit(main())
