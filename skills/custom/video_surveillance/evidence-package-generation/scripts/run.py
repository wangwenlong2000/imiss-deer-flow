#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any
from uuid import uuid4

try:
    import yaml
except Exception:
    yaml = None

SKILL = "evidence-package-generation"
VERSION = "1.0.0"
DEFAULT_INDEX = "citybrain-video-library"


def require_video_index(index: str) -> str:
    if index != DEFAULT_INDEX:
        raise ValueError(f"Only {DEFAULT_INDEX} is allowed for video evidence lookup; received {index}")
    return index


class SkillInputError(Exception):
    """Input file could not be loaded; reported through the standard failure contract."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def argv_output() -> str | None:
    """Best-effort --output path so early input failures still honour it."""
    argv = sys.argv
    if "--output" in argv:
        index = argv.index("--output")
        if index + 1 < len(argv):
            return argv[index + 1]
    return None


def load_structured(path: str | None) -> Any:
    if not path:
        return {}
    source = Path(path)
    if not source.exists():
        raise SkillInputError("INPUT_NOT_FOUND", f"Input file not found: {path}")
    if source.is_dir():
        raise SkillInputError("INPUT_NOT_FOUND", f"Input path is a directory, not a file: {path}")
    try:
        text = source.read_text(encoding="utf-8")
    except OSError as exc:
        raise SkillInputError("INPUT_UNREADABLE", f"Could not read input file {path}: {exc}") from exc
    if source.suffix.lower() in {".yaml", ".yml"}:
        if yaml is None:
            raise SkillInputError("INPUT_INVALID_YAML", "PyYAML is required to read YAML files")
        try:
            return yaml.safe_load(text) or {}
        except Exception as exc:  # noqa: BLE001 - yaml raises library specific errors
            raise SkillInputError("INPUT_INVALID_YAML", f"Invalid YAML in {path}: {exc}") from exc
    if not text.strip():
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise SkillInputError("INPUT_INVALID_JSON", f"Invalid JSON in {path}: {exc}") from exc


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

    def get_video(self, index: str, video_id: str) -> dict[str, Any] | None:
        body = {"size": 1, "_source": {"excludes": ["vector"]}, "query": {"term": {"video_id": video_id}}}
        _, payload = self.request("POST", f"{index}/_search", body, ok={200})
        hits = payload.get("hits", {}).get("hits", [])
        return hits[0].get("_source", {}) if hits else None


def run_skill(skill: str, args: list[str]) -> dict[str, Any]:
    script = Path(__file__).resolve().parents[2] / skill / "scripts" / "run.py"
    proc = subprocess.run([sys.executable, str(script), *args], capture_output=True, text=True, check=False)
    text = proc.stdout.strip() or proc.stderr.strip()
    try:
        result = json.loads(text)
    except Exception:
        result = {"status": "failed", "error_code": "SCRIPT_OUTPUT_PARSE_FAILED", "message": text[-2000:], "returncode": proc.returncode}
    if proc.returncode != 0 and result.get("status") == "success":
        result["status"] = "failed"
        result["error_code"] = "SCRIPT_FAILED"
    return result


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def hash_file(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    return "sha256:" + sha256(path.read_bytes()).hexdigest()


def seconds_arg(value: float) -> str:
    return str(int(value)) if float(value).is_integer() else str(value)


def uri_to_path(uri: str | None) -> Path | None:
    if not uri:
        return None
    return Path(str(uri).removeprefix("file://"))


def ensure_artifact_hashes(artifacts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    for artifact in artifacts:
        result = artifact.get("result")
        data = result.get("data") if isinstance(result, dict) and isinstance(result.get("data"), dict) else {}
        uri = data.get("clip_uri") or data.get("uri")
        if uri and not data.get("hash"):
            path = uri_to_path(str(uri))
            if path:
                file_hash = hash_file(path)
                if file_hash:
                    data["hash"] = file_hash
    return artifacts


def summarize_evidence(evidence: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summary: list[dict[str, Any]] = []
    for item in evidence:
        for artifact in item.get("artifacts", []):
            result = artifact.get("result") if isinstance(artifact.get("result"), dict) else {}
            data = result.get("data") if isinstance(result.get("data"), dict) else {}
            uri = data.get("clip_uri") or data.get("uri")
            entry = {
                "video_id": item.get("video_id"),
                "event_id": item.get("event_id"),
                "type": artifact.get("type"),
                "status": artifact.get("status"),
                "uri": uri,
                "hash": data.get("hash"),
            }
            if data.get("start_time"):
                entry["start_time"] = data.get("start_time")
            if data.get("end_time"):
                entry["end_time"] = data.get("end_time")
            summary.append(entry)
    return summary


def event_from_input(input_data: dict[str, Any], event_json: str | None) -> dict[str, Any]:
    if event_json:
        data = load_structured(event_json)
        if isinstance(data, dict):
            return data.get("event") if isinstance(data.get("event"), dict) else data
    event = input_value(input_data, "event", default={})
    return event if isinstance(event, dict) else {}


def parse_elapsed_seconds(value: str | None) -> float | None:
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    if re.match(r"^\d+(?:\.\d+)?$", text):
        return float(text)
    parts = text.replace("：", ":").split(":")
    if len(parts) not in {2, 3}:
        return None
    try:
        numbers = [float(part) for part in parts]
    except ValueError:
        return None
    if len(numbers) == 2:
        minutes, seconds = numbers
        return minutes * 60 + seconds
    hours, minutes, seconds = numbers
    return hours * 3600 + minutes * 60 + seconds


def merge_cli_event(event: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    cli_event: dict[str, Any] = {}
    if args.event_id:
        cli_event["event_id"] = args.event_id
    if args.raw_segment_uri:
        cli_event["raw_segment_uri"] = args.raw_segment_uri
    if args.event_type:
        cli_event["event_type"] = args.event_type
    if args.camera_id:
        cli_event["camera_id"] = args.camera_id
    if args.event_elapsed_seconds is not None:
        cli_event["event_elapsed_seconds"] = args.event_elapsed_seconds
    elif args.event_time:
        elapsed = parse_elapsed_seconds(args.event_time)
        if elapsed is not None:
            cli_event["event_elapsed_seconds"] = elapsed
    if args.event_time and "event_elapsed_seconds" not in cli_event:
        cli_event["event_time"] = args.event_time
    if cli_event and "event_id" not in cli_event and "event_id" not in event:
        cli_event["event_id"] = f"EVT_{uuid4().hex[:8]}"
    if cli_event and "event_type" not in cli_event and "event_type" not in event:
        cli_event["event_type"] = "manual_evidence"
    return {**event, **cli_event}


def items_from_search(search_result_json: str | None, input_data: dict[str, Any]) -> list[dict[str, Any]]:
    data = load_structured(search_result_json) if search_result_json else input_value(input_data, "search_result", "search_results", default={})
    if isinstance(data, dict):
        hits = data.get("hits") or data.get("data", {}).get("hits") or []
    elif isinstance(data, list):
        hits = data
    else:
        hits = []
    return [hit for hit in hits if isinstance(hit, dict)]


def make_item(video: dict[str, Any] | None, event: dict[str, Any], hit: dict[str, Any] | None = None) -> dict[str, Any]:
    source = video or hit or {}
    event_id = event.get("event_id") or f"EVT_{uuid4().hex[:8]}"
    return {
        "event": {"event_id": event_id, **event},
        "video": source,
        "video_id": source.get("video_id") or event.get("video_id"),
        "raw_segment_uri": source.get("raw_segment_uri") or event.get("raw_segment_uri"),
        "camera_id": source.get("camera_id") or event.get("camera_id"),
    }


def generate_for_item(item: dict[str, Any], package_dir: Path, pre_seconds: float, post_seconds: float, config_path: str | None) -> list[dict[str, Any]]:
    event = item.get("event", {})
    event_id = event.get("event_id")
    artifacts: list[dict[str, Any]] = []
    item_dir = package_dir / "items" / str(event_id)
    event_path = item_dir / "event.json"
    frames_path = item_dir / "frames.json"
    frames = event.get("frames") if isinstance(event.get("frames"), list) else []
    if not frames and event.get("image_uri"):
        frames = [{"frame_id": event.get("evidence_frame_ids", ["frame_001"])[0] if event.get("evidence_frame_ids") else "frame_001", "image_uri": event.get("image_uri"), "timestamp": event.get("event_time") or event.get("start_time")}]
    write_json(event_path, event)
    write_json(frames_path, {"frames": frames})
    snapshot_args = ["--event-json", str(event_path), "--frames-json", str(frames_path), "--output-dir", str(item_dir), "--output", str(item_dir / "snapshot.json")]
    if config_path:
        snapshot_args.extend(["--config", config_path])
    snapshot = run_skill("evidence-snapshot", snapshot_args)
    artifacts.append({"type": "snapshot", "status": snapshot.get("status"), "result": snapshot})
    raw_segment_uri = item.get("raw_segment_uri")
    event_time = event.get("event_time") or event.get("start_time") or item.get("video", {}).get("started_at")
    if raw_segment_uri and not event_time and event.get("event_elapsed_seconds") is not None:
        event_time = datetime.now(timezone.utc).isoformat()
    if raw_segment_uri and event_time:
        clip_args = ["--event-id", str(event_id), "--raw-segment-uri", str(raw_segment_uri), "--event-time", str(event_time), "--pre-seconds", seconds_arg(pre_seconds), "--post-seconds", seconds_arg(post_seconds), "--output-dir", str(item_dir), "--output", str(item_dir / "clip.json")]
        if event.get("event_elapsed_seconds") is not None:
            clip_args.extend(["--event-elapsed-seconds", str(event["event_elapsed_seconds"])])
        if config_path:
            clip_args.extend(["--config", config_path])
        clip = run_skill("video-segment-extraction", clip_args)
        artifacts.append({"type": "clip", "status": clip.get("status"), "result": clip})
    return artifacts


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate a video evidence package.")
    parser.add_argument("--input")
    parser.add_argument("--index", default=DEFAULT_INDEX)
    parser.add_argument("--video-id")
    parser.add_argument("--event-json")
    parser.add_argument("--search-result-json")
    parser.add_argument("--raw-segment-uri")
    parser.add_argument("--event-id")
    parser.add_argument("--event-type")
    parser.add_argument("--event-time")
    parser.add_argument("--event-elapsed-seconds", type=float)
    parser.add_argument("--camera-id")
    parser.add_argument("--output-dir")
    parser.add_argument("--pre-seconds", type=float, default=2)
    parser.add_argument("--post-seconds", type=float, default=3)
    parser.add_argument("--config")
    parser.add_argument("--output")
    args = parser.parse_args()
    try:
        args.index = require_video_index(args.index)
    except ValueError as exc:
        return emit(failed("UNSUPPORTED_VIDEO_INDEX", str(exc), False, {"allowed_index": DEFAULT_INDEX, "received_index": args.index}), args.output)
    input_data = load_structured(args.input) if args.input else {}
    input_data = input_data if isinstance(input_data, dict) else {}
    config = load_config(args.config)
    package_id = input_value(input_data, "package_id", default=f"PKG_{uuid4().hex[:10]}")
    package_dir = Path(args.output_dir or input_value(input_data, "output_dir", default=str(Path("outputs") / "evidence-packages" / package_id)))
    package_dir.mkdir(parents=True, exist_ok=True)
    event = merge_cli_event(event_from_input(input_data, args.event_json), args)
    search_items = items_from_search(args.search_result_json, input_data)
    video_id = args.video_id or input_value(input_data, "video_id") or event.get("video_id")
    video = None
    event_has_source = bool(event.get("raw_segment_uri") or event.get("image_uri"))
    if video_id and not event_has_source:
        try:
            video = EsClient(config).get_video(args.index, str(video_id))
        except EsError as exc:
            return emit(failed(exc.code, exc.message, exc.retryable, exc.detail), args.output)
    if not video and video_id and not search_items and not event:
        return emit(failed("MISSING_INPUT", f"No indexed video found for video_id={video_id}"), args.output)
    items: list[dict[str, Any]] = []
    if search_items:
        for hit in search_items:
            items.append(make_item(None, event, hit))
    else:
        items.append(make_item(video, event))
    if not items:
        return emit(failed("MISSING_INPUT", "Provide --video-id, --event-json, or --search-result-json"), args.output)
    evidence = []
    for item in items:
        artifacts = generate_for_item(item, package_dir, args.pre_seconds, args.post_seconds, args.config)
        artifacts = ensure_artifact_hashes(artifacts)
        evidence.append({"video_id": item.get("video_id"), "event_id": item.get("event", {}).get("event_id"), "artifacts": artifacts})
    manifest = {"package_id": package_id, "created_at": datetime.now(timezone.utc).isoformat(), "index": args.index, "items": items, "evidence": evidence}
    manifest_path = package_dir / "manifest.json"
    write_json(manifest_path, manifest)
    data = {
        "package_id": package_id,
        "manifest_uri": str(manifest_path),
        "manifest_hash": hash_file(manifest_path),
        "items": items,
        "evidence": evidence,
        "artifact_summary": summarize_evidence(evidence),
    }
    return emit(success(data, 1.0), args.output)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SkillInputError as exc:
        raise SystemExit(
            emit(
                {
                    "skill": SKILL,
                    "version": VERSION,
                    "status": "failed",
                    "error_code": exc.code,
                    "message": exc.message,
                    "retryable": False,
                    "detail": {},
                },
                argv_output(),
            )
        )
