import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path


def now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def stable_int(value, minimum=0, maximum=100):
    digest = hashlib.sha256(str(value).encode("utf-8")).hexdigest()
    number = int(digest[:8], 16)
    return minimum + number % (maximum - minimum + 1)


def load_json(path, default):
    if not path:
        return default
    file_path = Path(path)
    if not file_path.exists():
        return default
    try:
        with file_path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError):
        return default


def write_json(path, payload):
    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
    return str(file_path)


def media_metadata(source_uri):
    exists = os.path.exists(source_uri) if source_uri else False
    size = os.path.getsize(source_uri) if exists else 0
    seed = source_uri or "unknown"
    return {
        "source_uri": source_uri,
        "exists": exists,
        "size_bytes": size,
        "duration_seconds": stable_int(seed + ":duration", 30, 900),
        "fps": stable_int(seed + ":fps", 20, 30),
        "resolution": "1920x1080",
        "codec": "h264",
    }


def event(event_id, event_type, camera_id, severity="medium", confidence=0.82, timestamp=None):
    return {
        "event_id": event_id,
        "event_type": event_type,
        "camera_id": camera_id,
        "timestamp_start": timestamp or now_iso(),
        "timestamp_end": timestamp or now_iso(),
        "severity": severity,
        "confidence": round(confidence, 3),
        "location": {
            "grid_id": "grid-001",
            "road": "人民路",
            "longitude": 116.397 + stable_int(event_id, 0, 99) / 10000,
            "latitude": 39.904 + stable_int(event_id + "lat", 0, 99) / 10000,
        },
        "evidence": [],
    }


def base_response(skill_name, source_uri=None, status="success"):
    return {
        "task_id": f"{skill_name}-{stable_int(str(source_uri) + now_iso(), 1000, 9999)}",
        "skill_name": skill_name,
        "status": status,
        "created_at": now_iso(),
        "input": {
            "source_uri": source_uri,
        },
        "result": {},
        "quality": {
            "confidence": 0.0,
            "warnings": [],
        },
        "lineage": {
            "script": skill_name,
            "model_version": "rule-baseline-v1",
            "parameters": {},
        },
    }
