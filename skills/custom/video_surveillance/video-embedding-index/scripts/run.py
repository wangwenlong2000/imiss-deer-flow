#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import hashlib
import http.client
import json
import sys
import math
import os
import shutil
import subprocess
import tempfile
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import yaml
except Exception:
    yaml = None

SKILL = "video-embedding-index"
VERSION = "1.1.0"
DEFAULT_SOURCE_INDEX = "citybrain-video-library"
DEFAULT_TARGET_INDEX = DEFAULT_SOURCE_INDEX
DEFAULT_STORAGE_MODE = "in_place"
DEFAULT_OWNER = "huangxiao"
DEFAULT_STREETMODEL_BASE_URL = "http://219.245.185.245:3130"
DEFAULT_STREETMODEL_MODEL = "Qwen3-VL-Embedding-2B"
DEFAULT_STREETMODEL_VECTOR_FIELD = "video_vector-Qwen3-VL-Embedding-2B_urban_governance"
DEFAULT_STREETMODEL_DIMS = 2048
DEFAULT_STREETMODEL_BATCH_SIZE = 1
DEFAULT_STREETMODEL_TIMEOUT = 600
DEFAULT_STREETMODEL_VIDEO_INSTRUCTION = "Represent this surveillance video for urban scene retrieval."
DEFAULT_STREETMODEL_TEXT_INSTRUCTION = "Represent this surveillance/street-view query for urban scene retrieval."
DEFAULT_STREETMODEL_FRAME_INSTRUCTION = "Represent this surveillance video frame for urban scene retrieval."
DEFAULT_DEERFLOW_VIDEO_PREFIX = "/data/deerflow/videos"
DEFAULT_STREETMODEL_VIDEO_PREFIX = "/nfsdat2/home/xhuangslm/shared_videos"
DEFAULT_VIDEO_PREPROCESS_MODE = "image_frames"
DEFAULT_FRAME_SAMPLE_FPS = 1.0
DEFAULT_MAX_SAMPLED_FRAMES = 300
DEFAULT_IMAGE_FRAME_COUNT = 8
DEFAULT_PROXY_OUTPUT_FPS = 4.0
DEFAULT_PROXY_MAX_WIDTH = 768
DEFAULT_PROXY_OUTPUT_SUBDIR = "embedding_proxies"
PROXY_ENV_KEYS = ("http_proxy", "https_proxy", "all_proxy", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY")


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


class EmbeddingError(RuntimeError):
    def __init__(self, code: str, message: str, retryable: bool = False, detail: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable
        self.detail = detail or {}


class VideoIndexPolicyError(ValueError):
    def __init__(self, code: str, message: str, detail: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
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
        ok = ok or {200, 201}
        data = None if body is None else json.dumps(body, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(f"{self.url}/{path.lstrip('/')}", data=data, method=method, headers={"Content-Type": "application/json"})
        if self.username and self.password:
            token = base64.b64encode(f"{self.username}:{self.password}".encode("utf-8")).decode("ascii")
            req.add_header("Authorization", f"Basic {token}")
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                text = resp.read().decode("utf-8", errors="replace")
                return resp.status, json.loads(text) if text.strip() else {}
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

    def vector_excludes(self, vector_field: str) -> list[str]:
        excludes = ["vector"]
        if vector_field != "vector":
            excludes.append(vector_field)
        return excludes

    def mapping_properties(self, index: str) -> dict[str, Any]:
        _, mapping = self.request("GET", f"{index}/_mapping", ok={200})
        return next(iter(mapping.values()), {}).get("mappings", {}).get("properties", {})

    def ensure_vector_field(self, index: str, vector_field: str, dims: int) -> None:
        props = self.mapping_properties(index)
        field_mapping = props.get(vector_field)
        if field_mapping is None:
            body = {
                "properties": {
                    vector_field: {"type": "dense_vector", "dims": dims, "index": True, "similarity": "cosine"}
                }
            }
            self.request("PUT", f"{index}/_mapping", body, ok={200})
            return
        if field_mapping.get("type") != "dense_vector" or int(field_mapping.get("dims") or 0) != dims:
            raise EsError(
                "VECTOR_FIELD_DIMENSION_MISMATCH",
                f"Vector field {vector_field} in {index} is not a {dims}-dim dense_vector",
                False,
                {"index": index, "vector_field": vector_field, "expected_dims": dims, "mapping": field_mapping},
            )

    def embedding_properties(self, vector_field: str, dims: int) -> dict[str, Any]:
        return {
            "embedding_owner": {"type": "keyword"},
            "embedding_status": {"type": "keyword"},
            "embedding_provider": {"type": "keyword"},
            "embedding_model": {"type": "keyword"},
            "embedding_vector_field": {"type": "keyword"},
            "embedding_text": {"type": "text"},
            "embedding_updated_at": {"type": "date"},
            "embedding_error": {"type": "object", "enabled": False},
            "deerflow_video_path": {"type": "keyword"},
            "video_embedding_uri": {"type": "keyword"},
            "video_embedding_source_uri": {"type": "keyword"},
            "video_embedding_proxy_uri": {"type": "keyword"},
            "video_embedding_proxy_path": {"type": "keyword"},
            "video_embedding_preprocess_mode": {"type": "keyword"},
            "video_embedding_input_type": {"type": "keyword"},
            "video_embedding_frame_aggregation": {"type": "keyword"},
            "video_embedding_frame_media_type": {"type": "keyword"},
            "video_embedding_source_duration_seconds": {"type": "float"},
            "video_embedding_sample_fps": {"type": "float"},
            "video_embedding_effective_sample_fps": {"type": "float"},
            "video_embedding_sampled_frames": {"type": "integer"},
            "video_embedding_dimensions": {"type": "integer"},
            vector_field: {"type": "dense_vector", "dims": dims, "index": True, "similarity": "cosine"},
        }

    def ensure_embedding_fields(self, index: str, vector_field: str, dims: int) -> None:
        self.ensure_vector_field(index, vector_field, dims)
        props = self.mapping_properties(index)
        additions = {
            name: mapping
            for name, mapping in self.embedding_properties(vector_field, dims).items()
            if name not in props and name != vector_field
        }
        if additions:
            self.request("PUT", f"{index}/_mapping", {"properties": additions}, ok={200})

    def ensure_index(self, index: str, dims: int, vector_field: str = "vector") -> None:
        status, _ = self.request("HEAD", index, ok={200, 404})
        if status == 200:
            self.ensure_embedding_fields(index, vector_field, dims)
            return
        vector_property = {"type": "dense_vector", "dims": dims, "index": True, "similarity": "cosine"}
        mapping = {
            "settings": {"number_of_shards": 1, "number_of_replicas": 0},
            "mappings": {
                "properties": {
                    "owner": {"type": "keyword"},
                    "video_id": {"type": "keyword"},
                    "camera_id": {"type": "keyword"},
                    "filename": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "raw_segment_uri": {"type": "keyword"},
                    "location": {"type": "object", "enabled": True},
                    "labels": {"type": "keyword"},
                    "content_text": {"type": "text"},
                    "embedding_text": {"type": "text"},
                    "embedding_model": {"type": "keyword"},
                    "embedding_provider": {"type": "keyword"},
                    "embedding_owner": {"type": "keyword"},
                    "embedding_status": {"type": "keyword"},
                    "embedding_vector_field": {"type": "keyword"},
                    "embedding_updated_at": {"type": "date"},
                    "embedding_error": {"type": "object", "enabled": False},
                    "deerflow_video_path": {"type": "keyword"},
                    "object_summary": {"type": "object", "enabled": True},
                    "tracks_summary": {"type": "object", "enabled": True},
                    "metadata": {"type": "object", "enabled": True},
                    "created_at": {"type": "date"},
                    "updated_at": {"type": "date"},
                    "video_embedding_uri": {"type": "keyword"},
                    "video_embedding_source_uri": {"type": "keyword"},
                    "video_embedding_proxy_uri": {"type": "keyword"},
                    "video_embedding_proxy_path": {"type": "keyword"},
                    "video_embedding_preprocess_mode": {"type": "keyword"},
                    "video_embedding_input_type": {"type": "keyword"},
                    "video_embedding_frame_aggregation": {"type": "keyword"},
                    "video_embedding_frame_media_type": {"type": "keyword"},
                    "video_embedding_source_duration_seconds": {"type": "float"},
                    "video_embedding_sample_fps": {"type": "float"},
                    "video_embedding_effective_sample_fps": {"type": "float"},
                    "video_embedding_sampled_frames": {"type": "integer"},
                    "video_embedding_dimensions": {"type": "integer"},
                    "vector": vector_property,
                }
            },
        }
        if vector_field != "vector":
            mapping["mappings"]["properties"][vector_field] = vector_property
        self.request("PUT", index, mapping, ok={200, 201})

    def fetch_docs(self, index: str, limit: int, video_id: str | None = None, vector_field: str = "vector") -> list[dict[str, Any]]:
        filters = []
        if video_id:
            filters.append({"term": {"video_id": video_id}})
        body = {
            "size": limit,
            "_source": {"excludes": self.vector_excludes(vector_field)},
            "query": {"bool": {"filter": filters, "must": [{"match_all": {}}]}},
            "sort": [{"updated_at": {"order": "desc", "unmapped_type": "date"}}],
        }
        _, payload = self.request("POST", f"{index}/_search", body, ok={200})
        return [hit.get("_source", {}) for hit in payload.get("hits", {}).get("hits", [])]

    def upsert_doc(self, index: str, doc_id: str, doc: dict[str, Any]) -> None:
        self.request("PUT", f"{index}/_doc/{doc_id}", doc, ok={200, 201})

    def update_doc(self, index: str, doc_id: str, doc: dict[str, Any], doc_as_upsert: bool = False) -> None:
        body: dict[str, Any] = {"doc": doc}
        if doc_as_upsert:
            body["doc_as_upsert"] = True
        self.request("POST", f"{index}/_update/{doc_id}", body, ok={200, 201})

    def refresh(self, index: str) -> None:
        self.request("POST", f"{index}/_refresh", None, ok={200})


def write_json_file(path: str | None, payload: dict[str, Any]) -> None:
    if not path:
        return
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def streetmodel_config(args: argparse.Namespace, config: dict[str, Any]) -> dict[str, Any]:
    cfg = config.get("streetmodel_embedding", {}) if isinstance(config.get("streetmodel_embedding"), dict) else {}
    video_cfg = cfg.get("video", {}) if isinstance(cfg.get("video"), dict) else {}
    return {
        "base_url": str(args.base_url or os.getenv("STREETMODEL_BASE_URL") or cfg.get("base_url") or DEFAULT_STREETMODEL_BASE_URL).rstrip("/"),
        "model_name": str(args.embedding_model or cfg.get("model_name") or DEFAULT_STREETMODEL_MODEL),
        "dimensions": int(args.dimensions or cfg.get("dimensions") or DEFAULT_STREETMODEL_DIMS),
        "batch_size": int(cfg.get("batch_size") or DEFAULT_STREETMODEL_BATCH_SIZE),
        "timeout_seconds": int(args.timeout_seconds or cfg.get("timeout_seconds") or DEFAULT_STREETMODEL_TIMEOUT),
        "video_instruction": str(cfg.get("instruction") or DEFAULT_STREETMODEL_VIDEO_INSTRUCTION),
        "text_instruction": str(cfg.get("text_instruction") or DEFAULT_STREETMODEL_TEXT_INSTRUCTION),
        "frame_instruction": str(cfg.get("frame_instruction") or cfg.get("image_instruction") or DEFAULT_STREETMODEL_FRAME_INSTRUCTION),
        "deerflow_path_prefix": str(args.deerflow_path_prefix or video_cfg.get("deerflow_path_prefix") or DEFAULT_DEERFLOW_VIDEO_PREFIX),
        "streetmodel_path_prefix": str(args.streetmodel_path_prefix or video_cfg.get("streetmodel_path_prefix") or DEFAULT_STREETMODEL_VIDEO_PREFIX),
    }


def _coerce_float(value: Any, default: float) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _coerce_int(value: Any, default: int) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def normalize_video_preprocess_mode(mode: str | None, enabled: bool = True) -> str:
    if not enabled:
        return "none"
    normalized = str(mode or DEFAULT_VIDEO_PREPROCESS_MODE).strip().lower().replace("-", "_")
    if normalized in {"", "false", "off", "disabled", "none"}:
        return "none"
    if normalized in {"image_frames", "imageframes", "frames", "images"}:
        return "image_frames"
    if normalized in {"frame_proxy", "frameproxy", "proxy"}:
        return "frame_proxy"
    raise EmbeddingError(
        "UNSUPPORTED_VIDEO_PREPROCESS_MODE",
        f"Unsupported video preprocess mode: {mode}",
        False,
        {"mode": mode, "supported_modes": ["image_frames", "frame_proxy", "none"]},
    )


def video_preprocess_config(args: argparse.Namespace, config: dict[str, Any]) -> dict[str, Any]:
    cfg = config.get("streetmodel_embedding", {}) if isinstance(config.get("streetmodel_embedding"), dict) else {}
    pre_cfg = cfg.get("video_preprocess", {}) if isinstance(cfg.get("video_preprocess"), dict) else {}
    enabled = bool(pre_cfg.get("enabled", True))
    mode_arg = getattr(args, "video_preprocess", None)
    mode = normalize_video_preprocess_mode(mode_arg or pre_cfg.get("mode"), enabled if mode_arg is None else True)
    return {
        "mode": mode,
        "sample_fps": max(0.001, _coerce_float(getattr(args, "frame_sample_fps", None), _coerce_float(pre_cfg.get("sample_fps"), DEFAULT_FRAME_SAMPLE_FPS))),
        "max_sampled_frames": max(1, _coerce_int(getattr(args, "max_sampled_frames", None), _coerce_int(pre_cfg.get("max_sampled_frames"), DEFAULT_MAX_SAMPLED_FRAMES))),
        "image_frame_count": max(1, _coerce_int(getattr(args, "image_frame_count", None), _coerce_int(pre_cfg.get("image_frame_count"), DEFAULT_IMAGE_FRAME_COUNT))),
        "proxy_output_fps": max(0.001, _coerce_float(getattr(args, "proxy_output_fps", None), _coerce_float(pre_cfg.get("proxy_output_fps"), DEFAULT_PROXY_OUTPUT_FPS))),
        "proxy_max_width": max(16, _coerce_int(getattr(args, "proxy_max_width", None), _coerce_int(pre_cfg.get("max_width"), DEFAULT_PROXY_MAX_WIDTH))),
        "output_subdir": str(pre_cfg.get("output_subdir") or DEFAULT_PROXY_OUTPUT_SUBDIR).strip("/ ") or DEFAULT_PROXY_OUTPUT_SUBDIR,
    }


def vector_field_config(args: argparse.Namespace, config: dict[str, Any], provider: str) -> str:
    cfg = config.get("streetmodel_embedding", {}) if isinstance(config.get("streetmodel_embedding"), dict) else {}
    if args.vector_field:
        return str(args.vector_field)
    if provider == "streetmodel":
        return str(cfg.get("vector_field") or DEFAULT_STREETMODEL_VECTOR_FIELD)
    return "vector"


def to_streetmodel_video_uri(video_path: str, deerflow_prefix: str, streetmodel_prefix: str) -> str:
    value = str(video_path).strip()
    if not value:
        raise EmbeddingError("MISSING_VIDEO_URI", "Video document does not contain a usable video path")
    if value.startswith(streetmodel_prefix):
        return value
    if value.startswith(deerflow_prefix):
        return value.replace(deerflow_prefix, streetmodel_prefix, 1)
    raise EmbeddingError(
        "VIDEO_URI_NOT_MAPPABLE",
        f"Video path is not under {deerflow_prefix} or {streetmodel_prefix}",
        False,
        {"video_path": value, "deerflow_path_prefix": deerflow_prefix, "streetmodel_path_prefix": streetmodel_prefix},
    )


def safe_video_copy_name(video_path: str, video_id: str | None = None) -> str:
    source = Path(video_path)
    stem_source = video_id or source.stem or "video"
    safe_stem = "".join(ch if ch.isascii() and (ch.isalnum() or ch in "-_.") else "_" for ch in stem_source)
    safe_stem = safe_stem.strip("._") or "video"
    suffix = source.suffix.lower() or ".mp4"
    digest_source = str(source.resolve()) if source.exists() else str(source)
    digest = hashlib.sha1(digest_source.encode("utf-8")).hexdigest()[:12]
    return f"{safe_stem}-{digest}{suffix}"


def materialize_streetmodel_video_uri(
    video_path: str,
    deerflow_prefix: str,
    streetmodel_prefix: str,
    *,
    copy_to_shared: bool = False,
    video_id: str | None = None,
) -> tuple[str, str | None]:
    try:
        return to_streetmodel_video_uri(video_path, deerflow_prefix, streetmodel_prefix), None
    except EmbeddingError as exc:
        if exc.code != "VIDEO_URI_NOT_MAPPABLE" or not copy_to_shared:
            raise

    source = Path(video_path).expanduser()
    if not source.is_file():
        raise EmbeddingError(
            "VIDEO_SOURCE_NOT_FOUND",
            f"Cannot copy video to shared directory because local source was not found: {video_path}",
            False,
            {"video_path": video_path},
        )

    target = Path(deerflow_prefix) / "direct_uploads" / safe_video_copy_name(video_path, video_id)
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
    except OSError as copy_exc:
        raise EmbeddingError(
            "VIDEO_COPY_TO_SHARED_FAILED",
            f"Failed to copy video to DeerFlow shared prefix {deerflow_prefix}: {copy_exc}",
            False,
            {"video_path": str(source), "target_path": str(target), "deerflow_path_prefix": deerflow_prefix},
        ) from copy_exc

    return to_streetmodel_video_uri(str(target), deerflow_prefix, streetmodel_prefix), str(target)


def local_video_source_path(
    original_video_path: str,
    model_video_uri: str,
    deerflow_prefix: str,
    streetmodel_prefix: str,
    copied_video_path: str | None = None,
) -> Path:
    candidates: list[str] = []
    if copied_video_path:
        candidates.append(copied_video_path)
    candidates.append(str(original_video_path).removeprefix("file://"))
    candidates.append(str(model_video_uri).removeprefix("file://"))
    if str(model_video_uri).startswith(streetmodel_prefix):
        candidates.append(str(model_video_uri).replace(streetmodel_prefix, deerflow_prefix, 1))

    seen: set[str] = set()
    for candidate in candidates:
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        path = Path(candidate).expanduser()
        if path.is_file():
            return path

    raise EmbeddingError(
        "VIDEO_PREPROCESS_SOURCE_NOT_READABLE",
        "Video preprocessing requires a local readable source file before calling StreetModel",
        False,
        {
            "video_path": original_video_path,
            "model_video_uri": model_video_uri,
            "deerflow_path_prefix": deerflow_prefix,
            "streetmodel_path_prefix": streetmodel_prefix,
            "checked_paths": list(seen),
        },
    )


def _parse_float(value: Any) -> float | None:
    if value in (None, "", "N/A"):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and number > 0 else None


def probe_video_metadata(path: Path) -> dict[str, Any]:
    if not shutil.which("ffprobe"):
        raise EmbeddingError("FFPROBE_MISSING", "ffprobe is required for video embedding preprocessing", False)
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-show_entries",
        "stream=width,height,nb_frames:format=duration",
        "-of",
        "json",
        str(path),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise EmbeddingError(
            "FFPROBE_FAILED",
            "ffprobe failed to read video metadata",
            True,
            {"video_path": str(path), "stderr": result.stderr[-2000:]},
        )
    try:
        payload = json.loads(result.stdout or "{}")
    except json.JSONDecodeError as exc:
        raise EmbeddingError(
            "FFPROBE_INVALID_RESPONSE",
            "ffprobe returned invalid JSON",
            True,
            {"video_path": str(path), "stdout": result.stdout[-2000:]},
        ) from exc
    stream = (payload.get("streams") or [{}])[0] if isinstance(payload.get("streams"), list) else {}
    metadata = {
        "duration_seconds": _parse_float((payload.get("format") or {}).get("duration")),
        "width": _coerce_int(stream.get("width"), 0) or None,
        "height": _coerce_int(stream.get("height"), 0) or None,
    }
    nb_frames = _parse_float(stream.get("nb_frames"))
    if nb_frames is not None:
        metadata["frame_count"] = int(nb_frames)
    return metadata


def safe_path_stem(value: str | None, fallback: str = "video") -> str:
    raw = value or fallback
    safe = "".join(ch if ch.isascii() and (ch.isalnum() or ch in "-_.") else "_" for ch in str(raw))
    return safe.strip("._") or fallback


def format_ffmpeg_float(value: float) -> str:
    text = f"{float(value):.6f}".rstrip("0").rstrip(".")
    return text or "0"


def build_proxy_paths(
    source_path: Path,
    video_id: str | None,
    deerflow_prefix: str,
    preprocess_cfg: dict[str, Any],
) -> Path:
    stem = safe_path_stem(video_id or source_path.stem)
    digest_source = "|".join(
        [
            str(source_path.resolve()) if source_path.exists() else str(source_path),
            str(source_path.stat().st_mtime_ns if source_path.exists() else ""),
            str(preprocess_cfg.get("sample_fps")),
            str(preprocess_cfg.get("max_sampled_frames")),
            str(preprocess_cfg.get("proxy_output_fps")),
            str(preprocess_cfg.get("proxy_max_width")),
        ]
    )
    digest = hashlib.sha1(digest_source.encode("utf-8")).hexdigest()[:12]
    return Path(deerflow_prefix) / str(preprocess_cfg["output_subdir"]) / f"{stem}-{digest}" / "proxy.mp4"


def create_frame_proxy_video(
    source_path: Path,
    video_id: str | None,
    street_cfg: dict[str, Any],
    preprocess_cfg: dict[str, Any],
) -> dict[str, Any]:
    if not shutil.which("ffmpeg"):
        raise EmbeddingError("FFMPEG_MISSING", "ffmpeg is required for video embedding preprocessing", False)

    metadata = probe_video_metadata(source_path)
    duration = metadata.get("duration_seconds")
    sample_fps = float(preprocess_cfg["sample_fps"])
    max_frames = int(preprocess_cfg["max_sampled_frames"])
    effective_fps = sample_fps
    if duration:
        estimated = max(1, math.ceil(duration * sample_fps))
        if estimated > max_frames:
            effective_fps = max_frames / duration
    sampled_frames = max(1, math.ceil(duration * effective_fps)) if duration else None

    proxy_path = build_proxy_paths(source_path, video_id, street_cfg["deerflow_path_prefix"], preprocess_cfg)
    try:
        proxy_path.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise EmbeddingError(
            "VIDEO_PROXY_OUTPUT_UNAVAILABLE",
            f"Could not create video embedding proxy directory: {proxy_path.parent}",
            False,
            {"proxy_path": str(proxy_path), "error": str(exc)},
        ) from exc

    fps_value = format_ffmpeg_float(effective_fps)
    output_fps_value = format_ffmpeg_float(float(preprocess_cfg["proxy_output_fps"]))
    max_width = int(preprocess_cfg["proxy_max_width"])
    vf = f"fps={fps_value},scale='min({max_width},iw)':-2,setpts=N/{output_fps_value}/TB"
    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        str(source_path),
        "-vf",
        vf,
        "-an",
        "-r",
        output_fps_value,
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "28",
        "-pix_fmt",
        "yuv420p",
        "-movflags",
        "+faststart",
        str(proxy_path),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0 or not proxy_path.is_file():
        raise EmbeddingError(
            "VIDEO_PROXY_GENERATION_FAILED",
            "ffmpeg failed to create video embedding frame proxy",
            True,
            {"video_path": str(source_path), "proxy_path": str(proxy_path), "stderr": result.stderr[-2000:]},
        )

    proxy_uri = to_streetmodel_video_uri(
        str(proxy_path),
        street_cfg["deerflow_path_prefix"],
        street_cfg["streetmodel_path_prefix"],
    )
    return {
        "mode": "frame_proxy",
        "video_uri": proxy_uri,
        "proxy_deerflow_path": str(proxy_path),
        "proxy_uri": proxy_uri,
        "source_duration_seconds": duration,
        "source_width": metadata.get("width"),
        "source_height": metadata.get("height"),
        "sample_fps": sample_fps,
        "effective_sample_fps": round(effective_fps, 6),
        "sampled_frames": sampled_frames,
        "max_sampled_frames": max_frames,
        "proxy_output_fps": float(preprocess_cfg["proxy_output_fps"]),
        "proxy_max_width": max_width,
    }


def embed_video_as_image_frames(
    source_path: Path,
    embedder: Any,
    preprocess_cfg: dict[str, Any],
) -> tuple[list[float], dict[str, Any]]:
    if not shutil.which("ffmpeg"):
        raise EmbeddingError("FFMPEG_MISSING", "ffmpeg is required to extract video frames for image embedding", False)

    metadata = probe_video_metadata(source_path)
    duration = metadata.get("duration_seconds")
    sample_fps = float(preprocess_cfg["sample_fps"])
    requested_frames = int(preprocess_cfg["image_frame_count"])
    frame_count = requested_frames
    effective_fps = sample_fps
    if duration:
        frame_count = min(requested_frames, max(1, math.ceil(duration * sample_fps)))
        effective_fps = frame_count / duration

    with tempfile.TemporaryDirectory(prefix="video-image-frames-") as temp_dir:
        output_pattern = Path(temp_dir) / "frame-%04d.jpg"
        fps_value = format_ffmpeg_float(effective_fps)
        max_width = int(preprocess_cfg["proxy_max_width"])
        vf = f"fps={fps_value},scale='min({max_width},iw)':-2"
        cmd = [
            "ffmpeg",
            "-y",
            "-i",
            str(source_path),
            "-vf",
            vf,
            "-frames:v",
            str(frame_count),
            "-q:v",
            "3",
            str(output_pattern),
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        frame_paths = sorted(Path(temp_dir).glob("frame-*.jpg"))
        if result.returncode != 0:
            raise EmbeddingError(
                "VIDEO_FRAME_EXTRACTION_FAILED",
                "ffmpeg failed to extract image frames for video embedding",
                True,
                {"video_path": str(source_path), "stderr": result.stderr[-2000:]},
            )
        if not frame_paths:
            raise EmbeddingError(
                "VIDEO_FRAME_EXTRACTION_EMPTY",
                "No image frames were extracted from the video",
                False,
                {"video_path": str(source_path), "requested_frames": frame_count},
            )
        image_bytes = [path.read_bytes() for path in frame_paths]
        vector = embedder.encode_image_bytes(image_bytes)

    prepared = {
        "mode": "image_frames",
        "input_type": "sampled_images",
        "source_uri": str(source_path),
        "video_uri": None,
        "proxy_uri": None,
        "proxy_deerflow_path": None,
        "source_duration_seconds": duration,
        "source_width": metadata.get("width"),
        "source_height": metadata.get("height"),
        "sample_fps": sample_fps,
        "effective_sample_fps": round(effective_fps, 6),
        "sampled_frames": len(image_bytes),
        "requested_frames": requested_frames,
        "frame_aggregation": "normalized_mean",
        "frame_media_type": "image/jpeg",
    }
    return vector, prepared


def prepare_streetmodel_embedding_video(
    original_video_path: str,
    model_video_uri: str,
    copied_video_path: str | None,
    street_cfg: dict[str, Any],
    preprocess_cfg: dict[str, Any],
    video_id: str | None = None,
) -> dict[str, Any]:
    if preprocess_cfg.get("mode") == "none":
        return {
            "mode": "none",
            "video_uri": model_video_uri,
            "source_uri": model_video_uri,
            "proxy_uri": None,
            "proxy_deerflow_path": None,
        }

    source_path = local_video_source_path(
        original_video_path,
        model_video_uri,
        street_cfg["deerflow_path_prefix"],
        street_cfg["streetmodel_path_prefix"],
        copied_video_path,
    )
    prepared = create_frame_proxy_video(source_path, video_id, street_cfg, preprocess_cfg)
    prepared["source_uri"] = model_video_uri
    return prepared


def apply_video_embedding_metadata(
    doc: dict[str, Any],
    prepared_video: dict[str, Any],
    copied_video_path: str | None = None,
) -> None:
    doc["video_embedding_uri"] = prepared_video.get("video_uri")
    doc["video_embedding_source_uri"] = prepared_video.get("source_uri") or prepared_video.get("video_uri")
    doc["video_embedding_proxy_uri"] = prepared_video.get("proxy_uri")
    doc["video_embedding_proxy_path"] = prepared_video.get("proxy_deerflow_path")
    doc["video_embedding_preprocess_mode"] = prepared_video.get("mode", "none")
    doc["video_embedding_input_type"] = prepared_video.get("input_type") or "video"
    doc["video_embedding_frame_aggregation"] = prepared_video.get("frame_aggregation")
    doc["video_embedding_frame_media_type"] = prepared_video.get("frame_media_type")
    if prepared_video.get("source_duration_seconds") is not None:
        doc["video_embedding_source_duration_seconds"] = prepared_video["source_duration_seconds"]
    if prepared_video.get("sample_fps") is not None:
        doc["video_embedding_sample_fps"] = prepared_video["sample_fps"]
    if prepared_video.get("effective_sample_fps") is not None:
        doc["video_embedding_effective_sample_fps"] = prepared_video["effective_sample_fps"]
    if prepared_video.get("sampled_frames") is not None:
        doc["video_embedding_sampled_frames"] = prepared_video["sampled_frames"]
    if copied_video_path:
        doc["deerflow_video_path"] = copied_video_path
    elif prepared_video.get("mode") == "image_frames":
        doc["deerflow_video_path"] = None


def source_video_path(doc: dict[str, Any]) -> str:
    for key in ("raw_segment_uri", "file_path", "video_path", "path"):
        value = doc.get(key)
        if isinstance(value, str) and value.strip():
            return value
    raise EmbeddingError("MISSING_VIDEO_URI", "Video document does not contain raw_segment_uri, file_path, video_path, or path")


class StreetModelEmbedder:
    def __init__(
        self,
        base_url: str,
        model: str,
        dims: int,
        batch_size: int,
        timeout_seconds: int,
        video_instruction: str,
        text_instruction: str,
        frame_instruction: str,
    ):
        self.provider = "streetmodel"
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.dims = dims
        self.batch_size = batch_size
        self.timeout_seconds = timeout_seconds
        self.video_instruction = video_instruction
        self.text_instruction = text_instruction
        self.frame_instruction = frame_instruction
        self.validate_service()

    def request(self, method: str, path: str, body: Any | None = None) -> Any:
        data = None if body is None else json.dumps(body, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(f"{self.base_url}/{path.lstrip('/')}", data=data, method=method)
        if body is not None:
            req.add_header("Content-Type", "application/json")
        try:
            with streetmodel_urlopen(req, self.timeout_seconds) as resp:
                text = resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            text = exc.read().decode("utf-8", errors="replace")
            raise EmbeddingError(
                "STREETMODEL_REQUEST_FAILED",
                f"StreetModel request failed: HTTP {exc.code}",
                exc.code >= 500,
                {"path": path, "response": text[-2000:]},
            )
        except urllib.error.URLError as exc:
            raise EmbeddingError(
                "STREETMODEL_CONNECTION_FAILED",
                f"Could not connect to StreetModel at {self.base_url}: {exc.reason}",
                True,
                {"base_url": self.base_url},
            )
        except (http.client.HTTPException, OSError) as exc:
            raise EmbeddingError(
                "STREETMODEL_CONNECTION_FAILED",
                f"Could not connect to StreetModel at {self.base_url}: {exc}",
                True,
                {"base_url": self.base_url},
            )
        try:
            return json.loads(text) if text.strip() else {}
        except json.JSONDecodeError:
            return {"raw": text}

    def validate_service(self) -> None:
        self.request("GET", "/health")
        models_payload = self.request("GET", "/models")
        models = extract_models(models_payload)
        for item in models:
            if item.get("name") == self.model and item.get("exists", True):
                return
        raise EmbeddingError(
            "STREETMODEL_MODEL_NOT_FOUND",
            f"StreetModel model {self.model} was not reported by /models",
            False,
            {"base_url": self.base_url, "models": models_payload},
        )

    def embed_items(self, items: list[dict[str, Any]], instruction: str) -> list[list[float]]:
        if not items:
            raise EmbeddingError("MISSING_EMBEDDING_ITEMS", "At least one StreetModel embedding item is required")
        payload = {
            "model_name": self.model,
            "instruction": instruction,
            "items": items,
            "batch_size": self.batch_size,
        }
        data = self.request("POST", "/embed", payload)
        shape = data.get("shape")
        embeddings = data.get("embeddings")
        expected_shape = [len(items), self.dims]
        if shape != expected_shape or not isinstance(embeddings, list) or len(embeddings) != len(items):
            raise EmbeddingError(
                "STREETMODEL_INVALID_RESPONSE",
                f"StreetModel returned an invalid embedding shape: {shape}",
                False,
                {"expected_shape": expected_shape, "response": data},
            )
        vectors: list[list[float]] = []
        for vector in embeddings:
            if not isinstance(vector, list) or len(vector) != self.dims:
                raise EmbeddingError(
                    "STREETMODEL_INVALID_RESPONSE",
                    f"StreetModel returned vector length {len(vector) if isinstance(vector, list) else 'non-list'}",
                    False,
                    {"expected_dimensions": self.dims, "shape": shape},
                )
            vectors.append([float(x) for x in vector])
        return vectors

    def embed_item(self, item: dict[str, Any], instruction: str) -> list[float]:
        return self.embed_items([item], instruction)[0]

    def encode_text(self, text: str) -> list[float]:
        return self.embed_item({"type": "text", "content": text}, self.text_instruction)

    def encode_video_uri(self, video_uri: str) -> list[float]:
        return self.embed_item({"type": "video", "uri": video_uri}, self.video_instruction)

    def encode_image_bytes(self, images: list[bytes]) -> list[float]:
        items = [
            {
                "type": "image_base64",
                "data": base64.b64encode(image).decode("ascii"),
                "encoding": "base64",
                "media_type": "image/jpeg",
            }
            for image in images
            if image
        ]
        vectors = self.embed_items(items, self.frame_instruction)
        mean_vector = [sum(vector[index] for vector in vectors) / len(vectors) for index in range(self.dims)]
        return normalize(mean_vector)


def embed_streetmodel_video_source(
    original_video_path: str,
    embedder: StreetModelEmbedder,
    street_cfg: dict[str, Any],
    preprocess_cfg: dict[str, Any],
    *,
    copy_to_shared: bool = False,
    video_id: str | None = None,
) -> tuple[list[float], dict[str, Any], str | None]:
    if preprocess_cfg.get("mode") == "image_frames":
        source_path = Path(str(original_video_path).removeprefix("file://")).expanduser()
        if not source_path.is_file():
            raise EmbeddingError(
                "VIDEO_SOURCE_NOT_FOUND",
                f"Video source was not found for image-frame embedding: {original_video_path}",
                False,
                {"video_path": original_video_path},
            )
        vector, prepared_video = embed_video_as_image_frames(source_path, embedder, preprocess_cfg)
        return vector, prepared_video, None

    video_uri, copied_video_path = materialize_streetmodel_video_uri(
        original_video_path,
        street_cfg["deerflow_path_prefix"],
        street_cfg["streetmodel_path_prefix"],
        copy_to_shared=copy_to_shared,
        video_id=video_id,
    )
    prepared_video = prepare_streetmodel_embedding_video(
        original_video_path,
        video_uri,
        copied_video_path,
        street_cfg,
        preprocess_cfg,
        video_id,
    )
    vector = embedder.encode_video_uri(prepared_video["video_uri"])
    return vector, prepared_video, copied_video_path


def extract_models(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        if isinstance(payload.get("models"), list):
            return [item for item in payload["models"] if isinstance(item, dict)]
        if isinstance(payload.get("data"), list):
            return [item for item in payload["data"] if isinstance(item, dict)]
        if payload.get("name"):
            return [payload]
    return []


def streetmodel_urlopen(req: urllib.request.Request, timeout_seconds: int):
    removed = {key: os.environ.pop(key) for key in PROXY_ENV_KEYS if key in os.environ}
    try:
        return urllib.request.urlopen(req, timeout=timeout_seconds)
    finally:
        os.environ.update(removed)


class Embedder:
    def __init__(self, provider: str, model: str, dims: int | None, normalize: bool):
        self.provider = provider
        self.model = model
        self.dims = dims
        self.normalize = normalize
        self._model: Any | None = None
        if provider == "sentence-transformers":
            try:
                from sentence_transformers import SentenceTransformer
            except ModuleNotFoundError as exc:
                raise RuntimeError("EMBEDDING_DEPENDENCY_MISSING: install sentence-transformers in the sandbox image") from exc
            try:
                self._model = SentenceTransformer(model)
            except Exception as exc:
                raise RuntimeError(f"EMBEDDING_MODEL_LOAD_FAILED: {exc}") from exc
        elif provider != "deterministic-hash":
            raise RuntimeError(f"UNSUPPORTED_EMBEDDING_PROVIDER: {provider}")

    def encode(self, text: str) -> list[float]:
        if self.provider == "deterministic-hash":
            dims = self.dims or 1024
            values = []
            seed = text.encode("utf-8")
            counter = 0
            while len(values) < dims:
                digest = hashlib.sha256(seed + str(counter).encode("ascii")).digest()
                for byte in digest:
                    values.append((byte / 127.5) - 1.0)
                    if len(values) == dims:
                        break
                counter += 1
            return normalize(values) if self.normalize else values
        vector = self._model.encode([text], normalize_embeddings=self.normalize)[0]
        return [float(x) for x in vector.tolist()]


def normalize(values: list[float]) -> list[float]:
    norm = math.sqrt(sum(x * x for x in values))
    if norm <= 0:
        return values
    return [x / norm for x in values]


def embedding_config(args: argparse.Namespace, config: dict[str, Any]) -> tuple[str, str, int | None, bool]:
    cfg = config.get("embedding", {}) if isinstance(config.get("embedding"), dict) else {}
    provider = args.embedding_provider or cfg.get("provider") or "sentence-transformers"
    model = args.embedding_model or cfg.get("model") or "BAAI/bge-m3"
    dims = args.dimensions if args.dimensions is not None else cfg.get("dimensions")
    normalize_embeddings = bool(cfg.get("normalize", True))
    return str(provider), str(model), int(dims) if dims else None, normalize_embeddings


def build_embedding_text(doc: dict[str, Any]) -> str:
    parts = [
        doc.get("embedding_text"),
        doc.get("content_text"),
        doc.get("description"),
        doc.get("filename"),
        doc.get("camera_id"),
        " ".join(doc.get("labels") or []),
        " ".join(doc.get("search_terms") or []),
    ]
    for key in ("location", "metadata", "object_summary"):
        value = doc.get(key)
        if isinstance(value, (dict, list)):
            parts.append(json.dumps(value, ensure_ascii=False))
    return " ".join(str(part) for part in parts if part).strip()


def compact_for_vector_index(doc: dict[str, Any]) -> dict[str, Any]:
    out = dict(doc)
    object_summary = out.get("object_summary")
    if isinstance(object_summary, dict):
        out["object_summary"] = {
            "total_objects": object_summary.get("total_objects", 0),
            "by_label": object_summary.get("by_label", {}),
            "analysis_status": object_summary.get("analysis_status"),
        }
    tracks_summary = out.get("tracks_summary")
    if isinstance(tracks_summary, dict):
        out["tracks_summary"] = {
            "total_tracks": tracks_summary.get("total_tracks", 0),
            "by_label": tracks_summary.get("by_label", {}),
            "movement_states": tracks_summary.get("movement_states", {}),
        }
    return out


def video_library_config(config: dict[str, Any]) -> dict[str, Any]:
    value = config.get("video_library")
    return value if isinstance(value, dict) else {}


def resolve_index_storage(args: argparse.Namespace, config: dict[str, Any]) -> tuple[str, str, str]:
    cfg = video_library_config(config)
    source_index = str(args.source_index or cfg.get("source_index") or DEFAULT_SOURCE_INDEX)
    target_index = str(args.target_index or cfg.get("embedding_target_index") or DEFAULT_TARGET_INDEX)
    storage_mode = str(args.storage_mode or cfg.get("embedding_storage_mode") or DEFAULT_STORAGE_MODE)
    invalid = {
        name: value
        for name, value in (("source_index", source_index), ("target_index", target_index))
        if value != DEFAULT_SOURCE_INDEX
    }
    if invalid:
        raise VideoIndexPolicyError(
            "UNSUPPORTED_VIDEO_INDEX",
            f"Only {DEFAULT_SOURCE_INDEX} is allowed for video embeddings",
            {"allowed_index": DEFAULT_SOURCE_INDEX, "received": invalid},
        )
    if storage_mode != "in_place":
        raise VideoIndexPolicyError(
            "UNSUPPORTED_EMBEDDING_STORAGE_MODE",
            "Only in_place embedding storage is allowed",
            {"allowed_storage_mode": "in_place", "received_storage_mode": storage_mode},
        )
    return source_index, target_index, storage_mode


def embedding_success_patch(
    out_doc: dict[str, Any],
    owner: str,
    vector_field: str,
    vector: list[float],
) -> dict[str, Any]:
    patch = {
        key: value
        for key, value in out_doc.items()
        if key.startswith("video_embedding_") or key in {"deerflow_video_path", "embedding_text"}
    }
    patch.update(
        {
            "embedding_owner": owner,
            "embedding_status": "success",
            "embedding_provider": out_doc.get("embedding_provider"),
            "embedding_model": out_doc.get("embedding_model"),
            "embedding_vector_field": vector_field,
            "embedding_updated_at": datetime.now(timezone.utc).isoformat(),
            "embedding_error": None,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            vector_field: vector,
        }
    )
    return patch


def embedding_failure_patch(
    owner: str,
    provider: str,
    model: str,
    vector_field: str,
    code: str,
    message: str,
    detail: dict[str, Any] | None = None,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    return {
        "embedding_owner": owner,
        "embedding_status": "failed",
        "embedding_provider": provider,
        "embedding_model": model,
        "embedding_vector_field": vector_field,
        "embedding_updated_at": now,
        "embedding_error": {"code": code, "message": message, "detail": detail or {}},
        "updated_at": now,
    }


def safe_target_index(index: str) -> bool:
    return index == DEFAULT_SOURCE_INDEX


def explicit_video_doc(video_uri: str, video_id: str | None = None) -> dict[str, Any]:
    doc_id = video_id or hashlib.sha1(video_uri.encode("utf-8")).hexdigest()
    filename = Path(video_uri).name
    return {
        "video_id": doc_id,
        "file_path": video_uri,
        "raw_segment_uri": video_uri,
        "filename": filename,
        "source_type": "streetmodel_video_uri",
        "content_text": filename,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate video-library embeddings and enrich citybrain-video-library in place.")
    parser.add_argument("--source-index")
    parser.add_argument("--target-index")
    parser.add_argument("--storage-mode", choices=["in_place"])
    parser.add_argument("--owner", default=DEFAULT_OWNER)
    parser.add_argument("--video-id")
    parser.add_argument("--video-uri")
    parser.add_argument("--video-vector-output")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--query")
    parser.add_argument("--query-vector-output")
    parser.add_argument("--embedding-provider")
    parser.add_argument("--embedding-model")
    parser.add_argument("--dimensions", type=int)
    parser.add_argument("--vector-field")
    parser.add_argument("--base-url")
    parser.add_argument("--timeout-seconds", type=int)
    parser.add_argument("--deerflow-path-prefix")
    parser.add_argument("--streetmodel-path-prefix")
    parser.add_argument("--copy-video-to-shared", action="store_true")
    parser.add_argument("--video-preprocess", choices=["image-frames", "image_frames", "frame-proxy", "frame_proxy", "none"])
    parser.add_argument("--frame-sample-fps", type=float)
    parser.add_argument("--max-sampled-frames", type=int)
    parser.add_argument("--image-frame-count", type=int)
    parser.add_argument("--proxy-output-fps", type=float)
    parser.add_argument("--proxy-max-width", type=int)
    parser.add_argument("--refresh", action="store_true", help="Accepted for workflow compatibility; target indices are refreshed automatically when writes complete.")
    parser.add_argument("--config")
    parser.add_argument("--output")
    args = parser.parse_args()

    config = load_config(args.config)
    try:
        source_index, target_index, storage_mode = resolve_index_storage(args, config)
    except VideoIndexPolicyError as exc:
        return emit(failed(exc.code, exc.message, False, exc.detail), args.output)
    args.source_index = source_index
    args.target_index = target_index
    provider, model, dims, normalize_embeddings = embedding_config(args, config)
    vector_field = vector_field_config(args, config, provider)
    street_cfg: dict[str, Any] = {}
    preprocess_cfg: dict[str, Any] = {}
    try:
        if provider == "streetmodel":
            street_cfg = streetmodel_config(args, config)
            preprocess_cfg = video_preprocess_config(args, config)
            model = street_cfg["model_name"]
            dims = street_cfg["dimensions"]
            embedder = StreetModelEmbedder(
                base_url=street_cfg["base_url"],
                model=model,
                dims=dims,
                batch_size=street_cfg["batch_size"],
                timeout_seconds=street_cfg["timeout_seconds"],
                video_instruction=street_cfg["video_instruction"],
                text_instruction=street_cfg["text_instruction"],
                frame_instruction=street_cfg["frame_instruction"],
            )
        else:
            embedder = Embedder(provider, model, dims, normalize_embeddings)
    except EmbeddingError as exc:
        return emit(failed(exc.code, exc.message, exc.retryable, exc.detail), args.output)
    except RuntimeError as exc:
        text = str(exc)
        code, _, message = text.partition(":")
        return emit(failed(code, message.strip() or text, False, {"provider": provider, "model": model}), args.output)

    if args.query:
        try:
            vector = embedder.encode_text(args.query) if provider == "streetmodel" else embedder.encode(args.query)
        except EmbeddingError as exc:
            return emit(failed(exc.code, exc.message, exc.retryable, exc.detail), args.output)
        payload = {
            "query": args.query,
            "vector": vector,
            "embedding_provider": provider,
            "embedding_model": model,
            "dimensions": len(vector),
            "vector_field": vector_field,
        }
        write_json_file(args.query_vector_output, payload)
        return emit(success(payload), args.output)

    in_place = storage_mode == "in_place"

    if args.video_uri:
        if provider != "streetmodel":
            return emit(failed("UNSUPPORTED_VIDEO_URI_PROVIDER", "--video-uri is only supported with --embedding-provider streetmodel", False, {"provider": provider}), args.output)
        try:
            es = EsClient(config)
            vector_dims = dims or DEFAULT_STREETMODEL_DIMS
            es.ensure_index(args.target_index, vector_dims, vector_field)
            vector, prepared_video, copied_video_path = embed_streetmodel_video_source(
                args.video_uri,
                embedder,
                street_cfg,
                preprocess_cfg,
                copy_to_shared=args.copy_video_to_shared,
                video_id=args.video_id,
            )
            document_video_uri = prepared_video.get("source_uri") or args.video_uri
            out_doc = explicit_video_doc(document_video_uri, args.video_id)
            if prepared_video.get("mode") == "image_frames":
                out_doc["source_type"] = "local_file"
            out_doc["owner"] = args.owner
            out_doc["embedding_provider"] = provider
            out_doc["embedding_model"] = model
            apply_video_embedding_metadata(out_doc, prepared_video, copied_video_path)
            out_doc["video_embedding_dimensions"] = len(vector)
            out_doc[vector_field] = vector
            doc_id = str(out_doc["video_id"])
            direct_patch = dict(out_doc)
            direct_patch.update(embedding_success_patch(out_doc, args.owner, vector_field, vector))
            es.update_doc(args.target_index, doc_id, direct_patch, doc_as_upsert=True)
            es.refresh(args.target_index)
        except EsError as exc:
            return emit(failed(exc.code, exc.message, exc.retryable, exc.detail), args.output)
        except EmbeddingError as exc:
            return emit(failed(exc.code, exc.message, exc.retryable, exc.detail), args.output)
        vector_payload = {
            "video_id": doc_id,
            "video_uri": prepared_video.get("video_uri"),
            "source_video_uri": prepared_video.get("source_uri"),
            "proxy_video_uri": prepared_video.get("proxy_uri"),
            "input_type": prepared_video.get("input_type") or "video",
            "frame_aggregation": prepared_video.get("frame_aggregation"),
            "sampled_frames": prepared_video.get("sampled_frames"),
            "vector": vector,
            "embedding_provider": provider,
            "embedding_model": model,
            "dimensions": len(vector),
            "vector_field": vector_field,
        }
        write_json_file(args.video_vector_output, vector_payload)
        return emit(
            success(
                {
                    "target_index": args.target_index,
                    "storage_mode": storage_mode,
                    "owner": args.owner,
                    "embedding_provider": provider,
                    "embedding_model": model,
                    "vector_field": vector_field,
                    "embedded_count": 1,
                    "failed_count": 0,
                    "documents": [
                        {
                            "video_id": doc_id,
                            "dimensions": len(vector),
                            "vector_field": vector_field,
                            "video_embedding_uri": prepared_video.get("video_uri"),
                            "video_embedding_source_uri": prepared_video.get("source_uri"),
                            "video_embedding_proxy_uri": prepared_video.get("proxy_uri"),
                            "video_embedding_preprocess_mode": prepared_video.get("mode"),
                            "video_embedding_input_type": prepared_video.get("input_type") or "video",
                            "video_embedding_frame_aggregation": prepared_video.get("frame_aggregation"),
                            "video_embedding_sampled_frames": prepared_video.get("sampled_frames"),
                            "deerflow_video_path": copied_video_path,
                            "video_embedding_proxy_path": prepared_video.get("proxy_deerflow_path"),
                        }
                    ],
                    "failures": [],
                }
            ),
            args.output,
        )

    try:
        es = EsClient(config)
        vector_dims = dims or (1024 if model == "BAAI/bge-m3" else len(embedder.encode("dimension probe")))
        es.ensure_index(args.target_index, vector_dims, vector_field)
        docs = es.fetch_docs(args.source_index, args.limit, args.video_id, vector_field)
    except EsError as exc:
        return emit(failed(exc.code, exc.message, exc.retryable, exc.detail), args.output)
    except EmbeddingError as exc:
        return emit(failed(exc.code, exc.message, exc.retryable, exc.detail), args.output)

    documents = []
    failures = []
    for doc in docs:
        try:
            out_doc = compact_for_vector_index(doc)
            out_doc["owner"] = args.owner
            out_doc["embedding_provider"] = provider
            out_doc["embedding_model"] = model
            text = build_embedding_text(doc)
            if text:
                out_doc["embedding_text"] = text
            if provider == "streetmodel":
                original_video_path = source_video_path(doc)
                vector, prepared_video, copied_video_path = embed_streetmodel_video_source(
                    original_video_path,
                    embedder,
                    street_cfg,
                    preprocess_cfg,
                    copy_to_shared=args.copy_video_to_shared,
                    video_id=str(out_doc.get("video_id") or ""),
                )
                apply_video_embedding_metadata(out_doc, prepared_video, copied_video_path)
                out_doc["video_embedding_dimensions"] = len(vector)
                doc_id = str(out_doc.get("video_id") or hashlib.sha1(original_video_path.encode("utf-8")).hexdigest())
            else:
                if not text:
                    failures.append({"video_id": doc.get("video_id"), "error_code": "EMPTY_EMBEDDING_TEXT"})
                    continue
                vector = embedder.encode(text)
                doc_id = str(out_doc.get("video_id") or hashlib.sha1(text.encode("utf-8")).hexdigest())
            out_doc[vector_field] = vector
            es.update_doc(args.target_index, doc_id, embedding_success_patch(out_doc, args.owner, vector_field, vector))
            item = {"video_id": doc_id, "dimensions": len(vector), "vector_field": vector_field, "embedding_text_chars": len(text)}
            if provider == "streetmodel":
                item["video_embedding_uri"] = out_doc.get("video_embedding_uri")
                item["video_embedding_source_uri"] = out_doc.get("video_embedding_source_uri")
                item["video_embedding_proxy_uri"] = out_doc.get("video_embedding_proxy_uri")
                item["video_embedding_preprocess_mode"] = out_doc.get("video_embedding_preprocess_mode")
                item["video_embedding_input_type"] = out_doc.get("video_embedding_input_type")
                item["video_embedding_frame_aggregation"] = out_doc.get("video_embedding_frame_aggregation")
                item["video_embedding_sampled_frames"] = out_doc.get("video_embedding_sampled_frames")
                if out_doc.get("video_embedding_proxy_path"):
                    item["video_embedding_proxy_path"] = out_doc.get("video_embedding_proxy_path")
                if out_doc.get("deerflow_video_path"):
                    item["deerflow_video_path"] = out_doc.get("deerflow_video_path")
            documents.append(item)
        except EmbeddingError as exc:
            failure = {"video_id": doc.get("video_id"), "error_code": exc.code, "message": exc.message, "detail": exc.detail}
            if in_place and doc.get("video_id"):
                try:
                    es.update_doc(
                        args.target_index,
                        str(doc["video_id"]),
                        embedding_failure_patch(args.owner, provider, model, vector_field, exc.code, exc.message, exc.detail),
                    )
                except EsError as update_exc:
                    failure["status_update_error"] = {"error_code": update_exc.code, "message": update_exc.message}
            failures.append(failure)
        except Exception as exc:
            failure = {"video_id": doc.get("video_id"), "error_code": "EMBEDDING_FAILED", "message": str(exc)}
            if in_place and doc.get("video_id"):
                try:
                    es.update_doc(
                        args.target_index,
                        str(doc["video_id"]),
                        embedding_failure_patch(args.owner, provider, model, vector_field, "EMBEDDING_FAILED", str(exc)),
                    )
                except EsError as update_exc:
                    failure["status_update_error"] = {"error_code": update_exc.code, "message": update_exc.message}
            failures.append(failure)
    try:
        es.refresh(args.target_index)
    except EsError as exc:
        failures.append({"error_code": exc.code, "message": exc.message, "detail": exc.detail})

    data = {
        "source_index": args.source_index,
        "target_index": args.target_index,
        "storage_mode": storage_mode,
        "owner": args.owner,
        "embedding_provider": provider,
        "embedding_model": model,
        "vector_field": vector_field,
        "embedded_count": len(documents),
        "failed_count": len(failures),
        "documents": documents,
        "failures": failures,
    }
    return emit(success(data, 1.0 if not failures else 0.75), args.output)


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
