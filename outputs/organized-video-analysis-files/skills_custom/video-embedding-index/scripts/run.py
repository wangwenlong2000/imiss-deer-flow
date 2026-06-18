#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import math
import os
import shutil
import subprocess
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

try:
    import yaml
except Exception:
    yaml = None

SKILL = "video-embedding-index"
VERSION = "1.0.0"
DEFAULT_SOURCE_INDEX = "citybrain-video-library"
DEFAULT_TARGET_INDEX = "huangxiao-video-library-vector-v1"
DEFAULT_OWNER = "huangxiao"
DEFAULT_STREETMODEL_BASE_URL = "http://219.245.185.245:3130"
DEFAULT_STREETMODEL_MODEL = "Qwen3-VL-Embedding-2B"
DEFAULT_STREETMODEL_VECTOR_FIELD = "video_vector-Qwen3-VL-Embedding-2B_urban_governance"
DEFAULT_STREETMODEL_DIMS = 2048
DEFAULT_STREETMODEL_BATCH_SIZE = 1
DEFAULT_STREETMODEL_TIMEOUT = 600
DEFAULT_STREETMODEL_VIDEO_INSTRUCTION = "Represent this surveillance video for urban scene retrieval."
DEFAULT_STREETMODEL_TEXT_INSTRUCTION = "Represent this surveillance/street-view query for urban scene retrieval."
DEFAULT_DEERFLOW_VIDEO_PREFIX = "/data/deerflow/videos"
DEFAULT_STREETMODEL_VIDEO_PREFIX = "/nfsdat2/home/xhuangslm/shared_videos"
DEFAULT_VIDEO_PREPROCESS_MODE = "frame_proxy"
DEFAULT_FRAME_SAMPLE_FPS = 1.0
DEFAULT_MAX_SAMPLED_FRAMES = 300
DEFAULT_PROXY_OUTPUT_FPS = 4.0
DEFAULT_PROXY_MAX_WIDTH = 768
DEFAULT_PROXY_OUTPUT_SUBDIR = "embedding_proxies"
PROXY_ENV_KEYS = ("http_proxy", "https_proxy", "all_proxy", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY")


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

    def ensure_index(self, index: str, dims: int, vector_field: str = "vector") -> None:
        status, _ = self.request("HEAD", index, ok={200, 404})
        if status == 200:
            self.ensure_vector_field(index, vector_field, dims)
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
    if normalized in {"frame_proxy", "frameproxy", "proxy"}:
        return "frame_proxy"
    raise EmbeddingError(
        "UNSUPPORTED_VIDEO_PREPROCESS_MODE",
        f"Unsupported video preprocess mode: {mode}",
        False,
        {"mode": mode, "supported_modes": ["frame_proxy", "none"]},
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
    doc["video_embedding_uri"] = prepared_video["video_uri"]
    doc["video_embedding_source_uri"] = prepared_video.get("source_uri") or prepared_video["video_uri"]
    doc["video_embedding_preprocess_mode"] = prepared_video.get("mode", "none")
    if prepared_video.get("proxy_uri"):
        doc["video_embedding_proxy_uri"] = prepared_video["proxy_uri"]
    if prepared_video.get("proxy_deerflow_path"):
        doc["video_embedding_proxy_path"] = prepared_video["proxy_deerflow_path"]
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
    ):
        self.provider = "streetmodel"
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.dims = dims
        self.batch_size = batch_size
        self.timeout_seconds = timeout_seconds
        self.video_instruction = video_instruction
        self.text_instruction = text_instruction
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

    def embed_item(self, item: dict[str, Any], instruction: str) -> list[float]:
        payload = {
            "model_name": self.model,
            "instruction": instruction,
            "items": [item],
            "batch_size": self.batch_size,
        }
        data = self.request("POST", "/embed", payload)
        shape = data.get("shape")
        embeddings = data.get("embeddings")
        if shape != [1, self.dims] or not isinstance(embeddings, list) or not embeddings:
            raise EmbeddingError(
                "STREETMODEL_INVALID_RESPONSE",
                f"StreetModel returned an invalid embedding shape: {shape}",
                False,
                {"expected_shape": [1, self.dims], "response": data},
            )
        vector = embeddings[0]
        if not isinstance(vector, list) or len(vector) != self.dims:
            raise EmbeddingError(
                "STREETMODEL_INVALID_RESPONSE",
                f"StreetModel returned vector length {len(vector) if isinstance(vector, list) else 'non-list'}",
                False,
                {"expected_dimensions": self.dims, "shape": shape},
            )
        return [float(x) for x in vector]

    def encode_text(self, text: str) -> list[float]:
        return self.embed_item({"type": "text", "content": text}, self.text_instruction)

    def encode_video_uri(self, video_uri: str) -> list[float]:
        return self.embed_item({"type": "video", "uri": video_uri}, self.video_instruction)


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


def safe_target_index(index: str, owner: str, allow_shared: bool) -> bool:
    return allow_shared or index.startswith(f"{owner}-")


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
    parser = argparse.ArgumentParser(description="Generate video-library embeddings and write them to a protected personal ES index.")
    parser.add_argument("--source-index", default=DEFAULT_SOURCE_INDEX)
    parser.add_argument("--target-index", default=DEFAULT_TARGET_INDEX)
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
    parser.add_argument("--video-preprocess", choices=["frame-proxy", "frame_proxy", "none"])
    parser.add_argument("--frame-sample-fps", type=float)
    parser.add_argument("--max-sampled-frames", type=int)
    parser.add_argument("--proxy-output-fps", type=float)
    parser.add_argument("--proxy-max-width", type=int)
    parser.add_argument("--allow-shared-index", action="store_true")
    parser.add_argument("--config")
    parser.add_argument("--output")
    args = parser.parse_args()

    config = load_config(args.config)
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

    if not safe_target_index(args.target_index, args.owner, args.allow_shared_index):
        return emit(failed("UNSAFE_TARGET_INDEX", f"Refusing to write to non-personal index: {args.target_index}. Use an index prefixed with {args.owner}- or pass --allow-shared-index."), args.output)

    if args.video_uri:
        if provider != "streetmodel":
            return emit(failed("UNSUPPORTED_VIDEO_URI_PROVIDER", "--video-uri is only supported with --embedding-provider streetmodel", False, {"provider": provider}), args.output)
        try:
            es = EsClient(config)
            vector_dims = dims or DEFAULT_STREETMODEL_DIMS
            es.ensure_index(args.target_index, vector_dims, vector_field)
            video_uri, copied_video_path = materialize_streetmodel_video_uri(
                args.video_uri,
                street_cfg["deerflow_path_prefix"],
                street_cfg["streetmodel_path_prefix"],
                copy_to_shared=args.copy_video_to_shared,
                video_id=args.video_id,
            )
            prepared_video = prepare_streetmodel_embedding_video(
                args.video_uri,
                video_uri,
                copied_video_path,
                street_cfg,
                preprocess_cfg,
                args.video_id,
            )
            vector = embedder.encode_video_uri(prepared_video["video_uri"])
            out_doc = explicit_video_doc(video_uri, args.video_id)
            out_doc["owner"] = args.owner
            out_doc["embedding_provider"] = provider
            out_doc["embedding_model"] = model
            apply_video_embedding_metadata(out_doc, prepared_video, copied_video_path)
            out_doc["video_embedding_dimensions"] = len(vector)
            out_doc[vector_field] = vector
            doc_id = str(out_doc["video_id"])
            es.upsert_doc(args.target_index, doc_id, out_doc)
            es.refresh(args.target_index)
        except EsError as exc:
            return emit(failed(exc.code, exc.message, exc.retryable, exc.detail), args.output)
        except EmbeddingError as exc:
            return emit(failed(exc.code, exc.message, exc.retryable, exc.detail), args.output)
        vector_payload = {
            "video_id": doc_id,
            "video_uri": prepared_video["video_uri"],
            "source_video_uri": prepared_video.get("source_uri"),
            "proxy_video_uri": prepared_video.get("proxy_uri"),
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
                            "video_embedding_uri": prepared_video["video_uri"],
                            "video_embedding_source_uri": prepared_video.get("source_uri"),
                            "video_embedding_proxy_uri": prepared_video.get("proxy_uri"),
                            "video_embedding_preprocess_mode": prepared_video.get("mode"),
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
                video_uri, copied_video_path = materialize_streetmodel_video_uri(
                    original_video_path,
                    street_cfg["deerflow_path_prefix"],
                    street_cfg["streetmodel_path_prefix"],
                    copy_to_shared=args.copy_video_to_shared,
                    video_id=str(out_doc.get("video_id") or ""),
                )
                prepared_video = prepare_streetmodel_embedding_video(
                    original_video_path,
                    video_uri,
                    copied_video_path,
                    street_cfg,
                    preprocess_cfg,
                    str(out_doc.get("video_id") or ""),
                )
                vector = embedder.encode_video_uri(prepared_video["video_uri"])
                apply_video_embedding_metadata(out_doc, prepared_video, copied_video_path)
                out_doc["video_embedding_dimensions"] = len(vector)
                doc_id = str(out_doc.get("video_id") or hashlib.sha1(video_uri.encode("utf-8")).hexdigest())
            else:
                if not text:
                    failures.append({"video_id": doc.get("video_id"), "error_code": "EMPTY_EMBEDDING_TEXT"})
                    continue
                vector = embedder.encode(text)
                doc_id = str(out_doc.get("video_id") or hashlib.sha1(text.encode("utf-8")).hexdigest())
            out_doc[vector_field] = vector
            es.upsert_doc(args.target_index, doc_id, out_doc)
            item = {"video_id": doc_id, "dimensions": len(vector), "vector_field": vector_field, "embedding_text_chars": len(text)}
            if provider == "streetmodel":
                item["video_embedding_uri"] = out_doc.get("video_embedding_uri")
                item["video_embedding_source_uri"] = out_doc.get("video_embedding_source_uri")
                item["video_embedding_proxy_uri"] = out_doc.get("video_embedding_proxy_uri")
                item["video_embedding_preprocess_mode"] = out_doc.get("video_embedding_preprocess_mode")
                item["video_embedding_sampled_frames"] = out_doc.get("video_embedding_sampled_frames")
                if out_doc.get("video_embedding_proxy_path"):
                    item["video_embedding_proxy_path"] = out_doc.get("video_embedding_proxy_path")
                if out_doc.get("deerflow_video_path"):
                    item["deerflow_video_path"] = out_doc.get("deerflow_video_path")
            documents.append(item)
        except EmbeddingError as exc:
            failures.append({"video_id": doc.get("video_id"), "error_code": exc.code, "message": exc.message, "detail": exc.detail})
        except Exception as exc:
            failures.append({"video_id": doc.get("video_id"), "error_code": "EMBEDDING_FAILED", "message": str(exc)})
    try:
        es.refresh(args.target_index)
    except EsError as exc:
        failures.append({"error_code": exc.code, "message": exc.message, "detail": exc.detail})

    data = {
        "source_index": args.source_index,
        "target_index": args.target_index,
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
    raise SystemExit(main())
