from __future__ import annotations

import importlib.util
import os
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
VIDEO_EMBEDDING_SCRIPT = REPO_ROOT / "skills/custom/video-embedding-index/scripts/run.py"

DEFAULT_TARGET_INDEX = "huangxiao-video-library-vector-v1"
DEFAULT_OWNER = "huangxiao"

_SKILL_MODULE: ModuleType | None = None


def _load_skill_module() -> ModuleType:
    """Load the existing video embedding skill script as the implementation backend."""
    global _SKILL_MODULE
    if _SKILL_MODULE is not None:
        return _SKILL_MODULE

    spec = importlib.util.spec_from_file_location("streetmodel_video_embedding_skill_backend", VIDEO_EMBEDDING_SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load video embedding skill script at {VIDEO_EMBEDDING_SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    _SKILL_MODULE = module
    return module


def resolve_config_path(config_path: str | None = None) -> str | None:
    if config_path:
        return config_path
    env_path = os.getenv("DEER_FLOW_CONFIG_PATH")
    if env_path:
        return env_path
    for candidate in (REPO_ROOT / "config.yaml", REPO_ROOT / "backend/config.yaml"):
        if candidate.exists():
            return str(candidate)
    return None


def _load_config(config_path: str | None = None) -> dict[str, Any]:
    module = _load_skill_module()
    return module.load_config(resolve_config_path(config_path))


def _args(**overrides: Any) -> SimpleNamespace:
    defaults = {
        "base_url": None,
        "embedding_model": None,
        "dimensions": None,
        "timeout_seconds": None,
        "deerflow_path_prefix": None,
        "streetmodel_path_prefix": None,
        "vector_field": None,
        "video_preprocess": None,
        "frame_sample_fps": None,
        "max_sampled_frames": None,
        "proxy_output_fps": None,
        "proxy_max_width": None,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _runtime(config_path: str | None = None, **overrides: Any) -> tuple[ModuleType, dict[str, Any], dict[str, Any], str, Any]:
    module = _load_skill_module()
    config = _load_config(config_path)
    args = _args(**overrides)
    street_cfg = module.streetmodel_config(args, config)
    vector_field = module.vector_field_config(args, config, "streetmodel")
    embedder = module.StreetModelEmbedder(
        base_url=street_cfg["base_url"],
        model=street_cfg["model_name"],
        dims=street_cfg["dimensions"],
        batch_size=street_cfg["batch_size"],
        timeout_seconds=street_cfg["timeout_seconds"],
        video_instruction=street_cfg["video_instruction"],
        text_instruction=street_cfg["text_instruction"],
    )
    return module, config, street_cfg, vector_field, embedder


def _failure(
    code: str,
    message: str,
    *,
    retryable: bool = False,
    detail: dict[str, Any] | None = None,
    target_index: str | None = None,
    video_id: str | None = None,
    query: str | None = None,
) -> dict[str, Any]:
    item: dict[str, Any] = {
        "error_code": code,
        "message": message,
        "retryable": retryable,
        "detail": detail or {},
    }
    if video_id is not None:
        item["video_id"] = video_id
    if query is not None:
        item["query"] = query
    return {
        "status": "failed",
        "error_code": code,
        "message": message,
        "retryable": retryable,
        "detail": detail or {},
        "target_index": target_index,
        "video_id": video_id,
        "query": query,
        "embedded_count": 0,
        "failed_count": 1,
        "failures": [item],
    }


def _failure_from_exception(
    exc: Exception,
    *,
    target_index: str | None = None,
    video_id: str | None = None,
    query: str | None = None,
) -> dict[str, Any]:
    code = getattr(exc, "code", exc.__class__.__name__.upper())
    message = getattr(exc, "message", str(exc))
    retryable = bool(getattr(exc, "retryable", False))
    detail = getattr(exc, "detail", {})
    if not isinstance(detail, dict):
        detail = {"detail": detail}
    return _failure(
        str(code),
        str(message),
        retryable=retryable,
        detail=detail,
        target_index=target_index,
        video_id=video_id,
        query=query,
    )


def map_video_uri(video_uri: str, deerflow_path_prefix: str, streetmodel_path_prefix: str) -> str:
    module = _load_skill_module()
    return module.to_streetmodel_video_uri(video_uri, deerflow_path_prefix, streetmodel_path_prefix)


def safe_target_index(index: str, owner: str = DEFAULT_OWNER, allow_shared_index: bool = False) -> bool:
    module = _load_skill_module()
    return module.safe_target_index(index, owner, allow_shared_index)


def streetmodel_health() -> dict[str, Any]:
    try:
        module, _, street_cfg, vector_field, embedder = _runtime()
        health_payload = embedder.request("GET", "/health")
        models_payload = embedder.request("GET", "/models")
        models = module.extract_models(models_payload)
        model_entry = next((item for item in models if item.get("name") == street_cfg["model_name"]), None)
        return {
            "status": "success",
            "embedding_provider": "streetmodel",
            "embedding_model": street_cfg["model_name"],
            "base_url": street_cfg["base_url"],
            "dimensions": street_cfg["dimensions"],
            "vector_field": vector_field,
            "health": health_payload,
            "model": model_entry,
            "models_count": len(models),
            "failures": [],
        }
    except Exception as exc:
        return _failure_from_exception(exc)


def streetmodel_embed_query(
    query: str,
    return_vector: bool = False,
    query_vector_output: str | None = None,
) -> dict[str, Any]:
    normalized_query = str(query or "").strip()
    if not normalized_query:
        return _failure("MISSING_QUERY", "query must be a non-empty string", query=query)

    try:
        module, _, street_cfg, vector_field, embedder = _runtime()
        vector = embedder.encode_text(normalized_query)
        vector_payload = {
            "query": normalized_query,
            "vector": vector,
            "embedding_provider": "streetmodel",
            "embedding_model": street_cfg["model_name"],
            "dimensions": len(vector),
            "vector_field": vector_field,
        }
        if query_vector_output:
            module.write_json_file(query_vector_output, vector_payload)

        result: dict[str, Any] = {
            "status": "success",
            "query": normalized_query,
            "target_index": None,
            "video_id": None,
            "video_embedding_uri": None,
            "embedding_provider": "streetmodel",
            "embedding_model": street_cfg["model_name"],
            "dimensions": len(vector),
            "vector_field": vector_field,
            "query_vector_output": query_vector_output,
            "embedded_count": 1,
            "failed_count": 0,
            "failures": [],
        }
        if return_vector:
            result["vector"] = vector
        return result
    except Exception as exc:
        return _failure_from_exception(exc, query=normalized_query)


def streetmodel_embed_video(
    video_uri: str,
    video_id: str | None = None,
    target_index: str = DEFAULT_TARGET_INDEX,
    owner: str = DEFAULT_OWNER,
    copy_video_to_shared: bool = False,
    allow_shared_index: bool = False,
    return_vector: bool = False,
    video_preprocess: str | None = None,
    frame_sample_fps: float | None = None,
    max_sampled_frames: int | None = None,
    proxy_output_fps: float | None = None,
    proxy_max_width: int | None = None,
) -> dict[str, Any]:
    normalized_uri = str(video_uri or "").strip()
    if not normalized_uri:
        return _failure(
            "MISSING_VIDEO_URI",
            "video_uri must be a non-empty string",
            target_index=target_index,
            video_id=video_id,
        )
    if not safe_target_index(target_index, owner, allow_shared_index):
        return _failure(
            "UNSAFE_TARGET_INDEX",
            (
                f"Refusing to write to non-personal index: {target_index}. "
                f"Use an index prefixed with {owner}- or pass allow_shared_index=true."
            ),
            target_index=target_index,
            video_id=video_id,
        )

    try:
        runtime_overrides = {
            "video_preprocess": video_preprocess,
            "frame_sample_fps": frame_sample_fps,
            "max_sampled_frames": max_sampled_frames,
            "proxy_output_fps": proxy_output_fps,
            "proxy_max_width": proxy_max_width,
        }
        module, config, street_cfg, vector_field, embedder = _runtime(**runtime_overrides)
        preprocess_cfg = module.video_preprocess_config(_args(**runtime_overrides), config)
        es = module.EsClient(config)
        es.ensure_index(target_index, street_cfg["dimensions"], vector_field)

        model_video_uri, copied_video_path = module.materialize_streetmodel_video_uri(
            normalized_uri,
            street_cfg["deerflow_path_prefix"],
            street_cfg["streetmodel_path_prefix"],
            copy_to_shared=copy_video_to_shared,
            video_id=video_id,
        )
        prepared_video = module.prepare_streetmodel_embedding_video(
            normalized_uri,
            model_video_uri,
            copied_video_path,
            street_cfg,
            preprocess_cfg,
            video_id,
        )
        vector = embedder.encode_video_uri(prepared_video["video_uri"])
        out_doc = module.explicit_video_doc(model_video_uri, video_id)
        out_doc["owner"] = owner
        out_doc["embedding_provider"] = "streetmodel"
        out_doc["embedding_model"] = street_cfg["model_name"]
        module.apply_video_embedding_metadata(out_doc, prepared_video, copied_video_path)
        out_doc["video_embedding_dimensions"] = len(vector)
        out_doc[vector_field] = vector
        doc_id = str(out_doc["video_id"])

        es.upsert_doc(target_index, doc_id, out_doc)
        es.refresh(target_index)

        document = {
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
        result: dict[str, Any] = {
            "status": "success",
            "target_index": target_index,
            "owner": owner,
            "video_id": doc_id,
            "video_embedding_uri": prepared_video["video_uri"],
            "video_embedding_source_uri": prepared_video.get("source_uri"),
            "video_embedding_proxy_uri": prepared_video.get("proxy_uri"),
            "video_embedding_preprocess_mode": prepared_video.get("mode"),
            "embedding_provider": "streetmodel",
            "embedding_model": street_cfg["model_name"],
            "dimensions": len(vector),
            "vector_field": vector_field,
            "embedded_count": 1,
            "failed_count": 0,
            "documents": [document],
            "failures": [],
        }
        if return_vector:
            result["vector"] = vector
        return result
    except Exception as exc:
        return _failure_from_exception(exc, target_index=target_index, video_id=video_id)
