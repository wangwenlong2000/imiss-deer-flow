#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
import os
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

try:
    import yaml
except Exception:
    yaml = None

SKILL = "video-search"
VERSION = "1.0.0"
DEFAULT_INDEX = "citybrain-video-library"
DEFAULT_STREETMODEL_VECTOR_FIELD = "video_vector-Qwen3-VL-Embedding-2B_urban_governance"
DEFAULT_STREETMODEL_BASE_URL = "http://219.245.185.245:3130"
DEFAULT_STREETMODEL_MODEL = "Qwen3-VL-Embedding-2B"
DEFAULT_STREETMODEL_DIMS = 2048
DEFAULT_STREETMODEL_BATCH_SIZE = 1
DEFAULT_STREETMODEL_TIMEOUT = 600
DEFAULT_STREETMODEL_TEXT_INSTRUCTION = "Represent this surveillance/street-view query for urban scene retrieval."
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
            if exc.code in ok:
                return exc.code, payload
            code = "INDEX_NOT_FOUND" if exc.code == 404 else "ES_REQUEST_FAILED"
            raise EsError(code, f"Elasticsearch request failed: HTTP {exc.code}", exc.code >= 500, {"path": path, "response": payload})
        except urllib.error.URLError as exc:
            raise EsError("ES_CONNECTION_FAILED", f"Could not connect to Elasticsearch at {self.url}: {exc.reason}", True, {"url": self.url})

    def mapping_has_vector(self, index: str, vector_field: str = "vector") -> bool:
        _, mapping = self.request("GET", f"{index}/_mapping", ok={200})
        props = next(iter(mapping.values()), {}).get("mappings", {}).get("properties", {})
        return props.get(vector_field, {}).get("type") == "dense_vector"

    def search(self, index: str, body: dict[str, Any]) -> dict[str, Any]:
        _, payload = self.request("POST", f"{index}/_search", body, ok={200})
        return payload


def vector_field_config(args: argparse.Namespace, config: dict[str, Any]) -> str:
    cfg = config.get("streetmodel_embedding", {}) if isinstance(config.get("streetmodel_embedding"), dict) else {}
    if args.vector_field:
        return str(args.vector_field)
    if cfg:
        return str(cfg.get("vector_field") or DEFAULT_STREETMODEL_VECTOR_FIELD)
    return "vector"


def streetmodel_config(args: argparse.Namespace, config: dict[str, Any]) -> dict[str, Any]:
    cfg = config.get("streetmodel_embedding", {}) if isinstance(config.get("streetmodel_embedding"), dict) else {}
    return {
        "base_url": str(args.base_url or os.getenv("STREETMODEL_BASE_URL") or cfg.get("base_url") or DEFAULT_STREETMODEL_BASE_URL).rstrip("/"),
        "model_name": str(args.embedding_model or cfg.get("model_name") or DEFAULT_STREETMODEL_MODEL),
        "dimensions": int(args.dimensions or cfg.get("dimensions") or DEFAULT_STREETMODEL_DIMS),
        "batch_size": int(cfg.get("batch_size") or DEFAULT_STREETMODEL_BATCH_SIZE),
        "timeout_seconds": int(args.timeout_seconds or cfg.get("timeout_seconds") or DEFAULT_STREETMODEL_TIMEOUT),
        "text_instruction": str(cfg.get("text_instruction") or DEFAULT_STREETMODEL_TEXT_INSTRUCTION),
    }


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


class StreetModelTextEmbedder:
    def __init__(self, base_url: str, model: str, dims: int, batch_size: int, timeout_seconds: int, text_instruction: str):
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.dims = dims
        self.batch_size = batch_size
        self.timeout_seconds = timeout_seconds
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

    def encode_text(self, text: str) -> list[float]:
        payload = {
            "model_name": self.model,
            "instruction": self.text_instruction,
            "items": [{"type": "text", "content": text}],
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


def source_excludes(vector_field: str) -> list[str]:
    excludes = ["vector"]
    if vector_field != "vector":
        excludes.append(vector_field)
    return excludes


def painless_string_literal(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def parse_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


def load_vector(path: str | None) -> list[float] | None:
    if not path:
        return None
    data = load_structured(path)
    if isinstance(data, list):
        return [float(x) for x in data]
    if isinstance(data, dict):
        vector = data.get("vector") or data.get("query_vector")
        if isinstance(vector, list):
            return [float(x) for x in vector]
    return None


def build_filters(args: argparse.Namespace) -> list[dict[str, Any]]:
    filters: list[dict[str, Any]] = []
    if args.camera_id:
        filters.append({"term": {"camera_id": args.camera_id}})
    labels = parse_csv(args.labels)
    if labels:
        filters.append({"terms": {"labels": labels}})
    if args.event_type:
        filters.append({"term": {"event_types": args.event_type}})
    time_range: dict[str, Any] = {}
    if args.start_time:
        time_range["gte"] = args.start_time
    if args.end_time:
        time_range["lte"] = args.end_time
    if time_range:
        filters.append({"range": {"started_at": time_range}})
    return filters


def build_keyword_query(query: str | None, filters: list[dict[str, Any]]) -> dict[str, Any]:
    must: list[dict[str, Any]] = []
    if query:
        must.append({"bool": {"should": [
            {
                "multi_match": {
                    "query": query,
                    "fields": ["content_text^3", "filename^2", "camera_id", "labels", "event_types", "location.*", "metadata.*"],
                    "type": "best_fields"
                }
            },
            {"match_phrase_prefix": {"content_text": {"query": query, "boost": 2}}},
            {"wildcard": {"filename.keyword": {"value": f"*{query}*", "case_insensitive": True, "boost": 2}}},
        ], "minimum_should_match": 1}})
    return {"bool": {"must": must or [{"match_all": {}}], "filter": filters}}


def build_filter_query(filters: list[dict[str, Any]]) -> dict[str, Any]:
    return {"bool": {"must": [{"match_all": {}}], "filter": filters}}


def compact_hit(hit: dict[str, Any]) -> dict[str, Any]:
    source = hit.get("_source", {})
    return {
        "score": hit.get("_score"),
        "video_id": source.get("video_id") or hit.get("_id"),
        "camera_id": source.get("camera_id"),
        "filename": source.get("filename"),
        "raw_segment_uri": source.get("raw_segment_uri"),
        "started_at": source.get("started_at"),
        "ended_at": source.get("ended_at"),
        "labels": source.get("labels", []),
        "object_summary": source.get("object_summary", {}),
        "location": source.get("location", {}),
        "metadata": source.get("metadata", {}),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Search the Elasticsearch video library.")
    parser.add_argument("--query")
    parser.add_argument("--index", default=DEFAULT_INDEX)
    parser.add_argument("--camera-id")
    parser.add_argument("--start-time")
    parser.add_argument("--end-time")
    parser.add_argument("--labels")
    parser.add_argument("--event-type")
    parser.add_argument("--query-vector-json")
    parser.add_argument("--embedding-provider")
    parser.add_argument("--embedding-model")
    parser.add_argument("--base-url")
    parser.add_argument("--dimensions", type=int)
    parser.add_argument("--timeout-seconds", type=int)
    parser.add_argument("--vector-field")
    parser.add_argument("--vector-query-mode", choices=["hybrid", "semantic"])
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--config")
    parser.add_argument("--output")
    args = parser.parse_args()
    config = load_config(args.config)
    es = EsClient(config)
    filters = build_filters(args)
    vector = load_vector(args.query_vector_json)
    vector_field = vector_field_config(args, config)
    query_embedding: dict[str, Any] | None = None
    if vector is None and args.embedding_provider == "streetmodel":
        if not args.query:
            return emit(failed("MISSING_QUERY", "--embedding-provider streetmodel requires --query when --query-vector-json is not provided"), args.output)
        try:
            street_cfg = streetmodel_config(args, config)
            embedder = StreetModelTextEmbedder(
                base_url=street_cfg["base_url"],
                model=street_cfg["model_name"],
                dims=street_cfg["dimensions"],
                batch_size=street_cfg["batch_size"],
                timeout_seconds=street_cfg["timeout_seconds"],
                text_instruction=street_cfg["text_instruction"],
            )
            vector = embedder.encode_text(args.query)
            query_embedding = {
                "embedding_provider": "streetmodel",
                "embedding_model": street_cfg["model_name"],
                "dimensions": len(vector),
            }
        except EmbeddingError as exc:
            return emit(failed(exc.code, exc.message, exc.retryable, exc.detail), args.output)

    vector_query_mode = args.vector_query_mode or ("semantic" if query_embedding else "hybrid")
    query_mode = "keyword_filter"
    try:
        base_query = build_filter_query(filters) if vector and vector_query_mode == "semantic" else build_keyword_query(args.query, filters)
        scoring_field = vector_field
        has_vector_field = bool(vector and es.mapping_has_vector(args.index, scoring_field))
        if vector and not has_vector_field and scoring_field != "vector" and es.mapping_has_vector(args.index, "vector"):
            scoring_field = "vector"
            has_vector_field = True
        if vector and has_vector_field:
            field_literal = painless_string_literal(scoring_field)
            body = {
                "size": args.top_k,
                "_source": {"excludes": source_excludes(scoring_field)},
                "query": {
                    "script_score": {
                        "query": base_query,
                        "script": {
                            "source": f"doc[{field_literal}].size() == 0 ? 0.0 : cosineSimilarity(params.query_vector, {field_literal}) + 1.0",
                            "params": {"query_vector": vector},
                        },
                    }
                },
            }
            query_mode = "vector_filter" if vector_query_mode == "semantic" else "vector_keyword_filter"
        else:
            body = {"size": args.top_k, "_source": {"excludes": source_excludes(vector_field)}, "query": base_query}
            if vector:
                query_mode = "keyword_filter_vector_unavailable"
        payload = es.search(args.index, body)
    except EsError as exc:
        return emit(failed(exc.code, exc.message, exc.retryable, exc.detail), args.output)
    hits = [compact_hit(hit) for hit in payload.get("hits", {}).get("hits", [])]
    total_raw = payload.get("hits", {}).get("total", {})
    total = total_raw.get("value") if isinstance(total_raw, dict) else total_raw
    data = {"hits": hits, "total": total or len(hits), "query_mode": query_mode, "index": args.index, "vector_field": vector_field}
    if query_embedding:
        data["query_embedding"] = query_embedding
    return emit(success(data), args.output)


if __name__ == "__main__":
    raise SystemExit(main())
