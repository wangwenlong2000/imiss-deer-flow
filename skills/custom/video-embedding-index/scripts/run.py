#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import math
import os
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

    def ensure_index(self, index: str, dims: int) -> None:
        status, _ = self.request("HEAD", index, ok={200, 404})
        if status == 200:
            return
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
                    "vector": {"type": "dense_vector", "dims": dims, "index": True, "similarity": "cosine"},
                }
            },
        }
        self.request("PUT", index, mapping, ok={200, 201})

    def fetch_docs(self, index: str, limit: int, video_id: str | None = None) -> list[dict[str, Any]]:
        filters = []
        if video_id:
            filters.append({"term": {"video_id": video_id}})
        body = {
            "size": limit,
            "_source": {"excludes": ["vector"]},
            "query": {"bool": {"filter": filters, "must": [{"match_all": {}}]}},
            "sort": [{"updated_at": {"order": "desc", "unmapped_type": "date"}}],
        }
        _, payload = self.request("POST", f"{index}/_search", body, ok={200})
        return [hit.get("_source", {}) for hit in payload.get("hits", {}).get("hits", [])]

    def upsert_doc(self, index: str, doc_id: str, doc: dict[str, Any]) -> None:
        self.request("PUT", f"{index}/_doc/{doc_id}", doc, ok={200, 201})

    def refresh(self, index: str) -> None:
        self.request("POST", f"{index}/_refresh", None, ok={200})


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


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate video-library embeddings and write them to a protected personal ES index.")
    parser.add_argument("--source-index", default=DEFAULT_SOURCE_INDEX)
    parser.add_argument("--target-index", default=DEFAULT_TARGET_INDEX)
    parser.add_argument("--owner", default=DEFAULT_OWNER)
    parser.add_argument("--video-id")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--query")
    parser.add_argument("--query-vector-output")
    parser.add_argument("--embedding-provider")
    parser.add_argument("--embedding-model")
    parser.add_argument("--dimensions", type=int)
    parser.add_argument("--allow-shared-index", action="store_true")
    parser.add_argument("--config")
    parser.add_argument("--output")
    args = parser.parse_args()

    config = load_config(args.config)
    provider, model, dims, normalize_embeddings = embedding_config(args, config)
    try:
        embedder = Embedder(provider, model, dims, normalize_embeddings)
    except RuntimeError as exc:
        text = str(exc)
        code, _, message = text.partition(":")
        return emit(failed(code, message.strip() or text, False, {"provider": provider, "model": model}), args.output)

    if args.query:
        vector = embedder.encode(args.query)
        payload = {"query": args.query, "vector": vector, "embedding_provider": provider, "embedding_model": model, "dimensions": len(vector)}
        if args.query_vector_output:
            Path(args.query_vector_output).parent.mkdir(parents=True, exist_ok=True)
            Path(args.query_vector_output).write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return emit(success(payload), args.output)

    if not safe_target_index(args.target_index, args.owner, args.allow_shared_index):
        return emit(failed("UNSAFE_TARGET_INDEX", f"Refusing to write to non-personal index: {args.target_index}. Use an index prefixed with {args.owner}- or pass --allow-shared-index."), args.output)

    try:
        es = EsClient(config)
        vector_dims = dims or (1024 if model == "BAAI/bge-m3" else len(embedder.encode("dimension probe")))
        es.ensure_index(args.target_index, vector_dims)
        docs = es.fetch_docs(args.source_index, args.limit, args.video_id)
    except EsError as exc:
        return emit(failed(exc.code, exc.message, exc.retryable, exc.detail), args.output)

    documents = []
    failures = []
    for doc in docs:
        try:
            text = build_embedding_text(doc)
            if not text:
                failures.append({"video_id": doc.get("video_id"), "error_code": "EMPTY_EMBEDDING_TEXT"})
                continue
            vector = embedder.encode(text)
            out_doc = compact_for_vector_index(doc)
            out_doc["owner"] = args.owner
            out_doc["embedding_text"] = text
            out_doc["embedding_provider"] = provider
            out_doc["embedding_model"] = model
            out_doc["vector"] = vector
            doc_id = str(out_doc.get("video_id") or hashlib.sha1(text.encode("utf-8")).hexdigest())
            es.upsert_doc(args.target_index, doc_id, out_doc)
            documents.append({"video_id": doc_id, "dimensions": len(vector), "embedding_text_chars": len(text)})
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
        "embedded_count": len(documents),
        "failed_count": len(failures),
        "documents": documents,
        "failures": failures,
    }
    return emit(success(data, 1.0 if not failures else 0.75), args.output)


if __name__ == "__main__":
    raise SystemExit(main())
