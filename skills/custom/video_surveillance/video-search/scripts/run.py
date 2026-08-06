#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import sys
import os
import re
import shutil
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

try:
    import yaml
except Exception:
    yaml = None

SKILL = "video-search"
VERSION = "1.2.0"
DEFAULT_INDEX = "citybrain-video-library"
DEFAULT_STREETMODEL_VECTOR_FIELD = "video_vector-Qwen3-VL-Embedding-2B_urban_governance"
DEFAULT_STREETMODEL_BASE_URL = "http://219.245.185.245:3130"
DEFAULT_STREETMODEL_MODEL = "Qwen3-VL-Embedding-2B"
DEFAULT_STREETMODEL_DIMS = 2048
DEFAULT_STREETMODEL_BATCH_SIZE = 1
DEFAULT_STREETMODEL_TIMEOUT = 600
DEFAULT_STREETMODEL_TEXT_INSTRUCTION = "Represent this surveillance/street-view query for urban scene retrieval."
DEFAULT_STREETMODEL_IMAGE_INSTRUCTION = "Represent this query image for retrieving semantically similar surveillance videos."
DEFAULT_LLM_TIMEOUT = 60
DEFAULT_RRF_K = 60
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".gif", ".tif", ".tiff"}
PROXY_ENV_KEYS = ("http_proxy", "https_proxy", "all_proxy", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY")


def require_video_index(index: str) -> str:
    if index != DEFAULT_INDEX:
        raise ValueError(f"Only {DEFAULT_INDEX} is allowed for video search; received {index}")
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


def resolve_config_path(path: str | None) -> str | None:
    if path:
        return path
    env_path = os.getenv("DEER_FLOW_CONFIG_PATH")
    if env_path and Path(env_path).is_file():
        return env_path
    local_path = Path.cwd() / "config.yaml"
    return str(local_path) if local_path.is_file() else None


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


class LlmQueryError(RuntimeError):
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
        candidates = [
            ("environment:ES_URL", os.getenv("ES_URL")),
            ("config:video_library.elasticsearch.url", nested_es.get("url")),
            ("config:video_library.elasticsearch.hosts[0]", nested_hosts[0] if nested_hosts else None),
            ("config:elasticsearch.url", es_config.get("url")),
            ("config:elasticsearch.hosts[0]", hosts[0] if hosts else None),
        ]
        source, url = next(
            ((source, str(value).strip()) for source, value in candidates if value and str(value).strip()),
            ("unconfigured", ""),
        )
        self.url = url.rstrip("/")
        self.config_source = source
        self.config_path = config.get("_video_search_config_path")
        self.username = os.getenv("ES_USERNAME") or nested_es.get("username") or es_config.get("username")
        self.password = os.getenv("ES_PASSWORD") or nested_es.get("password") or es_config.get("password")

    def diagnostics(self) -> dict[str, Any]:
        detail = {
            "url": self.url or None,
            "config_source": self.config_source,
            "auth_configured": bool(self.username and self.password),
            "username_configured": bool(self.username),
            "password_configured": bool(self.password),
        }
        if self.config_path:
            detail["config_path"] = self.config_path
        return detail

    def request(self, method: str, path: str, body: Any | None = None, ok: set[int] | None = None) -> tuple[int, Any]:
        ok = ok or {200}
        if not self.url:
            raise EsError(
                "ES_NOT_CONFIGURED",
                "Elasticsearch is not configured. Inject ES_URL, ES_USERNAME, and ES_PASSWORD into the sandbox or pass --config.",
                False,
                {
                    **self.diagnostics(),
                    "required_environment": ["ES_URL", "ES_USERNAME", "ES_PASSWORD"],
                },
            )
        req = urllib.request.Request(f"{self.url}/{path.lstrip('/')}", data=None if body is None else json.dumps(body, ensure_ascii=False).encode("utf-8"), method=method, headers={"Content-Type": "application/json"})
        if self.username and self.password:
            token = base64.b64encode(f"{self.username}:{self.password}".encode("utf-8")).decode("ascii")
            req.add_header("Authorization", f"Basic {token}")
        try:
            with es_urlopen(req, timeout_seconds=20) as resp:
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
            raise EsError(
                code,
                f"Elasticsearch request failed: HTTP {exc.code}",
                exc.code >= 500,
                {**self.diagnostics(), "path": path, "response": payload},
            )
        except urllib.error.URLError as exc:
            raise EsError(
                "ES_CONNECTION_FAILED",
                f"Could not connect to Elasticsearch at {self.url}: {exc.reason}",
                True,
                {
                    **self.diagnostics(),
                    "hint": "For an AIO sandbox, use a host-reachable URL such as http://host.docker.internal:3128 rather than localhost.",
                },
            )
        except TimeoutError as exc:
            raise EsError(
                "ES_CONNECTION_FAILED",
                f"Elasticsearch request timed out at {self.url}: {exc}",
                True,
                self.diagnostics(),
            )

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
    env_field = os.getenv("VIDEO_SEARCH_VECTOR_FIELD")
    if env_field:
        return env_field
    if cfg:
        return str(cfg.get("vector_field") or DEFAULT_STREETMODEL_VECTOR_FIELD)
    return "vector"


def streetmodel_config(args: argparse.Namespace, config: dict[str, Any]) -> dict[str, Any]:
    cfg = config.get("streetmodel_embedding", {}) if isinstance(config.get("streetmodel_embedding"), dict) else {}
    image_cfg = cfg.get("image", {}) if isinstance(cfg.get("image"), dict) else {}
    video_cfg = cfg.get("video", {}) if isinstance(cfg.get("video"), dict) else {}
    return {
        "base_url": str(args.base_url or os.getenv("STREETMODEL_BASE_URL") or cfg.get("base_url") or DEFAULT_STREETMODEL_BASE_URL).rstrip("/"),
        "model_name": str(args.embedding_model or cfg.get("model_name") or DEFAULT_STREETMODEL_MODEL),
        "dimensions": int(args.dimensions or cfg.get("dimensions") or DEFAULT_STREETMODEL_DIMS),
        "batch_size": int(cfg.get("batch_size") or DEFAULT_STREETMODEL_BATCH_SIZE),
        "timeout_seconds": int(args.timeout_seconds or cfg.get("timeout_seconds") or DEFAULT_STREETMODEL_TIMEOUT),
        "text_instruction": str(cfg.get("text_instruction") or DEFAULT_STREETMODEL_TEXT_INSTRUCTION),
        "image_instruction": str(image_cfg.get("instruction") or cfg.get("image_instruction") or DEFAULT_STREETMODEL_IMAGE_INSTRUCTION),
        "image_deerflow_path_prefix": str(
            getattr(args, "image_deerflow_path_prefix", None)
            or image_cfg.get("deerflow_path_prefix")
            or video_cfg.get("deerflow_path_prefix")
            or ""
        ),
        "image_streetmodel_path_prefix": str(
            getattr(args, "image_streetmodel_path_prefix", None)
            or image_cfg.get("streetmodel_path_prefix")
            or video_cfg.get("streetmodel_path_prefix")
            or ""
        ),
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


def es_urlopen(req: urllib.request.Request, timeout_seconds: int):
    removed = {key: os.environ.pop(key) for key in PROXY_ENV_KEYS if key in os.environ}
    try:
        return urllib.request.urlopen(req, timeout=timeout_seconds)
    finally:
        os.environ.update(removed)


def resolve_env_value(value: Any) -> str:
    if value is None:
        return ""
    raw = str(value).strip()
    resolved = os.path.expandvars(raw).strip()
    if raw.startswith("$") and resolved == raw:
        return ""
    return resolved


def llm_config(args: argparse.Namespace, config: dict[str, Any]) -> dict[str, Any]:
    search_cfg = config.get("video_search", {}) if isinstance(config.get("video_search"), dict) else {}
    llm_cfg = search_cfg.get("llm_query_optimization", {}) if isinstance(search_cfg.get("llm_query_optimization"), dict) else {}
    models = [item for item in config.get("models", []) if isinstance(item, dict)] if isinstance(config.get("models"), list) else []
    requested = str(args.llm_model or os.getenv("VIDEO_SEARCH_LLM_MODEL") or llm_cfg.get("model") or "").strip()
    selected = next(
        (
            item
            for item in models
            if requested and requested in {str(item.get("name") or ""), str(item.get("model") or "")}
        ),
        None,
    )
    if selected is None and models and not requested:
        selected = models[0]
    model = str((selected or {}).get("model") or requested).strip()
    base_url = resolve_env_value(
        args.llm_base_url
        or os.getenv("VIDEO_SEARCH_LLM_BASE_URL")
        or llm_cfg.get("base_url")
        or (selected or {}).get("base_url")
        or (selected or {}).get("api_base")
    ).rstrip("/")
    api_key = resolve_env_value(
        args.llm_api_key
        or os.getenv("VIDEO_SEARCH_LLM_API_KEY")
        or llm_cfg.get("api_key")
        or (selected or {}).get("api_key")
    )
    timeout_seconds = int(args.llm_timeout_seconds or llm_cfg.get("timeout_seconds") or DEFAULT_LLM_TIMEOUT)
    if not model or not base_url:
        raise LlmQueryError(
            "LLM_QUERY_OPTIMIZER_NOT_CONFIGURED",
            "A configured chat model and base_url are required for LLM query optimization",
            False,
            {"requested_model": requested or None},
        )
    if not api_key:
        raise LlmQueryError(
            "LLM_QUERY_OPTIMIZER_NOT_CONFIGURED",
            "The configured chat model API key is unavailable",
            False,
            {"model": model, "base_url": base_url},
        )
    return {
        "model": model,
        "base_url": base_url,
        "api_key": api_key,
        "timeout_seconds": timeout_seconds,
    }


def strip_markdown_fence(text: str) -> str:
    value = text.strip()
    if not value.startswith("```"):
        return value
    lines = value.splitlines()
    if len(lines) >= 3 and lines[-1].strip() == "```":
        return "\n".join(lines[1:-1]).strip()
    return value


def parse_json_object(text: str) -> dict[str, Any]:
    candidate = strip_markdown_fence(text)
    start = candidate.find("{")
    end = candidate.rfind("}")
    if start >= 0 and end > start:
        candidate = candidate[start : end + 1]
    value = json.loads(candidate)
    if not isinstance(value, dict):
        raise ValueError("response is not an object")
    return value


def response_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict) and item.get("type") == "text" and isinstance(item.get("text"), str):
                parts.append(item["text"])
        return "\n".join(parts)
    return "" if content is None else str(content)


class LlmQueryOptimizer:
    def __init__(self, model: str, base_url: str, api_key: str, timeout_seconds: int):
        self.model = model
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds

    def optimize(self, query: str, extracted_description: str) -> dict[str, str]:
        prompt = (
            "You are a query planner for surveillance-video retrieval. Do not answer the request.\n"
            "Extract only observable visual content from the user's text. Preserve explicitly stated objects, "
            "counts, colors, actions, environment, time, weather, and location. Do not invent facts.\n"
            "Return JSON only with exactly these string fields:\n"
            '- "description": the core visual description with command words removed;\n'
            '- "keyword_query": concise lexical search terms suitable for Elasticsearch;\n'
            '- "semantic_query": one complete natural visual-scene description optimized for text-to-video embedding.\n'
            "Use the same language as the user.\n\n"
            f"Original request: {query}\n"
            f"Initial extracted description: {extracted_description}\n"
        )
        body = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0,
            "max_tokens": 500,
        }
        endpoint = self.base_url if self.base_url.endswith("/chat/completions") else f"{self.base_url}/chat/completions"
        req = urllib.request.Request(
            endpoint,
            data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
            method="POST",
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_seconds) as resp:
                payload = json.loads(resp.read().decode("utf-8", errors="replace"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise LlmQueryError(
                "LLM_QUERY_OPTIMIZATION_FAILED",
                f"LLM query optimization failed: HTTP {exc.code}",
                exc.code >= 500,
                {"response": detail[-2000:]},
            )
        except urllib.error.URLError as exc:
            raise LlmQueryError(
                "LLM_QUERY_OPTIMIZATION_FAILED",
                f"Could not connect to the configured LLM service: {exc.reason}",
                True,
                {"base_url": self.base_url, "model": self.model},
            )
        except TimeoutError as exc:
            raise LlmQueryError(
                "LLM_QUERY_OPTIMIZATION_FAILED",
                f"LLM query optimization timed out: {exc}",
                True,
                {"base_url": self.base_url, "model": self.model},
            )
        try:
            content = payload["choices"][0]["message"]["content"]
            result = parse_json_object(response_text(content))
            description = str(result.get("description") or extracted_description).strip()
            keyword_query = str(result.get("keyword_query") or description).strip()
            semantic_query = str(result.get("semantic_query") or description).strip()
            if not description or not keyword_query or not semantic_query:
                raise ValueError("response contains empty query fields")
            return {
                "description": description,
                "keyword_query": keyword_query,
                "semantic_query": semantic_query,
            }
        except (KeyError, IndexError, TypeError, ValueError) as exc:
            raise LlmQueryError(
                "LLM_QUERY_OPTIMIZATION_INVALID_RESPONSE",
                f"LLM returned an invalid query plan: {exc}",
                False,
                {"response": payload},
            )


class StreetModelEmbedder:
    def __init__(
        self,
        base_url: str,
        model: str,
        dims: int,
        batch_size: int,
        timeout_seconds: int,
        text_instruction: str,
        image_instruction: str = DEFAULT_STREETMODEL_IMAGE_INSTRUCTION,
    ):
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.dims = dims
        self.batch_size = batch_size
        self.timeout_seconds = timeout_seconds
        self.text_instruction = text_instruction
        self.image_instruction = image_instruction
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
        except TimeoutError as exc:
            raise EmbeddingError(
                "STREETMODEL_CONNECTION_FAILED",
                f"StreetModel request timed out at {self.base_url}: {exc}",
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

    def encode_item(self, item: dict[str, Any], instruction: str) -> list[float]:
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
        return self.encode_item({"type": "text", "content": text}, self.text_instruction)

    def encode_image(self, image_uri: str) -> list[float]:
        return self.encode_item({"type": "image", "uri": image_uri}, self.image_instruction)


StreetModelTextEmbedder = StreetModelEmbedder


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


def extract_search_description(query: str) -> str:
    value = re.sub(r"\s+", " ", query).strip(" \t\r\n，。！？!?；;：:")
    patterns = [
        r"^(?:请|麻烦)?(?:帮我|给我)?(?:查找|搜索|检索|查询|找出|找一下|搜一下)(?:一下)?",
        r"^(?:please\s+)?(?:find|search(?:\s+for)?|retrieve|look\s+for)\s+",
    ]
    for pattern in patterns:
        value = re.sub(pattern, "", value, flags=re.IGNORECASE).strip()
    value = re.sub(r"(?:相关的?)?(?:监控)?视频(?:片段)?$", "", value).strip(" \t\r\n，。！？!?；;：:")
    return value or query.strip()


def is_remote_image_uri(value: str) -> bool:
    return urllib.parse.urlparse(value).scheme.lower() in {"http", "https"}


def path_is_within(path: Path, prefix: Path) -> bool:
    try:
        path.resolve().relative_to(prefix.resolve())
        return True
    except ValueError:
        return False


def map_shared_image_path(image_path: Path, deerflow_prefix: str, streetmodel_prefix: str) -> str | None:
    if streetmodel_prefix and path_is_within(image_path, Path(streetmodel_prefix)):
        return str(image_path)
    if deerflow_prefix and streetmodel_prefix and path_is_within(image_path, Path(deerflow_prefix)):
        relative = image_path.resolve().relative_to(Path(deerflow_prefix).resolve())
        return str(Path(streetmodel_prefix) / relative)
    return None


def materialize_image_uri(
    image_uri: str,
    deerflow_prefix: str,
    streetmodel_prefix: str,
    copy_to_shared: bool,
) -> tuple[str, str | None]:
    if is_remote_image_uri(image_uri):
        return image_uri, None
    source = Path(image_uri).expanduser()
    mapped = map_shared_image_path(source, deerflow_prefix, streetmodel_prefix)
    if mapped:
        return mapped, None
    if not source.exists() or not source.is_file():
        raise EmbeddingError(
            "IMAGE_SOURCE_NOT_FOUND",
            f"Query image was not found: {source}",
            False,
            {"image_uri": str(source)},
        )
    if source.suffix.lower() not in IMAGE_EXTENSIONS:
        raise EmbeddingError(
            "UNSUPPORTED_IMAGE_FORMAT",
            f"Unsupported query image format: {source.suffix or 'unknown'}",
            False,
            {"supported_extensions": sorted(IMAGE_EXTENSIONS)},
        )
    if not copy_to_shared:
        raise EmbeddingError(
            "IMAGE_URI_NOT_MAPPABLE",
            "The local query image is not visible to StreetModel; use --copy-image-to-shared or a mapped/remote URI",
            False,
            {"image_uri": str(source)},
        )
    if not deerflow_prefix or not streetmodel_prefix:
        raise EmbeddingError(
            "IMAGE_SHARED_PATH_NOT_CONFIGURED",
            "Both image DeerFlow and StreetModel path prefixes are required to copy a local query image",
            False,
        )
    digest = hashlib.sha1(str(source.resolve()).encode("utf-8")).hexdigest()[:12]
    target_dir = Path(deerflow_prefix) / "query_images"
    try:
        target_dir.mkdir(parents=True, exist_ok=True)
        target = target_dir / f"{digest}-{source.name}"
        shutil.copy2(source, target)
    except OSError as exc:
        raise EmbeddingError(
            "IMAGE_COPY_TO_SHARED_FAILED",
            f"Could not copy query image into the shared StreetModel path: {exc}",
            False,
            {"source": str(source), "target_dir": str(target_dir)},
        )
    mapped = map_shared_image_path(target, deerflow_prefix, streetmodel_prefix)
    if not mapped:
        raise EmbeddingError(
            "IMAGE_URI_NOT_MAPPABLE",
            "Copied query image could not be mapped to the StreetModel path",
            False,
            {"copied_path": str(target)},
        )
    return mapped, str(target)


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


def vector_search_body(
    vector: list[float],
    vector_field: str,
    base_query: dict[str, Any],
    top_k: int,
) -> dict[str, Any]:
    field_literal = painless_string_literal(vector_field)
    return {
        "size": top_k,
        "_source": {"excludes": source_excludes(vector_field)},
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


def keyword_search_body(query: str | None, filters: list[dict[str, Any]], vector_field: str, top_k: int) -> dict[str, Any]:
    return {
        "size": top_k,
        "_source": {"excludes": source_excludes(vector_field)},
        "query": build_keyword_query(query, filters),
    }


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
        "tags": source.get("tags", []),
        "description": source.get("description"),
        "content_text": source.get("content_text"),
        "object_summary": source.get("object_summary", {}),
        "location": source.get("location", {}),
        "metadata": source.get("metadata", {}),
    }


def payload_hits(payload: dict[str, Any]) -> tuple[list[dict[str, Any]], int]:
    hits = [compact_hit(hit) for hit in payload.get("hits", {}).get("hits", [])]
    total_raw = payload.get("hits", {}).get("total", {})
    total = total_raw.get("value") if isinstance(total_raw, dict) else total_raw
    return hits, int(total or len(hits))


def merge_ranked_hits(
    keyword_hits: list[dict[str, Any]],
    vector_hits: list[dict[str, Any]],
    top_k: int,
    rrf_k: int = DEFAULT_RRF_K,
) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for source_name, hits in (("keyword", keyword_hits), ("vector", vector_hits)):
        for rank, hit in enumerate(hits, start=1):
            key = str(hit.get("video_id") or hit.get("raw_segment_uri") or f"{source_name}:{rank}")
            item = merged.setdefault(
                key,
                {
                    **hit,
                    "rrf_score": 0.0,
                    "retrieval_sources": [],
                },
            )
            item["rrf_score"] += 1.0 / (rrf_k + rank)
            if source_name not in item["retrieval_sources"]:
                item["retrieval_sources"].append(source_name)
            if hit.get("score") is not None:
                item[f"{source_name}_score"] = hit["score"]
    ranked = sorted(merged.values(), key=lambda item: (-float(item["rrf_score"]), str(item.get("video_id") or "")))
    for item in ranked:
        item["rrf_score"] = round(float(item["rrf_score"]), 8)
    return ranked[:top_k]


def resolve_search_mode(args: argparse.Namespace, vector: list[float] | None, config: dict[str, Any]) -> str:
    if args.search_mode:
        return args.search_mode
    if args.image_uri:
        return "semantic"
    if vector is not None:
        return "semantic" if args.vector_query_mode == "semantic" else "hybrid"
    if args.embedding_provider == "streetmodel":
        return "hybrid" if args.vector_query_mode == "hybrid" else "semantic"
    if args.query:
        search_cfg = config.get("video_search", {}) if isinstance(config.get("video_search"), dict) else {}
        configured = str(search_cfg.get("default_text_search_mode") or "dual")
        return configured if configured in {"keyword", "semantic", "dual", "hybrid"} else "dual"
    return "keyword"


def resolve_scoring_field(es: EsClient, index: str, requested_field: str) -> str | None:
    if es.mapping_has_vector(index, requested_field):
        return requested_field
    if requested_field != "vector" and es.mapping_has_vector(index, "vector"):
        return "vector"
    return None


def embedding_metadata(street_cfg: dict[str, Any], vector: list[float], modality: str) -> dict[str, Any]:
    return {
        "embedding_provider": "streetmodel",
        "embedding_model": street_cfg["model_name"],
        "dimensions": len(vector),
        "modality": modality,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Search the Elasticsearch video library with text, image, keyword, and multimodal vector queries.")
    parser.add_argument("--query")
    parser.add_argument("--image", "--image-uri", dest="image_uri")
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
    parser.add_argument("--search-mode", choices=["keyword", "semantic", "dual", "hybrid"])
    parser.add_argument("--llm-model")
    parser.add_argument("--llm-base-url")
    parser.add_argument("--llm-api-key")
    parser.add_argument("--llm-timeout-seconds", type=int)
    parser.add_argument("--no-llm-query-optimization", action="store_true")
    parser.add_argument("--copy-image-to-shared", action="store_true")
    parser.add_argument("--image-deerflow-path-prefix")
    parser.add_argument("--image-streetmodel-path-prefix")
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--config")
    parser.add_argument("--output")
    args = parser.parse_args()
    try:
        args.index = require_video_index(args.index)
    except ValueError as exc:
        return emit(failed("UNSUPPORTED_VIDEO_INDEX", str(exc), False, {"allowed_index": DEFAULT_INDEX, "received_index": args.index}), args.output)
    if args.image_uri and (args.query or args.query_vector_json):
        return emit(
            failed(
                "AMBIGUOUS_SEARCH_INPUT",
                "Provide either an image query or a text/precomputed-vector query, not both",
                False,
            ),
            args.output,
        )
    config_path = resolve_config_path(args.config)
    config = load_config(config_path)
    if config_path:
        config["_video_search_config_path"] = config_path
    es = EsClient(config)
    filters = build_filters(args)
    vector = load_vector(args.query_vector_json)
    vector_field = vector_field_config(args, config)
    search_mode = resolve_search_mode(args, vector, config)
    query_embedding: dict[str, Any] | None = None
    query_plan: dict[str, Any] | None = None
    image_details: dict[str, Any] | None = None

    if search_mode == "keyword":
        description = extract_search_description(args.query) if args.query else None
        try:
            payload = es.search(args.index, keyword_search_body(description, filters, vector_field, args.top_k))
        except EsError as exc:
            return emit(failed(exc.code, exc.message, exc.retryable, exc.detail), args.output)
        hits, total = payload_hits(payload)
        return emit(
            success(
                {
                    "hits": hits,
                    "total": total,
                    "query_mode": "keyword_filter",
                    "search_mode": "keyword",
                    "input_type": "text" if args.query else "filters",
                    "original_query": args.query,
                    "description": description,
                    "index": args.index,
                    "vector_field": vector_field,
                }
            ),
            args.output,
        )

    keyword_hits: list[dict[str, Any]] = []
    keyword_total = 0
    description = extract_search_description(args.query) if args.query else None
    if search_mode == "dual":
        if not args.query:
            return emit(failed("MISSING_QUERY", "Dual text retrieval requires --query"), args.output)

    semantic_query = description
    if args.image_uri:
        if args.embedding_provider not in {None, "streetmodel"}:
            return emit(
                failed(
                    "UNSUPPORTED_IMAGE_EMBEDDING_PROVIDER",
                    "Image queries require --embedding-provider streetmodel",
                    False,
                    {"provider": args.embedding_provider},
                ),
                args.output,
            )
        try:
            street_cfg = streetmodel_config(args, config)
            model_image_uri, copied_image_path = materialize_image_uri(
                args.image_uri,
                street_cfg["image_deerflow_path_prefix"],
                street_cfg["image_streetmodel_path_prefix"],
                args.copy_image_to_shared,
            )
            embedder = StreetModelEmbedder(
                base_url=street_cfg["base_url"],
                model=street_cfg["model_name"],
                dims=street_cfg["dimensions"],
                batch_size=street_cfg["batch_size"],
                timeout_seconds=street_cfg["timeout_seconds"],
                text_instruction=street_cfg["text_instruction"],
                image_instruction=street_cfg["image_instruction"],
            )
            vector = embedder.encode_image(model_image_uri)
            query_embedding = embedding_metadata(street_cfg, vector, "image")
            image_details = {
                "source_uri": args.image_uri,
                "embedding_uri": model_image_uri,
                "copied_path": copied_image_path,
            }
        except EmbeddingError as exc:
            return emit(failed(exc.code, exc.message, exc.retryable, exc.detail), args.output)
    elif vector is None:
        if not args.query:
            return emit(failed("MISSING_QUERY", "Semantic text retrieval requires --query or --query-vector-json"), args.output)
        legacy_direct_query = args.search_mode is None and args.embedding_provider == "streetmodel"
        if args.no_llm_query_optimization or legacy_direct_query:
            query_plan = {
                "description": description,
                "keyword_query": description,
                "semantic_query": description,
                "optimization_status": "legacy_direct" if legacy_direct_query else "disabled",
            }
        else:
            try:
                optimizer_cfg = llm_config(args, config)
                optimizer = LlmQueryOptimizer(**optimizer_cfg)
                query_plan = optimizer.optimize(args.query, str(description or args.query))
                query_plan["optimization_status"] = "success"
                query_plan["llm_model"] = optimizer_cfg["model"]
            except LlmQueryError as exc:
                if search_mode != "dual":
                    return emit(failed(exc.code, exc.message, exc.retryable, exc.detail), args.output)
                try:
                    keyword_payload = es.search(args.index, keyword_search_body(description, filters, vector_field, args.top_k))
                    keyword_hits, keyword_total = payload_hits(keyword_payload)
                except EsError as search_exc:
                    return emit(failed(search_exc.code, search_exc.message, search_exc.retryable, search_exc.detail), args.output)
                data = {
                    "hits": keyword_hits,
                    "total": keyword_total,
                    "query_mode": "dual_keyword_only",
                    "search_mode": "dual",
                    "input_type": "text",
                    "overall_status": "partial_success",
                    "original_query": args.query,
                    "description": description,
                    "index": args.index,
                    "vector_field": vector_field,
                    "retrievals": {
                        "keyword": {
                            "status": "success",
                            "query": description,
                            "hits": keyword_hits,
                            "total": keyword_total,
                        },
                        "vector": {
                            "status": "failed",
                            "error_code": exc.code,
                            "message": exc.message,
                            "detail": exc.detail,
                        },
                    },
                }
                return emit(success(data, confidence=0.75), args.output)
        semantic_query = str(query_plan["semantic_query"])
        if search_mode == "dual":
            try:
                keyword_payload = es.search(
                    args.index,
                    keyword_search_body(str(query_plan["keyword_query"]), filters, vector_field, args.top_k),
                )
                keyword_hits, keyword_total = payload_hits(keyword_payload)
            except EsError as exc:
                return emit(failed(exc.code, exc.message, exc.retryable, exc.detail), args.output)
        try:
            street_cfg = streetmodel_config(args, config)
            embedder = StreetModelEmbedder(
                base_url=street_cfg["base_url"],
                model=street_cfg["model_name"],
                dims=street_cfg["dimensions"],
                batch_size=street_cfg["batch_size"],
                timeout_seconds=street_cfg["timeout_seconds"],
                text_instruction=street_cfg["text_instruction"],
                image_instruction=street_cfg["image_instruction"],
            )
            vector = embedder.encode_text(semantic_query)
            query_embedding = embedding_metadata(street_cfg, vector, "text")
        except EmbeddingError as exc:
            if search_mode != "dual":
                return emit(failed(exc.code, exc.message, exc.retryable, exc.detail), args.output)
            data = {
                "hits": keyword_hits,
                "total": keyword_total,
                "query_mode": "dual_keyword_only",
                "search_mode": "dual",
                "input_type": "text",
                "overall_status": "partial_success",
                "original_query": args.query,
                "description": query_plan.get("description") if query_plan else description,
                "query_plan": query_plan,
                "index": args.index,
                "vector_field": vector_field,
                "retrievals": {
                    "keyword": {
                        "status": "success",
                        "query": query_plan.get("keyword_query") if query_plan else description,
                        "hits": keyword_hits,
                        "total": keyword_total,
                    },
                    "vector": {
                        "status": "failed",
                        "query": semantic_query,
                        "error_code": exc.code,
                        "message": exc.message,
                        "detail": exc.detail,
                    },
                },
            }
            return emit(success(data, confidence=0.75), args.output)

    if vector is None:
        return emit(failed("MISSING_QUERY_VECTOR", "No query vector could be resolved"), args.output)

    try:
        scoring_field = resolve_scoring_field(es, args.index, vector_field)
    except EsError as exc:
        return emit(failed(exc.code, exc.message, exc.retryable, exc.detail), args.output)

    if not scoring_field:
        if args.image_uri or search_mode == "dual":
            error = {
                "status": "failed",
                "error_code": "VECTOR_FIELD_UNAVAILABLE",
                "message": f"No dense vector field is available for {vector_field}",
            }
            if search_mode == "dual":
                return emit(
                    success(
                        {
                            "hits": keyword_hits,
                            "total": keyword_total,
                            "query_mode": "dual_keyword_only",
                            "search_mode": "dual",
                            "input_type": "text",
                            "overall_status": "partial_success",
                            "original_query": args.query,
                            "query_plan": query_plan,
                            "index": args.index,
                            "vector_field": vector_field,
                            "retrievals": {
                                "keyword": {
                                    "status": "success",
                                    "query": query_plan.get("keyword_query") if query_plan else description,
                                    "hits": keyword_hits,
                                    "total": keyword_total,
                                },
                                "vector": error,
                            },
                        },
                        confidence=0.75,
                    ),
                    args.output,
                )
            return emit(failed(error["error_code"], error["message"], False, {"vector_field": vector_field}), args.output)
        fallback_query = str(query_plan.get("keyword_query") if query_plan else description or args.query or "")
        try:
            payload = es.search(args.index, keyword_search_body(fallback_query, filters, vector_field, args.top_k))
        except EsError as exc:
            return emit(failed(exc.code, exc.message, exc.retryable, exc.detail), args.output)
        hits, total = payload_hits(payload)
        return emit(
            success(
                {
                    "hits": hits,
                    "total": total,
                    "query_mode": "keyword_filter_vector_unavailable",
                    "search_mode": search_mode,
                    "input_type": "text",
                    "original_query": args.query,
                    "query_plan": query_plan,
                    "index": args.index,
                    "vector_field": vector_field,
                },
                confidence=0.75,
            ),
            args.output,
        )

    vector_base_query = (
        build_keyword_query(str(query_plan.get("keyword_query") if query_plan else description or args.query or ""), filters)
        if search_mode == "hybrid"
        else build_filter_query(filters)
    )
    try:
        vector_payload = es.search(args.index, vector_search_body(vector, scoring_field, vector_base_query, args.top_k))
    except EsError as exc:
        if search_mode != "dual":
            return emit(failed(exc.code, exc.message, exc.retryable, exc.detail), args.output)
        return emit(
            success(
                {
                    "hits": keyword_hits,
                    "total": keyword_total,
                    "query_mode": "dual_keyword_only",
                    "search_mode": "dual",
                    "input_type": "text",
                    "overall_status": "partial_success",
                    "original_query": args.query,
                    "query_plan": query_plan,
                    "index": args.index,
                    "vector_field": scoring_field,
                    "retrievals": {
                        "keyword": {
                            "status": "success",
                            "query": query_plan.get("keyword_query") if query_plan else description,
                            "hits": keyword_hits,
                            "total": keyword_total,
                        },
                        "vector": {
                            "status": "failed",
                            "error_code": exc.code,
                            "message": exc.message,
                            "detail": exc.detail,
                        },
                    },
                },
                confidence=0.75,
            ),
            args.output,
        )

    vector_hits, vector_total = payload_hits(vector_payload)
    if search_mode == "dual":
        keyword_query = str(query_plan.get("keyword_query") if query_plan else description or args.query or "")
        hits = merge_ranked_hits(keyword_hits, vector_hits, args.top_k)
        data = {
            "hits": hits,
            "total": len(hits),
            "query_mode": "dual_rrf",
            "search_mode": "dual",
            "input_type": "text",
            "overall_status": "success",
            "original_query": args.query,
            "description": query_plan.get("description") if query_plan else description,
            "query_plan": query_plan,
            "index": args.index,
            "vector_field": scoring_field,
            "retrievals": {
                "keyword": {
                    "status": "success",
                    "query": keyword_query,
                    "hits": keyword_hits,
                    "total": keyword_total,
                },
                "vector": {
                    "status": "success",
                    "query": semantic_query,
                    "hits": vector_hits,
                    "total": vector_total,
                },
            },
        }
    else:
        query_mode = "image_vector_filter" if args.image_uri else ("vector_keyword_filter" if search_mode == "hybrid" else "vector_filter")
        data = {
            "hits": vector_hits,
            "total": vector_total,
            "query_mode": query_mode,
            "search_mode": search_mode,
            "input_type": "image" if args.image_uri else ("precomputed_vector" if args.query_vector_json else "text"),
            "original_query": args.query,
            "query_plan": query_plan,
            "index": args.index,
            "vector_field": scoring_field,
        }
    if query_embedding:
        data["query_embedding"] = query_embedding
    if image_details:
        data["image"] = image_details
    return emit(success(data), args.output)


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
