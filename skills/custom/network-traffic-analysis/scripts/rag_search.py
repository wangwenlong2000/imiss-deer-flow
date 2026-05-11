#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from collections import Counter
from pathlib import Path
from typing import Any

from utils.path import repo_root, to_repo_relative_display, network_traffic_workspace_root

try:
    import yaml
except ImportError as exc:
    raise ImportError("Missing dependency 'pyyaml'. Install it first: pip install pyyaml") from exc

DEFAULT_MODEL = "text-embedding-v3-large"
LOCAL_PROVIDERS = {"sentence-transformers", "local"}
REMOTE_PROVIDERS = {"openai", "openai-compatible", "dashscope"}


def load_dotenv_file() -> None:
    dotenv_path = repo_root() / ".env"
    if not dotenv_path.exists():
        return
    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ[key] = value


def _cache_root() -> Path:
    env_path = os.environ.get("NETWORK_TRAFFIC_CACHE_ROOT")
    if env_path:
        return Path(env_path)
    ws = network_traffic_workspace_root()
    if ws.exists():
        return ws / ".cache"
    return repo_root() / "datasets" / "network-traffic" / ".cache"


def query_embedding_cache_dir() -> Path:
    return _cache_root() / "query-embeddings"


def query_embedding_cache_path(
    *,
    provider: str,
    model: str,
    normalize: bool,
    query: str,
) -> Path:
    payload = json.dumps(
        {
            "provider": provider,
            "model": model,
            "normalize": normalize,
            "query": query,
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return query_embedding_cache_dir() / f"{digest}.json"


def load_cached_query_embedding(
    *,
    provider: str,
    model: str,
    normalize: bool,
    query: str,
) -> tuple[list[float], str, int] | None:
    cache_path = query_embedding_cache_path(
        provider=provider,
        model=model,
        normalize=normalize,
        query=query,
    )
    if not cache_path.exists():
        return None
    payload = json.loads(cache_path.read_text(encoding="utf-8"))
    vector = payload.get("embedding") or []
    if not vector:
        return None
    return list(vector), str(payload.get("model") or model), int(payload.get("dimensions") or len(vector))


def save_cached_query_embedding(
    *,
    provider: str,
    model: str,
    normalize: bool,
    query: str,
    vector: list[float],
) -> None:
    cache_path = query_embedding_cache_path(
        provider=provider,
        model=model,
        normalize=normalize,
        query=query,
    )
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "provider": provider,
        "model": model,
        "normalize": normalize,
        "query": query,
        "dimensions": len(vector),
        "embedding": vector,
    }
    cache_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def to_repo_relative_display(value: str | Path) -> str:
    path = Path(value).expanduser()
    if not path.is_absolute():
        return path.as_posix()
    try:
        return path.resolve().relative_to(repo_root()).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def resolve_env_value(value: Any) -> Any:
    if isinstance(value, str) and value.startswith("$"):
        return os.getenv(value[1:], "")
    return value


def parse_bool(value: Any, default: bool = True) -> bool:
    if value in (None, ""):
        return default
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    return normalized in {"1", "true", "yes", "y", "on"}


def get_config_path() -> Path:
    configured = os.getenv("DEER_FLOW_CONFIG_PATH")
    if configured:
        path = Path(configured).expanduser()
        if not path.is_absolute():
            path = Path.cwd() / path
        if not path.exists():
            raise FileNotFoundError(f"Config file specified by DEER_FLOW_CONFIG_PATH not found at {path}")
        return path.resolve()
    for candidate in (Path.cwd(), *Path.cwd().parents):
        cfg = candidate / "config.yaml"
        if cfg.exists():
            return cfg.resolve()
    raise FileNotFoundError("config.yaml file not found")


def load_app_config() -> dict[str, Any]:
    config_path = get_config_path()
    with open(config_path, encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    payload["_config_path"] = str(config_path)
    return payload


def resolve_embedding_config(config: dict[str, Any]) -> dict[str, Any]:
    embedding = dict(config.get("embedding") or {})
    dimensions_raw = resolve_env_value(embedding.get("dimensions"))
    dimensions = None
    if dimensions_raw not in (None, "") and str(dimensions_raw).strip().lower() != "none":
        dimensions = int(dimensions_raw)
    api_key = str(resolve_env_value(embedding.get("api_key")) or "")
    if not api_key:
        api_key = str(os.getenv("OPENAI_API_KEY", "") or os.getenv("DASHSCOPE_API_KEY", ""))
    local_model_path = resolve_env_value(embedding.get("local_model_path"))
    if local_model_path and local_model_path.strip():
        p = Path(local_model_path)
        if not p.is_absolute():
            repo_root = Path(config.get("_config_path", ".")).resolve().parent
            local_model_path = str(repo_root / p)
        local_model_path = str(local_model_path) if Path(local_model_path).is_dir() else None
    return {
        "provider": str(resolve_env_value(embedding.get("provider")) or "openai").strip().lower(),
        "model": str(resolve_env_value(embedding.get("model")) or DEFAULT_MODEL),
        "api_key": api_key,
        "base_url": str(resolve_env_value(embedding.get("base_url")) or ""),
        "dimensions": dimensions,
        "device": str(resolve_env_value(embedding.get("device")) or ""),
        "normalize": parse_bool(resolve_env_value(embedding.get("normalize")), True),
        "allow_download": parse_bool(resolve_env_value(embedding.get("allow_download")), False),
        "local_model_path": local_model_path,
        "config_path": str(config.get("_config_path", "")),
    }


def resolve_elasticsearch_config(config: dict[str, Any], cli_overrides: dict[str, Any]) -> dict[str, Any]:
    elasticsearch = dict(config.get("elasticsearch") or {})
    hosts = cli_overrides.get("es_host") or resolve_env_value(elasticsearch.get("hosts")) or "http://localhost:9200"
    if isinstance(hosts, str):
        host_list = [item.strip() for item in hosts.split(",") if item.strip()]
    elif isinstance(hosts, list):
        host_list = [str(resolve_env_value(item)).strip() for item in hosts if str(resolve_env_value(item)).strip()]
    else:
        host_list = []
    username = cli_overrides.get("es_username") or str(resolve_env_value(elasticsearch.get("username")) or "")
    password = cli_overrides.get("es_password") or str(resolve_env_value(elasticsearch.get("password")) or "")
    api_key = cli_overrides.get("es_api_key") or str(resolve_env_value(elasticsearch.get("api_key")) or "")
    index_name = cli_overrides.get("es_index") or str(resolve_env_value(elasticsearch.get("index_name")) or "")
    if not index_name:
        raise ValueError(
            "Elasticsearch index name is required. "
            "Set NETWORK_TRAFFIC_ES_INDEX in .env or config.yaml, or pass --index-name."
        )
    return {
        "hosts": host_list or ["http://localhost:9200"],
        "index_name": index_name,
        "api_key": api_key,
        "username": username,
        "password": password,
        "verify_certs": parse_bool(resolve_env_value(elasticsearch.get("verify_certs")), True),
        "request_timeout": int(resolve_env_value(elasticsearch.get("request_timeout")) or 30),
        "config_path": str(config.get("_config_path", "")),
    }


def load_openai_client(api_key: str, base_url: str | None = None) -> Any:
    try:
        from openai import OpenAI
    except ImportError as exc:
        raise ImportError("Missing dependency 'openai'. Install it before using remote embeddings: pip install openai") from exc
    kwargs: dict[str, Any] = {"api_key": api_key}
    if base_url:
        kwargs["base_url"] = base_url
    return OpenAI(**kwargs)


def require_sentence_transformers() -> None:
    try:
        import sentence_transformers  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            "Missing dependency 'sentence-transformers'. Install it before using local embeddings: pip install sentence-transformers"
        ) from exc


def _load_model_from_path(model_name: str, **kwargs):
    """Load a SentenceTransformer model, reloading modules if needed."""
    from sentence_transformers import SentenceTransformer
    try:
        return SentenceTransformer(model_name, **kwargs)
    except Exception:
        for module_name in list(sys.modules):
            if module_name == "sentence_transformers" or module_name.startswith("sentence_transformers."):
                sys.modules.pop(module_name, None)
        require_sentence_transformers()
        from sentence_transformers import SentenceTransformer as RefreshedSentenceTransformer
        return RefreshedSentenceTransformer(model_name, **kwargs)


def _local_model_path(model_name: str) -> str | None:
    """Check .models/ directory under project root for a local copy."""
    repo_root = Path(__file__).resolve().parent.parent.parent.parent.parent
    candidate = repo_root / ".models" / model_name.split("/")[-1]
    return str(candidate) if candidate.is_dir() else None


def load_sentence_transformer(
    model_name: str,
    *,
    device: str | None = None,
    local_model_path: str | None = None,
    allow_download: bool = False,
) -> Any:
    kwargs: dict[str, Any] = {}
    if device:
        kwargs["device"] = device

    # 1. Try configured local_model_path first (from config.yaml)
    if local_model_path and Path(local_model_path).is_dir():
        return _load_model_from_path(local_model_path, **kwargs)

    # 2. Try HF local cache without network
    prev = os.environ.pop("HF_HUB_OFFLINE", None)
    os.environ["HF_HUB_OFFLINE"] = "1"
    try:
        return _load_model_from_path(model_name, **kwargs)
    except Exception as offline_error:
        if not allow_download:
            raise RuntimeError(
                "Local model/cache unavailable and embedding.allow_download=false. "
                f"model={model_name}, local_model_path={local_model_path or ''}"
            ) from offline_error
    finally:
        if prev is None:
            os.environ.pop("HF_HUB_OFFLINE", None)
        else:
            os.environ["HF_HUB_OFFLINE"] = prev

    # 3. Cache miss and no local copy — allow download from HuggingFace Hub
    return _load_model_from_path(model_name, **kwargs)


def load_elasticsearch_client(config: dict[str, Any]) -> Any:
    try:
        from elasticsearch import Elasticsearch
    except ImportError as exc:
        raise ImportError(
            "Missing dependency 'elasticsearch'. Install it before searching RAG docs: pip install elasticsearch"
        ) from exc
    kwargs: dict[str, Any] = {
        "hosts": config["hosts"],
        "verify_certs": config["verify_certs"],
        "request_timeout": config["request_timeout"],
    }
    if config["api_key"]:
        kwargs["api_key"] = config["api_key"]
    elif config["username"] and config["password"]:
        kwargs["basic_auth"] = (config["username"], config["password"])
    return Elasticsearch(**kwargs)


def sanitize_name(value: str) -> str:
    filtered = "".join(ch if ch.isalnum() or ch in "._-" else "-" for ch in value.strip())
    return filtered.strip("-._") or "dataset"


def normalize_dataset_name(value: str) -> str:
    normalized = sanitize_name(value)
    lowered = normalized.lower()
    if lowered.endswith(".flow.csv"):
        return sanitize_name(normalized[:-9])
    if lowered.endswith(".packet.csv"):
        return sanitize_name(normalized[:-11])
    if lowered.endswith(".pcapng"):
        return sanitize_name(normalized[:-7])
    if lowered.endswith(".pcap"):
        return sanitize_name(normalized[:-5])
    if lowered.endswith(".cap"):
        return sanitize_name(normalized[:-4])
    return normalized


def detect_dataset_hints(query: str) -> list[str]:
    matches = re.findall(r"([A-Za-z0-9._-]+\.(?:pcapng|pcap|cap|flow\.csv|packet\.csv))", query, flags=re.IGNORECASE)
    hints: list[str] = []
    for match in matches:
        dataset = sanitize_name(match)
        if dataset.endswith(".flow.csv"):
            dataset = sanitize_name(dataset[:-9])
        elif dataset.endswith(".packet.csv"):
            dataset = sanitize_name(dataset[:-11])
        else:
            dataset = sanitize_name(Path(dataset).stem)
        if dataset and dataset not in hints:
            hints.append(dataset)
    return hints


def infer_query_strategy(query: str) -> tuple[str, list[str]]:
    lowered = query.lower()
    protocol_terms = ["dns", "tls", "http", "sni", "host", "协议", "特征", "域名"]
    short_terms = ["short connection", "short-connection", "rst", "syn", "短连接", "会话异常", "握手异常", "重置"]
    scan_terms = ["scan", "scan-source", "probe", "扫描", "探测", "可疑扫描源", "端口扫描"]
    peak_terms = ["peak", "timeseries", "volume-spike", "峰值", "高峰", "时间序列", "流量峰值"]
    profile_terms = ["profile", "overview", "主要通信类型", "通信类型", "整体画像", "整体概况", "通信模式", "流量画像", "总体特征"]

    has_protocol = any(token in lowered for token in protocol_terms)
    has_short = any(token in lowered for token in short_terms)
    has_scan = any(token in lowered for token in scan_terms)
    has_peak = any(token in lowered for token in peak_terms)
    has_profile = any(token in lowered for token in profile_terms)

    if has_profile and (has_short or has_scan or has_peak):
        return "profile-with-anomaly", ["anomaly_summary", "endpoint_summary", "port_summary", "flow_summary"]
    if has_protocol:
        return "protocol-feature", ["protocol_summary", "port_summary", "flow_summary"]
    if has_short:
        return "short-connection", ["anomaly_summary", "endpoint_summary", "flow_summary"]
    if has_scan:
        return "scan-source", ["anomaly_summary", "endpoint_summary", "flow_summary"]
    if has_peak:
        return "traffic-peak", ["anomaly_summary", "endpoint_summary"]
    if has_profile:
        return "traffic-profile", ["endpoint_summary", "port_summary", "anomaly_summary", "flow_summary"]
    return "general", []

def embed_query_remote(query: str, embedding_config: dict[str, Any]) -> tuple[list[float], str, int]:
    cached = load_cached_query_embedding(
        provider=embedding_config["provider"],
        model=embedding_config["model"],
        normalize=embedding_config["normalize"],
        query=query,
    )
    if cached is not None:
        return cached
    api_key = embedding_config["api_key"]
    if not api_key:
        raise ValueError("No embedding API key resolved. Set embedding.api_key in config.yaml or configure the provider key in .env.")
    client = load_openai_client(api_key, embedding_config.get("base_url") or None)
    params: dict[str, Any] = {
        "model": embedding_config["model"],
        "input": [query],
    }
    if embedding_config["dimensions"] is not None:
        params["dimensions"] = embedding_config["dimensions"]
    response = client.embeddings.create(**params)
    vector = response.data[0].embedding
    save_cached_query_embedding(
        provider=embedding_config["provider"],
        model=embedding_config["model"],
        normalize=embedding_config["normalize"],
        query=query,
        vector=vector,
    )
    return vector, embedding_config["model"], len(vector)


def embed_query_local(query: str, embedding_config: dict[str, Any]) -> tuple[list[float], str, int]:
    if embedding_config["dimensions"] is not None:
        raise ValueError("Local sentence-transformers models do not support overriding dimensions. Leave embedding.dimensions empty.")
    cached = load_cached_query_embedding(
        provider=embedding_config["provider"],
        model=embedding_config["model"],
        normalize=embedding_config["normalize"],
        query=query,
    )
    if cached is not None:
        return cached
    model = load_sentence_transformer(
        embedding_config["model"],
        device=embedding_config["device"] or None,
        local_model_path=embedding_config.get("local_model_path"),
        allow_download=embedding_config["allow_download"],
    )
    vectors = model.encode(
        [query],
        normalize_embeddings=embedding_config["normalize"],
        show_progress_bar=False,
    )
    vector = vectors[0]
    vector_list = vector.tolist() if hasattr(vector, "tolist") else list(vector)
    save_cached_query_embedding(
        provider=embedding_config["provider"],
        model=embedding_config["model"],
        normalize=embedding_config["normalize"],
        query=query,
        vector=vector_list,
    )
    return vector_list, embedding_config["model"], len(vector_list)


def build_filter_clauses(
    *,
    dataset_names: list[str],
    source_file: str | None,
    schema_version: str | None,
    action_name: str | None,
    severity: str | None,
    entity_ip: str | None,
    domain: str | None,
    doc_types: list[str],
) -> list[dict[str, Any]]:
    filters: list[dict[str, Any]] = []
    if doc_types:
        filters.append({"terms": {"doc_type": doc_types}})
    if dataset_names:
        filters.append({"terms": {"dataset_name": dataset_names}})
    if source_file:
        normalized = source_file.strip()
        if normalized:
            if any(ch in normalized for ch in "*?"):
                filters.append({"wildcard": {"source_file": normalized}})
            else:
                filters.append({"wildcard": {"source_file": f"*{normalized}*"}})
    if schema_version:
        filters.append({"term": {"schema_version": schema_version}})
    if action_name:
        filters.append({"term": {"action_name": action_name}})
    if severity:
        filters.append({"term": {"severity": severity}})
    if entity_ip:
        filters.append({
            "bool": {
                "should": [
                    {"term": {"src_ips": entity_ip}},
                    {"term": {"dst_ips": entity_ip}},
                ]
            }
        })
    if domain:
        filters.append({"term": {"domains": domain}})
    return filters


def build_preference_shoulds(query_text: str, preferred_doc_types: list[str]) -> list[dict[str, Any]]:
    shoulds: list[dict[str, Any]] = [
        {
            "multi_match": {
                "query": query_text,
                "fields": [
                    "title^4",
                    "summary^3",
                    "content^2",
                    "keywords^3",
                ],
            }
        }
    ]
    for doc_type in preferred_doc_types:
        shoulds.append({"term": {"doc_type": {"value": doc_type, "boost": 2.0}}})
    # Soft boost: prefer action_finding and action_evidence over generic flow summaries
    if not preferred_doc_types or "action_finding" in preferred_doc_types:
        shoulds.append({"term": {"doc_type": {"value": "action_finding", "boost": 1.5}}})
    if not preferred_doc_types or "action_evidence" in preferred_doc_types:
        shoulds.append({"term": {"doc_type": {"value": "action_evidence", "boost": 1.3}}})
    # Soft boost: high/critical severity findings
    shoulds.append({"term": {"severity": {"value": "high", "boost": 1.2}}})
    shoulds.append({"term": {"severity": {"value": "critical", "boost": 1.2}}})
    return shoulds


def base_source_fields() -> list[str]:
    return [
        "doc_id",
        "dataset_name",
        "dataset_id",
        "source_file",
        "doc_type",
        "schema_version",
        "title",
        "summary",
        "keywords",
        "metadata",
        "action_name",
        "action_category",
        "finding_id",
        "severity",
        "confidence",
        "risk_score",
        "src_ips",
        "dst_ips",
        "domains",
        "ports",
        "protocols",
        "services",
        "threat_tags",
        "attack_stages",
        "mitre_techniques",
        "ioc_values",
        "ioc_types",
        "row_index",
        "flow_id",
        "time_bucket",
        "provenance_type",
        "raw_source_file",
        "evidence_refs",
        "payload",
        "evidence_id",
        "entity_type",
        "entity_value",
        "risk_level",
        "source_sha256",
        "artifact_generation_id",
    ]


def search_text_index(
    client: Any,
    *,
    index_name: str,
    query_text: str,
    size: int,
    filters: list[dict[str, Any]],
    preferred_doc_types: list[str],
) -> dict[str, Any]:
    body = {
        "size": max(size * 3, 20),
        "_source": base_source_fields(),
        "query": {
            "bool": {
                "filter": filters,
                "should": build_preference_shoulds(query_text, preferred_doc_types),
                "minimum_should_match": 0,
            }
        }
    }
    return client.search(index=index_name, body=body)


def search_vector_index(
    client: Any,
    *,
    index_name: str,
    query_text: str,
    query_vector: list[float],
    size: int,
    filters: list[dict[str, Any]],
    preferred_doc_types: list[str],
) -> dict[str, Any]:
    body = {
        "size": max(size * 3, 20),
        "_source": base_source_fields(),
        "query": {
            "script_score": {
                "query": {
                    "bool": {
                        "filter": filters,
                        "should": build_preference_shoulds(query_text, preferred_doc_types),
                        "minimum_should_match": 0,
                    }
                },
                "script": {
                    "source": "cosineSimilarity(params.query_vector, 'embedding') + 1.0",
                    "params": {"query_vector": query_vector},
                },
            }
        },
    }
    return client.search(index=index_name, body=body)


def shorten(text: str, limit: int = 240) -> str:
    normalized = " ".join(str(text or "").split())
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 3] + "..."


def _risk_of(hit: dict[str, Any]) -> str:
    """Return the risk level of a hit, falling back to 'low' for unknown."""
    source = hit if isinstance(hit, dict) and hit.get("doc_type") else {}
    if not source:
        source = hit.get("_source", {}) if isinstance(hit, dict) else {}
    return (
        source.get("risk_level")
        or source.get("linked_parent_risk_level")
        or source.get("severity", "low")
        or "low"
    ).lower()


def _parent_risk_for(
    linked_flow: dict[str, Any],
    action_hits: list[dict[str, Any]],
) -> str:
    """Return the severity/risk_level of the parent action for a linked flow."""
    parent_action = linked_flow.get("linked_to_action", "")
    parent_finding = linked_flow.get("linked_to_finding", "")
    for h in action_hits:
        src = h.get("_source", {})
        action_name = src.get("action_name", "")
        finding_id = src.get("finding_id", "")
        # Priority: finding_id exact match > action_name match
        if parent_finding and parent_finding == finding_id:
            return (src.get("severity") or src.get("risk_level", "info")).lower()
    for h in action_hits:
        src = h.get("_source", {})
        action_name = src.get("action_name", "")
        if parent_action and parent_action == action_name:
            return (src.get("severity") or src.get("risk_level", "info")).lower()
    return "info"


def format_hits(hits: list[dict[str, Any]]) -> list[dict[str, Any]]:
    formatted: list[dict[str, Any]] = []
    for item in hits:
        source = item.get("_source", {})
        hit = {
            "score": item.get("_score", 0.0),
            "doc_id": source.get("doc_id", ""),
            "doc_type": source.get("doc_type", ""),
            "schema_version": source.get("schema_version", ""),
            "title": source.get("title", ""),
            "summary": source.get("summary", ""),
            "dataset_name": source.get("dataset_name", ""),
            "source_file": source.get("source_file", ""),
            "keywords": source.get("keywords", []),
            "metadata": source.get("metadata", {}),
        }
        # v2 action taxonomy fields
        for key in (
            "action_name", "finding_id", "severity", "confidence",
            "src_ips", "dst_ips", "dst_port", "service", "timestamp",
            "domains", "risk_score", "risk_level",
            "entity_type", "entity_value", "evidence_refs",
            "row_index", "flow_id", "time_bucket", "provenance_type",
            "raw_source_file", "linked_to_action", "linked_to_finding",
            "linked_parent_risk_level",
        ):
            val = source.get(key)
            if val not in (None, "", []):
                hit[key] = val
        formatted.append(hit)
    return formatted


def fuse_hits(
    *,
    text_hits: list[dict[str, Any]],
    vector_hits: list[dict[str, Any]],
    preferred_doc_types: list[str],
    size: int,
) -> list[dict[str, Any]]:
    combined: dict[str, dict[str, Any]] = {}
    rrf_k = 60.0

    def apply(channel_hits: list[dict[str, Any]], channel: str) -> None:
        for rank, item in enumerate(channel_hits, start=1):
            source = item.get("_source", {})
            doc_id = source.get("doc_id", "")
            if not doc_id:
                continue
            entry = combined.setdefault(
                doc_id,
                {
                    "_source": source,
                    "_score": 0.0,
                    "_channel_scores": {},
                },
            )
            score = 1.0 / (rrf_k + rank)
            if source.get("doc_type", "") in preferred_doc_types:
                score += 0.01
            entry["_score"] += score
            entry["_channel_scores"][channel] = score

    apply(text_hits, "text")
    apply(vector_hits, "vector")
    ranked = sorted(
        combined.values(),
        key=lambda item: (
            item["_score"],
            item["_source"].get("doc_type", "") in preferred_doc_types,
            item["_source"].get("title", ""),
        ),
        reverse=True,
    )
    return ranked[:size]


def _extract_flow_filter_from_hit(hit: dict[str, Any]) -> dict[str, Any] | None:
    """Extract flow_filter from an action_finding/action_evidence hit.

    Checks payload.flow_filter first, then falls back to top-level entities.
    Returns None if no actionable entity filters exist.
    """
    payload = hit.get("_source", {}).get("payload", {})
    flow_filter = payload.get("flow_filter", {})
    if not flow_filter:
        return None

    # Check if there are actual entity conditions beyond dataset/source
    has_entities = bool(
        flow_filter.get("primary_entity")
        or flow_filter.get("dst_ips")
        or flow_filter.get("domains")
        or flow_filter.get("ports")
        or flow_filter.get("services")
    )
    if not has_entities:
        # Fallback: try top-level entity fields on the hit
        src_ips = hit.get("_source", {}).get("src_ips", [])
        domains = hit.get("_source", {}).get("domains", [])
        ports = hit.get("_source", {}).get("ports", [])
        services = hit.get("_source", {}).get("services", [])
        if not src_ips and not domains and not ports and not services:
            return None
        flow_filter = {
            "dataset_name": flow_filter.get("dataset_name", hit.get("_source", {}).get("dataset_name", "")),
            "source_file": flow_filter.get("source_file", hit.get("_source", {}).get("source_file", "")),
        }
        if src_ips:
            flow_filter["primary_entity"] = {"type": "src_ips", "values": src_ips[:5]}
        if domains:
            flow_filter["domains"] = domains[:5]
        if ports:
            flow_filter["ports"] = [str(p) for p in ports[:10]]
        if services:
            flow_filter["services"] = services[:5]

    return flow_filter


def _normalize_service_for_match(value: str) -> list[str]:
    """Expand a service value to aliases that may appear in flow_summary metadata.app_protocol."""
    normalized = value.strip().upper()
    aliases = {
        "SSLV3": ["SSL", "TLS", "SSLV3"],
        "TLSV1": ["SSL", "TLS", "TLSV1"],
        "TLSV1.2": ["SSL", "TLS", "TLSV1.2"],
        "TLSV1.3": ["SSL", "TLS", "TLSV1.3"],
        "HTTPS": ["SSL", "HTTP", "HTTPS"],
        "DNS QUERY": ["DNS"],
        "HTTP HOST": ["HTTP"],
    }
    return aliases.get(normalized, [value, normalized])


def _build_linked_flow_query(
    flow_filter: dict[str, Any],
    *,
    size: int = 5,
) -> dict[str, Any]:
    """Build an ES query for flow_summary docs matching a flow_filter."""
    must_clauses: list[dict[str, Any]] = [
        {"term": {"doc_type": "flow_summary"}},
    ]

    if flow_filter.get("dataset_name"):
        must_clauses.append({"term": {"dataset_name": flow_filter["dataset_name"]}})

    filter_clauses: list[dict[str, Any]] = []
    primary = flow_filter.get("primary_entity", {})
    if primary.get("type") == "src_ip" and primary.get("value"):
        filter_clauses.append({"term": {"metadata.src_ip": primary["value"]}})
    elif primary.get("type") == "src_ips" and primary.get("values"):
        filter_clauses.append({"terms": {"metadata.src_ip": primary["values"]}})

    for dst_ip in flow_filter.get("dst_ips", [])[:5]:
        filter_clauses.append({"term": {"metadata.dst_ip": dst_ip}})
    for domain in flow_filter.get("domains", [])[:5]:
        filter_clauses.append({"term": {"domains": domain}})
    for port in flow_filter.get("ports", [])[:10]:
        try:
            port_int = int(port)
            filter_clauses.append({"term": {"metadata.dst_port": port_int}})
        except (ValueError, TypeError):
            pass
    for service in flow_filter.get("services", [])[:5]:
        svc_values = _normalize_service_for_match(service)
        if svc_values:
            filter_clauses.append({"terms": {"metadata.app_protocol": svc_values}})

    if filter_clauses:
        must_clauses.append({"bool": {"should": filter_clauses, "minimum_should_match": 1}})

    return {
        "size": size,
        "_source": [
            "doc_id", "doc_type", "title", "summary", "dataset_name",
            "source_file", "metadata", "raw_source_file",
        ],
        "query": {"bool": {"must": must_clauses}},
    }


def _search_linked_flows(
    client: Any,
    index_name: str,
    action_hits: list[dict[str, Any]],
    *,
    per_hit_limit: int = 3,
    total_limit: int = 20,
) -> list[dict[str, Any]]:
    """Second-hop retrieval: look up flow_summary docs linked to action hits.

    For each action_finding/action_evidence hit, extract flow_filter entities
    and query for matching flows. Deduplicate by doc_id. Cap at total_limit.
    """
    seen_ids: set[str] = set()
    linked: list[dict[str, Any]] = []

    for hit in action_hits:
        if len(linked) >= total_limit:
            break

        flow_filter = _extract_flow_filter_from_hit(hit)
        if not flow_filter:
            continue

        query = _build_linked_flow_query(flow_filter, size=per_hit_limit)
        try:
            response = client.search(index=index_name, body=query)
        except Exception:
            continue

        for item in response.get("hits", {}).get("hits", []):
            source = item.get("_source", {})
            doc_id = source.get("doc_id", "")
            if not doc_id or doc_id in seen_ids:
                continue
            seen_ids.add(doc_id)
            meta = source.get("metadata", {})
            linked.append({
                "doc_id": doc_id,
                "doc_type": "flow_summary",
                "title": source.get("title", ""),
                "summary": shorten(source.get("summary", ""), 120),
                "dataset_name": source.get("dataset_name", ""),
                "source_file": source.get("source_file", ""),
                "src_ips": [meta.get("src_ip", "")],
                "dst_ips": [meta.get("dst_ip", "")] if meta.get("dst_ip") else [],
                "dst_port": meta.get("dst_port"),
                "service": meta.get("app_protocol", ""),
                "timestamp": meta.get("time_bucket", ""),
                "linked_to_action": hit.get("_source", {}).get("action_name", ""),
                "linked_to_finding": hit.get("_source", {}).get("finding_id", ""),
                "raw_source_file": source.get("raw_source_file", ""),
            })

    return linked


def build_text_output(result: dict[str, Any]) -> str:
    lines = [
        f"查询：{result['query']}",
        f"索引：{result['index_name']}",
        f"检索策略：{result.get('retrieval_strategy', 'unknown')}",
        f"推断意图：{result['intent']}",
        f"文档类型偏好：{', '.join(result['doc_types']) if result['doc_types'] else '未限制'}",
        f"数据集过滤：{', '.join(result['dataset_names']) if result['dataset_names'] else '未限制'}",
        f"命中数量：{result['hit_count']}",
        f"embedding provider：{result['embedding_provider']}",
        f"embedding model：{result['embedding_model']}",
    ]
    for idx, hit in enumerate(result['hits'], start=1):
        provenance_parts = []
        if hit.get('provenance_type'):
            provenance_parts.append(hit['provenance_type'])
        if hit.get('source_file'):
            provenance_parts.append(hit['source_file'])
        if hit.get('row_index') not in (None, ''):
            provenance_parts.append(f"row={hit['row_index']}")

        lines.extend(
            [
                '',
                f"[{idx}] {hit['title']}",
                f"  doc_type: {hit['doc_type']}",
                f"  score: {hit['score']:.4f}",
                f"  dataset_name: {hit['dataset_name']}",
                f"  source_file: {hit['source_file']}",
                f"  provenance: {' | '.join(provenance_parts) if provenance_parts else 'unknown'}",
                f"  summary: {shorten(hit['summary'])}",
            ]
        )
    return "\n".join(lines)

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Search the network traffic RAG Elasticsearch index.")
    parser.add_argument("--query", required=True, help="Natural-language search query")
    parser.add_argument("--index-name", default=None, help="Override Elasticsearch index name")
    parser.add_argument("--dataset-name", action="append", default=[], help="Restrict search to a dataset name. Can be specified multiple times")
    parser.add_argument("--dataset", action="append", default=[], help="Alias of --dataset-name")
    parser.add_argument("--source-file", default=None, help="Restrict search to source_file. Supports substring matching")
    parser.add_argument("--doc-type", action="append", default=[], help="Restrict to one or more doc_type values")
    parser.add_argument("--schema-version", default=None, help="Restrict to schema_version (e.g. rag_doc_v2)")
    parser.add_argument("--action", default=None, help="Restrict to action_name (e.g. signature-review)")
    parser.add_argument("--severity", default=None, help="Restrict to severity (info|low|medium|high|critical)")
    parser.add_argument("--entity-ip", default=None, help="Restrict to documents containing this IP in src_ips or dst_ips")
    parser.add_argument("--domain", default=None, help="Restrict to documents containing this domain in domains")
    parser.add_argument("--size", type=int, default=5, help="Maximum number of hits to return")
    parser.add_argument("--format", choices=["text", "json"], default="text", help="Output format")
    parser.add_argument("--risk-level", action="append", default=[], help="Filter by risk level (can specify multiple: low, medium, high, critical)")
    parser.add_argument("--explain-route", action="store_true", help="Include retrieval channel scores and routing explanation in output")
    parser.add_argument("--es-host", default=None, help="Override Elasticsearch host(s)")
    parser.add_argument("--es-username", default=None, help="Override Elasticsearch username")
    parser.add_argument("--es-password", default=None, help="Override Elasticsearch password")
    parser.add_argument("--es-api-key", default=None, help="Override Elasticsearch API key")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        load_dotenv_file()
        config = load_app_config()
        embedding_config = resolve_embedding_config(config)
        es_cli_overrides = {
            "es_host": args.es_host,
            "es_username": args.es_username,
            "es_password": args.es_password,
            "es_api_key": args.es_api_key,
            "es_index": args.index_name,
        }
        elasticsearch_config = resolve_elasticsearch_config(config, cli_overrides=es_cli_overrides)
        index_name = args.index_name or elasticsearch_config["index_name"]

        inferred_intent, inferred_doc_types = infer_query_strategy(args.query)
        dataset_names = [normalize_dataset_name(value) for value in (args.dataset_name + args.dataset) if value]
        for value in detect_dataset_hints(args.query):
            if value not in dataset_names:
                dataset_names.append(value)
        explicit_doc_types = [value.strip() for value in args.doc_type if value.strip()]
        preferred_doc_types = explicit_doc_types or inferred_doc_types

        provider = embedding_config["provider"]
        if provider in REMOTE_PROVIDERS:
            query_vector, embedding_model, embedding_dimensions = embed_query_remote(args.query, embedding_config)
        elif provider in LOCAL_PROVIDERS:
            query_vector, embedding_model, embedding_dimensions = embed_query_local(args.query, embedding_config)
        else:
            raise ValueError(
                f"Unsupported embedding provider '{provider}'. Expected one of: {', '.join(sorted(LOCAL_PROVIDERS | REMOTE_PROVIDERS))}."
            )

        client = load_elasticsearch_client(elasticsearch_config)
        filters = build_filter_clauses(
            dataset_names=dataset_names,
            source_file=args.source_file,
            schema_version=args.schema_version,
            action_name=args.action,
            severity=args.severity,
            entity_ip=args.entity_ip,
            domain=args.domain,
            doc_types=explicit_doc_types,
        )
        text_response = search_text_index(
            client,
            index_name=index_name,
            query_text=args.query,
            size=args.size,
            filters=filters,
            preferred_doc_types=preferred_doc_types,
        )
        vector_response = search_vector_index(
            client,
            index_name=index_name,
            query_text=args.query,
            query_vector=query_vector,
            size=args.size,
            filters=filters,
            preferred_doc_types=preferred_doc_types,
        )
        fused_hits = fuse_hits(
            text_hits=text_response.get("hits", {}).get("hits", []),
            vector_hits=vector_response.get("hits", {}).get("hits", []),
            preferred_doc_types=preferred_doc_types,
            size=args.size,
        )
        all_formatted = format_hits(fused_hits)

        # Two-hop linked flow retrieval: extract action hits from the raw
        # fused set, look up matching flows, then inject into the formatted list.
        linked_flows: list[dict[str, Any]] = []
        action_hits = [
            h for h in fused_hits
            if h.get("_source", {}).get("doc_type", "") in ("action_finding", "action_evidence")
        ]
        if action_hits:
            linked_flows = _search_linked_flows(
                client, index_name, action_hits,
                per_hit_limit=5, total_limit=30,
            )
            if linked_flows:
                for lf in linked_flows:
                    lf["linked_parent_risk_level"] = _parent_risk_for(lf, action_hits)
                fused_hits.extend([{"_source": lf, "_score": 0.0} for lf in linked_flows])
                all_formatted = format_hits(fused_hits)

        # Risk-level filter
        risk_levels = [r.lower() for r in args.risk_level if r.strip()]
        if risk_levels:
            risk_level_order = {"low": 0, "medium": 1, "high": 2, "critical": 3}
            min_score = min(risk_level_order.get(r, 0) for r in risk_levels)
            filtered = []
            for hit in all_formatted:
                rl = _risk_of(hit)
                if risk_level_order.get(rl, 0) >= min_score:
                    filtered.append(hit)
            all_formatted = filtered

        # Separate linked_flows from the rest for clean grouping
        linked_flow_hits: list[dict[str, Any]] = []
        primary_hits: list[dict[str, Any]] = []
        for hit in all_formatted:
            source = hit if isinstance(hit, dict) and hit.get("doc_type") else {}
            if source.get("linked_to_action"):
                linked_flow_hits.append(hit)
            else:
                primary_hits.append(hit)

        # Retrieval coverage
        text_hits = text_response.get("hits", {}).get("hits", [])
        vec_hits = vector_response.get("hits", {}).get("hits", [])
        retrieval_coverage = {
            "text_hits": len(text_hits),
            "vector_hits": len(vec_hits),
            "final_hits": len(all_formatted),
            "linked_flows": len(linked_flow_hits),
            "channels_used": [ch for ch in ["text", "vector"] if (text_hits if ch == "text" else vec_hits)],
            "doc_type_counts": dict(Counter(h.get("doc_type", "unknown") for h in all_formatted)),
            "provenance_type_counts": dict(Counter(h.get("provenance_type", "empty") for h in all_formatted)),
            "dataset_counts": dict(Counter(h.get("dataset_name", "unknown") for h in all_formatted)),
        }

        # Groups by doc_type (linked flows get their own group)
        groups: dict[str, list[str]] = {}
        for hit in primary_hits:
            key = hit.get("doc_type", "unknown")
            if key not in groups:
                groups[key] = []
            groups[key].append(hit.get("doc_id", ""))
        if linked_flow_hits:
            groups["linked_flows"] = [h.get("doc_id", "") for h in linked_flow_hits]

        result = {
            "query": args.query,
            "index_name": index_name,
            "intent": inferred_intent,
            "dataset_names": dataset_names,
            "doc_types": preferred_doc_types,
            "hit_count": len(all_formatted),
            "retrieval_strategy": "hybrid-text-plus-vector",
            "retrieval_coverage": retrieval_coverage,
            "groups": groups,
            "embedding_provider": provider,
            "embedding_model": embedding_model,
            "embedding_dimensions": embedding_dimensions,
            "config_path": to_repo_relative_display(embedding_config["config_path"]) if embedding_config["config_path"] else "",
            "hits": all_formatted,
        }
        if args.explain_route:
            result["explain_route"] = {
                "text_channel": len(text_hits),
                "vector_channel": len(vec_hits),
                "fused_before_dedup": len(fused_hits),
                "fused_after_dedup": len(all_formatted),
            }
        if args.format == "json":
            print(json.dumps(result, ensure_ascii=False))
        else:
            print(build_text_output(result))
        return 0
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
