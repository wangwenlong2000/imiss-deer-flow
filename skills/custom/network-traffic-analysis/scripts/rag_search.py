#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError:
    os.system(f"{sys.executable} -m pip install pyyaml -q")
    import yaml

DEFAULT_INDEX_NAME = "network-traffic-rag"
DEFAULT_MODEL = "text-embedding-v3-large"
LOCAL_PROVIDERS = {"sentence-transformers", "local"}
REMOTE_PROVIDERS = {"openai", "openai-compatible", "dashscope"}


def repo_root() -> Path:
    script_path = Path(__file__).resolve()
    for candidate in script_path.parents:
        if (candidate / "config.yaml").exists():
            return candidate
    return script_path.parents[3]


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


def query_embedding_cache_dir() -> Path:
    return repo_root() / "datasets" / "network-traffic" / ".cache" / "query-embeddings"


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
    cwd = Path.cwd()
    for candidate in (cwd / "config.yaml", cwd.parent / "config.yaml"):
        if candidate.exists():
            return candidate.resolve()
    raise FileNotFoundError("config.yaml file not found in the current directory or its parent directory")


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
    return {
        "provider": str(resolve_env_value(embedding.get("provider")) or "openai").strip().lower(),
        "model": str(resolve_env_value(embedding.get("model")) or DEFAULT_MODEL),
        "api_key": api_key,
        "base_url": str(resolve_env_value(embedding.get("base_url")) or ""),
        "dimensions": dimensions,
        "device": str(resolve_env_value(embedding.get("device")) or ""),
        "normalize": parse_bool(resolve_env_value(embedding.get("normalize")), True),
        "config_path": str(config.get("_config_path", "")),
    }


def resolve_elasticsearch_config(config: dict[str, Any]) -> dict[str, Any]:
    elasticsearch = dict(config.get("elasticsearch") or {})
    hosts = resolve_env_value(elasticsearch.get("hosts")) or "http://localhost:9200"
    if isinstance(hosts, str):
        host_list = [item.strip() for item in hosts.split(",") if item.strip()]
    elif isinstance(hosts, list):
        host_list = [str(resolve_env_value(item)).strip() for item in hosts if str(resolve_env_value(item)).strip()]
    else:
        host_list = []
    return {
        "hosts": host_list or ["http://localhost:9200"],
        "index_name": str(resolve_env_value(elasticsearch.get("index_name")) or DEFAULT_INDEX_NAME),
        "api_key": str(resolve_env_value(elasticsearch.get("api_key")) or ""),
        "username": str(resolve_env_value(elasticsearch.get("username")) or ""),
        "password": str(resolve_env_value(elasticsearch.get("password")) or ""),
        "verify_certs": parse_bool(resolve_env_value(elasticsearch.get("verify_certs")), True),
        "request_timeout": int(resolve_env_value(elasticsearch.get("request_timeout")) or 30),
        "config_path": str(config.get("_config_path", "")),
    }


def load_openai_client(api_key: str, base_url: str | None = None) -> Any:
    try:
        from openai import OpenAI
    except ImportError:
        os.system(f"{sys.executable} -m pip install openai -q")
        from openai import OpenAI
    kwargs: dict[str, Any] = {"api_key": api_key}
    if base_url:
        kwargs["base_url"] = base_url
    return OpenAI(**kwargs)


def install_sentence_transformers() -> None:
    subprocess.check_call(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--upgrade",
            "sentence-transformers",
        ]
    )


def load_sentence_transformer(model_name: str, *, device: str | None = None) -> Any:
    os.environ.setdefault("HF_HUB_DISABLE_PROGRESS_BARS", "1")
    os.environ.setdefault("TRANSFORMERS_VERBOSITY", "error")
    try:
        from sentence_transformers import SentenceTransformer
    except Exception:
        for module_name in list(sys.modules):
            if module_name == "sentence_transformers" or module_name.startswith("sentence_transformers."):
                sys.modules.pop(module_name, None)
        install_sentence_transformers()
        from sentence_transformers import SentenceTransformer
    kwargs: dict[str, Any] = {}
    if device:
        kwargs["device"] = device
    try:
        return SentenceTransformer(model_name, **kwargs)
    except Exception:
        for module_name in list(sys.modules):
            if module_name == "sentence_transformers" or module_name.startswith("sentence_transformers."):
                sys.modules.pop(module_name, None)
        install_sentence_transformers()
        from sentence_transformers import SentenceTransformer as RefreshedSentenceTransformer
        return RefreshedSentenceTransformer(model_name, **kwargs)


def load_elasticsearch_client(config: dict[str, Any]) -> Any:
    try:
        from elasticsearch import Elasticsearch
    except ImportError:
        os.system(f"{sys.executable} -m pip install elasticsearch -q")
        from elasticsearch import Elasticsearch
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
    model = load_sentence_transformer(embedding_config["model"], device=embedding_config["device"] or None)
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
) -> list[dict[str, Any]]:
    filters: list[dict[str, Any]] = []
    if dataset_names:
        filters.append({"terms": {"dataset_name": dataset_names}})
    if source_file:
        normalized = source_file.strip()
        if normalized:
            if any(ch in normalized for ch in "*?"):
                filters.append({"wildcard": {"source_file": normalized}})
            else:
                filters.append({"wildcard": {"source_file": f"*{normalized}*"}})
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
    return shoulds


def base_source_fields() -> list[str]:
    return [
        "doc_id",
        "dataset_name",
        "source_file",
        "doc_type",
        "title",
        "summary",
        "keywords",
        "metadata",
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


def format_hits(hits: list[dict[str, Any]]) -> list[dict[str, Any]]:
    formatted: list[dict[str, Any]] = []
    for item in hits:
        source = item.get("_source", {})
        formatted.append(
            {
                "score": item.get("_score", 0.0),
                "doc_id": source.get("doc_id", ""),
                "doc_type": source.get("doc_type", ""),
                "title": source.get("title", ""),
                "summary": source.get("summary", ""),
                "dataset_name": source.get("dataset_name", ""),
                "source_file": source.get("source_file", ""),
                "keywords": source.get("keywords", []),
                "metadata": source.get("metadata", {}),
            }
        )
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
        lines.extend(
            [
                '',
                f"[{idx}] {hit['title']}",
                f"  doc_type: {hit['doc_type']}",
                f"  score: {hit['score']:.4f}",
                f"  dataset_name: {hit['dataset_name']}",
                f"  source_file: {hit['source_file']}",
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
    parser.add_argument("--size", type=int, default=5, help="Maximum number of hits to return")
    parser.add_argument("--format", choices=["text", "json"], default="text", help="Output format")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        load_dotenv_file()
        config = load_app_config()
        embedding_config = resolve_embedding_config(config)
        elasticsearch_config = resolve_elasticsearch_config(config)
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
        )
        if explicit_doc_types:
            filters.append({"terms": {"doc_type": explicit_doc_types}})
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
        hits = format_hits(fused_hits)
        result = {
            "query": args.query,
            "index_name": index_name,
            "intent": inferred_intent,
            "dataset_names": dataset_names,
            "doc_types": preferred_doc_types,
            "hit_count": len(hits),
            "retrieval_strategy": "hybrid-text-plus-vector",
            "embedding_provider": provider,
            "embedding_model": embedding_model,
            "embedding_dimensions": embedding_dimensions,
            "config_path": to_repo_relative_display(embedding_config["config_path"]) if embedding_config["config_path"] else "",
            "hits": hits,
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
