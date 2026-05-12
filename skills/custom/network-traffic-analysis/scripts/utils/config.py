"""Shared configuration loaders for network traffic analysis scripts."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from utils.path import repo_root

try:
    import yaml
except ImportError as exc:
    raise ImportError("Missing dependency 'pyyaml'. Install it first: pip install pyyaml") from exc


DEFAULT_MODEL = "text-embedding-v3-large"


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


def _is_sandbox_environment() -> bool:
    """Return True when running inside a sandbox container where config.yaml is unavailable."""
    return (
        "ES_URL" in os.environ
        or "EMBEDDING_BASE_URL" in os.environ
    )


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
    if _is_sandbox_environment():
        return Path("/dev/null")  # sentinel: env-only config mode
    raise FileNotFoundError("config.yaml file not found")


def load_app_config() -> dict[str, Any]:
    config_path = get_config_path()
    if config_path == Path("/dev/null"):
        # Sandbox mode: no config.yaml on disk, all config from env vars
        return {"_config_path": ""}
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
        api_key = str(os.getenv("EMBEDDING_API_KEY", "")
                      or os.getenv("OPENAI_API_KEY", "")
                      or os.getenv("DASHSCOPE_API_KEY", ""))
    base_url = str(resolve_env_value(embedding.get("base_url")) or "")
    if not base_url:
        base_url = str(os.getenv("EMBEDDING_BASE_URL", ""))
    local_model_path = resolve_env_value(embedding.get("local_model_path"))
    if local_model_path and local_model_path.strip():
        p = Path(local_model_path)
        if not p.is_absolute():
            cfg_root = Path(config.get("_config_path", ".")).resolve().parent
            local_model_path = str(cfg_root / p)
        local_model_path = str(local_model_path) if Path(local_model_path).is_dir() else None
    return {
        "provider": str(resolve_env_value(embedding.get("provider")) or "openai").strip().lower(),
        "model": str(resolve_env_value(embedding.get("model")) or DEFAULT_MODEL),
        "api_key": api_key,
        "base_url": base_url,
        "dimensions": dimensions,
        "device": str(resolve_env_value(embedding.get("device")) or ""),
        "normalize": parse_bool(resolve_env_value(embedding.get("normalize")), True),
        "allow_download": parse_bool(resolve_env_value(embedding.get("allow_download")), False),
        "local_model_path": local_model_path,
        "config_path": str(config.get("_config_path", "")),
    }


def resolve_elasticsearch_config(config: dict[str, Any], cli_overrides: dict[str, Any]) -> dict[str, Any]:
    elasticsearch = dict(config.get("elasticsearch") or {})
    hosts = cli_overrides.get("es_host") or resolve_env_value(elasticsearch.get("hosts"))
    # Sandbox fallback: config.yaml is unavailable, resolve directly from env vars
    if not hosts:
        hosts = os.getenv("ES_URL", "")
    if not hosts:
        raise ValueError(
            "Elasticsearch hosts are required. Set ES_URL in .env or config.yaml, or pass --es-host."
        )
    if isinstance(hosts, str):
        host_list = [item.strip() for item in hosts.split(",") if item.strip()]
    elif isinstance(hosts, list):
        host_list = [str(resolve_env_value(item)).strip() for item in hosts if str(resolve_env_value(item)).strip()]
    else:
        host_list = []
    username = cli_overrides.get("es_username") or str(resolve_env_value(elasticsearch.get("username")) or "")
    if not username:
        username = os.getenv("ES_USERNAME", "")
    password = cli_overrides.get("es_password") or str(resolve_env_value(elasticsearch.get("password")) or "")
    if not password:
        password = os.getenv("ES_PASSWORD", "")
    api_key = cli_overrides.get("es_api_key") or str(resolve_env_value(elasticsearch.get("api_key")) or "")
    index_name = cli_overrides.get("es_index") or str(resolve_env_value(elasticsearch.get("index_name")) or "")
    if not index_name:
        index_name = os.getenv("ES_INDEX", "")
    if not index_name:
        raise ValueError(
            "Elasticsearch index name is required. Set ES_INDEX in .env or config.yaml, or pass --index-name."
        )
    return {
        "hosts": host_list,
        "index_name": index_name,
        "api_key": api_key,
        "username": username,
        "password": password,
        "verify_certs": parse_bool(resolve_env_value(elasticsearch.get("verify_certs")), True),
        "request_timeout": int(resolve_env_value(elasticsearch.get("request_timeout")) or 30),
        "config_path": str(config.get("_config_path", "")),
    }
