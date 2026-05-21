#!/usr/bin/env python3
"""Shared Elasticsearch helpers for code RAG scripts."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any

try:
    from elasticsearch import Elasticsearch
except ImportError:  # pragma: no cover - exercised in minimal test envs
    Elasticsearch = None  # type: ignore[assignment]


def add_es_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--es-url", default=None, help="Elasticsearch URL")
    parser.add_argument("--es-username", default=None, help="Basic auth username")
    parser.add_argument("--es-password", default=None, help="Basic auth password")
    parser.add_argument("--es-api-key", default=None, help="API key")


def load_config(path: str | None) -> dict[str, Any]:
    if path is None:
        return {}
    try:
        import yaml
    except ImportError as exc:  # pragma: no cover - depends on optional local env
        raise RuntimeError("pyyaml is required for --config. Install with: pip install pyyaml") from exc

    with Path(path).expanduser().open(encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}
    if not isinstance(config, dict):
        raise ValueError(f"Config file must contain a mapping: {path}")
    return config


def config_section(config: dict[str, Any], name: str) -> dict[str, Any]:
    value = config.get(name) or {}
    if not isinstance(value, dict):
        raise ValueError(f"Config section must be a mapping: {name}")
    return value


def add_config_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", default=None, help="Path to YAML config file")


def build_es_client(args: argparse.Namespace) -> Elasticsearch:
    if Elasticsearch is None:
        raise RuntimeError("elasticsearch is required. Install with: pip install 'elasticsearch>=8,<9'")

    hosts = args.es_url or os.getenv("ES_URL", "http://localhost:9200")
    api_key = args.es_api_key or os.getenv("ES_API_KEY")
    username = args.es_username or os.getenv("ES_USERNAME")
    password = args.es_password or os.getenv("ES_PASSWORD")

    if api_key:
        return Elasticsearch(hosts=hosts, api_key=api_key)
    if username and password:
        return Elasticsearch(hosts=hosts, basic_auth=(username, password))
    return Elasticsearch(hosts=hosts)
