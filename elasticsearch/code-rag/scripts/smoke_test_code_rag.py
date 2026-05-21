#!/usr/bin/env python3
"""Smoke tests for Elasticsearch-backed code RAG.

Modes:
- local: validate chunking and embedding-text construction without ES/model.
- connection: validate Elasticsearch connectivity.
- end-to-end: index a temporary mini repository and retrieve from it.
"""

from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import yaml

from code_chunker import build_chunks_for_file, chunk_to_document
from code_embedding import build_embedding_text, embed_texts
from code_indexer import index_chunks
from code_retrieve_topk import retrieve
from es_common import build_es_client


def load_config(path: str | None) -> dict[str, Any]:
    if path is None:
        return {}
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def section(config: dict[str, Any], name: str) -> dict[str, Any]:
    value = config.get(name) or {}
    if not isinstance(value, dict):
        raise ValueError(f"config section must be a mapping: {name}")
    return value


def es_namespace(config: dict[str, Any]) -> SimpleNamespace:
    es = section(config, "elasticsearch")
    return SimpleNamespace(
        es_url=es.get("url"),
        es_username=es.get("username"),
        es_password=es.get("password"),
        es_api_key=es.get("api_key"),
    )


def local_smoke() -> dict[str, Any]:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source = root / "backend" / "tools.py"
        source.parent.mkdir(parents=True)
        source.write_text(
            "\n".join(
                [
                    "import ast",
                    "from langchain.tools import tool",
                    "",
                    "def code_search_tool(query):",
                    "    return query",
                ]
            ),
            encoding="utf-8",
        )
        chunks = build_chunks_for_file(source, root_path=root, repo="smoke")
        function = next(chunk for chunk in chunks if chunk.kind == "function")
        embedding_text = build_embedding_text(chunk_to_document(function))
        return {
            "status": "ok",
            "mode": "local",
            "chunks": len(chunks),
            "function_symbol": function.symbol,
            "metadata": function.metadata,
            "embedding_text_preview": embedding_text[:300],
        }


def connection_smoke(config: dict[str, Any]) -> dict[str, Any]:
    es = build_es_client(es_namespace(config))
    info = es.info()
    indices = es.cat.indices(format="json", h="index,docs.count,health,status")
    return {
        "status": "ok",
        "mode": "connection",
        "cluster": dict(info.body if hasattr(info, "body") else info).get("cluster_name"),
        "indices": list(indices.body if hasattr(indices, "body") else indices)[:20],
    }


def end_to_end_smoke(config: dict[str, Any]) -> dict[str, Any]:
    es_cfg = section(config, "elasticsearch")
    index_cfg = section(config, "index")
    embedding_cfg = section(config, "embedding")
    retrieval_cfg = section(config, "retrieval")

    index_name = index_cfg.get("name", "code_chunks_smoke")
    if index_name == "code_chunks":
        index_name = "code_chunks_smoke"

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source = root / "backend" / "community" / "code_rag" / "tools.py"
        source.parent.mkdir(parents=True)
        source.write_text(
            "\n".join(
                [
                    "from langchain.tools import tool",
                    "",
                    "@tool('code_search')",
                    "def code_search_tool(query: str) -> str:",
                    "    return query",
                ]
            ),
            encoding="utf-8",
        )

        model = embedding_cfg.get("model") or "sentence-transformers/all-MiniLM-L6-v2"
        index_args = SimpleNamespace(
            root_path=str(root),
            repo="smoke",
            index=index_name,
            embedding_model=model,
            batch_size=int(index_cfg.get("batch_size", 8)),
            device=embedding_cfg.get("device"),
            model_cache_dir=embedding_cfg.get("model_cache_dir"),
            max_files_scanned=100,
            force=True,
            recreate_index=True,
            es_url=es_cfg.get("url"),
            es_username=es_cfg.get("username"),
            es_password=es_cfg.get("password"),
            es_api_key=es_cfg.get("api_key"),
        )
        index_result = index_chunks(index_args)

        retrieve_args = SimpleNamespace(
            query="where is code_search_tool implemented",
            index=index_name,
            repo="smoke",
            k=int(retrieval_cfg.get("k", 3)),
            window_size=int(retrieval_cfg.get("window_size", 10)),
            embedding_model=model,
            device=embedding_cfg.get("device"),
            model_cache_dir=embedding_cfg.get("model_cache_dir"),
            language="python",
            kind=None,
            tag=[],
            path_glob=None,
            rank_constant=int(retrieval_cfg.get("rank_constant", 60)),
            include_embedding_text=False,
            es_url=es_cfg.get("url"),
            es_username=es_cfg.get("username"),
            es_password=es_cfg.get("password"),
            es_api_key=es_cfg.get("api_key"),
        )
        retrieval_result = retrieve(retrieve_args)

    return {
        "status": "ok",
        "mode": "end-to-end",
        "index_result": index_result,
        "top_k": retrieval_result["top_k"],
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Smoke test Elasticsearch code RAG")
    parser.add_argument("--config", default=None, help="Path to config YAML")
    parser.add_argument(
        "--mode",
        choices=["local", "connection", "end-to-end"],
        default="local",
        help="Smoke test mode",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    if args.mode == "local":
        result = local_smoke()
    elif args.mode == "connection":
        result = connection_smoke(config)
    else:
        result = end_to_end_smoke(config)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

