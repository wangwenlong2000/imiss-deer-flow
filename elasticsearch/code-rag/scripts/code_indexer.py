#!/usr/bin/env python3
"""Index code chunks into Elasticsearch with local open-source embeddings."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from code_chunker import build_chunks, chunk_to_document
from code_embedding import DEFAULT_EMBEDDING_MODEL, build_embedding_text, embed_texts, embedding_dimension
from es_common import add_config_arg, add_es_args, build_es_client, config_section, load_config


DEFAULT_INDEX = "code_chunks"


def vector_field_name(model_name: str) -> str:
    safe = model_name.replace("/", "__").replace("-", "_").replace(".", "_")
    return f"vector-{safe}"


def build_mapping(*, vector_field: str, dims: int) -> dict[str, Any]:
    return {
        "mappings": {
            "properties": {
                "id": {"type": "keyword"},
                "repo": {"type": "keyword"},
                "path": {"type": "keyword"},
                "absolute_path": {"type": "keyword", "index": False},
                "language": {"type": "keyword"},
                "kind": {"type": "keyword"},
                "symbol": {"type": "keyword"},
                "start_line": {"type": "integer"},
                "end_line": {"type": "integer"},
                "imports": {"type": "keyword"},
                "tags": {"type": "keyword"},
                "content_hash": {"type": "keyword"},
                "file_hash": {"type": "keyword"},
                "code": {"type": "text"},
                "embedding_text": {"type": "text"},
                "metadata": {"type": "object", "enabled": True},
                vector_field: {
                    "type": "dense_vector",
                    "dims": dims,
                    "index": True,
                    "similarity": "cosine",
                },
            }
        }
    }


def ensure_index(es: Any, *, index: str, vector_field: str, dims: int, recreate: bool = False) -> None:
    if recreate and es.indices.exists(index=index):
        es.indices.delete(index=index)
    if not es.indices.exists(index=index):
        es.indices.create(index=index, body=build_mapping(vector_field=vector_field, dims=dims))


def existing_file_hashes(es: Any, *, index: str, repo: str) -> dict[str, str]:
    if not es.indices.exists(index=index):
        return {}
    body = {
        "size": 0,
        "query": {"term": {"repo": repo}},
        "aggs": {
            "paths": {
                "terms": {"field": "path", "size": 100_000},
                "aggs": {"file_hash": {"terms": {"field": "file_hash", "size": 1}}},
            }
        },
    }
    response = es.search(index=index, body=body)
    buckets = response.get("aggregations", {}).get("paths", {}).get("buckets", [])
    hashes: dict[str, str] = {}
    for bucket in buckets:
        hash_buckets = bucket.get("file_hash", {}).get("buckets", [])
        if hash_buckets:
            hashes[bucket["key"]] = hash_buckets[0]["key"]
    return hashes


def delete_paths(es: Any, *, index: str, repo: str, paths: set[str]) -> None:
    for path in sorted(paths):
        es.delete_by_query(
            index=index,
            body={"query": {"bool": {"filter": [{"term": {"repo": repo}}, {"term": {"path": path}}]}}},
            conflicts="proceed",
            refresh=False,
        )


def batched(items: list[Any], batch_size: int) -> list[list[Any]]:
    return [items[index : index + batch_size] for index in range(0, len(items), batch_size)]


def index_chunks(args: argparse.Namespace) -> dict[str, Any]:
    try:
        from elasticsearch import helpers
    except ImportError as exc:
        raise RuntimeError("elasticsearch is required. Install with: pip install 'elasticsearch>=8,<9'") from exc

    es = build_es_client(args)
    vector_field = vector_field_name(args.embedding_model)
    dims = embedding_dimension(args.embedding_model, device=args.device, cache_folder=args.model_cache_dir)
    ensure_index(es, index=args.index, vector_field=vector_field, dims=dims, recreate=args.recreate_index)

    root_path = Path(args.root_path).expanduser().resolve()
    chunks = build_chunks(root_path, repo=args.repo, max_files_scanned=args.max_files_scanned)
    known_hashes = {} if args.force else existing_file_hashes(es, index=args.index, repo=args.repo)
    changed_paths = {chunk.path for chunk in chunks if known_hashes.get(chunk.path) != chunk.file_hash}
    chunks_to_index = [chunk for chunk in chunks if chunk.path in changed_paths]

    if changed_paths:
        delete_paths(es, index=args.index, repo=args.repo, paths=changed_paths)

    indexed = 0
    for chunk_batch in batched(chunks_to_index, args.batch_size):
        documents = [chunk_to_document(chunk) for chunk in chunk_batch]
        embedding_texts = [build_embedding_text(document) for document in documents]
        vectors = embed_texts(
            embedding_texts,
            model_name=args.embedding_model,
            batch_size=args.batch_size,
            device=args.device,
            cache_folder=args.model_cache_dir,
        )
        actions = []
        for document, embedding_text, vector in zip(documents, embedding_texts, vectors, strict=True):
            document["embedding_text"] = embedding_text
            document[vector_field] = vector
            actions.append(
                {
                    "_op_type": "index",
                    "_index": args.index,
                    "_id": document["id"],
                    "_source": document,
                }
            )
        helpers.bulk(es, actions)
        indexed += len(actions)

    es.indices.refresh(index=args.index)
    return {
        "index": args.index,
        "repo": args.repo,
        "root_path": str(root_path),
        "embedding_model": args.embedding_model,
        "vector_field": vector_field,
        "dimension": dims,
        "total_chunks": len(chunks),
        "changed_paths": len(changed_paths),
        "indexed_chunks": indexed,
        "skipped_chunks": len(chunks) - indexed,
    }


def parse_args() -> argparse.Namespace:
    pre_parser = argparse.ArgumentParser(add_help=False)
    add_config_arg(pre_parser)
    pre_args, _ = pre_parser.parse_known_args()
    config = load_config(pre_args.config)
    elasticsearch_cfg = config_section(config, "elasticsearch")
    source_cfg = config_section(config, "source")
    index_cfg = config_section(config, "index")
    embedding_cfg = config_section(config, "embedding")

    parser = argparse.ArgumentParser(description="Index code chunks into Elasticsearch", parents=[pre_parser])
    parser.add_argument("--root-path", default=source_cfg.get("root_path"))
    parser.add_argument("--repo", default=source_cfg.get("repo", "deerflow"))
    parser.add_argument("--index", default=index_cfg.get("name", DEFAULT_INDEX))
    parser.add_argument("--embedding-model", default=embedding_cfg.get("model", DEFAULT_EMBEDDING_MODEL))
    parser.add_argument("--batch-size", type=int, default=index_cfg.get("batch_size", 32))
    parser.add_argument("--device", default=embedding_cfg.get("device"))
    parser.add_argument("--model-cache-dir", default=embedding_cfg.get("model_cache_dir"))
    parser.add_argument("--max-files-scanned", type=int, default=source_cfg.get("max_files_scanned", 20_000))
    parser.add_argument(
        "--force",
        action="store_true",
        default=bool(index_cfg.get("force", False)),
        help="Re-index all chunks even if file hashes are unchanged",
    )
    parser.add_argument(
        "--recreate-index",
        action="store_true",
        default=bool(index_cfg.get("recreate_index", False)),
        help="Delete and recreate the index",
    )
    add_es_args(parser)
    parser.set_defaults(
        es_url=elasticsearch_cfg.get("url"),
        es_username=elasticsearch_cfg.get("username"),
        es_password=elasticsearch_cfg.get("password"),
        es_api_key=elasticsearch_cfg.get("api_key"),
    )
    args = parser.parse_args()
    if not args.root_path:
        parser.error("--root-path is required unless source.root_path is set in --config")
    return args


def main() -> None:
    print(json.dumps(index_chunks(parse_args()), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
