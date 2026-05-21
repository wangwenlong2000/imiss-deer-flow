#!/usr/bin/env python3
"""Retrieve code chunks from Elasticsearch using local embeddings and keyword fusion."""

from __future__ import annotations

import argparse
import json
from typing import Any

from code_embedding import DEFAULT_EMBEDDING_MODEL, embed_texts
from code_indexer import DEFAULT_INDEX, vector_field_name
from es_common import add_config_arg, add_es_args, build_es_client, config_section, load_config


def make_filters(args: argparse.Namespace) -> list[dict[str, Any]]:
    filters: list[dict[str, Any]] = []
    if args.repo:
        filters.append({"term": {"repo": args.repo}})
    if args.language:
        filters.append({"term": {"language": args.language}})
    if args.kind:
        filters.append({"term": {"kind": args.kind}})
    if args.tag:
        filters.append({"terms": {"tags": args.tag}})
    if args.path_glob:
        filters.append({"wildcard": {"path": args.path_glob}})
    return filters


def dense_query(es: Any, args: argparse.Namespace, query_vector: list[float], filters: list[dict[str, Any]]) -> list[dict[str, Any]]:
    body = {
        "size": args.window_size,
        "_source": source_fields(args),
        "knn": {
            "field": vector_field_name(args.embedding_model),
            "query_vector": query_vector,
            "k": args.window_size,
            "num_candidates": max(args.window_size * 5, 100),
            "filter": filters,
        },
    }
    response = es.search(index=args.index, body=body)
    return response.get("hits", {}).get("hits", [])


def keyword_query(es: Any, args: argparse.Namespace, filters: list[dict[str, Any]]) -> list[dict[str, Any]]:
    body = {
        "size": args.window_size,
        "_source": source_fields(args),
        "query": {
            "bool": {
                "must": [
                    {
                        "multi_match": {
                            "query": args.query,
                            "fields": ["symbol^5", "path^4", "tags^3", "imports^2", "code", "embedding_text"],
                            "type": "best_fields",
                        }
                    }
                ],
                "filter": filters,
            }
        },
    }
    response = es.search(index=args.index, body=body)
    return response.get("hits", {}).get("hits", [])


def source_fields(args: argparse.Namespace) -> list[str]:
    fields = [
        "id",
        "repo",
        "path",
        "absolute_path",
        "language",
        "kind",
        "symbol",
        "start_line",
        "end_line",
        "imports",
        "tags",
        "content_hash",
        "file_hash",
        "code",
        "metadata",
    ]
    if args.include_embedding_text:
        fields.append("embedding_text")
    return fields


def rrf_fuse(rankings: list[list[dict[str, Any]]], *, rank_constant: int = 60) -> list[dict[str, Any]]:
    fused: dict[str, dict[str, Any]] = {}
    for ranking_index, ranking in enumerate(rankings):
        channel = "dense" if ranking_index == 0 else "keyword"
        for rank, hit in enumerate(ranking, start=1):
            doc_id = hit["_id"]
            entry = fused.setdefault(
                doc_id,
                {
                    "_id": doc_id,
                    "_source": hit.get("_source", {}),
                    "rrf_score": 0.0,
                    "dense_score": None,
                    "keyword_score": None,
                },
            )
            entry["rrf_score"] += 1.0 / (rank_constant + rank)
            entry[f"{channel}_score"] = hit.get("_score")
    return sorted(fused.values(), key=lambda item: item["rrf_score"], reverse=True)


def render_rows(hits: list[dict[str, Any]], *, top_k: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for rank, hit in enumerate(hits[:top_k], start=1):
        source = hit.get("_source", {})
        rows.append(
            {
                "rank": rank,
                "rrf_score": hit.get("rrf_score"),
                "dense_score": hit.get("dense_score"),
                "keyword_score": hit.get("keyword_score"),
                "path": source.get("path"),
                "symbol": source.get("symbol"),
                "kind": source.get("kind"),
                "language": source.get("language"),
                "start_line": source.get("start_line"),
                "end_line": source.get("end_line"),
                "tags": source.get("tags", []),
                "imports": source.get("imports", []),
                "code": source.get("code"),
                "metadata": source.get("metadata", {}),
            }
        )
    return rows


def retrieve(args: argparse.Namespace) -> dict[str, Any]:
    es = build_es_client(args)
    filters = make_filters(args)
    query_vector = embed_texts([args.query], model_name=args.embedding_model, device=args.device, cache_folder=args.model_cache_dir)[0]
    dense_hits = dense_query(es, args, query_vector, filters)
    keyword_hits = keyword_query(es, args, filters)
    fused_hits = rrf_fuse([dense_hits, keyword_hits], rank_constant=args.rank_constant)
    return {
        "index": args.index,
        "query": args.query,
        "embedding_model": args.embedding_model,
        "filters": {
            "repo": args.repo,
            "language": args.language,
            "kind": args.kind,
            "tag": args.tag,
            "path_glob": args.path_glob,
        },
        "dense_hits": len(dense_hits),
        "keyword_hits": len(keyword_hits),
        "top_k": render_rows(fused_hits, top_k=args.k),
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
    retrieval_cfg = config_section(config, "retrieval")

    parser = argparse.ArgumentParser(description="Retrieve code chunks from Elasticsearch", parents=[pre_parser])
    parser.add_argument("--query", required=True)
    parser.add_argument("--index", default=index_cfg.get("name", DEFAULT_INDEX))
    parser.add_argument("--repo", default=source_cfg.get("repo", "deerflow"))
    parser.add_argument("--k", type=int, default=retrieval_cfg.get("k", 8))
    parser.add_argument("--window-size", type=int, default=retrieval_cfg.get("window_size", 50))
    parser.add_argument("--embedding-model", default=embedding_cfg.get("model", DEFAULT_EMBEDDING_MODEL))
    parser.add_argument("--device", default=embedding_cfg.get("device"))
    parser.add_argument("--model-cache-dir", default=embedding_cfg.get("model_cache_dir"))
    parser.add_argument("--language", default=retrieval_cfg.get("language"))
    parser.add_argument("--kind", default=retrieval_cfg.get("kind"))
    parser.add_argument("--tag", action="append", default=list(retrieval_cfg.get("tags") or []))
    parser.add_argument("--path-glob", default=retrieval_cfg.get("path_glob"))
    parser.add_argument("--rank-constant", type=int, default=retrieval_cfg.get("rank_constant", 60))
    parser.add_argument("--include-embedding-text", action="store_true")
    add_es_args(parser)
    parser.set_defaults(
        es_url=elasticsearch_cfg.get("url"),
        es_username=elasticsearch_cfg.get("username"),
        es_password=elasticsearch_cfg.get("password"),
        es_api_key=elasticsearch_cfg.get("api_key"),
    )
    return parser.parse_args()


def main() -> None:
    print(json.dumps(retrieve(parse_args()), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
