#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Policy / Regulation Elasticsearch Search Script

用途：
- 从 Elasticsearch 中检索政策法规条文
- 支持关键词检索、精确法规名过滤、条号过滤、类别过滤、效力状态过滤、发布机关过滤
- 默认排除向量字段，避免输出过大
"""

import argparse
import json
import os
import sys
from typing import Any, Dict, List, Optional

import requests
from requests.auth import HTTPBasicAuth


DEFAULT_ES_URL = os.getenv("POLICY_ES_URL", "http://172.17.0.1:3128")
DEFAULT_ES_USER = os.getenv("POLICY_ES_USER", "citybrain-street")
DEFAULT_ES_PASSWORD = os.getenv("POLICY_ES_PASSWORD", "123456")
DEFAULT_ES_INDEX = os.getenv("POLICY_ES_INDEX", "cn_law_articles_text_embedding_v4")
DEFAULT_VECTOR_FIELD = os.getenv("POLICY_ES_VECTOR_FIELD", "vector-text-embedding-v4")


def build_term_filter(field: str, value: Optional[str]) -> Optional[Dict[str, Any]]:
    if not value:
        return None
    return {"term": {field: value}}


def build_match_filter(field: str, value: Optional[str]) -> Optional[Dict[str, Any]]:
    if not value:
        return None
    return {"match_phrase": {field: value}}


def build_query(args: argparse.Namespace) -> Dict[str, Any]:
    must: List[Dict[str, Any]] = []
    filters: List[Dict[str, Any]] = []

    if args.query:
        must.append(
            {
                "multi_match": {
                    "query": args.query,
                    "fields": [
                        "title^5",
                        "law_name^5",
                        "article_no^4",
                        "content^3",
                        "page_content^2",
                        "metadata.source_title^4",
                        "metadata.article_label^4",
                    ],
                    "type": "best_fields",
                    "operator": "or",
                }
            }
        )
    else:
        must.append({"match_all": {}})

    # 精确过滤字段
    for item in [
        build_match_filter("law_name", args.law_name),
        build_match_filter("title", args.title),
        build_term_filter("article_no", args.article_no),
        build_term_filter("article_number", args.article_number),
        build_term_filter("category", args.category),
        build_term_filter("validity_status", args.validity_status),
        build_match_filter("office", args.office),
        build_match_filter("office_level", args.office_level),
        build_match_filter("office_category", args.office_category),
    ]:
        if item:
            filters.append(item)

    # 日期范围过滤
    if args.publish_date_from or args.publish_date_to:
        range_query: Dict[str, Any] = {}
        if args.publish_date_from:
            range_query["gte"] = args.publish_date_from
        if args.publish_date_to:
            range_query["lte"] = args.publish_date_to
        filters.append({"range": {"publish_date": range_query}})

    if args.effective_date_from or args.effective_date_to:
        range_query = {}
        if args.effective_date_from:
            range_query["gte"] = args.effective_date_from
        if args.effective_date_to:
            range_query["lte"] = args.effective_date_to
        filters.append({"range": {"effective_date": range_query}})

    query: Dict[str, Any] = {
        "bool": {
            "must": must,
            "filter": filters,
        }
    }

    return query


def search_es(args: argparse.Namespace) -> Dict[str, Any]:
    es_url = args.es_url.rstrip("/")
    index = args.index
    url = f"{es_url}/{index}/_search"

    query = build_query(args)

    source_excludes = [
        DEFAULT_VECTOR_FIELD,
        "vector-*",
        "*.vector-*",
    ]

    body: Dict[str, Any] = {
        "size": args.top_k,
        "from": args.from_,
        "query": query,
        "_source": {
            "excludes": source_excludes
        },
    }

    if args.sort_by_date:
        body["sort"] = [
            {"publish_date": {"order": "desc", "missing": "_last"}},
            {"_score": {"order": "desc"}}
        ]

    resp = requests.post(
        url,
        auth=HTTPBasicAuth(args.es_user, args.es_password),
        headers={"Content-Type": "application/json"},
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        timeout=args.timeout,
    )

    if resp.status_code >= 400:
        raise RuntimeError(
            f"Elasticsearch query failed: HTTP {resp.status_code}\n{resp.text}"
        )

    raw = resp.json()
    hits = raw.get("hits", {}).get("hits", [])

    results: List[Dict[str, Any]] = []
    for rank, hit in enumerate(hits, start=1):
        src = hit.get("_source", {})
        metadata = src.get("metadata", {}) or {}

        results.append(
            {
                "rank": rank,
                "score": hit.get("_score"),
                "id": src.get("id") or hit.get("_id"),
                "title": src.get("title"),
                "law_name": src.get("law_name") or metadata.get("source_title"),
                "article_no": src.get("article_no") or metadata.get("article_label"),
                "article_number": src.get("article_number") or metadata.get("article_number"),
                "category": src.get("category") or metadata.get("source_type"),
                "validity_status": src.get("validity_status") or metadata.get("source_status"),
                "publish_date": src.get("publish_date") or metadata.get("source_publish_date"),
                "effective_date": src.get("effective_date") or metadata.get("source_effective_date"),
                "office": src.get("office") or metadata.get("source_office"),
                "office_level": src.get("office_level") or metadata.get("source_office_level"),
                "content": src.get("content"),
                "page_content": src.get("page_content"),
                "source_path": src.get("source_path") or metadata.get("source_path"),
                "part_title": metadata.get("part_title"),
                "chapter_title": metadata.get("chapter_title"),
                "section_title": metadata.get("section_title"),
                # "highlight": hit.get("highlight", {}),
            }
        )

    return {
        "ok": True,
        "index": index,
        "query": args.query,
        "total": raw.get("hits", {}).get("total", {}),
        "returned": len(results),
        "results": results,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Search policy / regulation articles from Elasticsearch."
    )

    parser.add_argument("--query", default="", help="用户问题或检索关键词")
    parser.add_argument("--top-k", type=int, default=5, help="返回结果数量")
    parser.add_argument("--from", dest="from_", type=int, default=0, help="分页起点")
    parser.add_argument("--timeout", type=int, default=15, help="请求超时时间，单位秒")

    parser.add_argument("--es-url", default=DEFAULT_ES_URL)
    parser.add_argument("--es-user", default=DEFAULT_ES_USER)
    parser.add_argument("--es-password", default=DEFAULT_ES_PASSWORD)
    parser.add_argument("--index", default=DEFAULT_ES_INDEX)

    parser.add_argument("--law-name", default="", help="法规名称，例如：中华人民共和国消防法")
    parser.add_argument("--title", default="", help="标题过滤")
    parser.add_argument("--article-no", default="", help="条号，例如：第三十二条")
    parser.add_argument("--article-number", default="", help="条号数字，例如：32")
    parser.add_argument("--category", default="", help="法规类别，例如：宪法、法律、行政法规、地方性法规")
    parser.add_argument("--validity-status", default="", help="效力状态，例如：有效")
    parser.add_argument("--office", default="", help="发布机关")
    parser.add_argument("--office-level", default="", help="机关层级")
    parser.add_argument("--office-category", default="", help="机关类别")

    parser.add_argument("--publish-date-from", default="", help="发布日期起始，例如：2018-01-01")
    parser.add_argument("--publish-date-to", default="", help="发布日期结束，例如：2026-12-31")
    parser.add_argument("--effective-date-from", default="", help="实施日期起始")
    parser.add_argument("--effective-date-to", default="", help="实施日期结束")

    parser.add_argument("--sort-by-date", action="store_true", help="优先按发布日期倒序")
    parser.add_argument("--pretty", action="store_true", help="格式化输出 JSON")

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    try:
        result = search_es(args)
        if args.pretty:
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            print(json.dumps(result, ensure_ascii=False))
    except Exception as exc:
        error = {
            "ok": False,
            "error": str(exc),
        }
        print(json.dumps(error, ensure_ascii=False, indent=2), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()