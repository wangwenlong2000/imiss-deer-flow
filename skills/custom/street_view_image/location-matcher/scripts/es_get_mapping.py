#!/usr/bin/env python3
"""Get the mapping for an Elasticsearch index.

Usage:
  python es_get_mapping.py --index street --es-url http://localhost:3128

Example Output:
  Shows the complete mapping including:
  - Field types (keyword, double, dense_vector)
  - Vector dimensions and index settings
  - Similarity functions and index options

ES Configuration:
  - URL: http://localhost:3128
  - Index: street
  - Vector fields: vector-ImAge4VPR, vector-Qwen3-VL-Embedding-2B_urban_governance
"""

from __future__ import annotations

import argparse
import json
import os

from elasticsearch import Elasticsearch


def build_es_client(args: argparse.Namespace) -> Elasticsearch:
    hosts = args.es_url or os.getenv("ES_URL", "http://localhost:9200")
    api_key = args.es_api_key or os.getenv("ES_API_KEY")
    username = args.es_username or os.getenv("ES_USERNAME")
    password = args.es_password or os.getenv("ES_PASSWORD")

    if api_key:
        return Elasticsearch(hosts=hosts, api_key=api_key)
    if username and password:
        return Elasticsearch(hosts=hosts, basic_auth=(username, password))
    return Elasticsearch(hosts=hosts)


def get_mapping(es: Elasticsearch, index: str) -> dict:
    response = es.indices.get_mapping(index=index)
    if hasattr(response, "body"):
        return dict(response.body)
    return dict(response)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Get Elasticsearch index mapping")
    parser.add_argument("--index", required=True, help="Elasticsearch index name")
    parser.add_argument("--es-url", default=None, help="Elasticsearch URL")
    parser.add_argument("--es-username", default=None, help="Basic auth username")
    parser.add_argument("--es-password", default=None, help="Basic auth password")
    parser.add_argument("--es-api-key", default=None, help="API key")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    es = build_es_client(args)
    mapping = get_mapping(es, args.index)
    print(json.dumps(mapping, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
