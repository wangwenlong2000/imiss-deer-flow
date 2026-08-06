#!/usr/bin/env python3
"""List all indices in an Elasticsearch cluster.

Usage:
  python es_list_indices.py --es-url http://localhost:3128

Example Output:
  Shows all indices with:
  - Index name
  - Document count
  - Store size
  - Health status

ES Configuration:
  - URL: http://localhost:3128
  - Target Index: street
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


def list_indices(es: Elasticsearch) -> list[dict]:
    response = es.cat.indices(format="json", h="index,docs.count,store.size,health,status")
    if hasattr(response, "body"):
        return list(response.body)
    return list(response)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="List all Elasticsearch indices")
    parser.add_argument("--es-url", default=None, help="Elasticsearch URL")
    parser.add_argument("--es-username", default=None, help="Basic auth username")
    parser.add_argument("--es-password", default=None, help="Basic auth password")
    parser.add_argument("--es-api-key", default=None, help="API key")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    es = build_es_client(args)
    indices = list_indices(es)
    print(json.dumps(indices, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
