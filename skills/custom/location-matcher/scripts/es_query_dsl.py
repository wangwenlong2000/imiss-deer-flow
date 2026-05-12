#!/usr/bin/env python3
"""Execute an arbitrary DSL query against an Elasticsearch index.

Usage:
  # Using --dsl parameter
  python es_query_dsl.py --index street --dsl '{"query": {"match_all": {}}}' --es-url http://localhost:3128

  # Using stdin (not supported with conda run)
  echo '{"query": {"match_all": {}}}' | python es_query_dsl.py --index street --es-url http://localhost:3128

Example Queries:
  # Match all
  {"query": {"match_all": {}}, "size": 10}

  # With source filtering
  {"query": {"match_all": {}}, "size": 5, "_source": ["id", "source_path"]}

  # kNN search
  {"knn": {"field": "vector-ImAge4VPR", "query_vector": [...], "k": 10}}

ES Configuration:
  - URL: http://localhost:3128
  - Index: street
"""

from __future__ import annotations

import argparse
import json
import os
import sys

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Execute a DSL query against an Elasticsearch index")
    parser.add_argument("--index", required=True, help="Elasticsearch index name")
    parser.add_argument(
        "--dsl",
        default=None,
        help="DSL query as a JSON string. If omitted, reads from stdin.",
    )
    parser.add_argument("--es-url", default=None, help="Elasticsearch URL")
    parser.add_argument("--es-username", default=None, help="Basic auth username")
    parser.add_argument("--es-password", default=None, help="Basic auth password")
    parser.add_argument("--es-api-key", default=None, help="API key")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.dsl:
        dsl = json.loads(args.dsl)
    else:
        raw = sys.stdin.read().strip()
        if not raw:
            print("Error: provide --dsl or pipe a JSON DSL via stdin", file=sys.stderr)
            sys.exit(1)
        dsl = json.loads(raw)

    es = build_es_client(args)
    response = es.search(index=args.index, body=dsl)
    if hasattr(response, "body"):
        result = dict(response.body)
    else:
        result = dict(response)

    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
