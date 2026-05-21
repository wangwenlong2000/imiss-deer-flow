#!/usr/bin/env python3
"""Execute raw Elasticsearch DSL against a code RAG index."""

from __future__ import annotations

import argparse
import json
import sys

from code_indexer import DEFAULT_INDEX
from es_common import add_es_args, build_es_client


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Execute DSL query against code RAG index")
    parser.add_argument("--index", default=DEFAULT_INDEX)
    parser.add_argument("--dsl", default=None, help="JSON DSL string. If omitted, reads stdin.")
    add_es_args(parser)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    raw = args.dsl or sys.stdin.read().strip()
    if not raw:
        raise SystemExit("Error: provide --dsl or stdin JSON")
    body = json.loads(raw)
    es = build_es_client(args)
    response = es.search(index=args.index, body=body)
    print(json.dumps(dict(response.body if hasattr(response, "body") else response), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

