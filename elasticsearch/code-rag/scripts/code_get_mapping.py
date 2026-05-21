#!/usr/bin/env python3
"""Print an Elasticsearch index mapping."""

from __future__ import annotations

import argparse
import json

from code_indexer import DEFAULT_INDEX
from es_common import add_es_args, build_es_client


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Get code RAG index mapping")
    parser.add_argument("--index", default=DEFAULT_INDEX)
    add_es_args(parser)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    es = build_es_client(args)
    response = es.indices.get_mapping(index=args.index)
    print(json.dumps(dict(response.body if hasattr(response, "body") else response), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

