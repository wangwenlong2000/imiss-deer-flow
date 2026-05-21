#!/usr/bin/env python3
"""List Elasticsearch indices for code RAG debugging."""

from __future__ import annotations

import argparse
import json

from es_common import add_es_args, build_es_client


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="List Elasticsearch indices")
    add_es_args(parser)
    return parser.parse_args()


def main() -> None:
    es = build_es_client(parse_args())
    response = es.cat.indices(format="json", h="index,docs.count,store.size,health,status")
    print(json.dumps(list(response.body if hasattr(response, "body") else response), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

