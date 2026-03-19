#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from file_resolution import get_default_search_roots, resolve_reference


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Resolve a network traffic dataset file reference")
    parser.add_argument("reference", help="Filename, relative suffix, or explicit path")
    parser.add_argument(
        "--format",
        choices=["json", "text"],
        default="json",
        help="Output format",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    result = resolve_reference(args.reference)

    payload = {
        "reference": result.reference,
        "status": result.status,
        "strategy": result.strategy,
        "matches": result.matches,
        "message": result.message,
        "roots": [str(root) for root in get_default_search_roots()],
    }

    if args.format == "json":
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(result.message)
        for match in result.matches:
            print(match)

    return 0 if result.status == "resolved" else 1


if __name__ == "__main__":
    raise SystemExit(main())
