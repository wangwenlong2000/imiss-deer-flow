#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from file_resolution import resolve_reference


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Resolve a local network traffic dataset reference and run analyze.py on the resolved file."
    )
    parser.add_argument(
        "--reference",
        required=True,
        help="Filename, relative suffix, or explicit path under datasets/network-traffic",
    )
    parser.add_argument(
        "--action",
        required=True,
        choices=[
            "inspect",
            "summary",
            "query",
            "topn",
            "timeseries",
            "distribution",
            "filter",
            "aggregate",
            "detect-anomaly",
            "export",
        ],
    )
    parser.add_argument(
        "extra_args",
        nargs=argparse.REMAINDER,
        help="Extra arguments forwarded to analyze.py. Prefix with -- before extra args if needed.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    resolution = resolve_reference(args.reference)
    if resolution.status != "resolved":
        print(resolution.message, file=sys.stderr)
        for match in resolution.matches:
            print(match, file=sys.stderr)
        return 1

    resolved_file = resolution.matches[0]
    script_path = Path(__file__).resolve().parent / "analyze.py"

    forwarded = list(args.extra_args)
    if forwarded and forwarded[0] == "--":
        forwarded = forwarded[1:]

    command = [
        sys.executable,
        str(script_path),
        "--files",
        resolved_file,
        "--action",
        args.action,
        *forwarded,
    ]

    print(f"Resolved '{args.reference}' -> {resolved_file}", file=sys.stderr)
    completed = subprocess.run(command, check=False)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
