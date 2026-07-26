#!/usr/bin/env python3
"""Export the detector contract as JSON Schema.

Detectors reached over ``subprocess_cli`` or ``http_service`` exchange JSON. This
emits the exact shapes from ``deerflow.compliance.contract``, so an author
working in another language never has to transcribe field names by hand — and a
contract change shows up as a diff rather than as a runtime surprise.

    make compliance-export-schema
    make compliance-export-schema ARGS="--output docs/compliance-detector-contract.schema.json"
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
HARNESS = REPO_ROOT / "backend/packages/harness"
if str(HARNESS) not in sys.path:
    sys.path.insert(0, str(HARNESS))

from deerflow.compliance.contract import CONTRACT_VERSION, export_json_schema  # noqa: E402

DEFAULT_OUTPUT = REPO_ROOT / "docs/compliance-detector-contract.schema.json"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Where to write the schema.")
    parser.add_argument("--stdout", action="store_true", help="Print to stdout instead of writing a file.")
    parser.add_argument("--check", action="store_true", help="Exit 1 if the file on disk is stale (for CI).")
    args = parser.parse_args(argv)

    schema = export_json_schema()
    rendered = json.dumps(schema, ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    if args.stdout:
        print(rendered, end="")
        return 0

    if args.check:
        if not args.output.is_file():
            print(f"FAIL: {args.output} does not exist; run `make compliance-export-schema`.", file=sys.stderr)
            return 1
        if args.output.read_text(encoding="utf-8") != rendered:
            print(f"FAIL: {args.output} is stale; run `make compliance-export-schema`.", file=sys.stderr)
            return 1
        print(f"OK: {args.output} matches contract version {CONTRACT_VERSION}")
        return 0

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(rendered, encoding="utf-8")
    print(f"contract version {CONTRACT_VERSION} -> {args.output}")
    print(f"types exported: {', '.join(sorted(schema['$defs']))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
