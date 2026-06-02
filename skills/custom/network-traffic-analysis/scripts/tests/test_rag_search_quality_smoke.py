#!/usr/bin/env python3
"""Live Elasticsearch smoke test for RAG search quality.

Runs a fixed set of queries via rag_search.py (as a subprocess) and produces a
JSON report. This depends on a live ES instance and should not be included in
offline unit test runs.

Usage:
    python3 test_rag_search_quality_smoke.py \
        --index-name network-traffic-rag \
        --es-host http://localhost:9200 \
        --es-user elastic \
        --es-pass changeme

Output:
    JSON report is written to stdout.
    A copy is optionally saved to datasets/network-traffic/smoke_search_report.json.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent.parent
RAG_SEARCH = SCRIPT_DIR / "rag_search.py"


def detect_repo_root() -> Path:
    """Find the repository root from this script location."""
    current = Path(__file__).resolve()
    for parent in (current.parent, *current.parents):
        if (parent / "config.yaml").exists() and (parent / "datasets").exists():
            return parent
    return Path.cwd()


DEFAULT_REPORT_PATH = detect_repo_root() / "datasets" / "network-traffic" / "smoke_search_report.json"


# ---------------------------------------------------------------------------
# Test case definitions
# ---------------------------------------------------------------------------
@dataclass
class QueryCase:
    name: str
    dataset: str
    query: str
    min_hits: int = 1
    risk_level: str | None = None
    min_linked_flows: int = 0
    require_dataset_only: bool = False


QUERIES: list[QueryCase] = [
    QueryCase(
        name="neris_overview",
        dataset="Neris",
        query="network traffic overview anomaly scan",
        min_hits=3,
        require_dataset_only=True,
    ),
    QueryCase(
        name="neris_c2_dns",
        dataset="Neris",
        query="Neris C2 beacon dynamic dns",
        min_hits=1,
        risk_level="high",
        min_linked_flows=1,
    ),
    QueryCase(
        name="neris_scan",
        dataset="Neris",
        query="scan source broad destination probing",
        min_hits=3,
    ),
    QueryCase(
        name="zeus_overview",
        dataset="Zeus",
        query="network traffic overview anomaly scan",
        min_hits=3,
        require_dataset_only=True,
    ),
    QueryCase(
        name="zeus_c2",
        dataset="Zeus",
        query="Zeus malware command and control dns http ssl beacon",
        min_hits=1,
    ),
]


# ---------------------------------------------------------------------------
# Subprocess runner
# ---------------------------------------------------------------------------
def run_rag_search(
    *,
    query: str,
    dataset: str,
    index_name: str,
    es_host: str,
    es_username: str,
    es_password: str,
    risk_level: str | None = None,
    size: int = 20,
) -> dict[str, Any]:
    """Call rag_search.py as a subprocess and return its parsed JSON output."""
    cmd: list[str] = [
        sys.executable,
        str(RAG_SEARCH),
        "--query", query,
        "--dataset-name", dataset,
        "--index-name", index_name,
        "--es-host", es_host,
        "--es-username", es_username,
        "--es-password", es_password,
        "--format", "json",
        "--size", str(size),
    ]
    if risk_level:
        cmd += ["--risk-level", risk_level]

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"rag_search.py exited with code {result.returncode}: "
            f"{result.stderr.strip()}"
        )

    # The last line of stdout should be the JSON payload
    output = result.stdout.strip()
    # Handle case where there may be extra logging lines before the JSON
    for line in output.splitlines():
        line = line.strip()
        if line.startswith("{"):
            return json.loads(line)

    # If no line starts with '{', try parsing the whole thing
    return json.loads(output)


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------
def evaluate_case(case: QueryCase, search_result: dict[str, Any]) -> dict[str, Any]:
    """Evaluate a single query case and return the case-level report dict."""
    hit_count = search_result.get("hit_count", 0)
    retrieval = search_result.get("retrieval_coverage", {})
    dataset_counts = retrieval.get("dataset_counts", {})
    doc_type_counts = retrieval.get("doc_type_counts", {})
    linked_flows = retrieval.get("linked_flows", 0)
    hits = search_result.get("hits", [])
    top_titles = [h.get("title", "") for h in hits[:5] if h.get("title")]

    errors: list[str] = []
    status = "PASS"

    # Check min_hits
    if hit_count < case.min_hits:
        errors.append(
            f"hit_count {hit_count} < min_hits {case.min_hits}"
        )
        status = "FAIL"

    # Check require_dataset_only: dataset_counts must have exactly the expected dataset
    if case.require_dataset_only:
        keys = set(dataset_counts.keys())
        if keys != {case.dataset}:
            errors.append(
                f"dataset_counts has unexpected keys: {sorted(keys)} "
                f"(expected only '{case.dataset}')"
            )
            status = "FAIL"

    # Check min_linked_flows
    if case.min_linked_flows > 0 and linked_flows < case.min_linked_flows:
        errors.append(
            f"linked_flows {linked_flows} < min_linked_flows {case.min_linked_flows}"
        )
        status = "FAIL"

    return {
        "name": case.name,
        "status": status,
        "hit_count": hit_count,
        "dataset_counts": dataset_counts,
        "doc_type_counts": doc_type_counts,
        "linked_flows": linked_flows,
        "top_titles": top_titles,
        "min_hits_required": case.min_hits,
        "min_linked_flows_required": case.min_linked_flows,
        "error": "; ".join(errors) if errors else None,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Smoke test: run fixed queries against ES RAG index and produce a JSON report."
    )
    parser.add_argument("--index-name", required=True, help="Elasticsearch index name")
    parser.add_argument("--es-host", required=True, help="Elasticsearch host (e.g. http://localhost:9200)")
    parser.add_argument("--es-username", required=True, help="Elasticsearch username")
    parser.add_argument("--es-password", required=True, help="Elasticsearch password")
    parser.add_argument(
        "--report-path",
        default=None,
        help="Write a copy of the report to this path (default: datasets/network-traffic/smoke_search_report.json)",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    report: dict[str, Any] = {
        "index_name": args.index_name,
        "passed": True,
        "cases": [],
    }

    for case in QUERIES:
        try:
            search_result = run_rag_search(
                query=case.query,
                dataset=case.dataset,
                index_name=args.index_name,
                es_host=args.es_host,
                es_username=args.es_username,
                es_password=args.es_password,
                risk_level=case.risk_level,
            )
            case_result = evaluate_case(case, search_result)
        except Exception as exc:
            case_result = {
                "name": case.name,
                "status": "FAIL",
                "hit_count": 0,
                "dataset_counts": {},
                "doc_type_counts": {},
                "linked_flows": 0,
                "top_titles": [],
                "min_hits_required": case.min_hits,
                "min_linked_flows_required": case.min_linked_flows,
                "error": str(exc),
            }

        report["cases"].append(case_result)
        if case_result["status"] == "FAIL":
            report["passed"] = False

    report_json = json.dumps(report, ensure_ascii=False, indent=2)
    print(report_json)

    # Optionally write to disk
    report_path = Path(args.report_path) if args.report_path else DEFAULT_REPORT_PATH
    try:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(report_json, encoding="utf-8")
        print(f"\nReport also saved to: {report_path}", file=sys.stderr)
    except Exception as exc:
        print(f"\nFailed to save report to {report_path}: {exc}", file=sys.stderr)

    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
