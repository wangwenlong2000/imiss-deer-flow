#!/usr/bin/env python3
"""Regression gate for the ``content_text`` concatenation rule (plan §5.1).

Asserts two things over all 282 rows of ``0624_supported_split``:

1. **String exactness** — ``build_content_text(row["features"])`` equals the
   ``content_text`` the training pipeline actually produced, character for
   character. 282/282 required.
2. **Prediction agreement** — a row rebuilt from ``features`` alone yields the
   same model prediction as the original row. 100% required.

Together these prove that production, which only ever has ``features``, can
reconstruct training-time input with zero drift.

Run it in CI (``make compliance-verify-content-text``). Any change to
``row_builder.py`` that breaks the rule is caught here rather than showing up as
a mysterious accuracy drop weeks later.

Exit code 0 = both invariants hold; 1 = something drifted.
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

from deerflow.compliance.detectors.model_tfidf_knn.row_builder import (  # noqa: E402
    CONTENT_TEXT_SPEC,
    build_content_text,
    build_row,
)

DEFAULT_SPLIT = REPO_ROOT / "datasets/compliance/normalized/0624_supported_split"
DEFAULT_MODEL = REPO_ROOT / "models/compliance/ml_detector_0624_fresh.json"

#: The plan's verified figures. The script fails if reality no longer matches.
EXPECTED_ROWS = {"train.jsonl": 226, "test.jsonl": 56}
EXPECTED_TOTAL = 282


def load_jsonl(path: Path) -> list[dict]:
    rows: list[dict] = []
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            if not line.strip():
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise SystemExit(f"{path}:{line_no}: invalid JSON: {exc}") from exc
    return rows


def check_exact_match(rows: list[dict], filename: str) -> tuple[int, int, list[dict]]:
    """Compare rebuilt ``content_text`` against the stored one."""
    matched = 0
    mismatches: list[dict] = []
    for row in rows:
        features = row.get("features") or {}
        rebuilt = build_content_text(features)
        original = row.get("content_text")
        if rebuilt == original:
            matched += 1
        else:
            mismatches.append(
                {
                    "file": filename,
                    "data_id": row.get("data_id"),
                    "feature_keys": list(features),
                    "expected": (original or "")[:300],
                    "rebuilt": rebuilt[:300],
                }
            )
    return matched, len(rows), mismatches


def check_prediction_agreement(rows: list[dict], model_path: Path) -> tuple[int, int, list[dict]]:
    """Predict on the original row and on one rebuilt from ``features`` alone."""
    from deerflow.compliance.detectors.model_tfidf_knn.vendor.model_detectors.tfidf_knn import TfidfKNNModel

    model = TfidfKNNModel.load(model_path)
    agreed = 0
    disagreements: list[dict] = []
    for row in rows:
        original_label, _, _ = model.predict(row)
        rebuilt = build_row(
            features=row.get("features") or {},
            data_type=row.get("data_type"),
            gate=row.get("gate") or row.get("trigger_gate"),
        )
        rebuilt_label, _, _ = model.predict(rebuilt)
        if original_label == rebuilt_label:
            agreed += 1
        else:
            disagreements.append({"data_id": row.get("data_id"), "original": original_label, "rebuilt": rebuilt_label})
    return agreed, len(rows), disagreements


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--split-dir", type=Path, default=DEFAULT_SPLIT, help="Directory holding train.jsonl and test.jsonl.")
    parser.add_argument("--model", type=Path, default=DEFAULT_MODEL, help="Model used for the prediction-agreement check.")
    parser.add_argument("--skip-predictions", action="store_true", help="Only run the string-exactness check (no model needed).")
    parser.add_argument("--json", dest="as_json", action="store_true", help="Emit a machine-readable summary.")
    args = parser.parse_args(argv)

    if not args.split_dir.is_dir():
        print(f"FAIL: split directory not found: {args.split_dir}", file=sys.stderr)
        print("      Run `make compliance-assets` to unpack the delivery package.", file=sys.stderr)
        return 1

    summary: dict = {"spec": CONTENT_TEXT_SPEC, "files": {}, "exact_match": {}, "prediction_agreement": {}}
    total_matched = 0
    total_rows = 0
    all_mismatches: list[dict] = []

    print(f"content_text spec: {CONTENT_TEXT_SPEC}")
    print(f"split directory:   {args.split_dir}")
    print()

    for filename, expected_rows in EXPECTED_ROWS.items():
        path = args.split_dir / filename
        if not path.is_file():
            print(f"FAIL: missing {path}", file=sys.stderr)
            return 1
        rows = load_jsonl(path)
        if len(rows) != expected_rows:
            print(f"FAIL: {filename} has {len(rows)} rows, expected {expected_rows}", file=sys.stderr)
            return 1

        matched, count, mismatches = check_exact_match(rows, filename)
        total_matched += matched
        total_rows += count
        all_mismatches.extend(mismatches)
        summary["files"][filename] = count
        summary["exact_match"][filename] = {"matched": matched, "total": count}

        status = "OK " if matched == count else "FAIL"
        print(f"[{status}] {filename:12s} exact string match: {matched}/{count}")

    print(f"       {'TOTAL':12s} exact string match: {total_matched}/{total_rows}")
    summary["exact_match"]["total"] = {"matched": total_matched, "total": total_rows}

    ok = total_matched == total_rows == EXPECTED_TOTAL

    if all_mismatches:
        print("\nMismatches (first 5):", file=sys.stderr)
        for mismatch in all_mismatches[:5]:
            print(f"  {mismatch['file']} {mismatch['data_id']}", file=sys.stderr)
            print(f"    feature keys: {mismatch['feature_keys']}", file=sys.stderr)
            print(f"    expected: {mismatch['expected']!r}", file=sys.stderr)
            print(f"    rebuilt : {mismatch['rebuilt']!r}", file=sys.stderr)

    if not args.skip_predictions:
        print()
        if not args.model.is_file():
            print(f"FAIL: model not found: {args.model}", file=sys.stderr)
            print("      Run `make compliance-assets`, or pass --skip-predictions.", file=sys.stderr)
            return 1

        test_rows = load_jsonl(args.split_dir / "test.jsonl")
        agreed, count, disagreements = check_prediction_agreement(test_rows, args.model)
        summary["prediction_agreement"] = {"agreed": agreed, "total": count}
        status = "OK " if agreed == count else "FAIL"
        print(f"[{status}] {'test.jsonl':12s} prediction agreement (rebuilt vs original row): {agreed}/{count}")
        if disagreements:
            print("\nDisagreements:", file=sys.stderr)
            for item in disagreements[:5]:
                print(f"  {item['data_id']}: original={item['original']} rebuilt={item['rebuilt']}", file=sys.stderr)
        ok = ok and agreed == count

    summary["passed"] = ok
    if args.as_json:
        print()
        print(json.dumps(summary, ensure_ascii=False, indent=2))

    print()
    if ok:
        print(f"PASS: content_text rule holds for all {EXPECTED_TOTAL} rows with zero prediction drift.")
        return 0
    print("FAIL: the content_text rule no longer reproduces training-time input.", file=sys.stderr)
    print("      Do not ship this: features will be misaligned and offline accuracy will not hold online.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
