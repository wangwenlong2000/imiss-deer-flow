#!/usr/bin/env python3
"""Run one full-engine evaluation over all ten annotation types.

The 0624 normalized split is intentionally a model-only 8/9/10 corpus.  This
runner uses the ten raw JSONL annotation files instead, sends every row through
the registry, router, engine and policy matrix, and reports the provenance
needed to distinguish a real ten-type run from the 56-row model regression.

Rows without an annotation gate are retained as negative examples and assigned
``InputGate`` explicitly.  The assignment is recorded in each prediction and
in the report metadata; it is not silently treated as an original annotation.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from collections import defaultdict
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
HARNESS = ROOT / "backend/packages/harness"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(HARNESS) not in sys.path:
    sys.path.insert(0, str(HARNESS))

from deerflow.compliance.contract import GATES, VIOLATION_TYPES, TextItem  # noqa: E402
from deerflow.compliance.engine import ComplianceEngine  # noqa: E402
from deerflow.compliance.normalizers.base import build_unit, flatten_fields, stringify  # noqa: E402
from deerflow.compliance.policy import load_policy_matrix  # noqa: E402
from deerflow.compliance.registry import build_registry  # noqa: E402
from deerflow.compliance.detectors.model_tfidf_knn.composite_risk import canonical_data_type  # noqa: E402
from scripts.run_compliance_gate_eval import prf, score_group  # noqa: E402

DEFAULT_INPUT = ROOT / "合规检测/合规评测全部十类违规类型数据"
DEFAULT_OUTPUT = ROOT / "outputs/compliance-eval/unified-ten-type.json"
VALID_GATES = set(GATES)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def resolve_path(path: Path) -> Path:
    """Resolve CLI paths consistently when the command is run from the repo.

    The previous implementation kept relative ``--input`` paths relative to
    the caller's cwd, then called ``relative_to(ROOT)`` on them.  That made
    the documented command fail before evaluating a single row.  Prefer the
    repository root for relative paths when the target exists, while still
    allowing an explicit path outside the repository.
    """
    if path.is_absolute():
        return path.resolve()
    repo_path = (ROOT / path).resolve()
    if repo_path.exists():
        return repo_path
    return (Path.cwd() / path).resolve()


def display_path(path: Path) -> str:
    """Return a stable repo-relative path when possible."""
    try:
        return str(path.resolve().relative_to(ROOT))
    except ValueError:
        return str(path)


def git_head() -> str | None:
    pinned = os.getenv("COMPLIANCE_EVAL_COMMIT_ID")
    if pinned:
        return pinned
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def git_dirty() -> bool:
    try:
        return bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True).strip())
    except (OSError, subprocess.CalledProcessError):
        return True


def load_records(path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    files = [path] if path.is_file() else sorted(path.glob("*.jsonl"))
    records: list[dict[str, Any]] = []
    sources: list[dict[str, Any]] = []
    for source_path in files:
        rows = [json.loads(line) for line in source_path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]
        sources.append({
            "path": display_path(source_path),
            "rows": len(rows),
            "sha256": sha256(source_path),
        })
        for row_number, row in enumerate(rows, start=1):
            if not isinstance(row, dict):
                continue
            row["_source_path"] = display_path(source_path)
            row["_source_file"] = source_path.name
            row["_source_row"] = row_number
            records.append(row)
    return records, sources


def gate_for(record: Mapping[str, Any]) -> tuple[str, bool]:
    raw = record.get("gate") or record.get("trigger_gate")
    if raw in VALID_GATES:
        return str(raw), False
    return "InputGate", True


def unit_for(record: Mapping[str, Any], gate: str):
    features = record.get("features")
    if isinstance(features, dict):
        fields = flatten_fields(features, prefix="", source="eval", max_depth=6)
    else:
        fields = flatten_fields(record.get("content"), prefix="content", source="eval", max_depth=6)
    field_items = tuple(fields[:256])
    if field_items:
        text_items = ()
    else:
        text = stringify(record.get("content_text") or record.get("content") or record.get("raw_content"))
        text_items = () if not text.strip() else (("content", text),)
    return build_unit(
        unit_id=str(record.get("data_id") or record.get("sample_id") or "sample"),
        gate=gate,
        data_type=canonical_data_type(record.get("data_type")) or None,
        text_items=tuple(
            TextItem(item_id=f"t-{index:04d}", text=value, source=source)
            for index, (source, value) in enumerate(text_items, start=1)
        ),
        field_items=field_items,
        raw={
            "kind": "media_preprocessed" if canonical_data_type(record.get("data_type")) == "satellite_remote_sensing" else "unified_ten_type_eval",
            "media_preprocessed": canonical_data_type(record.get("data_type")) == "satellite_remote_sensing",
            "source_path": record.get("_source_path"),
        },
    )


def score_pairs(pairs: list[tuple[str, str]]) -> dict[str, Any]:
    return score_group(pairs)


def score_prediction_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Score one annotated label against all detector hits for each row.

    A production decision may contain several independent violation types.  A
    single-label ``hits[0]`` projection loses later hits (for example a row can
    expose both an internal IP and confidential material), which turns a real
    confidential true positive into a false negative.  The acceptance metric
    is therefore one-vs-rest per violation type; the legacy single-label score
    remains available in ``single_label`` for diagnosis.
    """
    per_type: dict[str, dict[str, Any]] = {}
    for violation_type in VIOLATION_TYPES:
        tp = sum(
            1
            for row in rows
            if row["gold"] == violation_type and violation_type in row["predicted_types"]
        )
        fp = sum(
            1
            for row in rows
            if row["gold"] != violation_type and violation_type in row["predicted_types"]
        )
        fn = sum(
            1
            for row in rows
            if row["gold"] == violation_type and violation_type not in row["predicted_types"]
        )
        if tp + fp + fn:
            per_type[violation_type] = prf(tp, fp, fn)

    exact_correct = sum(
        1
        for row in rows
        if set(row["predicted_types"]) == ({row["gold"]} if row["gold"] != "none" else set())
    )
    macro_f1 = sum(stats["f1"] for stats in per_type.values()) / len(per_type) if per_type else 0.0
    return {
        "samples": len(rows),
        "correct": exact_correct,
        "accuracy": round(exact_correct / len(rows), 4) if rows else 0.0,
        "macro_f1": round(macro_f1, 4),
        "per_violation_type": per_type,
        "metric": "one_vs_rest_presence; gold annotations remain single-label",
    }


def build_fp_fn_manifest(predictions: list[dict[str, Any]]) -> dict[str, dict[str, list[dict[str, Any]]]]:
    """Emit auditable false-positive/false-negative sample IDs per type."""
    labels = (*VIOLATION_TYPES, "none")
    manifest = {
        label: {"false_positive": [], "false_negative": []}
        for label in labels
        if label != "none"
    }
    for row in predictions:
        gold = row["gold"]
        predicted = row["predicted"]
        if gold == predicted:
            continue
        base = {
            "sample_id": row["sample_id"],
            "source": row["source"],
            "source_row": row.get("source_row"),
            "data_type": row["data_type"],
            "evaluated_gate": row["evaluated_gate"],
            "annotation_gate": row["annotation_gate"],
            "gold": gold,
            "predicted": predicted,
        }
        predicted_types = set(row.get("predicted_types") or ())
        for predicted_type in sorted(predicted_types & manifest.keys()):
            if predicted_type != gold:
                manifest[predicted_type]["false_positive"].append(base)
        if gold in manifest and gold not in predicted_types:
            manifest[gold]["false_negative"].append(base)
    return manifest


def build_manual_review_manifest(predictions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep every unresolved single-label/multi-hit disagreement reviewable.

    The raw bundle has one ``final_violation_type`` per row, while the engine
    can emit several independent types.  Until a human supplies a multi-label
    gold decision, a disagreement must not disappear into a summary metric.
    """
    rows: list[dict[str, Any]] = []
    for row in predictions:
        expected = {row["gold"]} if row["gold"] != "none" else set()
        actual = set(row.get("predicted_types") or ())
        if expected == actual:
            continue
        rows.append({
            "sample_id": row["sample_id"],
            "source": row["source"],
            "source_row": row.get("source_row"),
            "data_type": row["data_type"],
            "evaluated_gate": row["evaluated_gate"],
            "gold_primary_type": row["gold"],
            "predicted_types": row.get("predicted_types") or [],
            "actions": row.get("actions") or [],
            "scene_key": row.get("scene_key"),
            "manual_review_required": True,
            "review_reason": "single-label gold cannot distinguish a legitimate co-hit from a true false positive; adjudicate each predicted type independently",
        })
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--default-missing-gate", choices=sorted(VALID_GATES), default="InputGate")
    args = parser.parse_args()
    args.input = resolve_path(args.input)
    args.output = resolve_path(args.output)

    if not args.input.exists():
        parser.error(f"input path does not exist: {args.input}")

    records, sources = load_records(args.input)
    registry = build_registry(config_path=ROOT / "config/compliance/detectors.yaml", strict=True)
    engine = ComplianceEngine(
        registry=registry,
        policy=load_policy_matrix(ROOT / "config/compliance/policy_matrix.yaml"),
    )
    pairs: list[tuple[str, str]] = []
    by_gate: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_data_type: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_source: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_annotation_gate: dict[str, list[dict[str, Any]]] = defaultdict(list)
    inferred_gate_count = 0
    predictions: list[dict[str, Any]] = []
    enabled_detectors = [registration.detector_id for registration in registry.enabled()]
    manifest_gates = {gate: list(engine.router.violation_types_for(gate)) for gate in GATES}
    try:
        for record in records:
            gate, inferred = gate_for(record)
            if inferred:
                gate = args.default_missing_gate
                inferred_gate_count += 1
            gold = str(record.get("final_violation_type") or "none")
            if gold not in (*VIOLATION_TYPES, "none"):
                gold = "none"
            unit = unit_for(record, gate)
            decision = engine.check(
                (unit,),
                gate=gate,
                request_id=f"unified-{unit.unit_id}",
                budget_ms=2000,
                origin={"kind": "unified_ten_type_eval", "source_path": record.get("_source_path")},
            )
            predicted_types = list(dict.fromkeys(hit.violation_type for hit in decision.hits))
            predicted = predicted_types[0] if predicted_types else "none"
            data_type = canonical_data_type(record.get("data_type")) or "unknown"
            source = str(record.get("_source_file") or record.get("_source_path") or "unknown")
            annotation_gate = str(record.get("trigger_gate") or record.get("gate") or "missing")
            pair = (gold, predicted)
            pairs.append(pair)
            prediction = {
                "sample_id": unit.unit_id,
                "source": source,
                "source_row": record.get("_source_row"),
                "data_type": data_type,
                "annotation_gate": annotation_gate,
                "evaluated_gate": gate,
                "gate_inferred": inferred,
                "gold": gold,
                "predicted": predicted,
                "predicted_types": predicted_types,
                "actions": list(decision.actions),
                "scene_key": decision.scene_key,
                "diagnostic_warnings": list(decision.diagnostics.warnings),
            }
            predictions.append(prediction)
            by_gate[gate].append(prediction)
            by_data_type[data_type].append(prediction)
            by_source[source].append(prediction)
            by_annotation_gate[annotation_gate].append(prediction)
    finally:
        registry.close()

    report = {
        "schema_version": "compliance_unified_ten_type_eval_1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "command": "PYTHONPATH=backend/packages/harness python scripts/run_compliance_unified_ten_type_eval.py --input '合规检测/合规评测全部十类违规类型数据' --output 'outputs/compliance-eval/unified-ten-type.json'",
        "dataset": {
            "path": display_path(args.input),
            "data_version": "7.23 raw annotation bundle as checked out; no independent freeze marker",
            "independent_frozen_set": False,
            "rows_loaded": len(records),
            "source_files": sources,
            "missing_gate_policy": f"assign {args.default_missing_gate}; retained in report as gate_inferred=true",
            "missing_gate_rows": inferred_gate_count,
        },
        "code": {
            "commit_id": git_head(),
            "working_tree_dirty": git_dirty(),
            "detectors_config_sha256": sha256(ROOT / "config/compliance/detectors.yaml"),
            "policy_matrix_sha256": sha256(ROOT / "config/compliance/policy_matrix.yaml"),
            "manifest_sha256": {
                display_path(path): sha256(path)
                for path in sorted((HARNESS / "deerflow/compliance/detectors").glob("*/manifest.yaml"))
            },
        },
        "detector_coverage": {
            "enabled_detectors": enabled_detectors,
            "manifest_gate_coverage": manifest_gates,
            "gold_types_present": {label: sum(1 for gold, _ in pairs if gold == label) for label in VIOLATION_TYPES},
        },
        "primary": score_prediction_rows(predictions),
        "single_label": {
            "primary": score_pairs(pairs),
            "by_gate": {key: score_pairs([(
                row["gold"], row["predicted"]
            ) for row in value]) for key, value in sorted(by_gate.items())},
        },
        "by_gate": {key: score_prediction_rows(value) for key, value in sorted(by_gate.items())},
        "by_annotation_gate": {key: score_prediction_rows(value) for key, value in sorted(by_annotation_gate.items())},
        "by_data_type": {key: score_prediction_rows(value) for key, value in sorted(by_data_type.items())},
        "by_source": {key: score_prediction_rows(value) for key, value in sorted(by_source.items())},
        "fp_fn": build_fp_fn_manifest(predictions),
    }
    manual_review = build_manual_review_manifest(predictions)
    # Rebuild the registry only for the serializable enabled-detector list above;
    # close it immediately so model resources are not retained by this report.
    report["predictions"] = str(args.output.with_name(args.output.stem + "-predictions.jsonl"))
    report["manual_review"] = {
        "required": True,
        "rows": len(manual_review),
        "path": str(args.output.with_name(args.output.stem + "-manual-review.jsonl")),
        "reason": "single-label annotation versus multi-type engine output; no row is accepted as a false positive or a co-hit until adjudicated",
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    with Path(report["predictions"]).open("w", encoding="utf-8") as stream:
        for row in predictions:
            stream.write(json.dumps(row, ensure_ascii=False) + "\n")
    with Path(report["manual_review"]["path"]).open("w", encoding="utf-8") as stream:
        for row in manual_review:
            stream.write(json.dumps(row, ensure_ascii=False) + "\n")
    print(json.dumps(report["primary"], ensure_ascii=False, indent=2))
    print(f"report written to: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
