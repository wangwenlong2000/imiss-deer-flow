#!/usr/bin/env python3
"""End-to-end gate evaluation through the full ``ComplianceEngine``.

Groups annotated samples by ``trigger_gate``, builds the matching payload, runs
the complete pipeline (normalize -> route -> detect -> intent -> policy -> audit)
and produces the four metric groups the guide's chapter 7 asks for:

1. **Primary** — precision / recall / F1 per violation type
2. **By gate** — the same, split across InputGate / ContextGate / OutputGate
3. **By scene** — phase 1 can only produce the ``_unknown`` column (see below)
4. **By data type** — surveillance / telecom / netflow / ...

Plus the free consistency check from plan §9.2: every sample carries an
``expected_action`` per scene, so the matrix transcription can be validated
against the annotations instead of trusted.

⚠️ The scene dimension is a single ``_unknown`` column in phase 1
----------------------------------------------------------------
There is no source of truth for usage scene yet, so ``SceneResolver`` returns
``None`` for everything. The report says so explicitly and prints the column as
NOT EVALUATED rather than leaving a blank that reads like a pass.

Output lands in ``outputs/compliance-eval/{timestamp}/``.

    make compliance-eval-gates
    python3 scripts/run_compliance_gate_eval.py --dump-features   # risk 4 check
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
HARNESS = REPO_ROOT / "backend/packages/harness"
if str(HARNESS) not in sys.path:
    sys.path.insert(0, str(HARNESS))

from deerflow.compliance.contract import GATES, SCENES, VIOLATION_TYPES, DetectionUnit, TextItem  # noqa: E402
from deerflow.compliance.engine import ComplianceEngine  # noqa: E402
from deerflow.compliance.normalizers.base import build_unit  # noqa: E402
from deerflow.compliance.policy import load_policy_matrix  # noqa: E402
from deerflow.compliance.registry import build_registry  # noqa: E402
from deerflow.compliance.types import rank_action  # noqa: E402

DEFAULT_INPUT = REPO_ROOT / "datasets/compliance/normalized/0624_supported_split/test.jsonl"
DEFAULT_MATRIX_INPUT = REPO_ROOT / "datasets/compliance/normalized/0624_supported_split/all_supported_deduplicated.jsonl"
DEFAULT_MATRIX = REPO_ROOT / "config/compliance/policy_matrix.yaml"
DEFAULT_DETECTORS = REPO_ROOT / "config/compliance/detectors.yaml"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs/compliance-eval"


# ── sample -> detection unit ────────────────────────────────────────────────


def sample_to_unit(sample: dict[str, Any], gate: str) -> DetectionUnit:
    """Rebuild a detection unit from an annotated sample.

    Uses ``features`` directly, mirroring what the normalizers produce at
    runtime, so the evaluation exercises the same code path production does.
    """
    features = sample.get("features") or {}
    text_items = tuple(
        TextItem(item_id=f"t-{index:03d}", text=str(value), source=str(key))
        for index, (key, value) in enumerate(features.items(), start=1)
        if str(value).strip()
    )
    return build_unit(
        unit_id=str(sample.get("data_id") or sample.get("sample_id") or "sample"),
        gate=gate,
        data_type=sample.get("data_type"),
        text_items=text_items,
        field_items=(),
        raw={"kind": "eval_sample", "data_id": sample.get("data_id")},
    )


# ── metrics ─────────────────────────────────────────────────────────────────


def prf(tp: int, fp: int, fn: int) -> dict[str, float]:
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = (2 * precision * recall / (precision + recall)) if precision + recall else 0.0
    return {"tp": tp, "fp": fp, "fn": fn, "support": tp + fn, "precision": round(precision, 4), "recall": round(recall, 4), "f1": round(f1, 4)}


def score_group(pairs: list[tuple[str, str]]) -> dict[str, Any]:
    """Score ``(gold, predicted)`` pairs. ``"none"`` means no violation."""
    per_type: dict[str, dict[str, Any]] = {}
    for violation_type in VIOLATION_TYPES:
        tp = sum(1 for gold, pred in pairs if gold == violation_type and pred == violation_type)
        fp = sum(1 for gold, pred in pairs if gold != violation_type and pred == violation_type)
        fn = sum(1 for gold, pred in pairs if gold == violation_type and pred != violation_type)
        if tp + fp + fn == 0:
            continue
        per_type[violation_type] = prf(tp, fp, fn)

    total = len(pairs)
    correct = sum(1 for gold, pred in pairs if gold == pred)
    macro_f1 = sum(stats["f1"] for stats in per_type.values()) / len(per_type) if per_type else 0.0
    return {
        "samples": total,
        "correct": correct,
        "accuracy": round(correct / total, 4) if total else 0.0,
        "macro_f1": round(macro_f1, 4),
        "per_violation_type": per_type,
    }


# ── matrix cross-validation (plan §9.2) ─────────────────────────────────────


def cross_validate_matrix(samples: list[dict[str, Any]], matrix) -> dict[str, Any]:
    """Compare matrix output against each sample's annotated ``expected_action``.

    Only positive samples are meaningful here: a negative sample's
    ``expected_action`` describes what happens when nothing is detected.

    Mismatches are reported as *discrepancies*, not failures — they can equally
    mean a transcription error or an annotation dispute, and deciding which is a
    human call.
    """
    checked = 0
    agreements = 0
    discrepancies: list[dict[str, Any]] = []
    per_type_checked: Counter = Counter()
    per_type_agreed: Counter = Counter()
    seen_cells: set[tuple[str, str, str]] = set()

    for sample in samples:
        violation_type = sample.get("final_violation_type")
        if not violation_type or violation_type == "none" or violation_type not in VIOLATION_TYPES:
            continue
        gate = sample.get("gate") or sample.get("trigger_gate")
        if gate not in GATES:
            continue
        expected = sample.get("expected_action")
        if not isinstance(expected, dict):
            continue

        for scene, expected_actions in expected.items():
            if scene not in SCENES or not isinstance(expected_actions, list) or not expected_actions:
                continue
            cell = matrix.cell(violation_type, gate, scene)
            if cell is None:
                continue  # "—" in the guide: nothing to compare against
            checked += 1
            per_type_checked[violation_type] += 1
            # Compare the strongest action on each side: annotators list actions
            # in varying order and combinations, but the disposition level is
            # what actually matters.
            strongest_expected = max(expected_actions, key=rank_action)
            strongest_matrix = max(cell, key=rank_action)
            if strongest_expected == strongest_matrix:
                agreements += 1
                per_type_agreed[violation_type] += 1
            elif (violation_type, gate, scene) not in seen_cells:
                # One row per distinct matrix cell; hundreds of samples hitting
                # the same cell would otherwise bury the actual disagreements.
                seen_cells.add((violation_type, gate, scene))
                discrepancies.append(
                    {
                        "violation_type": violation_type,
                        "gate": gate,
                        "scene": scene,
                        "matrix": list(cell),
                        "annotated": list(expected_actions),
                        "matrix_strongest": strongest_matrix,
                        "annotated_strongest": strongest_expected,
                        "example_data_id": sample.get("data_id"),
                    }
                )

    per_type = {
        violation_type: {
            "checked": count,
            "agreed": per_type_agreed[violation_type],
            "agreement_rate": round(per_type_agreed[violation_type] / count, 4) if count else 0.0,
        }
        for violation_type, count in sorted(per_type_checked.items())
    }

    return {
        "checked": checked,
        "agreements": agreements,
        "agreement_rate": round(agreements / checked, 4) if checked else 0.0,
        "per_violation_type": per_type,
        "distinct_disagreeing_cells": len(discrepancies),
        "discrepancies": discrepancies,
        "note": (
            "The disposition matrix is transcribed verbatim from the 7.23 guide chapter 3, which plan §4.5 designates "
            "as authoritative. A discrepancy therefore means the annotated expected_action diverges from the guide table, "
            "not that the transcription is wrong. Each one needs a human call: fix the guide, fix the annotation, or "
            "record a deliberate exception."
        ),
    }


# ── main ────────────────────────────────────────────────────────────────────


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument(
        "--matrix-input",
        type=Path,
        default=DEFAULT_MATRIX_INPUT,
        help="Samples used for the matrix cross-check (defaults to the full deduplicated 0624 set; validating the matrix does not need a train/test split).",
    )
    parser.add_argument("--policy-matrix", type=Path, default=DEFAULT_MATRIX)
    parser.add_argument("--detectors-config", type=Path, default=DEFAULT_DETECTORS)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--timestamp", default=None, help="Override the output directory name (default: UTC now).")
    parser.add_argument("--dump-features", action="store_true", help="Also dump the feature keys each sample produces (plan risk 4 check).")
    args = parser.parse_args(argv)

    if not args.input.is_file():
        print(f"FAIL: input not found: {args.input}", file=sys.stderr)
        print("      Run `make compliance-assets` to unpack the delivery package.", file=sys.stderr)
        return 1

    samples = [json.loads(line) for line in args.input.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not samples:
        print(f"FAIL: no samples in {args.input}", file=sys.stderr)
        return 1

    registry = build_registry(config_path=args.detectors_config, strict=True)
    engine = ComplianceEngine(registry=registry, policy=load_policy_matrix(args.policy_matrix))

    enabled = [r.detector_id for r in registry.enabled()]
    coverage = {gate: list(engine.router.violation_types_for(gate)) for gate in GATES}

    all_pairs: list[tuple[str, str]] = []
    by_gate: dict[str, list[tuple[str, str]]] = defaultdict(list)
    by_data_type: dict[str, list[tuple[str, str]]] = defaultdict(list)
    scene_keys: Counter = Counter()
    records: list[dict[str, Any]] = []
    feature_keys: Counter = Counter()

    for sample in samples:
        gate = sample.get("gate") or sample.get("trigger_gate")
        if gate not in GATES:
            continue
        gold = sample.get("final_violation_type") or "none"
        if gold not in VIOLATION_TYPES:
            gold = "none"

        unit = sample_to_unit(sample, gate)
        if args.dump_features:
            feature_keys.update((sample.get("features") or {}).keys())

        decision = engine.check([unit], gate=gate, request_id=str(sample.get("data_id") or "s"))
        predicted = decision.hits[0].violation_type if decision.hits else "none"
        scene_keys[decision.scene_key] += 1

        all_pairs.append((gold, predicted))
        by_gate[gate].append((gold, predicted))
        by_data_type[str(sample.get("data_type") or "unknown")].append((gold, predicted))

        records.append(
            {
                "data_id": sample.get("data_id"),
                "gate": gate,
                "data_type": sample.get("data_type"),
                "gold": gold,
                "predicted": predicted,
                "scene_key": decision.scene_key,
                "actions": list(decision.actions),
                "severity": decision.hits[0].severity if decision.hits else None,
                "top_similarity": decision.hits[0].evidence.get("top_similarity") if decision.hits else None,
                "warnings": decision.diagnostics.warnings,
            }
        )

    matrix_samples = samples
    if args.matrix_input.is_file():
        matrix_samples = [json.loads(line) for line in args.matrix_input.read_text(encoding="utf-8").splitlines() if line.strip()]
    matrix_check = cross_validate_matrix(matrix_samples, engine.policy)
    matrix_check["input"] = str(args.matrix_input if args.matrix_input.is_file() else args.input)

    timestamp = args.timestamp or datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    output_dir = args.output_root / timestamp
    output_dir.mkdir(parents=True, exist_ok=True)

    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "input": str(args.input),
        "samples": len(records),
        "enabled_detectors": enabled,
        "gate_coverage": coverage,
        "primary": score_group(all_pairs),
        "by_gate": {gate: score_group(pairs) for gate, pairs in sorted(by_gate.items())},
        "by_scene": {
            "status": "NOT EVALUATED",
            "reason": "Phase 1 has no scene information source; SceneResolver returns None for every request and the matrix falls back to its `_unknown` column. This dimension cannot be measured until a real resolver is wired up (plan §7.2).",
            "resolved_scene_keys": dict(scene_keys),
        },
        "by_data_type": {data_type: score_group(pairs) for data_type, pairs in sorted(by_data_type.items())},
        "matrix_cross_validation": matrix_check,
    }
    if args.dump_features:
        report["feature_keys"] = dict(feature_keys.most_common())

    (output_dir / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    with (output_dir / "predictions.jsonl").open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    _print_report(report, output_dir)
    return 0


def _print_report(report: dict[str, Any], output_dir: Path) -> None:
    print("=" * 78)
    print("合规检测端到端评测报告 / Compliance gate evaluation")
    print("=" * 78)
    print(f"samples           : {report['samples']}")
    print(f"enabled detectors : {', '.join(report['enabled_detectors']) or '(none)'}")
    print()

    print("── gate coverage ──────────────────────────────────────────────────────")
    for gate, types in report["gate_coverage"].items():
        missing = sorted(set(VIOLATION_TYPES) - set(types))
        print(f"  {gate:12s} covered: {', '.join(types) or '(none)'}")
        if missing:
            print(f"               NOT covered: {', '.join(missing)}")
    print("  (uncovered types have no detector registered yet — those columns below")
    print("   measure nothing, they do not mean 'no violations found')")
    print()

    print("── 1. primary: violation type P/R/F1 ──────────────────────────────────")
    _print_scores(report["primary"])

    print()
    print("── 2. by gate ─────────────────────────────────────────────────────────")
    for gate, scores in report["by_gate"].items():
        print(f"  [{gate}] samples={scores['samples']} accuracy={scores['accuracy']} macro_f1={scores['macro_f1']}")
        _print_scores(scores, indent="    ")

    print()
    print("── 3. by scene ────────────────────────────────────────────────────────")
    print(f"  status: {report['by_scene']['status']}")
    print(f"  {report['by_scene']['reason']}")
    print(f"  resolved scene keys: {report['by_scene']['resolved_scene_keys']}")

    print()
    print("── 4. by data type ────────────────────────────────────────────────────")
    for data_type, scores in report["by_data_type"].items():
        print(f"  {data_type:28s} samples={scores['samples']:3d} accuracy={scores['accuracy']} macro_f1={scores['macro_f1']}")

    print()
    print("── matrix cross-validation (matrix vs annotated expected_action) ──────")
    check = report["matrix_cross_validation"]
    print(f"  source          : {check.get('input')}")
    print(f"  checked cells   : {check['checked']}")
    print(f"  agreements      : {check['agreements']} ({check['agreement_rate']:.1%})")
    print(f"  disagreeing cells: {check['distinct_disagreeing_cells']}")
    print()
    for violation_type, stats in check["per_violation_type"].items():
        marker = "  " if stats["agreement_rate"] >= 0.95 else "! "
        print(f"  {marker}{violation_type:18s} {stats['agreed']:>5d}/{stats['checked']:<5d} {stats['agreement_rate']:.1%}")
    if check["discrepancies"]:
        print()
        print("  disagreeing cells (matrix from guide 7.23 ch.3 vs annotated expected_action):")
        for item in check["discrepancies"][:12]:
            print(f"    {item['violation_type']:16s} {item['gate']:12s} {item['scene']:15s} matrix={item['matrix']} annotated={item['annotated']}")
        if len(check["discrepancies"]) > 12:
            print(f"    ... and {len(check['discrepancies']) - 12} more (full list in report.json)")
        print()
        print(f"  {check['note']}")

    print()
    print(f"report written to: {output_dir}")


def _print_scores(scores: dict[str, Any], indent: str = "  ") -> None:
    per_type = scores.get("per_violation_type") or {}
    if not per_type:
        print(f"{indent}(no violation type had any prediction or gold label)")
        return
    print(f"{indent}accuracy={scores['accuracy']} macro_f1={scores['macro_f1']} samples={scores['samples']}")
    for violation_type, stats in sorted(per_type.items()):
        print(f"{indent}  {violation_type:18s} support={stats['support']:3d} P={stats['precision']:.4f} R={stats['recall']:.4f} F1={stats['f1']:.4f}")


if __name__ == "__main__":
    sys.exit(main())
