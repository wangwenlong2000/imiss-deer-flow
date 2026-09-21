#!/usr/bin/env python3
"""Evaluate the 56-row three-gate regression across all policy scenes.

``run_compliance_gate_eval.py`` intentionally uses the null scene resolver, so
its scene column is ``_unknown``.  This companion is an explicit policy-path
evaluation: it runs the same rows with a fixed, test-only scene resolver and
compares both detector labels and disposition actions with the annotation's
``expected_action`` matrix.

The resolver is deliberately not production authorization.  It exists only to
prove that the policy matrix is wired at each gate and at each of the five
scene columns.  Production scene trust remains an upstream/IAM concern.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
HARNESS = ROOT / "backend/packages/harness"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(HARNESS) not in sys.path:
    sys.path.insert(0, str(HARNESS))

from deerflow.compliance.contract import GATES, SCENES, VIOLATION_TYPES  # noqa: E402
from deerflow.compliance.engine import ComplianceEngine  # noqa: E402
from deerflow.compliance.policy import load_policy_matrix  # noqa: E402
from deerflow.compliance.registry import build_registry  # noqa: E402
from deerflow.compliance.types import rank_action  # noqa: E402
from scripts.run_compliance_gate_eval import prf, sample_to_unit  # noqa: E402

DEFAULT_INPUT = ROOT / "datasets/compliance/normalized/0624_supported_split/test.jsonl"
DEFAULT_OUTPUT = ROOT / "outputs/compliance-eval/gate-scene-eval.json"
DEFAULT_PREDICTIONS = ROOT / "outputs/compliance-eval/gate-scene-eval-predictions.jsonl"


class FixedSceneResolver:
    """Test-only resolver returning the scene selected by the evaluator."""

    def __init__(self, scene: str) -> None:
        self.scene = scene

    def resolve(self, _request):  # noqa: ANN001
        return self.scene


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git_head() -> str | None:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def git_dirty() -> bool:
    try:
        return bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True).strip())
    except (OSError, subprocess.CalledProcessError):
        return True


def detector_score(pairs: list[tuple[str, str]]) -> dict[str, Any]:
    tp = sum(1 for gold, predicted in pairs if gold != "none" and gold == predicted)
    fp = sum(1 for gold, predicted in pairs if predicted != "none" and gold != predicted)
    fn = sum(1 for gold, predicted in pairs if gold != "none" and predicted == "none")
    tn = sum(1 for gold, predicted in pairs if gold == "none" and predicted == "none")
    result = prf(tp, fp, fn)
    result["tn"] = tn
    result["samples"] = len(pairs)
    result["accuracy"] = round((tp + tn) / len(pairs), 4) if pairs else 0.0
    return result


def strongest(actions: list[str] | tuple[str, ...]) -> str | None:
    return max(actions, key=rank_action) if actions else None


def effective_actions(actions: list[str] | tuple[str, ...]) -> list[str]:
    """Represent an empty engine action tuple as the policy's implicit allow."""
    return list(actions) if actions else ["allow"]


def action_score(
    rows: list[dict[str, Any]],
    *,
    expected_source: str = "policy_matrix",
    source: str = "gold_violation",
) -> dict[str, Any]:
    """Score policy actions for positive detector rows only.

    The policy matrix is the executable source of truth.  The annotation
    action column is retained as a separate reconciliation view because the
    checked corpus contains cells that differ from that matrix.
    """
    if expected_source not in {"policy_matrix", "annotation"}:
        raise ValueError(f"unsupported expected action source: {expected_source}")
    if source not in {"decision", "gold_violation"}:
        raise ValueError(f"unsupported action score source: {source}")
    expected_key = "matrix_expected_actions" if expected_source == "policy_matrix" else "expected_actions"
    all_compared = [row for row in rows if row[expected_key] is not None]
    compared = [row for row in all_compared if row.get("gold") != "none"]

    def scored_actions(row: dict[str, Any]) -> list[str]:
        if source == "gold_violation":
            per_violation = row.get("per_violation_actions") or {}
            return effective_actions(per_violation.get(row.get("gold"), row["actual_actions"]))
        return row["effective_actions"]

    def expected_actions(row: dict[str, Any]) -> list[str]:
        return list(row[expected_key])

    exact = sum(set(expected_actions(row)) == set(scored_actions(row)) for row in compared)
    strongest_matches = sum(
        strongest(expected_actions(row)) == strongest(scored_actions(row))
        for row in compared
    )
    return {
        "scope": "positive_gold_violation_rows",
        "expected_actions_source": expected_source,
        "action_source": source,
        "cells": len(compared),
        "excluded_cells": len(rows) - len(compared),
        "excluded_non_violation_cells": sum(1 for row in rows if row.get("gold") == "none"),
        "excluded_not_applicable_cells": len(rows) - len(all_compared) - sum(
            1 for row in rows if row.get("gold") == "none" and row[expected_key] is None
        ),
        "exact_action_agreements": exact,
        "exact_action_agreement_rate": round(exact / len(compared), 4) if compared else 0.0,
        "strongest_action_agreements": strongest_matches,
        "strongest_action_agreement_rate": round(strongest_matches / len(compared), 4) if compared else 0.0,
        "mismatches": [
            {
                "sample_id": row["sample_id"],
                "gate": row["gate"],
                "scene": row["scene"],
                "gold": row.get("gold"),
                "expected_actions": expected_actions(row),
                "actual_actions": row["actual_actions"],
                "scored_actions": scored_actions(row),
            }
            for row in compared
            if set(expected_actions(row)) != set(scored_actions(row))
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--predictions", type=Path, default=DEFAULT_PREDICTIONS)
    args = parser.parse_args()

    samples = [json.loads(line) for line in args.input.read_text(encoding="utf-8").splitlines() if line.strip()]
    registry = build_registry(config_path=ROOT / "config/compliance/detectors.yaml", strict=True)
    policy = load_policy_matrix(ROOT / "config/compliance/policy_matrix.yaml")
    # One run per scene uses the same registry and detector set.  This is a
    # policy-path evaluation, not a new detector training or tuning pass.
    engines = {
        scene: ComplianceEngine(registry=registry, policy=policy, scene_resolver=FixedSceneResolver(scene))
        for scene in SCENES
    }
    by_gate_scene: dict[tuple[str, str], list[tuple[str, str]]] = defaultdict(list)
    by_gate_scene_actions: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    by_scene: dict[str, list[tuple[str, str]]] = defaultdict(list)
    by_gate: dict[str, list[tuple[str, str]]] = defaultdict(list)
    predictions: list[dict[str, Any]] = []
    same_text: dict[str, Any] | None = None

    try:
        for sample in samples:
            gate = sample.get("gate") or sample.get("trigger_gate")
            if gate not in GATES:
                continue
            gold = sample.get("final_violation_type") or "none"
            if gold not in (*VIOLATION_TYPES, "none"):
                gold = "none"
            unit = sample_to_unit(sample, gate)
            expected_by_scene = sample.get("expected_action") or {}
            for scene in SCENES:
                if scene not in expected_by_scene:
                    continue
                decision = engines[scene].check(
                    (unit,),
                    gate=gate,
                    request_id=f"scene-eval-{unit.unit_id}-{scene}",
                    budget_ms=2000,
                    origin={"kind": "three_gate_56_scene_eval", "scene": scene},
                )
                predicted = decision.hits[0].violation_type if decision.hits else "none"
                actual_actions = list(decision.actions)
                expected_actions = list(expected_by_scene[scene])
                matrix_actions = policy.cell(gold, gate, scene) if gold != "none" else None
                pair = (gold, predicted)
                by_gate_scene[(gate, scene)].append(pair)
                by_scene[scene].append(pair)
                by_gate[gate].append(pair)
                action_row = {
                    "sample_id": unit.unit_id,
                    "gate": gate,
                    "scene": scene,
                    "expected_actions": expected_actions,
                    "matrix_expected_actions": list(matrix_actions) if matrix_actions is not None else None,
                    "actual_actions": actual_actions,
                    "effective_actions": effective_actions(actual_actions),
                    "per_violation_actions": {
                        key: list(value) for key, value in decision.per_violation_actions.items()
                    },
                }
                by_gate_scene_actions[(gate, scene)].append(action_row)
                predictions.append(
                    {
                        **action_row,
                        "gold": gold,
                        "predicted": predicted,
                        "scene_key": decision.scene_key,
                        "per_violation_actions": {
                            key: list(value) for key, value in decision.per_violation_actions.items()
                        },
                        "audit_ref": decision.audit_ref,
                    }
                )

                if same_text is None and sample.get("sample_id") == "spatiotemporal-formal-060":
                    same_text = {"sample_id": sample.get("sample_id"), "content": sample.get("content_text") or sample.get("content")}
                    same_text["content_sha256"] = hashlib.sha256(
                        json.dumps(same_text["content"], ensure_ascii=False, sort_keys=True).encode("utf-8")
                    ).hexdigest()
                if sample.get("sample_id") == "spatiotemporal-formal-060":
                    same_text.setdefault("scenes", {})[scene] = {
                        "gate": gate,
                        "actions": actual_actions,
                        "per_violation_actions": {
                            key: list(value) for key, value in decision.per_violation_actions.items()
                        },
                    }
    finally:
        registry.close()

    report = {
        "schema_version": "compliance_three_gate_56_scene_eval_3",
        "generated_at": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
        "command": "PYTHONPATH=backend/packages/harness python scripts/run_compliance_gate_scene_eval.py",
        "dataset": {
            "path": str(args.input.relative_to(ROOT) if args.input.is_relative_to(ROOT) else args.input),
            "rows": len(samples),
            "sha256": sha256(args.input),
            "data_version": "0624_supported_split test.jsonl; legacy 8/9/10 model regression",
            "independent_frozen_set": False,
            "scope_note": "This 56-row corpus covers only video_meta_leak/re_identify/domain; it is not a ten-type evaluation.",
        },
        "code": {
            "commit_id": git_head(),
            "working_tree_dirty": git_dirty(),
            "detectors_config_sha256": sha256(ROOT / "config/compliance/detectors.yaml"),
            "policy_matrix_sha256": sha256(ROOT / "config/compliance/policy_matrix.yaml"),
        },
        "coverage": {
            "rows_loaded": len(samples),
            "rows_by_gate": {gate: sum(1 for row in samples if (row.get("gate") or row.get("trigger_gate")) == gate) for gate in GATES},
            "scene_cells_by_scene": {scene: len(by_scene.get(scene, [])) for scene in SCENES},
            "scene_cells_total": len(predictions),
        },
        "classification": {
            "overall_by_scene_cells": detector_score([(row["gold"], row["predicted"]) for row in predictions]),
            "by_gate": {gate: detector_score(rows) for gate, rows in sorted(by_gate.items())},
            "by_scene": {scene: detector_score(rows) for scene, rows in sorted(by_scene.items())},
            "by_gate_scene": {
                f"{gate}/{scene}": detector_score(rows)
                for (gate, scene), rows in sorted(by_gate_scene.items())
            },
        },
        "policy_actions": {
            # Primary acceptance evidence: the executable policy matrix versus
            # the action tuple for the annotated violation type.
            "overall": action_score(predictions),
            "decision_aggregate_vs_matrix": action_score(predictions, source="decision"),
            # Reconciliation evidence: the source annotation's expected_action
            # cells are not identical to the checked-in 7.23 policy matrix.
            "annotation_reconciliation": action_score(predictions, expected_source="annotation"),
            "annotation_reconciliation_aggregate": action_score(
                predictions, expected_source="annotation", source="decision"
            ),
            "by_scene": {scene: action_score(rows) for scene, rows in sorted(
                ((scene, [row for row in predictions if row["scene"] == scene]) for scene in SCENES),
                key=lambda item: item[0],
            )},
            "annotation_reconciliation_by_scene": {scene: action_score(rows, expected_source="annotation") for scene, rows in sorted(
                ((scene, [row for row in predictions if row["scene"] == scene]) for scene in SCENES),
                key=lambda item: item[0],
            )},
            "by_gate_scene": {
                f"{gate}/{scene}": action_score(rows)
                for (gate, scene), rows in sorted(by_gate_scene_actions.items())
            },
            "annotation_reconciliation_by_gate_scene": {
                f"{gate}/{scene}": action_score(rows, expected_source="annotation")
                for (gate, scene), rows in sorted(by_gate_scene_actions.items())
            },
        },
        "same_text_self_use_public_release": same_text,
        "verification_contracts": {
            "input_gate_termination": {
                "test": "backend/tests/test_compliance_gates.py::test_input_gate_refusal_does_not_reach_the_model",
                "required_evidence": ["jump_to=end", "model_not_called", "streaming_mode=pre_model_refusal"],
            },
            "sse_compliance_event": {
                "test": "backend/tests/test_compliance_output_retract.py",
                "required_evidence": ["event.type=compliance_retract", "JSON serializable", "original text absent", "audit_ref present"],
            },
        },
        "predictions": str(args.predictions.relative_to(ROOT) if args.predictions.is_relative_to(ROOT) else args.predictions),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.predictions.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    args.predictions.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in predictions), encoding="utf-8")
    print(json.dumps({
        "classification": report["classification"]["overall_by_scene_cells"],
        "policy_actions": {
            key: {
                field: report["policy_actions"][key][field]
                for field in ("cells", "exact_action_agreement_rate", "strongest_action_agreement_rate")
            }
            for key in ("overall", "decision_aggregate_vs_matrix", "annotation_reconciliation")
        },
    }, ensure_ascii=False, indent=2))
    print(f"report written to: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
