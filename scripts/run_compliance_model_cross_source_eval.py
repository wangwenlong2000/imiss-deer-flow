#!/usr/bin/env python3
"""Evaluate detector types 8, 9 and 10 across normalized compliance sources.

The evaluator accepts both the normalized JSONL delivery format and the raw
annotation JSONL files under ``合规检测/``. Raw ``content`` values are flattened
using the same bounded field contract as runtime input normalization.

Rows whose gold label is outside a detector's declared gate scope are reported
as ``scope_excluded`` rather than silently counted as false negatives. This is
important for type 9, which is output-only, and for the unresolved type 8
remote-sensing ownership question.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from collections import defaultdict
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
HARNESS = REPO_ROOT / "backend/packages/harness"
if str(HARNESS) not in sys.path:
    sys.path.insert(0, str(HARNESS))

from deerflow.compliance.contract import DetectContext, DetectionUnit, FieldItem, GATES, TextItem  # noqa: E402
from deerflow.compliance.detectors.model_tfidf_knn.composite_risk import canonical_data_type  # noqa: E402
from deerflow.compliance.detectors.model_tfidf_knn.detector import ModelTfidfKnnDetector  # noqa: E402
from deerflow.compliance.normalizers.base import flatten_fields, stringify  # noqa: E402


ACTIVE_TYPES = ("video_meta_leak", "re_identify", "domain")
ALLOWED_GATES = {
    "video_meta_leak": {"InputGate", "ContextGate", "OutputGate"},
    "re_identify": {"OutputGate"},
    "domain": {"InputGate", "ContextGate", "OutputGate"},
}
DEFAULT_INPUT = REPO_ROOT / "合规检测/合规评测全部十类违规类型数据"
DEFAULT_MODEL = REPO_ROOT / "models/compliance/ml_detector_0624_fresh.json"
DEFAULT_OUTPUT = REPO_ROOT / "outputs/compliance-eval/model-cross-source.json"


def load_records(path: Path) -> list[dict[str, Any]]:
    """Load JSONL records, accepting a directory of JSONL files too."""
    paths = [path] if path.is_file() else sorted(path.rglob("*.jsonl"))
    records: list[dict[str, Any]] = []
    for item in paths:
        with item.open("r", encoding="utf-8-sig", errors="replace") as stream:
            for line_no, line in enumerate(stream, start=1):
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ValueError(f"{item}:{line_no}: invalid JSON: {exc}") from exc
                if isinstance(record, dict):
                    record.setdefault("_source_path", str(item))
                    records.append(record)
    return records


def _field_items(record: Mapping[str, Any]) -> tuple[FieldItem, ...]:
    features = record.get("features")
    if isinstance(features, dict):
        items = flatten_fields(features, prefix="", source="eval", max_depth=6)
    else:
        content = record.get("content")
        if content is None:
            content = record.get("raw_content")
        items = flatten_fields(content, prefix="content", source="eval", max_depth=6)
    return tuple(
        FieldItem(item_id=f"f-{index:04d}", path=item.path, value=item.value, source=item.source)
        for index, item in enumerate(items[:256], start=1)
    )


def record_to_unit(record: Mapping[str, Any]) -> DetectionUnit:
    gate = record.get("gate") or record.get("trigger_gate")
    if gate not in GATES:
        gate = "OutputGate"
    fields = _field_items(record)
    if fields:
        text_items: tuple[TextItem, ...] = ()
    else:
        raw_text = record.get("content_text") or record.get("content") or record.get("raw_content")
        text = stringify(raw_text)
        text_items = (TextItem(item_id="t-0001", text=text, source="content"),) if text.strip() else ()
    return DetectionUnit(
        unit_id=str(record.get("sample_id") or record.get("data_id") or "sample"),
        gate=gate,
        data_type=canonical_data_type(record.get("data_type")) or None,
        text_items=text_items,
        field_items=fields,
        raw={"kind": "cross_source_eval", "source_path": record.get("_source_path")},
    )


def gold_label(record: Mapping[str, Any]) -> str:
    label = record.get("final_violation_type") or "none"
    return str(label) if label in (*ACTIVE_TYPES, "none") else "none"


def source_key(record: Mapping[str, Any]) -> str:
    data_source = record.get("data_source")
    if isinstance(data_source, Mapping):
        for key in ("type", "source_archive", "source_file"):
            value = data_source.get(key)
            if value:
                return str(value)
    return str(record.get("_source_path") or record.get("data_type") or "unknown")


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def commit_id() -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=REPO_ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return result.stdout.strip() or None


def prf(pairs: Iterable[tuple[str, str]]) -> dict[str, Any]:
    pairs = list(pairs)
    result: dict[str, Any] = {"samples": len(pairs), "accuracy": round(sum(gold == pred for gold, pred in pairs) / len(pairs), 4) if pairs else 0.0}
    for label in ACTIVE_TYPES:
        tp = sum(gold == label and pred == label for gold, pred in pairs)
        fp = sum(gold != label and pred == label for gold, pred in pairs)
        fn = sum(gold == label and pred != label for gold, pred in pairs)
        precision = tp / (tp + fp) if tp + fp else 0.0
        recall = tp / (tp + fn) if tp + fn else 0.0
        result[label] = {
            "tp": tp,
            "fp": fp,
            "fn": fn,
            "support": tp + fn,
            "precision": round(precision, 4),
            "recall": round(recall, 4),
            "f1": round(2 * precision * recall / (precision + recall), 4) if precision + recall else 0.0,
        }
    return result


def evaluate(
    records: list[dict[str, Any]],
    *,
    model_path: Path,
    min_confidence: float = 0.35,
    min_similarity: float = 0.20,
    video_data_types: Iterable[str] = ("video_monitoring", "surveillance", "street_view", "satellite_remote_sensing"),
) -> dict[str, Any]:
    detector = ModelTfidfKnnDetector()
    scope = sorted({str(value).casefold().replace("-", "_").strip() for value in video_data_types if str(value).strip()})
    detector.setup(
        {
            "model_path": str(model_path),
            "min_confidence": min_confidence,
            "min_similarity": min_similarity,
            "video_meta_leak_data_types": scope,
        }
    )

    pairs: list[tuple[str, str]] = []
    by_gate: dict[str, list[tuple[str, str]]] = defaultdict(list)
    by_data_type: dict[str, list[tuple[str, str]]] = defaultdict(list)
    by_source: dict[str, list[tuple[str, str]]] = defaultdict(list)
    excluded: list[dict[str, Any]] = []
    errors: dict[str, list[dict[str, Any]]] = {label: [] for label in ACTIVE_TYPES}

    for record in records:
        unit = record_to_unit(record)
        gold = gold_label(record)
        gate = str(unit.gate)
        if gold != "none" and gate not in ALLOWED_GATES[gold]:
            excluded.append(
                {
                    "sample_id": unit.unit_id,
                    "gold": gold,
                    "gate": gate,
                    "data_type": unit.data_type,
                    "reason": "gold label is outside the detector's declared gate scope",
                }
            )
            continue
        hits = detector.detect(unit, DetectContext(gate=unit.gate))
        predicted = hits[0].violation_type if hits else "none"
        pairs.append((gold, predicted))
        data_type = str(unit.data_type or "unknown")
        source = source_key(record)
        by_gate[gate].append((gold, predicted))
        by_data_type[data_type].append((gold, predicted))
        by_source[source].append((gold, predicted))

        if predicted != gold and (gold in ACTIVE_TYPES or predicted in ACTIVE_TYPES):
            bucket = gold if gold in ACTIVE_TYPES else predicted
            errors[bucket].append(
                {
                    "sample_id": unit.unit_id,
                    "data_id": record.get("data_id"),
                    "gold": gold,
                    "predicted": predicted,
                    "gate": gate,
                    "data_type": data_type,
                    "source": source,
                    "reason_code": hits[0].reason_code if hits else None,
                    "top_similarity": hits[0].evidence.get("top_similarity") if hits else None,
                    "content_preview": (record.get("content_text") or stringify(record.get("content")))[:400],
                }
            )

    config = {
        "min_confidence": min_confidence,
        "min_similarity": min_similarity,
        "video_meta_leak_data_types": scope,
    }
    model_meta: dict[str, Any] = {"path": str(model_path), "sha256": file_sha256(model_path)}
    try:
        payload = json.loads(model_path.read_text(encoding="utf-8"))
        model_meta.update({key: payload.get(key) for key in ("model_type", "labels", "total_docs", "k")})
    except (OSError, json.JSONDecodeError):
        pass
    return {
        "schema_version": "compliance_model_cross_source_eval_1",
        "input": sorted({record.get("_source_path") for record in records}),
        "records_loaded": len(records),
        "records_evaluated": len(pairs),
        "scope_excluded": excluded,
        "metadata": {
            "model": model_meta,
            "config": config,
            "config_sha256": hashlib.sha256(json.dumps(config, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest(),
            "commit_id": commit_id(),
        },
        "primary": prf(pairs),
        "by_gate": {key: prf(value) for key, value in sorted(by_gate.items())},
        "by_data_type": {key: prf(value) for key, value in sorted(by_data_type.items())},
        "by_source": {key: prf(value) for key, value in sorted(by_source.items())},
        "fp_fn": errors,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--model", type=Path, default=DEFAULT_MODEL)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--min-confidence", type=float, default=0.35)
    parser.add_argument("--min-similarity", type=float, default=0.20)
    parser.add_argument("--video-data-types", default="video_monitoring,surveillance,street_view,satellite_remote_sensing")
    args = parser.parse_args(argv)

    if not args.input.exists():
        print(f"FAIL: input not found: {args.input}", file=sys.stderr)
        return 1
    if not args.model.is_file():
        print(f"FAIL: model not found: {args.model}; run `make compliance-assets`", file=sys.stderr)
        return 1
    records = load_records(args.input)
    report = evaluate(
        records,
        model_path=args.model,
        min_confidence=args.min_confidence,
        min_similarity=args.min_similarity,
        video_data_types=args.video_data_types.split(","),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report["primary"], ensure_ascii=False, indent=2))
    print(f"report written to: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
