"""``content_text`` rule: 282/282 exact match and 100% prediction agreement.

This is the single highest-value test in the compliance suite. The normalizer
that produced ``content_text`` was never shipped, so the rule was reverse-
engineered from data. If it drifts, the model keeps returning confident answers
against a misaligned feature distribution — a silent degradation that is
extremely hard to trace back from symptoms.

``scripts/verify_content_text_rule.py`` runs the same assertions in CI.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from deerflow.compliance.detectors.model_tfidf_knn.row_builder import (
    CONTENT_TEXT_SPEC,
    ContentTextSpecError,
    assert_spec_supported,
    build_content_text,
    build_row,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
SPLIT_DIR = REPO_ROOT / "datasets/compliance/normalized/0624_supported_split"
MODEL_PATH = REPO_ROOT / "models/compliance/ml_detector_0624_fresh.json"

EXPECTED_COUNTS = {"train.jsonl": 226, "test.jsonl": 56}
EXPECTED_TOTAL = 282


def _load(filename: str) -> list[dict]:
    path = SPLIT_DIR / filename
    if not path.is_file():
        pytest.skip(f"{path} missing; run `make compliance-assets`")
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


# ── the rule itself ─────────────────────────────────────────────────────────


def test_single_content_key_has_no_prefix() -> None:
    assert build_content_text({"content": "hello"}) == "hello"


def test_single_non_content_key_gets_a_prefix() -> None:
    """Only the literal key ``content`` is special-cased."""
    assert build_content_text({"output_text": "hello"}) == "output_text: hello"


def test_multiple_keys_join_with_newlines() -> None:
    assert build_content_text({"a": 1, "b": 2}) == "a: 1\nb: 2"


def test_content_alongside_other_keys_is_not_special_cased() -> None:
    assert build_content_text({"content": "x", "other": "y"}) == "content: x\nother: y"


def test_insertion_order_is_the_concatenation_order() -> None:
    """Dicts are ordered in Python 3.7+; the rule relies on it."""
    assert build_content_text({"b": 1, "a": 2}) == "b: 1\na: 2"
    assert build_content_text({"a": 2, "b": 1}) == "a: 2\nb: 1"


def test_empty_features_produce_empty_text() -> None:
    assert build_content_text({}) == ""


def test_non_string_values_are_stringified() -> None:
    assert build_content_text({"n": 3, "f": 1.5, "b": True, "none": None}) == "n: 3\nf: 1.5\nb: True\nnone: None"


# ── ★ the regression that matters ───────────────────────────────────────────


@pytest.mark.parametrize("filename", sorted(EXPECTED_COUNTS))
def test_content_text_matches_exactly_on_every_row(filename: str) -> None:
    """Every row's stored ``content_text`` must be reproducible from ``features``."""
    rows = _load(filename)
    assert len(rows) == EXPECTED_COUNTS[filename], f"{filename} should hold {EXPECTED_COUNTS[filename]} rows"

    mismatches = [
        {"data_id": row.get("data_id"), "keys": list(row.get("features") or {})}
        for row in rows
        if build_content_text(row.get("features") or {}) != row.get("content_text")
    ]
    assert not mismatches, f"{filename}: {len(mismatches)}/{len(rows)} rows no longer reproduce; first: {mismatches[:3]}"


def test_all_282_rows_match() -> None:
    """The headline number from plan §5.1.4."""
    total = matched = 0
    for filename in EXPECTED_COUNTS:
        for row in _load(filename):
            total += 1
            if build_content_text(row.get("features") or {}) == row.get("content_text"):
                matched += 1
    assert total == EXPECTED_TOTAL
    assert matched == EXPECTED_TOTAL, f"only {matched}/{EXPECTED_TOTAL} rows reproduce"


def test_rebuilt_rows_predict_identically() -> None:
    """End-to-end: production only has ``features``, and must not drift.

    Exact string equality is necessary but not sufficient — this asserts the
    model actually behaves the same, which is the property that matters.
    """
    if not MODEL_PATH.is_file():
        pytest.skip(f"{MODEL_PATH} missing; run `make compliance-assets`")
    from deerflow.compliance.detectors.model_tfidf_knn.vendor.model_detectors.tfidf_knn import TfidfKNNModel

    model = TfidfKNNModel.load(MODEL_PATH)
    rows = _load("test.jsonl")

    disagreements = []
    for row in rows:
        original, _, _ = model.predict(row)
        rebuilt, _, _ = model.predict(
            build_row(
                features=row.get("features") or {},
                data_type=row.get("data_type"),
                gate=row.get("gate") or row.get("trigger_gate"),
            )
        )
        if original != rebuilt:
            disagreements.append({"data_id": row.get("data_id"), "original": original, "rebuilt": rebuilt})

    assert not disagreements, f"{len(disagreements)}/{len(rows)} predictions drifted: {disagreements[:3]}"


# ── row assembly ────────────────────────────────────────────────────────────


def test_build_row_emits_exactly_the_fields_the_extractor_reads() -> None:
    row = build_row(features={"a": 1}, data_type="surveillance", gate="OutputGate")
    assert set(row) == {"data_type", "gate", "content_text", "features"}
    assert row["content_text"] == "a: 1"


def test_build_row_copies_features_defensively() -> None:
    features = {"a": 1}
    row = build_row(features=features)
    row["features"]["b"] = 2
    assert "b" not in features, "the caller's mapping must not be mutated"


# ── spec binding (plan risk 3) ──────────────────────────────────────────────


def test_declared_spec_matches_the_implementation() -> None:
    assert CONTENT_TEXT_SPEC == "0624"
    assert_spec_supported("0624")  # must not raise


def test_a_different_spec_is_rejected() -> None:
    """`all_normalized_800` uses another spec; mixing them destroys accuracy."""
    with pytest.raises(ContentTextSpecError, match="spec mismatch"):
        assert_spec_supported("800")


def test_a_missing_spec_declaration_is_rejected() -> None:
    """Silence is not consent: two incompatible specs exist, so say which."""
    with pytest.raises(ContentTextSpecError, match="does not declare"):
        assert_spec_supported(None)


def test_shipped_manifest_declares_the_spec() -> None:
    import yaml

    manifest = REPO_ROOT / "backend/packages/harness/deerflow/compliance/detectors/model_tfidf_knn/manifest.yaml"
    data = yaml.safe_load(manifest.read_text(encoding="utf-8"))
    assert data["content_text_spec"] == CONTENT_TEXT_SPEC


def test_row_builder_is_the_only_place_content_text_is_assembled() -> None:
    """Duplicated concatenation is how the two specs drifted apart originally."""
    compliance_dir = REPO_ROOT / "backend/packages/harness/deerflow/compliance"
    offenders = []
    for source in compliance_dir.rglob("*.py"):
        if source.name == "row_builder.py" or "vendor" in source.parts:
            continue
        text = source.read_text(encoding="utf-8")
        if '"\\n".join(f"{' in text or "'\\n'.join(f'{" in text:
            offenders.append(str(source.relative_to(compliance_dir)))
    assert not offenders, f"content_text-style concatenation found outside row_builder.py: {offenders}"
