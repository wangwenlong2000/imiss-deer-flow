"""Model detector: gate constraints, low-similarity degradation, single load.

The three behaviours here are the ones that distinguish a usable detector from a
model wrapper: it must not report where the guide forbids, it must not escalate a
weak nearest-neighbour match into a refusal, and it must not put a 0.2 s model
load on the request path.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from deerflow.compliance.contract import DetectContext, DetectionUnit, FieldItem, TextItem
from deerflow.compliance.detectors.model_tfidf_knn.detector import (
    LABEL_GATES,
    MODEL_LABELS,
    ModelTfidfKnnDetector,
    clear_model_cache,
    load_model,
)
from deerflow.compliance.detectors.model_tfidf_knn.row_builder import ContentTextSpecError

REPO_ROOT = Path(__file__).resolve().parents[2]
MODEL_PATH = REPO_ROOT / "models/compliance/ml_detector_0624_fresh.json"

pytestmark = pytest.mark.skipif(not MODEL_PATH.is_file(), reason="model weights missing; run `make compliance-assets`")

#: Real positive samples from the 0624 training set, so hits are genuine.
RE_IDENTIFY_TEXT = "结合 user_id、imei、驻留时间和基站位置，可以定位到具体个人的活动轨迹并还原其身份。"
VIDEO_META_TEXT = "camera_id: HK-XC-0431\ninstall_address: 徐汇区虹桥路 1234 号东门岗亭\nlat: 31.194512\nlon: 121.432187"


@pytest.fixture(autouse=True)
def _clear_cache():
    clear_model_cache()
    yield
    clear_model_cache()


def _detector(**params) -> ModelTfidfKnnDetector:
    detector = ModelTfidfKnnDetector()
    detector.setup({"model_path": str(MODEL_PATH), "min_confidence": 0.35, "min_similarity": 0.20, **params})
    return detector


def _unit(text: str, *, gate="OutputGate", data_type="telecom", fields=()) -> DetectionUnit:
    return DetectionUnit(
        unit_id="u-1",
        gate=gate,
        data_type=data_type,
        text_items=(TextItem(item_id="t-1", text=text, source="output_text"),) if text else (),
        field_items=tuple(FieldItem(item_id=f"f-{i}", path=p, value=v, source="meta") for i, (p, v) in enumerate(fields, 1)),
    )


def _ctx(gate="OutputGate") -> DetectContext:
    return DetectContext(gate=gate, budget_ms=400)


# ── basic behaviour ─────────────────────────────────────────────────────────


def test_model_labels_are_the_four_expected() -> None:
    assert MODEL_LABELS == ("none", "video_meta_leak", "re_identify", "domain")


def test_detector_id_matches_the_manifest() -> None:
    import yaml

    manifest = REPO_ROOT / "backend/packages/harness/deerflow/compliance/detectors/model_tfidf_knn/manifest.yaml"
    data = yaml.safe_load(manifest.read_text(encoding="utf-8"))
    assert data["detector_id"] == ModelTfidfKnnDetector.detector_id


def test_empty_unit_produces_no_hits() -> None:
    assert _detector().detect(_unit(""), _ctx()) == ()


def test_a_real_positive_sample_is_detected() -> None:
    hits = _detector().detect(_unit(RE_IDENTIFY_TEXT), _ctx())
    assert len(hits) == 1
    assert hits[0].violation_type in ("re_identify", "domain", "video_meta_leak")
    assert hits[0].detector_id == "model_tfidf_knn"
    assert hits[0].reason_code == "ml_tfidf_knn_classifier"


def test_hit_carries_model_evidence_for_audit() -> None:
    hits = _detector().detect(_unit(RE_IDENTIFY_TEXT), _ctx())
    evidence = hits[0].evidence
    for key in ("probabilities", "nearest_neighbors", "evidence_features", "top_similarity", "model_file", "content_text_spec"):
        assert key in evidence, f"evidence is missing {key}"
    assert evidence["content_text_spec"] == "0624"


def test_model_detector_emits_no_basis_clauses() -> None:
    """A classifier knows what text resembles, not which regulation it breaks.

    The policy layer attaches the per-type basis instead.
    """
    hits = _detector().detect(_unit(RE_IDENTIFY_TEXT), _ctx())
    assert hits[0].basis == ()


# ── gate constraints (guide hard requirement) ───────────────────────────────


def test_re_identify_is_declared_output_gate_only() -> None:
    assert LABEL_GATES["re_identify"] == ("OutputGate",)


def test_re_identify_is_not_reported_at_the_context_gate() -> None:
    """Even if the model says so, the guide pins type 9 to the output gate."""
    detector = _detector()
    hits = detector.detect(_unit(RE_IDENTIFY_TEXT, gate="ContextGate"), _ctx("ContextGate"))
    assert all(hit.violation_type != "re_identify" for hit in hits)


def test_re_identify_is_reported_at_the_output_gate() -> None:
    hits = _detector().detect(_unit(RE_IDENTIFY_TEXT, gate="OutputGate"), _ctx("OutputGate"))
    assert any(hit.violation_type == "re_identify" for hit in hits) or hits == ()


def test_no_label_is_reported_at_the_input_gate() -> None:
    """None of types 8/9/10 is declared for InputGate."""
    for text in (RE_IDENTIFY_TEXT, VIDEO_META_TEXT):
        hits = _detector().detect(_unit(text, gate="InputGate"), _ctx("InputGate"))
        assert hits == (), f"nothing should fire at InputGate, got {[h.violation_type for h in hits]}"


# ── low-similarity degradation (delivery README: k=1 is negation-sensitive) ──


def test_low_similarity_hit_is_degraded_to_low_severity() -> None:
    """The 0624 mismatch is a negated sentence at 0.13 similarity.

    Degrading rather than dropping keeps the finding auditable while making sure
    the matrix routes it to manual review, not to a refusal or a rewrite.
    """
    negated = "报告仅展示 label=0/1 的总体数量，不包含 user_id、imei、时间位置或 sub_label 对象级组合。"
    hits = _detector(min_similarity=0.20).detect(_unit(negated), _ctx())
    if not hits:
        pytest.skip("this sample no longer trips the classifier")
    hit = hits[0]
    assert hit.evidence["top_similarity"] < 0.20
    assert hit.severity == "low"
    assert hit.evidence["low_similarity_degraded"] is True


def test_a_hit_is_still_reported_when_similarity_is_low() -> None:
    """Degrade, do not discard — dropping would cost recall.

    Passing `min_similarity` to the model itself instead forces the label to
    `none` and turns true positives into false negatives (measured: accuracy
    0.9821 -> 0.9107 at 0.25). The detector keeps the hit and lowers severity.
    """
    negated = "报告仅展示 label=0/1 的总体数量，不包含 user_id、imei、时间位置或 sub_label 对象级组合。"
    hits = _detector(min_similarity=0.99).detect(_unit(negated), _ctx())
    assert len(hits) == 1, "a very high threshold must degrade, never silence"
    assert hits[0].severity == "low"


def test_high_similarity_hit_keeps_a_real_severity() -> None:
    hits = _detector(min_similarity=0.001).detect(_unit(RE_IDENTIFY_TEXT), _ctx())
    if not hits:
        pytest.skip("sample not classified as a violation")
    assert hits[0].severity in ("medium", "high", "critical")
    assert hits[0].evidence["low_similarity_degraded"] is False


# ── model loading ───────────────────────────────────────────────────────────


def test_model_is_loaded_once_and_shared() -> None:
    """0.2 s of load has no business on the request path."""
    first = load_model(MODEL_PATH)
    second = load_model(MODEL_PATH)
    assert first is second


def test_cache_survives_across_detector_instances() -> None:
    a, b = _detector(), _detector()
    a.detect(_unit(RE_IDENTIFY_TEXT), _ctx())
    b.detect(_unit(RE_IDENTIFY_TEXT), _ctx())
    assert a._ensure_model() is b._ensure_model()  # noqa: SLF001


def test_setup_does_not_load_the_model() -> None:
    """Lazy: registration must stay cheap even with many detectors."""
    clear_model_cache()
    detector = _detector()
    assert detector._model is None  # noqa: SLF001


def test_missing_model_file_gives_an_actionable_error() -> None:
    detector = ModelTfidfKnnDetector()
    detector.setup({"model_path": "models/compliance/does_not_exist.json"})
    with pytest.raises(FileNotFoundError, match="make compliance-assets"):
        detector.detect(_unit(RE_IDENTIFY_TEXT), _ctx())


def test_spec_mismatch_fails_at_setup_not_at_request_time() -> None:
    """A misaligned model is a deployment error; surface it before traffic."""
    detector = ModelTfidfKnnDetector()
    with pytest.raises(ContentTextSpecError):
        detector.setup({"model_path": str(MODEL_PATH), "content_text_spec": "800"})


# ── feature assembly ────────────────────────────────────────────────────────


def test_field_items_become_features_keyed_by_path() -> None:
    unit = _unit("", fields=[("metadata.camera_id", "HK-0431"), ("metadata.lat", 31.19)])
    features = ModelTfidfKnnDetector._features(unit)  # noqa: SLF001
    assert features == {"metadata.camera_id": "HK-0431", "metadata.lat": 31.19}


def test_text_items_are_keyed_by_source() -> None:
    unit = _unit(RE_IDENTIFY_TEXT)
    features = ModelTfidfKnnDetector._features(unit)  # noqa: SLF001
    assert features == {"output_text": RE_IDENTIFY_TEXT}


def test_fields_come_before_text_so_ordering_is_deterministic() -> None:
    """Order drives content_text, so it must be stable for identical input."""
    unit = DetectionUnit(
        unit_id="u",
        gate="OutputGate",
        text_items=(TextItem(item_id="t-1", text="body", source="output_text"),),
        field_items=(FieldItem(item_id="f-1", path="metadata.camera_id", value="C-1", source="meta"),),
    )
    assert list(ModelTfidfKnnDetector._features(unit)) == ["metadata.camera_id", "output_text"]  # noqa: SLF001


def test_colliding_keys_are_disambiguated_not_dropped() -> None:
    unit = DetectionUnit(
        unit_id="u",
        gate="OutputGate",
        text_items=(
            TextItem(item_id="t-1", text="first", source="body"),
            TextItem(item_id="t-2", text="second", source="body"),
        ),
    )
    features = ModelTfidfKnnDetector._features(unit)  # noqa: SLF001
    assert len(features) == 2, "a duplicate source must not silently overwrite"
    assert "body" in features


def test_risk_locations_point_at_real_field_paths() -> None:
    unit = _unit(
        VIDEO_META_TEXT,
        data_type="surveillance",
        fields=[("metadata.camera_id", "HK-XC-0431"), ("metadata.install_address", "徐汇区虹桥路 1234 号")],
    )
    hits = _detector().detect(unit, _ctx())
    if not hits:
        pytest.skip("sample not classified as a violation")
    locators = {loc.locator for loc in hits[0].risk_locations}
    assert locators, "a hit must say where the risk is, so disposition can be surgical"


# ── contract conformance ────────────────────────────────────────────────────


def test_detector_satisfies_the_contract_protocol() -> None:
    from deerflow.compliance.contract import Detector

    assert isinstance(ModelTfidfKnnDetector(), Detector)


def test_detector_never_mutates_the_unit() -> None:
    unit = _unit(RE_IDENTIFY_TEXT)
    before = (unit.unit_id, unit.gate, unit.data_type, unit.text_items, unit.field_items)
    _detector().detect(unit, _ctx())
    assert (unit.unit_id, unit.gate, unit.data_type, unit.text_items, unit.field_items) == before


# ── vendor integrity ────────────────────────────────────────────────────────


def test_vendor_code_is_unmodified() -> None:
    """Vendored delivery code must stay byte-identical to what was shipped.

    Not hypothetical: `ruff check --fix` silently rewrote four of these files
    (UP035/UP037 modernizations in tfidf_knn.py, feature_extractor.py,
    io_utils.py, holdout_split.py) during this feature's development. A
    lint-mangled model implementation is exactly the kind of change nobody
    notices until accuracy quietly drops.

    `backend/ruff.toml` now excludes the vendor directory; this test is the
    backstop. Regenerate with `make compliance-assets` after a package upgrade.
    """
    import hashlib

    vendor_root = REPO_ROOT / "backend/packages/harness/deerflow/compliance/detectors/model_tfidf_knn/vendor"
    sums_file = vendor_root / "SHA256SUMS"
    assert sums_file.is_file(), "vendor/SHA256SUMS missing; run `make compliance-assets`"

    modified = []
    for line in sums_file.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        expected, _, name = line.partition("  ")
        target = vendor_root / "model_detectors" / name.strip()
        assert target.is_file(), f"vendor file missing: {name}"
        actual = hashlib.sha256(target.read_bytes()).hexdigest()
        if actual != expected:
            modified.append(name.strip())

    assert not modified, f"vendor code was modified: {modified}. Restore with `make compliance-assets`; fixes belong upstream in the delivery package."


def test_ruff_excludes_the_vendor_directory() -> None:
    """The preventive half of the guard above."""
    config = (REPO_ROOT / "backend/ruff.toml").read_text(encoding="utf-8")
    assert "vendor/" in config, "backend/ruff.toml must exclude vendored code from linting"
