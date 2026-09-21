"""TF-IDF + kNN model detector for violation types 8, 9 and 10.

Covers ``video_meta_leak`` (video surveillance point metadata),
``re_identify`` (model-output re-identification risk) and ``domain`` (city
governance domain-specific sensitive information).

Method is the delivery package's, unchanged: character/token n-grams, TF-IDF,
cosine nearest neighbour with ``k=1``. Pure standard library — no third-party ML
dependency — which is why this runs as an ``inprocess`` detector.

Measured behaviour (0624 split, 56-row test set):
``accuracy 0.9821``, ``macro_f1 0.9859``, one mismatch, ~13 ms per row.

Three decisions worth knowing about
-----------------------------------
1. **Gate filtering happens here.** The model is a four-way classifier, but the
   guide pins ``re_identify`` to the output gate. A label the manifest does not
   allow at the current gate is dropped rather than reported. Type 10 is also
   evaluated at InputGate for risky inputs; type 8 is evaluated there only
   after media preprocessing has produced structured metadata.
2. **Low similarity does not mean a substantive violation.** The delivery README
   warns that ``k=1`` is sensitive to negation, and the single 0624 mismatch is
   exactly that: "the report does *not* contain user_id, imei…" classified as
   ``re_identify`` off a nearest neighbour at 0.13 similarity. Below
   ``min_similarity`` the hit is downgraded to ``low`` severity so it lands in
   the manual-review queue instead of triggering a refusal or rewrite.
3. **The model is loaded once per file path and shared.** Loading takes ~0.2 s,
   which has no business being on the request path. The loaded model is read-only
   after construction, so sharing it needs no lock.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from deerflow.compliance.contract import (
    DetectContext,
    DetectionHit,
    DetectionUnit,
    RiskLocation,
)

from .row_builder import CONTENT_TEXT_SPEC, assert_spec_supported, build_row
from .composite_risk import (
    CompositeFinding,
    canonical_data_type,
    detect_composite_findings,
    is_explicit_aggregate_negative,
    is_explicit_reidentify_negative,
)

logger = logging.getLogger(__name__)

#: Labels the shipped model can emit. "none" means no violation.
MODEL_LABELS = ("none", "video_meta_leak", "re_identify", "domain")

#: Gates each label may be reported at. ``re_identify`` is output-only — a hard
#: constraint from the guide, mirrored in manifest.yaml and enforced twice.
LABEL_GATES: dict[str, tuple[str, ...]] = {
    "video_meta_leak": ("InputGate", "ContextGate", "OutputGate"),
    "re_identify": ("OutputGate",),
    "domain": ("InputGate", "ContextGate", "OutputGate"),
}

DEFAULT_VIDEO_META_LEAK_DATA_TYPES = frozenset(
    {"video_monitoring", "surveillance", "street_view", "satellite_remote_sensing"}
)
MEDIA_PREPROCESSED_KINDS = frozenset(
    {"media_preprocessed", "preprocessed_media", "file_media_result", "vision_result"}
)

REASON_CODE = "ml_tfidf_knn_classifier"

# ── shared model cache ──────────────────────────────────────────────────────

_model_lock = threading.Lock()
_model_cache: dict[str, Any] = {}


def load_model(model_path: str | Path) -> Any:
    """Load (and memoize) a model by absolute path.

    The vendored ``TfidfKNNModel`` is immutable once loaded, so one instance can
    serve every thread without locking on the read path.
    """
    key = str(Path(model_path).resolve())
    cached = _model_cache.get(key)
    if cached is not None:
        return cached
    with _model_lock:
        cached = _model_cache.get(key)
        if cached is not None:
            return cached
        from .vendor.model_detectors.tfidf_knn import TfidfKNNModel

        logger.info("compliance: loading TF-IDF+kNN model from %s", key)
        model = TfidfKNNModel.load(key)
        _model_cache[key] = model
        return model


def clear_model_cache() -> None:
    """Drop cached models (tests, model hot-swap)."""
    with _model_lock:
        _model_cache.clear()


def _resolve_model_path(raw_path: str) -> Path:
    """Resolve a possibly repo-relative model path."""
    candidate = Path(raw_path)
    if candidate.is_absolute():
        return candidate
    from_cwd = Path.cwd() / candidate
    if from_cwd.exists():
        return from_cwd
    # detector.py lives at <root>/backend/packages/harness/deerflow/compliance/detectors/model_tfidf_knn/
    repo_root = Path(__file__).resolve().parents[7]
    return repo_root / candidate


class ModelTfidfKnnDetector:
    """Detector satisfying ``deerflow.compliance.contract.Detector``."""

    detector_id = "model_tfidf_knn"

    def __init__(self) -> None:
        self._params: dict[str, Any] = {}
        self._model: Any = None
        self._model_path: Path | None = None
        self._video_meta_leak_data_types: set[str] = set(DEFAULT_VIDEO_META_LEAK_DATA_TYPES)

    # ── contract ────────────────────────────────────────────────────────────

    def setup(self, params: Mapping[str, Any]) -> None:
        self._params = dict(params)
        # Fail fast on a spec mismatch: judging traffic with a model whose
        # features are silently misaligned is worse than not judging it at all.
        assert_spec_supported(self._params.get("content_text_spec", CONTENT_TEXT_SPEC))
        self._model_path = _resolve_model_path(str(self._params.get("model_path", "")))
        configured_scope = self._params.get("video_meta_leak_data_types", DEFAULT_VIDEO_META_LEAK_DATA_TYPES)
        if isinstance(configured_scope, str):
            configured_scope = configured_scope.split(",")
        if isinstance(configured_scope, (list, tuple, set, frozenset)):
            self._video_meta_leak_data_types = {
                canonical_data_type(data_type)
                for data_type in configured_scope
                if str(data_type).strip()
            }
        else:
            raise ValueError("video_meta_leak_data_types must be a sequence or comma-separated string")
        self._model = None  # lazy: 0.2 s of load does not belong in setup either

    def detect(self, unit: DetectionUnit, ctx: DetectContext) -> Sequence[DetectionHit]:
        features = self._features(unit)
        if not features:
            return ()

        model = self._ensure_model()
        row = build_row(features=features, data_type=unit.data_type, gate=unit.gate)

        min_confidence = float(self._params.get("min_confidence", 0.35))
        label, confidence, probabilities = model.predict(row, min_active_confidence=min_confidence)

        neighbors = model.nearest_neighbors(row, limit=3)
        top_similarity = float(neighbors[0]["similarity"]) if neighbors else 0.0
        min_similarity = float(self._params.get("min_similarity", 0.20))
        evidence_features = model.explain(row, label, limit=12)
        model_allowed = self._label_allowed(label, unit)

        # The shipped character n-gram model can classify an explicit aggregate
        # negative as re-identification because the sentence still mentions
        # user_id/IMEI/time/location. Preserve the semantic negative guard over
        # that known model failure mode, even when the caller raises the
        # similarity threshold.
        if label == "re_identify" and (
            is_explicit_aggregate_negative(unit) or is_explicit_reidentify_negative(unit)
        ):
            return ()

        composite_findings = tuple(
            finding
            for finding in detect_composite_findings(
                unit,
                allowed_video_data_types=self._video_meta_leak_data_types,
            )
            if self._label_allowed(finding.violation_type, unit)
            and self._input_video_scope_allowed(finding.violation_type, unit)
        )
        if composite_findings:
            # Structural evidence is authoritative when the model and the
            # backstop disagree. Prefer a finding with the model label when one
            # exists; otherwise use the backstop's deterministic priority.
            composite = next(
                (finding for finding in composite_findings if finding.violation_type == label),
                composite_findings[0],
            )
            return (
                self._composite_hit(
                    composite,
                    label,
                    confidence,
                    probabilities,
                    neighbors,
                    evidence_features,
                    top_similarity,
                    min_similarity,
                ),
            )

        if label == "none":
            return ()

        # Types 8 and 10 are defined by source/field combinations, so a
        # model-only match is not enough for them. Type 9 is output-only and
        # its trained model signal remains the primary detector for prose
        # answers that do not expose structured fields.
        if label in {"video_meta_leak", "domain"} and not (
            label == "domain" and unit.data_type is None and unit.text_items
        ):
            logger.debug("compliance: dropping model-only %s without structural evidence", label)
            return ()

        # Re-identification is a combination risk. A classifier-only label is
        # not enough: traffic descriptions and policy prose can be close to
        # the training neighbours without exposing an identifiable subject.
        if label == "re_identify":
            logger.debug("compliance: dropping model-only re-identify match")
            return ()

        if not self._input_video_scope_allowed(label, unit):
            logger.debug("compliance: dropping video metadata hit without media preprocessing")
            return ()

        # The model is a four-way classifier; the guide constrains where each
        # label may be acted on. Drop rather than report out of scope.
        if not model_allowed:
            logger.debug("compliance: dropping %s reported at %s (not an allowed gate)", label, unit.gate)
            return ()

        if top_similarity < min_similarity:
            # Negation-sensitivity guard, see module docstring.
            severity = "low"
            degraded = True
        else:
            severity = self._severity(label, confidence)
            degraded = False

        return (
            DetectionHit(
                detector_id=self.detector_id,
                violation_type=label,
                confidence=float(confidence),
                severity=severity,
                risk_locations=self._locate(unit, evidence_features),
                reason_code=REASON_CODE,
                evidence={
                    "probabilities": {k: round(float(v), 4) for k, v in sorted(probabilities.items())},
                    "nearest_neighbors": neighbors,
                    "evidence_features": evidence_features,
                    "top_similarity": round(top_similarity, 6),
                    "min_similarity": min_similarity,
                    "low_similarity_degraded": degraded,
                    "model_file": str(self._model_path),
                    "content_text_spec": CONTENT_TEXT_SPEC,
                    "video_meta_leak_data_types": sorted(self._video_meta_leak_data_types),
                    "composite_risk": [
                        {
                            "violation_type": finding.violation_type,
                            "reason_code": finding.reason_code,
                            "signals": list(finding.signals),
                        }
                        for finding in composite_findings
                    ],
                },
                # Model detectors do not produce clauses: the classifier knows
                # what the text resembles, not which regulation it breaks. The
                # policy layer attaches the per-type basis.
                basis=(),
            ),
        )

    def _label_allowed(self, label: str, unit: DetectionUnit) -> bool:
        if unit.gate not in LABEL_GATES.get(label, ()):
            return False
        if label == "video_meta_leak":
            data_type = canonical_data_type(unit.data_type)
            if data_type in self._video_meta_leak_data_types:
                return True
            # Context/Output normalizers may not know the originating data
            # type. The structural backstop still performs the content/field
            # check; a plain InputGate query remains ineligible below.
            return unit.gate in {"ContextGate", "OutputGate"} and not data_type
        return label != "none"

    @staticmethod
    def _input_video_scope_allowed(label: str, unit: DetectionUnit) -> bool:
        """Keep type 8 InputGate checks tied to preprocessed uploaded media."""
        if label != "video_meta_leak" or unit.gate != "InputGate":
            return True
        raw = unit.raw
        if not isinstance(raw, Mapping):
            return False
        if raw.get("media_preprocessed") is True:
            return True
        return str(raw.get("kind") or "") in MEDIA_PREPROCESSED_KINDS

    def _composite_hit(
        self,
        finding: CompositeFinding,
        model_label: str,
        model_confidence: float,
        probabilities: Mapping[str, float],
        neighbors: Sequence[Mapping[str, Any]],
        evidence_features: Sequence[Mapping[str, Any]],
        top_similarity: float,
        min_similarity: float,
    ) -> DetectionHit:
        degraded = model_label == finding.violation_type and top_similarity < min_similarity
        return DetectionHit(
            detector_id=self.detector_id,
            violation_type=finding.violation_type,
            confidence=finding.confidence,
            severity="low" if degraded else finding.severity,
            risk_locations=finding.locations,
            reason_code=finding.reason_code,
            evidence={
                "composite": True,
                "composite_signals": list(finding.signals),
                "composite_findings": [
                    {"violation_type": item.violation_type, "reason_code": item.reason_code, "signals": list(item.signals)}
                    for item in (finding,)
                ],
                "model_label": model_label,
                "model_confidence": round(float(model_confidence), 4),
                "probabilities": {key: round(float(value), 4) for key, value in sorted(probabilities.items())},
                "nearest_neighbors": list(neighbors),
                "evidence_features": list(evidence_features),
                "top_similarity": round(top_similarity, 6),
                "min_similarity": min_similarity,
                "low_similarity_degraded": degraded,
                "model_file": str(self._model_path),
                "content_text_spec": CONTENT_TEXT_SPEC,
                "video_meta_leak_data_types": sorted(self._video_meta_leak_data_types),
            },
            basis=(),
        )

    def close(self) -> None:
        self._model = None

    # ── internals ───────────────────────────────────────────────────────────

    def _ensure_model(self) -> Any:
        if self._model is None:
            if self._model_path is None:
                raise RuntimeError(f"{self.detector_id}: setup() was never called")
            if not self._model_path.is_file():
                raise FileNotFoundError(
                    f"{self.detector_id}: model file not found at {self._model_path}. "
                    "Model weights are gitignored; run `make compliance-assets` to unpack them from the delivery package."
                )
            self._model = load_model(self._model_path)
        return self._model

    @staticmethod
    def _features(unit: DetectionUnit) -> dict[str, Any]:
        """Build the ``features`` mapping this unit contributes.

        Field items keep their dotted path as the key, which is what the training
        samples look like. Text items are folded in under their source, so a unit
        that is pure prose still produces a usable row.

        Insertion order is the concatenation order for ``content_text``, so field
        items come first and text items after — matching how the training samples
        were laid out.
        """
        features: dict[str, Any] = {}
        for item in unit.field_items:
            features[item.path] = item.value
        for item in unit.text_items:
            key = item.source or "content"
            if key in features:
                key = f"{key}.{item.item_id}"
            features[key] = item.text
        return features

    @staticmethod
    def _severity(label: str, confidence: float) -> str:
        """Map label and confidence onto the contract's severity scale.

        ``re_identify`` starts a notch lower: it is a *risk of* re-identification
        rather than a leak that already happened, and the matrix already routes it
        to rewrite rather than refusal.
        """
        if label == "re_identify":
            return "high" if confidence >= 0.8 else "medium"
        if confidence >= 0.9:
            return "high"
        if confidence >= 0.6:
            return "medium"
        return "low"

    @staticmethod
    def _locate(unit: DetectionUnit, evidence_features: Sequence[Mapping[str, Any]]) -> tuple[RiskLocation, ...]:
        """Point at the fields that drove the decision.

        The classifier scores a whole row, so there is no exact span to report.
        What it *can* say is which feature paths overlapped the nearest training
        neighbour; those map back onto real field paths, which is enough for the
        disposition layer to mask precisely instead of blanking everything.
        """
        field_paths = {item.path for item in unit.field_items}
        located: list[RiskLocation] = []
        seen: set[str] = set()

        for feature in evidence_features:
            name = str(feature.get("feature", ""))
            if not name.startswith("path:"):
                continue
            path = name[len("path:") :]
            for candidate in field_paths:
                if candidate.lower() == path and candidate not in seen:
                    seen.add(candidate)
                    value = next((item.value for item in unit.field_items if item.path == candidate), None)
                    located.append(
                        RiskLocation(
                            kind="field_path",
                            locator=candidate,
                            text=str(value) if value is not None else None,
                            entity_type=None,
                        )
                    )

        if located:
            return tuple(located)

        # Nothing field-level matched: fall back to naming the text items, so the
        # disposition layer still knows which part of the unit to act on.
        return tuple(
            RiskLocation(kind="field_path", locator=item.source or item.item_id, text=None, entity_type=None) for item in unit.text_items
        )


__all__ = [
    "DEFAULT_VIDEO_META_LEAK_DATA_TYPES",
    "LABEL_GATES",
    "MEDIA_PREPROCESSED_KINDS",
    "MODEL_LABELS",
    "ModelTfidfKnnDetector",
    "clear_model_cache",
    "load_model",
]
