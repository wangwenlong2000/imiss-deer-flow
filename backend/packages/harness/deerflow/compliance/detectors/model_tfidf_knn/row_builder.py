"""``content_text`` reconstruction — the 0624 normalization spec.

Why this file exists
--------------------
The delivery package ships the code that *consumes* ``content_text``
(``feature_extractor.py``) but not the normalizer that *produced* it. The rule
was therefore reverse-engineered from the data and validated across all 282 rows
of ``0624_supported_split``: 282/282 exact string matches, and 56/56 identical
model predictions when the row is rebuilt from ``features`` alone.

That means production only needs ``features`` to reproduce training-time
``content_text`` byte for byte, so the model behaves online exactly as it did
offline.

⚠️ Two datasets, two specs — never mix them
-------------------------------------------
============================  ===========================  =====================
dataset                       model                        content_text spec
============================  ===========================  =====================
``0624_supported_split``      ``ml_detector_0624_fresh``   ``key: value`` per line
(282 rows)                    **the production model**     — implemented here
``all_normalized_800.jsonl``  ``ml_detector.json``         recursive walk of
(818 rows)                                                 ``raw_content``
============================  ===========================  =====================

Feeding 0624-spec text to the 800-row model (or vice versa) misaligns the feature
distribution and quietly destroys the 98% offline accuracy — a silent degradation
that is extremely hard to trace back. ``manifest.yaml`` therefore records
``content_text_spec: "0624"`` and :func:`assert_spec_supported` fails fast on any
mismatch.

**This is the only place ``content_text`` is assembled.** Duplicating the
concatenation elsewhere is how the two specs would drift apart again.
``scripts/verify_content_text_rule.py`` guards this file in CI.
"""

from __future__ import annotations

from typing import Any, Mapping

#: The normalization spec this module implements.
CONTENT_TEXT_SPEC = "0624"


class ContentTextSpecError(ValueError):
    """Raised when a model declares a spec this builder does not implement."""


def build_content_text(features: Mapping[str, Any]) -> str:
    """Reconstruct ``content_text`` from a sample's ``features`` (0624 spec).

    Two cases, both verified against all 282 rows:

    * exactly one key and it is named ``content`` -> the bare value, no prefix
    * anything else -> ``"key: value"`` joined by newlines, in insertion order

    Insertion order is the concatenation order (dicts are ordered in Python 3.7+),
    so callers must preserve the order in which fields were produced.
    """
    if len(features) == 1 and "content" in features:
        return str(features["content"])
    return "\n".join(f"{key}: {value}" for key, value in features.items())


def build_row(
    *,
    features: Mapping[str, Any],
    data_type: str | None = None,
    gate: str | None = None,
) -> dict[str, Any]:
    """Assemble the full row the vendor model expects.

    ``feature_extractor.extract_features`` reads ``data_type``, ``gate`` (or
    ``trigger_gate``), ``content_text`` and ``features``. Anything else is
    ignored, so the row is kept to exactly those four keys.
    """
    ordered = dict(features)
    return {
        "data_type": data_type,
        "gate": gate,
        "content_text": build_content_text(ordered),
        "features": ordered,
    }


def assert_spec_supported(declared: str | None) -> None:
    """Fail fast when a model was trained under a different spec.

    Called at detector setup, not per request: a spec mismatch is a deployment
    error, and the only safe moment to surface it is before any traffic is judged
    against a model whose features are silently misaligned.
    """
    if declared is None:
        raise ContentTextSpecError(
            "the model manifest does not declare `content_text_spec`. Two incompatible normalization specs exist "
            f"(see this module's docstring); declare `content_text_spec: \"{CONTENT_TEXT_SPEC}\"` explicitly."
        )
    if str(declared) != CONTENT_TEXT_SPEC:
        raise ContentTextSpecError(
            f"content_text spec mismatch: the model declares {declared!r} but row_builder implements {CONTENT_TEXT_SPEC!r}. "
            "Mixing specs misaligns the feature distribution and silently destroys offline accuracy. "
            "Use the matching model file, or implement the declared spec here."
        )


__all__ = ["CONTENT_TEXT_SPEC", "ContentTextSpecError", "assert_spec_supported", "build_content_text", "build_row"]
