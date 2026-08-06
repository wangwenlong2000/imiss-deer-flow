"""Startup self-check for the compliance subsystem.

Why this exists
---------------
``fail_mode: closed`` means "this content could not be judged, so hold it back".
It does **not** mean "the system was never installed correctly, so hold back
everyone's traffic" — but without a startup check those two collapse into the
same behaviour, and catastrophically so:

``get_engine()`` builds lazily *on the request path*. If the policy matrix or the
model weights are missing, every gate raises, every raise becomes a
``failure_decision()``, and with the default fail mode every answer and every
tool result is replaced by a compliance-failure notice. A forgotten volume mount
takes the whole product down, and the logs blame compliance detection rather
than the missing file.

So: check once at startup, loudly, and let the app run without gates if the
install is broken. A compliance system that is *off and screaming* is far better
than one that is *on and refusing everything*.

Why a canary and not just ``build_engine()``
--------------------------------------------
Model weights are **lazy-loaded** — ``ModelTfidfKnnDetector._ensure_model()``
opens the file on the first ``detect()``, not at setup. And the asset split makes
this the likely failure: ``config/compliance/*.yaml`` is committed while
``models/compliance/`` is gitignored, so a fresh checkout gets a working policy
matrix and an empty models directory. Building the engine would succeed and the
first real request would still fail. The canary runs an actual detection, so the
lazy load happens here instead of in front of a user.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from deerflow.compliance.contract import DetectionUnit, TextItem

logger = logging.getLogger(__name__)

#: Deliberately benign. The canary proves the pipeline runs end to end; it is not
#: asserting a detection outcome, so a harmless string is exactly right.
CANARY_TEXT = "系统自检：这是一段用于验证合规检测链路可用性的无害文本。"


@dataclass
class PreflightResult:
    ok: bool
    detectors: tuple[str, ...] = ()
    errors: list[str] = field(default_factory=list)

    def describe(self) -> str:
        if self.ok:
            return f"compliance preflight OK — detectors: {', '.join(self.detectors) or '(none registered)'}"
        return "compliance preflight FAILED — " + "; ".join(self.errors)


def run_preflight() -> PreflightResult:
    """Build the engine and push one harmless unit through every enabled gate.

    Never raises. Returns a result the caller decides what to do with.
    """
    errors: list[str] = []
    detectors: tuple[str, ...] = ()

    try:
        from deerflow.compliance.engine import get_engine

        engine = get_engine()
        detectors = tuple(r.detector_id for r in engine.registry.enabled())
    except Exception as exc:
        errors.append(f"engine could not be built: {type(exc).__name__}: {exc}")
        return PreflightResult(ok=False, errors=errors)

    if not detectors:
        # Not a failure: zero detectors is a supported state (P0 shipped that
        # way). Say so, because a silently empty gate looks identical to a
        # working one.
        logger.warning("compliance: no detectors are enabled; gates will mount but never report anything")

    from deerflow.compliance.runtime import gate_enabled

    for gate in ("InputGate", "ContextGate", "OutputGate"):
        if not gate_enabled(gate):
            continue
        unit = DetectionUnit(
            unit_id=f"canary:{gate}",
            gate=gate,
            data_type=None,
            text_items=(TextItem(item_id="t-1", text=CANARY_TEXT, source="preflight"),),
        )
        try:
            engine.check([unit], gate=gate, request_id=f"preflight-{gate}")
        except Exception as exc:
            errors.append(f"{gate} canary failed: {type(exc).__name__}: {exc}")

    return PreflightResult(ok=not errors, detectors=detectors, errors=errors)


__all__ = ["CANARY_TEXT", "PreflightResult", "run_preflight"]
