"""Intent and permission checks.

Guide §9.7 is explicit: a keyword hit is not a violation. At the **input gate**,
"sensitive entity + high-risk intent + scene" must all hold. "Please mask the
phone numbers in this table" is a compliance-friendly request; "extract all these
phone numbers for public release" is not — the entities are identical.

So this module can only *downgrade* an input-gate hit, never create one. At the
context and output gates the content has already been produced or retrieved, so
intent no longer excuses it and hits pass through untouched.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from deerflow.compliance.contract import BASELINE_VIOLATION_TYPES, DetectionHit, Gate, IntentInfo, UserContext

if TYPE_CHECKING:  # pragma: no cover - typing only
    pass

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class IntentVerdict:
    """Outcome of the intent gate for one batch of hits."""

    hits: tuple[DetectionHit, ...]
    dropped: tuple[DetectionHit, ...] = ()
    warnings: tuple[str, ...] = ()


class IntentGuard:
    """Combine detector hits with recognized intent and user permissions."""

    def __init__(self, *, require_high_risk_intent_at_input: bool = True, role_check_enabled: bool = False) -> None:
        self._require_high_risk = require_high_risk_intent_at_input
        # Phase 1 has no auth module to ask, so role_check degrades to
        # warn + audit rather than blocking (plan risk 12).
        self._role_check_enabled = role_check_enabled

    def apply(
        self,
        hits: tuple[DetectionHit, ...] | list[DetectionHit],
        *,
        gate: Gate,
        intent: IntentInfo | None,
        user: UserContext | None = None,  # noqa: ARG002 - reserved for the auth module
    ) -> IntentVerdict:
        hits = tuple(hits)
        if gate != "InputGate" or not self._require_high_risk:
            return IntentVerdict(hits=hits)

        # No intent signal at all: we cannot prove high-risk intent, but we also
        # cannot prove its absence. Keep the hits and say so, rather than
        # silently dropping them — a missing signal is not an all-clear.
        if intent is None:
            return IntentVerdict(hits=hits, warnings=("InputGate: no intent signal available; keyword/entity hits were not intent-filtered",) if hits else ())

        if intent.high_risk:
            return IntentVerdict(hits=hits)

        kept: list[DetectionHit] = []
        dropped: list[DetectionHit] = []
        for hit in hits:
            # Baseline violations are never excused by benign intent: hardcoded
            # credentials in a query are a leak whatever the user meant.
            if hit.violation_type in BASELINE_VIOLATION_TYPES:
                kept.append(hit)
            else:
                dropped.append(hit)

        warnings: list[str] = []
        if dropped:
            warnings.append(
                f"InputGate: {len(dropped)} hit(s) downgraded — sensitive entities present but intent {intent.intent!r} is not high-risk (guide §9.7)"
            )
        return IntentVerdict(hits=tuple(kept), dropped=tuple(dropped), warnings=tuple(warnings))

    def role_check(self, user: UserContext | None) -> tuple[bool, str | None]:
        """Resolve a ``role_check`` action.

        Phase 1 has no auth module, so this always degrades to "not verified" and
        the caller turns it into warn + audit. Wiring in real role verification
        later changes this function only.
        """
        if not self._role_check_enabled:
            return False, "role_check requested but no authorization module is wired up; degraded to warn + audit (plan risk 12)"
        if user is None or not user.roles:
            return False, "role_check requested but the request carries no user roles"
        return True, None


__all__ = ["IntentGuard", "IntentVerdict"]
