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
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

from deerflow.compliance.contract import BASELINE_VIOLATION_TYPES, DetectionHit, Gate, IntentInfo, UserContext

if TYPE_CHECKING:  # pragma: no cover - typing only
    pass

logger = logging.getLogger(__name__)


def classify_compliance_intent(query: str) -> dict[str, object]:
    """Classify disclosure intent without treating sensitive entities as intent.

    This deterministic layer complements the routing LLM.  It only examines the
    requested operation and purpose; a phone number or coordinate by itself
    never raises the risk level.
    """
    text = (query or "").strip()
    lowered = text.lower()

    def has(*patterns: str) -> bool:
        return any(re.search(pattern, lowered, re.IGNORECASE) for pattern in patterns)

    intent_type = "unknown"
    operation = "unknown"
    risk_level = "unknown"
    reason_codes: list[str] = []

    if has(r"绕过|规避|跳过.{0,6}(?:限制|审查|脱敏)|bypass"):
        intent_type, operation, risk_level = "bypass", "publish", "high"
        reason_codes.append("bypass_safety_controls")
    elif has(r"解释|什么是|概念|原理|知识咨询"):
        intent_type, operation, risk_level = "analysis", "analysis", "low"
        reason_codes.append("knowledge_consultation")
    elif has(r"脱敏|匿名化|掩码|打码|模糊化|去标识|desensiti[sz]e|anonymi[sz]e|mask"):
        intent_type, operation, risk_level = "desensitize", "transform", "low"
        reason_codes.append("protective_transformation")
    elif has(r"公开发布|对外公开|公开名单|互联网发布|新闻发布|公众报告"):
        intent_type, operation, risk_level = "public_release", "publish", "high"
        reason_codes.append("public_disclosure")
    elif has(r"完整提取|原始.{0,8}(?:手机号|号码|坐标|轨迹)|获取他人|隐私.{0,6}(?:提取|获取)"):
        intent_type, operation, risk_level = "privacy_extraction", "export", "high"
        reason_codes.append("privacy_extraction")
    elif has(r"批量导出|导出.{0,12}(?:手机号|号码|坐标|轨迹|位置)"):
        intent_type, operation, risk_level = "export", "export", "high"
        reason_codes.append("sensitive_export")
    elif has(r"跨组织|跨机构|外部机构|其他委办局|合作机构"):
        intent_type, operation, risk_level = "share", "share", "high"
        reason_codes.append("external_sharing")
    elif has(r"科研|研究|统计研究"):
        intent_type, operation, risk_level = "research", "analysis", "medium"
        reason_codes.append("research_use")
    elif has(r"解释|什么是|如何|内部分析|分析|咨询|说明"):
        intent_type, operation, risk_level = "analysis", "analysis", "low"
        reason_codes.append("analysis_or_consultation")

    return {
        "intent_type": intent_type,
        "requested_operation": operation,
        "risk_level": risk_level,
        "is_high_risk": risk_level == "high",
        "confidence": 0.95 if intent_type != "unknown" else 0.0,
        "reason_codes": reason_codes,
        "reason": reason_codes[0] if reason_codes else "intent_fields_unavailable",
        "source": "intent_recognition",
    }


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

        legacy = classify_compliance_intent(intent.intent or "")
        legacy_known_low = (
            intent.intent_type == "unknown"
            and intent.risk_level == "unknown"
            and legacy["risk_level"] in {"low", "medium"}
        )
        if (intent.intent_type == "unknown" or intent.risk_level == "unknown") and not legacy_known_low:
            return IntentVerdict(
                hits=hits,
                warnings=(
                    "InputGate: compliance intent fields are missing or unknown; "
                    "findings kept for conservative policy/manual review",
                ) if hits else (),
            )

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


__all__ = ["IntentGuard", "IntentVerdict", "classify_compliance_intent"]
