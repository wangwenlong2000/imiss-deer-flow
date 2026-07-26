"""Disposition executors: turn matrix actions into an actual payload change.

The matrix decides *what* should happen; this module performs it. Only actions
that transform text are implemented here — ``warn`` / ``report`` / ``role_check``
/ ``manual_review`` are recorded by the audit layer and have no payload effect,
and ``refuse`` replaces the payload wholesale.

Everything is surgical where it can be: risk locations carry character spans and
field paths, so ``desensitize`` masks the reported spans rather than nuking the
whole message. When a hit has no usable location, the fallback is deliberately
conservative.
"""

from __future__ import annotations

import logging
from typing import Iterable

from deerflow.compliance.contract import DetectionHit, RiskLocation

logger = logging.getLogger(__name__)

MASK_CHARACTER = "*"

#: Shown to the user in place of refused content.
REFUSAL_TEXT = "【合规拦截】该内容包含合规风险，已被安全策略拦截，无法展示。如需进一步处理，请联系数据合规负责人并说明业务场景。"

#: Prefix for content that was rewritten/aggregated rather than blocked outright.
REWRITE_NOTICE = "【合规改写】以下内容已按合规策略处理，移除了可能导致个体再识别的属性组合："

AGGREGATE_NOTICE = "【合规聚合】原始明细涉及合规风险，已改为仅输出统计层面的结论："


def _char_spans(hit: DetectionHit) -> list[tuple[int, int]]:
    """Extract ``char_span`` locations shaped ``"start:end"``."""
    spans: list[tuple[int, int]] = []
    for location in hit.risk_locations:
        if location.kind != "char_span":
            continue
        start_text, _, end_text = location.locator.partition(":")
        try:
            start, end = int(start_text), int(end_text)
        except ValueError:
            logger.debug("ignoring malformed char_span locator %r", location.locator)
            continue
        if 0 <= start < end:
            spans.append((start, end))
    return spans


def _literal_texts(hit: DetectionHit) -> list[str]:
    """Literal risky substrings a detector reported, longest first.

    Longest-first matters: masking a short substring that is contained in a
    longer one would leave the longer one partially visible.
    """
    texts = {location.text for location in hit.risk_locations if location.text}
    return sorted((t for t in texts if t), key=len, reverse=True)


def mask_text(text: str, hits: Iterable[DetectionHit]) -> tuple[str, bool]:
    """Mask every reported risk location in *text*.

    Returns ``(masked_text, changed)``. ``changed`` is ``False`` when nothing
    could be located, which the caller must treat as "desensitization failed"
    rather than "nothing to do".
    """
    hits = list(hits)
    spans: list[tuple[int, int]] = []
    for hit in hits:
        spans.extend(_char_spans(hit))

    result = text
    changed = False

    # Literal replacement first: spans are computed against the original string,
    # so doing spans first would invalidate them if lengths changed.
    for literal in {t for hit in hits for t in _literal_texts(hit)}:
        if literal and literal in result:
            result = result.replace(literal, MASK_CHARACTER * len(literal))
            changed = True

    if spans and not changed:
        # Apply right-to-left so earlier offsets stay valid.
        chars = list(result)
        for start, end in sorted(spans, key=lambda s: s[0], reverse=True):
            if start < len(chars):
                stop = min(end, len(chars))
                chars[start:stop] = [MASK_CHARACTER] * (stop - start)
                changed = True
        result = "".join(chars)

    return result, changed


def apply_desensitize(text: str, hits: Iterable[DetectionHit]) -> str:
    """Mask reported locations; fall back to refusal when nothing is locatable.

    Falling back to ``REFUSAL_TEXT`` is the conservative choice: the matrix said
    this content must not go out as-is, so returning it unchanged because we
    could not find the exact offsets would defeat the decision entirely.
    """
    masked, changed = mask_text(text, hits)
    if changed:
        return masked
    logger.warning("desensitize: no usable risk location; falling back to refusal")
    return REFUSAL_TEXT


def apply_refuse(_text: str = "") -> str:
    return REFUSAL_TEXT


def apply_rewrite(text: str, hits: Iterable[DetectionHit]) -> str:
    """Best-effort rewrite without calling a model.

    A true rewrite means asking an LLM to restate the text (guide §3, action 8),
    which the output gate cannot afford inline. Phase 1 therefore masks the risky
    spans and labels the result honestly, so the reader knows it was altered
    rather than being handed a silently doctored answer.
    """
    masked, changed = mask_text(text, hits)
    if not changed:
        return REFUSAL_TEXT
    return f"{REWRITE_NOTICE}\n{masked}"


def apply_aggregate(text: str, hits: Iterable[DetectionHit]) -> str:
    """Replace individual-level detail with an explicit aggregation notice."""
    masked, changed = mask_text(text, hits)
    if not changed:
        return f"{AGGREGATE_NOTICE}\n{REFUSAL_TEXT}"
    return f"{AGGREGATE_NOTICE}\n{masked}"


#: Action -> transformer. Actions absent from this map have no payload effect.
_TRANSFORMERS = {
    "refuse": lambda text, hits: apply_refuse(text),
    "rewrite": apply_rewrite,
    "aggregate": apply_aggregate,
    "desensitize": apply_desensitize,
}


def apply_actions(text: str, actions: "Iterable[str]", hits: Iterable[DetectionHit]) -> str | None:
    """Apply the strongest payload-changing action in *actions*.

    Only one transformation runs: chaining ``desensitize`` into ``rewrite`` would
    double-annotate the output and lose the original spans. Returns ``None`` when
    no action changes the payload.
    """
    hits = list(hits)
    for action in ("refuse", "rewrite", "aggregate", "desensitize"):
        if action in actions:
            return _TRANSFORMERS[action](text, hits)
    return None


__all__ = [
    "AGGREGATE_NOTICE",
    "MASK_CHARACTER",
    "REFUSAL_TEXT",
    "REWRITE_NOTICE",
    "apply_actions",
    "apply_aggregate",
    "apply_desensitize",
    "apply_refuse",
    "apply_rewrite",
    "mask_text",
]
