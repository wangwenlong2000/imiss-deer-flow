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
from collections.abc import Iterable

from deerflow.compliance.contract import DetectionHit

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

    # Literal replacement first: masking preserves length, so char spans remain
    # aligned with the original string while both location forms are applied.
    for literal in {t for hit in hits for t in _literal_texts(hit)}:
        if literal and literal in result:
            result = result.replace(literal, MASK_CHARACTER * len(literal))
            changed = True

    if spans:
        # Literal masking preserves string length, so the original offsets are
        # still valid. Apply right-to-left so overlapping replacements do not
        # shift earlier offsets, even when another hit was already masked by
        # its literal location.
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


def apply_actions(text: str, actions: Iterable[str], hits: Iterable[DetectionHit]) -> str | None:
    """Apply the payload-changing part of a matrix action set.

    The matrix contains both compound actions and fallbacks.  For example,
    ``aggregate + rewrite`` means that both transformations are required, while
    ``desensitize + refuse`` means "try to sanitize; refuse if that cannot be
    done reliably".  Treating every list as "pick the strongest action" loses
    that distinction and, in particular, would refuse answers that the matrix
    explicitly says to sanitize first.

    ``warn``/``report``/``role_check``/``manual_review`` are intentionally left
    to the audit/metadata layer. Returns ``None`` when no payload action exists.
    """
    hits = list(hits)
    action_set = set(actions)
    transform_actions = action_set & {"aggregate", "desensitize", "rewrite"}

    if not transform_actions:
        return apply_refuse(text) if "refuse" in action_set else None

    # All current matrix cells containing both `refuse` and a transform use
    # refuse as a reliability fallback.  Try the transform first and only
    # refuse when no safe location can be changed.
    if "aggregate" in transform_actions and "rewrite" in transform_actions:
        masked, changed = mask_text(text, hits)
        if not changed:
            return apply_refuse(text)
        return f"{REWRITE_NOTICE}\n{AGGREGATE_NOTICE}\n{masked}"

    if "aggregate" in transform_actions:
        masked, changed = mask_text(text, hits)
        return f"{AGGREGATE_NOTICE}\n{masked}" if changed else apply_refuse(text)

    if "rewrite" in transform_actions:
        masked, changed = mask_text(text, hits)
        return f"{REWRITE_NOTICE}\n{masked}" if changed else apply_refuse(text)

    # Desensitization is also the first choice for cells such as
    # `desensitize / refuse` and `refuse; desensitize`.
    masked, changed = mask_text(text, hits)
    if changed:
        return masked
    return apply_refuse(text) if "refuse" in action_set else apply_desensitize(text, hits)


def has_terminal_refusal(
    actions: Iterable[str],
    per_violation_actions: dict[str, Iterable[str]] | None = None,
) -> bool:
    """Whether a decision contains a refusal that is not a transform fallback.

    The merged action tuple can contain ``refuse`` together with a sanitizer
    belonging to another violation.  A bottom-line hit such as a credential or
    illegal content must still win that merge.  Per-violation matrix cells let
    the middlewares distinguish that terminal refusal from cells that mean
    "sanitize, otherwise refuse".
    """
    if per_violation_actions:
        for cell_actions in per_violation_actions.values():
            cell = set(cell_actions)
            if "refuse" in cell and not cell & {"aggregate", "desensitize", "rewrite"}:
                return True
        return False
    return "refuse" in set(actions)


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
    "has_terminal_refusal",
    "mask_text",
]
