"""ContextGate normalizer: ``SkillResult`` -> detection units.

Follows the annotation guide §1.2.3: each entry in ``result.evidence[]`` becomes
one unit, its ``data``/``description`` go into ``text_items``, and its
``metadata`` business fields are flattened into dotted ``FieldItem`` paths.

That layout is not arbitrary — it mirrors the ``features`` structure of the
training samples, which is exactly why the ``content_text`` rule reconstructed in
plan §5.1 applies unchanged to live retrieval results.
"""

from __future__ import annotations

import itertools
import json
import logging
from collections.abc import Mapping
from typing import Any

from deerflow.compliance.contract import DetectionUnit, Gate
from deerflow.compliance.normalizers.base import Normalizer, build_unit, flatten_fields, make_text_items

logger = logging.getLogger(__name__)

#: Evidence keys that describe the envelope rather than the payload.
_ENVELOPE_KEYS = {"evidence_id", "type", "title", "uri", "source_refs", "query_id", "token_estimate"}


def try_parse_skill_result(content: Any) -> dict[str, Any] | None:
    """Return the payload if *content* looks like a ``SkillResult``, else ``None``.

    Tool messages carry arbitrary text; only the structured envelope is worth
    normalizing. Anything else is left completely alone.
    """
    data: Any = content
    if isinstance(content, str):
        stripped = content.strip()
        if not stripped.startswith("{"):
            return None
        try:
            data = json.loads(stripped)
        except (json.JSONDecodeError, ValueError):
            return None
    if isinstance(content, list):
        # LangChain content blocks: find the first text block that parses.
        for block in content:
            if isinstance(block, dict) and isinstance(block.get("text"), str):
                parsed = try_parse_skill_result(block["text"])
                if parsed is not None:
                    return parsed
        return None
    if not isinstance(data, Mapping):
        return None

    # `invoke_skill` wraps results as {"status": "wrapped", "skill_result": {...}}.
    if isinstance(data.get("skill_result"), Mapping):
        data = data["skill_result"]

    if not isinstance(data, Mapping):
        return None
    if "result" not in data or not isinstance(data.get("result"), Mapping):
        return None
    if "schema_version" not in data and "skill_name" not in data:
        return None
    return dict(data)


class SkillResultNormalizer(Normalizer):
    """Turn a parsed ``SkillResult`` into one unit per evidence entry."""

    def to_units(self, payload: Any, *, gate: Gate = "ContextGate", data_type: str | None = None, **kwargs: Any) -> tuple[DetectionUnit, ...]:
        result = payload.get("result") if isinstance(payload, Mapping) else None
        if not isinstance(result, Mapping):
            return ()

        skill_name = str(payload.get("skill_name") or "skill")
        request_id = str(payload.get("request_id") or "")
        resolved_data_type = data_type or self._infer_data_type(payload)

        units: list[DetectionUnit] = []
        evidence_list = result.get("evidence")
        if isinstance(evidence_list, list):
            for index, evidence in enumerate(evidence_list, start=1):
                unit = self._evidence_unit(evidence, index, gate, skill_name, request_id, resolved_data_type)
                if unit is not None:
                    units.append(unit)

        # `display_text` is what actually reaches the model when a skill returns
        # no structured evidence; skipping it would leave that path unchecked.
        display_unit = self._display_text_unit(result, gate, skill_name, request_id, resolved_data_type)
        if display_unit is not None:
            units.append(display_unit)

        return tuple(units)

    # ── helpers ─────────────────────────────────────────────────────────────

    def _evidence_unit(
        self,
        evidence: Any,
        index: int,
        gate: Gate,
        skill_name: str,
        request_id: str,
        data_type: str | None,
    ) -> DetectionUnit | None:
        if not isinstance(evidence, Mapping):
            return None

        evidence_id = str(evidence.get("evidence_id") or f"e-{index:03d}")
        source = evidence_id
        text_counter = itertools.count(1)
        field_counter = itertools.count(1)

        texts: list[tuple[str, str]] = []
        for key in ("description", "title"):
            value = evidence.get(key)
            if isinstance(value, str) and value.strip():
                texts.append((source, value))

        data = evidence.get("data")
        if isinstance(data, str) and data.strip():
            texts.append((source, data))

        field_items = []
        if isinstance(data, (Mapping, list, tuple)):
            field_items.extend(flatten_fields(data, prefix="data", source=source, counter=field_counter))

        metadata = evidence.get("metadata")
        if isinstance(metadata, Mapping):
            field_items.extend(flatten_fields(metadata, prefix="metadata", source=source, counter=field_counter))

        # Any non-envelope key is business payload too.
        extras = {k: v for k, v in evidence.items() if k not in _ENVELOPE_KEYS and k not in ("data", "metadata", "description")}
        if extras:
            field_items.extend(flatten_fields(extras, prefix="", source=source, counter=field_counter))

        text_items = make_text_items(texts, counter=text_counter)
        if not text_items and not field_items:
            return None

        return build_unit(
            unit_id=f"{request_id or skill_name}:{evidence_id}",
            gate=gate,
            data_type=data_type,
            text_items=text_items,
            field_items=field_items,
            raw={
                "kind": "skill_evidence",
                "skill_name": skill_name,
                "evidence_id": evidence_id,
                "evidence_type": evidence.get("type"),
            },
        )

    def _display_text_unit(
        self,
        result: Mapping[str, Any],
        gate: Gate,
        skill_name: str,
        request_id: str,
        data_type: str | None,
    ) -> DetectionUnit | None:
        display_text = result.get("display_text")
        if not isinstance(display_text, str) or not display_text.strip():
            return None
        text_items = make_text_items([("display_text", display_text)])
        if not text_items:
            return None
        return build_unit(
            unit_id=f"{request_id or skill_name}:display_text",
            gate=gate,
            data_type=data_type,
            text_items=text_items,
            field_items=(),
            raw={"kind": "skill_display_text", "skill_name": skill_name},
        )

    @staticmethod
    def _infer_data_type(payload: Mapping[str, Any]) -> str | None:
        """Best-effort data type, used only for routing.

        Returning ``None`` is fine and common: it means "unknown", and the router
        then only offers the unit to detectors that accept any data type.
        """
        for key in ("data_type", "scenario"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None


def evidence_scores(payload: Mapping[str, Any]) -> dict[str, float]:
    """Map ``evidence_id -> score`` so the caller can prioritize under budget.

    Plan §6.2: when there are more evidence items than the budget allows, keep
    the highest-scoring ones rather than an arbitrary prefix.
    """
    scores: dict[str, float] = {}
    result = payload.get("result")
    if not isinstance(result, Mapping):
        return scores
    evidence_list = result.get("evidence")
    if not isinstance(evidence_list, list):
        return scores
    for index, evidence in enumerate(evidence_list, start=1):
        if not isinstance(evidence, Mapping):
            continue
        evidence_id = str(evidence.get("evidence_id") or f"e-{index:03d}")
        metadata = evidence.get("metadata") if isinstance(evidence.get("metadata"), Mapping) else {}
        raw_score = evidence.get("score", metadata.get("score"))
        try:
            scores[evidence_id] = float(raw_score)
        except (TypeError, ValueError):
            continue
    return scores


__all__ = ["SkillResultNormalizer", "evidence_scores", "try_parse_skill_result"]
