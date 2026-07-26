"""Normalizer base: turn a source payload into ``DetectionUnit`` objects.

Normalization is pluggable because the three gates see completely different
shapes — a retrieved ``SkillResult``, a generated ``AIMessage``, a user query or
an uploaded file. Detectors should never have to care which.
"""

from __future__ import annotations

import itertools
from abc import ABC, abstractmethod
from typing import Any, Iterable, Mapping, Sequence

from deerflow.compliance.contract import DetectionUnit, FieldItem, Gate, TextItem

#: Values longer than this are truncated before detection. Guards against a
#: single enormous field blowing the latency budget; truncation is reported by
#: the caller rather than hidden.
MAX_FIELD_CHARS = 20000


class Normalizer(ABC):
    """Convert one source payload into detection units."""

    @abstractmethod
    def to_units(self, payload: Any, *, gate: Gate, **kwargs: Any) -> tuple[DetectionUnit, ...]:
        """Return the units to inspect for *payload*."""


def stringify(value: Any, *, limit: int = MAX_FIELD_CHARS) -> str:
    """Render *value* as text for detection purposes."""
    if value is None:
        return ""
    if isinstance(value, str):
        text = value
    elif isinstance(value, (int, float, bool)):
        text = str(value)
    else:
        import json

        try:
            text = json.dumps(value, ensure_ascii=False, sort_keys=True)
        except (TypeError, ValueError):
            text = str(value)
    return text[:limit] if len(text) > limit else text


def flatten_fields(
    data: Any,
    *,
    prefix: str,
    source: str,
    counter: "itertools.count[int] | None" = None,
    max_depth: int = 6,
) -> list[FieldItem]:
    """Flatten a nested structure into dotted-path ``FieldItem`` objects.

    ``{"metadata": {"camera_id": "C-1"}}`` becomes ``metadata.camera_id``. The
    dotted paths matter: they are the same shape the model detector's training
    samples use as ``features`` keys, so the path itself carries signal.
    """
    counter = counter if counter is not None else itertools.count(1)
    items: list[FieldItem] = []

    def walk(node: Any, path: str, depth: int) -> None:
        if depth > max_depth:
            return
        if isinstance(node, Mapping):
            for key, value in node.items():
                walk(value, f"{path}.{key}" if path else str(key), depth + 1)
        elif isinstance(node, (list, tuple)):
            for index, value in enumerate(node):
                walk(value, f"{path}[{index}]", depth + 1)
        else:
            if node is None or node == "":
                return
            items.append(
                FieldItem(
                    item_id=f"f-{next(counter):04d}",
                    path=path,
                    value=node,
                    source=source,
                )
            )

    walk(data, prefix, 0)
    return items


def make_text_items(texts: Iterable[tuple[str, str]], *, counter: "itertools.count[int] | None" = None) -> tuple[TextItem, ...]:
    """Build ``TextItem`` objects from ``(source, text)`` pairs, skipping blanks."""
    counter = counter if counter is not None else itertools.count(1)
    items: list[TextItem] = []
    for source, text in texts:
        rendered = stringify(text)
        if rendered.strip():
            items.append(TextItem(item_id=f"t-{next(counter):04d}", text=rendered, source=source))
    return tuple(items)


def build_unit(
    *,
    unit_id: str,
    gate: Gate,
    data_type: str | None,
    text_items: "Sequence[TextItem]",
    field_items: "Sequence[FieldItem]",
    raw: Mapping[str, Any] | None = None,
) -> DetectionUnit:
    return DetectionUnit(
        unit_id=unit_id,
        gate=gate,
        data_type=data_type,
        text_items=tuple(text_items),
        field_items=tuple(field_items),
        raw=dict(raw or {}),
    )


__all__ = ["MAX_FIELD_CHARS", "Normalizer", "build_unit", "flatten_fields", "make_text_items", "stringify"]
