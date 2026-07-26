"""OutputGate normalizer: model output -> detection units.

The output gate sees free text, so a unit here is simple: one text item holding
the assistant's answer. The single field that matters for routing is
``data_type``, which the caller may pass in when it knows what the answer was
built from.
"""

from __future__ import annotations

from typing import Any

from deerflow.compliance.contract import DetectionUnit, Gate
from deerflow.compliance.normalizers.base import Normalizer, build_unit, make_text_items


def extract_text(content: Any) -> str:
    """Flatten LangChain message content into plain text.

    Message content is either a string or a list of content blocks; reasoning and
    tool-use blocks are skipped because only what the user actually sees needs
    retracting.
    """
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict) and block.get("type") == "text" and isinstance(block.get("text"), str):
                parts.append(block["text"])
        return "".join(parts)
    return "" if content is None else str(content)


class LlmOutputNormalizer(Normalizer):
    """Wrap a generated answer as a single detection unit."""

    def to_units(
        self,
        payload: Any,
        *,
        gate: Gate = "OutputGate",
        message_id: str | None = None,
        data_type: str | None = None,
        **kwargs: Any,
    ) -> tuple[DetectionUnit, ...]:
        text = extract_text(payload if not hasattr(payload, "content") else payload.content)
        if not text.strip():
            return ()

        resolved_id = message_id or getattr(payload, "id", None) or "output"
        text_items = make_text_items([("output", text)])
        if not text_items:
            return ()

        return (
            build_unit(
                unit_id=f"output:{resolved_id}",
                gate=gate,
                data_type=data_type,
                text_items=text_items,
                field_items=(),
                raw={"kind": "llm_output", "message_id": resolved_id},
            ),
        )


__all__ = ["LlmOutputNormalizer", "extract_text"]
