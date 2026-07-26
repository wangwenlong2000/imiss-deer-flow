"""InputGate normalizer: user query and uploaded files -> detection units.

Phase 1 builds the units and the plumbing; the detectors that consume them belong
to the owners of violation types 1-7, so this path legitimately produces hits
only once those are switched on in ``detectors.yaml``.

Note the guide's constraint (§9.7): a hit here is not on its own a violation.
``IntentGuard`` combines it with recognized intent before the matrix ever runs.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

from deerflow.compliance.contract import DetectionUnit, Gate
from deerflow.compliance.normalizers.base import Normalizer, build_unit, make_text_items, stringify

#: Only text-ish uploads are scanned inline; binaries need a dedicated detector.
TEXT_SUFFIXES = {".txt", ".md", ".csv", ".tsv", ".json", ".jsonl", ".yaml", ".yml", ".log", ".py", ".sql", ".xml", ".html"}

#: Bytes read per uploaded file. A whole file would blow the latency budget; the
#: caller is told when a file was only partially scanned.
MAX_UPLOAD_BYTES = 200_000


class UserInputNormalizer(Normalizer):
    """Normalize a user query into a detection unit."""

    def to_units(self, payload: Any, *, gate: Gate = "InputGate", thread_id: str | None = None, **kwargs: Any) -> tuple[DetectionUnit, ...]:
        text = stringify(payload)
        if not text.strip():
            return ()
        text_items = make_text_items([("query", text)])
        if not text_items:
            return ()
        return (
            build_unit(
                unit_id=f"query:{thread_id or 'anonymous'}",
                gate=gate,
                data_type=None,
                text_items=text_items,
                field_items=(),
                raw={"kind": "user_query", "thread_id": thread_id},
            ),
        )


class UploadedFileNormalizer(Normalizer):
    """Normalize uploaded files into one unit per readable file."""

    def to_units(self, payload: Any, *, gate: Gate = "InputGate", thread_id: str | None = None, **kwargs: Any) -> tuple[DetectionUnit, ...]:
        paths: Sequence[Any] = payload if isinstance(payload, (list, tuple)) else [payload]
        units: list[DetectionUnit] = []
        for item in paths:
            unit = self._file_unit(item, gate, thread_id)
            if unit is not None:
                units.append(unit)
        return tuple(units)

    def _file_unit(self, item: Any, gate: Gate, thread_id: str | None) -> DetectionUnit | None:
        path = Path(item)
        if not path.is_file() or path.suffix.lower() not in TEXT_SUFFIXES:
            return None
        try:
            raw = path.read_bytes()[:MAX_UPLOAD_BYTES]
            text = raw.decode("utf-8", errors="replace")
        except OSError:
            return None
        text_items = make_text_items([(str(path), text)])
        if not text_items:
            return None
        truncated = path.stat().st_size > MAX_UPLOAD_BYTES
        return build_unit(
            unit_id=f"upload:{path.name}",
            gate=gate,
            data_type=None,
            text_items=text_items,
            field_items=(),
            raw={"kind": "uploaded_file", "path": str(path), "thread_id": thread_id, "truncated": truncated},
        )


__all__ = ["MAX_UPLOAD_BYTES", "TEXT_SUFFIXES", "UploadedFileNormalizer", "UserInputNormalizer"]
