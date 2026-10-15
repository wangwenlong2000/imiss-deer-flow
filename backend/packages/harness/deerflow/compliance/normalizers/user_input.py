"""InputGate normalizer: user query and uploaded files -> detection units.

The normalizer preserves source provenance. In particular, a plain query is
marked ``user_query`` and cannot be mistaken for the structured result emitted
by a media preprocessor. The latter uses ``MediaPreprocessedNormalizer`` and
is the only InputGate source eligible for type-8 media metadata detection.

Note the guide's constraint (§9.7): a hit here is not on its own a violation.
``IntentGuard`` combines it with recognized intent before the matrix ever runs.
"""

from __future__ import annotations

import json
import re
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from deerflow.compliance.contract import DetectionUnit, FieldItem, Gate
from deerflow.compliance.normalizers.base import Normalizer, build_unit, make_text_items, stringify

#: Only text-ish uploads are scanned inline; binaries need a dedicated detector.
TEXT_SUFFIXES = {".txt", ".md", ".csv", ".tsv", ".json", ".jsonl", ".yaml", ".yml", ".log", ".py", ".sql", ".xml", ".html"}

#: Bytes read per uploaded file. A whole file would blow the latency budget; the
#: caller is told when a file was only partially scanned.
MAX_UPLOAD_BYTES = 200_000

# Structured input is untrusted. These limits keep parsing bounded while leaving
# enough room for ordinary JSON requests and embedded result fragments.
MAX_STRUCTURED_BYTES = 200_000
MAX_STRUCTURED_DEPTH = 6
MAX_STRUCTURED_FIELDS = 256
MAX_EMBEDDED_JSON_VALUES = 16
_KEY_VALUE_RE = re.compile(
    r"^(?P<indent>[ \t]*)(?P<key>\"(?:[^\"\\\\]|\\\\.)*\"|'[^']*'|[A-Za-z_][\w.-]*)\s*(?::|=)\s*(?P<value>.*?)\s*$"
)


def _bounded_scalar(value: str) -> Any:
    """Decode common YAML/key=value scalars without evaluating code."""
    value = value.strip()
    if not value:
        return None
    if value.startswith(("\"", "'")) and value.endswith(value[0]) and len(value) >= 2:
        if value[0] == '"':
            try:
                return json.loads(value)
            except (json.JSONDecodeError, ValueError):
                pass
        return value[1:-1]
    lowered = value.lower()
    if lowered in {"null", "~"}:
        return None
    if lowered in {"true", "false"}:
        return lowered == "true"
    if value.startswith(("[", "{")) and value.endswith(("]", "}")):
        try:
            parsed = json.loads(value)
        except (json.JSONDecodeError, ValueError):
            pass
        else:
            if isinstance(parsed, (dict, list)):
                return parsed
    try:
        return float(value) if "." in value else int(value)
    except ValueError:
        return value


def _parse_simple_key_values(text: str) -> tuple[dict[str, Any], str | None]:
    """Parse bounded scalar YAML/key=value lines as a field-only fallback.

    This intentionally supports the common interchange form, not arbitrary YAML
    syntax. Complex or malformed input remains available to the text detector.
    """
    fields: dict[str, Any] = {}
    stack: list[tuple[int, str]] = []
    saw_candidate = False
    for line in text.splitlines()[:MAX_STRUCTURED_FIELDS]:
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        match = _KEY_VALUE_RE.match(line)
        if not match:
            continue
        saw_candidate = True
        indent = len(match.group("indent"))
        key = match.group("key")
        if key[:1] in {"\"", "'"} and key[-1:] == key[:1]:
            key = key[1:-1]
        value = _bounded_scalar(match.group("value").split(" #", 1)[0])
        while stack and indent <= stack[-1][0]:
            stack.pop()
        path = ".".join([item[1] for item in stack] + [key])
        if value is None and match.group("value").strip() in {"", "null", "~"}:
            stack.append((indent, key))
            continue
        if value is not None:
            fields[path] = value
    if not saw_candidate:
        return {}, None
    return fields, None if fields else "no_scalar_fields"


def _parse_structured_payload(payload: Any, text: str) -> tuple[Any, str, str | None]:
    """Find a bounded JSON object/array or simple key/value representation."""
    if isinstance(payload, (dict, list, tuple)):
        try:
            encoded_size = len(json.dumps(payload, ensure_ascii=False).encode("utf-8"))
        except (TypeError, ValueError):
            return None, "none", "payload_not_json_serializable"
        if encoded_size > MAX_STRUCTURED_BYTES:
            return None, "none", "structured_payload_too_large"
        return payload, "structured_payload", None

    if not isinstance(payload, str):
        return None, "none", None

    if len(payload.encode("utf-8")) <= MAX_STRUCTURED_BYTES:
        decoder = json.JSONDecoder()
        candidates = [payload.strip()]
        saw_json_candidate = payload.lstrip().startswith(("{", "["))
        # raw_decode also handles JSON embedded in a natural-language request;
        # start positions are bounded so a long text cannot cause unbounded work.
        # An embedded object is useful in natural-language requests, but an
        # embedded array is often just a value on a YAML/key=value line. Only
        # parse arrays when the whole payload is an array; otherwise the
        # fallback parser must retain its parent field name.
        candidates.extend(
            payload[index:]
            for index, char in enumerate(payload[:MAX_STRUCTURED_BYTES])
            if char == "{"
        )
        saw_json_candidate = saw_json_candidate or any(char == "{" for char in payload[:MAX_STRUCTURED_BYTES])
        for candidate in candidates[:MAX_STRUCTURED_FIELDS]:
            try:
                value, _ = decoder.raw_decode(candidate)
            except (json.JSONDecodeError, ValueError):
                continue
            if isinstance(value, (dict, list, tuple)):
                return value, "json", None

    fields, error = _parse_simple_key_values(text)
    if fields:
        return fields, "yaml_or_key_value", None
    if len(payload.encode("utf-8")) > MAX_STRUCTURED_BYTES:
        return None, "none", "structured_payload_too_large"
    if saw_json_candidate:
        return None, "none", "invalid_json"
    return None, "none", error


def _bounded_field_items(payload: Any, *, source: str) -> tuple[FieldItem, ...]:
    from deerflow.compliance.normalizers.base import flatten_fields

    items = flatten_fields(
        payload,
        prefix="",
        source=source,
        max_depth=MAX_STRUCTURED_DEPTH,
    )[:MAX_STRUCTURED_FIELDS]
    return tuple(items)


def _embedded_json_values(payload: str) -> tuple[dict[str, Any], ...]:
    """Find bounded JSON objects embedded in one text request."""
    if len(payload.encode("utf-8")) > MAX_STRUCTURED_BYTES:
        return ()
    decoder = json.JSONDecoder()
    values: list[dict[str, Any]] = []
    cursor = 0
    attempts = 0
    while len(values) < MAX_EMBEDDED_JSON_VALUES and attempts < MAX_STRUCTURED_FIELDS:
        start = payload.find("{", cursor)
        if start < 0:
            break
        attempts += 1
        try:
            value, end = decoder.raw_decode(payload, start)
        except json.JSONDecodeError:
            cursor = start + 1
            continue
        cursor = end
        if isinstance(value, dict):
            values.append(value)
    return tuple(values)


class UserInputNormalizer(Normalizer):
    """Normalize a user query into a detection unit."""

    def to_units(self, payload: Any, *, gate: Gate = "InputGate", thread_id: str | None = None, **kwargs: Any) -> tuple[DetectionUnit, ...]:
        text = payload if isinstance(payload, str) else stringify(payload)
        if not text.strip():
            return ()
        text_items = make_text_items([("query", text)])
        if not text_items:
            return ()
        structured, structured_format, parse_error = _parse_structured_payload(payload, text)
        if isinstance(payload, str) and structured_format == "json":
            embedded_values = _embedded_json_values(payload)
            if len(embedded_values) > 1:
                structured = {
                    f"embedded_json[{index}]": value
                    for index, value in enumerate(embedded_values, start=1)
                }
        field_items = _bounded_field_items(structured, source="query") if structured is not None else ()
        return (
            build_unit(
                unit_id=f"query:{thread_id or 'anonymous'}",
                gate=gate,
                data_type=None,
                text_items=text_items,
                field_items=field_items,
                raw={
                    "kind": "user_query",
                    "thread_id": thread_id,
                    "normalization": {
                        "structured_format": structured_format,
                        "structured": bool(field_items),
                        "field_count": len(field_items),
                        "parse_error": parse_error,
                    },
                },
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


class MediaPreprocessedNormalizer(Normalizer):
    """Normalize trusted output from an uploaded image/video preprocessor."""

    def to_units(
        self,
        payload: Any,
        *,
        gate: Gate = "InputGate",
        thread_id: str | None = None,
        **kwargs: Any,
    ) -> tuple[DetectionUnit, ...]:
        if gate != "InputGate" or not isinstance(payload, dict):
            return ()
        if payload.get("media_preprocessed") is not True:
            return ()
        data_type = str(payload.get("data_type") or "").strip()
        if not data_type:
            return ()

        text_values = []
        for key in ("ocr_text", "visual_labels", "description", "content"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                text_values.append((key, value))
        text_items = make_text_items(text_values)
        fields = payload.get("features") or payload.get("metadata") or {}
        field_items = _bounded_field_items(fields, source=str(payload.get("source_file") or "media"))
        if not text_items and not field_items:
            return ()
        source_file = str(payload.get("source_file") or "")
        return (
            build_unit(
                unit_id=f"media:{thread_id or 'anonymous'}:{source_file or 'result'}",
                gate=gate,
                data_type=data_type,
                text_items=text_items,
                field_items=field_items,
                raw={
                    "kind": "media_preprocessed",
                    "media_preprocessed": True,
                    "source_file": source_file,
                    "thread_id": thread_id,
                },
            ),
        )


__all__ = [
    "MAX_STRUCTURED_BYTES",
    "MAX_STRUCTURED_DEPTH",
    "MAX_STRUCTURED_FIELDS",
    "MAX_EMBEDDED_JSON_VALUES",
    "MAX_UPLOAD_BYTES",
    "MediaPreprocessedNormalizer",
    "TEXT_SUFFIXES",
    "UploadedFileNormalizer",
    "UserInputNormalizer",
]
