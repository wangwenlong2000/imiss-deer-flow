"""In-process adapter: a plain Python object satisfying the ``Detector`` protocol.

Suitable for pure-Python, dependency-light detectors. The model TF-IDF+kNN
detector is one of these — it needs nothing beyond the standard library.

Anything needing torch / OpenCV / spaCy should use ``subprocess_cli`` or
``http_service`` instead, so heavy dependencies stay out of the main venv.
"""

from __future__ import annotations

import importlib
import logging
from collections.abc import Mapping, Sequence
from typing import Any

from deerflow.compliance.contract import DetectContext, DetectionHit, DetectionUnit, validate_hit

logger = logging.getLogger(__name__)


def _resolve_entry(entry: str) -> Any:
    """Resolve a ``module.path:AttributeName`` entry point.

    Equivalent to ``deerflow.reflection.resolve_variable``, reimplemented here on
    purpose: the compliance subsystem is meant to stay importable on its own so a
    detector author can work against ``contract.py`` in a slim environment
    without installing the rest of the harness. It is ~10 lines of importlib.
    """
    try:
        module_path, attribute = entry.rsplit(":", 1)
    except ValueError as exc:
        raise ImportError(f"{entry!r} is not a valid entry point. Expected 'module.path:AttributeName'.") from exc

    try:
        module = importlib.import_module(module_path)
    except ImportError as exc:
        raise ImportError(f"cannot import module {module_path!r} for entry {entry!r}: {exc}") from exc

    try:
        return getattr(module, attribute)
    except AttributeError as exc:
        raise ImportError(f"module {module_path!r} has no attribute {attribute!r}") from exc


class InProcessAdapter:
    """Wrap a Python detector instance behind the uniform adapter interface."""

    adapter_name = "inprocess"

    def __init__(self, detector_id: str, entry: str, params: Mapping[str, Any]) -> None:
        self.detector_id = detector_id
        self._entry = entry
        self._params = dict(params)
        self._impl: Any | None = None

    def _resolve(self) -> Any:
        """Import and instantiate the detector class named by ``entry``.

        This is the *only* place a concrete detector is named, and even here the
        name comes from the detector's own manifest rather than from engine code.
        """
        target = _resolve_entry(self._entry)
        impl = target() if isinstance(target, type) else target
        setup = getattr(impl, "setup", None)
        if callable(setup):
            setup(self._params)
        return impl

    def setup(self) -> None:
        if self._impl is None:
            self._impl = self._resolve()

    def detect(self, unit: DetectionUnit, ctx: DetectContext) -> Sequence[DetectionHit]:
        if self._impl is None:
            self.setup()
        hits = self._impl.detect(unit, ctx)  # type: ignore[union-attr]
        if not hits:
            return ()
        # Validate even in-process results: a detector bug that emits an unknown
        # violation type must not reach the policy matrix.
        return tuple(validate_hit(hit) for hit in hits)

    def close(self) -> None:
        close = getattr(self._impl, "close", None)
        if callable(close):
            try:
                close()
            except Exception:
                logger.exception("detector %s failed to close cleanly", self.detector_id)
        self._impl = None


__all__ = ["InProcessAdapter"]
