"""Detector access adapters.

Three ways in, one interface out. The engine and router only ever see an object
with ``setup()`` / ``detect(unit, ctx)`` / ``close()``, so the choice of adapter
is invisible above this layer — that is what lets a detector author pick a
technology stack without negotiating with the engine.

===============  ====================================================  ==========
adapter          use when                                              isolation
===============  ====================================================  ==========
``inprocess``    pure Python, light dependencies                       none
``subprocess``   heavy deps (torch/OpenCV/spaCy) or another language   process
``http``         already a service, needs a GPU or its own scaling     network
===============  ====================================================  ==========
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from deerflow.compliance.adapters.http_service import HttpServiceAdapter
from deerflow.compliance.adapters.inprocess import InProcessAdapter
from deerflow.compliance.adapters.subprocess_cli import SubprocessCliAdapter

ADAPTER_NAMES: tuple[str, ...] = ("inprocess", "subprocess_cli", "http_service")


def build_adapter(*, adapter: str, detector_id: str, manifest: Mapping[str, Any], params: Mapping[str, Any]) -> Any:
    """Construct the adapter named by a detector's manifest.

    Raises ``ValueError`` for an unknown adapter or a manifest missing the field
    that adapter needs — a misconfigured detector must fail at registration, not
    at the first request through a compliance gate.
    """
    if adapter == "inprocess":
        entry = manifest.get("entry")
        if not entry:
            raise ValueError(f"detector {detector_id}: `entry` is required for the inprocess adapter")
        return InProcessAdapter(detector_id=detector_id, entry=str(entry), params=params)

    if adapter == "subprocess_cli":
        command = manifest.get("command")
        if isinstance(command, str):
            command = [command]
        return SubprocessCliAdapter(
            detector_id=detector_id,
            command=command or [],
            params=params,
            timeout_ms=int(params.get("timeout_ms") or manifest.get("timeout_ms") or 2000),
            cwd=manifest.get("cwd"),
            env=manifest.get("env"),
        )

    if adapter == "http_service":
        return HttpServiceAdapter(
            detector_id=detector_id,
            endpoint=str(manifest.get("endpoint") or ""),
            params=params,
            timeout_ms=int(params.get("timeout_ms") or manifest.get("timeout_ms") or 2000),
            headers=manifest.get("headers"),
        )

    raise ValueError(f"detector {detector_id}: unknown adapter {adapter!r}; expected one of {ADAPTER_NAMES}")


__all__ = [
    "ADAPTER_NAMES",
    "HttpServiceAdapter",
    "InProcessAdapter",
    "SubprocessCliAdapter",
    "build_adapter",
]
