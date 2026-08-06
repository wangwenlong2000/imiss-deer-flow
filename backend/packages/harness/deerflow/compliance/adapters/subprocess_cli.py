"""Subprocess adapter: ``stdin`` JSON request -> ``stdout`` JSON hits.

For detectors with heavy or conflicting dependencies (torch, OpenCV, spaCy) and
for detectors not written in Python. Process isolation is the point: the child
can crash, hang, or leak memory without touching the main service.

Wire format is :mod:`deerflow.compliance.adapters.serde`, i.e. exactly the JSON
Schema exported by ``contract.export_json_schema()``.
"""

from __future__ import annotations

import json
import logging
import subprocess
from collections.abc import Mapping, Sequence
from typing import Any

from deerflow.compliance.adapters.serde import hits_from_json, request_to_json
from deerflow.compliance.contract import ContractError, DetectContext, DetectionHit, DetectionUnit

logger = logging.getLogger(__name__)


class SubprocessCliAdapter:
    """Run a detector as a short-lived child process, one unit per invocation."""

    adapter_name = "subprocess_cli"

    def __init__(self, detector_id: str, command: Sequence[str], params: Mapping[str, Any], *, timeout_ms: int = 2000, cwd: str | None = None, env: Mapping[str, str] | None = None) -> None:
        if not command:
            raise ValueError(f"detector {detector_id}: `command` is required for the subprocess_cli adapter")
        self.detector_id = detector_id
        self._command = list(command)
        self._params = dict(params)
        self._timeout_s = max(timeout_ms, 1) / 1000.0
        self._cwd = cwd
        self._env = dict(env) if env else None

    def setup(self) -> None:
        """Nothing to warm up — the process is created per call."""

    def detect(self, unit: DetectionUnit, ctx: DetectContext) -> Sequence[DetectionHit]:
        payload = json.dumps(request_to_json(unit, ctx, self._params), ensure_ascii=False)
        try:
            completed = subprocess.run(  # noqa: S603 - command comes from a trusted manifest
                self._command,
                input=payload,
                capture_output=True,
                text=True,
                timeout=self._timeout_s,
                cwd=self._cwd,
                env=self._env,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise TimeoutError(f"detector {self.detector_id} exceeded {self._timeout_s:.3f}s") from exc
        except OSError as exc:
            raise ContractError(f"detector {self.detector_id} could not be launched: {exc}") from exc

        if completed.returncode != 0:
            stderr = (completed.stderr or "").strip()[:500]
            raise ContractError(f"detector {self.detector_id} exited with {completed.returncode}: {stderr}")

        stdout = (completed.stdout or "").strip()
        if not stdout:
            return ()
        try:
            parsed = json.loads(stdout)
        except json.JSONDecodeError as exc:
            raise ContractError(f"detector {self.detector_id} produced invalid JSON on stdout: {exc}") from exc
        return hits_from_json(parsed)

    def close(self) -> None:
        """No persistent resources to release."""


__all__ = ["SubprocessCliAdapter"]
