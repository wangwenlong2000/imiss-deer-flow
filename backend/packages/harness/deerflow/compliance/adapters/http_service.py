"""HTTP adapter: ``POST /detect`` against a standalone detector service.

For detectors that already run as a service, need a GPU, or need to scale
independently. Same JSON contract as the subprocess adapter.

``httpx`` is already a harness dependency, so this adds nothing to the venv.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from typing import Any

from deerflow.compliance.adapters.serde import hits_from_json, request_to_json
from deerflow.compliance.contract import ContractError, DetectContext, DetectionHit, DetectionUnit

logger = logging.getLogger(__name__)


class HttpServiceAdapter:
    """Call a remote detector over HTTP."""

    adapter_name = "http_service"

    def __init__(self, detector_id: str, endpoint: str, params: Mapping[str, Any], *, timeout_ms: int = 2000, headers: Mapping[str, str] | None = None) -> None:
        if not endpoint:
            raise ValueError(f"detector {detector_id}: `endpoint` is required for the http_service adapter")
        self.detector_id = detector_id
        self._endpoint = endpoint
        self._params = dict(params)
        self._timeout_s = max(timeout_ms, 1) / 1000.0
        self._headers = dict(headers or {})
        self._client: Any | None = None

    def setup(self) -> None:
        """Create the pooled client lazily so import never needs the network."""
        if self._client is None:
            import httpx

            self._client = httpx.Client(timeout=self._timeout_s, headers=self._headers)

    def detect(self, unit: DetectionUnit, ctx: DetectContext) -> Sequence[DetectionHit]:
        import httpx

        if self._client is None:
            self.setup()
        assert self._client is not None

        try:
            response = self._client.post(self._endpoint, json=request_to_json(unit, ctx, self._params))
        except httpx.TimeoutException as exc:
            raise TimeoutError(f"detector {self.detector_id} timed out after {self._timeout_s:.3f}s") from exc
        except httpx.HTTPError as exc:
            raise ContractError(f"detector {self.detector_id} transport error: {exc}") from exc

        if response.status_code >= 400:
            raise ContractError(f"detector {self.detector_id} returned HTTP {response.status_code}: {response.text[:300]}")

        try:
            parsed = response.json()
        except ValueError as exc:
            raise ContractError(f"detector {self.detector_id} returned a non-JSON body: {exc}") from exc
        return hits_from_json(parsed)

    def close(self) -> None:
        if self._client is not None:
            try:
                self._client.close()
            except Exception:
                logger.exception("detector %s failed to close its HTTP client", self.detector_id)
            self._client = None


__all__ = ["HttpServiceAdapter"]
