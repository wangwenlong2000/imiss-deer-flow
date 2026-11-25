"""HTTP adapter for the OneCityAgent identity and authorization service.

The mobile client token is checked by OneCityAgent. DeerFlow never trusts
caller-supplied user, role, permission, or resource-owner fields.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any

import httpx

from app.gateway.mobile_iam import MobileAuthorizationInput, MobileIAMPrincipal


class OneCityIAMProvider:
    """Delegate token validation and resource authorization to OneCityAgent."""

    def __init__(self) -> None:
        self.endpoint = os.getenv("MOBILE_IAM_ENDPOINT", "").strip()
        if not self.endpoint:
            raise RuntimeError("MOBILE_IAM_ENDPOINT is required")
        self.timeout = httpx.Timeout(float(os.getenv("MOBILE_IAM_TIMEOUT", "5")))

    async def _post(self, authorization_header: str, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    self.endpoint,
                    json=payload,
                    headers={"Authorization": authorization_header},
                )
                if response.status_code in {401, 403}:
                    raise PermissionError("OneCityAgent denied the identity or operation")
                response.raise_for_status()
                result = response.json()
        except PermissionError:
            raise
        except (httpx.HTTPError, ValueError) as exc:
            raise RuntimeError("OneCityAgent IAM service is unavailable") from exc

        if not isinstance(result, dict):
            raise RuntimeError("OneCityAgent IAM returned an invalid response")
        return result

    async def authenticate(self, authorization_header: str | None) -> Mapping[str, Any]:
        if not authorization_header:
            raise PermissionError("A OneCityAgent access token is required")
        result = await self._post(authorization_header, {"action": "authenticate"})
        principal = result.get("principal")
        if not isinstance(principal, dict):
            raise RuntimeError("OneCityAgent IAM did not return an authenticated principal")
        return {**principal, "iam_credential": authorization_header}

    async def authorize(
        self,
        principal: MobileIAMPrincipal,
        request: MobileAuthorizationInput,
    ) -> Mapping[str, Any]:
        credential = principal.iam_credential
        if not credential:
            raise PermissionError("The verified OneCityAgent credential is unavailable")
        payload = {"action": "authorize", **request.model_dump(exclude_none=True)}
        result = await self._post(credential, payload)
        decision = result.get("decision")
        if not isinstance(decision, dict):
            raise RuntimeError("OneCityAgent IAM did not return an authorization decision")
        return decision


__all__ = ["OneCityIAMProvider"]
