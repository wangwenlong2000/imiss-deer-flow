from __future__ import annotations

from typing import TypedDict


class Evidence(TypedDict):
    evidence_id: str
    event_id: str
    type: str
    uri: str
    hash: str
    created_at: str
    privacy_masked: bool

