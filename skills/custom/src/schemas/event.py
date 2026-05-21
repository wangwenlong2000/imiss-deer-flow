from __future__ import annotations

from typing import TypedDict


class EventCandidate(TypedDict, total=False):
    event_id: str
    event_type: str
    camera_id: str
    start_time: str
    end_time: str
    confidence: float
    severity: str
    related_tracks: list[str]
    reason: str
    evidence_frame_ids: list[str]
    status: str

