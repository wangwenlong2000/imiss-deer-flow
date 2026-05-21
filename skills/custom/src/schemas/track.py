from __future__ import annotations

from typing import TypedDict


class Track(TypedDict, total=False):
    track_id: str
    camera_id: str
    label: str
    start_time: str
    end_time: str
    duration_seconds: int
    last_bbox: list[float]
    trajectory: list[list[float]]
    movement_state: str
    confidence: float

