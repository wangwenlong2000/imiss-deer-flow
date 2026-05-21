from __future__ import annotations

from typing import TypedDict


class DetectedObject(TypedDict, total=False):
    object_id: str
    label: str
    confidence: float
    bbox: list[float]
    attributes: dict


class Detection(TypedDict):
    frame_id: str
    objects: list[DetectedObject]

