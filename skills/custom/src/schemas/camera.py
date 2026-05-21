from __future__ import annotations

from typing import TypedDict


class CameraConfig(TypedDict, total=False):
    camera_id: str
    name: str
    source_type: str
    stream_url: str
    resolution: list[int]
    rois: list[dict]

