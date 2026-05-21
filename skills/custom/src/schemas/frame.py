from __future__ import annotations

from typing import TypedDict


class Frame(TypedDict):
    frame_id: str
    camera_id: str
    timestamp: str
    image_uri: str
    width: int
    height: int
    sequence: int

