from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class InMemoryStorage:
    """Small storage adapter for demos and tests."""

    objects: dict[str, bytes] = field(default_factory=dict)

    def upload_bytes(self, content: bytes, key: str) -> str:
        uri = key if "://" in key else f"memory://{key.lstrip('/')}"
        self.objects[uri] = content
        return uri

    def copy(self, source_uri: str, key: str) -> str:
        return self.upload_bytes(self.read_bytes(source_uri), key)

    def read_bytes(self, uri: str) -> bytes:
        if uri in self.objects:
            return self.objects[uri]
        path = uri.removeprefix("file://")
        if Path(path).exists():
            return Path(path).read_bytes()
        return uri.encode("utf-8")


@dataclass
class SkillContext:
    trace_id: str
    tenant_id: str
    config: dict[str, Any]
    storage: Any = field(default_factory=InMemoryStorage)
    logger: Any = None
    registry: Any = None


class BaseSkill(ABC):
    name: str
    version: str = "1.0.0"

    @abstractmethod
    def run(self, input_data: dict[str, Any], context: SkillContext) -> dict[str, Any]:
        raise NotImplementedError

    def success(self, data: dict[str, Any], confidence: float = 1.0) -> dict[str, Any]:
        return {
            "skill": self.name,
            "version": self.version,
            "status": "success",
            "confidence": round(confidence, 4),
            "data": data,
        }

    def failed(
        self,
        code: str,
        message: str,
        retryable: bool = False,
        detail: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return {
            "skill": self.name,
            "version": self.version,
            "status": "failed",
            "error_code": code,
            "message": message,
            "retryable": retryable,
            "detail": detail or {},
        }
