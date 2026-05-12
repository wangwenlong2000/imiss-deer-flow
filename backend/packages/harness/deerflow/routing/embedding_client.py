"""SkillRouter embedding service client.

Calls the SkillRouter-Embedding-0.6B OpenAI-compatible API to generate
routing_text vectors for Router Cards and query/task-segment vectors at runtime.
"""

import logging
import os

import requests

logger = logging.getLogger(__name__)


class SkillRouterEmbeddingClient:
    """Client for the SkillRouter embedding service."""

    def __init__(self, base_url: str | None = None, api_key: str | None = None) -> None:
        self.base_url = (base_url or os.getenv("SKILLROUTER_EMBEDDING_BASE_URL") or "http://192.168.200.1:7800/v1").rstrip("/")
        self.api_key = api_key or os.getenv("SKILLROUTER_EMBEDDING_BASE_KEY", "unused")

    def embed_text(self, text: str) -> list[float]:
        """Return a single embedding for *text*."""
        result = self.embed_texts([text])
        return result[0]

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        """Return one embedding per string in *texts*."""
        if not texts:
            return []

        url = f"{self.base_url}/embeddings"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}",
        }
        payload = {
            "model": "SkillRouter-Embedding-0.6B",
            "input": texts,
        }

        response = requests.post(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()

        data = response.json()
        items = sorted(data["data"], key=lambda item: item["index"])
        return [item["embedding"] for item in items]
