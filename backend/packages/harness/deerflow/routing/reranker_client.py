"""SkillRouter reranker service client.

Calls the SkillRouter-Reranker-0.6B API to score (query, candidate) pairs
and return reranked candidates sorted by relevance.
"""

import logging
import os

import requests

logger = logging.getLogger(__name__)


class SkillRouterRerankerClient:
    """Client for the SkillRouter reranker service."""

    def __init__(self, base_url: str | None = None, api_key: str | None = None) -> None:
        self.base_url = (base_url or os.getenv("SKILLROUTER_RERANKER_BASE_URL") or "http://192.168.200.1:7801/v1").rstrip("/")
        self.api_key = api_key or os.getenv("SKILLROUTER_RERANKER_BASE_KEY", "unused")

    def rerank(self, query: str, candidates: list[dict]) -> list[dict]:
        """Rerank *candidates* against *query*, returning scored dicts.

        Each candidate must at least contain ``skill_id``, ``name``,
        ``description`` and ``body``.

        Returns a new list sorted by descending score, with a ``score`` key
        attached to each candidate.
        """
        if not candidates:
            return []

        documents = []
        for c in candidates:
            parts = [
                c.get("name", ""),
                c.get("description", ""),
                c.get("routing_text", ""),
                ", ".join(c.get("scenes", [])),
                ", ".join(c.get("task_types", [])),
                c.get("body", ""),
            ]
            doc = " ".join(p for p in parts if p)
            documents.append(doc)

        url = f"{self.base_url}/rerank"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}",
        }
        payload = {
            "model": "SkillRouter-Reranker-0.6B",
            "query": query,
            "documents": documents,
        }

        response = requests.post(url, headers=headers, json=payload, timeout=60)
        response.raise_for_status()

        data = response.json()
        # Cohere-compatible response: {"results": [{"index": N, "relevance_score": F}]}
        scored = []
        for item in data["results"]:
            candidate = dict(candidates[item["index"]])
            candidate["score"] = item["relevance_score"]
            scored.append(candidate)

        scored.sort(key=lambda c: c["score"], reverse=True)
        return scored
