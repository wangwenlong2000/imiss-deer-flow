"""Elasticsearch store for SkillRouter Router Cards.

Reads ES connection from environment variables and operates exclusively on
SKILL_ROUTER_ES_INDEX.  Never writes to the RAG index (ES_INDEX).
"""

import logging
import os

import requests

logger = logging.getLogger(__name__)


class SkillRouterElasticStore:
    """Thin HTTP wrapper around the Elasticsearch REST API for SkillRouter documents."""

    def __init__(
        self,
        es_url: str | None = None,
        username: str | None = None,
        password: str | None = None,
        index: str | None = None,
    ) -> None:
        self.es_url = (es_url or os.getenv("ES_URL", "http://172.17.0.1:3128")).rstrip("/")
        self.username = username or os.getenv("ES_USERNAME", "")
        self.password = password or os.getenv("ES_PASSWORD", "")
        self.index = index or os.getenv("SKILL_ROUTER_ES_INDEX", "citybrain-skill-router-cards")

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def search(self, query_vector: list[float], top_k: int, filters: dict | None = None) -> list[dict]:
        """Vector-search the SkillRouter index and return hit ``_source`` dicts.

        Uses ES 9.x top-level ``knn`` syntax with ``filter`` nested inside
        the KNN clause (not under ``query``).
        """
        knn_clause: dict = {
            "field": "embedding_vector",
            "query_vector": query_vector,
            "k": top_k,
            "num_candidates": top_k * 2,
        }

        body: dict = {
            "knn": knn_clause,
            "size": top_k,
            "_source": True,
        }

        if filters:
            filter_clauses = [{"term": {k: v}} for k, v in filters.items()]
            body["knn"]["filter"] = filter_clauses

        url = f"{self.es_url}/{self.index}/_search"
        resp = requests.post(
            url,
            json=body,
            auth=(self.username, self.password),
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        resp.raise_for_status()

        hits = resp.json().get("hits", {}).get("hits", [])
        results: list[dict] = []
        for hit in hits:
            doc = hit["_source"]
            doc["_score"] = hit.get("_score")
            results.append(doc)
        return results

    def upsert_card(self, card_doc: dict) -> None:
        """Index or update a single Router Card document in ES."""
        skill_id = card_doc.get("skill_id")
        if not skill_id:
            raise ValueError("card_doc must contain 'skill_id' for use as ES document ID")

        url = f"{self.es_url}/{self.index}/_doc/{skill_id}"
        resp = requests.put(
            url,
            json=card_doc,
            auth=(self.username, self.password),
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        resp.raise_for_status()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def ensure_index_exists(self, mapping: dict) -> None:
        """Create the index with *mapping* if it does not already exist."""
        url = f"{self.es_url}/{self.index}"
        resp = requests.head(url, auth=(self.username, self.password), timeout=10)
        if resp.status_code == 404:
            resp = requests.put(url, json=mapping, auth=(self.username, self.password), headers={"Content-Type": "application/json"}, timeout=10)
            resp.raise_for_status()
            logger.info("Created ES index '%s'", self.index)
        else:
            resp.raise_for_status()
            logger.info("ES index '%s' already exists", self.index)
