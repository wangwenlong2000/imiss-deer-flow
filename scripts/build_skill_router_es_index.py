"""Build the SkillRouter Elasticsearch vector index from Router Cards.

Usage:
    python scripts/build_skill_router_es_index.py

Reads skills/registry.json (or scans skills/ directory if missing),
loads each router_card.json, generates embeddings via the Embedding API,
creates/updates the SKILL_ROUTER_ES_INDEX, and writes back registry status.
"""

import hashlib
import json
import logging
import os
import sys
from datetime import datetime, timezone

# Allow running from project root without PYTHONPATH manipulation
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_PROJECT_ROOT, "backend", "packages", "harness"))

from deerflow.routing.embedding_client import SkillRouterEmbeddingClient
from deerflow.routing.es_store import SkillRouterElasticStore

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _project_root() -> str:
    """Return the repo root (parent of scripts/)."""
    return _PROJECT_ROOT


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _load_registry(root: str) -> dict:
    """Load registry.json or return a stub to be filled from disk scan."""
    path = os.path.join(root, "skills", "registry.json")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    logger.info("registry.json not found, scanning skills directories")
    return _scan_skills(root)


def _scan_skills(root: str) -> dict:
    """Walk skills/custom and skills/public for router_card.json files."""
    skills: list[dict] = []
    for category in ("custom", "public"):
        cat_dir = os.path.join(root, "skills", category)
        if not os.path.isdir(cat_dir):
            continue
        for entry in sorted(os.listdir(cat_dir)):
            if entry.startswith("."):
                continue
            card_path = os.path.join(cat_dir, entry, "router_card.json")
            skill_md_path = os.path.join(cat_dir, entry, "SKILL.md")
            if os.path.exists(card_path):
                with open(card_path, "r", encoding="utf-8") as f:
                    card = json.load(f)
                skill_hash = ""
                if os.path.exists(skill_md_path):
                    with open(skill_md_path, "rb") as f:
                        skill_hash = "sha256:" + hashlib.sha256(f.read()).hexdigest()
                skills.append(
                    {
                        "id": card["identity"]["id"],
                        "name": card["identity"]["name"],
                        "scenes": card["scope"]["scenes"],
                        "is_public": card["scope"]["is_public"],
                        "task_types": card["scope"]["task_types"],
                        "input_types": card["scope"]["input_types"],
                        "router_card_path": card_path.replace(root + "/", ""),
                        "skill_md_path": os.path.join(category, entry, "SKILL.md"),
                        "enabled": True,
                        "routing_text_hash": "sha256:" + _sha256(card["routing"]["routing_text"]),
                        "es_index": os.getenv("SKILL_ROUTER_ES_INDEX", "citybrain-skill-router-cards"),
                        "es_doc_id": card["identity"]["id"],
                        "es_indexed": False,
                        "router_status": "pending_index",
                    }
                )
    return {"version": 1, "schema_version": "1.0.0", "skills": skills}


def _build_es_document(card: dict, embedding: list[float]) -> dict:
    """Convert a Router Card + embedding into an ES document."""
    identity = card["identity"]
    scope = card["scope"]
    routing = card["routing"]
    source = card["source"]
    embedding_info = card.get("embedding", {})

    return {
        "skill_id": identity["id"],
        "name": identity["name"],
        "description": identity["description"],
        "scenes": scope["scenes"],
        "is_public": scope["is_public"],
        "task_types": scope["task_types"],
        "input_types": scope["input_types"],
        "output_types": scope.get("output_types", []),
        "routing_text": routing["routing_text"],
        "body": card.get("body", {}).get("content", ""),
        "skill_dir": source.get("skill_dir", ""),
        "skill_md_path": source.get("skill_md_path", ""),
        "router_card_path": source.get("skill_dir", "") + "/router_card.json",
        "skill_md_hash": source.get("skill_md_hash", ""),
        "routing_text_hash": embedding_info.get("text_hash", "sha256:" + _sha256(routing["routing_text"])),
        "embedding_model": embedding_info.get("model", "SkillRouter-Embedding-0.6B"),
        "embedding_vector": embedding,
        "enabled": True,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def _build_mapping(dims: int) -> dict:
    """Return the ES index mapping with the detected vector dimensions."""
    return {
        "mappings": {
            "properties": {
                "skill_id": {"type": "keyword"},
                "name": {"type": "text"},
                "description": {"type": "text"},
                "scenes": {"type": "keyword"},
                "is_public": {"type": "boolean"},
                "task_types": {"type": "keyword"},
                "input_types": {"type": "keyword"},
                "output_types": {"type": "keyword"},
                "routing_text": {"type": "text"},
                "body": {"type": "text"},
                "skill_dir": {"type": "keyword"},
                "skill_md_path": {"type": "keyword"},
                "router_card_path": {"type": "keyword"},
                "skill_md_hash": {"type": "keyword"},
                "routing_text_hash": {"type": "keyword"},
                "embedding_model": {"type": "keyword"},
                "embedding_vector": {
                    "type": "dense_vector",
                    "dims": dims,
                    "index": True,
                    "similarity": "cosine",
                },
                "enabled": {"type": "boolean"},
                "updated_at": {"type": "date"},
            }
        }
    }


def main() -> None:
    root = _project_root()
    registry = _load_registry(root)

    embedding_client = SkillRouterEmbeddingClient()
    es_store = SkillRouterElasticStore()

    # ------------------------------------------------------------------
    # Collect routing texts and generate embeddings
    # ------------------------------------------------------------------
    cards: list[dict] = []
    routing_texts: list[str] = []

    for skill in registry.get("skills", []):
        if not skill.get("enabled", True):
            logger.info("Skipping disabled skill: %s", skill["id"])
            continue

        card_path = os.path.join(root, skill["router_card_path"])
        if not os.path.exists(card_path):
            logger.warning("Router card not found: %s", card_path)
            continue

        with open(card_path, "r", encoding="utf-8") as f:
            card = json.load(f)

        cards.append(card)
        routing_texts.append(card["routing"]["routing_text"])

    if not cards:
        logger.error("No router cards found to index")
        sys.exit(1)

    logger.info("Generating embeddings for %d skills", len(cards))
    # Batch embeddings to avoid very long requests
    batch_size = 16
    all_embeddings: list[list[float]] = []
    for i in range(0, len(routing_texts), batch_size):
        batch = routing_texts[i : i + batch_size]
        embeddings = embedding_client.embed_texts(batch)
        all_embeddings.extend(embeddings)
        logger.info("  Embedded %d/%d", min(i + batch_size, len(routing_texts)), len(routing_texts))

    if not all_embeddings:
        logger.error("No embeddings generated")
        sys.exit(1)

    # Auto-detect dims from first embedding
    dims = len(all_embeddings[0])
    logger.info("Embedding vector dims: %d", dims)

    # ------------------------------------------------------------------
    # Create / update ES index
    # ------------------------------------------------------------------
    mapping = _build_mapping(dims)
    es_store.ensure_index_exists(mapping)

    # ------------------------------------------------------------------
    # Upsert each document
    # ------------------------------------------------------------------
    for card, embedding in zip(cards, all_embeddings):
        doc = _build_es_document(card, embedding)
        es_store.upsert_card(doc)
        logger.info("  Indexed: %s", doc["skill_id"])

    # ------------------------------------------------------------------
    # Update registry status
    # ------------------------------------------------------------------
    now_iso = datetime.now(timezone.utc).isoformat()
    for skill in registry.get("skills", []):
        skill_id = skill["id"]
        indexed_ids = {c["identity"]["id"] for c in cards}
        if skill_id in indexed_ids:
            skill["es_indexed"] = True
            skill["router_status"] = "ready"
            skill["last_indexed_at"] = now_iso
            skill["last_router_error"] = None
        else:
            skill["es_indexed"] = False
            skill["router_status"] = skill.get("router_status", "error")

    registry["version"] = registry.get("version", 1) + 1

    out_path = os.path.join(root, "skills", "registry.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(registry, f, indent=2, ensure_ascii=False)
    logger.info("Updated registry: %s", out_path)

    logger.info("Done. %d skills indexed in '%s'", len(cards), es_store.index)


if __name__ == "__main__":
    main()
