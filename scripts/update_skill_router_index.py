#!/usr/bin/env python3
"""Incremental update of Router Card and ES index for a single Skill.

Called by Skill Creator after creating or modifying a Skill to ensure the
routing assets (router_card.json, ES document, registry entry) stay in sync.

Usage:
    python scripts/update_skill_router_index.py --skill custom/my-new-skill
    python scripts/update_skill_router_index.py --skill public/data-analysis
"""

import argparse
import hashlib
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Allow importing sibling modules under repo-root layout
# ---------------------------------------------------------------------------
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_PROJECT_ROOT, "backend", "packages", "harness"))

from deerflow.routing.embedding_client import SkillRouterEmbeddingClient
from deerflow.routing.es_store import SkillRouterElasticStore

# Import the card-building machinery from extract_router_cards
from extract_router_cards import (
    CUSTOM_SKILL_PROFILES,
    DEFAULT_PUBLIC_PROFILE,
    GENERATOR_VERSION,
    PUBLIC_SKILL_DEFAULTS,
    SCHEMA_VERSION,
    build_router_card,
    make_routing_text,
    parse_frontmatter,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Profile resolution (mirrors extract_router_cards logic)
# ---------------------------------------------------------------------------

def resolve_profile(skill_id: str, is_custom: bool) -> dict:
    """Return the routing profile for a given skill."""
    if is_custom:
        profile = CUSTOM_SKILL_PROFILES.get(skill_id)
        if not profile:
            logger.warning("No curated profile for custom skill %s, using defaults", skill_id)
            profile = {
                "scenes": [skill_id],
                "is_public": False,
                "task_types": [],
                "input_types": [],
                "output_types": [],
                "routing": {
                    "positive_triggers": [],
                    "negative_triggers": [],
                    "keywords": [],
                    "anti_keywords": [],
                },
                "execution": {
                    "required_tools": ["read_file", "bash"],
                    "optional_tools": ["write_file"],
                    "allowed_file_patterns": [],
                    "can_run_standalone": True,
                    "can_compose_with": [],
                },
                "routing_policy": {
                    "priority": 70,
                    "conflict_group": skill_id.replace("-", "_"),
                    "prefer_when": [],
                    "defer_when": [],
                },
            }
        return profile
    else:
        return PUBLIC_SKILL_DEFAULTS.get(skill_id, dict(DEFAULT_PUBLIC_PROFILE))


# ---------------------------------------------------------------------------
# Schema validation (lightweight, no external deps)
# ---------------------------------------------------------------------------

def validate_card(card: dict) -> list[str]:
    """Return a list of validation error messages (empty = pass)."""
    errors: list[str] = []
    required_top = ["schema_version", "identity", "scope", "routing", "body", "execution", "routing_policy", "source", "embedding"]
    for key in required_top:
        if key not in card:
            errors.append(f"Missing top-level key: {key}")

    identity = card.get("identity", {})
    for key in ("id", "name", "description"):
        if key not in identity:
            errors.append(f"Missing identity.{key}")

    routing = card.get("routing", {})
    if "routing_text" not in routing:
        errors.append("Missing routing.routing_text")

    return errors


def validate_card_schema(card: dict) -> list[str]:
    """Validate against router_card.schema.json if available."""
    schema_path = Path(_PROJECT_ROOT) / "skills" / "router_card.schema.json"
    if not schema_path.exists():
        logger.debug("Schema file not found at %s, skipping JSON-schema validation", schema_path)
        return validate_card(card)

    try:
        import jsonschema
    except ImportError:
        return validate_card(card)

    with open(schema_path, "r", encoding="utf-8") as f:
        schema = json.load(f)

    try:
        jsonschema.validate(card, schema)
    except jsonschema.ValidationError as e:
        return [str(e.message)]

    return []


# ---------------------------------------------------------------------------
# Registry operations
# ---------------------------------------------------------------------------

def load_registry(root: str) -> dict:
    """Load registry.json or return empty stub."""
    path = os.path.join(root, "skills", "registry.json")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "version": 1,
        "schema_version": SCHEMA_VERSION,
        "router_index": {
            "type": "elasticsearch",
            "url_env": "ES_URL",
            "username_env": "ES_USERNAME",
            "password_env": "ES_PASSWORD",
            "index_env": "SKILL_ROUTER_ES_INDEX",
            "default_url": "http://172.17.0.1:3128",
            "default_index": os.getenv("SKILL_ROUTER_ES_INDEX", "citybrain-skill-router-cards"),
            "embedding_model": "SkillRouter-Embedding-0.6B",
            "vector_field": "embedding_vector",
            "text_field": "routing_text",
            "id_field": "skill_id",
        },
        "skills": [],
    }


def save_registry(root: str, registry: dict) -> None:
    path = os.path.join(root, "skills", "registry.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(registry, f, indent=2, ensure_ascii=False)
    logger.info("Registry saved to %s", path)


def upsert_registry_entry(root: str, registry: dict, skill_id: str, card: dict) -> dict:
    """Insert or update a single skill entry in the registry."""
    es_index = os.getenv("SKILL_ROUTER_ES_INDEX", "citybrain-skill-router-cards")

    # Build relative paths from the repo root
    skill_dir_rel = card["source"]["skill_dir"]
    skill_md_path_rel = card["source"]["skill_md_path"]
    router_card_path_rel = str(Path(skill_dir_rel) / "router_card.json")

    entry = {
        "id": skill_id,
        "name": card["identity"]["name"],
        "scenes": card["scope"]["scenes"],
        "is_public": card["scope"]["is_public"],
        "task_types": card["scope"]["task_types"],
        "input_types": card["scope"]["input_types"],
        "router_card_path": router_card_path_rel,
        "skill_md_path": skill_md_path_rel,
        "enabled": True,
        "routing_text_hash": card["embedding"]["text_hash"],
        "es_index": es_index,
        "es_doc_id": skill_id,
        "es_indexed": False,
        "router_status": "pending_index",
        "last_indexed_at": None,
        "last_router_error": None,
    }

    skills = registry.get("skills", [])
    for i, existing in enumerate(skills):
        if existing["id"] == skill_id:
            # Merge new values while preserving fields not in entry
            existing.update(entry)
            skills[i] = existing
            break
    else:
        skills.append(entry)

    registry["skills"] = skills
    return registry


def update_registry_status(
    registry: dict,
    skill_id: str,
    *,
    success: bool = True,
    error_stage: str | None = None,
    error_message: str | None = None,
) -> None:
    """Set status fields for a skill in the registry."""
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    for skill in registry.get("skills", []):
        if skill["id"] == skill_id:
            if success:
                skill["enabled"] = True
                skill["router_status"] = "ready"
                skill["es_indexed"] = True
                skill["last_indexed_at"] = now_iso
                skill["last_router_error"] = None
            else:
                skill["enabled"] = False
                skill["router_status"] = "error"
                skill["es_indexed"] = False
                skill["last_indexed_at"] = None
                skill["last_router_error"] = {
                    "stage": error_stage or "unknown",
                    "message": error_message or "Unknown error",
                    "updated_at": now_iso,
                }
            break


# ---------------------------------------------------------------------------
# Main workflow
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Incremental update Router Card and ES index for a single Skill")
    parser.add_argument(
        "--skill",
        required=True,
        help="Relative skill path, e.g. custom/my-new-skill or public/data-analysis",
    )
    parser.add_argument(
        "--skills-root",
        default=str(Path(_PROJECT_ROOT) / "skills"),
        help="Root skills directory (default: repo root/skills)",
    )
    parser.add_argument(
        "--skip-es",
        action="store_true",
        help="Skip ES write (useful for local dry-run)",
    )
    parser.add_argument(
        "--skip-conflict-check",
        action="store_true",
        help="Skip conflict detection",
    )
    args = parser.parse_args()

    root = Path(args.skills_root).resolve()
    skill_path = Path(args.skill)

    # Resolve skill directory
    if skill_path.is_absolute():
        skill_dir = skill_path
        rel_skill = skill_path.relative_to(root)
    else:
        skill_dir = root / skill_path
        rel_skill = skill_path

    if not skill_dir.is_dir():
        logger.error("Skill directory %s does not exist", skill_dir)
        sys.exit(1)

    skill_md = skill_dir / "SKILL.md"
    if not skill_md.exists():
        logger.error("SKILL.md not found in %s", skill_dir)
        sys.exit(1)

    skill_id = skill_dir.name
    is_custom = "custom" in str(skill_dir)
    category = "custom" if is_custom else "public"

    logger.info("[%s] Processing skill: %s", category, skill_id)

    # ------------------------------------------------------------------
    # Stage 1: Read SKILL.md
    # ------------------------------------------------------------------
    raw_content = skill_md.read_text(encoding="utf-8")
    frontmatter, body = parse_frontmatter(raw_content)
    name = frontmatter.get("name", skill_id)
    description = frontmatter.get("description", "")
    skill_md_path_rel = str(rel_skill / "SKILL.md")

    logger.info("  Stage 1: Read SKILL.md (name=%s)", name)

    # ------------------------------------------------------------------
    # Stage 2: Resolve profile and build Router Card
    # ------------------------------------------------------------------
    profile = resolve_profile(skill_id, is_custom)
    es_index = os.getenv("SKILL_ROUTER_ES_INDEX", "citybrain-skill-router-cards")

    card = build_router_card(
        skill_id=skill_id,
        skill_name=name,
        skill_description=description,
        body_content=body,
        skill_dir=skill_dir,
        skill_md_path=skill_md_path_rel,
        profile=profile,
        es_index=es_index,
    )

    # Write router_card.json
    card_path = skill_dir / "router_card.json"
    card_path.write_text(
        json.dumps(card, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    logger.info("  Stage 2: Wrote Router Card -> %s", card_path)

    # ------------------------------------------------------------------
    # Stage 3: Schema validation
    # ------------------------------------------------------------------
    errors = validate_card_schema(card)
    if errors:
        logger.error("  Stage 3: Schema validation FAILED:")
        for e in errors:
            logger.error("    - %s", e)
        # Write error status to registry
        registry = load_registry(_PROJECT_ROOT)
        registry = upsert_registry_entry(_PROJECT_ROOT, registry, skill_id, card)
        update_registry_status(registry, skill_id, success=False, error_stage="schema_validation", error_message="; ".join(errors))
        save_registry(_PROJECT_ROOT, registry)
        sys.exit(1)

    logger.info("  Stage 3: Schema validation passed")

    # ------------------------------------------------------------------
    # Stage 4: Generate embedding
    # ------------------------------------------------------------------
    embedding_client = SkillRouterEmbeddingClient()
    routing_text = card["routing"]["routing_text"]

    try:
        embedding = embedding_client.embed_text(routing_text)
    except Exception as e:
        logger.error("  Stage 4: Embedding API failed: %s", e)
        registry = load_registry(_PROJECT_ROOT)
        registry = upsert_registry_entry(_PROJECT_ROOT, registry, skill_id, card)
        update_registry_status(registry, skill_id, success=False, error_stage="build_embedding", error_message=str(e))
        save_registry(_PROJECT_ROOT, registry)
        sys.exit(1)

    logger.info("  Stage 4: Generated embedding (dims=%d)", len(embedding))

    # ------------------------------------------------------------------
    # Stage 5: Upsert to ES
    # ------------------------------------------------------------------
    if not args.skip_es:
        es_store = SkillRouterElasticStore()

        # Ensure index exists with correct mapping
        mapping = {
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
                        "dims": len(embedding),
                        "index": True,
                        "similarity": "cosine",
                    },
                    "enabled": {"type": "boolean"},
                    "updated_at": {"type": "date"},
                }
            }
        }
        es_store.ensure_index_exists(mapping)

        # Build ES document
        identity = card["identity"]
        scope = card["scope"]
        source = card["source"]
        embedding_info = card.get("embedding", {})

        es_doc = {
            "skill_id": skill_id,
            "name": identity["name"],
            "description": identity["description"],
            "scenes": scope["scenes"],
            "is_public": scope["is_public"],
            "task_types": scope["task_types"],
            "input_types": scope["input_types"],
            "output_types": scope.get("output_types", []),
            "routing_text": routing_text,
            "body": card.get("body", {}).get("content", ""),
            "skill_dir": source.get("skill_dir", ""),
            "skill_md_path": source.get("skill_md_path", ""),
            "router_card_path": str(Path(source.get("skill_dir", "")) / "router_card.json"),
            "skill_md_hash": source.get("skill_md_hash", ""),
            "routing_text_hash": embedding_info.get("text_hash", "sha256:" + _sha256(routing_text)),
            "embedding_model": embedding_info.get("model", "SkillRouter-Embedding-0.6B"),
            "embedding_vector": embedding,
            "enabled": True,
            "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

        try:
            es_store.upsert_card(es_doc)
            logger.info("  Stage 5: Upserted ES document -> %s / %s", es_store.index, skill_id)
        except Exception as e:
            logger.error("  Stage 5: ES upsert failed: %s", e)
            registry = load_registry(_PROJECT_ROOT)
            registry = upsert_registry_entry(_PROJECT_ROOT, registry, skill_id, card)
            update_registry_status(registry, skill_id, success=False, error_stage="es_upsert", error_message=str(e))
            save_registry(_PROJECT_ROOT, registry)
            sys.exit(1)
    else:
        logger.info("  Stage 5: Skipped ES write (--skip-es)")

    # ------------------------------------------------------------------
    # Stage 6: Update registry
    # ------------------------------------------------------------------
    registry = load_registry(_PROJECT_ROOT)
    registry = upsert_registry_entry(_PROJECT_ROOT, registry, skill_id, card)

    # ------------------------------------------------------------------
    # Stage 7: Conflict detection
    # ------------------------------------------------------------------
    conflict_result = None
    if not args.skip_conflict_check:
        try:
            conflict_script = Path(_PROJECT_ROOT) / "scripts" / "check_skill_router_conflicts.py"
            if conflict_script.exists():
                import subprocess
                result = subprocess.run(
                    [sys.executable, str(conflict_script), "--skill", str(rel_skill), "--skills-root", str(root)],
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
                conflict_result = result.stdout.strip()
                if result.returncode != 0:
                    logger.warning("  Stage 7: Conflict check returned warnings:\n%s", conflict_result)
                    # Still mark as ready but note conflicts
                else:
                    logger.info("  Stage 7: Conflict check passed\n%s", conflict_result)
        except subprocess.TimeoutExpired:
            logger.warning("  Stage 7: Conflict check timed out, skipping")
        except Exception as e:
            logger.warning("  Stage 7: Conflict check error: %s", e)

    # ------------------------------------------------------------------
    # Stage 8: Finalize status
    # ------------------------------------------------------------------
    update_registry_status(registry, skill_id, success=True)
    save_registry(_PROJECT_ROOT, registry)

    logger.info("  Stage 8: Skill %s -> router_status=ready", skill_id)
    logger.info("Done. Skill '%s' is now routable.", skill_id)


if __name__ == "__main__":
    main()
