"""Incremental SkillRouter index updater.

Provides the orchestrator ``update_single_skill_index()`` that can be called
from both gateway (app) and client (harness) without circular imports.
Follows the three-layer design: validate -> build -> upsert.

Usage::

    from deerflow.routing.index_updater import update_single_skill_index

    result = update_single_skill_index(skill_id="my-skill")
    print(result.router_status)  # "ready" | "already_up_to_date" | "invalid_card" | "index_failed"
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent.parent


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------


@dataclass
class IndexUpdateResult:
    """Result of a single-skill index update."""

    skill_id: str
    success: bool
    router_indexed: bool
    router_status: Literal["ready", "invalid_card", "index_failed", "already_up_to_date", "error"]
    already_up_to_date: bool = False
    router_error: str | None = None
    skill_hash: str | None = None


# ---------------------------------------------------------------------------
# Registry helpers (mirrors update_skill_router_index.py)
# ---------------------------------------------------------------------------


def _load_registry(root: Path) -> dict:
    """Load registry.json or return empty stub."""
    path = root / "skills" / "registry.json"
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "version": 1,
        "schema_version": "1.0.0",
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


def _save_registry(root: Path, registry: dict) -> None:
    path = root / "skills" / "registry.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(registry, f, indent=2, ensure_ascii=False)
    logger.info("Registry saved to %s", path)


def _update_registry_status(
    registry: dict,
    skill_id: str,
    *,
    success: bool = True,
    error_stage: str | None = None,
    error_message: str | None = None,
    skill_hash: str | None = None,
) -> None:
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    for skill in registry.get("skills", []):
        if skill["id"] == skill_id:
            if success:
                skill["enabled"] = True
                skill["router_status"] = "ready"
                skill["es_indexed"] = True
                skill["last_indexed_at"] = now_iso
                skill["last_router_error"] = None
                if skill_hash:
                    skill["skill_hash"] = skill_hash
            else:
                skill["enabled"] = False
                skill["router_status"] = error_stage or "error"
                skill["es_indexed"] = False
                skill["last_indexed_at"] = None
                skill["last_router_error"] = {
                    "stage": error_stage or "unknown",
                    "message": error_message or "Unknown error",
                    "updated_at": now_iso,
                }
            return

    # Skill not found in registry — add a new entry
    entry = {
        "id": skill_id,
        "name": skill_id,
        "scenes": [],
        "is_public": False,
        "task_types": [],
        "input_types": [],
        "router_card_path": "",
        "skill_md_path": "",
        "enabled": success,
        "router_status": "ready" if success else (error_stage or "error"),
        "es_indexed": success,
        "last_indexed_at": now_iso if success else None,
        "last_router_error": None if success else {
            "stage": error_stage or "unknown",
            "message": error_message or "Unknown error",
            "updated_at": now_iso,
        },
    }
    if skill_hash:
        entry["skill_hash"] = skill_hash
    registry.setdefault("skills", []).append(entry)


# ---------------------------------------------------------------------------
# Locking
# ---------------------------------------------------------------------------


def _acquire_lock(skill_id: str, lock_dir: Path) -> tuple[int, Path]:
    """Acquire exclusive file lock for *skill_id*. Returns (fd, lock_path)."""
    lock_dir.mkdir(parents=True, exist_ok=True)
    lock_path = lock_dir / f"{skill_id}.lock"
    fd = os.open(str(lock_path), os.O_CREAT | os.O_WRONLY)
    fcntl.flock(fd, fcntl.LOCK_EX)
    return fd, lock_path


def _release_lock(fd: int, lock_path: Path) -> None:
    fcntl.flock(fd, fcntl.LOCK_UN)
    os.close(fd)


# ---------------------------------------------------------------------------
# Layer 1: Validate
# ---------------------------------------------------------------------------


def validate_skill_dir(skill_dir: Path) -> tuple[bool, str | None]:
    """Check SKILL.md exists and directory structure is valid.

    Returns (is_valid, error_message).
    """
    if not skill_dir.is_dir():
        return False, f"Skill directory does not exist: {skill_dir}"

    skill_md = skill_dir / "SKILL.md"
    if not skill_md.exists():
        return False, f"SKILL.md not found in {skill_dir}"

    # tool_manifest.json is optional
    manifest = skill_dir / "tool_manifest.json"
    if manifest.exists():
        try:
            with open(manifest, "r", encoding="utf-8") as f:
                json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            return False, f"Invalid tool_manifest.json: {e}"

    return True, None


# ---------------------------------------------------------------------------
# Hash computation
# ---------------------------------------------------------------------------


def compute_skill_hash(skill_dir: Path) -> str:
    """Compute SHA-256 of SKILL.md + router_card.json + tool_manifest.json.

    Missing files are silently skipped.
    """
    h = hashlib.sha256()
    for name in ("SKILL.md", "router_card.json", "tool_manifest.json"):
        p = skill_dir / name
        if p.exists():
            h.update(p.read_bytes())
    return h.hexdigest()


# ---------------------------------------------------------------------------
# Layer 2: Build router card
# ---------------------------------------------------------------------------


def build_router_card_for_skill(
    skill_dir: Path,
    skills_root: Path,
) -> tuple[dict | None, str | None]:
    """Generate a Router Card dict from source files.

    Reads SKILL.md, resolves profile, assembles card.  Does NOT write to disk.

    Returns (card_dict, error_message).
    """
    # Lazy import to avoid circular deps at module load
    from extract_router_cards import (
        CUSTOM_SKILL_PROFILES,
        DEFAULT_PUBLIC_PROFILE,
        PUBLIC_SKILL_DEFAULTS,
        build_router_card,
        parse_frontmatter,
    )

    skill_md = skill_dir / "SKILL.md"
    try:
        raw_content = skill_md.read_text(encoding="utf-8")
    except OSError as e:
        return None, f"Cannot read SKILL.md: {e}"

    frontmatter, body = parse_frontmatter(raw_content)
    skill_id = skill_dir.name
    name = frontmatter.get("name", skill_id)
    description = frontmatter.get("description", "")

    is_custom = "custom" in str(skill_dir.relative_to(skills_root))
    category = "custom" if is_custom else "public"

    # Profile resolution
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
    else:
        profile = PUBLIC_SKILL_DEFAULTS.get(skill_id, dict(DEFAULT_PUBLIC_PROFILE))

    skill_md_path_rel = str(skill_dir.relative_to(skills_root.parent)) + "/SKILL.md"
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

    return card, None


# ---------------------------------------------------------------------------
# Layer 3: Upsert to ES
# ---------------------------------------------------------------------------


def upsert_router_card_to_es(
    card: dict,
    skill_hash: str,
) -> tuple[bool, str | None]:
    """Generate embedding, write Router Card to Elasticsearch.

    Returns (success, error_message).
    """
    from deerflow.routing.embedding_client import SkillRouterEmbeddingClient
    from deerflow.routing.es_store import SkillRouterElasticStore

    es_store = SkillRouterElasticStore()
    embedding_client = SkillRouterEmbeddingClient()

    # Ensure index exists
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
                    "dims": 0,  # will be set by ensure_index
                    "index": True,
                    "similarity": "cosine",
                },
                "enabled": {"type": "boolean"},
                "updated_at": {"type": "date"},
                "skill_hash": {"type": "keyword"},
            }
        }
    }
    es_store.ensure_index_exists(mapping)

    # Generate embedding
    routing_text = card.get("routing", {}).get("routing_text", "")
    try:
        embedding = embedding_client.embed_text(routing_text)
    except Exception as e:
        return False, f"Embedding API failed: {e}"

    # Build ES document
    identity = card.get("identity", {})
    scope = card.get("scope", {})
    source = card.get("source", {})
    embedding_info = card.get("embedding", {})

    es_doc = {
        "skill_id": identity.get("id", ""),
        "name": identity.get("name", ""),
        "description": identity.get("description", ""),
        "scenes": scope.get("scenes", []),
        "is_public": scope.get("is_public", False),
        "task_types": scope.get("task_types", []),
        "input_types": scope.get("input_types", []),
        "output_types": scope.get("output_types", []),
        "routing_text": routing_text,
        "body": card.get("body", {}).get("content", ""),
        "skill_dir": source.get("skill_dir", ""),
        "skill_md_path": source.get("skill_md_path", ""),
        "router_card_path": str(Path(source.get("skill_dir", "")) / "router_card.json"),
        "skill_md_hash": source.get("skill_md_hash", ""),
        "routing_text_hash": embedding_info.get("text_hash", ""),
        "embedding_model": embedding_info.get("model", "SkillRouter-Embedding-0.6B"),
        "embedding_vector": embedding,
        "enabled": True,
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "skill_hash": skill_hash,
    }

    # Update embedding_vector dims in mapping if needed
    mapping["mappings"]["properties"]["embedding_vector"]["dims"] = len(embedding)
    es_store.ensure_index_exists(mapping)

    try:
        es_store.upsert_card(es_doc)
    except Exception as e:
        return False, f"ES upsert failed: {e}"

    return True, None


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------


def _validate_card_schema(card: dict) -> list[str]:
    """Lightweight schema validation. Uses jsonschema if available."""
    errors: list[str] = []
    required_top = [
        "schema_version", "identity", "scope", "routing",
        "body", "execution", "routing_policy", "source", "embedding",
    ]
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

    if errors:
        return errors

    # Try jsonschema if available
    schema_path = _PROJECT_ROOT / "skills" / "router_card.schema.json"
    if not schema_path.exists():
        return []

    try:
        import jsonschema
    except ImportError:
        return []

    try:
        with open(schema_path, "r", encoding="utf-8") as f:
            schema = json.load(f)
        jsonschema.validate(card, schema)
    except (jsonschema.ValidationError, json.JSONDecodeError) as e:
        errors.append(str(e))

    return errors


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def update_single_skill_index(
    skill_id: str,
    skill_dir: Path | None = None,
    skills_root: Path | None = None,
) -> IndexUpdateResult:
    """Orchestrator: lock -> validate -> hash -> build -> embed -> upsert -> registry.

    If *skill_dir* is None, resolves from *skills_root* + skill_id.
    If *skills_root* is None, uses the project's ``skills/`` directory.

    Returns an ``IndexUpdateResult`` with status and error details.
    """
    if skills_root is None:
        skills_root = _PROJECT_ROOT / "skills"
    else:
        skills_root = Path(skills_root).resolve()

    if skill_dir is None:
        # Try to find skill_dir under skills_root
        for category in ("custom", "public"):
            candidate = skills_root / category / skill_id
            if candidate.is_dir():
                skill_dir = candidate
                break

    if skill_dir is None:
        return IndexUpdateResult(
            skill_id=skill_id,
            success=False,
            router_indexed=False,
            router_status="error",
            router_error=f"Skill directory not found for {skill_id}",
        )

    skill_dir = Path(skill_dir).resolve()
    lock_dir = skills_root / ".locks"

    # Step 1: Acquire lock
    fd, lock_path = _acquire_lock(skill_id, lock_dir)
    try:
        return _update_locked(skill_id, skill_dir, skills_root)
    finally:
        _release_lock(fd, lock_path)


def _update_locked(
    skill_id: str,
    skill_dir: Path,
    skills_root: Path,
) -> IndexUpdateResult:
    """Core update logic, must be called with lock held."""

    # Step 2: Validate
    is_valid, err = validate_skill_dir(skill_dir)
    if not is_valid:
        return IndexUpdateResult(
            skill_id=skill_id,
            success=True,
            router_indexed=False,
            router_status="invalid_card",
            router_error=err,
        )

    # Step 3: Compute hash and check idempotency
    skill_hash = compute_skill_hash(skill_dir)

    # Check registry for existing hash + ES doc
    registry = _load_registry(skills_root.parent)
    for skill_entry in registry.get("skills", []):
        if skill_entry.get("id") == skill_id:
            if (
                skill_entry.get("skill_hash") == skill_hash
                and skill_entry.get("es_indexed")
                and skill_entry.get("router_status") == "ready"
            ):
                return IndexUpdateResult(
                    skill_id=skill_id,
                    success=True,
                    router_indexed=True,
                    router_status="already_up_to_date",
                    already_up_to_date=True,
                    skill_hash=skill_hash,
                )
            break

    # Step 4: Build router card
    card, build_err = build_router_card_for_skill(skill_dir, skills_root)
    if card is None:
        _update_registry_status(
            registry, skill_id, success=False,
            error_stage="invalid_card", error_message=build_err,
            skill_hash=skill_hash,
        )
        _save_registry(skills_root.parent, registry)
        return IndexUpdateResult(
            skill_id=skill_id,
            success=True,
            router_indexed=False,
            router_status="invalid_card",
            router_error=build_err,
            skill_hash=skill_hash,
        )

    # Write router_card.json to disk
    card_path = skill_dir / "router_card.json"
    try:
        card_path.write_text(
            json.dumps(card, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
    except OSError as e:
        return IndexUpdateResult(
            skill_id=skill_id,
            success=False,
            router_indexed=False,
            router_status="error",
            router_error=f"Cannot write router_card.json: {e}",
        )

    # Step 5: Schema validation
    schema_errors = _validate_card_schema(card)
    if schema_errors:
        err_msg = "; ".join(schema_errors)
        logger.error("Schema validation failed for %s: %s", skill_id, err_msg)
        _update_registry_status(
            registry, skill_id, success=False,
            error_stage="schema_validation", error_message=err_msg,
            skill_hash=skill_hash,
        )
        _save_registry(skills_root.parent, registry)
        return IndexUpdateResult(
            skill_id=skill_id,
            success=True,
            router_indexed=False,
            router_status="invalid_card",
            router_error=err_msg,
            skill_hash=skill_hash,
        )

    # Step 6: Upsert to ES
    es_ok, es_err = upsert_router_card_to_es(card, skill_hash)
    if not es_ok:
        _update_registry_status(
            registry, skill_id, success=False,
            error_stage="index_failed", error_message=es_err,
            skill_hash=skill_hash,
        )
        _save_registry(skills_root.parent, registry)
        return IndexUpdateResult(
            skill_id=skill_id,
            success=True,
            router_indexed=False,
            router_status="index_failed",
            router_error=es_err,
            skill_hash=skill_hash,
        )

    # Step 7: Update registry
    _update_registry_status(
        registry, skill_id, success=True, skill_hash=skill_hash,
    )
    _save_registry(skills_root.parent, registry)

    logger.info("SkillRouter index updated for %s -> ready", skill_id)
    return IndexUpdateResult(
        skill_id=skill_id,
        success=True,
        router_indexed=True,
        router_status="ready",
        skill_hash=skill_hash,
    )
