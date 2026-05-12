#!/usr/bin/env python3
"""Build skills/registry.json from existing Router Cards.

Scans all router_card.json files and generates a unified registry.

Usage:
    python scripts/build_skill_router_registry.py [--skills-root PATH]
"""

import argparse
import json
import os
import sys
from pathlib import Path


REGISTRY_VERSION = 1
SCHEMA_VERSION = "1.0.0"


def main():
    parser = argparse.ArgumentParser(description="Build skill_router registry.json")
    parser.add_argument(
        "--skills-root",
        default=str(Path(__file__).resolve().parent.parent / "skills"),
        help="Root directory containing skills/ (default: repo root/skills)",
    )
    args = parser.parse_args()

    skills_root = Path(args.skills_root).resolve()
    registry_output = skills_root / "registry.json"

    if not skills_root.is_dir():
        print(f"ERROR: {skills_root} does not exist", file=sys.stderr)
        sys.exit(1)

    es_index = os.environ.get("SKILL_ROUTER_ES_INDEX", "citybrain-skill-router-cards")

    skills_list = []
    for card_file in sorted(skills_root.rglob("router_card.json")):
        try:
            with open(card_file, encoding="utf-8") as f:
                card = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            print(f"WARN: Skipping invalid {card_file}: {e}", file=sys.stderr)
            continue

        identity = card.get("identity", {})
        scope = card.get("scope", {})
        source = card.get("source", {})
        embedding = card.get("embedding", {})

        skill_id = identity.get("id", card_file.parent.name)
        skill_name = identity.get("name", skill_id)
        skill_md_path = source.get("skill_md_path", "")
        routing_text_hash = embedding.get("text_hash", "")
        es_doc_id = embedding.get("es_doc_id", skill_id)

        # Compute relative paths from skills root
        rel_card = str(card_file.relative_to(skills_root.parent))
        rel_md = skill_md_path if skill_md_path else rel_card.replace("router_card.json", "SKILL.md")

        skills_list.append({
            "id": skill_id,
            "name": skill_name,
            "scenes": scope.get("scenes", []),
            "is_public": scope.get("is_public", False),
            "task_types": scope.get("task_types", []),
            "input_types": scope.get("input_types", []),
            "router_card_path": rel_card,
            "skill_md_path": rel_md,
            "enabled": True,
            "routing_text_hash": routing_text_hash,
            "es_index": es_index,
            "es_doc_id": es_doc_id,
        })

    # Read router_index config from existing registry if present, else build default
    registry = {
        "version": REGISTRY_VERSION,
        "schema_version": SCHEMA_VERSION,
        "router_index": {
            "type": "elasticsearch",
            "url_env": "ES_URL",
            "username_env": "ES_USERNAME",
            "password_env": "ES_PASSWORD",
            "index_env": "SKILL_ROUTER_ES_INDEX",
            "default_url": "http://172.17.0.1:3128",
            "default_index": es_index,
            "embedding_model": "SkillRouter-Embedding-0.6B",
            "vector_field": "embedding_vector",
            "text_field": "routing_text",
            "id_field": "skill_id",
        },
        "skills": skills_list,
    }

    registry_output.write_text(
        json.dumps(registry, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(f"Registry written to {registry_output}")
    print(f"Total skills: {len(skills_list)}")
    for s in skills_list:
        print(f"  - {s['id']} (enabled={s['enabled']}, es_index={s['es_index']})")


if __name__ == "__main__":
    main()
