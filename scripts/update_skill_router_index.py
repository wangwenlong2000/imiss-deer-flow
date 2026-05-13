#!/usr/bin/env python3
"""Incremental update of Router Card and ES index for a single Skill.

Thin CLI wrapper around ``deerflow.routing.index_updater.update_single_skill_index``.

Usage:
    python scripts/update_skill_router_index.py --skill custom/my-new-skill
    python scripts/update_skill_router_index.py --skill public/data-analysis
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_PROJECT_ROOT, "backend", "packages", "harness"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


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
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output result as JSON",
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

    skill_id = skill_dir.name

    if args.skip_es:
        # Dry-run mode: validate and build card only, skip ES
        from deerflow.routing.index_updater import build_router_card_for_skill, validate_skill_dir

        is_valid, err = validate_skill_dir(skill_dir)
        if not is_valid:
            logger.error("Validation failed: %s", err)
            sys.exit(1)

        card, build_err = build_router_card_for_skill(skill_dir, root)
        if card is None:
            logger.error("Card build failed: %s", build_err)
            sys.exit(1)

        # Write card to disk
        card_path = skill_dir / "router_card.json"
        card_path.write_text(
            json.dumps(card, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        logger.info("Router Card written to %s (ES skipped)", card_path)
        return

    from deerflow.routing.index_updater import update_single_skill_index

    result = update_single_skill_index(skill_id=skill_id, skill_dir=skill_dir, skills_root=root)

    if args.json:
        print(json.dumps({
            "skill_id": result.skill_id,
            "success": result.success,
            "router_indexed": result.router_indexed,
            "router_status": result.router_status,
            "already_up_to_date": result.already_up_to_date,
            "router_error": result.router_error,
        }, indent=2, ensure_ascii=False))
    else:
        logger.info("Result: skill_id=%s status=%s indexed=%s",
                     result.skill_id, result.router_status, result.router_indexed)

    if result.router_error:
        logger.warning("Error: %s", result.router_error)

    if result.router_status in ("invalid_card", "index_failed", "error"):
        sys.exit(1)

    # ------------------------------------------------------------------
    # Optional conflict detection
    # ------------------------------------------------------------------
    if not args.skip_conflict_check:
        try:
            conflict_script = Path(_PROJECT_ROOT) / "scripts" / "check_skill_router_conflicts.py"
            if conflict_script.exists():
                import subprocess
                conflict_result = subprocess.run(
                    [sys.executable, str(conflict_script), "--skill", str(rel_skill), "--skills-root", str(root)],
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
                if conflict_result.stdout.strip():
                    print(conflict_result.stdout.strip())
                if conflict_result.returncode != 0:
                    logger.warning("Conflict check returned warnings")
        except subprocess.TimeoutExpired:
            logger.warning("Conflict check timed out, skipping")
        except Exception as e:
            logger.warning("Conflict check error: %s", e)


if __name__ == "__main__":
    main()
