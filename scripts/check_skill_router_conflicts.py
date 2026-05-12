#!/usr/bin/env python3
"""Detect routing conflicts between a target Skill and existing Skills.

Compares routing fields, set overlaps, and semantic similarity to flag
potential collisions that may require Router Card boundary adjustments.

Usage:
    python scripts/check_skill_router_conflicts.py --skill custom/my-skill
    python scripts/check_skill_router_conflicts.py --all   # check all skills
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_PROJECT_ROOT, "backend", "packages", "harness"))

try:
    from deerflow.routing.embedding_client import SkillRouterEmbeddingClient
    _EMBEDDING_AVAILABLE = True
except ImportError:
    _EMBEDDING_AVAILABLE = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------

OVERLAP_READY = 0.70
OVERLAP_REVIEW = 0.85


def jaccard(a, b):
    """Return Jaccard similarity between two iterables of hashable items."""
    sa, sb = set(a), set(b)
    if not sa and not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


def cosine_sim(a, b):
    """Cosine similarity between two float vectors."""
    if not a or not b:
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = sum(x * x for x in a) ** 0.5
    norm_b = sum(x * x for x in b) ** 0.5
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def load_card(path: Path) -> dict | None:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        logger.warning("Skipping invalid card %s: %s", path, e)
        return None


def load_all_cards(skills_root: Path) -> dict[str, dict]:
    """Return {skill_id: card} for every skill that has a router_card.json."""
    cards: dict[str, dict] = {}
    for category in ("custom", "public"):
        cat_dir = skills_root / category
        if not cat_dir.is_dir():
            continue
        for d in sorted(cat_dir.iterdir()):
            card_path = d / "router_card.json"
            if card_path.exists():
                card = load_card(card_path)
                if card:
                    cards[card["identity"]["id"]] = card
    return cards


# ---------------------------------------------------------------------------
# Conflict detection
# ---------------------------------------------------------------------------

def check_single_conflict(target: dict, other: dict, embedding_client=None) -> dict:
    """Compare target skill against one existing skill. Return conflict record."""
    t_scope = target.get("scope", {})
    o_scope = other.get("scope", {})
    t_routing = target.get("routing", {})
    o_routing = other.get("routing", {})
    t_exec = target.get("execution", {})
    o_exec = other.get("execution", {})

    # Set-based overlaps
    scene_ov = jaccard(t_scope.get("scenes", []), o_scope.get("scenes", []))
    task_ov = jaccard(t_scope.get("task_types", []), o_scope.get("task_types", []))
    input_ov = jaccard(t_scope.get("input_types", []), o_scope.get("input_types", []))
    output_ov = jaccard(t_scope.get("output_types", []), o_scope.get("output_types", []))
    tools_ov = jaccard(t_exec.get("required_tools", []), o_exec.get("required_tools", []))

    # Trigger overlap
    pos_ov = jaccard(t_routing.get("positive_triggers", []), o_routing.get("positive_triggers", []))

    # Negative triggers conflict: does the target lack boundaries the other has?
    neg_conflict = False
    t_neg = set(t_routing.get("negative_triggers", []))
    o_neg = set(o_routing.get("negative_triggers", []))
    # If two skills share many positive triggers but target's negative_triggers
    # don't mention the other's positive territory, that's a gap.
    shared_pos = set(t_routing.get("positive_triggers", [])) & set(o_routing.get("positive_triggers", []))
    if shared_pos and len(t_neg) == 0:
        neg_conflict = True

    # Semantic similarity of routing_text
    routing_sim = 0.0
    if embedding_client:
        try:
            vectors = embedding_client.embed_texts([
                t_routing.get("routing_text", ""),
                o_routing.get("routing_text", ""),
            ])
            routing_sim = cosine_sim(vectors[0], vectors[1])
        except Exception:
            pass

    # Composite overlap score: weighted combination
    overlap_score = (
        0.20 * scene_ov
        + 0.25 * task_ov
        + 0.10 * input_ov
        + 0.10 * output_ov
        + 0.15 * pos_ov
        + 0.05 * tools_ov
        + 0.15 * routing_sim
    )
    # Normalize to 0-1 range (max raw is 1.0)

    # Determine status
    if overlap_score >= OVERLAP_REVIEW:
        status = "conflict"
    elif overlap_score >= OVERLAP_READY:
        status = "pending_review"
    else:
        status = "ready"

    overlap_dimensions = []
    if scene_ov > 0:
        overlap_dimensions.append("scenes")
    if task_ov > 0:
        overlap_dimensions.append("task_types")
    if input_ov > 0:
        overlap_dimensions.append("input_types")
    if output_ov > 0:
        overlap_dimensions.append("output_types")
    if pos_ov > 0:
        overlap_dimensions.append("positive_triggers")
    if tools_ov > 0:
        overlap_dimensions.append("required_tools")
    if routing_sim > 0.3:
        overlap_dimensions.append("routing_text_similarity")

    suggestion = _build_suggestion(
        target, other, overlap_dimensions, neg_conflict, status,
    )

    return {
        "existing_skill_id": other["identity"]["id"],
        "overlap_score": round(overlap_score, 4),
        "overlap_dimensions": overlap_dimensions,
        "detail": {
            "scene_jaccard": round(scene_ov, 4),
            "task_types_jaccard": round(task_ov, 4),
            "input_types_jaccard": round(input_ov, 4),
            "output_types_jaccard": round(output_ov, 4),
            "positive_triggers_jaccard": round(pos_ov, 4),
            "required_tools_jaccard": round(tools_ov, 4),
            "routing_text_cosine": round(routing_sim, 4),
            "negative_triggers_gap": neg_conflict,
        },
        "suggestion": suggestion,
        "status": status,
    }


def _build_suggestion(target, other, dims, neg_conflict, status):
    tid = target["identity"]["id"]
    oid = other["identity"]["id"]
    tid_name = target["identity"]["name"]
    oid_name = other["identity"]["name"]

    if status == "ready":
        return f"{tid} 与 {oid} 边界清晰，无需调整。"

    parts = [f"{tid} ({tid_name}) 与 {oid} ({oid_name}) 存在重叠"]
    if "scenes" in dims:
        parts.append("两者属于相同或相近场景")
    if "task_types" in dims:
        parts.append("两者处理相同任务类型")
    if "input_types" in dims:
        parts.append("两者接收相同输入类型")
    if "output_types" in dims:
        parts.append("两者输出类型重叠")
    if "positive_triggers" in dims:
        parts.append("两者正例触发描述相似")
    if "routing_text_similarity" in dims:
        parts.append("routing_text 语义相似度高")
    if neg_conflict:
        parts.append(f"建议在 {tid} 的 negative_triggers 中明确排除 {oid} 适用的任务")

    if status == "conflict":
        parts.append("重叠度过高，默认不启用，需要修改 Router Card 边界后再试")
    elif status == "pending_review":
        parts.append("进入 pending_review，请人工确认边界是否合理")

    return "；".join(parts) + "。"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Detect routing conflicts for a Skill")
    parser.add_argument("--skill", help="Relative skill path, e.g. custom/my-skill")
    parser.add_argument(
        "--all",
        action="store_true",
        help="Check all skills against each other",
    )
    parser.add_argument(
        "--skills-root",
        default=str(Path(_PROJECT_ROOT) / "skills"),
        help="Root skills directory",
    )
    parser.add_argument(
        "--no-embedding",
        action="store_true",
        help="Disable semantic similarity (set-based only)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output as JSON instead of human-readable text",
    )
    args = parser.parse_args()

    skills_root = Path(args.skills_root).resolve()
    all_cards = load_all_cards(skills_root)

    if not all_cards:
        logger.error("No router_card.json files found under %s", skills_root)
        sys.exit(1)

    embedding_client = None
    if not args.no_embedding and _EMBEDDING_AVAILABLE:
        try:
            embedding_client = SkillRouterEmbeddingClient()
        except Exception:
            pass

    conflicts_report = {"new_skill_id": None, "conflicts": []}

    if args.all:
        # Pairwise check: every card against every other
        skill_ids = sorted(all_cards.keys())
        for i, sid_a in enumerate(skill_ids):
            for sid_b in skill_ids[i + 1:]:
                rec = check_single_conflict(all_cards[sid_a], all_cards[sid_b], embedding_client)
                if rec["status"] != "ready":
                    conflicts_report["conflicts"].append({
                        "skill_a": sid_a,
                        "skill_b": sid_b,
                        **rec,
                    })
    elif args.skill:
        target_dir = skills_root / args.skill
        if not target_dir.is_dir():
            logger.error("Skill directory %s does not exist", target_dir)
            sys.exit(1)

        card_path = target_dir / "router_card.json"
        if not card_path.exists():
            logger.error("router_card.json not found in %s", target_dir)
            sys.exit(1)

        target = load_card(card_path)
        if not target:
            sys.exit(1)

        target_id = target["identity"]["id"]
        conflicts_report["new_skill_id"] = target_id

        for other_id, other_card in sorted(all_cards.items()):
            if other_id == target_id:
                continue
            rec = check_single_conflict(target, other_card, embedding_client)
            conflicts_report["conflicts"].append(rec)

    # ------------------------------------------------------------------
    # Output
    # ------------------------------------------------------------------
    if args.json:
        print(json.dumps(conflicts_report, indent=2, ensure_ascii=False))
        return

    # Human-readable
    has_conflicts = False
    for c in conflicts_report["conflicts"]:
        if c.get("status", "ready") != "ready":
            has_conflicts = True
            status_label = {"pending_review": "PENDING_REVIEW", "conflict": "CONFLICT"}.get(c["status"], c["status"])
            print(f"\n[{status_label}] vs {c.get('existing_skill_id', c.get('skill_b', ''))}")
            print(f"  overlap_score: {c['overlap_score']}")
            print(f"  dimensions:    {', '.join(c['overlap_dimensions'])}")
            print(f"  suggestion:    {c['suggestion']}")

    if not has_conflicts:
        target_id = conflicts_report["new_skill_id"] or "all skills"
        print(f"No routing conflicts detected for {target_id}. Status: ready")
    else:
        # Exit 1 if any hard conflicts exist
        hard = any(c["status"] == "conflict" for c in conflicts_report["conflicts"])
        sys.exit(1 if hard else 0)


if __name__ == "__main__":
    main()
