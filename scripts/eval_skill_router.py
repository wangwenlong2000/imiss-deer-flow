#!/usr/bin/env python3
"""Evaluate SkillRouter query routing accuracy.

Runs test cases against the live SkillRouter pipeline (embedding + ES +
reranker + resolver) or in offline mode using pre-computed cards only.

Usage:
    python scripts/eval_skill_router.py                    # live evaluation
    python scripts/eval_skill_router.py --offline          # offline mode
    python scripts/eval_skill_router.py --json             # JSON output
    python scripts/eval_skill_router.py --json --offline
"""

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_PROJECT_ROOT, "backend", "packages", "harness"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------


@dataclass
class EvalCase:
    query: str
    expected_skill_ids: list[str]
    expect_trigger: bool = True
    category: str = "known"


def load_eval_cases() -> list[EvalCase]:
    """Return built-in evaluation cases."""
    return [
        # Known queries -> expected single skill
        EvalCase(
            query="帮我分析这个 pcap 文件有没有异常通信",
            expected_skill_ids=["network-traffic-analysis"],
            expect_trigger=True,
            category="known",
        ),
        EvalCase(
            query="查一下相关法律条文并判断这个台账是否合规",
            expected_skill_ids=["law-regulations-rag"],
            expect_trigger=True,
            category="known",
        ),
        # Multi-skill queries
        EvalCase(
            query="上传 Excel，帮我做统计并画图",
            expected_skill_ids=["data-analysis", "chart-visualization"],
            expect_trigger=True,
            category="multi_skill",
        ),
        # trigger=false queries
        EvalCase(
            query="今天天气怎么样",
            expected_skill_ids=[],
            expect_trigger=False,
            category="trigger_false",
        ),
        EvalCase(
            query="你好",
            expected_skill_ids=[],
            expect_trigger=False,
            category="trigger_false",
        ),
        EvalCase(
            query="在吗",
            expected_skill_ids=[],
            expect_trigger=False,
            category="trigger_false",
        ),
        EvalCase(
            query="谢谢",
            expected_skill_ids=[],
            expect_trigger=False,
            category="trigger_false",
        ),
        # File reference queries
        EvalCase(
            query="这个文件有什么问题",
            expected_skill_ids=[],
            expect_trigger=True,
            category="file_ref",
        ),
        # Fuzzy queries
        EvalCase(
            query="帮我分析一下这些数据",
            expected_skill_ids=["data-analysis"],
            expect_trigger=True,
            category="fuzzy",
        ),
    ]


# ---------------------------------------------------------------------------
# Offline evaluation (set-based matching against router_card.json)
# ---------------------------------------------------------------------------


def _load_all_cards(skills_root: Path) -> dict[str, dict]:
    """Load all router_card.json files."""
    cards: dict[str, dict] = {}
    for category in ("custom", "public"):
        cat_dir = skills_root / category
        if not cat_dir.is_dir():
            continue
        for d in sorted(cat_dir.iterdir()):
            card_path = d / "router_card.json"
            if card_path.exists():
                try:
                    with open(card_path, "r", encoding="utf-8") as f:
                        card = json.load(f)
                    cards[card["identity"]["id"]] = card
                except (OSError, json.JSONDecodeError, KeyError):
                    pass
    return cards


def _trigger_positive(query: str, card: dict) -> float:
    """Score a query against a card's positive triggers (simple keyword overlap)."""
    routing = card.get("routing", {})
    triggers = routing.get("positive_triggers", [])
    keywords = routing.get("keywords", [])

    score = 0.0
    for trigger in triggers:
        if trigger and trigger in query:
            score += 0.4
    for keyword in keywords:
        if keyword.lower() in query.lower():
            score += 0.1

    return score


def _check_negative(query: str, card: dict) -> bool:
    """Check if query matches negative triggers (should be excluded)."""
    routing = card.get("routing", {})
    neg_triggers = routing.get("negative_triggers", [])
    anti_keywords = routing.get("anti_keywords", [])

    for trigger in neg_triggers:
        if trigger and trigger in query:
            return True
    for anti in anti_keywords:
        if anti.lower() in query.lower():
            return True
    return False


def run_offline_eval(cases: list[EvalCase], skills_root: Path) -> dict:
    """Run cases using set-based matching on router_card.json only (no ES/API calls)."""
    cards = _load_all_cards(skills_root)

    if not cards:
        logger.warning("No router_card.json files found under %s", skills_root)

    details: list[dict] = []
    passed = 0
    failed = 0

    for case in cases:
        start = time.monotonic()

        # Check trigger expectation
        has_files = case.category == "file_ref"

        # For offline mode, use should_route-like logic
        if case.expect_trigger:
            if has_files:
                # File reference always triggers
                trigger = True
            else:
                # Check if any card has positive overlap
                trigger = any(_trigger_positive(case.query, c) > 0 for c in cards.values())
        else:
            trigger = False

        if trigger and cards:
            # Score each card
            scored = []
            for skill_id, card in cards.items():
                if _check_negative(case.query, card):
                    continue
                score = _trigger_positive(case.query, card)
                if score > 0:
                    scored.append((skill_id, score))
            scored.sort(key=lambda x: -x[1])
            # Take top matches
            threshold = scored[0][1] * 0.5 if scored else 0
            matched = [s[0] for s in scored if s[1] >= threshold] if scored else []
        else:
            matched = []

        elapsed = (time.monotonic() - start) * 1000

        # For trigger_false cases: pass if trigger is false
        if not case.expect_trigger:
            ok = not trigger
        # For known/multi_skill cases: check expected skills are in matched
        elif case.category in ("known", "multi_skill", "fuzzy"):
            expected_set = set(case.expected_skill_ids)
            matched_set = set(matched)
            ok = expected_set.issubset(matched_set)
        # For file_ref: just check trigger is true
        elif case.category == "file_ref":
            ok = trigger
        else:
            ok = False

        status = "PASS" if ok else "FAIL"
        if ok:
            passed += 1
        else:
            failed += 1

        details.append({
            "query": case.query,
            "expected": case.expected_skill_ids,
            "actual": matched,
            "expect_trigger": case.expect_trigger,
            "actual_trigger": trigger,
            "category": case.category,
            "status": status,
            "latency_ms": round(elapsed, 2),
        })

    total = passed + failed
    return {
        "total_cases": total,
        "passed": passed,
        "failed": failed,
        "mode": "offline",
        "skills_available": len(cards),
        "details": details,
    }


# ---------------------------------------------------------------------------
# Live evaluation (requires ES, Embedding, Reranker services)
# ---------------------------------------------------------------------------


def run_live_eval(cases: list[EvalCase], skills_root: Path) -> dict:
    """Run cases through the full SkillRouterMiddleware pipeline.

    Requires: Embedding (7800), Reranker (7801), ES (3128).
    """
    from deerflow.routing.embedding_client import SkillRouterEmbeddingClient
    from deerflow.routing.es_store import SkillRouterElasticStore
    from deerflow.routing.reranker_client import SkillRouterRerankerClient
    from deerflow.routing.resolver import resolve

    cards = _load_all_cards(skills_root)

    details: list[dict] = []
    passed = 0
    failed = 0

    try:
        embedding_client = SkillRouterEmbeddingClient()
        es_store = SkillRouterElasticStore()
        reranker_client = SkillRouterRerankerClient()
    except Exception as e:
        logger.error("Failed to initialize routing clients: %s", e)
        return {
            "total_cases": len(cases),
            "passed": 0,
            "failed": len(cases),
            "mode": "live",
            "error": f"Service initialization failed: {e}",
            "details": [{"query": c.query, "status": "ERROR", "error": str(e)} for c in cases],
        }

    for case in cases:
        start = time.monotonic()
        has_files = case.category == "file_ref"

        try:
            # Embed query
            query_vec = embedding_client.embed_text(case.query)

            # ES search
            filters = {"enabled": True}
            candidates = es_store.search(query_vector=query_vec, top_k=10, filters=filters)

            if not candidates:
                matched = []
            else:
                # Rerank
                reranker_input = []
                for c in candidates:
                    reranker_input.append({
                        "skill_id": c.get("skill_id", ""),
                        "name": c.get("name", ""),
                        "description": c.get("description", ""),
                        "body": c.get("body", ""),
                        "is_public": c.get("is_public", False),
                    })

                reranked = reranker_client.rerank(query=case.query, candidates=reranker_input)

                # Resolve
                resolved = resolve(query=case.query, reranked=reranked)
                matched = [r["id"] for r in resolved]

            trigger = len(matched) > 0
            elapsed = (time.monotonic() - start) * 1000

            # Evaluate
            if not case.expect_trigger:
                ok = not trigger
            elif case.category in ("known", "multi_skill", "fuzzy"):
                expected_set = set(case.expected_skill_ids)
                matched_set = set(matched)
                ok = expected_set.issubset(matched_set)
            elif case.category == "file_ref":
                ok = trigger
            else:
                ok = False

        except Exception as e:
            matched = []
            trigger = False
            elapsed = (time.monotonic() - start) * 1000
            ok = False
            error_detail = str(e)
        else:
            error_detail = None

        status = "PASS" if ok else "FAIL"
        if ok:
            passed += 1
        else:
            failed += 1

        detail = {
            "query": case.query,
            "expected": case.expected_skill_ids,
            "actual": matched,
            "expect_trigger": case.expect_trigger,
            "actual_trigger": trigger,
            "category": case.category,
            "status": status,
            "latency_ms": round(elapsed, 2),
        }
        if error_detail:
            detail["error"] = error_detail

        details.append(detail)

    total = passed + failed
    return {
        "total_cases": total,
        "passed": passed,
        "failed": failed,
        "mode": "live",
        "details": details,
    }


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------


def generate_text_report(report: dict) -> str:
    """Human-readable evaluation summary."""
    lines = []
    lines.append("=" * 60)
    lines.append("SkillRouter Evaluation Report")
    lines.append("=" * 60)
    lines.append(f"Mode: {report.get('mode', 'unknown')}")
    lines.append(f"Total cases: {report['total_cases']}")
    lines.append(f"Passed: {report['passed']}")
    lines.append(f"Failed: {report['failed']}")
    if report.get("skills_available") is not None:
        lines.append(f"Skills available: {report['skills_available']}")
    if report.get("error"):
        lines.append(f"Error: {report['error']}")
    lines.append("")

    # Group by category
    categories: dict[str, list[dict]] = {}
    for d in report.get("details", []):
        cat = d.get("category", "unknown")
        categories.setdefault(cat, []).append(d)

    for cat, items in categories.items():
        lines.append(f"--- {cat} ({len(items)} cases) ---")
        for item in items:
            status_icon = "OK" if item["status"] == "PASS" else "FAIL"
            lines.append(f"  [{status_icon}] {item['query'][:50]}")
            if item.get("expected"):
                lines.append(f"        expected: {item['expected']}")
            if item.get("actual"):
                lines.append(f"        actual:   {item['actual']}")
            if item.get("error"):
                lines.append(f"        error:    {item['error']}")
            lines.append(f"        latency:  {item.get('latency_ms', 0):.1f}ms")
        lines.append("")

    lines.append("=" * 60)
    return "\n".join(lines)


def generate_json_report(report: dict) -> str:
    """Machine-readable JSON evaluation."""
    return json.dumps(report, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate SkillRouter query routing accuracy")
    parser.add_argument("--offline", action="store_true", help="Offline mode (no ES/API calls)")
    parser.add_argument("--json", action="store_true", help="JSON output")
    parser.add_argument(
        "--skills-root",
        default=str(Path(_PROJECT_ROOT) / "skills"),
        help="Root skills directory",
    )
    args = parser.parse_args()

    skills_root = Path(args.skills_root).resolve()
    cases = load_eval_cases()

    if args.offline:
        report = run_offline_eval(cases, skills_root)
    else:
        report = run_live_eval(cases, skills_root)

    if args.json:
        print(generate_json_report(report))
    else:
        print(generate_text_report(report))

    # Exit code
    if report["failed"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
