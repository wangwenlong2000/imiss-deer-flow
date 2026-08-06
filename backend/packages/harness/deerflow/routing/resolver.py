"""Resolver for SkillRouterMiddleware.

Takes ES Top-K candidates and Reranker scores, applies public-skill
constraints, and produces the final ``selected_skills`` list with roles.
"""

import logging

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration constants
# ---------------------------------------------------------------------------

RERANKER_MIN_SCORE = 0.65
SUPPORTING_SCORE_RATIO = 0.72


def resolve(
    query: str,
    reranked: list[dict],
    scene: str | None = None,
) -> list[dict]:
    """Resolve final skill selection from reranker output.

    Parameters
    ----------
    query:
        The task segment text (unused in v1 but available for future logic).
    reranked:
        List of dicts from ``SkillRouterRerankerClient.rerank``, each
        containing at least ``skill_id``, ``is_public``, ``scenes``, and ``score``.
    scene:
        Optional scene label from the query segmenter (e.g. ``"policy_regulation"``).
        When set, non-public skills whose ``scenes`` include *scene* are
        prioritised as primary.

    Returns
    -------
    list[dict]
        Selected skills with ``id``, ``role``, ``score`` keys, ordered by
        descending score. Public skills are ignored here because they are
        injected separately as default shared capabilities.
    """
    if not reranked:
        return []

    eligible = [
        c for c in reranked
        if not c.get("is_public", False) and not _matches_negative_routing(query, c)
    ]
    if not eligible:
        return []

    selected: list[dict] = []

    # When a scene is known, promote the highest-scoring non-public skill
    # whose scenes match the segment scene to primary.
    if scene:
        scene_matched = [
            c for c in eligible
            if scene in c.get("scenes", [])
        ]
        if scene_matched:
            best = max(scene_matched, key=lambda c: c.get("score", 0.0))
            selected.append({
                "id": best["skill_id"],
                "role": "primary",
                "score": best["score"],
            })

    supporting_min_score = RERANKER_MIN_SCORE
    if selected:
        primary_score = selected[0].get("score", 0.0)
        if primary_score > RERANKER_MIN_SCORE:
            supporting_min_score = max(RERANKER_MIN_SCORE, primary_score * SUPPORTING_SCORE_RATIO)

    for candidate in eligible:
        skill_id = candidate.get("skill_id", "")

        # Already selected as primary via scene match — skip duplicate
        if selected and skill_id == selected[0]["id"]:
            continue

        score = candidate.get("score", 0.0)

        min_score = supporting_min_score if selected else RERANKER_MIN_SCORE
        if score < min_score and not _matches_positive_routing(query, candidate, scene=scene):
            continue

        role = "primary" if not selected else "supporting"
        selected.append({
            "id": skill_id,
            "role": role,
            "score": score,
        })

    return selected


def _matches_negative_routing(query: str, candidate: dict) -> bool:
    """Return True when the query explicitly matches a card's negative boundary."""
    query_lower = query.lower()
    for field in ("negative_triggers", "anti_keywords"):
        values = candidate.get(field, []) or []
        for value in values:
            text = str(value).strip()
            if text and text.lower() in query_lower:
                return True
    return False


def _matches_positive_routing(query: str, candidate: dict, scene: str | None = None) -> bool:
    """Return True when the query explicitly asks for a candidate's capability.

    Reranker scores are useful for primary selection, but compound requests can
    contain several explicit subtasks. Keep low-scoring supporting skills when
    their router card has an exact positive trigger or keyword match in the
    same scene, while still respecting negative routing boundaries first.
    """
    if scene and scene not in candidate.get("scenes", []):
        return False

    query_lower = query.lower()
    for field in ("positive_triggers", "keywords", "prefer_when"):
        values = candidate.get(field, []) or []
        for value in values:
            text = str(value).strip()
            if text and text.lower() in query_lower:
                return True
    return False


def pick_primary(selected: list[dict | object]) -> dict | None:
    """Return the primary skill from *selected* or None."""
    for s in selected:
        role = s.get("role") if isinstance(s, dict) else getattr(s, "role", None)
        if role == "primary":
            return s if isinstance(s, dict) else {"id": s.id, "role": s.role, "score": s.score}
    # Fallback: highest score
    if selected:
        def _score(s):
            return s.get("score") if isinstance(s, dict) else getattr(s, "score", 0)
        best = max(selected, key=_score)
        if isinstance(best, dict):
            return best
        return {"id": best.id, "role": best.role, "score": best.score}
    return None
