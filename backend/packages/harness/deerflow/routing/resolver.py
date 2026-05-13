"""Resolver for SkillRouterMiddleware.

Takes ES Top-K candidates and Reranker scores, applies public-skill
constraints, and produces the final ``selected_skills`` list with roles.
"""

import logging

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration constants
# ---------------------------------------------------------------------------

MAX_PUBLIC_SKILLS_PER_SEGMENT = 2
RERANKER_MIN_SCORE = 0.65


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
        prioritised as primary; public skills are demoted to supporting.

    Returns
    -------
    list[dict]
        Selected skills with ``id``, ``role``, ``score`` keys, ordered by
        descending score.  At most ``MAX_PUBLIC_SKILLS_PER_SEGMENT`` public
        skills are included.
    """
    if not reranked:
        return []

    selected: list[dict] = []
    public_count = 0

    # When a scene is known, promote the highest-scoring non-public skill
    # whose scenes match the segment scene to primary.
    if scene:
        scene_matched = [
            c for c in reranked
            if scene in c.get("scenes", []) and not c.get("is_public", False)
        ]
        if scene_matched:
            best = max(scene_matched, key=lambda c: c.get("score", 0.0))
            selected.append({
                "id": best["skill_id"],
                "role": "primary",
                "score": best["score"],
            })

    for candidate in reranked:
        skill_id = candidate.get("skill_id", "")

        # Already selected as primary via scene match — skip duplicate
        if selected and skill_id == selected[0]["id"]:
            continue

        is_public = candidate.get("is_public", False)
        score = candidate.get("score", 0.0)

        if score < RERANKER_MIN_SCORE:
            continue

        # When a scene-matched primary exists, public skills can only be
        # supporting.
        if is_public and selected and selected[0]["role"] == "primary":
            pass  # can still be added as supporting below

        if is_public:
            if public_count >= MAX_PUBLIC_SKILLS_PER_SEGMENT:
                continue
            public_count += 1

        role = "primary" if not selected else "supporting"
        selected.append({
            "id": skill_id,
            "role": role,
            "score": score,
        })

    return selected


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
