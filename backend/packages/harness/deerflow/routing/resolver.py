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
) -> list[dict]:
    """Resolve final skill selection from reranker output.

    Parameters
    ----------
    query:
        The task segment text (unused in v1 but available for future logic).
    reranked:
        List of dicts from ``SkillRouterRerankerClient.rerank``, each
        containing at least ``skill_id``, ``is_public``, and ``score``.

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

    for candidate in reranked:
        skill_id = candidate.get("skill_id", "")
        is_public = candidate.get("is_public", False)
        score = candidate.get("score", 0.0)

        # Skip candidates below the threshold
        if score < RERANKER_MIN_SCORE:
            continue

        # Enforce public-skill cap
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
