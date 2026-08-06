"""Query segmenter for SkillRouterMiddleware.

Performs rule-based task segmentation on user queries, splitting multi-scenario
requests into coarse-grained task segments and determining whether routing
should be skipped (e.g. for chit-chat).
"""

import logging
import re
import uuid

from deerflow.routing.intent_policy import policy_float, policy_list

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# should_route
# ---------------------------------------------------------------------------

def is_obvious_chitchat(query: str, uploaded_files: list[dict] | None = None) -> bool:
    """Return True when a turn is clearly conversational and should not route."""
    if uploaded_files:
        return False
    if not query or not query.strip():
        return True

    text = query.strip()
    if text in policy_list("chitchat", "exact_keywords"):
        return True

    return any(re.match(str(pat), text, re.IGNORECASE) for pat in policy_list("chitchat", "regex_patterns"))


def should_route(query: str, uploaded_files: list[dict] | None = None) -> bool:
    """Return True if *query* needs professional Skill routing.

    Skip obvious chit-chat.  Do NOT skip when:
    - there are uploaded files,
    - the query references "this file/table/data",
    - the query contains task intents like analyse, generate, process, etc.
    """
    if not query or not query.strip():
        return False

    text = query.strip()
    if is_obvious_chitchat(text):
        return False

    # --- signals that force routing ---

    # Uploaded files present
    if uploaded_files:
        return True

    # References to "this file/table/data"
    for ref in policy_list("should_route", "file_refs"):
        if ref in text:
            return True

    # Task-intent keywords
    for kw in policy_list("should_route", "task_intents"):
        if kw in text:
            return True

    return False


# ---------------------------------------------------------------------------
# Task segmentation
# ---------------------------------------------------------------------------

def segment_query(
    query: str,
    scene_hint: str | None = None,
    scene_hints: list[str] | None = None,
) -> list[dict]:
    """Split *query* into coarse-grained task segments.

    First version uses rule-based matching.  Returns a list of dicts with
    keys: ``segment_id``, ``text``, ``scene``, ``input_refs``.
    """
    text = query.strip()
    if not text:
        return []

    hints: list[str] = []
    if scene_hints:
        for hint in scene_hints:
            if isinstance(hint, str) and hint.strip() and hint.strip() not in hints:
                hints.append(hint.strip())
    if not hints and scene_hint and scene_hint.strip():
        hints.append(scene_hint.strip())

    if hints:
        return [{
            "segment_id": _new_id(),
            "text": text,
            "scene": hint,
            "input_refs": [],
        } for hint in hints]

    matched_scenes = _match_configured_scenes(text)

    # If no scene matched, return a single "unknown" segment
    if not matched_scenes:
        return [{
            "segment_id": _new_id(),
            "text": text,
            "scene": None,
            "input_refs": [],
        }]

    # Build one segment per matched scene.  Also extract relevant sub-texts
    # when possible; for v1, use the full query as segment text.
    segments: list[dict] = []
    for scene in matched_scenes:
        segments.append({
            "segment_id": _new_id(),
            "text": text,
            "scene": scene,
            "input_refs": [],
        })

    return segments


def _new_id() -> str:
    return f"seg_{uuid.uuid4().hex[:6]}"


def _match_configured_scenes(text: str) -> list[str]:
    """Return scene ids matched from configured intent scene templates."""
    try:
        from deerflow.routing.intent import load_scene_templates
        from deerflow.routing.intent.classifier import _compact, _scene_terms
    except Exception:
        return []

    matches: list[tuple[str, float]] = []
    try:
        query_compact = _compact(text)
        for scene_id, scene_config in load_scene_templates().items():
            terms = _scene_terms(scene_config)
            if not terms:
                continue

            score = 0.0
            matched = 0
            for term, weight in terms.items():
                if term and term in query_compact:
                    matched += 1
                    score += weight

            if matched:
                score = min(
                    policy_float("thresholds", "scene_match_max_score"),
                    policy_float("thresholds", "scene_match_base_score")
                    + score / policy_float("thresholds", "scene_match_score_divisor"),
                )

            if score >= policy_float("thresholds", "scene_match_min_score"):
                configured_scene = scene_config.get("scene")
                if isinstance(configured_scene, str) and configured_scene.strip():
                    matches.append((configured_scene.strip(), score))
                else:
                    matches.append((scene_id, score))
    except Exception:
        logger.exception("Failed to match configured scenes for query segmentation")
        return []

    seen: set[str] = set()
    scenes: list[str] = []
    for scene, _score in sorted(matches, key=lambda item: item[1], reverse=True):
        if scene not in seen:
            seen.add(scene)
            scenes.append(scene)
    return scenes
