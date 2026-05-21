from __future__ import annotations

from hashlib import sha1

from src.skills.base import BaseSkill, SkillContext, utc_now_iso


class EventTemplateMappingSkill(BaseSkill):
    name = "event-template-mapping"

    def run(self, input_data: dict, context: SkillContext) -> dict:
        template_name = input_data.get("template_name")
        template = input_data.get("template") or context.config.get("event_templates", {}).get(template_name, {})
        method_results = input_data.get("method_results", [])
        event = build_event_from_method_results(template_name, template, method_results, input_data.get("camera_id"))
        if not event:
            return self.success({"matched": False})
        return self.success({"matched": True, **event}, event["confidence"])


def build_event_from_method_results(template_name: str, template: dict, method_results: list[dict], camera_id: str | None) -> dict | None:
    if not method_results:
        return None
    hits = [result for result in method_results if result.get("matched")]
    join_policy = template.get("join_policy", "all_required")
    if join_policy == "all_required" and len(hits) != len(method_results):
        return None
    if join_policy == "any_required" and not hits:
        return None

    scores = [_method_confidence(result) for result in method_results]
    score = sum(scores) / len(scores)
    min_score = float(template.get("score_policy", {}).get("min_score", 0))
    if join_policy == "weighted_score":
        score = _weighted_score(template, method_results)
    if score < min_score:
        return None

    related_tracks = sorted({sid for result in method_results for sid in _subject_ids(result) if sid})
    start_times = [item.get("start_time") for result in method_results for item in result.get("matches", []) if item.get("start_time")]
    end_times = [item.get("end_time") for result in method_results for item in result.get("matches", []) if item.get("end_time")]
    roi_ids = [item.get("roi_id") for result in method_results for item in result.get("matches", []) + result.get("clusters", []) + result.get("groups", []) if item.get("roi_id")]

    stable_key = "|".join([str(camera_id), template_name, ",".join(related_tracks), ",".join(roi_ids)])
    event_suffix = sha1(stable_key.encode("utf-8")).hexdigest()[:10]
    return {
        "event_id": f"EVT_{template_name}_{event_suffix}",
        "event_type": template.get("event_type", template_name),
        "camera_id": camera_id,
        "roi_id": roi_ids[0] if roi_ids else None,
        "start_time": min(start_times) if start_times else None,
        "end_time": max(end_times) if end_times else None,
        "confidence": round(score, 4),
        "severity": template.get("severity", "medium"),
        "related_tracks": related_tracks,
        "reason": _reason(template_name, hits),
        "method_hits": [result.get("method") for result in hits],
        "rule_hits": _rule_hits(template, hits),
        "evidence_frame_ids": sorted({fid for result in method_results for fid in _evidence_ids(result)}),
        "status": "candidate",
        "created_at": utc_now_iso(),
    }


def _method_confidence(result: dict) -> float:
    items = result.get("matches") or result.get("clusters") or result.get("groups") or result.get("anomalies") or []
    if not items:
        return 0.0
    return sum(float(item.get("confidence", 1.0)) for item in items) / len(items)


def _weighted_score(template: dict, method_results: list[dict]) -> float:
    weights = template.get("score_policy", {}).get("weights", {})
    total_weight = 0.0
    total = 0.0
    for result in method_results:
        weight = float(weights.get(result.get("method"), 1.0))
        total_weight += weight
        total += _method_confidence(result) * weight
    return total / total_weight if total_weight else 0.0


def _subject_ids(result: dict) -> list[str]:
    ids = []
    for key in ("matches", "clusters", "groups"):
        for item in result.get(key, []):
            ids.extend(item.get("object_ids", []))
            if item.get("subject_id"):
                ids.append(item["subject_id"])
    return ids


def _evidence_ids(result: dict) -> list[str]:
    ids = []
    for item in result.get("matches", []):
        ids.extend(item.get("evidence_frame_ids", []))
    return ids


def _reason(template_name: str, hits: list[dict]) -> str:
    methods = ", ".join(result.get("method", "unknown") for result in hits)
    return f"{template_name} matched by {methods}"


def _rule_hits(template: dict, hits: list[dict]) -> list[str]:
    rules = []
    for method in template.get("methods", []):
        if method.get("target_labels"):
            rules.append("label:" + ",".join(method["target_labels"]))
        if method.get("roi_types"):
            rules.append("roi:" + ",".join(method["roi_types"]))
        if method.get("min_duration_seconds") is not None:
            rules.append(f"duration>={method['min_duration_seconds']}")
    rules.extend(f"method:{hit.get('method')}" for hit in hits)
    return rules
