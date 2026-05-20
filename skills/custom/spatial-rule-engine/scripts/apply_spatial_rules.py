import argparse
import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2] / "_shared"))
from urban_video_common import base_response, load_json


DEFAULT_RULES = [
    {"rule_id": "roi-non-motor-lane", "event_types": ["illegal_parking", "construction_occupation"], "severity": "high"},
    {"rule_id": "fence-public-square", "event_types": ["crowd_gathering", "roadside_stall"], "severity": "medium"},
    {"rule_id": "direction-main-road", "event_types": ["wrong_way_driving"], "severity": "high"},
    {"rule_id": "safety-critical", "event_types": ["fall", "fire_smoke"], "severity": "critical"},
]


def collect_events(payload):
    result = payload.get("result", {})
    if "events" in result:
        return result["events"]
    events = []
    for values in result.get("business_categories", {}).values():
        events.extend(values)
    return events


def apply_rules(input_path, rules_path):
    response = base_response("spatial-rule-engine", input_path)
    payload = load_json(input_path, {})
    rules = load_json(rules_path, DEFAULT_RULES)
    events = collect_events(payload)
    matches = []
    unmatched = []
    for item in events:
        matched_rules = [rule for rule in rules if item.get("event_type") in rule.get("event_types", [])]
        if not matched_rules:
            unmatched.append(item)
            continue
        for rule in matched_rules:
            enriched = dict(item)
            enriched["rule_id"] = rule["rule_id"]
            enriched["rule_severity"] = rule["severity"]
            enriched["spatial_decision"] = "violation"
            matches.append(enriched)
    response["result"] = {
        "rule_version": "urban-spatial-rules-v1",
        "rule_matches": matches,
        "violations": [item for item in matches if item["spatial_decision"] == "violation"],
        "unmatched_events": unmatched,
    }
    response["quality"]["confidence"] = 0.84
    if not events:
        response["status"] = "partial"
        response["quality"]["warnings"].append("未发现可应用空间规则的结构化事件。")
    return response


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input")
    parser.add_argument("--rules", default="")
    args = parser.parse_args()
    print(json.dumps(apply_rules(args.input, args.rules), ensure_ascii=False, indent=2))
