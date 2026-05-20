import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2] / "_shared"))
from urban_video_common import base_response, load_json


def collect_items(payload):
    result = payload.get("result", {})
    for key in ("violations", "events", "rule_matches"):
        if key in result:
            return result[key]
    return []


def deduplicate(input_path, min_confidence):
    response = base_response("alert-deduplication", input_path)
    payload = load_json(input_path, {})
    items = collect_items(payload)
    groups = defaultdict(list)
    suppressed = []
    for item in items:
        if item.get("confidence", 0) < min_confidence:
            suppressed.append({"reason": "low_confidence", "item": item})
            continue
        key = (item.get("camera_id", "unknown"), item.get("event_type", "unknown"), item.get("rule_id", "no-rule"))
        groups[key].append(item)
    alerts = []
    dedup_groups = []
    for index, (key, grouped) in enumerate(groups.items(), start=1):
        primary = max(grouped, key=lambda item: item.get("confidence", 0))
        alert = dict(primary)
        alert["alert_id"] = f"alert-{index:04d}"
        alert["merged_count"] = len(grouped)
        alert["dispatch_priority"] = "P1" if primary.get("severity") in {"critical", "high"} else "P2"
        alerts.append(alert)
        dedup_groups.append({"group_key": "|".join(key), "kept": alert["alert_id"], "merged_count": len(grouped)})
    response["result"] = {
        "alerts": alerts,
        "suppressed": suppressed,
        "dedup_groups": dedup_groups,
        "feedback_actions": [
            "支持将 alert_id 标记为 false_positive，用于后续阈值调优。",
            "支持记录人工复核结果，回流到模型治理 skill。",
        ],
    }
    response["quality"]["confidence"] = 0.9
    return response


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input")
    parser.add_argument("--min-confidence", type=float, default=0.75)
    args = parser.parse_args()
    print(json.dumps(deduplicate(args.input, args.min_confidence), ensure_ascii=False, indent=2))
