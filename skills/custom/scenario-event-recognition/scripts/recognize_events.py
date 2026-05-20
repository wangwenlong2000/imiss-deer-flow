import argparse
import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2] / "_shared"))
from urban_video_common import base_response, event, load_json, stable_int


SCENARIOS = {
    "traffic": ["traffic_congestion", "illegal_parking", "wrong_way_driving"],
    "governance": ["roadside_stall", "construction_occupation", "garbage_exposure"],
    "safety": ["crowd_gathering", "fall", "fire_smoke"],
}


def recognize(input_path, scenario):
    response = base_response("scenario-event-recognition", input_path)
    upstream = load_json(input_path, {})
    result = upstream.get("result", {})
    camera_id = result.get("camera_id") or result.get("tracks", [{}])[0].get("camera_id", "camera-001")
    selected = []
    if scenario == "all":
        for values in SCENARIOS.values():
            selected.extend(values)
    else:
        selected = SCENARIOS.get(scenario, SCENARIOS["traffic"])
    events = []
    for index, event_type in enumerate(selected):
        score = stable_int(input_path + event_type, 60, 96) / 100
        if score < 0.68:
            continue
        severity = "high" if event_type in {"fire_smoke", "fall", "traffic_congestion"} and score > 0.8 else "medium"
        events.append(event(f"evt-{event_type}-{index + 1:03d}", event_type, camera_id, severity, score))
    response["result"] = {
        "scenario": scenario,
        "events": events,
        "event_count": len(events),
        "business_categories": {
            "traffic": [item for item in events if item["event_type"] in SCENARIOS["traffic"]],
            "governance": [item for item in events if item["event_type"] in SCENARIOS["governance"]],
            "safety": [item for item in events if item["event_type"] in SCENARIOS["safety"]],
        },
    }
    response["quality"]["confidence"] = 0.8
    return response


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input")
    parser.add_argument("--scenario", default="all", choices=["traffic", "governance", "safety", "all"])
    args = parser.parse_args()
    print(json.dumps(recognize(args.input, args.scenario), ensure_ascii=False, indent=2))
