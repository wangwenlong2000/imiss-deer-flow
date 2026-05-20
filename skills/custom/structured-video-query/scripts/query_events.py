import argparse
import json
import sys
from collections import Counter
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2] / "_shared"))
from urban_video_common import base_response, load_json


def extract_records(payload):
    result = payload.get("result", {})
    for key in ("alerts", "events", "violations", "correlated_cases"):
        if key in result:
            if key == "correlated_cases":
                return [item.get("alert", {}) for item in result[key]]
            return result[key]
    return []


def query(input_path, query_text):
    response = base_response("structured-video-query", input_path)
    payload = load_json(input_path, {})
    records = extract_records(payload)
    event_counter = Counter(item.get("event_type", "unknown") for item in records)
    road_counter = Counter(item.get("location", {}).get("road", "未知道路") for item in records)
    high_risk = [item for item in records if item.get("severity") in {"critical", "high"} or item.get("dispatch_priority") == "P1"]
    if "违停" in query_text or "parking" in query_text.lower():
        filtered = [item for item in records if item.get("event_type") == "illegal_parking"]
        answer = f"检索到 {len(filtered)} 条违停相关结构化记录。"
    elif "高风险" in query_text:
        answer = f"检索到 {len(high_risk)} 条高风险事件，主要集中在 {road_counter.most_common(1)[0][0] if road_counter else '未知区域'}。"
    else:
        answer = f"共检索到 {len(records)} 条结构化视频记录，事件类型分布为 {dict(event_counter)}。"
    response["result"] = {
        "query": query_text,
        "answer": answer,
        "supporting_facts": {
            "record_count": len(records),
            "event_distribution": dict(event_counter),
            "top_roads": road_counter.most_common(5),
            "high_risk_count": len(high_risk),
        },
        "filters": {
            "source": input_path,
            "basis": "structured_events_only",
        },
        "residual_risk": "如果上游事件抽取不完整，本查询结果也会相应缺失。",
    }
    response["quality"]["confidence"] = 0.83 if records else 0.45
    if not records:
        response["status"] = "partial"
        response["quality"]["warnings"].append("未在输入中找到结构化事件或告警记录。")
    return response


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input")
    parser.add_argument("--query", required=True)
    args = parser.parse_args()
    print(json.dumps(query(args.input, args.query), ensure_ascii=False, indent=2))
