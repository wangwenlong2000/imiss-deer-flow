import argparse
import json
from pathlib import Path


REGISTRY_PATH = Path(__file__).resolve().parents[1] / "references" / "skill_registry.json"


def load_registry():
    with REGISTRY_PATH.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def score_chain(task_text, input_kind, chain):
    text = task_text.lower()
    score = 0
    matched_keywords = []
    for keyword in chain.get("keywords", []):
        if keyword.lower() in text:
            score += 3
            matched_keywords.append(keyword)
    if input_kind in chain.get("input_kinds", []):
        score += 2
    if input_kind == "unknown":
        score += 1
    return score, matched_keywords


def plan(task_text, input_kind):
    registry = load_registry()
    candidates = []
    for chain in registry["task_chains"]:
        score, matched_keywords = score_chain(task_text, input_kind, chain)
        if score > 0:
            item = dict(chain)
            item["score"] = score
            item["matched_keywords"] = matched_keywords
            candidates.append(item)
    if not candidates:
        fallback = next(item for item in registry["task_chains"] if item["task_type"] == "realtime_event_discovery")
        candidates = [dict(fallback, score=1, matched_keywords=[])]
    candidates.sort(key=lambda item: item["score"], reverse=True)
    selected = candidates[0]
    if input_kind in {"video", "stream"} and "video-source-operations" not in selected["chain"]:
        raw_input_candidate = choose_raw_input_candidate(task_text, candidates, registry)
        selected = raw_input_candidate or selected
    chain = selected["chain"]
    preconditions = []
    if "video-source-operations" in chain:
        preconditions.append("如果视频源巡检输出 ready_for_analysis=false，应停止后续目标检测和事件识别。")
    if "structured-video-query" in chain and input_kind in {"video", "stream", "unknown"}:
        preconditions.append("自然语言查询需要结构化事件或告警；如果当前只有原始视频，应先运行事件发现链路。")
    if "alert-deduplication" in chain:
        preconditions.append("对外输出告警、报告或证据前必须使用去重后的 alerts。")
    return {
        "task": task_text,
        "input_kind": input_kind,
        "selected_task_type": selected["task_type"],
        "description": selected["description"],
        "scenario": selected.get("scenario"),
        "chain": chain,
        "final_output": selected["final_output"],
        "preconditions": preconditions,
        "matched_keywords": selected["matched_keywords"],
        "alternatives": [
            {
                "task_type": item["task_type"],
                "score": item["score"],
                "chain": item["chain"],
                "matched_keywords": item["matched_keywords"],
            }
            for item in candidates[1:4]
        ],
        "registry_version": registry["version"],
    }


def choose_raw_input_candidate(task_text, candidates, registry):
    text = task_text.lower()
    scenario_hints = [
        ("urban_governance", ["城管", "占道", "摊贩", "施工", "垃圾", "乱停", "违章", "市容"]),
        ("traffic_operations", ["交通", "拥堵", "违停", "逆行", "车流", "车速", "车型", "路口", "道路"]),
        ("public_safety", ["安全", "聚集", "摔倒", "打斗", "烟火", "火灾", "翻越", "徘徊", "人群"]),
    ]
    for task_type, keywords in scenario_hints:
        if any(keyword.lower() in text for keyword in keywords):
            selected = dict(next(item for item in registry["task_chains"] if item["task_type"] == task_type))
            selected["score"] = 99
            selected["matched_keywords"] = [keyword for keyword in keywords if keyword.lower() in text]
            return selected
    for item in candidates:
        if "video-source-operations" in item["chain"]:
            return item
    selected = dict(next(item for item in registry["task_chains"] if item["task_type"] == "realtime_event_discovery"))
    selected["score"] = 1
    selected["matched_keywords"] = []
    return selected


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("task", help="用户任务描述")
    parser.add_argument("--input-kind", default="unknown", choices=["video", "stream", "alerts", "events", "feedback", "unknown"])
    args = parser.parse_args()
    print(json.dumps(plan(args.task, args.input_kind), ensure_ascii=False, indent=2))
