import argparse
import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2] / "_shared"))
from urban_video_common import base_response, load_json, stable_int


def govern(input_path, model_name):
    response = base_response("model-ops-governance", input_path)
    payload = load_json(input_path, {})
    suppressed = payload.get("result", {}).get("suppressed", [])
    false_positive_pressure = len(suppressed)
    precision_estimate = max(0.55, 0.92 - false_positive_pressure * 0.03)
    recall_estimate = stable_int(model_name + ":recall", 70, 94) / 100
    response["result"] = {
        "model_registry": {
            "model_name": model_name,
            "active_version": "rule-baseline-v1",
            "candidate_version": "urban-video-v2",
            "scenario_profile": "urban_governance_default",
        },
        "performance_summary": {
            "precision_estimate": round(precision_estimate, 3),
            "recall_estimate": recall_estimate,
            "latency_p95_ms": stable_int(model_name + ":latency", 80, 420),
            "false_positive_feedback_count": false_positive_pressure,
        },
        "threshold_recommendations": {
            "current_confidence_threshold": 0.75,
            "recommended_confidence_threshold": 0.8 if false_positive_pressure > 3 else 0.75,
            "reason": "误报反馈偏多时提高阈值；否则保持当前阈值。",
        },
        "feedback_queue": {
            "pending_samples": false_positive_pressure,
            "next_actions": ["人工复核误报样本", "按场景归档样本", "纳入下一轮回归测试"],
        },
        "deployment_risk": "medium" if precision_estimate < 0.75 else "low",
    }
    response["quality"]["confidence"] = 0.81
    return response


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input")
    parser.add_argument("--model-name", default="urban-video-detector")
    args = parser.parse_args()
    print(json.dumps(govern(args.input, args.model_name), ensure_ascii=False, indent=2))
