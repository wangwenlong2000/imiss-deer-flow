import argparse
import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2] / "_shared"))
from urban_video_common import base_response, load_json, stable_int


def correlate(alerts_path, context_path):
    response = base_response("multi-source-correlation", alerts_path)
    payload = load_json(alerts_path, {})
    context = load_json(context_path, {})
    alerts = payload.get("result", {}).get("alerts", [])
    cases = []
    for alert in alerts:
        location = alert.get("location", {})
        case = {
            "case_id": f"case-{alert.get('alert_id', alert.get('event_id', 'unknown'))}",
            "alert": alert,
            "map_context": {
                "grid_id": location.get("grid_id", "grid-001"),
                "road": location.get("road", "人民路"),
                "jurisdiction": context.get("jurisdiction", "中心城区网格"),
                "nearby_cameras": [alert.get("camera_id", "camera-001"), "camera-002", "camera-003"],
            },
            "work_order_context": {
                "related_open_orders": stable_int(str(alert) + ":orders", 0, 3),
                "recommended_department": "交警" if "parking" in alert.get("event_type", "") or "traffic" in alert.get("event_type", "") else "城管",
            },
            "iot_context": {
                "nearby_sensor_signal": "normal",
                "explanation": "未发现传感器异常，视频告警为主要依据。",
            },
        }
        if alert.get("event_type") == "traffic_congestion":
            case["iot_context"] = {
                "nearby_sensor_signal": "traffic_flow_high",
                "explanation": "道路流量传感器与视频拥堵告警一致。",
            }
        cases.append(case)
    response["result"] = {
        "correlated_cases": cases,
        "case_count": len(cases),
        "external_context_used": bool(context),
    }
    response["quality"]["confidence"] = 0.78
    return response


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("alerts")
    parser.add_argument("--context", default="")
    args = parser.parse_args()
    print(json.dumps(correlate(args.alerts, args.context), ensure_ascii=False, indent=2))
