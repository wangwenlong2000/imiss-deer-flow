import argparse
import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2] / "_shared"))
from urban_video_common import base_response, event, media_metadata, stable_int


def detect(video_path, camera_id):
    response = base_response("object-tracking-analysis", video_path)
    meta = media_metadata(video_path)
    person_count = stable_int(video_path + ":person", 5, 120)
    vehicle_count = stable_int(video_path + ":vehicle", 10, 180)
    non_motor_count = stable_int(video_path + ":bike", 3, 80)
    tracks = []
    for index, kind in enumerate(["person", "vehicle", "non_motor_vehicle", "special_vehicle"]):
        tracks.append({
            "track_id": f"{camera_id}-{kind}-{index + 1:03d}",
            "camera_id": camera_id,
            "object_type": kind,
            "first_seen": "00:00:03",
            "last_seen": f"00:{stable_int(kind + video_path, 1, 8):02d}:{stable_int(video_path + kind, 10, 55):02d}",
            "trajectory": [
                {"t": "00:00:03", "x": 120 + index * 80, "y": 360 + index * 20},
                {"t": "00:00:08", "x": 180 + index * 80, "y": 350 + index * 18},
                {"t": "00:00:15", "x": 260 + index * 80, "y": 330 + index * 15},
            ],
            "confidence": round(0.76 + index * 0.04, 3),
        })
    primitive_events = [
        event("primitive-intrusion-001", "zone_intrusion", camera_id, "medium", 0.81),
        event("primitive-left-object-001", "left_object", camera_id, "low", 0.72),
    ]
    response["result"] = {
        "camera_id": camera_id,
        "media": meta,
        "counts": {
            "person": person_count,
            "vehicle": vehicle_count,
            "non_motor_vehicle": non_motor_count,
            "special_vehicle": stable_int(video_path + ":special", 0, 6),
            "left_object": stable_int(video_path + ":left_object", 0, 3),
        },
        "tracks": tracks,
        "primitive_events": primitive_events,
    }
    response["quality"]["confidence"] = 0.82
    if not meta["exists"]:
        response["status"] = "partial"
        response["quality"]["warnings"].append("输入文件不存在，返回基于源标识的模拟结构化结果。")
    return response


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("video_path")
    parser.add_argument("--camera-id", default="camera-001")
    args = parser.parse_args()
    print(json.dumps(detect(args.video_path, args.camera_id), ensure_ascii=False, indent=2))
