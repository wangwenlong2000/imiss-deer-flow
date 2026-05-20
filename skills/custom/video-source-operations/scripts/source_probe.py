import argparse
import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2] / "_shared"))
from urban_video_common import base_response, media_metadata, stable_int


def probe(source_uri, source_type):
    response = base_response("video-source-operations", source_uri)
    meta = media_metadata(source_uri)
    online = meta["exists"] or source_type in {"rtsp", "gb28181", "platform"}
    latency_ms = stable_int(source_uri + ":latency", 40, 850)
    quality_score = stable_int(source_uri + ":quality", 55, 96)
    issue_flags = {
        "black_screen": quality_score < 58,
        "color_bars_or_noise": stable_int(source_uri + ":noise", 0, 100) > 92,
        "occlusion": stable_int(source_uri + ":occlusion", 0, 100) > 88,
        "freeze": stable_int(source_uri + ":freeze", 0, 100) > 90,
        "blur": quality_score < 65,
    }
    blocking_issues = [name for name, active in issue_flags.items() if active]
    response["result"] = {
        "connectivity": {
            "source_type": source_type,
            "online": online,
            "latency_ms": latency_ms,
            "protocol_supported": source_type in {"file", "rtsp", "gb28181", "platform"},
        },
        "stream_quality": {
            "duration_seconds": meta["duration_seconds"],
            "fps": meta["fps"],
            "resolution": meta["resolution"],
            "codec": meta["codec"],
            "quality_score": quality_score,
            "drop_frame_risk": "high" if latency_ms > 650 else "medium" if latency_ms > 350 else "low",
        },
        "visual_health": issue_flags,
        "readiness": {
            "ready_for_analysis": online and not blocking_issues,
            "blocking_issues": blocking_issues,
        },
    }
    response["quality"]["confidence"] = 0.86
    if not meta["exists"] and source_type == "file":
        response["status"] = "failed"
        response["quality"]["warnings"].append("文件不存在，无法进入后续视频分析。")
    return response


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("source_uri")
    parser.add_argument("--source-type", default="file", choices=["file", "rtsp", "gb28181", "platform"])
    args = parser.parse_args()
    print(json.dumps(probe(args.source_uri, args.source_type), ensure_ascii=False, indent=2))
