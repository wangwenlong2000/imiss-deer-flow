import argparse
import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2] / "_shared"))
from urban_video_common import base_response, now_iso, stable_int, write_json


def build(input_path, output_dir):
    response = base_response("evidence-package-builder", input_path)
    package_id = f"evidence-{stable_int(input_path + now_iso(), 10000, 99999)}"
    package_dir = Path(output_dir) / package_id
    timeline = [
        {"time": "T-10s", "description": "事件前状态，目标进入监控区域。"},
        {"time": "T", "description": "触发告警，规则命中并生成事件。"},
        {"time": "T+10s", "description": "事件持续或目标离开，形成复核片段。"},
    ]
    evidence_doc = {
        "package_id": package_id,
        "source_input": input_path,
        "timeline": timeline,
        "artifacts": [
            {"type": "snapshot", "path": str(package_dir / "snapshot_placeholder.jpg"), "status": "planned"},
            {"type": "clip", "path": str(package_dir / "clip_placeholder.mp4"), "status": "planned"},
            {"type": "json", "path": str(package_dir / "evidence.json"), "status": "created"},
        ],
        "structured_description": "已生成事件证据包结构，包含时间线、目标轨迹占位、截图和短视频片段引用。",
        "traceability": {
            "created_at": now_iso(),
            "source_result": input_path,
            "chain_of_custody": ["analysis_result", "alert_review", "evidence_package"],
        },
    }
    written = write_json(package_dir / "evidence.json", evidence_doc)
    response["result"] = evidence_doc
    response["result"]["artifacts"][2]["path"] = written
    response["quality"]["confidence"] = 0.88
    return response


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input")
    parser.add_argument("--output-dir", default="/mnt/user-data/outputs")
    args = parser.parse_args()
    print(json.dumps(build(args.input, args.output_dir), ensure_ascii=False, indent=2))
