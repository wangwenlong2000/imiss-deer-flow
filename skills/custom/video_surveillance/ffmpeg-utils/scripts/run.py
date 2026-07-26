#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

VERSION = "1.0.0"
SKILL = "ffmpeg-utils"


def load_input(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    source = Path(path)
    text = source.read_text(encoding="utf-8")
    return json.loads(text) if text.strip() else {}


def value(input_data: dict[str, Any], *keys: str, default: Any = None) -> Any:
    nested = input_data.get("data") if isinstance(input_data.get("data"), dict) else {}
    for key in keys:
        if input_data.get(key) is not None:
            return input_data[key]
        if nested.get(key) is not None:
            return nested[key]
    return default


def success(data: dict[str, Any], confidence: float = 1.0) -> dict[str, Any]:
    return {
        "skill": SKILL,
        "version": VERSION,
        "status": "success",
        "confidence": round(float(confidence), 4),
        "data": data,
    }


def failed(code: str, message: str, retryable: bool = False, detail: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "skill": SKILL,
        "version": VERSION,
        "status": "failed",
        "error_code": code,
        "message": message,
        "retryable": retryable,
        "detail": detail or {},
    }


def emit(result: dict[str, Any], output: str | None) -> int:
    text = json.dumps(result, ensure_ascii=False, indent=2)
    if output:
        path = Path(output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0 if result.get("status") == "success" else 1


def run_ffmpeg(command: list[str]) -> dict[str, Any]:
    if not shutil.which("ffmpeg"):
        return failed("FFMPEG_MISSING", "ffmpeg is required but was not found in PATH", False)
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        return failed("FFMPEG_FAILED", "ffmpeg command failed", True, {"stderr": result.stderr[-2000:]})
    return success({"command": command, "stdout": result.stdout})


def main() -> int:
    p = argparse.ArgumentParser(description="Run small FFmpeg operations for custom video skills.")
    p.add_argument("positional", nargs="*")
    p.add_argument("--input", help="Optional JSON payload; CLI flags override matching fields.")
    p.add_argument("--operation", "--op", choices=["segment", "keyframe"])
    p.add_argument("--input-path", "--video", dest="input_path")
    p.add_argument("--start-time", "--start", dest="start_time")
    p.add_argument("--timestamp")
    p.add_argument("--duration")
    p.add_argument("--output-path", dest="output_path")
    p.add_argument("--output")
    args = p.parse_args()
    input_data = load_input(args.input)

    positional = args.positional
    operation = args.operation or (positional[0] if positional else None) or value(input_data, "operation", "op")
    input_path = args.input_path or (positional[1] if len(positional) > 1 else None) or value(input_data, "input_path", "video", "raw_segment_uri")
    output_path = args.output_path or value(input_data, "output_path", "output_uri", "output_file")

    if not operation:
        return emit(failed("MISSING_OPERATION", "operation is required: segment or keyframe"), args.output)
    if not input_path:
        return emit(failed("MISSING_INPUT", "input_path is required"), args.output)

    if operation == "segment":
        start_time = args.start_time or (positional[2] if len(positional) > 2 else None) or value(input_data, "start_time", "start", default=0)
        duration = args.duration or (positional[3] if len(positional) > 3 else None) or value(input_data, "duration")
        output_path = output_path or (positional[4] if len(positional) > 4 else None)
        if duration is None or output_path is None:
            return emit(failed("MISSING_ARGUMENT", "segment requires duration and output_path"), args.output)
        command = ["ffmpeg", "-y", "-ss", str(start_time), "-i", str(input_path), "-t", str(duration), "-c", "copy", str(output_path)]
    elif operation == "keyframe":
        timestamp = args.timestamp or args.start_time or (positional[2] if len(positional) > 2 else None) or value(input_data, "timestamp", "start_time")
        output_path = output_path or (positional[3] if len(positional) > 3 else None)
        if timestamp is None or output_path is None:
            return emit(failed("MISSING_ARGUMENT", "keyframe requires timestamp and output_path"), args.output)
        command = ["ffmpeg", "-y", "-ss", str(timestamp), "-i", str(input_path), "-frames:v", "1", "-q:v", "2", str(output_path)]
    else:
        return emit(failed("UNKNOWN_OPERATION", f"Unknown operation: {operation}"), args.output)

    result = run_ffmpeg(command)
    if result.get("status") == "success":
        result["data"]["operation"] = operation
        result["data"]["output_path"] = str(output_path)
    return emit(result, args.output)


if __name__ == "__main__":
    raise SystemExit(main())
