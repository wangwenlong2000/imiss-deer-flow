#!/usr/bin/env python3
import sys
import os
import json
import subprocess

def run_ffmpeg(command):
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        return {"success": True, "stdout": result.stdout}
    except subprocess.CalledProcessError as e:
        return {"success": False, "error": e.stderr}

def extract_segment(input_path, start_time, duration, output_path):
    """截取视频片段"""
    cmd = [
        "ffmpeg", "-y", "-ss", str(start_time), "-i", input_path,
        "-t", str(duration), "-c", "copy", output_path
    ]
    return run_ffmpeg(cmd)

def extract_keyframe(input_path, timestamp, output_path):
    """提取特定时间点的关键帧"""
    cmd = [
        "ffmpeg", "-y", "-ss", str(timestamp), "-i", input_path,
        "-frames:v", "1", "-q:v", "2", output_path
    ]
    return run_ffmpeg(cmd)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(json.dumps({"success": False, "error": "缺少操作指令: segment | keyframe"}))
        sys.exit(1)

    op = sys.argv[1]
    
    if op == "segment":
        res = extract_segment(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    elif op == "keyframe":
        res = extract_keyframe(sys.argv[2], sys.argv[3], sys.argv[4])
    else:
        res = {"success": False, "error": "未知操作"}

    print(json.dumps(res))
