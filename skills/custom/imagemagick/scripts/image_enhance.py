import sys
import os
import json
import subprocess

def run_convert(args):
    cmd = ["convert"] + args
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return {"success": True}
    except subprocess.CalledProcessError as e:
        return {"success": False, "error": e.stderr}

def enhance_for_surveillance(input_path, output_path, mode="auto"):
    """
    针对监控画面进行优化。
    auto: 自动对比度与锐化
    night: 针对夜间画面提亮
    """
    if mode == "night":
        # 提亮并增加对比度
        args = [input_path, "-modulate", "150,120", "-contrast-stretch", "1x1%", output_path]
    else:
        # 自动调优与锐化
        args = [input_path, "-auto-level", "-sharpen", "0x1", output_path]
    
    return run_convert(args)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(json.dumps({"success": False, "error": "用法: <input> <output> [mode]"}))
        sys.exit(1)

    input_f = sys.argv[1]
    output_f = sys.argv[2]
    mode = sys.argv[3] if len(sys.argv) > 3 else "auto"

    res = enhance_for_surveillance(input_f, output_f, mode)
    print(json.dumps(res))
