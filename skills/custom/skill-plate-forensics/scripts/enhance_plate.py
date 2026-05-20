import sys
import os
import json
import subprocess

def run_cmd(cmd):
    try:
        subprocess.run(cmd, check=True, capture_output=True)
        return True
    except:
        return False

def enhance_plate(input_path, output_path):
    """
    针对性增强车牌区域：
    1. 灰度化。
    2. 局部对比度归一化。
    3. 两次不同权重的锐化。
    4. 边缘增强。
    """
    # 使用 ImageMagick 进行多链式处理
    cmd = [
        "convert", input_path,
        "-colorspace", "gray",
        "-sigmoidal-contrast", "10,50%",
        "-unsharp", "0x5+1.5+0.02",
        "-adaptive-sharpen", "0x2",
        "-modulate", "100,100",
        output_path
    ]
    
    if run_cmd(cmd):
        return {"success": True, "output_path": output_path}
    else:
        return {"success": False, "error": "ImageMagick 处理失败"}

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(json.dumps({"success": False, "error": "用法: <input> <output>"}))
        sys.exit(1)

    res = enhance_plate(sys.argv[1], sys.argv[2])
    print(json.dumps(res))
