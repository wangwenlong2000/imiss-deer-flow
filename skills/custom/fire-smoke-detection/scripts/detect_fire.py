import os
import sys
import random
import json
import argparse

def detect_fire_smoke(input_path):
    """
    仿真烟火灾害检测。
    """
    print(f"🔥 正在启动烟火巡查: {input_path}")
    
    if not os.path.exists(input_path):
        return {"error": "File not found"}

    # 模拟检测逻辑
    # 我们随机生成一种状态，方便测试 AI 的回复逻辑
    is_fire = random.choice([True, False, False, False]) # 25% 概率着火
    
    result = {
        "is_fire": is_fire,
        "is_smoke": is_fire, # 通常有火就有烟
        "confidence": round(random.uniform(0.8, 0.99), 2) if is_fire else 0.05,
        "location_in_frame": "Upper-right corner" if is_fire else "none"
    }

    return result

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="视频路径")
    args = parser.parse_args()
    
    result = detect_fire_smoke(args.input)
    print(json.dumps(result, indent=2, ensure_ascii=False))
