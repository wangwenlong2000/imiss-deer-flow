import os
import sys
import random
import json
import argparse

def detect_urban_violations(input_path):
    """
    仿真城市违章检测。
    """
    print(f"🏘️ 正在启动城市违章行为巡检: {input_path}")
    
    if not os.path.exists(input_path):
        return {"error": "File not found"}

    # 模拟违章场景
    violation_types = ["非法占道经营", "非机动车进入机动车道", "乱丢垃圾", "违章停放"]
    
    # 随机生成 0-3 个违章
    found_violations = []
    num_v = random.randint(0, 3)
    
    for _ in range(num_v):
        v_type = random.choice(violation_types)
        timestamp = f"00:00:{random.randint(10, 50)}"
        found_violations.append({
            "type": v_type,
            "timestamp": timestamp,
            "confidence": round(random.uniform(0.75, 0.98), 2),
            "evidence_frame": f"/mnt/user-data/outputs/violation_{random.randint(100,999)}.jpg"
        })

    return {
        "status": "Success",
        "violations_found": len(found_violations),
        "data": found_violations
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="视频路径")
    args = parser.parse_args()
    
    result = detect_urban_violations(args.input)
    print(json.dumps(result, indent=2, ensure_ascii=False))
