import os
import sys
import random
import json
import argparse

def detect_behavior_anomaly(input_path):
    """
    仿真异常行为识别。
    """
    print(f"🕵️ 正在启动行为风险识别: {input_path}")
    
    if not os.path.exists(input_path):
        return {"error": "File not found"}

    # 模拟异常行为列表
    anomalies = [
        {"type": "Fall", "description": "疑似人员摔倒"},
        {"type": "Fighting", "description": "疑似肢体冲突"},
        {"type": "Loitering", "description": "长时间徘徊"},
        {"type": "Climbing", "description": "翻越围栏"}
    ]
    
    # 50% 概率检测到异常
    found = random.choice([True, False])
    
    if found:
        anomaly = random.choice(anomalies)
        return {
            "anomaly_detected": True,
            "type": anomaly["type"],
            "description": anomaly["description"],
            "confidence": round(random.uniform(0.7, 0.95), 2),
            "timestamp": f"00:00:{random.randint(5, 55)}"
        }
    else:
        return {"anomaly_detected": False, "type": "Normal", "description": "正常行为"}

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="视频路径")
    args = parser.parse_args()
    
    result = detect_behavior_anomaly(args.input)
    print(json.dumps(result, indent=2, ensure_ascii=False))
