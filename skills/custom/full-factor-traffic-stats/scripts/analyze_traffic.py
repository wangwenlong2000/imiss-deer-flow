import os
import sys
import random
import json
import argparse

def analyze_traffic_stats(input_path):
    """
    仿真全要素交通统计。
    """
    print(f"🚗 正在启动全要素交通流量分析: {input_path}")
    
    if not os.path.exists(input_path):
        return {"error": "File not found"}

    # 仿真检测结果
    stats = {
        "vehicle_counts": {
            "sedan": random.randint(50, 200),
            "truck": random.randint(10, 50),
            "bus": random.randint(5, 20),
            "motorcycle": random.randint(20, 100)
        },
        "average_speed_kmh": random.randint(25, 65),
        "occupancy_rate": f"{random.randint(10, 80)}%",
        "congestion_level": "Low"
    }
    
    # 根据速度判定拥堵等级
    if stats["average_speed_kmh"] < 20:
        stats["congestion_level"] = "Critical"
    elif stats["average_speed_kmh"] < 40:
        stats["congestion_level"] = "Moderate"
    else:
        stats["congestion_level"] = "Smooth"

    return stats

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="视频路径")
    args = parser.parse_args()
    
    result = analyze_traffic_stats(args.input)
    print(json.dumps(result, indent=2, ensure_ascii=False))
