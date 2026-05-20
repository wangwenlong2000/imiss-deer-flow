import os
import sys
import random
import json
import argparse

def calculate_crowd_density(input_path):
    """
    仿真人群密度分析。
    """
    print(f"👥 正在启动人群密度分析: {input_path}")
    
    if not os.path.exists(input_path):
        return {"error": "File not found"}

    # 仿真检测结果
    num_people = random.randint(10, 200)
    area_sqm = 40.0 # 假设分析区域为 40 平米
    density = round(num_people / area_sqm, 2)
    
    status = "Comfortable"
    if density > 4.0:
        status = "Danger"
    elif density > 2.0:
        status = "Crowded"

    return {
        "person_count": num_people,
        "analyzed_area_sqm": area_sqm,
        "density_per_sqm": density,
        "status": status,
        "recommendation": "Normal monitoring" if status == "Comfortable" else "Dispatch security for crowd control"
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="视频路径")
    args = parser.parse_args()
    
    result = calculate_crowd_density(args.input)
    print(json.dumps(result, indent=2, ensure_ascii=False))
