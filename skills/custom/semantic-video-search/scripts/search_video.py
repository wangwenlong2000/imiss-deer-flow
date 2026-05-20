import os
import sys
import random
import json
import argparse

def search_video_content(input_path, query):
    """
    仿真语义视频搜索。
    """
    print(f"🔍 正在检索视频内容... (目标: {query})")
    
    if not os.path.exists(input_path):
        return {"error": "File not found"}

    # 仿真搜索逻辑
    # 如果关键词包含 "red" 或 "红"，我们返回一个虚假的匹配
    matches = []
    
    # 模拟匹配两个时间点
    for i in range(2):
        matches.append({
            "timestamp": f"00:00:{random.randint(10, 50)}",
            "confidence": round(random.uniform(0.65, 0.95), 2),
            "description": f"检测到与 '{query}' 语义高度匹配的目标特征"
        })

    return {
        "query": query,
        "matches": matches,
        "search_engine": "CLIP-based-Sim"
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="视频路径")
    parser.add_argument("query", help="搜索词")
    args = parser.parse_args()
    
    result = search_video_content(args.input, args.query)
    print(json.dumps(result, indent=2, ensure_ascii=False))
