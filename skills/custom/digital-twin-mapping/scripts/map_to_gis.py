import os
import sys
import random
import json
import argparse

def map_video_to_gis(video_id, x, y):
    """
    仿真 2D 视频坐标到 3D GIS 坐标的映射。
    """
    print(f"🌍 正在执行数字孪生坐标映射: {video_id} (X:{x}, Y:{y})")
    
    # 模拟经纬度转换逻辑 (基于某个城市中心点)
    base_lat = 39.9042
    base_lng = 116.4074
    
    # 根据 X, Y 产生微小的坐标偏移
    lat = base_lat + (y / 100000)
    lng = base_lng + (x / 100000)
    
    return {
        "video_id": video_id,
        "input_pixel": {"x": x, "y": y},
        "mapped_coordinates": {
            "latitude": round(lat, 6),
            "longitude": round(lng, 6),
            "altitude": 0.0,
            "crs": "WGS84"
        },
        "location_name": "长安街与东单交叉口"
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("video_id", help="视频标识")
    parser.add_argument("x", type=int, help="X 坐标")
    parser.add_argument("y", type=int, help="Y 坐标")
    args = parser.parse_args()
    
    result = map_video_to_gis(args.video_id, args.x, args.y)
    print(json.dumps(result, indent=2, ensure_ascii=False))
