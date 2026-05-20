import cv2
import sys
import os
import json
import time
from datetime import datetime

def capture_snapshot(source_url, output_path):
    """
    抓取视频源的单帧快照并保存到指定路径。
    支持 RTSP, HTTP 流或本地设备。
    """
    cap = cv2.VideoCapture(source_url)
    
    if not cap.isOpened():
        return {"status": "error", "message": f"无法打开视频源: {source_url}"}

    # 读取一帧 (通常跳过前几帧以等待自动对焦/曝光稳定)
    for _ in range(5):
        ret, frame = cap.read()
    
    if ret:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"camsnap_{timestamp}.jpg"
        save_path = os.path.join(output_path, filename)
        
        cv2.imwrite(save_path, frame)
        cap.release()
        
        return {
            "status": "success",
            "filename": filename,
            "save_path": save_path,
            "timestamp": timestamp,
            "message": "抓拍成功"
        }
    else:
        cap.release()
        return {"status": "error", "message": "无法从视频流中读取帧"}

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(json.dumps({"status": "error", "message": "缺少参数: <source_url> [output_path]"}))
        sys.exit(1)

    source = sys.argv[1]
    # 默认输出到沙箱的标准输出目录
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "/mnt/user-data/outputs"
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    result = capture_snapshot(source, output_dir)
    print(json.dumps(result))
