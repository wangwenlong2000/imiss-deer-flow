import cv2
import sys
import os
import json

def detect_motion(video_path, threshold=500, min_area=500):
    """
    使用背景减除法检测视频中的运动。
    返回存在显著运动的时间戳列表。
    """
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        return {"success": False, "error": "无法打开视频"}

    fps = cap.get(cv2.CAP_PROP_FPS)
    fgbg = cv2.createBackgroundSubtractorMOG2(history=500, varThreshold=16, detectShadows=True)
    
    motion_events = []
    frame_idx = 0
    is_motion_active = False
    start_time = 0

    while True:
        ret, frame = cap.read()
        if not ret:
            break
        
        frame_idx += 1
        timestamp = frame_idx / fps
        
        # 预处理
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        fgmask = fgbg.apply(gray)
        
        # 降噪
        kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (3, 3))
        fgmask = cv2.morphologyEx(fgmask, cv2.MORPH_OPEN, kernel)
        
        # 计算白色像素面积（运动程度）
        contours, _ = cv2.findContours(fgmask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        
        motion_detected = any(cv2.contourArea(c) > min_area for c in contours)
        
        if motion_detected and not is_motion_active:
            is_motion_active = True
            start_time = timestamp
        elif not motion_detected and is_motion_active:
            is_motion_active = False
            duration = timestamp - start_time
            if duration > 0.5: # 忽略极短的抖动
                motion_events.append({
                    "start": round(start_time, 2),
                    "end": round(timestamp, 2),
                    "duration": round(duration, 2)
                })
        
        # 性能优化：每 5 帧处理一次，或者对长视频进行跳帧
        # cap.set(cv2.CAP_PROP_POS_FRAMES, frame_idx + 2)

    cap.release()
    return {"success": True, "events": motion_events, "total_events": len(motion_events)}

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(json.dumps({"success": False, "error": "缺少参数: <video_path>"}))
        sys.exit(1)

    video_path = sys.argv[1]
    result = detect_motion(video_path)
    print(json.dumps(result, indent=2))
