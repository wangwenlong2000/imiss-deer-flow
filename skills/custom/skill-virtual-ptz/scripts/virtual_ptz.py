import cv2
import sys
import os
import json

def virtual_ptz(input_path, output_path, x_center, y_center, zoom_factor=2.0):
    """
    模拟云台缩放。
    x_center, y_center: 0.0 - 1.0 之间的比例坐标。
    zoom_factor: 缩放倍数。
    """
    img = cv2.imread(input_path)
    if img is None:
        return {"success": False, "error": "无法读取图片"}
    
    h, w = img.shape[:2]
    
    # 计算裁剪窗口大小
    new_w = int(w / zoom_factor)
    new_h = int(h / zoom_factor)
    
    # 计算起始坐标
    x = int(w * x_center - new_w / 2)
    y = int(h * y_center - new_h / 2)
    
    # 边界限制
    x = max(0, min(x, w - new_w))
    y = max(0, min(y, h - new_h))
    
    # 裁剪并放大回原始尺寸（模拟光学缩放感）
    cropped = img[y:y+new_h, x:x+new_w]
    result = cv2.resize(cropped, (w, h), interpolation=cv2.INTER_CUBIC)
    
    cv2.imwrite(output_path, result)
    return {"success": True, "output_path": output_path, "metadata": {"center": [x_center, y_center], "zoom": zoom_factor}}

if __name__ == "__main__":
    if len(sys.argv) < 5:
        print(json.dumps({"success": False, "error": "用法: <input> <output> <x_center> <y_center> [zoom]"}))
        sys.exit(1)

    input_f = sys.argv[1]
    output_f = sys.argv[2]
    xc = float(sys.argv[3])
    yc = float(sys.argv[4])
    zoom = float(sys.argv[5]) if len(sys.argv) > 5 else 2.0

    res = virtual_ptz(input_f, output_f, xc, yc, zoom)
    print(json.dumps(res))
