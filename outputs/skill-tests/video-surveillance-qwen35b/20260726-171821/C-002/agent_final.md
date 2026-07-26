视频元数据提取完成。以下是标准化整理结果：

## Trafic.mp4 视频元数据

| 字段 | 值 |
|------|-----|
| **文件名** | Trafic.mp4 |
| **文件大小** | 75,965,424 bytes (~72.4 MB) |
| **时长** | 900.916 秒 (14分59秒) |
| **分辨率** | 1280 × 674 像素 |
| **帧率** | 12 fps (实际平均帧率 11.995 fps) |
| **总帧数** | 10,811 帧 |
| **视频编码** | H.264 / AVC High Profile |
| **像素格式** | yuv420p |
| **色彩空间** | BT.709 |
| **颜色范围** | TV (Limited, 16-235) |
| **比特率** | 674,561 bps (~658 kbps) |
| **容器格式** | MP4 (isom) |
| **编码器** | Lavf59.27.100 (Bilibili XCoder v2.0.2) |

### 关键分析参数

```json
{
  "video_path": "/mnt/user-data/uploads/Trafic.mp4",
  "duration_seconds": 900.916,
  "resolution": {"width": 1280, "height": 674},
  "frame_rate": 12.0,
  "total_frames": 10811,
  "codec": "h264",
  "profile": "High",
  "pix_fmt": "yuv420p",
  "color_space": "bt709",
  "bitrate_bps": 674561,
  "file_size_bytes": 75965424,
  "time_base": "1/16000",
  "stream_count": 1,
  "has_audio": false
}
```

### 备注

1. **单视频流**：该视频仅包含视频轨道，无音频流。
2. **低帧率**：12fps 属于监控类视频的常见帧率，适合目标检测与跟踪任务。
3. **非标准分辨率**：1280×674 是自定义裁剪分辨率（非标准 720p），可能经过后期处理或截取。
4. **TV Range**：色彩范围为 TV (Limited)，这是摄像头原始数据的典型特征，而非电影级的 Full Range (0-255)。