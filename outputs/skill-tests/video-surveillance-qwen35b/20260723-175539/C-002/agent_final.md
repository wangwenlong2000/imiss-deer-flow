以下是从 `Trafic.mp4` 中提取并整理的标准输入数据：

```json
{
  "metadata": {
    "file_path": "/mnt/user-data/uploads/Trafic.mp4",
    "file_size_bytes": 75965424,
    "file_size_mb": 72.4,
    "format_name": "mov,mp4,m4a,3gp,3g2,mj2",
    "major_brand": "isom",
    "encoder": "Lavf59.27.100"
  },
  "duration": {
    "seconds": 900.916,
    "minutes": 15.015,
    "hhmmss": "00:15:00.916"
  },
  "resolution": {
    "width": 1280,
    "height": 674,
    "aspect_ratio": "16:9 (approx)",
    "total_pixels": 862720
  },
  "frame_rate": {
    "display_fps": 12,
    "avg_fps_approx": 11.999,
    "time_base": "1/16000",
    "total_frames": 10811
  },
  "codec": {
    "video_codec": "h264",
    "profile": "High",
    "level": 40,
    "pix_fmt": "yuv420p",
    "color_space": "bt709",
    "color_range": "tv"
  },
  "bitrate": {
    "video_bitrate_bps": 672879,
    "overall_bitrate_bps": 674561,
    "estimated_duration_seconds": 900.916
  },
  "streams": [
    {
      "index": 0,
      "type": "video",
      "codec": "h264",
      "has_audio": false
    }
  ]
}
```

**关键参数汇总：**

| 字段 | 值 |
|------|-----|
| 时长 | 900.916 秒（约15分钟） |
| 分辨率 | 1280×674 |
| 帧率 | 12 fps |
| 总帧数 | 10,811 帧 |
| 视频编码 | H.264 High Profile |
| 码率 | ~674 kbps |
| 色彩空间 | BT.709, YUV 4:2:0 |
| 音轨 | 无（纯视频流） |

该文件为纯视频MP4，无音频轨道。后续分析中如需进行目标检测或追踪，建议设置合适的帧采样间隔（如每N帧取一帧）以控制计算开销。