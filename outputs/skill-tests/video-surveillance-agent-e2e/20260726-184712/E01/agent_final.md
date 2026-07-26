视频元数据已成功提取，以下是整理后的标准输入格式：

## 视频文件：Trafic-30s.mp4

| 字段 | 值 | 类型 |
|---|---|---|
| **时长** | 30.0 秒 | `video_duration_seconds` |
| **分辨率** | 1280 × 674 像素 | `width` / `height` |
| **帧率** | 12.0 fps | `fps` |
| **编码格式** | H.264 (AVC) | `codec` |
| 像素格式 | yuv420p | `pixel_format` |
| 封装容器 | MP4 | `container_format` |
| 总帧数 | 360 帧 | `frame_count` |
| 码率 | ~409 kbps | `bit_rate` |
| 文件大小 | 1,533,354 字节 (~1.46 MB) | `file_size_bytes` |

### 下游分析可直接使用的 JSON 配置

已将标准化结果保存至 `/mnt/user-data/workspace/traffic-video-metadata.json`，核心结构如下：

```json
{
  "camera_id": "CAM_DEERFLOW_001",
  "source_type": "local_file",
  "file_status": "ok",
  "video_session_id": "VS_bdfd29e7",
  "raw_segment_uri": "/mnt/user-data/uploads/Trafic-30s.mp4",
  "duration_seconds": 10,
  "video_duration_seconds": 30.0,
  "width": 1280,
  "height": 674,
  "resolution": "1280x674",
  "fps": 12.0,
  "codec": "h264",
  "codec_long_name": "H.264 / AVC / MPEG-4 AVC / MPEG-4 part 10",
  "pixel_format": "yuv420p",
  "container_format": "mov,mp4,m4a,3gp,3g2,mj2",
  "bit_rate": 408894,
  "frame_count": 360,
  "file_size_bytes": 1533354
}
```

**关键提示：** 该视频分辨率为 1280×674（非标准 16:9），帧率为 12fps（低于常见的 25/30fps）。如果后续进行目标检测或事件分析，建议根据算法对低帧率视频的适配情况适当调整采样策略。