## 测试结果

**`/mnt/user-data/outputs/privacy-masking/result.json` 内容：**

```json
{
  "skill": "privacy-masking",
  "version": "1.0.0",
  "status": "success",
  "confidence": 1.0,
  "data": {
    "uri": "/mnt/user-data/workspace/privacy-masking/source_masked.jpg",
    "privacy_masked": true,
    "masked_regions": [
      {
        "type": "face",
        "bbox": [
          20,
          20,
          180,
          160
        ]
      }
    ],
    "method": "gaussian_blur"
  }
}
```

**结论：**

✅ **测试成功**。`privacy-masking` skill 真链路执行正常：
- SKILL.md 已读取
- ffmpeg-utils 成功从视频提取关键帧
- privacy-masking 成功对指定人脸区域 (bbox: [20,20,180,160]) 应用高斯模糊
- 输出文件 `/mnt/user-data/outputs/privacy-masking/result.json` 已生成，包含 masked URI、掩码状态和处理方法