---
name: camsnap
description: 直接抓取网络摄像头或视频流的单帧快照图像并回传。
version: 0.1.0
author: Gemini
compatibility: ">=2.0.0"
allowed-tools:
  - bash
  - read_file
  - write_file
---

# 抓拍快照技能 (CamSnap Skill)

## 技能说明
`camsnap` 技能允许 Agent 通过视频流（如 RTSP, HTTP 或本地摄像头设备）抓取单帧快照图像。

## 触发关键词
- "快照", "抓拍", "截图", "camsnap", "查看现场"

## 权限声明
- `bash`: 用于执行 `python3 /mnt/skills/custom/camsnap/scripts/camsnap.py`。
- `read_file`: 读取抓拍后的日志或结果。

## 操作规范 (Guidelines)
1. **工作流程**：
   - 接收用户的抓拍请求，识别视频源路径（通常由内置数据集或外部 URL 提供）。
   - 调用脚本进行抓拍：`python3 /mnt/skills/custom/camsnap/scripts/camsnap.py <source_url>`。
   - 解析脚本返回的 JSON 结果，提取 `filename`。
2. **输出规范**：
   - 必须告知用户抓拍文件的名称。
   - 抓拍的图像将保存在 `/mnt/user-data/outputs/`，前端会自动识别并呈现。

## 示例 (Example)
**用户：** "抓拍一下 RTSP 地址为 rtsp://admin:123456@192.168.1.100 的现场图像。"
**Agent 动作：**
- 执行：`python3 /mnt/skills/custom/camsnap/scripts/camsnap.py rtsp://admin:123456@192.168.1.100`
- 返回结果中的 `filename: camsnap_20260420_143000.jpg`。
- 回复用户： "已为您完成现场抓拍，文件名为 camsnap_20260420_143000.jpg。您可以在侧边栏查看该图像。"
