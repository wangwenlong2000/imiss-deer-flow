---
name: digital-twin-mapping
description: 将监控视频中的事件（如事故、拥堵）实时映射到城市的 3D 数字孪生（GIS）地图上。
version: 1.0.0
author: Urban Intelligence Lab
compatibility: ">=2.0.0"
allowed-tools:
  - bash
---

# 3D 场景数字孪生映射技能 (Digital Twin Mapping)

## 技能说明
该技能实现了物理世界与数字世界的同步。它将 2D 视频坐标转换为真实的经纬度坐标（WGS84），并反馈给指挥中心。

## 触发关键词
- "地图映射", "地理位置", "他在地图哪里", "三维定位", "gis-mapping"

## 操作规范
1. **执行映射**：`python3 /mnt/skills/custom/digital-twin-mapping/scripts/map_to_gis.py <video_id> <x_coord> <y_coord>`
2. **坐标转换**：脚本将利用预设的单目视觉标定参数，返回精确的经纬度。
