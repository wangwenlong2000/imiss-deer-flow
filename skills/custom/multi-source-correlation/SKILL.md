---
name: multi-source-correlation
description: 多源关联分析，将视频告警关联地图、摄像头点位、网格、道路、工单和 IoT 传感器数据。
version: 1.0.0
author: Urban Intelligence Lab
compatibility: ">=2.0.0"
allowed-tools:
  - bash
---

# 多源关联

## 定位

城市超脑不应只看视频。本 skill 将去重后的告警与地图、点位、网格、道路、工单、IoT 传感器等外部数据进行关联，形成可解释的城市事件画像。

## 触发关键词

- 多源关联
- 地图关联
- 网格关联
- 道路关联
- 工单关联
- IoT 传感器
- 告警关联

## 脚本入口

```bash
python3 /mnt/skills/custom/multi-source-correlation/scripts/correlate_sources.py <alerts_json> --context <context_json>
```

## 输出要求

输出 `correlated_cases`，每个 case 包含视频告警、地理网格、道路、相邻摄像头、相关工单和可能的传感器解释。
