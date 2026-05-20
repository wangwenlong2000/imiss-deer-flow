---
name: evidence-package-builder
description: 证据生成，输出截图、短视频片段、事件时间线、目标轨迹和结构化描述，支持派单、执法和复核追溯。
version: 1.0.0
author: Urban Intelligence Lab
compatibility: ">=2.0.0"
allowed-tools:
  - bash
  - write_file
---

# 证据生成

## 定位

本 skill 将告警或关联 case 打包成可追溯证据。真实环境中应调用 ffmpeg 截图和切片；当前实现会生成结构化证据清单和时间线 JSON。

## 触发关键词

- 证据包
- 截图
- 短视频
- 事件时间线
- 目标轨迹
- 执法取证
- 复核材料

## 脚本入口

```bash
python3 /mnt/skills/custom/evidence-package-builder/scripts/build_evidence_package.py <case_or_alert_json> --output-dir /mnt/user-data/outputs
```

## 输出要求

输出 `package_id`、`timeline`、`artifacts`、`structured_description`、`traceability`。证据文件必须写入用户可访问输出目录。
