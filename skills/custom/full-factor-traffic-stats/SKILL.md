---
name: full-factor-traffic-stats
description: 全要素交通流量统计，包括车型分类（客/货/轿）、平均车速、车流密度及路口拥堵状态分析。
version: 1.0.0
author: Urban Intelligence Lab
compatibility: ">=2.0.0"
allowed-tools:
  - bash
---

# 全要素交通流量统计技能 (Full-Factor Traffic Statistics)

## 技能说明
该技能为交通管理部门提供量化的底数支撑。它不仅能计数，还能感知交通流的动力学特征。

## 触发关键词
- "交通流量统计", "车速分析", "车型比例", "拥堵评估", "traffic-stats"

## 操作规范
1. **执行统计**：`python3 /mnt/skills/custom/full-factor-traffic-stats/scripts/analyze_traffic.py <video_path>`
2. **多维度输出**：脚本将返回各类车型的数量、平均速度（km/h）及拥堵等级。
