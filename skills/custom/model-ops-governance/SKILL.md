---
name: model-ops-governance
description: 运维与模型治理，覆盖模型版本、场景适配、误报样本回流、阈值调优和性能监控。
version: 1.0.0
author: Urban Intelligence Lab
compatibility: ">=2.0.0"
allowed-tools:
  - bash
---

# 运维与模型治理

## 定位

上线后的核心不是单次识别，而是持续治理。本 skill 跟踪模型版本、场景适配、误报反馈、阈值调优和运行性能，帮助维护城市超脑的长期可用性。

## 触发关键词

- 模型治理
- 模型版本
- 误报回流
- 阈值调优
- 性能监控
- 场景适配
- 模型运维

## 脚本入口

```bash
python3 /mnt/skills/custom/model-ops-governance/scripts/govern_models.py <feedback_or_metrics_json> --model-name object-detector
```

## 输出要求

输出 `model_registry`、`performance_summary`、`threshold_recommendations`、`feedback_queue`、`deployment_risk`。
