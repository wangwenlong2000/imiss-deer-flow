# 文档与资料目录

按用途查找开发文档、操作指南、测试报告和项目材料。

## 常用入口

- [Linux amd64 rc4 配置与安装指南](operations/Linux-amd64-rc4-配置安装指南.md)
- [基础版安装、备份与恢复](installation/README.md)
- [当前 rc4 交付验收记录](acceptance/20261127-rc4.md)
- [跨机器迁移与 Docker 部署指南](跨机器迁移与Docker部署指南_20261126.md)
- [OneCityAgent 统一项目说明](design/integration-onecity-20261125.md)
- [项目环境说明](project-materials/项目环境说明_外部验收_20261126.md)
- [合规检测器接入指南](compliance/compliance-detector-integration-guide.md)

## 按用途分类

| 分类 | 内容 |
|---|---|
| [安装与运维](operations/README.md) | 服务启动、沙箱、模型服务、端口、离线资源、日常维护 |
| [架构与集成](design/README.md) | 项目架构、OneCity 整合、网络流量 RAG 方案 |
| [合规检测](compliance/README.md) | 检测器接入、契约、策略、交接、标注指南与 PDF |
| [Skill 与路由](skills/README.md) | 接口规范、路由设计、开发分工、依赖盘点与 Excel |
| [测试与评测](testing/README.md) | 流程、意图、移动平台、视频测试报告及原始评测目录 |
| [历史记录](history/README.md) | 并发、权限、修复过程、发布记录和笔记 |
| [项目资料](project-materials/README.md) | 环境说明、Word、PPT、海报、演示与录制材料 |

## 其他目录的职责

| 路径 | 内容与入口 |
|---|---|
| `apps/`、`backend/`、`frontend/`、`services/` | 应用与服务代码；各组件的 README 保留在源码旁 |
| `skills/`、`contracts/` | 可执行技能与结构化契约；文档导航见 [Skill 目录](skills/README.md) |
| `deploy/`、`docker/`、`scripts/`、`tools/` | 打包、部署、运维与开发工具；操作文档见 [运维目录](operations/README.md) |
| `config/`、`datasets/`、`models/`、`logs/` | 配置、数据、模型与运行日志，保留原有运行路径 |
| [outputs/](../outputs/README.md) | 交付证据、并发和性能实验、合规结果、汇报与录制产物 |
| [合规检测/](../合规检测/README.md) | 十场景标注语料、类型 3/6/7 整改数据、预测与评测 |
| [docs/releases/](releases/) | 版本发布记录和历史发布清单 |
| [docs/acceptance/](acceptance/) | 各版交付验收记录；rc4 范围以具体记录为准 |
| [docs/skill-dependencies-20261126/](skill-dependencies-20261126/README.md) | 依赖盘点的结构化元数据与原始证据 |

## 固定路径与整理记录

以下文件保留在 `docs/` 一级，供发布清单、Schema 导出或部署说明引用：

- [项目架构整理与易安装改造计划](项目架构整理与易安装改造计划_20261126.md)
- [跨机器迁移与 Docker 部署指南](跨机器迁移与Docker部署指南_20261126.md)
- [Skill 依赖版本盘点](Skill依赖版本盘点_20261126.md)
- [检测器契约 Schema](compliance-detector-contract.schema.json)
- [无 sudo / Docker 部署依赖清单](no-sudo-no-docker.requirements.txt)

原始评测目录、运行配置和交付证据保留原路径。本次目录整理适用于当前工作区；已封版 rc4 发布包保持原样。

完整旧路径对照和检查结果见 [2026-11-27 目录整理记录](history/目录整理记录_20261127.md)。后续新增人读文档请放入对应主题目录，并补充分类索引；机器读取的夹具和原始结果应保留可复现路径。
