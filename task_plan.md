# Task Plan

## Goal
为 DeerFlow 增加宿主机文件访问服务，使 AI 可基于文件路径生成浏览器可点击链接，访问宿主机上的 /mnt/nas 和 /home/anker/imiss-deer-flow/datasets 文件；环境运行在 Docker 容器中。

## Phases
- [completed] 1. 确认现有文件访问链路与最小改动点
- [completed] 2. 在 gateway 增加白名单宿主路径文件访问路由
- [completed] 3. 更新 docker 挂载与 nginx 转发
- [completed] 4. 做最小验证并补充使用说明

## Hypothesis
前端消息区已经支持普通 Markdown 链接；只要 gateway 提供一个受限白名单的文件访问 API，并让 nginx 转发该路由，同时把 datasets 目录挂载进相关容器，AI 输出的链接就能在浏览器中直接打开文件。

## Validation
- 后端新增路由测试，覆盖白名单路径访问与越权拒绝
- 检查 nginx 配置已将新 API 转发到 gateway
- 检查 docker compose 已挂载 datasets 与 /mnt/nas

## Usage
- AI 可直接输出指向原始绝对路径的 Markdown 链接，例如 [打开文件](/mnt/nas/demo/report.pdf)
- 前端会自动把允许的绝对路径重写为 /api/host-files/...，由 gateway 负责受限读取
