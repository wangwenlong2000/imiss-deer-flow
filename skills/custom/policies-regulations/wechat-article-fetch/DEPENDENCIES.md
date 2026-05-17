# wechat-article-fetch 环境依赖记录

## 功能
在 DeerFlow 中抓取微信公众号文章，使用 Playwright + Chromium 渲染页面，提取正文并保存为 Markdown。

## 当前部署方式
- DeerFlow 运行在 Docker 容器中
- 本 skill 源代码目录挂载到容器内
- 容器内真实路径：
  `/app/skills/custom/policies-regulations/wechat-article-fetch`

## 已确认的运行环境
- 容器：`gaozhuohui-deer-flow-langgraph`
- 系统：Debian GNU/Linux 13 (trixie)
- Node.js：`/usr/bin/node`
- Node 版本：`v22.22.2`

## Skill 目录内依赖
以下依赖位于 skill 目录内，容器可见：

- `package.json`
- `package-lock.json`
- `node_modules/playwright`
- `node_modules/playwright-core`
- `.playwright-browsers/`
  - `chromium-1217`
  - `chromium_headless_shell-1217`
  - `ffmpeg-1011`

## 容器内系统依赖
为使 Playwright/Chromium 正常启动，需要在 DeerFlow 实际执行 skill 的容器内安装 Linux 系统依赖。

本次在容器内执行并验证通过的安装命令：

```bash
docker exec -it gaozhuohui-deer-flow-langgraph bash
cd /app/skills/custom/policies-regulations/wechat-article-fetch
apt-get update
npx playwright install-deps chromium