# Progress

## 2026-05-12
- 初始化任务计划，目标是增加宿主机文件访问服务。
- 已确认现有 artifacts 路由仅支持线程虚拟路径，不能直接服务 /mnt/nas 或项目 datasets。
- 已确认前端 Markdown 链接不会拦截普通 a 标签，可直接打开 API 链接。
- 已新增 gateway 路由 /api/host-files/{path:path}，限制为 /mnt/nas 和宿主机 datasets 根目录。
- 已更新前端 Markdown 链接重写逻辑：当 href 指向允许的绝对路径时，自动改写到 /api/host-files。
- 已更新 docker compose 与 nginx 配置，使 gateway/langgraph 可读 datasets 且 nginx 可代理新接口。
- 已运行 backend/tests/test_host_files_router.py，3 个用例全部通过；同时检查新增后端与前端文件均无诊断错误。
- 已在 docs 目录新增《文件访问服务说明》，说明实现方式、修改文件、容器挂载关系和使用示例。
- 已更新 location-matcher skill 中的原图链接生成规则：不再硬编码 localhost，统一改为相对路径 `/api/host-files<source_path>`。
