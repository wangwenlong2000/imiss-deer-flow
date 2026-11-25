# 街景服务与完整定位流程修复（2026-11-25）

本次发布基于已发布的轻量并发版本 `a92b62dc45d145319b60d04b3092de4b9e7e41a6`，保留其遥感面积、视频时间线、多会话并发和答复展示修复。新增可部署的街景模型服务，补齐真实地址查询、候选审阅、证据生成及网页文件下载流程。

## 当前行为

- 本地服务实现 ImAge4VPR、Qwen3-VL-Embedding-2B 和 Qwen3-VL-Reranker-2B 的 embed/rerank/batch 接口，支持 CUDA；模型版本和下载校验固定在服务目录中。
- 正反地理编码使用 Photon，统一 WGS84；服务错误、无覆盖和歧义返回明确失败。地址反查失败时，仍保留已核验的图库坐标。
- 候选图片从已配置的只读图库复制到当前会话 outputs，生成 retrieval.json；审阅后由 finalize_match.py 生成 evidence.json，无可靠匹配也交付证据。
- 执行检查核对实际候选查看、证据生成及交付记录；最终下载路径必须与已交付文件一致。网页 Markdown 文件链接使用当前会话 artifact 接口。
- 识别纠错区分“禁止联网或安装”和“禁止全部工具”，仍遵守用户明确禁令，不自动开放工具、不增加重试次数。
- Docker 启动脚本读取根目录 .env 中的并发设置，已导出的环境变量优先；源码默认两个执行任务、最多十个运行或等待任务。

## 部署

模型服务安装和启停见 [服务说明](../../services/streetmodel/README.md)。模型权重、独立环境、PID 和日志不随代码提交；安装脚本会下载模型及上游实现，部署方需核对其许可证和资源要求。

1. 由 config.example.yaml 创建本地配置，使用环境变量设置模型与 Elasticsearch 凭证；不要提交填充后的配置。
2. 为沙箱配置 STREETMODEL_BASE_URL。Docker bridge 示例为 http://172.17.0.1:3130，实际地址按部署网络确定。
3. 图服务和沙箱都要能读取候选原图。开发 Compose 默认只读挂载 /mnt/nas/streetview_meta，其他机器须调整宿主路径。也可将本地图库映射为 /mnt/datasets/street/streetview_meta，供缺图回退使用。
4. 配置 STREET_GEOCODING_BASE_URL，默认 https://photon.komoot.io。多个沙箱需挂载同一个可写限频目录，并设置 STREET_GEOCODING_CACHE_DIR；该目录只存请求锁及限频时间，地址缓存按会话隔离。
5. 更改挂载、命令或环境后，在已有任务结束时重新创建对应容器并使用新会话；普通 restart 不会更新容器配置。

Photon 公共服务仅适合合理请求量，生产容量和可用性需自行安排；本应用按约每 1.1 秒最多一次新请求访问地图端点，成功响应缓存 24 小时。地址指附近地图对象，不承诺精确门牌；输出包含 © OpenStreetMap contributors 和数据归属链接。

## 验证

此前真实部署验证包括：六张库内原图 Top-1 检索回自身、实际模型维度与旧向量比较、东京塔及东京站真实地址查询，以及两次完整聊天（库内图定位、无可靠匹配处理）。最终聊天中的候选均实际查看，图片和 JSON 下载均返回 HTTP 200。原始会话、调用记录及部署元数据留在本地。

这些受控样例不能用于推导任意照片定位准确率。旧服务的确切检查点和预处理仍未完全恢复；当前显式采用官方 ImAge_Merged.pth，向量维度相同并不能单独证明与任意已有索引兼容。

本次发布快照重新验证：后端相关回归 161 项、模型服务接口 10 项、前端渲染 8 项通过；前端 TypeScript 与改动文件 ESLint 检查通过。检查在隔离的临时源码目录使用已有依赖完成，未调用外部模型或修改运行服务。

可复验的离线检查：

```bash
# 使用已安装项目依赖的后端 Python 环境，在 backend/ 下运行
python -m pytest -q tests/test_structured_intent.py tests/test_graph_admission.py tests/test_scene_service_fixes.py tests/test_street_review_guard.py ../skills/custom/street_view_image/location-matcher/tests/test_street_workflow.py

# 模型服务环境；使用测试替身，不加载 CUDA 权重
.runtime/streetmodel/venv/bin/python -m pytest -q services/streetmodel/tests

# frontend/ 下运行
node --experimental-strip-types --test src/components/workspace/messages/markdown-content.test.ts src/components/workspace/messages/message-list.test.ts
pnpm typecheck
```

本次发布不含任务书、进展演示文件、原始图库、视频、模型权重、填充后的运行配置、API 密钥、原始模型记录和会话标识。依赖未发布原视频或历史代码备份的本地集成测试与耗时对照脚本也不纳入本次新增文件。三个历史街景测试说明中的固定认证示例替换为环境变量；这不会清除已有提交历史。
