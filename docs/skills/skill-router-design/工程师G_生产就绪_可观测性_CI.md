# 工程师 G：生产就绪、可观测性、CI/CD、Gateway API

> 本文档为工程师 G 的实施方案。
> 工程师 G 在 A～F 已完成工作的基础上，补齐生产级能力。

---

## 0. 最新版 PRD 查阅位置

请先打开最新版 PRD：

```text
/home/wwl/imiss-deer-flow-main/docs/SKILL_ROUTER_PRD.md
```

本文档中提到的 PRD 章节，均指该文件中的章节。

---

## 1. 背景：A～F 已完成的工作

| 工程师 | 完成情况 |
|---|---|
| A | Schema、Router Card JSON Schema、配置模块 |
| B | Router Card 自动生成、Registry 构建 |
| C | Embedding/Reranker 客户端、ES Store、ES 索引构建 |
| D | SkillRouterMiddleware、query segmenter、resolver |
| E | Agent 集成、ThreadState 扩展、Todo 对接、Skill Loader 过滤 |
| F | Skill Creator 联动、增量索引更新、冲突检测、测试 |

**当前已实现的能力**：
- `make build-skill-router-index` 可构建 ES 索引
- `make update-skill-router-index` 可更新单个 Skill
- `make check-skill-router-conflicts` 可检测路由冲突
- `make test-skill-router` 可运行单元测试
- SkillRouterMiddleware 在 Agent middleware chain 中可查询 ES 路由

**尚未实现的能力（G 的任务）**：
- 端到端评估脚本（`eval_skill_router.py`）
- Gateway API 暴露路由状态和刷新接口
- docker-compose 集成 SkillRouter 模型服务
- 可观测性：路由命中率、延迟、错误率
- CI/CD 自动化验证

---

## 2. 分支

当前集成分支状态：

```bash
git checkout feature/skill-router-integration
git pull origin feature/skill-router-integration

git checkout -b feature/skill-router-production-ready
```

---

## 3. 必须阅读的 PRD 章节

| PRD 章节 | 阅读目的 |
|---|---|
| 第 17 章 验证方案 | G 需要实现端到端评估 |
| 第 19 章 风险控制 | 生产级风险项 |
| 第 20 章 最终交付物 | G 负责补齐未交付项 |
| 第 4 章 总体方案 | 在线服务架构 |
| 第 6 章 ES 索引设计 | ES 索引边界和 mapping |

---

## 4. G 的任务清单

### 任务 G1：端到端评估脚本 `eval_skill_router.py`

#### 4.1 目标

实现 `scripts/eval_skill_router.py`，对应 Makefile 中已注册但尚未实现的 `make eval-skill-router`。

#### 4.2 功能

```python
# eval_skill_router.py

测试用例集（内置 + 可扩展）：
1. 已知 query → 预期命中 Skill
   - "帮我分析这个 pcap 文件有没有异常通信" → network-traffic-analysis
   - "查一下相关法律条文并判断这个台账是否合规" → law-regulations-rag
   - "你好" → trigger=false，不命中任何 Skill
   - "在吗" → trigger=false
   - "谢谢" → trigger=false

2. 多场景 query
   - "分析 pcap 异常，并查法律条文判断台账合规"
     → 同时命中 network-traffic-analysis + law-regulations-rag

3. 模糊 query
   - "这个数据有问题" → 应触发但不命中特定 Skill（验证 should_route 泛化）

4. 文件引用 query
   - 有 uploaded_files 时，即使 query 是"帮我看看"也应 trigger=true

输出格式：
{
    "total_cases": N,
    "passed": M,
    "failed": K,
    "details": [
        {"query": "...", "expected": [...], "actual": [...], "status": "PASS|FAIL"},
        ...
    ]
}
```

#### 4.3 运行方式

```bash
make eval-skill-router
# 等价于：
python scripts/eval_skill_router.py
```

#### 4.4 要求

- 依赖在线服务：Embedding（7800）、Reranker（7801）、ES（3128）
- 可离线模式运行：跳过 Embedding/Reranker 调用，仅测试 should_route 和 segment_query
- 测试结果输出 JSON + 可读文本两种格式

---

### 任务 G2：Gateway API 路由管理端点

#### 5.1 目标

在 Gateway API 中新增 `/api/skill-router` 路由组，暴露以下端点：

| 方法 | 路径 | 功能 |
|---|---|---|
| GET | `/api/skill-router/status` | 返回路由状态（enabled、索引技能数、ES 连接状态） |
| POST | `/api/skill-router/refresh` | 刷新路由配置缓存（触发 reset_agent） |
| GET | `/api/skill-router/conflicts` | 返回当前冲突检测结果 |
| POST | `/api/skill-router/rebuild` | 触发全量 ES 索引重建 |

#### 5.2 实现位置

```text
backend/app/gateway/routers/skill_router.py
```

并在 `app/gateway/app.py` 中注册路由。

#### 5.3 响应示例

`GET /api/skill-router/status`：

```json
{
    "enabled": true,
    "es_connected": true,
    "indexed_skills": 19,
    "last_updated": "2026-05-13T10:30:00Z",
    "embedding_service": "http://192.168.200.1:7800/v1",
    "reranker_service": "http://192.168.200.1:7801/v1"
}
```

`POST /api/skill-router/refresh`：

```json
{
    "success": true,
    "message": "SkillRouter configuration refreshed"
}
```

#### 5.4 和热更新的关系

当 `update_skill_router_index.py` 完成后，可调用 `POST /api/skill-router/refresh` 通知 Gateway 层配置已变更。但 SkillRouterMiddleware 本身每次请求都实时查询 ES，不需要额外缓存刷新。

---

### 任务 G3：docker-compose 集成 SkillRouter 模型服务

#### 6.1 目标

当前 `make model-services-start` 在宿主机启动 Embedding/Reranker 服务。G 需要决定：

**方案 A**：将 Embedding/Reranker 作为 docker-compose-dev 的可选服务

```yaml
# docker/docker-compose-dev.yaml 新增
skillrouter-embedding:
  profiles: ["skillrouter"]
  image: pipizhao/SkillRouter-Embedding-0.6B
  ports: ["7800:8000"]
  environment:
    - MODEL_NAME=pipizhao/SkillRouter-Embedding-0.6B

skillrouter-reranker:
  profiles: ["skillrouter"]
  image: pipizhao/SkillRouter-Reranker-0.6B
  ports: ["7801:8000"]
  environment:
    - MODEL_NAME=pipizhao/SkillRouter-Reranker-0.6B
```

**方案 B**：保持宿主机启动，在 `docker-start` 中增加检测

在 `scripts/docker.sh start()` 中增加检查逻辑：

```bash
# 检测 SkillRouter 模型服务是否在线
check_skillrouter_services() {
    local embed_url="${SKILLROUTER_EMBEDDING_BASE_URL:-http://192.168.200.1:7800/v1}"
    local rerank_url="${SKILLROUTER_RERANKER_BASE_URL:-http://192.168.200.1:7801/v1}"

    if ! curl -sf "${embed_url}/models" >/dev/null 2>&1; then
        echo "WARNING: SkillRouter Embedding service not reachable at ${embed_url}"
        echo "  Run: make model-services-start"
    fi
    if ! curl -sf "${rerank_url}/models" >/dev/null 2>&1; then
        echo "WARNING: SkillRouter Reranker service not reachable at ${rerank_url}"
        echo "  Run: make model-services-start"
    fi
}
```

选择方案 A 还是 B 由 G 根据镜像大小、GPU 可用性决定。如果模型镜像过大不适合打包进 compose，选择方案 B。

#### 6.2 .env 和 docker-start 联动

在 `docker-start` 中确认以下环境变量已设置：
- `SKILLROUTER_EMBEDDING_BASE_URL`
- `SKILLROUTER_RERANKER_BASE_URL`
- `SKILL_ROUTER_ES_INDEX`

---

### 任务 G4：可观测性

#### 7.1 路由命中日志

在 `SkillRouterMiddleware.before_agent()` 中增加结构化日志输出：

```python
logger.info("SkillRouter: query=%r trigger=%s mode=%s skills=%s latency_ms=%d",
    query[:80], routing_ctx.trigger, routing_ctx.route_mode,
    routing_ctx.global_selected_skills, latency_ms)
```

#### 7.2 路由统计

新增一个可选的统计模块：

```text
backend/packages/harness/deerflow/routing/metrics.py
```

记录：
- 总请求数
- trigger=true 的比例
- 各 Skill 的命中次数
- 平均路由延迟

可通过 `GET /api/skill-router/metrics` 查询。

---

### 任务 G5：CI/CD 自动化

#### 8.1 PR 验证

新增 GitHub Actions workflow：

```yaml
# .github/workflows/skill-router-validation.yml
name: SkillRouter Validation

on:
  push:
    paths:
      - 'skills/**/router_card.json'
      - 'skills/**/SKILL.md'
      - 'scripts/*skill_router*'
      - 'backend/packages/harness/deerflow/routing/**'

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - name: Validate all router cards against schema
        run: python scripts/extract_router_cards.py

      - name: Check for routing conflicts
        run: python scripts/check_skill_router_conflicts.py --all

      - name: Run router unit tests
        run: PYTHONPATH=backend/packages/harness python3 backend/tests/test_skill_router_middleware.py
```

#### 8.2 路由卡片 Schema 校验

在 CI 中对所有 `router_card.json` 执行 JSON Schema 校验。

---

## 5. 验收清单

```text
[ ] eval_skill_router.py 可运行，内置测试用例全部通过
[ ] GET /api/skill-router/status 返回正确的路由状态
[ ] POST /api/skill-router/refresh 可触发热刷新
[ ] GET /api/skill-router/conflicts 返回冲突检测结果
[ ] docker-start 启动时检测 SkillRouter 模型服务可用性
[ ] 路由命中日志结构化输出可查询
[ ] CI workflow 对 router_card.json 变更自动校验
[ ] make eval-skill-router 可在模型服务在线时运行
[ ] make eval-skill-router --offline 可离线运行（仅测试 should_route/segment_query）
```

---

## 6. 不属于 G 的任务

G 不负责：
- 修改已有的 Router Card Schema（A 的职责）
- 修改 SkillRouterMiddleware 核心路由逻辑（D 的职责）
- 修改 Agent 中间件链顺序（E 的职责）
- 修改 Skill Creator 联动逻辑（F 的职责）

G 只做**生产就绪化**，不改动已有核心逻辑。

---

## 7. 建议提交顺序

```text
commit 1: add eval_skill_router.py end-to-end evaluation
commit 2: add Gateway API /api/skill-router endpoints
commit 3: integrate SkillRouter model services into docker-start
commit 4: add structured logging and metrics to SkillRouterMiddleware
commit 5: add CI workflow for skill-router validation
```
