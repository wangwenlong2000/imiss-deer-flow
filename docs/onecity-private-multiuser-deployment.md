# OneCityAgent 专用多用户服务部署准备

`scripts/deploy_onecity_multiuser.py` 为 OneCityAgent 准备独立的 Gateway、LangGraph 和持久化目录。脚本不会修改现有 3328 服务、其工作树、旧会话或 OneCityAgent 容器。`prepare` 只读取现网配置并生成本地私有部署包；`start` 是单独的实际启动操作。此文档前半部分说明可复用流程，文末记录本次实际部署及验收范围。

## 准备与审查

先完成两端代码、权限和接口回归。由现有部署中读取的模型配置及凭据仅写到仓库外的私有目录，目录权限为 `0700`，文件为 `0600`；不要复制到 Git、聊天或公开测试报告中。脚本用标准库运行，使用现有 LangGraph 容器中的 PyYAML 读取配置，不连接模型或数据库。

```sh
python3 scripts/deploy_onecity_multiuser.py prepare \
  --runtime-dir "$HOME/.local/state/onecity-multiuser"
```

默认读取 `gaozhuohui-deer-flow-gateway`、`gaozhuohui-deer-flow-langgraph`，目标是 `onecity-deerflow-gateway`、`onecity-deerflow-langgraph`，加入 OneCity 已在使用的 `onecityagent-net`。这些名称均可由参数修改。准备目录必须为空；脚本拒绝覆盖已有部署包或悄悄轮换共享签名密钥。命令输出仅含拓扑、容量、数据位置及待切换的连接配置。

准备包包含 `plan.json`、两个私有环境文件、独立配置、共享受理签名密钥、图启动配置、源代码快照与摘要、`launch.sh`。快照仅包含 backend 的 `app/`、`packages/`、`config/` 及构建元数据，排除虚拟环境、用户数据、本地图运行状态、缓存、日志、数据库和隐藏文件，不跟随符号链接。后续修改开发工作树不会影响该部署包。启动时拒绝配置或快照被修改、权限放宽或新容器名称已经存在的情况。需要变更参数或发布代码时使用另一个私有目录重新准备。模型凭据继承自现网容器且不出现在启动命令参数中。

## 文件与权限

| 容器路径 | 来源和访问方式 |
| --- | --- |
| `/app/backend` | 当前选定仓库生成的 `release/backend` 源码快照，只读；不会直接挂载开发工作树 |
| `/app/backend/.venv` | 对应现网服务已有 venv 卷，只读；editable `.pth` 继续指向 `/app/backend/packages/harness` |
| `/app/models`、`/app/skills`、`/app/datasets`、`/app/config` | 对应现网业务资源，只读 |
| `/app/extensions_config.json` | 对应现网扩展配置，只读 |
| `/mnt/nas/streetview_meta` | 现网已存在该挂载时保留，只读 |
| `/data/deerflow` | 新建独立 `data/`，两个新服务共同读写；保存 SQLite、会话文件、用户记忆及审计 |
| `/app/backend/.langgraph_api` | 新建独立 `langgraph-runtime/`，保存 LangGraph 本地任务、存储及恢复状态 |
| `/app/logs` | 独立日志目录，可写；原始运行事件日志默认关闭 |
| `/var/run/docker.sock` | 保留现有 AIO 沙箱调度方式；不挂载给用户沙箱 |

`DEER_FLOW_HOME=/data/deerflow`，`DEER_FLOW_HOST_BASE_DIR` 必须是该 `data/` 在宿主机的绝对路径。AIO 调度通过宿主 Docker 创建沙箱，因此两个路径不能混用。Gateway 和图服务明确以容器 root 运行。线程工作目录由现有 provider 创建并在沙箱就绪后修复到已有 `writable_uid/gid`，当前现网为 `1000:1000`。Docker daemon 将每个线程的工作、上传和输出叶目录分别挂到沙箱内，沙箱用户无需遍历宿主机的私有父目录，故不用放宽 `runtime/`、`data/`、配置或密钥的权限，也不需要给整个数据根目录增加 ACL。技能和公共数据只读挂入沙箱；脚本拒绝继承可写的公共沙箱资源挂载。

脚本保留现网模型、工具、检测器资源和环境配置，独立配置会调整以下字段：

- 沙箱使用独立名称前缀 `onecity-multiuser-sandbox` 和端口起点 `18481`，避免发现或接管旧实例沙箱。
- 合规场景使用 `trusted_upstream`，并启用 `strict_startup`；审计使用 `/data/deerflow/compliance/audit`。
- Checkpoint 使用独立 `/data/deerflow/checkpoints.db`。旧 checkpoint、移动会话映射及用户记忆不会复制或自动认领。
- 禁止启动重复 IM 订阅；OneCity 对接不经过 IM 通道。
- 两端均强制 IAM 和签名受理；内部图地址为新 LangGraph 的 Docker 名称。

新准备包默认执行任务数为 `3`，运行加排队总数为 `6`，单用户总数为 `2`。2026-11-25 的真实业务验证与配置范围见 [验收材料](validation-20261125/README.md)。这些是单个图服务的配置上限，具体模型与文件规模的容量需结合部署环境验证。脚本直接执行已有 venv 内的命令，关闭自动代码重载，不运行可能修改只读环境的 `uv run`。

## 启动专用服务

先部署并验证 OneCity 的 IAM bridge 和认证修改，再启动新服务：

```sh
python3 scripts/deploy_onecity_multiuser.py start \
  --runtime-dir "$HOME/.local/state/onecity-multiuser"
```

新服务不发布宿主端口，Gateway 在私有网络监听 `3330`，LangGraph 监听 `3329`。脚本依次检查新图服务 `/ok` 和 Gateway `/health`。若启动失败，新容器保留用于排查，既有服务保持原状态；脚本不会自动删除已产生数据的容器。启动检查不是认证、合规和模型业务验收。

```sh
python3 scripts/deploy_onecity_multiuser.py status \
  --runtime-dir "$HOME/.local/state/onecity-multiuser"
```

## OneCity 部署与切换需单独执行

先备份 OneCity 实际运行源码、前端 dist、反向代理和完整进程启动配置，再部署已通过回归的两端代码。仅重启需要变更的 OneCity Flask 与 Coordinator，不调用会重启所有协作智能体的旧 `start.sh`。这些操作不在本脚本中。

两端 IAM 地址使用 OneCity 的 Docker 内网名称，例如 `http://onecityagent-a-dev:7010/internal/deerflow/iam`。OneCity 切换配置以准备包输出为准，必须同时覆盖以下项目，避免旧显式 URL 继续访问 3328：

```dotenv
DEERFLOW_API_URL=http://onecity-deerflow-gateway:3330/api/deerflow/mobile/run
DEERFLOW_THREAD_ENSURE_URL=http://onecity-deerflow-gateway:3330/api/deerflow/mobile/threads/ensure
DEERFLOW_THREAD_UPLOAD_URL=http://onecity-deerflow-gateway:3330/api/threads/{thread_id}/uploads
DEERFLOW_FILE_INTERNAL_BASE=http://onecity-deerflow-gateway:3330
DEERFLOW_PUBLIC_BASE_URL=
```

文件在浏览器中经 OneCity 同源认证代理获取，无需公开新 Gateway。配置 PUBLIC base 时也不能使前端直接绕开认证代理。切换前用两个独立测试账号验证认证、跨用户拒绝、各自上传与产物下载、实际双任务并发及排队状态；真实模型验证使用合成小样本。旧无可靠归属会话保留在旧服务，不应自动归给首次访问者。

回滚流量时恢复 OneCity 备份中的旧连接配置和对应版本，独立新数据目录保留以便核查。停止或移除新容器应在任务排空后由操作人员单独执行；本脚本没有删除数据或停止现有服务的命令。

## 离线验证

```sh
python3 -m unittest discover -s backend/tests -p test_onecity_deployment_plan.py -v
```

测试使用合成容器信息、凭据和配置，不连接 Docker。覆盖准备操作只读、密钥权限、共享状态隔离、无公开端口、原服务拒绝覆盖及危险配置拒绝。

## 本次实际发布与验收（2026-11-12）

OneCity 入口已切换到专用内网 Gateway，API 与 Coordinator 各一个进程、调试模式关闭。前端构建及三份主入口文件摘要与线上一致，服务与业务图 Schema 健康检查通过。Gateway、LangGraph 强制 IAM 与签名受理，新后端没有公开宿主端口；公开 OneCity 的 `/internal/` 返回 404。原 3328 实例及其源码、数据与独立网页保持原运行方式。

生产配置为 **执行 3、运行与排队合计 10、单用户合计 2**。隔离真实 HTTP 联调用执行 2/总量 4/单用户 1 验证了两个任务实际并行和第三任务 pending→running；生产本次未调用业务模型或做模型吞吐压测。

验收结果：DeerFlow 定向回归 147 项、OneCity 定向回归 77 项、真实 HTTP 双向集成 10 组、生产入口 44 项通过。生产使用真实注册及 RSA 登录生成两个临时普通账号，验证身份、会话持久化、线程映射、并行同名 CSV 上传与下载、双向越权拒绝、资料不得改权及退出后两端 401。全部测试账号已撤销令牌并停用，保留停用的合成记录及合成文件；未读取现有用户凭据。

部署包位于操作员私有目录，具体位置不写入公开配置。OneCity 原始完整回退备份为容器内 `/app/.onecity-multiuser-rollout/20261112T100203Z-58b395`；切换最终版本的上一版备份为 `/app/.onecity-multiuser-rollout/20261112T100705Z-3cec89`。恢复旧后端应选择前者，恢复前先排空任务并在回退后确认原业务健康。受限启动配置由 `/app/.onecity-runtime.env` 持久保存，旧 start.sh 已只增加读取该文件的入口。

首次目录切换遇到 Docker OverlayFS 不能重命名下层目录的问题，已恢复服务并将发布改为先复制静态资源、最后原子替换 index.html；回退采用原文件覆盖，保留新哈希静态资源供已打开页面加载。现网 Dialog 主键为 32 字符，城市会话 ID 总长现限制为 32，超长请求在写库前返回 400，避免 MySQL 静默截断。注册也补齐工作区必填默认值。

报告见 `outputs/onecity-multiuser-integration-20261112/deployment-summary.json`、`production-smoke.json` 与 `real-http-summary.json`。独立 DeerFlow 网页使用 OneCity 登录、旧历史归属迁移、多图进程共享配额及模型容量压测留待后续。OneCity 其他既有配置管理接口尚未进行全量安全审计，本文验收结论限于上述入口与链路。
