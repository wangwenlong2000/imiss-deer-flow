# CityBrain 基础版安装与恢复

本手册对应 `basic-online-api` 单机发布档：Linux x86_64、OneCity 主入口、MySQL 8.0、外部兼容聊天模型 API、一个固定兼容工具沙箱。应用节点需要 Bash 4+、Docker Engine 与 Docker Compose v2+，无需安装项目 Python、Node 或 venv。

业务范围包含账号、登录、会话、鉴权、合规检查、真实问答、文件上传、基础 Python/CSV 分析及产物下载。119 个 Skill 的声明随包提供，其他场景默认关闭；清单通过不等于全部场景通过业务验收。ES、视觉、遥感、视频、NAS 等扩展需要独立资产与场景验收，不属于基础版自动恢复档。外部聊天 API 需要网络；镜像随包提供不代表业务完全离线。

## 1. 安装材料与核验

交付目录包含源码快照、`manifest.json`、`checksums.sha256`、构建文件、Skill/合规模型资产和安装工具。`images/application-images.tar.gz` 是显式选择随包镜像时生成的归档；秘密、账号密码及业务数据不在通用发布包内。

发布工具无需 Git。工具箱未安装 Git，或从解压后的非 Git 源码目录打包时，manifest 的 `source.git_metadata_available` 为 `false`，`commit`、`dirty` 为 `null`，不将未知状态标成干净工作区。当前源码以完整文件清单、`source.content_sha256` 和 `checksums.sha256` 为准；有可用 Git 元数据时会同时记录提交和工作区状态。

```bash
cd /path/to/citybrain-release
sha256sum -c checksums.sha256
# 收到随包镜像时执行；不把镜像归档当作数据备份。
docker load --input images/application-images.tar.gz
export CITYBRAIN_TOOLBOX_IMAGE=citybrain-toolbox:20261127-rc4
./citybrain --root /srv/citybrain release check --path "$PWD"
./citybrain --root /srv/citybrain init
```

镜像以 manifest 内的 image ID/digest 为准。基础沙箱是固定的兼容运行资产，来源及构建范围在 manifest 中单列；它不是本轮重新构建并验证过全部场景的镜像。无随包镜像时，应从发布方提供的仓库或独立镜像材料取得 manifest 记录的精确镜像，不能拿任意 `latest` 替代。

## 2. 站点配置与安装

编辑 `/srv/citybrain/config/deployment.yaml`：填写管理员邮箱、对外 URL/端口和模型名称/地址。发布目录的 `init` 会从 manifest 填入本版镜像引用。不要把令牌、密码写入 YAML 或 URL。

通过受限文件写入模型密钥 `/srv/citybrain/secrets/model.key`，权限为 `0600`，秘密目录为 `0700`。初始管理员密码位于 `secrets/admin.password`，其他秘密首次生成，重复 init/install 不轮换、不重置密码。新站点自动生成独立 RSA 私钥；前端从同源公钥接口获取当前公钥，无需重新构建页面。

```bash
./citybrain --root /srv/citybrain assets-import --source "$PWD"
./citybrain --root /srv/citybrain check
./citybrain --root /srv/citybrain config-render
./citybrain --root /srv/citybrain install --mode fresh --timeout 600
./citybrain --root /srv/citybrain status
./citybrain --root /srv/citybrain verify --suite full --timeout 900
```

`install` 顺序为配置校验、数据服务就绪、数据库恢复（如有）、OneCity/Gateway 迁移、应用启动、站点健康与公钥身份核验。安装记录和迁移 checksum 支持相同版本中断后重试；未知已有数据库、不同安装根/项目归属、迁移漂移及镜像被替换会明确拒绝。不要删除迁移记录来强行继续。

`check` 是只读预检，不能证明外部 API 或业务场景成功。`basic` 只证明本站就绪；`interfaces` 校验真实注册、登录、历史和隔离；`scenes` 使用真实模型与工具；`full` 合并接口与场景验收，不包含备份恢复。默认验收创建随机测试账号，完成后撤销令牌并禁用，仅保留审计和测试产物。报告保留每项失败与未覆盖情况，模型失败不按成功计。

其他选中能力必须提供 `--scene-manifest /absolute/path/scenes.json`，格式见 `contracts/onecity-deerflow/v1/`。缺样例会使验收失败。验收中断后，可用报告同目录的私有 `cleanup-state.json` 执行：

```bash
./citybrain --root /srv/citybrain verify-cleanup --state /srv/citybrain/evidence/verification/RUN_ID/cleanup-state.json
```

## 3. 生命周期与故障定位

```bash
./citybrain --root /srv/citybrain logs
./citybrain --root /srv/citybrain stop --timeout 120
./citybrain --root /srv/citybrain start --timeout 600
```

`stop` 停止应用写入者和本安装的动态沙箱，保留数据库供逻辑备份；应先确保业务任务已结束。`start` 不执行迁移、不安装依赖，只启动已经完成安装且没有待恢复标记的站点。启动前重新核对安装账本的 root、project、release 和精确镜像 ID，执行完整预检，并比对生成的 Compose 与本站配置、检查计划挂载路径；镜像被替换、配置被篡改、秘密不合规或挂载重定向均拒绝启动，不会隐式升级。内部 API、Graph 和 IAM 不公开，外部仅通过 edge 访问。

失败时先保留 `evidence/last-command-error.private.json`、服务日志及 `data/platform/installation.json`。该 private 文件可能含上游诊断，不应直接对外发送。`export-evidence` 只导出明确白名单内的结构化脱敏证据，不能代替核验：

```bash
./citybrain --root /srv/citybrain export-evidence --output /srv/citybrain/exports/acceptance-evidence
```

主机空间不足时应扩容或清理已确认属于本次构建的临时材料；不要对共享 Docker 主机执行全局 prune。

## 4. 一致备份与异目录恢复

自动恢复档要求 ES disabled、MySQL 8.0.x、全部 InnoDB、无启用的数据库 scheduled events。其他档明确拒绝，需增加经过验证的 ES 快照/外部状态协调流程。备份前应排空任务并停止应用写入者；不得在外部系统仍向同一数据库写入时宣称一致。

```bash
./citybrain --root /srv/citybrain stop
./citybrain --root /srv/citybrain backup --output /srv/citybrain/backups/pre-migration.tar.gz
./citybrain --root /srv/citybrain-restored restore \
  --from /srv/citybrain/backups/pre-migration.tar.gz \
  --project citybrain-restored --http-port 8087 --public-url http://localhost:8087
./citybrain --root /srv/citybrain-restored assets-import --source "$PWD"
./citybrain --root /srv/citybrain-restored install
./citybrain --root /srv/citybrain-restored verify --suite full --timeout 900
```

备份包含 MySQL 逻辑 dump、SQLite 一致快照、checkpoint、LangGraph 状态、线程上传/产物、管理覆盖、站点配置和秘密；必须按秘密材料独立保管。模型/数据资产以完整文件哈希引用，恢复前必须导入完全相同的资产。NAS 不自动复制。恢复要求相同实际 MySQL 版本，并拒绝非空目标目录/数据库、归档穿越、符号链接、校验和变化和不确定的中断导入。

迁移到其他机器时，另行传输同版发布材料、镜像、备份与被引用的资产/NAS；修改公开地址和挂载，并验证账号、既有历史、文件、数值产物及真实场景后再切换入口。同一主机的独立目录/Compose 项目演练只能证明该恢复路径，不能声称完成不同物理机器验收。

首版升级/回退采用停写备份、独立目录恢复并验证后切换的方式。`upgrade/rollback plan` 显示兼容阻断；未经版本间迁移/checkpoint兼容验证，不开放原地 apply。
