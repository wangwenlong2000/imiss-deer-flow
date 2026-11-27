# Linux amd64 版配置与安装指南（20261127-rc4）

本文用于在一台新 Linux x86_64 服务器上，安装已经交付的 `20261127-rc4` 基础单机版。采用随包 Docker 镜像，不需要在宿主机安装项目的 Python、Node.js、venv，也不需要重新编译源码。

基础版包含 OneCity 页面、账号与登录、会话隔离、合规检测、外部模型问答、基础 Python/CSV 工具分析和文件下载。模型通过外部 API 调用，需要有效 API Key 和网络；本流程保持 Elasticsearch 关闭，不启用 GPU、街景、视频、遥感、NAS 等扩展。

## 1. 安装前准备

目标服务器需要：

- Linux x86_64 / amd64，Bash 4+，`sha256sum` 等常用命令。
- 本机 Docker Engine 和 Docker Compose v2；当前账号可访问 `/var/run/docker.sock`。本流程使用本机 Docker daemon，不使用远程 Docker context。
- 一个可用的 OpenAI 兼容聊天模型 API：模型名称、API 基础地址和 API Key。基础工具场景需要模型支持工具调用。
- 浏览器和安装服务器均能访问站点公开地址；应用容器能访问模型 API。
- 足够的磁盘与内存。镜像压缩包约 4.64 GiB，导入后的镜像、数据库、上传和产物还需额外空间；同时检查 Docker 数据盘和站点数据盘。

未安装 Docker 时，先按目标系统的 [Docker Engine 安装说明](https://docs.docker.com/engine/install/) 安装 Engine 与 Compose 插件。以下示例在管理员的 Bash 终端运行；已有 root 权限可跳过 `sudo -i`。

```bash
sudo -i
bash --version
uname -m
docker version
docker compose version
docker info --format '{{.OSType}}/{{.Architecture}} {{.DockerRootDir}}'
df -h
```

`uname -m` 应为 `x86_64`，Docker 应运行 Linux amd64 容器。选择一个未占用的 HTTP 端口，示例为 `8086`；按访问范围在主机防火墙/云安全组放行该端口。用户访问只需放行 edge HTTP 入口，MySQL、Graph、Gateway 不发布宿主机端口。动态沙箱会另外发布宿主机端口供应用使用，应通过云安全组或适配 Docker 的访问控制限制外部访问。

默认 edge 直接提供 HTTP。需要 HTTPS 时，应在外部反向代理上配置证书，并转发到本站 edge，保留流式响应；不要只将 `public_url` 改成 HTTPS 后就认为证书已配置。

## 2. 准备发布目录与安装目录

当前打包机上的原始交付目录是：

```text
/mnt/data/citybrain-delivery-20261127-rc4/citybrain-20261127-rc4/
```

将这个**完整目录**复制到目标服务器，保留文件权限，例如通过 `rsync -a` 传到 `/opt/citybrain/releases/`。它不是单个应用压缩包；`images/application-images.tar.gz` 仅是 Docker 镜像归档，不要把它当成整套应用的源码压缩包解压。

目标服务器上的示例布局：

```text
/opt/citybrain/releases/citybrain-20261127-rc4/  发布文件，保持原样
  citybrain
  manifest.json
  checksums.sha256
  images/application-images.tar.gz
  deploy/、apps/、backend/、skills/、models/、config/ 等
/srv/citybrain/                                本站配置、密钥和业务数据
```

在同一个 Bash 会话中设置以下变量；后面的命令沿用这些变量。如果直接在当前打包机安装，将第一行改成上述 `/mnt/data/.../citybrain-20261127-rc4`。

```bash
export CITYBRAIN_RELEASE_DIR=/opt/citybrain/releases/citybrain-20261127-rc4
export CITYBRAIN_SITE_ROOT=/srv/citybrain
export CITYBRAIN_TOOLBOX_IMAGE=citybrain-toolbox:20261127-rc4
cd "$CITYBRAIN_RELEASE_DIR"
```

首次安装选用新的站点目录。发布目录与站点目录必须分开，使用绝对路径，不使用符号链接或含 `..`、冒号、逗号的路径。若本机已有一个 CityBrain 站点，应使用不同的站点目录、`project` 名称和 HTTP 端口。

## 3. 核验文件并导入镜像

### 3.1 检查交付文件

```bash
sha256sum -c checksums.sha256
```

全部条目应为 `OK`，命令退出码为 0。出现缺失或不匹配时先重新传输对应发布材料，不要修改清单绕过检查，也不要在发布目录内另放日志、站点配置或本手册副本。

本版镜像归档的固定信息：

| 项目 | 值 |
|---|---|
| 路径 | `images/application-images.tar.gz` |
| 大小 | `4,979,118,035` 字节 |
| SHA-256 | `b131961e7aa45276b50e996d4020f0dfd8cb5ff9cb0fe62d50a34c86e19d5b72` |

### 3.2 导入六个随包镜像

先执行 `docker image load --help`。如果支持 `--platform`，优先选择以下命令，只导入目标平台：

```bash
docker image load --platform linux/amd64 --input images/application-images.tar.gz
```

旧版 Docker 不支持该选项时，使用：

```bash
docker load --input images/application-images.tar.gz
```

两条导入命令选择一条即可。本版已完成归档完整性和 Linux amd64 载荷检查，但最终证据中 `docker_load_tested=false`；MySQL OCI 索引还包含缺失的非目标 ARM64 manifest 及 attestation。目标机上的实际导入成功仍需确认。如果导入报 descriptor/blob 缺失，保留错误，先取得发布方补充的同身份 amd64 镜像材料，再继续安装；不要改用任意 `latest`。

导入成功后，核对以下六个镜像的架构和 ID。输出顺序与下表一致：

```bash
docker image inspect --format '{{.Os}}/{{.Architecture}} {{.Id}}' \
  citybrain-toolbox:20261127-rc4 \
  citybrain-onecity:20261127-rc4 \
  citybrain-deerflow:20261127-rc4 \
  citybrain-edge:20261127-rc4 \
  citybrain-sandbox:20261126-compat \
  mysql:8.0.39
```

| 镜像 | 本版期望的 Image ID（前缀为 `sha256:`） |
|---|---|
| `citybrain-toolbox:20261127-rc4` | `e5c0bf78378e9d27d77bf3b72c0252a41e389ad5d1cc1f21be6b7b0d983480b7` |
| `citybrain-onecity:20261127-rc4` | `36b63076e0a08cdfd6102e696d42d00e38735504d528c76eeb0ed46e90adad4f` |
| `citybrain-deerflow:20261127-rc4` | `18154f3a763802313db14417978cf3ff320bb07f659c6884699ae6e11e9ecd59` |
| `citybrain-edge:20261127-rc4` | `aa1ac8f1220c26760683452b34686e2c353bbbd58c595feb16e4b9cee3033db4` |
| `citybrain-sandbox:20261126-compat` | `89847f269c6355162a02c0acf4bb54e5b5753c2c12608c514f7d88aba52da672` |
| `mysql:8.0.39` | `ccb8f749bb5e59f9f8f03bf7282c7ef27a93a1814a24f0a8a926fb4e19b7fb97` |

每项平台均须为 `linux/amd64`，ID 与表格及包内 `manifest.json` 一致。随后运行发布完整性检查：

```bash
./citybrain --root "$CITYBRAIN_SITE_ROOT" release check --path "$CITYBRAIN_RELEASE_DIR"
```

结果应有 `"passed": true`。这里的 `images_checked=false` 和 `business_acceptance_checked=false` 是检查范围说明：此命令核对发布文件，不代替镜像导入核验或后面的真实业务验收。

一定要先导入镜像并设置 `CITYBRAIN_TOOLBOX_IMAGE`。若工具箱镜像不存在，`./citybrain` 会尝试本地构建，不会自动替你导入镜像归档。每次新开终端也需重新设置上述变量。

## 4. 初始化并填写站点配置

```bash
./citybrain --root "$CITYBRAIN_SITE_ROOT" init
```

新站点将生成 `config/deployment.yaml`、`secrets/` 以及数据目录。版本号和本版镜像引用会从发布包 manifest 自动填入；模型 Key 初始为空。重复 `init` 不覆盖已有 YAML，也不重置管理员或其他秘密。

编辑目标服务器上的 `/srv/citybrain/config/deployment.yaml`。通常只需改管理员邮箱、公开地址/端口和模型配置；以下是完整的基础档示例。

```bash
vi "$CITYBRAIN_SITE_ROOT/config/deployment.yaml"
```

**将示例 IP `192.0.2.10` 换成实际可访问的服务器 IP/域名，将邮箱和模型配置换成实际值。不要原样使用示例 IP。**

```yaml
schema_version: 1
release: 20261127-rc4
project: citybrain
public_url: http://192.0.2.10:8086
http_port: 8086
capabilities: [basic]

bootstrap_admin:
  email: admin@your-company.com
  nickname: Administrator

model:
  name: qwen3.6-35b-a3b
  base_url: https://dashscope.aliyuncs.com/compatible-mode/v1

images:
  onecity: citybrain-onecity:20261127-rc4
  deerflow: citybrain-deerflow:20261127-rc4
  edge: citybrain-edge:20261127-rc4
  sandbox: citybrain-sandbox:20261126-compat
  mysql: mysql:8.0.39
  elasticsearch: docker.elastic.co/elasticsearch/elasticsearch:9.3.3

mysql:
  mode: local
  host: mysql
  port: 3306
  database: onecityagent
  user: onecity

elasticsearch:
  mode: disabled
  url: http://elasticsearch:9200

sandbox:
  port_start: 18481
  replicas: 3

capacity:
  jobs: 3
  max_inflight: 6
  per_user: 2

services: {}
```

配置项含义：

| 字段 | 怎么填写 |
|---|---|
| `public_url` | 用户浏览器与安装服务器都能访问的站点源地址，例如 `http://实际IP:8086`。不能带账号密码、查询参数或 `/citybrain` 等路径前缀。远程浏览器不能使用服务器的 `localhost` 地址。 |
| `http_port` | 本机 edge 监听的端口，范围 `1024–65535`。直接访问时与 `public_url` 端口保持一致；外部 HTTPS 代理可使用不同的公开端口。 |
| `bootstrap_admin.email` | 首次创建管理员使用的真实管理邮箱；默认 `.invalid` 占位邮箱会被拒绝。 |
| `model.name` | 模型服务实际提供的模型 ID，必须与 API Key 权限匹配。 |
| `model.base_url` | OpenAI 兼容 API 的基础地址，例如以 `/v1` 结尾；不要填写完整的 `/chat/completions` 接口，也不要把 Key 写进 URL。容器内 `localhost` 指向容器自己。 |
| `release`、`images` | 保持本版初始化值，不替换镜像标签。ES 镜像字段由配置结构要求保留；`mode: disabled` 时不会启动或要求导入 ES。 |
| `mysql` | 首装保持 `local`、`mysql:3306`，使用新站点独立数据库，不接管已有业务数据库。 |
| `capacity` | 默认同时执行 3 个任务、运行和排队合计 6 个、每用户合计 2 个；`jobs`、`per_user` 均不得大于 `max_inflight`。 |
| `sandbox` | 默认保持即可；多站点同机时采用不同的起始端口。沙箱从 `port_start` 起查找可用端口，`replicas` 是软上限，不代表固定连续端口范围。动态沙箱由业务按需创建，不在 `status` 中作为固定 Compose 服务列出。 |

管理员邮箱、`project`、站点根目录和数据库归属应在首次安装前确定。已有站点不是通过修改这些字段就能搬迁或重建管理员。

## 5. 写入模型密钥并导入资产

在目标服务器管理员终端中交互输入 API Key，输入过程不回显，也不将密钥字面值写入 shell 历史：

```bash
read -r -s -p '请输入模型 API Key: ' CITYBRAIN_MODEL_KEY
printf '\n'
printf '%s\n' "$CITYBRAIN_MODEL_KEY" > "$CITYBRAIN_SITE_ROOT/secrets/model.key"
unset CITYBRAIN_MODEL_KEY
chmod 700 "$CITYBRAIN_SITE_ROOT/secrets"
chmod 600 "$CITYBRAIN_SITE_ROOT"/secrets/*
```

Key 只写入 `secrets/model.key`，不放在 YAML、发布包或源码 `.env` 中。初始管理员密码、数据库密码和 RSA 私钥已由 `init` 生成，一般保持生成值；如果确需在首装前自定义管理员密码，至少 20 个字符。

首次导入本版 Skill、合规模型及合规配置：

```bash
./citybrain --root "$CITYBRAIN_SITE_ROOT" assets-import --source "$CITYBRAIN_RELEASE_DIR"
```

该命令复制 `skills/`、`models/compliance/` 和 `config/compliance/` 到站点 `assets/`。基础版不需另外导入业务数据集。目标资产目录必须为空；导入成功后不要反复执行，已有资产不会被自动覆盖。

## 6. 预检并安装

```bash
./citybrain --root "$CITYBRAIN_SITE_ROOT" check
./citybrain --root "$CITYBRAIN_SITE_ROOT" config-render
./citybrain --root "$CITYBRAIN_SITE_ROOT" install --mode fresh --timeout 600
./citybrain --root "$CITYBRAIN_SITE_ROOT" status
```

**逐条执行，上一步失败就先排查，不要继续执行后续步骤。** `check` 应返回 `"passed": true`；它会检查配置、秘密权限、必需资产、镜像架构、端口及 Docker/Compose。其磁盘预检仅有 1 GiB 的最低保护阈值，不能作为长期容量规划。

`install` 会生成运行配置、启动 MySQL、执行 OneCity 和 Gateway 数据库迁移、启动应用并检查站点身份与就绪状态。成功结果应包含 `"installed": true` 和 `"readiness_passed": true`。此时 `business_acceptance_passed=false` 表示尚需下一节业务验收。

`status` 中应能看到 `mysql`、`api`、`coordinator`、`graph`、`gateway`、`edge` 等常驻服务运行，带健康检查的服务应为 healthy。此命令退出码为 0 本身不代表所有服务健康，仍需查看输出。迁移工具容器完成后退出是正常现象。

安装中断时，先处理报告中的原因，再在相同版本、目录和配置下重试 `install`。不要删除 `data/platform/installation.json` 或迁移记录来绕过校验。初始安装没有完成时使用 `install` 继续，不用 `start` 代替。

## 7. 业务验收与首次登录

执行真实业务验收：

```bash
./citybrain --root "$CITYBRAIN_SITE_ROOT" verify --suite full --timeout 900
```

本步骤会调用真实模型 API，消耗对应额度；覆盖账号、登录、隔离、问答、CSV 工具计算和产物下载。默认创建随机验收账号，结束后撤销令牌并禁用这些账号，保留审计和测试产物。`full` 不包含备份恢复，也不代表全部 Skill 或额外的严格 Gateway 验收通过。

结果应为 `"passed": true`，报告默认保存在 `/srv/citybrain/evidence/verification/<RUN_ID>/report.json`。若失败，保留报告并处理失败项；不要只凭页面能打开或容器 healthy 就判断业务验收完成。

然后在浏览器打开 `public_url` 指定的地址，例如 `http://实际服务器IP:8086`：

1. 登录邮箱使用 `bootstrap_admin.email`。
2. 在受控的管理员终端本地查看初始密码：

   ```bash
   cat "$CITYBRAIN_SITE_ROOT/secrets/admin.password"
   ```

3. 登录后进行一次真实问答、上传一份无敏感信息的小 CSV，要求按列分组求和并生成可下载 CSV；确认结果和文件均可用。

密码文件是初始化材料，修改文件并重跑 `init/install` 不会重置已有管理员密码。如果之后通过页面修改该管理员密码，默认 `interfaces/full` 验收还会读取此文件登录，需要将文件受限地同步为新密码，或另行配置专用验收凭据。

## 8. 日常启停与备份

在发布目录内、设置好第 2 节变量后执行：

```bash
./citybrain --root "$CITYBRAIN_SITE_ROOT" status
./citybrain --root "$CITYBRAIN_SITE_ROOT" logs
./citybrain --root "$CITYBRAIN_SITE_ROOT" logs --follow
```

跟随日志用 `Ctrl+C` 退出。服务停启：

```bash
./citybrain --root "$CITYBRAIN_SITE_ROOT" stop --timeout 120
./citybrain --root "$CITYBRAIN_SITE_ROOT" start --timeout 600
```

停机前先结束业务任务。`stop` 停止应用写入者及本站动态沙箱，保留数据库供备份；`start` 用于已安装完成的原站点，不执行升级或数据库迁移。

需要备份时，在任务排空、应用停止且无其他程序向同一数据库写入的情况下执行：

```bash
./citybrain --root "$CITYBRAIN_SITE_ROOT" stop --timeout 120
./citybrain --root "$CITYBRAIN_SITE_ROOT" backup \
  --output "$CITYBRAIN_SITE_ROOT/backups/manual-backup-20261127.tar.gz"
./citybrain --root "$CITYBRAIN_SITE_ROOT" start --timeout 600
```

备份文件名应按实际日期/批次修改，避免复用已有文件名。备份含数据库、站点秘密及业务数据，需单独保管；镜像归档不能代替业务备份。恢复、迁移和独立目录切换步骤见发布包内 `docs/installation/README.md`。本版已演练同一物理机器的独立目录恢复，尚未完成不同物理机器迁移验收。

## 9. 常见问题

| 现象 | 处理 |
|---|---|
| `./citybrain` 开始执行 `docker build` | 先停止本次意外构建，检查 rc4 工具箱是否导入，以及 `CITYBRAIN_TOOLBOX_IMAGE` 是否已设置。 |
| `release check` 报 `unlisted_or_missing_files` / `mode_changed` | 使用保留权限的方式重新传输发布目录，移走自行添加的文件；不要修改 manifest 或校验清单。 |
| `check` 提示管理员邮箱无效 | 将 `bootstrap_admin.email` 的 `.invalid` 占位值改为实际邮箱。 |
| `empty or missing secret: model.key` | 重新按第 5 节写入有效 Key，确认不是空文件，并保持 `secrets` 为 0700、文件为 0600。 |
| `missing image` / 平台不符 | 完成六镜像导入，核对标签、Image ID 和 `linux/amd64`。本版不是 ARM64 安装包。 |
| 端口占用 / Compose 项目属于另一站点 | 首装前换端口；同机另装站点还需独立目录、`project` 和沙箱端口范围，不停止其他站点来强占资源。 |
| `Asset destination is not empty` | 说明该资产目标已有文件，不重复覆盖导入；若上次只导入了一部分，先保留现场并检查，勿直接清空数据目录。 |
| 页面打不开，但容器运行 | 检查 edge 状态、实际 HTTP 端口、公开地址、主机防火墙与安全组；远程浏览器不要访问 `localhost`。 |
| 安装卡在就绪检查 | 查看 `logs` 和 `evidence/last-command-error.private.json`；确认安装服务器能访问 `public_url`，反向代理没有指向另一站点。 |
| 模型 401/403/404、问答或 CSV 场景失败 | 分别核对 API Key 权限、模型 ID、基础地址、额度、容器网络与工具调用支持，再复验失败项。容器不会自动继承宿主机所有代理设置。 |
| 验收中断，随机测试账号未清理 | 按对应报告目录运行下方 `verify-cleanup`，不要删报告后假定账号已清理。 |
| 想接入视频/街景/ES 或直接覆盖升级 | 另准备对应服务、资产与场景验收；本版没有开放原地 upgrade/rollback apply。 |

验收中断后的清理示例，将 `RUN_ID` 换成实际报告目录名：

```bash
./citybrain --root "$CITYBRAIN_SITE_ROOT" verify-cleanup \
  --state "$CITYBRAIN_SITE_ROOT/evidence/verification/RUN_ID/cleanup-state.json"
```

对外提供排查材料时，使用工具导出明确白名单内的脱敏证据：

```bash
./citybrain --root "$CITYBRAIN_SITE_ROOT" export-evidence \
  --output "$CITYBRAIN_SITE_ROOT/exports/install-evidence"
```

`last-command-error.private.json`、`cleanup-state.json` 及 `secrets/` 可能含秘密或私有诊断，不直接对外发送。

## 10. 安装完成的判断

- 发布文件核验通过，六个镜像的平台和 ID 正确。
- `check` 通过，`install` 返回安装成功，常驻服务就绪。
- `verify --suite full` 通过，报告已保存。
- 通过实际公开地址可以登录、问答、上传并下载正确的计算产物。

本手册为封版后的独立操作说明；不回写原发布目录或既有交付证据。版本能力与已知限制以发布包内 `docs/acceptance/20261127-rc4.md` 为准。
