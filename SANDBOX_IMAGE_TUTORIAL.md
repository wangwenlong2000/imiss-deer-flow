# Sandbox 镜像定制与打包教程

本文档用于帮助团队成员复现本次改造：

- 在不破坏原有 sandbox 依赖的前提下，扩展 `tshark` / `zeek`
- 自动按当前系统用户给镜像加前缀，避免多用户冲突
- 通过配置和脚本实现“下载基础镜像 + 二次打包 + 自动引用”

## 一、改造目标

1. 保留上游基础 sandbox 能力，不重写环境。
2. 只在 sandbox 中安装网络分析工具，后端主镜像不引入这类依赖。
3. 镜像名自动按当前用户前缀命名，例如 `wwl-deerflow-sandbox:network-tools`。
4. 运行时自动把构建好的镜像注入到 `config.yaml` 与 compose 环境。

## 二、核心文件与职责

1. [docker/sandbox/Dockerfile](../docker/sandbox/Dockerfile)
说明：sandbox 二次打包入口。`FROM` 上游基础镜像并安装 `tshark` / `zeek`。

如果仓库里还没有这个文件，请先手动创建：

```bash
cd /path/to/imiss-deer-flow-main
mkdir -p docker/sandbox
touch docker/sandbox/Dockerfile
```

然后把基础模板写入 `docker/sandbox/Dockerfile`（最少要包含 `FROM` 与安装步骤），例如：

```dockerfile
FROM enterprise-public-cn-beijing.cr.volces.com/vefaas-public/all-in-one-sandbox:latest

USER root

RUN apt-get update \
   && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
     ca-certificates \
     curl \
     gnupg2 \
     tshark \
   && curl -fsSL https://download.opensuse.org/repositories/security:zeek/xUbuntu_22.04/Release.key \
     | gpg --dearmor -o /etc/apt/trusted.gpg.d/zeek.gpg \
   && echo "deb https://download.opensuse.org/repositories/security:/zeek/xUbuntu_22.04/ /" \
     > /etc/apt/sources.list.d/zeek.list \
   && apt-get update \
   && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends zeek \
   && ln -sf /opt/zeek/bin/zeek /usr/local/bin/zeek \
   && ln -sf /opt/zeek/bin/zeekctl /usr/local/bin/zeekctl \
   && rm -rf /var/lib/apt/lists/*
```

2. [config.yaml](../config.yaml)
说明：AIO sandbox 使用环境变量镜像名。

3. [scripts/docker.sh](../scripts/docker.sh)
说明：开发环境入口。负责自动读取用户名、生成镜像前缀、导出 `DEER_FLOW_SANDBOX_IMAGE`。

4. [scripts/deploy.sh](../scripts/deploy.sh)
说明：生产环境入口。与开发环境保持同样的用户前缀逻辑。

5. [docker/docker-compose-dev.yaml](../docker/docker-compose-dev.yaml) 与 [docker/docker-compose.yaml](../docker/docker-compose.yaml)
说明：服务镜像名和 `SANDBOX_IMAGE` 均支持动态变量。

6. [Makefile](../Makefile)
说明：提供统一命令：`sandbox-build`、`docker-build-all`、`docker-start`、`up`。

## 三、配置改造要点

### 第二步：修改 config.yaml（必做）

在创建好 `docker/sandbox/Dockerfile` 后，下一步就是把 Sandbox 配置切到 AIO，并让镜像名从环境变量读取。

操作位置：项目根目录 [config.yaml](../config.yaml) 的 `sandbox:` 段。

把该段改为：

```yaml
sandbox:
  use: deerflow.community.aio_sandbox:AioSandboxProvider
  image: $DEER_FLOW_SANDBOX_IMAGE
  port: 自己设置一下
  replicas: 3
  container_prefix: deer-flow-sandbox
```

说明：

1. `use` 必须是 `AioSandboxProvider`，否则不会在容器化 sandbox 中执行。
2. `image` 必须是 `$DEER_FLOW_SANDBOX_IMAGE`，这样脚本才能按当前用户自动注入镜像名。
3. 不要在这里写死固定镜像名（例如 `deerflow-sandbox:network-tools`），否则会绕过自动前缀机制。

快速检查：

```bash
grep -n "^sandbox:\|image:\|use:" config.yaml | sed -n '1,10p'
```

输出中应能看到：

- `use: deerflow.community.aio_sandbox:AioSandboxProvider`
- `image: $DEER_FLOW_SANDBOX_IMAGE`

### 1) AIO sandbox 镜像配置改为环境变量

在 [config.yaml](../config.yaml) 中使用：

```yaml
sandbox:
  use: deerflow.community.aio_sandbox:AioSandboxProvider
  image: $DEER_FLOW_SANDBOX_IMAGE
  port: 7080
  replicas: 3
  container_prefix: deer-flow-sandbox
```

这样 `config.yaml` 不再写死镜像名，镜像由启动脚本自动注入。

### 2) 用户名前缀自动生成

在 [scripts/docker.sh](../scripts/docker.sh) / [scripts/deploy.sh](../scripts/deploy.sh) 中：

- 读取 `SUDO_USER` 或 `USER`
- 规范化为 slug（小写、非法字符转 `-`）
- 生成 `IMAGE_PREFIX`
- 导出 `DEER_FLOW_SANDBOX_IMAGE=${IMAGE_PREFIX}-deerflow-sandbox:network-tools`

### 第三步：在 Makefile 新增指令（必做）

如果你希望团队成员用统一命令（例如 `make sandbox-build` / `make docker-build-all`），需要在 [Makefile](../Makefile) 中新增目标。

#### 1) 把新命令加入 `.PHONY`

示例（按你的命令名追加）：

```makefile
.PHONY: ... sandbox-build docker-build-all
```

#### 2) 在 `help` 里补一行说明

```makefile
  @echo "  make sandbox-build   - Build the custom sandbox image (auto-prefixed by user)"
  @echo "  make docker-build-all - Build all Docker images used by DeerFlow"
```

#### 3) 新增目标实现

最小可用模板：

```makefile
sandbox-build:
  @echo "=========================================="
  @echo "  Building Custom Sandbox Image"
  @echo "=========================================="
  @echo ""
  @SANDBOX_TAG="$${SANDBOX_TAG:-$${USER:-deerflow}}-deerflow-sandbox:network-tools"; \
  docker build -f docker/sandbox/Dockerfile -t "$$SANDBOX_TAG" . && \
  echo "" && \
  echo "✓ Custom sandbox image built: $$SANDBOX_TAG" || \
  (echo "" && echo "✗ Sandbox build failed" && exit 1)
  @echo ""
```

如果你把复杂逻辑放在脚本里（推荐），Makefile 目标可写成：

```makefile
docker-build-all:
  @./scripts/docker.sh build-all
```

#### 4) 验证新指令是否生效

```bash
make help | grep -E "sandbox-build|docker-build-all"
make sandbox-build
```

通过标准：

1. `make help` 能看到新命令。
2. `make sandbox-build` 构建成功并输出带用户名前缀的镜像名。

## 四、镜像构建策略

### 1) 基础镜像下载

`make docker-init` / `make setup-sandbox` 会拉取上游基础 sandbox 镜像（加速首次启动）。

### 2) 二次打包（定制工具）

`make sandbox-build` 基于 [docker/sandbox/Dockerfile](../docker/sandbox/Dockerfile) 构建用户前缀镜像。

### 3) 全量构建

`make docker-build-all` 会统一构建：

- 自定义 sandbox 镜像
- dev/prod 的 frontend、gateway、langgraph、provisioner 镜像

## 五、标准操作流程

### 开发环境

```bash
make docker-init
make sandbox-build
make docker-start
```

### 一键全编译后启动

```bash
make docker-build-all
make docker-start
```

### 生产环境

```bash
make up
```

## 六、Zeek 依赖冲突修复说明

历史问题：`zeek-core` 依赖 `libc6 >= 2.36`，但基础镜像是 Ubuntu 22.04（`libc6 2.35`）。

修复方式：

1. 不使用 `Debian_12` 的 Zeek 仓库。
2. 在 [docker/sandbox/Dockerfile](../docker/sandbox/Dockerfile) 使用 `xUbuntu_22.04` 仓库。

这样可避免 glibc 版本不兼容导致的安装失败。

## 七、如何验证是否生效

1. 查看 sandbox 镜像名是否带当前用户前缀。
2. 在运行中的 sandbox 环境执行：

```bash
which tshark
which zeek
zeek --version
```

3. 确认后端主镜像中不再强依赖这两个工具（符合“只在 sandbox 使用”的目标）。

## 八、常见问题

1. 问：为什么 compose 里还有 `nginx:alpine` 这类非前缀镜像？
答：这是外部公共基础镜像，不是项目自产镜像，保持上游名字是正常行为。

2. 问：多人同机开发会冲突吗？
答：不会。用户前缀会把自产镜像和 compose project 隔离开。

3. 问：改了 Dockerfile 后为什么没生效？
答：需要重新执行 `make sandbox-build`（或 `make docker-build-all`）再启动。

## 九、建议

1. 团队统一优先使用 `make docker-build-all` + `make docker-start`。
2. CI 或共享环境建议固定上游基础镜像 tag，避免 `latest` 漂移。
3. 如果需要新增系统工具，只改 [docker/sandbox/Dockerfile](../docker/sandbox/Dockerfile)，不要加回后端主镜像。
