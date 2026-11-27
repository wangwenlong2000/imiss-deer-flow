## 1. 之前为什么 skill 不能执行

根因不是 skill 文件本身有问题，而是本地 sandbox 的路径安全策略和映射策略不一致：

1) read_file/ls 的路径校验一开始没有把 /mnt/skills 作为允许的只读虚拟根。
- 结果：读取 /mnt/skills/public/.../SKILL.md 时，先被本地权限校验拒绝，报 Permission denied。

2) bash 的绝对路径校验一开始只放行 /mnt/user-data（外加少量系统路径），没有放行 /mnt/skills。
- 结果：即使你在本机真实目录里有 skill，也会在执行 node /mnt/skills/... 或 cp -r /mnt/skills/... 时被判定 Unsafe absolute paths。

简化理解：
- “路径映射”负责把虚拟路径翻译到本机真实路径。
- “路径校验”负责决定这个虚拟路径是否允许访问。
- 之前是映射部分支持了 skills，但校验部分没放行，导致看起来像“skill 不能执行”。

## 2. sandbox 是什么

sandbox 是 DeerFlow 工具执行的隔离层，给模型统一的文件和命令执行环境。

- 在本地模式（LocalSandbox）下：
	- 命令和文件操作实际发生在宿主机。
	- 但模型看到的是统一的虚拟路径（例如 /mnt/user-data、/mnt/skills）。

- 在容器模式（AIO Sandbox）下：
	- 命令和文件操作在容器里执行。
	- /mnt/user-data、/mnt/skills 是容器内挂载点。

不管哪种模式，模型都应该使用“虚拟路径”来操作文件，而不是直接写宿主机绝对路径。

## 3. sandbox 与本地文件映射的关系

在本地模式下，系统会维护一套“虚拟路径 -> 本机路径”的映射关系。例如：

- /mnt/user-data/workspace -> 某个线程的本地 workspace 目录
- /mnt/user-data/uploads -> 某个线程的上传目录
- /mnt/user-data/outputs -> 某个线程的输出目录
- /mnt/skills -> 本机 skills 目录（来自 config.skills.path + config.skills.container_path）

执行流程可以理解为：

1. 模型发起工具调用，传入虚拟路径。
2. 本地 sandbox 先做安全校验（是否在允许根路径内）。
3. 通过后再把虚拟路径映射成真实本机路径执行。
4. 返回结果时再尽量用虚拟路径展示，避免泄露宿主机细节。

因此，“能否执行 skill”同时取决于两件事：

- 映射是否存在（/mnt/skills 是否映射到真实 skills 目录）
- 校验是否放行（/mnt/skills 是否在允许访问根路径中）

## 4. 本次修复做了什么

这次针对 skill 读取与执行，一共做了 2 处代码修复，都在 backend/packages/harness/deerflow/sandbox/tools.py：

1) 扩展本地只读映射，允许读取 skills 路径
- 之前只把 /mnt/datasets/network-traffic 映射为只读。
- 现在会额外读取应用配置里的 skills 挂载信息，把 config.skills.container_path（默认 /mnt/skills）映射到本机 skills 目录（存在时才生效）。
- 目的：修复 read_file /mnt/skills/.../SKILL.md 被拒绝的问题。

2) 放宽本地 bash 绝对路径校验，允许访问只读虚拟根（含 /mnt/skills）
- 之前只允许 /mnt/user-data/**（以及系统路径）。
- 现在允许集合变为：/mnt/user-data + 所有只读映射根（包括 /mnt/datasets/...、/mnt/skills）。
- 错误提示也从“只能用 /mnt/user-data”改为“可用根路径列表”，更易排查。
- 目的：修复 bash 执行 node /mnt/skills/...、cp -r /mnt/skills/... 被判定 unsafe 的问题。

## 5. 生效与验证

修改后需要重启服务加载新逻辑：

- make stop-no-nginx
- make dev-no-nginx

预期结果：

- 可直接读取 /mnt/skills/public/.../SKILL.md
- 可直接执行 /mnt/skills 下的脚本
- 不需要再手工把 skills 复制到 workspace 才能运行

## 6. 可控放宽方案（新增）

为覆盖“URL 被误判路径”和“本地调试需要更宽松命令”的场景，本次额外增加了一个可控开关方案。

### 6.1 新增环境变量开关

- 新增变量：DEERFLOW_LOCAL_BASH_ALLOW_UNSAFE_PATHS
- 当值为 1 时：跳过本地 bash 工具的绝对路径安全校验。
- 当值为 0 或未设置时：保持原有安全校验逻辑。

默认策略仍然是安全优先；是否放宽由环境变量控制，便于按环境切换。

### 6.2 修复 URL 误判为绝对路径

此前在命令中包含 https://... 时，URL 里的路径段可能被误识别为本地绝对路径（例如 /one_clip/...），触发 Unsafe absolute paths。

本次已修复该误判：

- 对 URL 场景进行识别并忽略其路径段，不再按本地文件路径拦截。
- 这样 curl、requests 等常见网络下载命令不会因 URL 路径被误拦。

### 6.3 本次新增修改点

代码位置：backend/packages/harness/deerflow/sandbox/tools.py

1) 在本地 bash 路径校验函数中增加环境变量开关分支。
2) 在绝对路径检测中增加 URL 识别，避免误判。

环境变量位置：.env

- 增加 DEERFLOW_LOCAL_BASH_ALLOW_UNSAFE_PATHS=1（用于本地调试快速放宽）。

### 6.4 生效方式

修改 .env 后需要重启服务：

- make stop-no-nginx
- make dev-no-nginx

### 6.5 风险与边界说明

1) 放宽开关只影响“本地 bash 绝对路径校验”，并不会自动重写 Python 脚本内部硬编码路径。
2) 在本地模式下，路径替换主要发生在工具命令层；脚本内部如果直接写死 /mnt/...，仍可能出现找不到路径或权限问题。
3) 生产环境不建议长期开启 DEERFLOW_LOCAL_BASH_ALLOW_UNSAFE_PATHS=1，建议仅用于可信开发环境排障。
