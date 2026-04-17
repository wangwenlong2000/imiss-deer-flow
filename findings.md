# Findings

- AIO sandbox 实际运行镜像来源于 `config.yaml` 的 `sandbox.image`，由 `AioSandboxProvider` 读取。
- `scripts/docker.sh init` 原来固定拉取默认镜像，已改为从 `config.yaml` 读取镜像并回退默认值。
- `skills/custom/location-matcher` 原先只有脚本，无可安装包元数据；已补充 `pyproject.toml`，可被 `pip install <path>` 安装（用于依赖注入）。
- 自定义镜像采用 `FROM all-in-one-sandbox` 方式，保留原 sandbox API 能力，仅增量安装 Conda 与 skill 依赖。
