# OpenGrep 离线运行资产下载说明

本文档说明在 Linux 服务器环境中，为 DeerFlow 的 `opengrep-compliance` Skill 准备离线运行资产时需要下载的内容。

## 目标

构建阶段可以联网下载 OpenGrep 和规则集；实际运行阶段不访问网络，只使用仓库内已经准备好的二进制和规则文件。

## 默认版本

- OpenGrep: `v1.21.0`
- 规则集: `opengrep-rules` 仓库 `main` 分支快照

如需升级版本，可以在运行下载脚本时设置 `OPENGREP_VERSION`。

## Linux x86_64 需要下载的内容

### OpenGrep CLI

普通 Linux 服务器通常使用 manylinux x86_64 版本：

```text
https://github.com/opengrep/opengrep/releases/download/v1.21.0/opengrep_manylinux_x86
```

下载后保存为：

```text
skills/public/opengrep-compliance/vendor/opengrep/linux/opengrep
```

### OpenGrep 签名校验文件

建议一并下载并归档：

```text
https://github.com/opengrep/opengrep/releases/download/v1.21.0/opengrep_manylinux_x86.cert
https://github.com/opengrep/opengrep/releases/download/v1.21.0/opengrep_manylinux_x86.sig
```

下载后保存为：

```text
skills/public/opengrep-compliance/vendor/opengrep/linux/opengrep.cert
skills/public/opengrep-compliance/vendor/opengrep/linux/opengrep.sig
```

### OpenGrep 规则集

下载官方规则仓库快照：

```text
https://github.com/opengrep/opengrep-rules/archive/refs/heads/main.zip
```

解压后，把官方仓库中的全部运行时 `.yaml/.yml` 规则文件复制到：

```text
skills/public/opengrep-compliance/rules/official/opengrep-rules/
```

注意：只导入运行时规则文件，不导入官方仓库中的 `.py/.js/.ts/.go/.java` 等测试样例文件，也不导入 `.github/`、`stats/`、`scripts/` 或 `*.test.yaml`。这些内容不是运行时规则，且可能导致 OpenGrep 加载噪声或被安全软件拦截。

官方规则库会保留原始目录结构，包括但不限于：

```text
ai/
apex/
bash/
c/
clojure/
csharp/
dockerfile/
elixir/
generic/
go/
html/
java/
javascript/
json/
kotlin/
php/
python/
problem-based-packs/
ruby/
rust/
scala/
solidity/
swift/
terraform/
trusted_python/
typescript/
yaml/
```

## 下载脚本

仓库中提供了脚本：

```text
tools/opengrep/download-opengrep-assets.sh
```

该脚本会自动下载并整理以下内容：

- Linux x86_64 OpenGrep CLI
- OpenGrep `.cert` 和 `.sig` 文件
- `opengrep-rules` 规则 ZIP
- 解压后的 OpenGrep 官方本地规则目录

Trail of Bits 社区规则库使用单独脚本下载：

```text
tools/opengrep/download-trailofbits-rules.sh
```

该脚本会下载 `trailofbits/semgrep-rules` 并只导入运行时 `.yaml/.yml` 规则，输出到：

```text
skills/public/opengrep-compliance/rules/community/trailofbits-semgrep-rules/
```

Apiiro 恶意代码/供应链风险规则库使用单独脚本下载：

```text
tools/opengrep/download-apiiro-rules.sh
```

该脚本会下载 `apiiro/malicious-code-ruleset` 并只导入运行时 `.yaml/.yml` 规则，输出到：

```text
skills/public/opengrep-compliance/rules/community/apiiro-malicious-code-ruleset/
```

默认输出位置：

```text
skills/public/opengrep-compliance/
skills/public/opengrep-compliance/rules/official/opengrep-rules/
```

## 运行方式

在仓库根目录执行：

```bash
bash tools/opengrep/download-opengrep-assets.sh
```

下载 Trail of Bits 社区规则：

```bash
bash tools/opengrep/download-trailofbits-rules.sh
```

下载 Apiiro 恶意代码规则：

```bash
bash tools/opengrep/download-apiiro-rules.sh
```

指定 OpenGrep 版本：

```bash
OPENGREP_VERSION=v1.21.0 bash tools/opengrep/download-opengrep-assets.sh
```

指定下载架构：

```bash
OPENGREP_ASSET=opengrep_manylinux_x86 bash tools/opengrep/download-opengrep-assets.sh
```

ARM64 Linux 可使用：

```bash
OPENGREP_ASSET=opengrep_manylinux_aarch64 bash tools/opengrep/download-opengrep-assets.sh
```

## 下载完成后的离线检查

下载完成后，可以在无网环境执行：

```bash
skills/public/opengrep-compliance/vendor/opengrep/linux/opengrep --version
```

后续运行扫描时，应只使用本地规则：

```bash
skills/public/opengrep-compliance/vendor/opengrep/linux/opengrep scan \
  --config skills/public/opengrep-compliance/rules \
  --json-output reports/opengrep.json \
  --sarif-output reports/opengrep.sarif \
  backend frontend scripts skills
```

不要使用 `--config auto`、远程 URL 规则或在线 registry。
