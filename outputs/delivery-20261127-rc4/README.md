# 2026-11-27 rc4 续作与交付证据

本目录接续 `delivery-20261127-continuation` 中未完成的基础单机交付。
验收对象由 `sites.json` 固定为 rc4 新站点和独立恢复站点，既有业务站点、旧镜像和用户工作区改动不属于本轮修改范围。

## 已验证

- 最终业务源码的部署与 Skill 离线回归：143 项，0 跳过、失败或错误；4,584 个发布源文件在测试期间哈希不变。
- OneCity、DeerFlow、Toolbox、Edge 四个 rc4 镜像构建成功。
- 三个 Python 镜像 COPY 文件分别为 142/142、313/313、47/47，与冻结源码完全一致。
- Edge 使用构建缓存；782 个当前前端输入与保留 builder 输入一致，328 个编译文件与最终镜像一致。没有将此记录为独立重编译。
- 主站安装、重复安装成功；安装身份、已完成迁移步骤、全部站点秘密均保持不变。
- 在 rc4 镜像中复跑 OneCity 111 项、合规 181 项通过，合规有 1 个既有跳过；部署/Skill、OneCity、合规合计 435 项通过。
- 主站和恢复站 basic/interfaces/full 分别为 5/5、19/19、23/23，真实问答、CSV 计算、文件下载和账号隔离通过。
- 持久留样、停写备份、同机独立 root/Compose 恢复通过；原 2 个账号、历史、上传及结果文件哈希经真实请求核对，恢复后的新业务另行通过。
- 主站 Gateway 首次严格核验失败（`bash` 缺必填 `description`），一次同命令重试通过；恢复站 Gateway 首次通过。全部 3 次尝试中保留 1 次失败，不能称为全部尝试通过。
- 两站当前各 6 个服务均健康，主站已重新启动；只导出允许字段的站点/恢复证据已收集并通过独立 SHA-256 校验。

最终状态以 [验收报告](../../docs/acceptance/20261127-rc4.md) 和 `release-delivery-summary.json` 为准，存在日志不表示通过。含镜像包与归档身份的包后校验单独记入最终摘要。

`delivery-evidence-public/summary.json` 的成功只表示采集时站点报告快照齐全且通过；不能覆盖 `gateway-attempt-history.json` 中 `all_attempts_passed=false` 的历史结论。首次失败日志、诊断、重试日志均保留。`commands-acceptance-snapshot.jsonl` 保存打包前业务步骤完整快照，供采集时命令日志哈希核对；`commands.jsonl` 随包后检查继续追加。

## 续作改进

`pack_release_monitored.py` 新增 `--output`，可将发布目录写到空间充足的文件系统，并同时监测输出盘与系统盘，任一低于 2 GiB 保留量即停止。本轮专用父目录为 `/mnt/data/citybrain-delivery-20261127-rc4/`；不降低原有保留阈值，不清理共享 Docker 资源。

`collect_delivery_evidence.py` 补齐现有 `export-evidence` 不导出的持久留样、恢复核验和 Gateway 报告，使用明确字段白名单，仅公开哈希、状态和完整性检查。站点私有口令、令牌、私钥、模型密钥与业务备份继续保存在站点私有目录。

## 关键材料

- `commands.jsonl`：追加式步骤结果；同名步骤日志拒绝覆盖。
- `image-build-summary.json`：精确镜像身份、构建及来源核验汇总。
- `source-build-inventory.json`：构建和 143 项回归使用的业务源码清单。
- `source-release-freeze.json`：仅验收文档相对构建源码变化，可执行代码与资产一致；最终发布源码哈希 `b89a97da0e36b73c9c0d12b6b3d93a128708fcd65caa02f590c10114b6fa3de7`。
- `regression-deploy-sealed-summary.json`：离线回归，不等于真实业务验收。
- `regression-summary.json`：435 项通过、1 个既有跳过及镜像内回归日志哈希。
- `gateway-attempt-history.json`：区分最新尝试和全部尝试，保存 1 次失败，关联 3 份日志和原始命令记录行的哈希。
- `delivery-evidence-public/`：站点快照、持久留样完整性、恢复与生命周期白名单证据；两个 `recovery-summary` 缺失为 CLI 不产生的可选报告。
- `runtime-health-summary.json`：双站最终服务状态及实际运行镜像 ID。
- `sites.json`：本轮两个独立 root、Compose 项目和端口。
- `acceptance_steps.py`：已限定站点的业务和恢复执行入口。

发布目录一旦生成，不再回写验收结果；含镜像包的校验、归档内部身份核验及最终交付摘要写在本目录，避免改变包内 checksum。

## 最终包与包后检查

发布目录为 `/mnt/data/citybrain-delivery-20261127-rc4/citybrain-20261127-rc4/`，镜像归档为 4,979,118,035 字节。包内 CLI `release check` 通过；独立 `sha256sum -c` 的 4,586 项通过；流式归档检查的 6 个镜像 ID、6 个 manifest/index ID 均链接成功，不提取或加载镜像。发布清单仍保留 `delivery_accepted=false`，不由完整性工具自行授予业务或跨机器验收。

原 MySQL 多平台 OCI 索引仍引用未导出的 ARM64/v8 manifest 及其 attestation，共 2 项；原始缺失数组保留。6 个 Linux amd64 镜像的 config 和全部层文件存在，选中 manifest 的摘要、大小与有序层列表均匹配，137 个实际 blob 的 SHA-256 已核验。目标平台 payload 完整，但完整多平台 OCI 索引闭包不成立，`--require-manifest-identities` 的成功不代表该缺失数组为空。未在共享 daemon 实测 `docker load`，也不声称不同 Docker 版本的导入兼容性已验收；细节见 `image-archive-target-platform-summary.json`。

`release-source-consistency.json` 的初次附加核对因比较双方清单排序不同误报失败，真实文件差异为 0；`release-source-consistency-r2.json` 对三方清单按相对路径排序后全部通过。两份记录均保留，未修改发布包或业务源码。

本轮结论范围仍为基础单机版、同机独立目录/Compose 恢复；历史短 CSV 的内容误判、兼容沙箱来源、前端未独立重编译和 119 个 Skill 未全部业务验收等限制见正式验收报告。
