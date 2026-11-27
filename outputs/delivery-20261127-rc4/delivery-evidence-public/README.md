# rc4 脱敏交付证据

本目录由包外收集器生成。源站点通过无网络、只读、无 Docker socket 的 toolbox 容器读取；源文件必须为 0600 普通文件。只导出明确白名单内的类型化字段，原账号、用户/组织/会话/run 身份、口令、token、URL、业务正文与原始错误文本不导出。源文件 SHA-256 将脱敏结果关联至单独保管的原证据；文件名中的 verification 编号不是原 run ID。

summary.json 的 passed 只表示所需证据齐全且所观察结果均通过，不授予交付验收。missing/invalid/failed 保持显式失败。主站 delivery-verify-restored 为不适用；其余 delivery 报告、持久 state 完整性和 basic/interfaces/full 验收报告均要求存在。恢复站另要求 last-restore。recovery-summary 是当前 CLI 不产生的可选报告：缺失仍标 missing 与 optional_not_produced_by_cli，不使收集失败，也不制造替代源文件。所有发现的验收失败记录均保留，不因后续成功删除。

恢复证明组合为 commands.jsonl 中成功 backup/restore 分项及其日志哈希、恢复站 last-restore 和真实 delivery-verify-restored。lifecycle 只输出命令步骤名、退出码、时间和原日志哈希，完整 command（可能含脚本）从不导出；日志只读取哈希，不复制内容。缺失 backup/restore 成功证据不能判为完成。

private state 只输出完整性布尔、计数及哈希：核实恰好两个默认 case、原 CSV 上传哈希、产物哈希及固定 CSV 数值预期合同齐全。它不重新执行业务、不验证文件当前仍可下载，也不代替 verify-restored 的真实校验。原站和恢复站的 seed 分别校验各自来源；旧数据恢复仍以恢复站 delivery-verify-restored 为准。

同机独立目录/Compose 恢复不证明跨物理机器迁移；静态 Skill 清单和发布校验不证明全部场景通过。本目录也不证明源证据具有签名真实性。index.json 记录导出文件哈希，checksums.sha256 可用 sha256sum -c 核验。
