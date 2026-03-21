这次针对 skill 读取与执行，一共做了 2 处代码修复，都在同一个文件里：

1. 扩展本地只读映射，允许读取 skills 路径  
修改了 tools.py（文件 tools.py）。  
具体变化：
- 之前只把 `/mnt/datasets/network-traffic` 映射为只读。
- 现在会额外读取应用配置里的 skills 挂载信息，把 `config.skills.container_path`（默认 `/mnt/skills`）映射到本机 skills 目录（存在时才生效）。
- 目的：修复 `read_file /mnt/skills/.../SKILL.md` 被拒绝的问题。

2. 放宽本地 bash 绝对路径校验，允许访问只读虚拟根（含 /mnt/skills）  
修改了 tools.py（同文件）。  
具体变化：
- 之前只允许 `/mnt/user-data/**`（以及系统路径如 bin、bin）。
- 现在允许集合变为：`/mnt/user-data` + 所有只读映射根（包括 `/mnt/datasets/...`、`/mnt/skills`）。
- 错误提示也从“只能用 /mnt/user-data”改为“可用根路径列表”，更易排查。
- 目的：修复 `bash` 执行 `node /mnt/skills/...`、`cp -r /mnt/skills/...` 被判定 unsafe 的问题。

验证结果：
- 修改后该文件静态检查无新报错（No errors found）。
- 需要重启服务使运行时加载新逻辑（`make stop-no-nginx` 后再 `make dev-no-nginx`）。

简要效果：
- 不需要再把 skills 手工搬到 workspace 才能读取/执行。
- 可以直接从 `/mnt/skills/public/...` 读取 `SKILL.md` 并执行相关脚本。