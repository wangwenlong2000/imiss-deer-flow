# 服务器配置登录 GitHub 教程

这份文档记录如何在一台**不能直接访问外网**的 Linux 服务器上，通过本地代理和 SSH key 配置，完成 GitHub 登录与代码推送。

本文档基于本项目实际踩坑过程整理，适合以下场景：

- 服务器本身不能直连 GitHub
- 用户没有 root 权限
- 需要在自己的目录下运行代理
- 最终通过 SSH 将代码推送到 GitHub

## 一、整体思路

整个流程分成四步：

1. 在自己的目录里启动代理程序，让服务器本地出现代理端口
2. 配置 `~/.ssh/config`，让 GitHub SSH 连接走本地 SOCKS 代理，并使用 443 端口
3. 配置 SSH key，确保服务器本地私钥与 GitHub 账号中的公钥匹配
4. 把 Git 仓库远端改成 SSH 地址，然后推送

## 二、前提条件

在开始前，需要确认以下条件：

- 服务器上已经有可用的代理程序，例如 Mihomo/Clash
- 该代理程序已经在服务器本地监听端口，例如：
  - `127.0.0.1:10808`：SOCKS5
  - `127.0.0.1:10809`：HTTP/HTTPS
- 服务器上安装了 `ssh` 和 `nc`

可以用下面命令检查：

```bash
which ssh
which nc
ss -lnt | grep 1080
```

## 三、配置 SSH 走本地代理

### 1. 创建 `~/.ssh/config`

执行：

```bash
mkdir -p ~/.ssh
chmod 700 ~/.ssh

cat > ~/.ssh/config <<'EOF'
Host github.com
    HostName ssh.github.com
    Port 443
    User git
    IdentityFile ~/.ssh/id_rsa
    IdentitiesOnly yes
    ProxyCommand nc -x 127.0.0.1:10808 %h %p
EOF

chmod 600 ~/.ssh/config
```

说明：

- `HostName ssh.github.com`
  - GitHub SSH 支持通过 `ssh.github.com:443` 连接，适合受限网络
- `Port 443`
  - 避开默认的 22 端口限制
- `ProxyCommand nc -x 127.0.0.1:10808 %h %p`
  - 通过本地 SOCKS5 代理转发 SSH 流量
- `IdentityFile ~/.ssh/id_rsa`
  - 指定使用的私钥文件
- `IdentitiesOnly yes`
  - 强制 SSH 只使用指定身份，减少干扰

如果你使用的是其他私钥文件，例如 `~/.ssh/github_ed25519`，把 `IdentityFile` 改成对应路径即可。

## 四、配置代理环境变量

为了让 `curl`、`git` 和其他命令行工具走代理，建议写一个脚本。

### 1. 创建 `~/proxy-on.sh`

执行：

```bash
cat > ~/proxy-on.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

export http_proxy="http://127.0.0.1:10809"
export https_proxy="$http_proxy"
export HTTP_PROXY="$http_proxy"
export HTTPS_PROXY="$https_proxy"
export all_proxy="socks5://127.0.0.1:10808"
export ALL_PROXY="$all_proxy"
export no_proxy="localhost,127.0.0.1,::1,db,redis,backend,frontend,postgres,172.17.0.1"
export NO_PROXY="$no_proxy"
export CONTAINER_PROXY_URL="http://172.17.0.1:10810"

# Make git over SSH use the local SOCKS proxy in this shell.
export GIT_SSH_COMMAND='ssh -o ProxyCommand="nc -x 127.0.0.1:10808 %h %p"'

echo "http_proxy enabled: $http_proxy"
echo "all_proxy enabled: $all_proxy"
echo "container proxy enabled: $CONTAINER_PROXY_URL"
echo "git ssh proxy enabled via GIT_SSH_COMMAND"
EOF

chmod +x ~/proxy-on.sh
```

### 2. 启用代理

注意一定要用 `source`：

```bash
source ~/proxy-on.sh
```

不要用：

```bash
bash ~/proxy-on.sh
```

因为 `bash` 只会在子 shell 中生效，退出后环境变量就没了。

## 五、配置 SSH key

### 1. 检查本地 SSH 目录

```bash
ls -lah ~/.ssh
```

常见情况：

- 已有 `id_rsa` 和 `id_rsa.pub`
- 只有 `id_rsa.pub`，没有 `id_rsa`
- 没有任何可用 key

### 2. 如果没有私钥，生成一对新的 key

推荐生成 ed25519：

```bash
ssh-keygen -t ed25519 -C "你的GitHub邮箱" -f ~/.ssh/github_ed25519
```

如果你生成了新 key，需要把 `~/.ssh/config` 里的 `IdentityFile` 改成：

```text
IdentityFile ~/.ssh/github_ed25519
```

### 3. 把公钥加到 GitHub

打印公钥：

```bash
cat ~/.ssh/id_rsa.pub
```

或者：

```bash
cat ~/.ssh/github_ed25519.pub
```

然后去 GitHub：

- `Settings`
- `SSH and GPG keys`
- `New SSH key`

填写：

- `Title`：例如 `server245`
- `Key type`：`Authentication Key`
- `Key`：粘贴刚才输出的整行公钥

### 4. 私钥权限必须正确

SSH 对私钥权限要求很严格。如果权限太宽，会报：

```text
UNPROTECTED PRIVATE KEY FILE
Permissions 0644 for '~/.ssh/id_rsa' are too open
```

修复命令：

```bash
chmod 700 ~/.ssh
chmod 600 ~/.ssh/config
chmod 600 ~/.ssh/id_rsa
chmod 600 ~/.ssh/github_ed25519  # 如果你用的是这把
```

## 六、测试 GitHub SSH 连接

### 1. 执行测试

```bash
source ~/proxy-on.sh
ssh -T git@github.com
```

注意：

- 这个命令成功后会**立刻退出**
- 这是正常现象，不是闪退
- GitHub 本来就不提供交互式 shell

成功时会看到类似：

```text
Hi your-username! You've successfully authenticated, but GitHub does not provide shell access.
```

### 2. 如果终端退出太快，看日志

可以把输出重定向到文件：

```bash
source ~/proxy-on.sh
ssh -vT git@github.com > ~/ssh_github_debug.log 2>&1
tail -n 80 ~/ssh_github_debug.log
```

常见错误与原因：

- `Permission denied (publickey)`
  - GitHub 上没加对应公钥
  - 或私钥不存在
  - 或 SSH 使用了错误的身份文件
- `Identity file ... not accessible`
  - 指定的私钥文件不存在
- `UNPROTECTED PRIVATE KEY FILE`
  - 私钥权限太宽
- `nc: command not found`
  - 代理命令 `nc` 不存在

## 七、使用 ssh-agent 免重复输入 passphrase

如果你的私钥设置了 passphrase，每次推送都会要求输入。

可以用 `ssh-agent` 暂存：

```bash
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/id_rsa
```

或者：

```bash
ssh-add ~/.ssh/github_ed25519
```

之后当前会话内通常就不需要重复输入 passphrase 了。

查看已加载的 key：

```bash
ssh-add -l
```

## 八、配置 Git 推送到 GitHub

### 1. 查看当前远端

```bash
git remote -v
```

### 2. 设置 SSH 远端

进入项目目录：

```bash
cd ~/imiss-deer-flow-main
```

设置远端：

```bash
git remote set-url origin git@github.com:你的用户名/你的仓库名.git
```

如果还没有 `origin`，就用：

```bash
git remote add origin git@github.com:你的用户名/你的仓库名.git
```

### 3. 推送

```bash
git push -u origin main
```

## 九、常见问题

### 1. `Repository not found`

这通常不是 SSH 问题，而是：

- GitHub 上还没有创建这个仓库
- 仓库名写错了
- 用户名或远端地址不对

检查：

```bash
git remote -v
```

### 2. `ssh -T git@github.com` 闪退

这是正常现象。

原因：

- GitHub SSH 测试成功后就会主动断开连接
- 它不会进入交互式 shell

要看是否成功，请看输出内容，而不是看终端有没有停住。

### 3. 只有 `id_rsa.pub`，没有 `id_rsa`

这说明你只有公钥，没有私钥。

解决方式：

- 重新生成一对新的 SSH key
- 或者把真正的私钥放到服务器上

### 4. 已经把公钥加到 GitHub，但还是认证失败

优先检查：

- `~/.ssh/config` 是否指定了正确的 `IdentityFile`
- 私钥权限是否是 `600`
- `ssh -vT git@github.com` 里实际拿去认证的是哪把 key

## 十、推荐做法总结

对于“服务器无法直接上外网，但需要推 GitHub”的场景，推荐方案是：

1. 在自己目录运行代理程序
2. 使用 `~/.ssh/config` + `ProxyCommand` 让 GitHub SSH 走代理
3. 使用服务器本地专用 SSH key
4. 用 `ssh-agent` 缓存 passphrase
5. 最终通过 SSH 地址推送 GitHub

一句话总结：

**代理负责连通性，SSH key 负责身份认证，Git remote 负责把本地仓库映射到 GitHub 仓库。**
