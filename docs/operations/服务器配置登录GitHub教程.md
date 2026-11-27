# 服务器配置登录 GitHub 教程

这份文档记录如何在 Linux 服务器上配置 GitHub SSH 登录，并完成代码推送。

当前版本不依赖 Clash、Mihomo 或其他本地代理程序，默认前提是：
- 服务器可以访问 GitHub 的 SSH over 443
- 你使用 SSH key 认证

## 一、整体思路

整个流程分成四步：
1. 配置 `~/.ssh/config`，让 GitHub SSH 连接走 `ssh.github.com:443`
2. 配置 SSH key，确保服务器本地私钥与 GitHub 账号中的公钥匹配
3. 测试 GitHub SSH 登录
4. 将 Git 仓库远端改成 SSH 地址并推送

## 二、前提条件

开始前请确认：

- 服务器上已经安装 `ssh`
- 服务器可以连通 `ssh.github.com:443`
- 你有一个 GitHub 账号

可先检查：

```bash
which ssh
ssh -V
```

## 三、配置 SSH 连接 GitHub

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
EOF

chmod 600 ~/.ssh/config
```

说明：
- `HostName ssh.github.com`
  - GitHub 官方支持通过 `ssh.github.com:443` 访问 SSH
- `Port 443`
  - 避开默认的 22 端口限制
- `IdentityFile ~/.ssh/id_rsa`
  - 指定使用的私钥文件
- `IdentitiesOnly yes`
  - 强制只使用指定私钥，减少身份混淆

如果你使用的是其他私钥，比如 `~/.ssh/github_ed25519`，把 `IdentityFile` 改成对应路径即可。

## 四、配置 SSH key

### 1. 检查本地 SSH 目录

```bash
ls -lah ~/.ssh
```

常见情况：
- 已有 `id_rsa` 和 `id_rsa.pub`
- 只有 `id_rsa.pub`，没有 `id_rsa`
- 没有可用 key

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

然后在 GitHub 网页中进入：
- `Settings`
- `SSH and GPG keys`
- `New SSH key`

填写：
- `Title`：例如 `server245`
- `Key type`：`Authentication Key`
- `Key`：粘贴公钥全文

### 4. 私钥权限必须正确

如果权限太宽，SSH 会拒绝使用私钥。

修复命令：

```bash
chmod 700 ~/.ssh
chmod 600 ~/.ssh/config
chmod 600 ~/.ssh/id_rsa
chmod 600 ~/.ssh/github_ed25519  # 如果你用的是这把
```

## 五、测试 GitHub SSH 登录

执行：

```bash
ssh -T git@github.com
```

注意：
- 这个命令成功后会立即退出
- 这是正常现象，不是闪退
- GitHub 不提供交互式 shell

成功时会看到类似：

```text
Hi your-username! You've successfully authenticated, but GitHub does not provide shell access.
```

如果终端退出太快，可以把日志写到文件：

```bash
ssh -vT git@github.com > ~/ssh_github_debug.log 2>&1
tail -n 80 ~/ssh_github_debug.log
```

常见错误：

- `Permission denied (publickey)`
  - GitHub 没有保存对应公钥
  - 或私钥不存在
  - 或 `IdentityFile` 指错了
- `Identity file ... not accessible`
  - 私钥文件不存在
- `UNPROTECTED PRIVATE KEY FILE`
  - 私钥权限过宽

## 六、使用 ssh-agent 缓存 passphrase

如果你的私钥设置了 passphrase，每次推送都输入会比较麻烦。

可以执行：

```bash
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/id_rsa
```

或者：

```bash
ssh-add ~/.ssh/github_ed25519
```

查看已加载 key：

```bash
ssh-add -l
```

## 七、配置 Git 推送到 GitHub

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

如果还没有 `origin`：

```bash
git remote add origin git@github.com:你的用户名/你的仓库名.git
```

### 3. 推送

```bash
git push -u origin main
```

## 八、同步 GitHub 最新更新

如果是多人协作，建议先看本地状态：

```bash
git status
```

如果有本地未提交改动，推荐先暂存：

```bash
git stash push -u -m "temp-before-pull"
git pull origin main
git stash pop
```

如果工作区本来就是干净的，直接：

```bash
git pull origin main
```

## 九、常见问题

### 1. `Repository not found`

通常不是 SSH 问题，而是：
- GitHub 上还没有这个仓库
- 仓库名写错
- 用户名或远端地址不对

检查：

```bash
git remote -v
```

### 2. `ssh -T git@github.com` 看起来像闪退

这是正常现象。

原因：
- GitHub SSH 测试成功后会主动断开连接
- 它不会进入交互式 shell

### 3. 只有 `id_rsa.pub`，没有 `id_rsa`

说明你只有公钥，没有私钥。

解决方式：
- 重新生成一对新 key
- 或把真正对应的私钥放到服务器

### 4. 已经把公钥加到 GitHub，但仍然认证失败

优先检查：
- `~/.ssh/config` 是否指向正确的 `IdentityFile`
- 私钥权限是否为 `600`
- `ssh -vT git@github.com` 里实际拿去认证的是哪把 key

## 十、推荐做法总结

对于当前这套服务器环境，推荐方案是：
1. 使用 `ssh.github.com:443`
2. 使用服务器本地专用 SSH key
3. 使用 `ssh-agent` 缓存 passphrase
4. 通过 SSH 地址推送 GitHub

一句话总结：

**SSH over 443 负责连通性，SSH key 负责身份认证，Git remote 负责把本地仓库映射到 GitHub 仓库。**
