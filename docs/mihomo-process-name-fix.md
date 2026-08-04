# Mihomo PROCESS-NAME 规则导致 Subs-Check 无法通过代理访问外网

## 问题描述

当 subs-check 运行在 mihomo（Clash Meta）代理环境下时，程序无法从 GitHub 拉取订阅链接内容，所有请求均超时或返回 EOF。

## 根因分析

mihomo 配置文件（`mihomo-proxy-8199.yaml`）的 `rules` 段中存在以下两条规则：

```yaml
rules:
  - PROCESS-NAME,subs-check.exe,🎯 全球直连
  - PROCESS-NAME,subs-check,🎯 全球直连
```

这两条规则导致 mihomo 将 **所有来自 `subs-check` 进程的流量** 强制路由到「全球直连」，完全绕过了代理节点。

### 为什么 curl 能用但 Go 不行？

- `curl` 进程名是 `curl`，匹配不到上述规则，流量正常走代理节点
- `subs-check` 进程名完全匹配，所有出站连接被 mihomo 强制直连

### 为什么直连会失败？

该服务器**无公网出口**，所有外网访问必须通过代理节点。直连意味着：
- DNS 解析 → 系统 DNS（8.8.8.8）被阻断 → 超时
- TCP 连接 → 无法到达目标服务器 → 超时/EOF

## 解决方案

从 mihomo 配置文件中**删除**这两条 `PROCESS-NAME` 规则：

```yaml
# 删除前
rules:
  - PROCESS-NAME,subs-check.exe,🎯 全球直连
  - PROCESS-NAME,subs-check,🎯 全球直连
  - RULE-SET,LocalAreaNetwork,🎯 全球直连

# 删除后
rules:
  - RULE-SET,LocalAreaNetwork,🎯 全球直连
```

然后重启 mihomo：

```bash
# 找到 mihomo 进程
ps aux | grep mihomo | grep -v grep

# 杀掉旧进程
kill -9 <PID>

# 重启
nohup /tmp/mihomo -f /path/to/mihomo.yaml -d /path/to/dir > /tmp/mihomo.log 2>&1 &
```

## 影响范围

- 删除规则后，`subs-check` 进程的流量将正常走 mihomo 代理节点
- 不影响其他进程的路由规则
- 其他 `PROCESS-NAME` 规则不受影响

## 排查方法

如果 subs-check 启动后卡在「开始准备检测代理」且无任何错误输出，按以下步骤排查：

1. **检查 mihomo 代理是否正常**：
   ```bash
   curl -x http://127.0.0.1:7890 -sL "https://raw.githubusercontent.com/test/test/main/test.txt"
   ```

2. **检查是否有 PROCESS-NAME 规则拦截**：
   ```bash
   grep "PROCESS-NAME,subs" /path/to/mihomo.yaml
   ```

3. **确认 mihomo 日志中无 subs-check 连接记录**（被直连的连接不会出现在 mihomo 日志中）

## 相关文件

- mihomo 配置：`/workspace/2api/github/mihomo-proxy-8199.yaml`
- 问题规则位置：`rules:` 段开头
- subs-check 配置：`/workspace/2api/github/subs-check/config/config.yaml`
