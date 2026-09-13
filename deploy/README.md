# deploy/ — 进程守护与启动自检

本目录把设计文档 [docs/control-plane-ha-design.md](../docs/control-plane-ha-design.md) 里的
**正确性前提**落到可执行的部署物上(前提 A1/A2/A3/A4/A5,见设计 §1.3)。

## 1. 为什么需要它(而不是"写完就算高可用")

高可用链的最后一环是"**进程崩了有人把它拉回来**":

- 控制面 3 副本靠多数派仲裁;**少一个副本 = 只剩 2 个,再少一个 = 仲裁失效**(写停)。
- 仓库此前的启动方式是 `scripts/rdsctl.sh` 里的 `nohup ... &` + pid 文件 ——
  `kill -9` 之后**没有任何东西会把它拉起来**。这不是理论问题:见设计文档风险 R2。

因此本目录交付:守护模板(systemd/launchd)+ 启动前自检(拒绝在前提不达标时进入集群模式)。

## 2. 与 `scripts/` 的分工(重要)

| 用途 | 用什么 | 说明 |
|---|---|---|
| 开发、单机 lab、**同机多副本演练** | `scripts/deploy.sh`(含 `--cluster`)/ `scripts/rdsctl.sh` / `scripts/cluster.sh` / `scripts/ha-drill.sh` | `nohup` + pid 文件;pid/日志按实例键(single=端口,cluster=节点 id)区分;适合反复起停与 drill |
| 生产/长期运行(多机) | `deploy/` 下的守护模板 | 崩溃自动拉起、日志接管、启动自检、优雅停止 |

### 2.1 脚本路径(本机可实测)

```bash
# 同机三副本 + 本机 agent(lab 放行;仅开发/演练)
./scripts/cluster.sh up --with-agent --lab
./scripts/cluster.sh status          # 每副本 /healthz + /readyz(ready/quorum/premises)+ leader
./scripts/cluster.sh down

# 端到端演练(起→验租约/fence/失效转移/失多数派 fail-closed/自愈→停)
./scripts/ha-drill.sh --release

# 按节点部署(生产可配合 systemd 模板使用):先过前提自检,不通过即中止
RDSCTL_MODE=cluster RDSCTL_NODE_ID=node1 \
RDSCTL_CLUSTER=1@10.0.0.1:9330,2@10.0.0.2:9330,3@10.0.0.3:9330 \
RDSCTL_RPC_PORT=9330 RDSCTL_DATA_DIR=/var/lib/rdsctl/node1 \
  ./scripts/deploy.sh --cluster
```

> 与 systemd 的关系:两者**不要同时管同一个进程**(脚本 `nohup` 与 `Restart=always` 会互相抢)。
> 生产用 systemd;脚本路径的价值是"能在开发机上真跑一遍",以及给 systemd 的 `RDSCTL_ARGS` 提供同构参数。

**不要用两套同时管同一个进程**(会出现两个 supervisor 抢同一进程/端口)。同一台机器上
只保留其中一种托管方式:要么 `rdsctl.sh stop` 后再交给 systemd,要么反过来。

## 3. 目录结构

```
deploy/
├── README.md                          # 本文件
├── bin/rdsctl-preflight.sh            # 启动前自检(退出码 2 = 前提不达标,拒绝启动)
├── systemd/rdsctl@.service            # 控制面节点模板单元(%i = 节点标识,如 node1)
├── systemd/rdsctl-agent@.service      # 执行面 agent 模板单元(每宿主机一个)
└── launchd/com.rdsctl.node1.plist     # macOS 等价物(开发/演练;复制成 node2/node3)
    launchd/com.rdsctl.agent.plist     # macOS agent
```

## 4. 安装(systemd)

约定安装前缀 `/opt/rdsctl`(仓库根),配置在 `/etc/rdsctl/`:

```bash
# 1) 代码与二进制
sudo install -d /opt/rdsctl /etc/rdsctl /var/log/rdsctl
sudo cp -r . /opt/rdsctl/            # 或按你们的发布方式只放二进制 + deploy/ + docs/
sudo cp /opt/rdsctl/rdsctl.env.example /etc/rdsctl/rdsctl.env   # 全局配置(按需修改)

# 2) 每节点配置(端口/id/模式/参数都要独立)
sudo tee /etc/rdsctl/node1.env >/dev/null <<'EOF'
RDSCTL_PORT=9113
RDSCTL_NODE_ID=node1
RDSCTL_CONTROLLER_ID=node1
RDSCTL_MODE=cluster
RDSCTL_DATA_DIR=/var/lib/rdsctl/node1
RDSCTL_MAX_SKEW_MS=1000
RDSCTL_CLUSTER=1@10.0.0.1:9330,2@10.0.0.2:9330,3@10.0.0.3:9330
RDSCTL_SHARDS=0-9
RDSCTL_AGENT_URL=http://127.0.0.1:9190
RDSCTL_RESUME_TASKS=1
# cluster 模式的实际参数(serve 子命令随 M1a 落地):
RDSCTL_ARGS=serve --roles=gateway,controller,ingress --node-id=node1
EOF

# 3) 服务账号(需要 docker CLI 权限)
sudo useradd -r -s /usr/sbin/nologin -G docker rdsctl

# 4) 安装并启动
sudo cp /opt/rdsctl/deploy/systemd/rdsctl@.service /etc/systemd/system/
sudo cp /opt/rdsctl/deploy/systemd/rdsctl-agent@.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now rdsctl-agent@host1    # 每台宿主机(含控制器本机)
sudo systemctl enable --now rdsctl@node1          # 每个副本一个
sudo journalctl -u rdsctl@node1 -f
```

> **单机模式也可以用**:`RDSCTL_MODE=single` + `RDSCTL_ARGS=--port 9113`,守护与自检同样生效
> (崩溃拉起、fsync/数据目录校验),只是跳过集群相关校验。

## 5. 安装(macOS / launchd,开发与演练)

```bash
sudo install -d /opt/rdsctl /var/log/rdsctl
cp deploy/launchd/com.rdsctl.node1.plist ~/Library/LaunchAgents/
launchctl load -w ~/Library/LaunchAgents/com.rdsctl.node1.plist
launchctl list | grep rdsctl
```

多副本:复制 plist 为 `com.rdsctl.node2.plist` / `node3.plist`,分别改 `Label`、
`ProgramArguments` 端口、`--node-id`、以及 **`RDSCTL_DATA_DIR`(每个副本必须独立)**。

## 6. 启动自检 `bin/rdsctl-preflight.sh`

```bash
# systemd 由 ExecStartPre 自动调用;手工排查时:
RDSCTL_MODE=cluster RDSCTL_NODE_ID=node1 RDSCTL_CLUSTER='...' \
  deploy/bin/rdsctl-preflight.sh node1 [env-file ...]
```

| 退出码 | 含义 | 处置 |
|---|---|---|
| 0 | 前提满足 | 正常启动(告警项仍需关注) |
| 2 | **正确性前提不达标 → 拒绝启动** | 按提示修复;`rdsctl@.service` 用 `RestartPreventExitStatus=2` 避免无意义重启 |
| 3 | 用法/配置错误(缺参数、文件不可读) | 修配置 |

检查项与前提的对应:

| 检查 | 前提 | cluster 模式判定 |
|---|---|---|
| 数据目录可写、fsync 探测、非网络文件系统 | A2 | 不通过 = 拒绝 |
| 时钟同步(chronyc / timedatectl / ntpq) | A1 | 未同步或**无法判定** = 拒绝 |
| 集群成员:奇数 ≥3、格式 `id@ip:port`、id 不重复 | A3 | 不通过 = 拒绝 |
| 启动时可与自身构成多数派(可达 peer ≥ ⌈N/2⌉−1) | C2 | 不满足 = 拒绝 |
| 执行面 agent 可达(`/agent/ping`) | A4 | 未配置/不可达 = 拒绝 |
| 公开端口未被占用 | — | 仅告警 |

**lab/演练的显式放行**:本机没有 chrony/timedatectl/ntpq 时(如 macOS)可用
`RDSCTL_PREFLIGHT_ALLOW_UNVERIFIED_CLOCK=1` 放行时钟检查。此时脚本会打印醒目告警:
**前提 A1 未验证**,该状态不得用于生产(设计文档 §17.1 R3)。

## 7. `/healthz` 与 `/readyz` 契约(随 M1a 落地)

当前二进制的公开端口只有业务 API(无自带探针)。M1a 落地后:

**已落地**(cluster 与 single 模式都提供;免登录,供守护与 LB 使用):

| 端点 | 语义 | 字段 |
|---|---|---|
| `GET /healthz` | 进程存活 | `{ok, mode(single\|cluster), version}`(+cluster 模式 `node_id`) |
| `GET /readyz` | 是否可接流量 | `{ready, role, leader, term, node_id, shard, commit_index, applied_index, quorum_ok, log_writable, fsync_ok, skew_measured_ms, agent_fence_ok, premises_ok, premises_unverified[], lab_degraded, degraded_reason, degraded_reasons[], selfcheck{...}}` |

语义要点:

- `ready` = 多数派可达 ∧ 日志可写 ∧ fsync 可信(HTTP 200/503 与之一致);单机模式为 `true`(进程活着即可接流量,与今天一致)。
- `degraded_reasons` 是**全量**降级原因(可能同时既失多数派又时钟超界),`degraded_reason` 为首要原因;**不允许"看起来就绪"**。
- `skew_measured_ms` 是**实测值**(来自共识消息携带的发送方时钟,见设计 §5.4),不是配置值;`premises_unverified` 中 `A1_clock` 会随实测结果实时出现/消失。
- `premises_ok=false` / `lab_degraded=true` 表示"以 lab 放行启动、前提未全部验证"(例如无 NTP、无 agent fence):此时节点仍可接流量,但**必须**在监控里区别对待。

## 8. 运维要点

1. **数据目录按副本隔离**(`RDSCTL_DATA_DIR`):日志/快照/fence 状态不可共享。
2. **端口按副本隔离**:`RDSCTL_PORT`(公开)、`RDSCTL_RPC_PORT`(集群内,默认 9330)。
3. **优雅停止**:`systemctl stop` 发 SIGTERM,集群模式应在 `TimeoutStopSec`(30s)内完成
   让位与落盘;若被 `kill -9`,租约自然过期、由其它副本接管(设计 §14)。
4. **滚动升级**:一次只动一个副本,先确认 `/readyz` 恢复再动下一个;规程见
   [docs/ops-guide-cluster.md](../docs/ops-guide-cluster.md)。
5. **前提运维**:NTP/chrony 必须常驻并监控偏移;数据盘不得使用网络文件系统。
