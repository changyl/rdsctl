# 管控面集群运维手册(ops-guide-cluster)

> 配套:[control-plane-ha-design.md](./control-plane-ha-design.md)(设计与前提)、
> [control-plane-ha-acceptance.md](./control-plane-ha-acceptance.md)(验收)、
> [deploy/README.md](../deploy/README.md)(守护与自检安装)。
>
> **状态标记**:本文区分两类内容 ——
> ✅ **今天可用**(`RDSCTL_MODE=single` 下的守护、自检、故障拉起);
> ⏳ **随 M1a 生效**(`RDSCTL_MODE=cluster`、`serve` 子命令、`/readyz`、共识组运维)。
> 未标记的章节默认为 ⏳。**不要在 M1a 落地前按 ⏳ 章节操作**(二进制会拒绝未知参数)。

---

## 1. 角色与拓扑

```
                    ┌────────── 业务入口(多地址 / DNS / LB)──────────┐
                    ▼                        ▼                        ▼
              gateway+controller      gateway+controller      gateway+controller
              (node1, 含 ingress)     (node2)                 (node3)
                    │  多数派共识(shard 组)│                        │
                    └──────────┬───────────┴───────────┬────────────┘
                               ▼                       ▼
                        agent(宿主1) ── docker/SQL ──►  实例容器组(数据面)
```

| 角色 | 数量 | 说明 |
|---|---|---|
| `gateway` | 任意(无状态) | 公开 API/页面;可独立扩容 |
| `controller`(= voter) | **3 或 5(奇数)** | 参与共识;每个 shard 一个组,leader 按 shard 分散 |
| `ingress` | 每宿主机一个 | VIP→Proxy 转发;路由绑定入共识日志 |
| `agent` | 每宿主机一个 | 执行面;cluster 模式下**本机也要有**(fence 强制点) |

关键约束:controller 必须是**奇数且 ≥3**;少于半数(含 2/3 挂掉)时**写停、读降级标注**。

---

## 2. 前提检查清单(上线前逐项确认)

| # | 前提 | 检查命令 / 方式 | 不达标后果 |
|---|---|---|---|
| A1 | NTP/chrony 常驻,节点间偏移 ≤ `RDSCTL_MAX_SKEW_MS`(默认 1000ms) | `chronyc tracking`(Leap status、System time)/ `timedatectl show -p NTPSynchronized`;并接入偏移监控 | 拒绝授予新租约,最终写停 |
| A2 | 数据盘本地、fsync 可信 | `deploy/bin/rdsctl-preflight.sh` 第 1 项;`stat -f -c %T <dir>` 不得为 nfs/cifs/fuse | 拒绝启动 |
| A3 | voter 奇数 ≥3,成员表一致 | preflight 第 3 项;各节点 `RDSCTL_CLUSTER` 必须字面一致 | 拒绝启动 |
| A4 | 每台宿主机 agent 在线 | `curl -s $RDSCTL_AGENT_URL/agent/ping` 且 `fence_capable:true` | 拒绝启动(否则 fence 无法强制) |
| A5 | 守护已安装(systemd/launchd) | `systemctl is-enabled rdsctl@node1` / `launchctl list \| grep rdsctl` | 崩溃后无人拉起,副本数静默减少 |
| A6 | 所有副作用 Step 可声明幂等键 | M1a 启动期静态校验(缺则拒绝启动) | 拒绝启动 |

一次性全量自检:

```bash
for n in node1 node2 node3; do
  RDSCTL_MODE=cluster RDSCTL_NODE_ID=$n deploy/bin/rdsctl-preflight.sh $n /etc/rdsctl/rdsctl.env /etc/rdsctl/$n.env \
    && echo "$n 前提 OK" || echo "$n 前提不达标(退出码 $?)"
done
```

---

## 3. 首次部署 / 扩容副本

> **本机可实测的路径(开发/演练)**:`./scripts/cluster.sh up --with-agent --lab`(同机 N 副本 + agent)、
> `./scripts/deploy.sh --cluster`(按 `RDSCTL_NODE_ID` 部署本节点,先过前提自检)、
> `./scripts/ha-drill.sh`(端到端演练)。生产多机仍按下面 §3 的 systemd 流程。

1. 确保 A1–A6 全部通过(§2),并确认所有节点的 `RDSCTL_CLUSTER` 字面一致、`config_epoch` 同版本。
2. 逐台安装 agent 并确认 ping(`deploy/systemd/rdsctl-agent@.service`)。
3. 按 `node1 → node2 → node3` 顺序起 controller;每起一台确认 `/readyz` 为 `ready=true`。
4. 全部就绪后确认多数派与 shard leader 分布:`GET /internal/status`(集群内端点)。
5. 业务入口按 `ingress[]` 清单配置 DNS/LB(多地址)。

> 为什么"必须奇数 ≥3":多数派仲裁需要能容忍 1 台故障;3 副本可容忍 1 台,5 副本可容忍 2 台。

---

## 4. 成员变更(停写重配)⏳

v1 不做自动化成员变更,**增删 voter 必须按以下停写规程执行**(设计 §5.6):

1. **公告维护窗**;确认 `sink` 已追平(`sink_lag_index` 接近 applied index),便于回滚。
2. 提交"预授权"(config_epoch+1)并等待**全部** voter 确认;此时**停止接受新写**,允许读。
3. 逐个停止要下线的成员(始终保留多数派在线),启动新成员并按新成员表加入。
4. 全部新成员就绪后提交 config_epoch+1 生效 op,**恢复写**。
5. 观察 30 分钟:错误率、`/readyz`、共识提交延迟、fence 拒绝计数;异常则按 §5 回滚。

期间客户端可见错误:`503 config_change_in_progress`(属预期,不是故障)。

---

## 5. 滚动升级 ⏳

**一次只动一个副本**,且必须等它重新加入并成为正常/就绪状态:

```bash
for n in node3 node2 node1; do                # 从非 leader 优先;或先 stepdown
  curl -sS -X POST http://$n:9113/internal/stepdown   # 先让位,避免写空窗
  sudo systemctl restart rdsctl@$n
  until curl -fsS http://$n:9113/readyz | grep -q '"ready":true'; do sleep 1; done
  echo "$n 升级完成"; sleep 30                          # 观察窗
done
```

约束与判据:

- **跨 `format_ver` 的升级必须走 §4 停写流程**(日志/快照格式不兼容时不支持混跑)。
- 全程 `R1–R4 回归门禁` + `F10(滚动升级)` 用例必须通过:无 5xx(客户端重试即成功)、
  无重复副作用(容器 `run` 计数不变)、`config_epoch` 不变。
- 升级期间**不要**同时做成员变更或数据面大规模变更(故障归因困难)。

回滚:回到上一版本二进制,重复上述逐副本流程;若已跨 `format_ver`,需按 §4 停写回退。

---

## 6. 故障处置 ⏳

| 现象 | 判定 | 处置 |
|---|---|---|
| 单副本崩溃 | 守护已拉起(`systemctl status`)/`/readyz` 恢复 | 无需人工;查日志定位崩溃原因 |
| 守护未拉起 | `systemctl status` 显示 failed | 查 `RestartPreventExitStatus=2` 是否为前提不达标 → 按 §2 修复;`StartLimitBurst` 触顶需 `systemctl reset-failed` |
| `/readyz` = `quorum_unavailable` | 失多数派 | 恢复网络/进程;**写会一直失败直到多数派恢复**(这是设计行为,不要绕过) |
| `/readyz` = `skew_exceeded` | 时钟偏移**持续**超限(已按最小延迟过滤 + 要求 ≥2 个有效样本) | 先看 `skew_latest_ms`:`skew_measured_ms` 小而 `skew_latest_ms` 很大 = **消息延迟尖峰**(查 CPU/调度/网络,不用动 NTP);两者都大才是真偏移 → 修 NTP。真超界期间该副本会拒绝授予租约(设计行为),确认已摘流量 |
| `/readyz` = `log_unwritable` | 磁盘满/fsync 失败 | 清盘或换盘;**不要**用 `fsync=never` 类开关硬顶(违背 A2) |
| 任务出现 `aborted(fence_lost)` | 旧 holder 被接管(fence 拒绝) | 正常信号;确认新 holder 已完成,必要时重提任务(幂等) |
| 任务出现 `aborted(lease_lost)` | 自身失去租约 | 查该实例是否被其它副本接管;按需重提 |
| 日志中间损坏(哈希链断裂) | 启动拒绝 | **禁止手工改日志**;从快照 + 对端日志恢复(按 M1a 恢复手册),必要时重建该副本 |
| 日志尾部截断 | 启动告警 | 正常(异常掉电);确认告警后继续 |
| `409 fence_stale`(agent 返回) | 旧 fence 命令被拒 | 正常;确认发起方是否僵尸副本,清理该进程 |
| sink 滞后/不可用 | `sink_lag_index` 增长 | 修 DB;**权威路径不受影响**;恢复后自动补齐(无需人工) |
| ingress 入口故障 | 业务连接失败 | 确认 controller 已重绑 `IngressBind` 到存活 host;客户端/DNS 走其它入口(RTO ≤5s) |

---

## 7. 监控项(必须接入告警)

| 指标 | 阈值建议 | 含义 |
|---|---|---|
| `readyz{ready=false}` 持续 | > 30s | 该副本不可接流量 |
| `skew_measured_ms` | > 500(告警)/ > 1000(拒绝授予) | A1 恶化趋势 |
| 共识提交延迟 p99 | 单 AZ > 50ms / 跨 AZ > 200ms | 磁盘或网络劣化 |
| leader 切换次数 | 突增 | 选举震荡(网络抖动/负载) |
| `fence_rejected_total` | 突增 | 出现僵尸副本或反复接管 |
| `sink_lag_index` | 持续增长 | 投影阻塞(分析视图滞后) |
| `fsync_failed_total` | > 0 | A2 受损,**立即处理** |
| 磁盘使用率(日志/快照目录) | > 80% | 压实/快照失败风险 |
| agent 心跳缺失 | > 30s | 该宿主进入 `remote` 盲区(不喂 ERS) |

---

## 8. 日常观测与两个运维动作(管控集群页)

登录任一副本 → 侧边栏「**管控集群**」(权限 `cluster.view`)。页面内容见
[control-plane-cluster-view.md](./control-plane-cluster-view.md);运维要点:

- **成员表**给出每个副本的角色/term/leader/提交点/复制进度/就绪与前提标注,由各副本**现场上报**
  (服务端扇出,单成员 700ms 超时)。看到 `本次探测无响应` 只代表这一次没连上,**不等于**该副本已死;
  请以 `/readyz` 与日志复核。
- 复制进度统一取 **leader 视角**;非 leader 行显示 `-`(只有 leader 维护 `match/next`,不是 0)。
- `last_ack` 显示「从未」= 本进程从未收到过该成员响应(与"刚刚收到"区分开)。

| 动作 | 权限 | 语义 | 不要用它做 |
|---|---|---|---|
| **主动让位**(`POST /api/rds/cluster/stepdown`) | `cluster.manage` | 让当前 leader 让位,raft 按"日志已追平优先"自动选继任者;可从任意副本发起(自动转发) | 指定继任者(刻意不支持:人工把未追平的副本推成主是数据风险);"抢回"领导权的捷径 |
| **重建审计投影**(`POST /api/rds/cluster/resync-sink`) | `cluster.manage` | 把 **leader** 的 sink 投影游标置 0,下一轮对账从日志幂等重放补齐(不写库、不影响共识与租约) | 在 follower 上执行(会被 409 拒绝,这是有意的:投影循环只在 leader 跑) |

**过期租约的处置(发现 15)**:正常路径释放后不留条目;进程被杀留下的残留由该副本每 5s
自愈回收(**只收自己持有 + 已过期 + 本进程未在操作**的);持有者永久离场(机器退役/副本下线)的条目
由 leader 按确定性 cutoff 回收,宽限默认 60s(`RDSCTL_LEASE_REAP_GRACE_MS`,宽限越短回收越快、
长停顿的持有者被回收后其续约会失败并 fail-closed 中止任务,不会双写)。台账里**已过期默认折叠**,
右上「显示已过期(N)」可展开。看到一批 `已过期` 不等于异常;`lease_count` 长期单调增才需要看这里。

**会话与账号(设计 C8)**:会话/用户/角色现在都在共识状态机里 ⇒
①同一 cookie 在任意副本都有效,**反向代理不再需要 sticky session**;
②冻结/改密/改权对所有副本生效,改权限**不需要重新登录**;
③**没有多数派就没有登录**:少数派下登录会明确 503(fail-closed),这是设计行为;
④切换到 cluster 模式后**所有人需重新登录一次**(会话不迁移);
⑤`POST /logout` 是真实撤销(服务端删记录并等全部可达副本生效),不是只清浏览器 cookie;
⑥口令摘要因此进入共识日志/快照 —— 所有副本可读,副本须与数据库同等信任。

两点必须知道:

1. **运维面不代替多数派判定**:`quorum.ok` 是**当前副本自己**的判定,页面对不可达成员不做推断。
   真正的健康判据仍是每个副本的 `/readyz` + 告警(§7)。
2. **会话是每副本进程内的**(M1a 现状):同一 cookie 换副本会 401。所以页面走服务端扇出(浏览器只需连一个副本);
   但反向代理后面**必须开 sticky session**,否则用户会随机被要求重新登录 —— 直到会话/RBAC 入状态机(M1c)。

---

## 9. single ↔ cluster 迁移与回滚

正式流程见设计 §15(模式互斥、导入导出、观察期)。运维要点:

1. **冻结写**(维护窗)→ 导出 `instances/tasks/hosts/RBAC` → 导入 shard 日志 → 起 cluster(写 `mode=cluster` 心跳)。
2. **模式互斥**:`single` 进程若发现 sink 中 cluster 心跳新鲜会**拒绝启动**(防双权威),不要用 `--force` 类手段绕过。
3. **回滚**:停 cluster → 导出回 MySQL → 起 `single`(模式标记写回)。
4. 迁移期必须做数据校验(实例/节点/任务/角色一致性),差异清单人工确认。

---

## 10. 未实现清单(避免误操作)

| 事项 | 状态 | 里程碑 |
|---|---|---|
| `serve` 子命令、`RDSCTL_MODE=cluster`、分片组共识 | 未实现 | M1a |
| `/healthz`、`/readyz`(§7 契约) | 未实现 | M1a |
| agent fence 头与 `fence_seen` 落盘 | 未实现 | M1a |
| `IngressBind`/`Unbind`、ingress 角色、端口权威分配 | 未实现 | M1d |
| 自动成员变更(joint config)、分片迁移 | 未实现 | v2 |
| `docs/ops-guide-cluster.md` 中所有 ⏳ 章节的操作 | 未实现 | 见上 |
| 同机多副本脚本路径(`cluster.sh`/`ha-drill.sh`/`deploy.sh --cluster`) | ✅ 已落地并实测 | 见 §3 顶部说明 |

> **当前可用的部分**:`RDSCTL_MODE=single` 下的守护安装与崩溃拉起(A5)、启动前自检
> (A1/A2/A3 的可执行检查),以及既有的 `scripts/*.sh` 开发链路。
