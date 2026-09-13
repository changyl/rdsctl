# 管控面高可用与横向线性扩展设计(control-plane-ha-design)

> 状态:设计定稿(契约完备,待实现)。交付范围 = **本文 + [control-plane-ha-acceptance.md](./control-plane-ha-acceptance.md)**,不含 `src/` 代码改动。
> 目标:在**正确性优先**的前提下,让 rdsctl 管控面同时具备 **高可用**(无单进程单点、故障自动接管)与 **横向线性扩展**(写随分片数、读随副本数近线性)。
> 仲裁形态:**控制面自带多数派仲裁**(自研分片组共识 + 租约 + fence),**不依赖外部数据库高可用做仲裁**;元数据库降级为异步投影 sink,永不进入正确性路径。
> 关联:`docs/scaling-design.md`(M0 底座与 M1 路线,本文承接并收敛其 §6/§7/M1.5)、`docs/meta-authority.md`(登记/事实/切换权威分层)、`docs/lvs-v0.md`(接入层 v0 边界)、`docs/physical-multi-site-ops.md`(跨机/多机房能力缺口)、`docs/ops-guide-dts-orchestrator.md`(PRS/ERS 运维口径)、`docs/m0-report.md`(M0 已落地项)。

---

## 0. 结论速览

| 维度 | 现状(`RDSCTL_MODE=single`,即今天) | 本设计(`RDSCTL_MODE=cluster`) |
|---|---|---|
| 控制面可用性 | 单进程;进程死 = 管控面死,LVS 入口随之消失 | 3/5 副本;少数派(<半)仍可读、写 fail-closed;多数派在则写入不中断 |
| 仲裁者 | 本机 MySQL 的 `instance_locks` 行(单库单点) | **控制面自身多数派**(每 shard 一个共识组),DB 不参与仲裁 |
| 单实例互斥 | lease CAS,但**切换路径不续约**、续约失败仅 warn 继续执行 | 共识提交的租约 + **fence token**,执行面强制拒绝过期持有者 |
| 崩溃恢复 | 启动即 `mark_interrupted()` + `clear_all_locks()`(会误杀/清空他人) | 日志重放;租约自然过期;**启动不做任何全局破坏动作** |
| 状态真相 | 进程内 `DashMap` + 整 JSON 全量覆盖写 | shard 状态机(日志 + 快照);内存降级为缓存 |
| 会话 | 进程内 `DashMap`(多副本登录态互不可见) | 会话/RBAC 入日志,任一网关服务任一 Cookie |
| 接入层 | 进程内转发器、绑 `127.0.0.1`、与进程同生命周期 | `ingress` 独立角色 + 路由绑定入日志 + 入口多地址 |
| 写扩展性 | 单进程单一事实源,无法多写者 | 写随 **shard 数**近线性(S 个 shard = S 个 leader) |
| 读扩展性 | 单进程全量内存 + 每请求 1 次 DB 点查 | 读随 **副本数**近线性(follower 读 + 本地状态机) |
| 元数据库故障 | 启动即 panic(连不上无法启动) | 只影响分析类页面(投影滞后);权威路径不受影响 |

一句话:**今天 rdsctl 是"单机单进程 + 单库"的 lab 形态;本设计把它变成"分片组共识 + fence + 无状态网关 + 独立 ingress + sink 投影"的集群形态,并且保留 single 模式逐字不变。**

---

## 1. 目标、非目标与正确性前提

### 1.1 目标

- **G1 正确性(前置,不可与可用性交换)**:任意崩溃/重启/分区/时钟偏移/乱序重放下的安全性质见 §3 不变量 C1–C10。任何不满足 C1–C10 的"高可用"实现一律不接受。
- **G2 高可用**:无单进程单点。任一副本崩溃/重启/滚动升级期间:管控 API 可用、自动故障转移能力可用、已建立的业务链路不受影响。少数派时**写 fail-closed、读降级且显式标注**;多数派在则写入不中断。
- **G3 横向线性扩展**:写吞吐随 shard 数近线性;读吞吐随副本数近线性;新增副本/分片不引入全局限流点。容量模型见 §13。

### 1.2 非目标(明确不做,避免范围蔓延)

1. 不提供跨机房强一致事务(region 间只做目录/审计最终一致,沿用 `docs/scaling-design.md:74-75`)。
2. 不承诺"单 VIP 无缝漂移"(内核级 IP 接管需 keepalived/特权,见 §10.4)。
3. 不把分析类大数据(审计/慢查/容量/报告/查询审计)纳入共识日志,它们走 sink(§12)。
4. 不在 v1 做动态成员变更与自动分片迁移(§5.6 给出计划性规程,v2 再自动化)。
5. 不改数据面 HA 语义(PRS/ERS/xenon raft 的业务口径不变,只改"由谁决策、如何防脑裂")。

### 1.3 正确性前提(设计成立的必要条件,必须可测、可告警)

| 编号 | 前提 | 说明与检测 |
|---|---|---|
| A1 | 部署侧提供 NTP/chrony,控制器间实测偏移 `skew_measured` 持续 ≤ `max_skew`(默认 1s) | 每个 controller 上报 `skew_measured`;超限即拒绝授予新租约并告警(§5.4)。**部署层已提供可执行检查**:`deploy/bin/rdsctl-preflight.sh` 第 2 项(chronyc / timedatectl / ntpq,cluster 模式下"未同步"或"无法判定"均拒绝启动);lab 可用 `RDSCTL_PREFLIGHT_ALLOW_UNVERIFIED_CLOCK=1` 显式放行并醒目告警(**该状态 = A1 未验证,不得用于生产**) |
| A2 | voter 本地磁盘 fsync 可信;fsync 失败视同节点故障 | 提交前 fsync;失败即降级自身并退出,由外部守护拉起 |
| A3 | 集群 voter 数为奇数(3 或 5);写入需多数派持久化 | 配置校验;集群不得以偶数 voter 启动 |
| A4 | 执行面部署 agent 且支持 fence 头(§9) | `ping` 上报 `fence_capable:true`;不支持则系统进入 `best-effort` 模式并**明示不满足 G1** |
| A5 | 提供进程守护,崩溃后自动拉起 | **已在仓库交付模板**:`deploy/systemd/rdsctl@.service`(模板单元,`Restart=always` + `RestartPreventExitStatus=2`)、`deploy/systemd/rdsctl-agent@.service`、`deploy/launchd/*.plist`(macOS 开发/演练);安装与运维见 `deploy/README.md`、`docs/ops-guide-cluster.md`。前提仍要求**部署方完成安装**并在监控中确认守护生效 |
| A6 | 所有会产生副作用的 Step 声明 `idem_key`;无法声明者必须带前置条件断言 | 步骤清单见 §8.4;缺 `idem_key` 的 Step 在 cluster 模式**拒绝执行**(启动期静态校验) |

> 不满足 A1–A6 时系统必须**显式拒绝进入 cluster 模式**或降级并标注,不允许"静默正确性损失"。

---

## 2. 现状事实与设计约束(全部有代码锚点)

设计不是从零画图,而是针对下面 15 条既有事实做替换。每条都决定了一个设计动作。

| # | 现状事实 | 锚点 | 设计动作 |
|---|---|---|---|
| 1 | 实例注册表、操作锁、端口计数器全在进程内 `DashMap`/`AtomicU64` | `src/instance.rs:679,680-687` | 迁入 shard 状态机,进程内只留缓存(§5/§6) |
| 2 | 任务调度状态全在进程内 `DashMap`,提交即 `tokio::spawn` | `src/dag.rs:369-376,526-595` | 改为"队列 op + claim + 步骤账本"(§8) |
| 3 | 启动即 `mark_interrupted()`(无条件把 pending/running 置 failed)+ `clear_all_locks()`(无条件 `DELETE FROM instance_locks`) | `src/main.rs:54,59`;`src/store.rs:1202-1226,2029-2032` | cluster 模式**禁止**这两个动作;C5(§3) |
| 4 | `orch_reparent`/`orch_rollback` 走 `tokio::spawn`,**全程不续约**(默认租约 30s):切换 >30s 即可被他人抢锁并发切换 = 双主脑裂 | `src/instance.rs:3201-3203,3252-3254,702-705` | 租约由共识组维护、随 op 续约;fence 使旧 holder 无法继续(§5.5) |
| 5 | 续约失败仅 `warn` 并继续执行("仍由本进程执行任务"),无 fencing | `src/instance.rs:1605-1612` | fence 强制:执行面拒旧 fence(§9.2),C3 |
| 6 | 租约过期判定用**各副本本地时钟**,无统一时间源 | `src/store.rs:1973,4163-4167` | 时间比较一律取 leader 提交时刻并随 op 落日志(§5.4) |
| 7 | 存储层 **0 事务 / 0 `FOR UPDATE` / 0 `GET_LOCK`**;每句 SQL fork 一个 `mysql` CLI(10–50ms);写失败只 `warn` 不上抛 | `src/store.rs:1005-1029,1062-1066,1305-1329` | 权威写入不再经 SQL;DB 仅作 sink(§12) |
| 8 | `task_nodes` 无 claim/lease/owner 列;`tasks` 无 owner;`resume_pending` 无跨副本仲裁,且不建 watcher(不续约/不解锁) | `src/store.rs:4170-4195`;`src/dag.rs:970-985,1145-1162` | 队列 claim op + 统一 watcher(§8.2) |
| 9 | 非幂等 Step:`DockerRun`(先 rm 再 run)、`ExecSql`(含切主 SQL)、`UpdateInstanceTopology`(`push` 无去重)、`Audit` | `src/docker.rs:28-36`;`src/instance.rs:5379,5403,5593,5633,6778,6786,6801` | 步骤账本 + 幂等补齐(§8.4) |
| 10 | 接入层是进程内转发器(绑 `127.0.0.1`、注册表进程内、与进程同生命周期);`StopLvs` 在 B 副本执行为"打空",A 的 gate 仍在服务 | `src/lvs.rs:28,59-90,141-146`;`src/instance.rs:5589-5592` | ingress 独立角色 + 路由绑定入日志(§10) |
| 11 | 端口分配是进程内计数器(起点 35000、本机 bind 探测);存储侧 `host_alloc_port` **已存在但创建路径未接** | `src/instance.rs:698,713-729,1239-1242`;`src/store.rs:2501-2510` | 端口分配改为日志权威原子分配器(§10.3) |
| 12 | 执行面 agent 已具备多机骨架(按 host 寻址、无状态、失联标 `remote` 且不喂 ERS),但无 fence/心跳/并发上限,且 `Local` 路由直连本机 docker | `src/agent.rs:9-22,119-210,402-438`;`src/instance.rs:440-468,547-561` | 本机也走 agent + fence(§9) |
| 13 | 每请求 1 次 `user_enabled` 查库;会话在进程内;`thread_local` actor 跨 await 失效导致审计记成 `system` | `src/http.rs:295`;`src/http.rs:30,41`;`src/auth.rs:17-19`;`src/query.rs:699-701` | 会话/RBAC 入日志;请求上下文显式传参(§11.4) |
| 14 | 五个后台循环每副本无门禁启动(sweeper/slow/backuplink/capacity/report);`alert_open` 是 check-then-insert | `src/main.rs:69-73`;`src/store.rs:2239-2262` | 定时职责改为 shard leader 驱动(§8.3) |
| 15 | 构建:`scripts/build.sh` 自称离线但 `.cargo-home`/`vendor/` 不存在,`--offline` 仅在目录存在时加 | `scripts/build.sh:74-81` | 实现期需先决策构建供给(§17 R5);本设计坚持**零新 crate** |

> 保留并复用的既有资产(不要重造):`instance_locks` 的 ODKU CAS 语义(`src/store.rs:1972-2001`)、`task_seq` 的 `LAST_INSERT_ID` 原子序号(`src/store.rs:1106-1124`)、可序列化 `Step`/`TaskNode`(`src/dag.rs:64-230`)、`StepExecutor` 抽象、`RDSCTL_CONTROLLER_ID` holder 标识(`src/instance.rs:6879-6888`)、手写协议先例(`src/sha256.rs:1-2`、`src/http.rs`、`src/agent.rs:9-22`)、验收 harness(`tests/acceptance.rs:346-401,1114-1142`)。

---

## 3. 正确性不变量(C1–C10)

每条不变量 = 「机制 + 前提 + 违反后果 + 验收用例 ID」,验收用例在 [control-plane-ha-acceptance.md](./control-plane-ha-acceptance.md) 展开。

| ID | 不变量 | 机制 | 前提 | 违反后果 | 验收 |
|---|---|---|---|---|---|
| C1 | **单写者**:任一实例在任一时刻至多一个有效 holder 可执行变更副作用 | 租约是共识组提交的日志 op;holder 唯一由提交顺序决定(§5.4) | A1,A3 | 双写/双主脑裂 | I1,I2,I3 |
| C2 | **无脑裂提交**:少数派无法提交任何变更 | 提交需多数派 fsync 落盘;检测到失去多数派即停止服务写(§5.3) | A2,A3 | 分区两侧各自变更 | I4,I5 |
| C3 | **fence 单调且在资源侧强制**:低于已见最高 fence 的命令会被执行面拒绝 | fence=(shard,term,index) 随每条变更命令下发;agent 落盘 `fence_seen` 后再执行(§5.5/§9.2) | A4 | 僵尸 holder 继续写(GC 停顿/分区恢复) | I6,I7 |
| C4 | **有效一次**:同一 `(task,node,step,idem_key)` 的副作用至多执行一次 | 步骤账本:`StepBegin` 提交后执行,`StepDone` 提交结果;重放命中即短路(§8.4) | A6 | 重复建容器/重复切主 | I8,I9 |
| C5 | **状态单调收敛**:实例状态机迁移只经受控 op;进程重启不覆盖他人状态 | 状态入日志;cluster 模式禁止启动期 `mark_interrupted`/`clear_all_locks`(§15) | — | 副本启动误杀他人任务 | I10,I11 |
| C6 | **读一致性显式**:默认线性一致读,任何降级必须显式标注 | leader-lease 读 / read-index 读 / `stale` 三档;降级写响应标记与响应头(§11.1) | A3 | 用户读到幻象并据此操作 | I12 |
| C7 | **队列不重复执行**:一个 task/node 至多一个 worker 持有 | claim 是提交型 op,由 shard leader 串行化(§8.2) | A3 | 重复执行 DAG | I13,I14 |
| C8 | **会话与授权跨副本一致**:任一网关可服务任意会话;冻结/撤销即时生效 | 会话与 RBAC 入日志状态机;每请求校验走本地状态机(§11.4) | A3 | 多副本登录漂移/撤销失效 | I15,I16 |
| C9 | **sink 不参与正确性**:sink 缺失/滞后/回滚不影响 C1–C8 | 权威读走状态机;sink 是幂等投影(带水位)(§12) | — | 分析库故障引发正确性问题 | I17,I18 |
| C10 | **决策审计不静默丢失**:C1–C9 相关决策的审计随 op 一同提交 | 决策 op 携带审计载荷,提交即持久;投影失败可重放补写(§12.3) | A2 | 审计谎报/漏报(现状:`auto_failover` 先写 submitted 再抢锁) | I19 |

---

## 4. 角色、进程模型与拓扑

### 4.1 角色

单二进制、多角色、可同进程也可拆分:

```
rdsctl serve --roles=gateway,controller,ingress \
             --cluster=1@10.0.0.1:9330,2@10.0.0.2:9330,3@10.0.0.3:9330 \
             --shards=0-7 [--public-port 9113] [--rpc-port 9330]
rdsctl agent [--port 9190]        # 执行面(每宿主机一个;本机也部署)
```

| 角色 | 状态 | 职责 | 可多副本 |
|---|---|---|---|
| `gateway` | 无状态 | HTTP、静态页、鉴权、路由、keyset 分页、一致性档位选择 | 是(任意数量) |
| `controller`(= voter) | 有状态 | 承载 shard 共识组(leader/follower)、队列 worker、巡检 worker、ingress 路由分配决策 | 是(3/5 个 voter;非 voter 的 controller 只做 worker,不参与投票) |
| `ingress` | 有状态(每 `(host,port)` 唯一) | VIP→Proxy 的 4 层转发,路由来自日志绑定 | 是(每 host 一个;不同 host 多副本) |
| `agent` | 无状态(带本地 `fence_seen`) | 该宿主机上的 docker/SQL/exec 执行面 | 是(每 host 一个) |

- **网关与控制器默认同进程**(减少跳数);拆进程时网关不含任何共识状态,可水平堆叠。
- **执行面必须独立进程**:即使本机,controller 也经 `agent`(loopback)执行副作用——理由见 §9.1。

### 4.2 集群配置与寻址

- `--cluster` 为**静态成员表**(id@ip:port),启动时校验:奇数 voter、id 唯一、可达性、`skew_measured ≤ max_skew`。
- 内部 RPC 走独立端口(`--rpc-port`,默认 9330),不暴露给业务网;公开端口(默认 9113)只服务 API/页面。
- **配置世代 `config_epoch`** 写入日志首条记录与快照;不同 `config_epoch` 的节点互不通信(防止半旧半新集群)。

### 4.3 模式开关与 single 模式

| env | 默认 | 语义 |
|---|---|---|
| `RDSCTL_MODE` | `single` | `single` = 今天的行为逐字不变(保留 `mark_interrupted`/`clear_all_locks`/进程内 LVS/MySQL 权威);`cluster` = 本设计 |
| `RDSCTL_CLUSTER` | 空 | `id@ip:port,...`(cluster 模式必填) |
| `RDSCTL_SHARDS` | `0` | shard 分配,如 `0-7` 或 `0,3,5` |
| `RDSCTL_NODE_ID` | 空 | 集群内节点 id(cluster 模式必填) |
| `RDSCTL_DATA_DIR` | `./logs/ha` | 日志/快照/`fence_seen` 的本地目录 |
| `RDSCTL_MAX_SKEW_MS` | `1000` | 允许的时钟偏移上界(A1) |
| `RDSCTL_LEASE_TTL_MS` | `30000` | 实例租约时长(必须 ≥ 10 × `max_skew`) |
| `RDSCTL_METADATA_SINK` | `mysql` | `mysql` = 投影到 MySQL(兼容既有页面);`none` = 全自持无 sink |
| `RDSCTL_UNSAFE_NO_FSYNC` | `0` | 仅 `single` 模式或 lab 可用;cluster 模式设 1 时启动即拒绝(违背 A2) |

> **single 模式零变化**是硬要求:既有 `cargo test`(单测 100 项)与 P0 验收 3 项必须继续全绿(见验收文档「回归门禁」)。

---

## 5. 仲裁:分片组共识协议

### 5.1 分片与组

- 实例按 `shard = hash(instance) % shard_count` 归属,`shard_count = ceil(实例数 / 10000)`(`docs/scaling-design.md:50` 的 ≤1 万/shard 基线)。
- **每个 shard 一个独立共识组**,组内副本 = 全部 voter(v1/v1.5 采用「全副本」放置:每个 controller 持有全部 shard 组;leader 按 shard 自然分散)。
- 选全副本的理由:避免分片放置/迁移这一整类再平衡缺陷;容量可行性用数字验证(§13.2:10 万实例元数据 ≈ 500MB/controller)。
- 组的隔离性:组间无共享锁、无全局序列;跨组操作只能走 saga(§6.3)。

### 5.2 Term 选举

- 沿用标准 Raft 选举语义(term、`RequestVote`、`prev_log_index/term` 比较、多数派当选、leader 首个空 op 推进 commit index)。
- 随机化选举超时(默认 1500–3000ms),`heartbeat` 300ms;`pre-vote` 阶段避免分区回来的旧 leader 打断健康集群。
- **优雅让位**:`POST /internal/stepdown`(§14 的滚动升级用):leader 先追加空 op 并投递以推进 commit,再向"日志最全"的 follower 发 `TimeoutNow`(对方立即选举,不等超时),随后自己降为 follower;为避免"让位方日志最新→立刻把领导权抢回来"导致转移失败,让位方进入 **2×选举超时的宽限期** 暂不参选(宽限期过后仍可参选,不会永久停摆)。**实现期实测**:缺该宽限期时,sim 用例 `step_down_transfers_leadership_without_two_leaders` 会卡在原节点反复当选。
- 选举期间写请求 → `503` + `Retry-After`(毫秒级),**不静默降级为本地写**。

### 5.3 日志提交与多数派

- 变更 = 一条日志条目;`commit` 条件 = 多数派已 fsync 持久化(§7.2 group commit);apply 到状态机后返回客户端。
- **失去多数派**即:停止接受写(返回 `503 quorum_unavailable`)、停止 leader 职能、进入 `degraded-majority-lost` 并在 `/readyz` 上报;恢复多数派后自动回到正常(不重启进程)。
- 提交顺序即权威顺序:凡"谁先"的判定(租约、角色互换、拓扑变更)一律以 `(term, index)` 为准,不引入第二权威(对应 `docs/meta-authority.md:33-40` 的单写者原则)。

### 5.4 租约语义与时钟规则

`LeaseGrant{shard, instance, holder, ttl_ms}` / `LeaseRenew{shard, instance, term}` / `LeaseRelease{shard, instance}` 均为提交型 op。

- 状态机维护 `lease[instance] = {holder, term, expire_at}`;`expire_at` 由**授予时 leader 的提交时刻 + ttl_ms** 计算,并随 op 落日志(消除"各副本本地时钟"的漂移影响,替换 `src/store.rs:1973`)。
- **冲突授予规则(实现期修正)**:新 leader 只在 `at_ms >= old.expire_at_ms + max_skew` 时才可授予冲突租约,否则拒绝(返回 `LeaseConflict{safe_from_ms}`,操作方要么等待、要么按 §5.6 停写流程处理)。**推导**:旧 holder 以自己的时钟判定"租约有效至 `expire_at`";任何两节点时钟差 ≤ `max_skew`,故新 leader 必须多等一个 `max_skew` 才能确定旧 holder 已停手 —— 即"过期后再等一个偏移上界"。设计初稿曾写作 `expire_at - max_skew`(会提前一个 `skew` 授予,属不安全),实现与验收一律以本规则为准。
- **确定性要求**:`LeaseGrant/LeaseRenew` 的 op 载荷必须携带 leader 授予时刻 `at_ms`,`expire_at = at_ms + ttl_ms` 在状态机内计算 —— 状态机不读本地时钟,保证各副本重放结果一致(§7 的日志重放确定性)。
- **续约规则**:holder 必须周期性提交 `LeaseRenew`,周期 = `ttl/3`;若一致性检查失败(自身已是 follower 或日志落后),续约不会被提交 → holder **必须立即停止执行副作用**(不再有"续约失败仍继续执行"的路径,替换 `src/instance.rs:1605-1612`)。
- `ttl ≥ 10 × max_skew`(默认 30s / 1s);`skew_measured > max_skew` 的节点拒绝授予新租约并告警(A1)。
- **不再需要**"启动清空孤儿锁":租约随 `expire_at` 自然失效(§15)。

### 5.5 fence token

- `fence = (shard, term, index)`,单调;每次授予/续约/变更 op 都产生新 fence。
- 每条变更命令(经网关→controller→执行面)携带 fence;执行面强制校验(§9.2),状态机侧同样校验(命令必须来自当前 holder 且 fence ≥ 已见最高)。
- fence 是 C3 的唯一强制点;**只有"资源侧强制"才算 fence**,进程内自检不算(这就是 §9.1 要求本机也走 agent 的原因)。

### 5.6 成员变更(v1 计划性规程)

- **v1:停写重配(stop-the-world)**——成员的增删由运维按以下规程执行(规程正文 = 本节;实现阶段追加为运维手册 `docs/ops-guide-cluster.md`,与本文件同源):
  1. 提交 `config_epoch+1` 的"预授权"op 并等待全部 voter 确认(不接受新写,允许读);
  2. 逐个停止旧成员(保留多数派在线),启动新成员并按新成员表加入;
  3. 新配置就绪后提交 `config_epoch+1` 生效 op,恢复写。
- **v2(不在本轮)**:joint config(新旧集合双多数派)自动变更 + 分片放置/迁移。
- 无论 v1/v2,客户端可见的错误语义一致:`503 config_change_in_progress`。

### 5.7 被否方案与理由

| 方案 | 否决理由 |
|---|---|
| 外部 DB 高可用(主从/云 RDS)+ 应用层 lease | 单库仍是仲裁单点;与"控制面自带多数派"前提冲突 |
| 单 Raft 组承载全部状态 | 写吞吐封顶于单一 leader,不满足 G3 |
| 引入 etcd/ZooKeeper/外部协调服务 | 与 `docs/scaling-design.md:49` 的"仅 MySQL + 语言栈"组件约束冲突;新增运维单点 |
| 引入第三方 raft/存储 crate | 构建链不可靠(事实 15:`--offline` 当前必然失败、`Cargo.lock` 离线不可解析);仓库手写协议先例明确(`src/sha256.rs`、`src/agent.rs:9-22`) |
| 静态主备(active/standby 热备) | 接管需人工/脚本,有脑裂风险,且无线性扩展 |
| 进程内 LVS 多副本(端口复用) | 不解决跨机入口;SO_REUSEADDR 不提供语义互斥 |
| keepalived VIP 漂移 | 需特权与内核模块,超出仓库边界;作为**可选部署层增强**记录(§10.4) |

---

## 6. 状态机与 op 契约

### 6.1 KV 与版本

- 每 shard 状态机 = `KV<key, {value, ver}>`。`ver` 单调递增,任何写 op 必须带 `expect_ver`(CAS),冲突返回 `409 version_conflict` 并附当前值(乐观并发,替换 `src/store.rs:1938-1941` 的无条件覆盖)。
- key 规划(全部带 shard 前缀,单 shard 内以 `instance` 为聚合根):

| key 前缀 | 内容 | 权威性 |
|---|---|---|
| `i/<instance>` | 实例登记骨架(节点/代理/分片/规格/标签/region/az/tenant/端口/node_hosts) | 权威 |
| `ist/<instance>` | 实例状态机状态(status/last_error/切换窗标记) | 权威 |
| `l/<instance>` | 租约(holder/term/expire_at/fence) | 权威 |
| `t/<task_id>`、`tn/<task_id>/<node>` | 任务与节点状态 | 权威 |
| `sl/<task_id>/<node>/<step>/<idem_key>` | 步骤账本 | 权威 |
| `q/<shard>`(内部) | 队列索引(按 `visible_at` 组织,内存索引 + 日志恢复) | 权威 |
| `h/<host>`、`hp/<host>/<port>` | Host 注册表与端口占用 | 权威 |
| `g/<instance>`、`gp/<host>/<port>` | ingress 路由绑定 | 权威 |
| `s/<token_hash>`、`u/<user>`、`r/<role>` | 会话、用户、角色权限 | 权威 |
| `d/<...>` | 目录/元数据(region/shard 归属、架构目录、模块定义) | 权威 |
| `f/<instance>` | 事实层(§6.4,可重算) | 非权威 |

### 6.2 op 全集(契约)

`op` 是日志条目的载荷;每条的字段、幂等性与 fence 要求如下(`fence` = 是否必须携带并校验 fence;`idem` = 幂等键来源)。

| op | 关键字段 | fence | idem | 语义 |
|---|---|---|---|---|
| `Noop` | — | 否 | 天然 | 新 term 首条,推进 commit |
| `Put` | `key, value, expect_ver` | 是 | CAS by ver | 通用带版本写入(登记/目录/模块/meta) |
| `InstanceRegister` | `instance, 骨架` | 是 | key+ver | 创建/销毁的登记变更 |
| `InstanceStatus` | `instance, from[], to, reason, task_id` | 是 | 状态前置条件 | 状态机迁移(仅允许合法迁移) |
| `InstanceTopology` | `instance, add_nodes[], remove[], set_parent[]` | 是 | 节点名幂等 + ver | 拓扑变更(替换 `nodes.push` 无去重) |
| `RoleSwap` | `instance, new_master, old_role, others[]` | 是 | 前置角色断言 | 主从角色互换(PRS/ERS 的提交点) |
| `HostBind` / `HostUnbind` | `instance, node, host` | 是 | key+ver | 节点↔宿主机绑定 |
| `PortAlloc` | `host, port, instance, node` | 是 | 唯一键冲突即重试 | 端口权威分配(§10.3) |
| `LeaseGrant` / `LeaseRenew` / `LeaseRelease` | `instance, holder, ttl_ms` | 是 | 天然(term/index) | 租约(§5.4) |
| `TaskEnqueue` | `task_id, kind, instance, creator, nodes_def, dedup_key` | 是 | `dedup_key` | 入队(同实例同 kind 重复提交按 `dedup_key` 去重) |
| `TaskClaim` / `TaskRenew` / `TaskRequeue` / `TaskTerminal` | `task_id, worker, status` | 是 | 天然 | 队列领取/接管/终态 |
| `NodeClaim` / `NodeTerminal` | `task_id, node, status, output` | 是 | 天然 | 节点级领取与终态 |
| `StepBegin` | `task_id, node, step_idx, idem_key, attempt` | 是 | `(task,node,step,idem_key)` | 执行前置登记(§8.4) |
| `StepDone` | 同上 + `result_json` | 是 | 同上 | 结果登记(重放短路) |
| `IngressBind` / `IngressUnbind` | `instance, entry_host, entry_port, backends[], ver` | 是 | key+ver | 接入层绑定(§10) |
| `SessionPut` / `SessionRevoke` / `UserUpsert` / `RolePermsSet` | 会话/RBAC 字段 | 视操作 | key+ver | 会话与授权(§11.4) |
| `AuditAppend` | `who, instance, action, params, result, task_id` | 否(随其他 op 合并) | `(task_id,action,seq)` | 决策审计随 op 提交(C10) |

约束:
1. **所有 op 必须可判定合法性**(状态机侧校验前置条件),非法 op 视为 fatal(不静默忽略)。
2. **op 不得携带不可序列化句柄**(路径/命令白名单化),对齐 `src/dag.rs:1-15` 的"可序列化 Step"原则。
3. **决策与审计同 op 提交**:不允许"先写审计 submitted、再抢锁失败"的谎报(现状见 `src/instance.rs:3029-3040`)。

### 6.3 跨 shard 规则

- **禁止跨组事务**。跨 shard 的操作(如 `migrate_instance` 跨 shard、批量任务)实现为 **saga**:每步是**单 shard 的提交型 op**,带 `saga_id` 与补偿 op;中断由 resume 逻辑按 `saga_id` 继续或补偿(幂等由 `sl/` 账本保证)。
- saga 状态记在**发起 shard**上(`sg/<saga_id>`),补偿动作必须幂等且可重入。
- 单实例的所有 op 恒在其归属 shard,迁移跨 shard 时以"目标 shard 建新聚合 + 源 shard 删除"两步 + 显式切换窗完成(§16 M1.5 细化)。

---

## 7. 日志、快照、fsync 与恢复

### 7.1 日志记录格式

本地日志文件 `$RDSCTL_DATA_DIR/shard-<n>/log`,定长头 + 变长载荷,逐条自校验:

```
[magic u32][ver u16][type u8][flags u8][shard u16]
[term u64][index u64][prev_hash 32B][payload_len u32][payload bytes][hash 32B]
```

- `hash = SHA-256(payload ‖ 头部)`;`prev_hash` 形成哈希链 → 捕获截断/错位/静默损坏(复用 `src/sha256.rs`,零新依赖)。
- `ver` = 记录格式版本;不匹配且无法升级 → 拒绝启动并要求运维介入(不"尽力解析")。
- 尾部截断容忍:最后一条不完整记录在启动时丢弃并告警(标准做法);中间损坏 = fatal。
- 目标:≤ 4KB/条(超出则拒绝构建,强制走"命令白名单 + 参数引用")。

### 7.2 group commit 与 fsync

- 提交路径:`append → 批量 fsync → 多数派确认 → commit → apply → 应答`。fsync 按 ~1–2ms 窗口合并(批内条目共享一次 fsync),把 p99 从 N×fsync 降为 1×fsync。
- fsync 失败 → 节点立即降级自身(停 vote、停 leader 职能)并退出进程,由守护拉起(A2);**绝不"忽略 fsync 失败继续应答"**。
- `RDSCTL_UNSAFE_NO_FSYNC=1` 仅在 `single` 模式接受;cluster 模式启动即拒绝(避免"看起来高可用但违反 A2")。

### 7.3 快照与压实

- 触发:`日志条数 > N`(默认 50000)或 `日志字节 > M`(默认 128MB)或 `apply 后距上次快照 > T`(默认 30min)。
- 快照内容:JSON(serde)+ `last_included_index/term` + `config_epoch` + `format_ver` + SHA-256;写入 `snapshot.tmp` → fsync → rename(原子)。
- 压实:保留最近 `keep`(默认 2)个快照 + 尾部日志;**仅保留仍在追赶的 follower 所需日志**,追不上者走 `InstallSnapshot`。
- 压实必须**分 shard 错峰**(同一时刻只压一个 shard),避免 I/O 尖刺(§13.3)。

### 7.4 崩溃恢复与格式版本

- 启动流程:校验配置(§4.2)→ 加载最新快照 → 重放尾部日志(校验哈希链)→ 校验本地 `log_index` 与集群一致性 → 加入集群 → 参与选举/vote。
- **不做任何全局破坏动作**(C5):不 `mark_interrupted`、不 `clear_all_locks`、不改他人任务状态。
- 恢复后 reconcile:本节点持有过租约的实例,若已失去租约 → 立即停手并把在途任务标记 `aborted(lease_lost)`(幂等续跑由账本保证)。
- 格式升级:同 `format_ver` 内滚动升级安全;跨 `format_ver` 需按 §5.6 停写流程升级(手册化)。

---

## 8. 队列、巡检节拍与 DAG 步骤账本

### 8.1 权威决策入日志,派生事实不入日志

| 类别 | 内容 | 是否入日志 | 理由 |
|---|---|---|---|
| 权威决策 | 登记、状态迁移、角色互换、租约、队列/节点/步骤、绑定、端口、会话/RBAC、ingress 路由、审计 | **是** | 决定"谁有权做什么",必须线性一致 |
| 派生事实 | 巡检 `node_states`、`orch` 事实、监控差分样本、慢查/容量样本、报告正文 | **否** | 可重算;入日志会让 10 万实例 × 30s ≈ 3.3k 次/s 的探测压垮共识(规模不可行) |

事实层由 **shard leader 在内存维护 + 定期 checkpoint**(leader 切换后新 leader 重探;该窗口内事实可能陈旧,**必须在 UI 标注"事实刷新中"**,不回写权威状态)。这与既有分层同构(`docs/meta-authority.md:20-31`)。

### 8.2 队列 op 与 claim

- 入队:`TaskEnqueue`(带 `dedup_key`,同实例同 kind 在途去重,替换现状无 dedup 的 `src/dag.rs:526-595`)。
- 领取:worker 走 `TaskClaim`(提交型)→ 由 leader 串行化 → 天然无重复执行(替换"提交即 `tokio::spawn`")。
- 可见性:claim 带 `visible_at`(退避)与 `lease_until`;worker 崩溃 → `TaskRenew` 停 → 到期由 leader 提交 `TaskRequeue` → 其它 worker 接管。
- watcher 统一:所有 claim 的 worker 都进入同一个 renew 循环(修复现状 `resume_one` 不建 watcher 的缺陷,`src/dag.rs:1145-1162`)。
- 任务并发上限:**按 shard 的 worker 池大小**(Semaphore/定量 worker),避免现状"波次内无界并发"(`src/dag.rs:859-882`)。

### 8.3 巡检节拍与定时职责

- `sweep`/`slow`/`capacity`/`report`/`backuplink` 的定时职责**只由 shard leader 驱动**:leader 按 `next_probe_at` 把到期实例以 `TaskEnqueue{kind=probe}` 入队(提交型 → 恰好一次),worker 执行探测。
- 探测结果写事实层(§8.1),**只有状态跃迁**(running↔degraded、failover 触发)才提交 op + 审计 + 告警;告警去重由唯一键(`instance+kind+state`)兜底(修 `alert_open` 的 check-then-insert)。
- leader 切换后:定时器由新 leader 按状态机时间重建,节拍允许一次抖动(说明:节拍不是正确性依赖,漏一拍只影响检测时延)。

### 8.4 步骤账本与幂等补齐清单

规则:cluster 模式启动期**静态校验**所有 Step 是否声明 `idem_key` 或前置条件(§1.3 A6),缺失即拒绝启动。

| Step | 现状风险 | 目标契约 |
|---|---|---|
| `DockerRun` | 先 rm 同名再 run → 重放销毁现网容器(`src/docker.rs:28-36`) | 改为"存在即校验规格并跳过;需重建走显式 `DockerRecreate` op"(幂等键 = 容器名+规格摘要) |
| `ExecSql` | 直透任意 SQL,重放即重复副作用;含切主语句 | 拆分为**带断言的白名单命令**(如 `SetReadOnly{expected}`、`StartReplica{source,gtid}`、`StopReplica`),每条带前置断言 + 账本键 |
| `UpdateInstanceTopology` | `nodes.push` 无去重 | 改为 `InstanceTopology` op(节点名幂等 + ver CAS) |
| `Audit` | 追加即重复行 | 合并进决策 op 的 `AuditAppend`(幂等键 `task_id+action+seq`) |
| `EnsureLvs`/`StopLvs` | 只作用于本进程注册表 | 改为 `IngressBind`/`IngressUnbind` op + 路由到 gate 所在进程(§10) |
| 其余(幂等已具备) | `DockerRm`/`NetworkCreate`/`NetworkRm`/`WaitHealthy`/`WaitMysql`/`WriteHostFile`/`Verify*`/`ReplaceNode*`/`Noop` | 保持;补 `idem_key` 声明即可 |

---

## 9. 执行面:agent 与 fence

### 9.1 本机也走 agent(取消 `Local` 特权路径)

- 现状 `resolve_route_of` 对未绑定节点返回 `Local` 并**直连本机 docker**(`src/instance.rs:440-468,484-508`),使"控制器宿主 = docker 宿主"成为隐含前提,且 fence 无法在资源侧强制(僵尸进程自检无意义)。
- cluster 模式:每个宿主机(含控制器所在机)部署 `rdsctl agent`,controller 一律**经 agent 执行副作用**;`Local` 仅保留给 `single` 模式。
- 已有验证:同机双进程模式已由 `scripts/agent-drill.sh:26-31,74-98` 真实演练过,协议与 Host 注册表(`store.rs` 的 `rds_hosts`)无需重造。

### 9.2 fence 协议(契约)

```
POST /agent/run|rm|exec|sql
Headers: X-Agent-Token: <token>
         X-Rdsctl-Fence: <shard>:<term>:<index>
         X-Rdsctl-Idem:  <idem_key>        # 幂等键(可选但推荐)
```

- agent 维护 `<DATA_DIR>/fence_seen/<instance>`(每实例一行 `shard:term:index`):收到命令后
  1. 读取 `fence_seen`;若 `incoming < seen` → `409 {"error":"fence_stale"}`(**不执行**);
  2. 若 `incoming > seen` → 先落盘并 **fsync**,再执行命令;
  3. 执行结果与 `X-Rdsctl-Idem` 写入 `<DATA_DIR>/idem/<idem_key>`(已完成即直接返回缓存结果)。
- `409 fence_stale` 是**正常运维信号**(说明旧 holder 被接管),controller 侧记录审计并停止该任务(`aborted(fence_lost)`)。
- `ping` 增加 `fence_capable` 与 `fence_seen_max` 字段;缺少 `fence_capable` 的 agent 使系统进入 `best-effort` 模式并**在 UI/`/readyz` 明示不满足 G1**(A4)。

### 9.3 心跳、并发与安全

- agent 增加 `GET /agent/ping` 心跳上报(host、版本、`fence_capable`、在途命令数),Host 注册表由心跳**自动更新 `agent_port`**(消除人工登记的现状);心跳缺失 → `remote`(沿用既有 `remote` 语义,不喂 ERS,`src/instance.rs:547-561`)。
- 并发上限:agent 侧 Semaphore(默认 = CPU 核数,可配);超限返回 `429`。超时:命令级 deadline(默认 30s,`WaitMysql` 等长任务单独声明),配合现有 30s 客户端超时(`src/agent.rs:435-438`)。
- 安全(不阻塞本轮,但契约先定):per-host token(从日志状态机下发/轮换)+ 命令白名单 + 可选 TLS;现状"单一共享 token + 明文 HTTP"沿用 `docs/physical-multi-site-ops.md` 的边界说明。

---

## 10. 接入层:ingress 角色与入口 HA

### 10.1 模型

```
业务入口(多地址)          ingress 副本(每 host 一个)        数据面
  vip-a:3306  ────────────►  gate(instance A)  ──────►  proxy1..K(host:port)
  vip-b:3306  ────────────►  gate(instance B)  ──────►  ...
```

- `IngressBind{instance, entry_host, entry_port, backends[], ver}` 入日志;同一 `(host,port)` **全局唯一**由一个 gate 持有(互斥由日志提交保证,不再依赖 `127.0.0.1` 绑定失败这种软判据)。
- gate 由 ingress 角色的 supervisor 循环维护:按日志中属于本 host 的绑定集合 ensure/abort;绑定变更(含 `ver` 变化)即重建后端列表(替换 ensure 时的后端快照语义,`src/lvs.rs:91,94`)。
- `IngressUnbind`(销毁/迁移的首步)保证"先摘流量再停数据层"的既有依赖链语义(`docs/xenon-create-design.md:98-100`)。
- `StopLvs` 在错误进程"打空"的缺陷由**路由到 gate 所在进程**消除(§8.4)。

### 10.2 入口 HA

- 业务入口 = **多地址清单**(DNS 多 A / LB 后端 / 每 host 一条 `ip:port`),与既有 `ingress[]` 模型对接(`docs/physical-multi-site-ops.md:321-332`)。
- 入口故障域 = host;host 故障 → controller 检测(ingress 绑定心跳) → 提交**新的** `IngressBind`(从存活 host 的端口池分配) → 客户端重连到新地址。目标 RTO ≤ 5s(检测 ≤ 2s + 提交 ≤ 1s + 客户端重连 ≤ 2s)。
- 同一实例可同时暴露**多个入口**(不同 host),提升入口层可用性;前端"连接信息"按入口清单展示(既有字段扩展,不改页面结构)。

### 10.3 端口权威分配

- `PortAlloc{host, port, instance, node}`:以 `hp/<host>/<port>` 唯一键做 CAS 插入,冲突即试下一个;**分配即占用**(消除进程内计数器 + bind 探测的 TOCTOU,`src/instance.rs:713-729`)。
- 分配域按 Host:`35000..60000` 为默认池,`rds_hosts` 的 `next_port` 仅作**提示位**(真实占用以状态机为准);已存在但未接线的 `store.host_alloc_port`(`src/store.rs:2501-2510`,非原子:UPDATE 与 SELECT 两次独立连接)由状态机版取代。

### 10.4 明示极限

单 VIP 无缝漂移(连接不中断、地址不变)需要内核级 IP 接管(keepalived/ipvs)或网络侧 LB,属**部署层增强**,不在本仓库代码范围。本设计的承诺口径是:**入口层可用性 = 多地址 + 快速重绑,RTO ≤ 5s,客户端需重连**。该口径写入 UI 与文档,避免过度承诺。

---

## 11. 读路径:一致性档位与端点归属

### 11.1 三档一致性

| 档位 | 语义 | 实现 | 适用 |
|---|---|---|---|
| `linearizable`(默认) | 读到你之前的所有已提交写 | 网关路由到 shard leader,leader 以 **lease-read**(leader 租约内)或 **read-index**(向多数派确认 commit index ≥ read_index)后本地读 | 实例详情、操作前校验、权限、任务状态 |
| `read_index` | 线性一致但允许 follower 承担 | follower 向 leader 取 read_index,等本地 apply 到位后读 | 列表/概览等高 QPS 读 |
| `stale` | 允许滞后(可配置上界) | follower 本地状态机直读,响应带 `X-Rdsctl-Staleness: <ms>` 与页面标记 | 大盘、容量、慢查、报告、审计分析类 |

- **降级必须显式**:任何因少数派/leader 不可用而降低档位的响应,必须带标记(响应头 + JSON 字段),C6。
- 一致性档位选择由**端点白名单**决定(不是客户端任意指定):写操作前置校验恒为 `linearizable`。

### 11.2 端点归属(逐类)

| 端点类 | 读源 | 档位 | 备注 |
|---|---|---|---|
| `/api/rds/instance(s)`、`/summary`、`/proxies`、`/hosts`、`/task(s)`、`/query/caps`、`/orch/*` | shard 状态机(网关本地副本)+ 事实层缓存 | `linearizable`/`read_index` | 权威字段不再读 MySQL(替换 `src/instance.rs:747-804,1250-1345`) |
| `/api/auth/me`、`/users`、`/roles`、`/permissions` | 状态机 | `linearizable` | 每请求不再查库(替换 `src/http.rs:295`) |
| `/audit`、`/alerts*`、`/timeline`、`/slow*`、`/capacity*`、`/report(s)`、`/monitor/*` | sink 投影(keyset 分页) | `stale` | 明确标注"分析视图,可能滞后 ≤ 投影延迟" |

### 11.3 分页与热点

- 列表统一 **keyset 游标**(`after=<key>&limit=`),替换 offset + 内存排序/`truncate`(`src/instance.rs:793-796`)与"取 20 万行再内存切片"(`src/api.rs:644-651,1317-1324`)。
- 每端点定义**最大返回量**与游标有效期;无参全量端点(`/summary`、`/insights`、`/report`)必须支持 `?region=&shard=` 限定或改为异步导出。
- 静态页:加 `ETag`/`Last-Modified` + 压缩(现状每请求 clone 593KB 且 `no-store`,`src/http.rs:313-315,440`);网关层可缓存。

### 11.4 会话、授权与请求上下文

- 会话与 RBAC 入日志状态机(C8):任一网关副本服务任意 Cookie;**补 logout/撤销端点**(当前全仓库无 logout);冻结即时生效(不再依赖逐请求查库)。
- 权限快照不固化在会话里:每请求从本地状态机取当前权限(角色变更即时生效)。
- **请求上下文显式传参**替换 `thread_local` actor:修 `src/auth.rs:17-19` 跨 await 失效导致审计记 `system`(`src/query.rs:699-701`)的缺陷。契约:所有需要审计归属的函数签名显式接收 `&Ctx`。

---

## 12. sink 投影与重建

### 12.1 定位

sink(元数据库,默认 MySQL,兼容既有页面/报表/慢查/容量表)是**只读投影**,不是权威:

- 写路径:日志提交 → apply → **入队投影**(内存队列,批量化)→ 异步写 MySQL;投影失败重试,不阻塞提交(消除现状"每句 SQL fork 进程 + 10–50ms"对热路径的影响,`src/store.rs:1005-1029`)。
- 水位:`sink_progress{shard, applied_index, applied_at}`(sink 侧表),用于重放补齐与滞后监控。
- `RDSCTL_METADATA_SINK=none`:完全不写 sink(全自持);此时分析类页面明确不可用(前端按 API 返回标记降级)。

### 12.2 投影幂等

- 每类投影带唯一键(如 `instances(name)`、`audit(task_id,action,seq)`、`alerts(instance,kind,state)`),重放即覆盖/忽略(修 `alert_open`/`slow_gov_ensure` 的 check-then-insert 与三张无唯一键表)。
- 投影按 `(shard, index)` 单线程顺序回放,保证同一 key 的最终值 = 日志顺序的最后一个(避免乱序覆盖)。

### 12.3 审计不丢(C10)

- 决策审计随 op 提交(§6.2 `AuditAppend`),持久化在日志/快照里 → 即使 sink 长期不可达也不丢,可用 `rdsctl admin resync-sink --from-log` 全量重建。
- 现状"审计写失败只 warn 不上抛"(`src/store.rs:1328`)在 cluster 模式下不适用于决策审计(它不再是 fire-and-forget)。

---

## 13. 横向线性扩展论证与容量模型

### 13.1 吞吐分解(谁限制什么)

| 负载 | 现状瓶颈 | cluster 模式 | 扩展轴 |
|---|---|---|---|
| 生命周期写(创建/销毁/扩容/迁移) | 单进程 + 每语句 fork mysql | 共识提交(批 fsync)+ 执行面并行 | **shard 数**(S 个 leader 并行);量级极小(§13.2) |
| 巡检探测(最大负载) | 单进程顺序循环 | 队列 + worker 池并行;**探测不入日志** | **副本/worker 数**(近线性) |
| UI 读(QPS) | 单进程全量内存 + 每请求查库 | 本地状态机只读 + keyset 游标 + ETag | **副本数**(近线性) |
| 分析查询(audit/slow/capacity/report) | 单库(单机 MySQL) | sink 分片(region×shard,`docs/scaling-design.md:65`) | sink 分片数 |
| 共识投票/心跳 | 无 | 每 shard 组独立,心跳批量 | **shard 数** |

### 13.2 容量模型(数字自洽性检查)

以 10 万实例为设计点(`docs/scaling-design.md:50`):

- **shard 数**:`ceil(100000/10000) = 10`(每个 1 万实例,符合单 shard 基线)。可按需提高(每 shard 更小 → 更多写并行)。
- **元数据体积**:实例骨架 ≈ 2–5KB(节点/代理/分片/标签/绑定);10 shard × 1 万 × 5KB ≈ **500MB/controller**(全副本放置),含快照 ×2 ≈ 1GB 磁盘/节点。可接受;超过 **2GB** 触发告警并进入"分片放置 v2"评估。
- **生命周期写速率**:按 `docs/scaling-design.md:50` 的 ~100 次/实例/月 → 10 万 × 100 / 2.6e6 s ≈ **3.8 op/s**(全局),分摊到 10 个 shard ≈ 0.4 op/s/shard → 共识完全不是瓶颈。
- **巡检探测速率**:10 万 / 30s ≈ **3.3k 探测/s**(全局);每探测 = 1 次 agent 调用(本机/远端) + 事实层写(内存)。3 副本 × 每副本 8 并发 worker ≈ 24 并发 → 单探测 100ms 量级可满足;扩展方式 = 加 worker/副本(近线性)。
- **状态跃迁写速率**:假设 0.1%/周期 → ≈ 3 op/s → 入日志无压力。
- **读 QPS**:现状每标签页 6–8 req/2.5s(`src/rds.html:11650-11713`)≈ 3 req/s/标签页;100 个并发标签页 ≈ 300 req/s,单副本本地状态机读可承担,再加副本线性扩展。
- **日志增长**:op ≈ 4/s × 200B ≈ **0.7KB/s**(≈ 60MB/天/集群) → 压实压力可忽略;快照周期由"距上次快照时长"为主(`§7.3` 的 30min)。

### 13.3 不可线性化清单(必须显式承认)

1. **sink 单库写入带宽**(审计/样本/报告膨胀):靠分片 sink + 批量 + 保留策略;它是唯一可能"加副本不涨"的部分。
2. **单实例自身**:同一实例的生命周期操作天然串行(这是特性不是缺陷);并行度来自实例数。
3. **跨 shard saga**:批量/跨机房迁移的协调开销随批次线性,不随副本数下降。
4. **成员变更**(§5.6):计划性停写,期间写不可用(读可用)。
5. **日志压实 I/O**:必须分 shard 错峰,否则与在线流量争 I/O。
6. **全局目录类只读聚合**(region 目录/全局审计检索):M1.5 用只读汇聚层解决,不进主路径。

### 13.4 度量与验收基线

- 指标:共识提交延迟 p50/p99、fsync 次数/op、leader 切换耗时、接管耗时、fence 拒绝计数、`sink_lag_index`、各端点 p99、探测吞吐/s。
- 线性验收(验收文档给出脚本化步骤):固定 3 万实例,副本数 1→2→4:探测吞吐与 API QPS **每翻倍 ≥ 0.7×**(即效率 ≥70%);p99 不得劣化 >20%;shard 数 1→8:写吞吐近线性。
- 明确记录"未达标项"的处理路径(先扩 shard,再查 sink,再查 agent 调用扇出)。

---

## 14. 故障模式矩阵

| 场景 | 期望行为 | 不变量 | 用户可见 |
|---|---|---|---|
| 1 个 controller 崩溃(3 副本) | 多数派仍在;leader 若在其中 → 1.5–3s 内新 leader;租约不受影响 | C1,C2 | 写可能瞬时 503 + `Retry-After`;读正常 |
| 少数派失联(1/3) | 写继续;失联节点被 `readyz` 摘除 | C1,C2 | 无感 |
| **失多数派(2/3 失联)** | 剩余节点**立即停写**(`503 quorum_unavailable`),读转 `stale` 并标注 | C2,C6 | 写不可用,读标注陈旧 |
| 分区(对称) | 少数派侧 fail-closed;多数派侧正常;恢复后少数派重新同步 | C2,C5 | 分区期少数派侧写失败 |
| leader GC 停顿/长时间调度延迟 | 旧 leader 被新 term 取代;其后续命令因 **fence 落后** 被 agent 拒绝(`409 fence_stale`) | C3 | 该操作标 `aborted(fence_lost)`,可安全重提 |
| 租约到期但 holder 仍在跑(慢步骤) | holder 续约失败即停手并 abort;若已产生副作用 → agent fence 拦不住时由**步骤账本 + 幂等命令**兜底 | C3,C4 | 任务转 failed/aborted,可续跑 |
| 时钟偏移 > `max_skew` | 该节点**拒绝授予新租约**并告警;已持租约操作正常 | A1,C1 | 告警;严重时新操作排队 |
| voter 磁盘 fsync 失败 | 节点自降级退出,守护拉起;日志损坏校验失败 → 拒绝启动 | A2 | 单节点抖动,集群不受影响 |
| 日志尾部截断(异常掉电) | 启动丢弃最后一条不完整记录并告警 | C5 | 无感 |
| 日志中间损坏(哈希链断裂) | **拒绝启动**,要求运维介入(禁止尽力解析) | C5 | 需人工处理 |
| 滚动升级 | 逐副本:stepdown → 停 → 起 → 加入;一次只动一个;写仅出现毫秒级重试窗 | C1,C2 | 无 5xx(客户端重试即成功) |
| 配置世代不一致(半新半旧) | 拒绝通信并告警 | C2 | 部署错误暴露在启动期 |
| agent 不可达 | 节点标 `remote`(**管理盲区**),不喂 ERS,不判 missing | — | 实例 degraded + 明确原因 |
| agent 返回 `409 fence_stale` | 视为接管信号:停任务、审计、允许重提 | C3 | 任务失败可重试 |
| ingress 进程崩溃 | 守护拉起;期间该入口地址不可用,业务走其它入口;超时 → controller 重绑 | — | 入口切换,RTO ≤5s |
| 入口 host 整体故障 | controller 提交新 `IngressBind`(存活 host),客户端重连 | — | 入口地址变更 |
| sink 不可用 | 投影积压(有界队列 + 磁盘落盘),权威路径不受影响;分析页标注"数据滞后" | C9 | 分析视图降级 |
| sink 落后/重建 | 按 `applied_index` 水位重放;唯一键保证幂等 | C9 | 无感 |
| 会话撤销/冻结 | 状态机 op 提交即全局生效(任一网关) | C8 | 立即失效 |
| 跨 shard 迁移中断 | saga 按 `saga_id` 续跑或补偿(幂等) | C4 | 任务中断可见、可重提 |

---

## 15. 迁移与共存(single ↔ cluster)

### 15.1 阶段化切换(不允许双权威并存)

```
① single 运行(MySQL 权威)
② 冻结写(维护窗,读可用)→ 导出 instances/tasks/hosts/RBAC/会话不导
③ rdsctl admin import --from-mysql → 写入各 shard 日志(幂等,带 ver)
④ 启动 cluster 模式(写入 mode 标记 op: mode=cluster + heartbeat)
⑤ 观察期:sink 继续投影,页面走状态机读
回滚:停 cluster → rdsctl admin export --to-mysql → 启 single(模式标记写回)
```

- **模式互斥**:cluster 模式在状态机里维护 `mode=cluster` + 心跳;`single` 模式进程启动时若发现 sink 中 cluster 心跳新鲜 → **拒绝启动**(防止"旧 single 进程 + 新 cluster"双权威)。
- 迁移期数据校验:实例数/节点数/任务数/角色一致性比对;差异清单人工确认(参考 `docs/meta-authority.md:78-90` 的漂移采纳语义)。
- `RDSCTL_MODE=single` 下,`mark_interrupted`/`clear_all_locks`/进程内 LVS/MySQL 权威**全部保留**,即有验收用例不受影响(§4.3)。

### 15.2 兼容性契约

| 项 | 契约 |
|---|---|
| API 兼容 | 现有 68 个前端调用路径不变;新增仅:一致性档位参数、`/healthz`、`/readyz`、`/internal/*`(集群内)、分页游标参数(offset 保留兼容一个版本) |
| 前端兼容 | 页面可继续 2.5s 轮询;新增"陈旧"标记与"事实刷新中"标记;分页改为游标(渐进) |
| 表结构兼容 | MySQL 表**只增列/增表**,不删列;新增 sink 水位表与唯一键(唯一键需评估既有重复行,迁移脚本先去重后加约束) |
| 运维脚本 | `scripts/deploy.sh`/`rdsctl.sh` 扩展为"副本组"部署(cluster 模式每个 id 一份 pid/log/env);`clean` 仍然通配清理(避免漏清) |
| 观测 | 每副本 `/metrics`(文本)+ `/healthz`(进程/依赖)/`/readyz`(是否可接流量:多数派可用 + 日志可写 + 档位) |

---

## 16. 阶段划分与门禁

| 阶段 | 范围 | 关键交付 | 门禁(必须全绿才进下一阶段) |
|---|---|---|---|
| **M1a 正确性内核** | 分片组共识(选举/提交/快照/恢复)+ 租约与 fence + 执行面 fence + 步骤账本;sink 投影替换权威写;**进程内启动自检 + `/healthz`/`/readyz`** | `src/ha/{log,snapshot,raft,state,lease,fence}.rs`、agent fence 头、`RDSCTL_MODE=cluster` 骨架、探针端点(契约见 `deploy/README.md` §7,自检退出码 2 与 `deploy/bin/rdsctl-preflight.sh` 对齐) | 不变量 I1–I11、I17–I19;仿真测试(时钟偏移/fsync 延迟/分区/丢包)全绿;故障注入 F1–F5、F8–F10(含 F11 守护拉起、F12 前提不达标拒绝启动);既有 `cargo test` 与 P0 验收全绿;**默认离线构建可用**(验收文档 R5) |
| **M1b 队列与调度** | 队列 op + claim/renew/requeue + 统一 watcher + leader 驱动定时职责 + 告警唯一键 | 巡检 worker 化、`dedup_key`、`aborted(lease_lost\|fence_lost)` 语义 | I13/I14 + 重复告警/重复报告行 = 0;接管用例(杀 worker) |
| **M1c 读扩展** | follower read-index + 三档一致性 + 会话/RBAC 入状态机 + 请求上下文显式化 + keyset 分页 + ETag | 端点归属表落地、`/readyz`、每请求 DB 往返移除 | I12/I15/I16;读 QPS 线性曲线;陈旧标记可断言 |
| **M1d 接入层** | `ingress` 角色 + `IngressBind/Unbind` + 入口多地址 + 端口权威分配 + agent 心跳自动登记 | ingress 进程、绑定路由、入口重绑 | 杀 ingress/入口 host 用例;`StopLvs` 路由正确性用例 |
| **M1.5 跨 region** | 每 region 一套控制面与分片 + 区域目录只读汇聚 + 跨区链路建模 | 区域网关路由、目录同步 | `docs/scaling-design.md:218-221` 的"A 区整体故障不影响 B 区"演练;跨 shard saga |
| **v2(登记,不在本轮)** | 分片放置/迁移自动化、joint config 成员变更、可达性优先读路由、审计归档到对象存储 | — | — |

实现顺序约束:M1a 必须先于其它阶段(正确性内核是一切前提);M1b/M1c 可并行;M1d 依赖 M1a 的 op 契约;M1.5 依赖 M1c 的读一致性与 sink 分片。

---

## 17. 风险、取舍与待决问题

### 17.1 风险与缓解

| ID | 风险 | 影响 | 缓解 |
|---|---|---|---|
| R1 | 自研共识的协议缺陷(最难的一类 bug) | 正确性(G1) | ①协议子集最小化(只做选举/提交/快照,不做 joint config/线性扩展特性);②**确定性仿真 harness**(单线程事件驱动 + 故障注入 + 不变量检查)作为主门禁;③关键不变量由 agent 侧 fence 独立兜底(纵深防御);④可选:用 sink 做独立审计校验器 |
| R2 | 仓库无进程守护 | G2 不成立 | 契约化运维前提(A5),文档给出 systemd/launchd 样例单元(不写入仓库脚本也可,作为部署文档);`/readyz` 供守护判断 |
| R3 | 时钟前提(A1)不达标 | C1 削弱 | 启动自检 + 持续上报 + 超限拒绝授予;`ttl ≥ 10×max_skew` |
| R4 | 全副本放置的存储/内存增长 | 单节点容量 | 阈值告警(§13.2 的 2GB);v2 分片放置 |
| R5 | 构建供给:`.cargo-home`/`vendor/` 缺失,`--offline` 实际不生效(`scripts/build.sh:74` 的判定目录不存在,`Cargo.lock` 离线不可解析) | 无法可重复构建 | **已决策并落地(选项 A)**:依赖源码入库 `vendor/`(50 包)+ `.cargo-home/config.toml` 做 source replacement;`build.sh` 改为基于 `$ROOT` 判定、供给缺失时**显式报错**而非静默联网(消除静默降级);已验证 `cargo build --offline` 与从子目录调用均成功。**残余风险**:`vendor/` 入库增加仓库体积,新增依赖须重新 vendor 并提交 |
| R6 | macOS 开发机 fsync 语义弱 | cluster 模式本地验证噪声 | cluster 模式的仿真/集成测试跑在 Linux CI 或容器;本机仅 `single`/lab |
| R7 | 运维复杂度上升(3/5 节点 + NTP + 守护 + 磁盘) | 落地门槛 | 提供"同机三进程"最小集群(端口隔离 + 独立 `DATA_DIR`)用于演练,与多机同一协议(`agent-drill.sh` 已证明同机多进程可控) |
| R8 | 迁移期双权威 | 数据破坏 | §15.1 的模式互斥 + 心跳检测 + 拒绝启动 |
| R9 | 分析视图滞后被误当权威 | 误操作 | 档位标记 + 响应头 + 页面徽标;写入前置校验恒 `linearizable` |

### 17.2 取舍(明确接受)

1. **接受控制面操作延迟上升**:写需一次多数派 fsync(批内 ~1–2ms,跨 AZ 可到数 ms~数十 ms),换来 C1/C2 的强保证;读走本地(不增延迟)。
2. **接受"入口重绑 + 客户端重连"**而非单 VIP 无缝漂移(§10.4)。
3. **接受事实层在 leader 切换后一个巡检周期内可能陈旧**(§8.1),以换取 10 万实例规模下的可行性。
4. **接受 v1 成员变更是计划性停写**(§5.6),换取实现规模可控。
5. **接受分析类数据的最终一致**(§12),它们不参与决策(任何决策依据都来自状态机)。

### 17.3 待决问题(实现前需拍板,均已给出推荐)

| # | 问题 | 推荐 |
|---|---|---|
| Q1 | voter 数固定 3 还是支持 5? | 默认 3;`--cluster` 支持 5,容量与延迟写入文档,不默认启用 |
| Q2 | 快照格式用 JSON 还是二进制? | v1 用 JSON(可人工检视、复用 serde);超过 500MB 快照时再评估二进制 |
| Q3 | `stale` 读的允许滞后上界? | 默认 5s,可配;超界则该端点回退 `read_index` 而非静默给旧数据 |
| Q4 | 大集群是否需要"每 shard 独立副本集"(分片放置)? | v1 不需要(§13.2 数字自洽);设为 v2 触发条件(元数据 >2GB 或单节点 CPU >70%) |
| Q5 | agent token 轮换是否本阶段做? | 不做,但协议留头字段;单独安全里程碑处理 |
| Q6 | 是否保留 MySQL 作为**可选**仲裁校验器(只读比对)? | 保留为诊断工具,不参与决策 |

---

## 18. 验收锚点(本阶段)

- 不变量 → 机制 → 用例的完整映射、可执行步骤与既有 harness 复用方式,见 **[docs/control-plane-ha-acceptance.md](./control-plane-ha-acceptance.md)**。
- 部署与运维物:`deploy/`(守护模板 + `rdsctl-preflight.sh` 启动自检)、**[docs/ops-guide-cluster.md](./ops-guide-cluster.md)**(前提检查表、成员变更停写规程、滚动升级、故障处置、未实现清单)。
- 构建供给(R5 决策落地):`vendor/`(依赖源码)+ `.cargo-home/config.toml`(source replacement);`scripts/build.sh` 默认基于 `$ROOT` 走离线,供给缺失即报错。

---

## 19. 实施进度(M1a)

> 按"先正确性内核、后接线"推进;每步以 `cargo test --offline` 门禁 + 文档同步收尾。

| 项 | 状态 | 说明 |
|---|---|---|
| 时钟抽象 `src/ha/clock.rs` | ✅ | `Clock` trait + `SystemClock` / `ManualClock`(仿真注入偏移与停顿) |
| 持久日志 `src/ha/log.rs` | ✅ | 记录格式 + SHA-256 哈希链 + 组提交 fsync;**尾部截断容忍 / 中间损坏拒绝启动**;压实与后缀截断(重写+原子 rename);9 项测试(含真实损坏注入) |
| 快照 `src/ha/snapshot.rs` | ✅ | JSON + 自校验哈希 + 原子写 + `snapshot.prev.json` 回退;4 项测试(含篡改检测) |
| 状态机 `src/ha/state.rs` | ✅ | KV 版本 CAS、**租约(单写者)**、步骤账本(有效一次)、审计尾、配置世代;`apply` 确定性(BTreeMap + 时间随 op 落日志);10 项测试 |
| 共识核心 `src/ha/raft.rs` | ✅ | 选举(**含 pre-vote**)、日志复制、冲突回退、提交规则、快照安装与压实、`hard_state`(term/voted_for)持久化、`ready()` 就绪模型;8 项测试含 **S1 随机分区安全扫描** |
| 确定性仿真骨架 | ✅(内嵌于 raft 测试) | 单线程事件驱动 + 手工时钟 + 可控分区/愈合;后续抽为 `ha-drill`/S2–S5 复用 |
| agent fence 头 + `fence_seen`/幂等落盘 `src/ha/fence.rs` | ✅ | 执行面**资源侧**强制(设计 §9.2):单调校验、fsync 后才执行、跨分片拒绝、键消毒防路径穿越、幂等缓存;agent 端已接入(`X-Rdsctl-Fence`/`X-Rdsctl-Idem`/409 `fence_stale`/400 畸形/`fence_required` 模式);5 项单测 + 3 项**真实进程**端到端(含重启后旧 fence 继续被拒 = I7/F5) |
| 集群运行时 `src/ha/runtime.rs` | ✅ | 成员表、启动自检(A1–A4)、tick 循环、内部 RPC(`/internal/raft`、`/internal/status`、`/internal/view`、`/internal/propose`、`/internal/state|lease|step`)、公开探针服务(`/healthz`、`/readyz`;业务 API 显式 503)、`premises_ok/premises_unverified/lab_degraded` 诚实上报 |
| `RDSCTL_MODE=cluster` + `serve` 子命令(角色/端口/自检退出码 2) | ✅ | `rdsctl serve --node-id --cluster --port --rpc-port --roles`;自检不通过 → **退出码 2**(与 deploy 脚本一致);sink 不可达时公开端口只提供探针,sink 可达时同端口起业务 API(含探针) |
| 多副本验收(F1/F2/F12 + I1/I3/I7) | ✅ | `tests/ha_cluster.rs`(真实 3 进程):选举/杀 leader 失效转移+term 提升+状态机复制到新 leader;少数派 503 且不提交;前提不达标拒绝启动;租约单写者/fence 单调/续约/释放后立即接管。**5 项全绿** |
| 管控集群运维面(`GET /api/rds/cluster` + `stepdown`/`resync-sink` + 页面) | ✅ | 全副本视图(角色/term/复制进度/就绪前提/租约台账),**服务端扇出**(单成员 700ms 并发超时,浏览器只连一个副本);`stepdown` 任意副本入口自动转发、继任者由 raft 按"已追平优先"选;`resync-sink` 仅 leader(否则 409,不假装成功);权限 `cluster.view`/`cluster.manage`;详见 [control-plane-cluster-view.md](./control-plane-cluster-view.md);验收 `cluster_view_api_and_ops` + `cluster_view_reports_single_mode_honestly` |
| 会话与 RBAC 入共识状态机(C8 / I15 / I16) | ✅ | `s/ u/ r/` 三层保留键空间(物理写入复用带版本 `Put/Delete`,零新 op、快照格式不变);会话只存 **token 哈希**;冻结/改密用 **`u/<user>.epoch` +1** 一次性作废该用户全部会话(原子、无需扫描);登录 = **read-index + 本地口令校验 + 写会话**,写后等**全部可达副本 apply**;每请求 = 本地状态机读 + **认证读屏障**(未追平即 503,fail-closed);补上 `POST /logout`(此前全仓库没有);**单机模式逐字不变**;详见 [session-rbac-consensus.md](./session-rbac-consensus.md);运维面同时给出会话台账(`auth_sessions_view`) |
| 步骤账本接 DAG、instance.rs 改经共识租约、多副本 I2/I8/I9/I10/I11 | ⏳ | 下一步(M1a 收尾) |

**部署脚本支持期发现并已修正的问题**:

10. **agent 探测漏带 token(两处)**:`src/ha/runtime.rs::probe_agent_fence` 与
    `deploy/bin/rdsctl-preflight.sh` 都用无 token 的 `/agent/ping` 探测;而 agent 配置
    `RDSCTL_AGENT_TOKEN` 时无 token 返回 **403** → 被误判"agent 不可达" → **A4 不满足 → 集群拒绝启动
    (退出码 2)**。实测触发:同机演练起 agent 后节点起不来。已修:两处探测都带上 token(query 参数)。
11. **脚本里 `:-` 只取值不赋值**:`cluster.sh` 用 `${RDSCTL_AGENT_TOKEN:-lab-token}` 启动 agent,
    但后续就绪探测判空时变量仍未设置 → 探测不带 token → 同样误判失败。已改为在 `do_up` 里
    `: "${RDSCTL_AGENT_TOKEN:=lab-token}"` 统一落定。
12. **启动拒绝信息把"已放行"也列成失败**:lab 开关放行后,A1/A4 仍被打印成失败项。已新增
    `SelfCheck::blocking_failures(allow_clock, allow_agent)`(放行的不计入失败,单独以"已放行但未验证"
    提示),`/readyz` 的 `premises_unverified` 保持如实标注。
13. **脚本自身的三处缺陷**(演练一次就暴露):`ha-drill.sh` 引用未定义的 `STATE_DIR`(set -u 直接失败,
    导致"停副本"静默没生效)、端口索引 off-by-one、以及**沿用过期的 leader**(restart 后换主 →
    把真 leader 停掉留下跟随者,断言从 503 变 409)。均已修,并在脚本里改为"用时重新解析"。
14. **会话仍是"每副本进程内"的**(做管控集群页验收时实测撞到;设计上早已登记为待办,这里补的是**运维后果**):
    §2 的现状表与 C8/M1c(§11.4、§16)都写明"登录态多副本互不可见",但直到写这个页面的验收用例才具体化:
    同一 cookie 换一个副本访问业务 API 会 **401**。两个后果必须记住 ——
    ①**运维面只能服务端扇出**:浏览器连一个副本,由该副本持 cluster token 去问其它成员(`/internal/view`),
    不能让前端逐个副本拉取(否则每换一个副本都要重新登录);
    ②多副本 + 负载均衡下用户会随机被要求重新登录,需要 sticky session 过渡,直到会话/RBAC 入状态机(M1c)。
    验收用例因此对每个被**直接调用**的副本各自登录一次,并在注释里写明原因(避免下一个人把它当 bug 改掉)。
15. **过期租约没有 GC**(做租约台账时在真实集群上看到的)—— **已修**,分三层:
    ①**正常路径本来就释放**(`instance.rs::unlock_instance`;`delete_task` 拒绝删除运行中任务、
    `Scheduler::get` 回退查库,所以 `watch_task`/`watch_backup_task`/DTS/受管切换这几个循环
    必然会看到终态并释放 —— 逐个核对过,不存在"任务消失导致锁悬挂"的路径);
    ②**残留自愈**:每个副本每 5s 回收"**自己持有、已过期、且本进程未在操作**(`op_locks` 无该实例)"
    的条目 —— 只收已过期的,避开"刚授予、`op_locks` 尚未插入"的窗口;holder 门禁保证不会替别人释放;
    ③**持有者永久离场**的条目由 leader 按 **`Op::LeasePurge{cutoff_ms}`** 回收:
    `cutoff = 提议时刻 - (max_skew + 宽限)`,宽限默认 60s(`RDSCTL_LEASE_REAP_GRACE_MS`)。
    正确性:任何后续授予的 `at_ms >= 提议时刻 >= cutoff + max_skew`,故被删条目"冲突检查本来就会放行",
    回收**不削弱 §5.4**;`cutoff` 随 op 落日志、apply 只做数值比较(**不读本地时钟**),因此确定性可回放;
    无候选时不提议(不往日志塞空 op);GC 不进审计(内部收敛动作,避免淹没人的操作留痕)。
    代价(如实记录):被回收的条目若其持有者其实还活着(仅长停顿),迟到的 `LeaseRenew` 会从
    "成功续期"变成 `LeaseNotHeld` → 任务按 `lease_lost` **fail-closed** 中止,**不会双写**。
    页面侧已把"已过期"默认折叠并提供开关,避免被误读成异常。
    验收:`tests/acceptance.rs::expired_lease_reap_and_deterministic_purge`(真实 3 副本:
    自愈/隔离/GC/不误伤四段)+ `ha::state::tests::lease_purge_is_deterministic_and_equivalent_to_conflict_rule`。
16. **"已提交"被误判成"失败":本地追平窗口写死 2s(修发现 15 的回归时抓到的既有缺陷)。**
    `acquire_lease`/`renew_lease` 用 `propose_op` 拿到结果后,判据是 **leader 侧**的 `applied`,
    然后等 **本副本**状态机出现该租约,超时 **2s** 就返回内部错误。提案可能在 **follower** 上发起
    (`propose_op` 会自动转发),因此"leader 已提交、本副本还没 apply"是常态窗口 ——
    启动/高负载下超过 2s 就把一次**已经提交**的授予丢掉,表现为业务侧 `create` 直接 400
    「租约操作内部错误:授予成功但本地状态机未追平」。
    实测:`cluster_lifecycle_survives_total_restart_and_replays_idempotently` 在 10 次连跑里命中 2 次;
    把 GC 关掉同样命中 2/10 → **与发现 15 的改动无关,是既有缺陷**。
    已修:①判据改为**按 index 确定性等待**(`wait_applied_async(index, …)`,index 来自提案返回值),
    不再轮询"租约是否出现";②超时 2s → `LEASE_LOCAL_CATCHUP_MS = 5s`(正常追平是毫秒级,
    这里只是"不因抖动放弃已提交的授予");③超时错误带上 `index/applied_index/commit_index/leader`,
    一眼区分"延迟"与"真掉队",不再是一句含糊的"稍后重试"。
    修后 21/22 次连跑通过(剩余 1 次是"某副本 30s 内未见该租约",已在用例里加自诊断输出各副本水位,
    下次复现可直接定位;两次连跑未再复现)。
    **教训**:凡是"提案 + 等本地生效"的路径,都必须 (a) 用返回的 index 而不是"轮询目标状态是否出现",
    (b) 把超时错误写成可诊断的,**绝不能把已提交的写入当作失败**丢弃。
17. **同一条教训在会话/RBAC 写路径上又出现一次(实现 I15/I16 时第一版就踩中)——已按同一原则修好。**
    `put_key`/`delete_key` 一开始只看 `propose_op` 返回的 **leader 侧** `applied` 就返回成功,
    结果:①在 n3 登录成功后,立刻用同一 cookie 访问 n3 自己 → **401**(本副本还没 apply 自己的会话);
    ②在 n1 登出后,n1 自己仍认这个 cookie → **200**。修法:所有认证写入在返回前 `await_local(index)`
    —— **"leader 已提交" ≠ "本副本已生效"**,这条不变式现在写进了 `put_key/delete_key` 的公共出口,
    而不是每个调用点各自小心。
18. **"还没学到 commit"的副本挡不住(屏障的能力边界)——用写入侧等待补齐。**
    认证读屏障只能挡住"**已经知道** commit 推进但还没 apply"的窗口;一个**还没从心跳里获知**新 commit
    的副本既不会拒绝、也不知道该拒绝,于是"刚登录就被踢回登录页"(实测在 3 副本上稳定复现,
    不是理论问题:屏障在它身上必然通过)。每次请求都做 read-index 要多一个 RTT,代价不可接受。
    因此把代价移到**少量写操作**上:登录/登出写完后 `wait_visible_on_all(index, 1.5s)`
    —— 直接问每个成员 `/internal/commit-index`,等它的 `applied_index` 覆盖该 index;
    不可达的成员**如实列出**并在响应里说明生效范围("已生效 N 个副本;未确认:x,恢复后生效")。
    于是:登录/登出对**全部可达副本**生效(窗口为零);其它请求仍有 ≤1 个心跳的陈旧窗口(已记录)。
    顺带确定了 cluster 模式的一条语义:**没有多数派就没有会话** —— 登录会 503
    (`i10_cluster_start_does_not_wipe_sink_locks_or_tasks` 因此断言 503 而不是 200:
    若这里返回 200,就意味着无多数派时也能凭空签发一个谁都撤销不掉的会话)。
19. **验收环境被 pid 复用污染(排查被带偏很久的元凶,纯测试侧)。**
    `Ctx::new` 用 `std::process::id()` 命名测试库,却用 `CREATE DATABASE IF NOT EXISTS` 建库、
    并且不清理临时目录。于是:**上一次同名用例失败后残留的库/目录**,会被下一次"恰好复用了同一 pid"
    的运行继承 —— 库里残留 `t-create-1(cl1, success)` 之类记录,leader 启动时的 `resume_pending`
    会**续跑那个陈旧任务**,与本用例新建的 `cl1` **争同一个实例租约**,最终以
    `共识租约请求超时(桥接等待超时)` 收场。实测在这台机器上留下了 **95 个残留测试库**。
    已修:①`Ctx::new` **先 DROP 再 CREATE 库、先删目录再建**(无条件重建);
    ②`drop_db` 先掐掉指向该库的连接再 DROP(节点进程此时可能还没退出,活跃连接会让 DROP 失败)。
    **教训**:任何声称"独立环境"的用例,建环境必须**无条件重建**,不能用 `IF NOT EXISTS`;
    否则"偶发失败"会被误诊成被测系统的缺陷(这次就先把两次误诊写进了 §19,后来才纠正)。
20. **超时预算不变量:桥接总超时必须严格大于内部各段之和。**
    修发现 16 时把"等本副本追平"的窗口从 2s 提到 5s,却忘了它位于 8s 的桥接超时之内 ——
    最坏 5s(提案)+ 5s(追平)= 10s > 8s,于是**已经提交的租约**会被报成
    `共识租约请求超时(桥接等待超时)`,而调用方无法区分"成功但慢"与"真的失败"。
    已修:抽出 `LEASE_BRIDGE_TIMEOUT_MS = 12s` 常量并在注释里写明预算关系
    (`propose` 5s + 本地追平 5s + 转发/调度余量 < 12s),本地追平改回 5s。
    **教训**:凡是"外层等待内层"的路径,超时必须写成有预算关系的常量,不能各写各的数字。
21. **诊断成本 vs 排查成本(方法论记录)**:为定位上面两个问题,新增了两处**永久诊断** ——
    ①投递耗时 ≥500ms 的 WARN(带消息种类与条数);②pre-vote 授予的 DEBUG(带
    `距上次心跳 Xms / 选举超时 Yms / 各判据`)与"收到更高 term 即降级"的 WARN。
    这三条把"选举风暴/续跑争锁"从"看不出原因"变成"一眼可读"。
    同时**如实记录一个未完全定位的现象**:3 副本同机、连跑压测下偶发"发起方 commit_index
    落后 leader 1 条且持续 >2s"(心跳已确认携带 `leader_commit`,投递耗时告警一次都没触发);
    当前以"本地追平等 5s"容忍(它是已提交的操作,不该失败),并留诊断待后续定位。
22. **把"单程延迟"当成"时钟偏移",并被永久锁死(生产可用性事故,已修)。**
    现象:实例操作报 `实测时钟偏移 1578ms 超出上限:拒绝授予新租约(前提 A1;请修 NTP)`,
    而三副本**同机部署**、真实偏移只有 2–4ms;且该副本此后**一直**拒绝授予租约。
    根因两条:
    ① `observe_clock` 用 `sender_ms - local_ms` 单次观测估偏移 —— 该值等于
       **真实偏移 + 这条消息的单程延迟**(延迟恒 ≥ 0),于是任意一次排队/调度延迟都被算成时钟偏移;
    ② `peer_offset_ms` 是"每个 peer 一个值、只在收到该 peer 消息时覆盖",而 n2/n3 这类
       非 leader 之间**只在选举时**互发消息 ⇒ 那条被拖慢的选举消息成了**永久**观测,
       再没有新样本去覆盖它 ⇒ 该节点永久拒绝授予租约(实例操作全线失败)。
       (实测触发场景:我在跑 3 副本验收套件时 CPU 被抢占,而用户正好在同一个 lab 集群上操作。)
    已修三件:
    ① **最小延迟过滤**(NTP clock-filter 思路):每个 peer 保留 `CLOCK_SAMPLE_WINDOW=16` 个带时间戳的
       采样,取**有效期**内 `|offset|` 最小者作为偏移估计 —— 延迟只会让观测值变大,故最小者最接近真值;
       而真实偏移会出现在每个样本里,所以不会被滤掉(优于滑动平均:不被尖峰拉偏、阶跃调整也能快速反映);
    ② **采样有效期** `CLOCK_SAMPLE_TTL_MS=10s`:过期观测不再参与判定,过期即"未验证"(不是"超界") ——
       旧观测不能代表"当前"偏移,这条同时消灭了"永久锁死";
    ③ **样本不足不判定**:至少 2 个有效样本才可能判超界,单次尖峰不足以定"持续偏移"。
       判据与 `skew_measured_ms()` **用同一个值**(第一版写成"超界样本数占多数",出现了
       "上界只有 20ms 却判超界"的自相矛盾,被 `s3_f7` 用例当场抓住)。
    可观测性:新增 `skew_diag() -> (过滤值, 最新采样, 有效样本数)`,`/readyz` 输出
    `skew_measured_ms` / `skew_latest_ms` / `skew_samples`,管控集群页成员行显示
    "时钟偏移 Xms(最近采样 Yms,含单程延迟)" —— **两个值的差就是被过滤掉的延迟量级**,
    运维据此区分「真 NTP 不同步」与「消息延迟尖峰」,不再被一句"请修 NTP"带偏。
    验收:`ha::raft::tests::delayed_message_is_not_mistaken_for_clock_skew`
    (正常样本→不误判;单次 1503ms 尖峰→不判超界且诊断可见;持续 1500ms→判超界;过期→回到未测量)。
23. **本副本"收不到 commit":桥接等待占住 tokio worker(现场两次复现后定位并修复)。**
    现象(用户在真实集群上两次遇到):
    `租约操作内部错误:租约已提交(index=121)但本副本未在 5000ms 内追平(applied=120, commit=120, leader=Some("n1"))`
    —— 操作**已经在 leader 上提交**,发起方却在 5s 内一直没学到那条 commit。
    定位过程与证据:
    ① 现场把三个副本的 `/internal/status` 与 `/readyz` 拉出来对比:事后已全部追平(index=118),
       说明不是永久丢失而是**可达数秒的停顿**;
    ② 两个相关副本的 `transport_errors` 均非 0(= 至少一次投递超过 3s 上限)⇒ 有节点在秒级内不响应;
    ③ 读代码找到根因:`acquire_lease_blocking` → `bridge_block` 用 `rx.recv_timeout(12s)`
       **阻塞在 tokio 的 worker 线程上**,而**同一个进程还必须处理 leader 发来的
       `/internal/raft`(AppendEntries,携带新 commit index)** —— worker 被占住 ⇒ 本副本学不到 commit ⇒
       已提交的租约操作被判"未追平"。同理,leader 侧的 tick 循环同步 `deliver` 会因慢 peer
       被拖住(每条上限 3s),心跳停发/延迟,既会引发选举,也会让 follower 迟迟拿不到 commit。
    已修(两处,都是"共识路径不得被 API 路径饿死"这一原则):
    ① `bridge_block` 在多线程运行时里改用 **`tokio::task::block_in_place`**(tokio 会临时补充 worker,
       线程池不被饿死);非运行时线程(单测直接调用)保持原阻塞语义。
       判据用 `Handle::try_current() + runtime_flavor()==MultiThread`,避免在 current-thread 运行时上 panic。
    ② tick 循环的**出站投递改为独立任务** + `delivering` in-flight 标记(上一轮没投完就跳过本轮,
       Raft 对延迟/丢失容错,下一拍重发)⇒ 慢 peer 不再推迟心跳。
    ③ 诊断:本副本追平超过 1s 就 WARN(`本副本追平租约 index=… 耗时 …ms(applied=…, commit=…)`),
       下次复现可直接从日志复盘;投递耗时 ≥500ms 也会 WARN(发现 21)。
    验收:`ha::runtime::tests::bridge_wait_does_not_starve_the_runtime_worker` —— 在**单 worker**
    多线程运行时里,桥接调用发生在运行时任务内(= 真实情形),要求"等待期间另一个任务仍能被调度";
    **已验证该用例在去掉修复后必然失败、加回后通过**(避免写出"空判据"的假测试)。
    这条同时解释了发现 21 记录的那个"未完全定位"的现象(偶发 commit_index 落后 1 条 >2s):
    它就是本条的轻量表现,5s 容忍只是掩盖,现已按根因修掉。

**实施期发现并已修正的问题**(均记录在案,避免回退):

0. **两处自死锁(测试直接卡住暴露)**:`parking_lot::Mutex` 不可重入,而我在
   ① `acquire_lease/renew_lease` 的 `if self.node.lock().skew_exceeded() { self.node.lock()... }`
   (if 条件中的临时 guard 活到整个 if 语句结束)、② `ready()`(先 `let node = self.node.lock()`
   后又插入两处 `self.node.lock()`)都重复取锁 → **进程挂死**。
   已改为"一次取锁求值"(`skew_violation()` 助手 + `ready()` 单次取锁),并加了静态粗检脚本;
   教训:**本项目内所有 `Mutex` 保护段禁止在 guard 存活期间再次取同一锁**,含 if 条件与闭包。


1. **设计文档自身的一处不安全规则**:租约冲突授予原写作 `now < old.expire_at - max_skew` 才拒绝 —— 会让新 leader 提前 `max_skew` 授予租约。已改为 `at_ms >= old.expire_at_ms + max_skew` 才允许(§5.4),实现与测试(`lease_grant_conflict_waits_for_expiry_plus_skew`)按新规则。
2. **实现期自研共识的竞选缺陷**:初版用 `voted_for.is_none()` 推断"是否处于 pre-vote 阶段";当过个 term 投过票时该推断为假,导致 pre-vote 无法升级为正式选举(可用性),并存在绕过选举直接当选的路径(安全性)。已改为显式 `Campaign{None,PreVote,Vote}` 状态机,并由 S1 安全扫描持续看护。
3. **写入落到非 leader 的节点会被整体拒绝**(实施期发现):cluster 模式下 3 个副本里只有 leader 能提交,而 API 请求可能落在任意副本 → 实测 `create` 直接 400「本节点不是 leader」。已实现**发起侧转发**(`propose_op`:非 leader 把提案转发给 leader;RPC 处理端只用本地版,因此不会成环)。
4. **每个副本都在续跑同一批未完成任务**(实施期发现):`resume_pending` 无跨副本仲裁(M0 遗留),3 副本模式下三个进程同时执行同一任务(垫片日志里同一容器被重复 RUN/RM、LVS 端口互相顶掉,任务永不收敛)。已按设计 D5 收敛为:**只有 shard leader 可续跑** + 本进程已在跑的任务跳过 + leader 每 5s 对账接管(换主后也能接手)。
5. **投递同步化导致 leader 误判失去多数派**(最难的一个):RPC 处理端在响应前**同步投递**自己产生的出站消息,于是"心跳延迟"与对端处理耗时耦合;一旦某条投递变慢,leader 的 ack 窗口过期 → `quorum_ok=false` → **拒绝一切写入**,连它自己正在执行的 DAG 的步骤账本都写不进去 → 任务 fail-closed 失败。已修:处理端异步投递、单条投递 3s 限时、提案投递并发、quorum 窗口取 max(4×心跳,3×选举超时)。
6. **续约失败的过度反应**:把一次"桥接超时"直接当成失去租约并中止任务。已按设计语义分级:`Held/Skew`(状态机明确不在我手上)→ 立即停手;传输/多数派/换主类 → 到 TTL 才升级(租约是时间保证,抖动不等于丢失)。
7. **桥接 runtime 被阻塞 sleep 饿死**:`wait_lease_local` 用 `std::thread::sleep` 占住桥接 runtime 的工作线程(仅 2 个),并发续约时把 HTTP 请求一起拖住。已改 async sleep + worker 提到 4。
8. **副本间实例视图不一致**:`load_persisted` 会把"操作中"的实例在内存里强判 Failed(单机语义),cluster 模式下这会与 leader 的续跑对打(实测 leader 已 running、follower 仍 failed)。已加集群门禁 + **每 5s 从 sink 回灌视图**(sink 作读模型,设计 §12);实例登记迁入状态机仍属 M1c。
9. **恢复期一处状态写回遗漏**:日志尾部截断后曾直接返回(本轮解析出的 entries 未写回 `self`),表现为"恢复后日志为空"。已改为截断后重放收敛。

**当前回归基线**:`cargo test --offline --bin rdsctl` = **149 通过 / 0 失败**(R1 绿);`cargo test --offline --test ha_agent_fence` = 3/3;`cargo test --offline --test ha_cluster` = 5/5。

> 更正记录:上一轮曾报告 `instance::xenon_create::*` 有 6 项"既有失败"。经复查,那是**环境问题**——本机 MySQL 未运行(该组用例需要真实 MySQL),**不是**代码缺陷:启动捆绑 MySQL 后同一套用例全绿。教训已记:涉及 MySQL 的用例,基线必须在 MySQL 就绪的前提下取。
- 设计完成度自检:**每条决策有依据锚点**(§2 表)+ **被否方案与理由**(§5.7)+ **代价与取舍**(§17.2);**每条不变量有机制与用例**(§3 ↔ 验收文档);**无 TBD**(§17.3 的 6 个开放项均已给出推荐默认,不阻塞实现)。
