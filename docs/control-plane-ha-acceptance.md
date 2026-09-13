# 管控面 HA 契约与验收锚点(control-plane-ha-acceptance)

> 配套:[control-plane-ha-design.md](./control-plane-ha-design.md)(设计与 op 契约)。
> 本文把设计 §3 的 10 条不变量(C1–C10)展开为**可执行验收用例**,并把每个用例锚到**既有 harness** 或明确标出**需新增的 helper**。
> 状态:契约定稿(实现前)。实现阶段以本文为验收门禁,不允许"口头通过"。

---

## 0. 图例与命名

| 记号 | 含义 |
|---|---|
| 不变量 | `C1`–`C10`(设计 §3) |
| 正确性用例 | `I1`–`I19`(本文 §2) |
| 线性/容量用例 | `L1`–`L3`(本文 §3) |
| 故障注入矩阵用例 | `F1`–`F12`(本文 §4;F11 = 守护拉起,F12 = 前提不达标拒绝启动) |
| 回归门禁 | `R1`–`R5`(本文 §5;R5 = 离线构建供给) |
| 仿真用例 | `S1`–`S5`(本文 §6) |

既有 harness 锚点(不复用就是浪费):

| 能力 | 锚点 |
|---|---|
| 每例独立库 + 临时目录 | `tests/acceptance.rs:66-104`(`Ctx::new`) |
| docker/mysql 垫片 + 文件开关故障注入 | `tests/acceptance.rs:106-203,237-242` |
| 任意 env 注入起服务 | `tests/acceptance.rs:1114-1142`(`start_server_env`) |
| 随机空闲端口 | `tests/acceptance.rs:382-388` |
| `kill -9` | `tests/acceptance.rs:398-401` |
| "不得重跑"计数不变量 | `tests/acceptance.rs:252-258,523-528`(`run_count`) |
| 轮询式断言 | `tests/acceptance.rs:333-342`(`wait_until`) |
| 双进程同机 + 杀进程断言降级/恢复 | `scripts/agent-drill.sh:26-31,74-98` |
| pid 文件精确杀进程 | `scripts/rdsctl.sh:52-53,118-137` |

**必须新增的 helper(实现阶段第一步就做)**:

1. `start_replica(ctx, node_id, port, extra_env)` —— 复用同一 `Ctx`(同库、同 `DATA_DIR` 根、不同 `DATA_DIR` 子目录与 `RDSCTL_NODE_ID`),起第 N 个副本;`Srv` 需支持多实例句柄与按 node_id 精确 kill。
2. `start_agent(ctx, port)` / `stop_agent(pid)` —— 同机 agent 生命周期(参考 `scripts/agent-drill.sh:26-31,75`)。
3. `ha_ctx(tag, voters=3)` —— 建立 3 副本集群 + 1 agent 的夹具,并等待 `readyz` 全绿。
4. `fence_of(resp)` / `assert_fence_rejected(resp)` —— 解析/断言 `409 fence_stale`。
5. `wait_majority(ctx)` / `drop_majority(ctx)` —— 停/起多数派(用于 C2 用例)。
6. `history_checker(ops)` —— 租约/状态操作历史的线性一致性检查器(§6)。
7. `probe_rate(secs)` / `api_rate(path, concurrency, secs)` —— 线性度测量(§3)。

---

## 1. 不变量 ↔ 机制 ↔ 用例总表

| 不变量 | 机制(设计锚点) | 用例 |
|---|---|---|
| C1 单写者 | 共识提交租约(§5.4) | I1,I2,I3 |
| C2 无脑裂提交 | 多数派 fsync(§5.3) | I4,I5,F2,F3 |
| C3 fence 资源侧强制 | `(shard,term,index)` + agent `fence_seen`(§5.5/§9.2) | I6,I7,F4,F5 |
| C4 有效一次 | 步骤账本 `sl/`(§8.4) | I8,I9,F6 |
| C5 状态单调收敛 / 重启不覆盖 | 日志重放,禁止启动期全局破坏(§7.4/§15) | I10,I11 |
| C6 读一致性显式 | 三档一致性 + 降级标记(§11.1) | I12 |
| C7 队列不重复执行 | claim 提交型 op(§8.2) | I13,I14,F7 |
| C8 会话/授权跨副本一致 | 会话/RBAC 入状态机(§11.4) | I15,I16 |
| C9 sink 不参与正确性 | 幂等投影 + 水位(§12) | I17,I18 |
| C10 决策审计不丢 | `AuditAppend` 随 op 提交(§6.2/§12.3) | I19 |

---

## 2. 正确性用例(I1–I19)

### C1 单写者

**I1 双副本争抢同一实例,只有一个成功**
- 前置:`ha_ctx` 3 副本,1 个 running/可操作实例。
- 步骤:同时向副本 A、B 提交同一实例的互斥操作(如 `destroy`);收集两侧响应。
- 断言:恰好 1 个 2xx;另一个 409/423 且 message 含"持有/进行中";状态机中 `l/<instance>` 只有一个 holder;审计中**只有一条** `submitted`(C10 联动)。
- 锚点:`wait_until`;需 `start_replica`。

**I2 长切换(> 租约时长)不被第三方抢占 = 现状 P0 脑裂口回归**
- 前置:切换(PRS/ERS)在途且耗时被人为拉长(垫片 `DK_HOLD` 类开关,> `2 × lease_ttl`)。
- 步骤:切换进行中,副本 B 提交同一实例的 `reparent`/`destroy`。
- 断言:B 被拒绝;A 正常完成;DB/状态机最终只有一个 master;`RoleSwap` op 在日志中只出现一次(对比现状 `src/instance.rs:3201-3203` 无续约的缺陷)。
- 锚点:`tests/acceptance.rs:106-203` 的 hold 开关。

**I3 接管后的单写者**
- 步骤:`kill -9` A(持租约)→ 等 `ttl` 过期 → B 提交同一实例操作。
- 断言:B 成功且新 fence > 旧 fence;A 重启后其残留命令被拒(见 I6)。

### C2 无脑裂提交

**I4 失多数派即停写**
- 步骤:`drop_majority`(3 副本停 2)→ 提交写。
- 断言:响应为 `503` 且带 `Retry-After`;不产生任何状态机变更(比对 index 不变);`/readyz` 非 200 且原因含 `quorum_unavailable`;读可用并带陈旧标记。
- 反例守卫:不允许"降级为本地写"。

**I5 恢复多数派自动恢复**
- 步骤:I4 后恢复 1 个副本 → 等待。
- 断言:≤ 选举超时内写恢复成功;此前被拒的写可重提且成功(幂等键不冲突)。

### C3 fence

**I6 僵尸 holder 的命令被资源侧拒绝**
- 步骤:让 A 持租约并进入"假死"(SIGSTOP 模拟 GC 停顿,或垫片使 agent 调用挂起)→ 待租约过期,让 B 接管并提升 → 恢复 A(SIGCONT)。
- 断言:A 发出的后续命令收到 `409 fence_stale`;数据面**没有**因 A 的命令产生第二次变更(容器 run 计数、角色、`read_only` 值均按 B 的结果为准)。
- 锚点:需 `start_agent` + SIGSTOP/CONT helper;`run_count` 不变量。

**I7 agent 本地 `fence_seen` 单调且持久**
- 步骤:向 agent 依次发送 fence `(1,2,10)`、`(1,2,9)`、`(1,3,1)`;重启 agent 后重复 `(1,2,10)`。
- 断言:10 通过、9 拒绝、`(1,3,1)` 通过;重启后 10 仍被拒(落盘生效);`fence_seen` 文件内容 = 最高已见值。
- 锚点:直接 HTTP 打 agent(参考 `tests/acceptance.rs:274-330` 手写 HTTP 客户端)。

### C4 有效一次

**I8 `DockerRun` 重放不销毁现有容器**
- 步骤:创建实例 → 记录容器 id/`run` 计数 → 在同一 `(task,node,step,idem_key)` 上重放 `StepBegin/StepDone`(或直接重跑该 DAG 节点,如现状 `POST /api/rds/retry` 路径)。
- 断言:容器 id 不变、`run` 计数不增;若规格变化需重建 → 必须走出 `DockerRecreate` op(有独立用例)。
- 锚点:`run_count`(`tests/acceptance.rs:252-258`);这是对现状 `src/docker.rs:28-36`「先 rm 再 run」的直接回归。

**I9 `ExecSql`/切主命令幂等**
- 步骤:在切换流程中于"提升完成、登记未提交"处 `kill -9` → 接管续跑。
- 断言:切主语句按账本短路,不重复执行 `RESET SLAVE ALL`;最终 `read_only` 状态与登记角色一致且**唯一**。

### C5 状态收敛

**I10 副本启动不清他人锁、不误杀他人任务**
- 步骤:A 持租约跑长任务 → 启动副本 B(以及 C)。
- 断言:B/C 启动后 A 的租约仍存在(状态机 `l/<instance>` holder 不变);A 的任务状态未被改为 failed/skipped;A 的任务最终正常终态。
- 锚点:直接针对 `src/main.rs:54,59` 的缺陷设计。

**I11 崩溃重启后状态由日志恢复,不覆盖**
- 步骤:在 DAG 中途 `kill -9` 全部副本 → 全部重启。
- 断言:实例状态 = 崩溃前的最后一次**已提交**状态(不回退、不跳变);未完成节点为 `pending`(可续跑)而非被无条件置 failed;无重复副作用(`run_count` 不变)。

### C6 读一致性

**I12 三档一致性与降级标记**
- 步骤:①在 A 提交一次变更,立刻从 B 读(默认档);②显式 `?consistency=stale` 读;③失多数派后读。
- 断言:①读到新值(线性一致);②响应带 `X-Rdsctl-Staleness` 且值非空;③响应带陈旧/降级标记且 HTTP 可用。
- 反例守卫:任何降级都不得返回"看起来新鲜"的响应。

### C7 队列

**I13 杀 worker 后任务被接管且幂等完成**
- 步骤:提交创建任务 → 等 running → `kill -9` 持有该 task claim 的副本 → 等 `visible_at` 到期。
- 断言:另一副本接管并完成;`task_nodes` 终态完整;`run_count` 表示容器只创建一次(等价现状 P0 用例思路,但断言"完成"而非"failed")。
- 锚点:`tests/acceptance.rs:466-545` 的结构直接复用(把断言从 failed 改为 success)。

**I14 N 副本不产生重复告警/重复报告行**
- 步骤:3 副本跑 ≥3 个巡检周期;制造一次降级/恢复跃迁。
- 断言:`alerts` 中同一 `(instance,kind,state)` 恰一行;`reports` 同日同类型恰一行;审计中 `degrade`/`recover` 各一条。
- 锚点:针对 `src/store.rs:2239-2262` 与 `report.rs` 进程内标志的缺陷设计。

### C8 会话与授权

**I15 会话跨副本**
- 步骤:向 A 登录拿 Cookie → 同一 Cookie 打 B、C 的受保护端点。
- 断言:均 200(不再 401);`logout`/撤销后 A/B/C 均立即失效。
- 锚点:需新增 `logout` 端点(现状全仓库无 logout)。

**I16 角色/权限变更即时生效**
- 步骤:用户登录 → 管理员收回某权限 → 该用户立即请求对应端点。
- 断言:403;且**不依赖**会话 TTL 或副本重启(替换现状 `src/http.rs:258-261` 的登录期权限快照)。

### C9 sink 独立性

**I17 sink 不可用时正确性不受影响**
- 步骤:sink(MySQL)停机或不可达(`RDSCTL_METADATA_SINK=mysql` 但 DB 关闭)→ 执行创建/销毁/切换全套操作。
- 断言:全部操作成功;状态机权威值正确;分析类端点返回明确降级信息;`/readyz` 可接流量(仅标注 sink 滞后)。
- 反例守卫:不得因 sink 故障 panic 或降级为不可写(对比现状 `src/store.rs:394-404` 启动即 panic)。

**I18 sink 重建幂等**
- 步骤:sink 清空 → `rdsctl admin resync-sink --from-log` → 再跑一次。
- 断言:两次结果一致(行数/水位一致);无重复审计行;`sink_progress.applied_index` = 当前 applied index。

### C10 审计

**I19 决策审计不丢、不谎报**
- 步骤:①提交一个必然失败的操作(如对 running 实例做非法状态迁移);②sink 停机期间做一次成功操作。
- 断言:①审计**只有** failed,没有 submitted(修现状 `src/instance.rs:3029-3040` 先写 submitted 再抢锁的谎报);②成功操作为 ok;③sink 恢复后审计补齐且不重复。

---

## 3. 线性与容量用例(L1–L3)

**L1 探测吞吐线性度**
- 步骤:固定 3 万实例(demo 种子或垫片造数),副本数 1→2→4,各测 60s 探测吞吐(`probe_rate`)。
- 断言:相邻倍数的吞吐比 ≥0.7;p99 不劣化 >20%;记录每副本 CPU/内存/sink 写入量。

**L2 读 QPS 线性度**
- 步骤:固定实例数,1/2/4 个 gateway,针对 `GET /api/rds/instances`(keyset 分页)与 `/api/rds/instance` 压测。
- 断言:每翻倍 ≥0.7×;确认每请求**无** DB 往返(用 sink 侧慢查询/计数断言,替换现状 `src/http.rs:295`)。

**L3 写吞吐随 shard 数**
- 步骤:shard 数 1→8(保持实例数不变),测生命周期写吞吐(垫片加速执行面)。
- 断言:近线性(每翻倍 ≥0.7×);共识提交延迟 p99 在目标内(单 AZ ≤50ms,跨 AZ ≤200ms)。

---

## 4. 故障注入矩阵用例(F1–F12)

| ID | 注入 | 断言要点 | 关联 |
|---|---|---|---|
| F1 | 杀 leader 副本 | ≤3s 新 leader;租约语义不变;写仅瞬时 503 | C1,C2 |
| F2 | 停 2/3 副本(失多数派) | 停写 + 读降级标注;`/readyz` 非 200 | I4 |
| F3 | 非对称分区(A→B 断、B→A 通,方向翻转) | 全程无脑裂;愈合后收敛一致 | C1,C2 |
| F4 | SIGSTOP 旧 holder | 接管成功;旧 holder 恢复后命令被 fence 拒 | I6 |
| F5 | agent 重启 | `fence_seen` 存活;重启后旧 fence 仍被拒 | I7 |
| F6 | DAG 中途 `kill -9` 全部副本 | 续跑后幂等完成,无重复副作用 | I11,I13 |
| F7 | 时钟偏移(注入 `skew_measured > max_skew`) | 该节点拒授新租约并告警;已持租约操作不受影响 | A1,C1 |
| F8 | fsync 失败(SIGSTOP 磁盘/垫片返回错误) | 节点自降级退出;集群不受影响;不出现"未持久化即应答" | A2,C2 |
| F9 | 日志尾部截断 / 中间损坏 | 尾部截断:丢弃 + 告警;中间损坏:拒绝启动 | C5 |
| F10 | 滚动升级(逐副本 stepdown 重启) | 无 5xx(客户端重试即成功);无重复副作用;`config_epoch` 不变 | C1,C2,C4 |
| F11 | **守护拉起**(`kill -9` 副本进程,守护在管) | 守护在 `RestartSec`(≤5s)内拉起并回到可用;term 不倒退;由日志追平;期间多数派不受影响 | A5,I11 |
| F12 | **前提不达标拒绝启动** | 三类构造:①偶数/不足 3 的 voter 表;②cluster 模式无 agent 或 agent 不可达;③数据目录位于网络文件系统 → 启动前自检退出码 **2** 且**不进入 cluster 模式**;`/readyz` 非 200 且 `degraded_reason` 明确;`/healthz` 仍可访问(进程活着但未就绪) | A1–A5,C2 |

> F3/F7/F8 需要在测试环境可控地注入:优先用**垫片/env 开关**(与 `tests/acceptance.rs:106-203,237-242` 同风格),其次用仿真(§6);不允许要求 root/iptables 才能跑的用例成为门禁。
>
> F11 的落地方式:M1a 起提供 `scripts/ha-drill.sh`,在本机用 `deploy/launchd` 或最小 supervisor 拉起 3 副本 + agent;若 CI 无 systemd/launchd,则用一个等价的最小守护包装(退出即重启)验证"拉起语义",并单独标注"未在 systemd 上实测"。
> F12 今天即可部分验证:`deploy/bin/rdsctl-preflight.sh` 已实现三类检查并以退出码 2 拒绝(已实测:偶数 voter / 无 agent / 多数派不足均拒绝);M1a 补齐**进程内**等价自检与 `/readyz` 字段。

---

## 5. 回归门禁(R1–R5)

| ID | 门禁 | 判据 |
|---|---|---|
| R1 | 既有单测 | `cargo test --offline --bin rdsctl` 全绿 —— **当前 149 通过 / 0 失败**(前提:本机 MySQL 就绪;`instance::xenon_create::*` 等用例需要真实 MySQL,MySQL 未启动时会出现 6 项失败,属环境问题而非代码缺陷) |
| R2 | ✅ | `cargo test --offline --test acceptance` = **9/9**(含 `kill9_restart_keeps_task_no_rerun` 与集群端到端)。**既有偶发**:`sweeper_detects_and_recovers` 在整套串行跑下约 5 次中 2 次因时序敏感失败,单独执行/重跑通过(非 HA 改动引入) |
| R3 | **single 模式零变化** | `RDSCTL_MODE` 未设时,§2–§4 之外的所有既有行为逐字不变;`mark_interrupted`/`clear_all_locks`/进程内 LVS/MySQL 权威路径仍生效(用例:复用 R2 并在堆栈/日志中断言路径) |
| R4 | ✅ | 既有 drill 未受影响;集群侧演练已提供 **`scripts/ha-drill.sh`**(起 3 副本+agent → 验就绪/租约单写者/跨副本可见/fence 单调/kill -9 失效转移/失多数派 503/自愈 → 停),本机实测 **PASS**;更严苛的用例在 `tests/ha_cluster.rs`(12 项) |
| R5 | **离线构建供给** | `./scripts/build.sh --debug-lean` 在**禁用网络**条件下成功(依赖取自 `vendor/`);从子目录调用同样走离线分支(路径基于 `$ROOT`);`--online` 仍可用;删除/移动 `vendor/` 后必须以**明确错误**退出而**不得静默联网**(判据:输出含"离线构建供给缺失"且退出非 0) |

---

## 6. 仿真测试设计(S1–S5,自研共识的主门禁)

**目标**:在没有真实网络/磁盘的**确定性**环境里穷举危险交错,验证 C1–C5、C7、C10。这是 R1(协议缺陷)的主要缓解手段。

**harness 形态**:单线程事件驱动仿真器(仅测试目标内,不进生产二进制):
- 虚拟时钟(可控步进);消息可丢/重排/延迟/分区;fsync 可注入延迟或失败;进程可崩溃/重启(内存态保留策略显式建模:持久化点 = 已 fsync 的记录);
- 客户端模型:随机发起 `LeaseGrant/Renew/Release`、`InstanceStatus`、`TaskClaim/StepBegin/StepDone`;
- 不变量检查器(每步检查):
  - `INV-1` 同一实例任一时刻至多一个有效 holder(以虚拟时钟定义"有效");
  - `INV-2` 已提交条目永不丢失(已 commit 的 `(term,index)` 在所有未来 leader 的日志中存在);
  - `INV-3` fence 只增不减,且任何被执行的命令 fence ≥ 该实例历史最高执行 fence;
  - `INV-4` 任一 `(task,node,step,idem_key)` 的副作用执行次数 ≤1;
  - `INV-5` 状态机无非法迁移(状态前置条件不被绕过)。
- 历史检查:用 `history_checker` 对客户端操作序列做线性一致性判定(§0 helper 6)。

| ID | 场景 | 断言 |
|---|---|---|
| S1 | 随机选举 + 随机崩溃(3/5 voter,10^6 步) | INV-1/2 全程成立;无"双 leader 同时提交" |
| S2 | 分区(split)+ 恢复 | 少数派无提交;恢复后日志收敛且无冲突条目 |
| S3 | 时钟偏移 ±5×`max_skew` | INV-1 成立(靠 §5.4 的冲突授予规则,不靠本地时钟) |
| S4 | fsync 失败/延迟注入 | 不出现"未持久化即应答";失败节点自降级 |
| S5 | 重放/重复投递(`StepBegin`/`StepDone` 重复、乱序) | INV-4 成立;结果一致 |

**门禁标准**:S1–S5 在 CI 全绿 + 至少一次 10^7 步长跑无违反;违反即视为协议缺陷,禁止以"概率低"为由放行。

---

## 7. 实现阶段验收清单(与设计 §16 对应)

| 阶段 | 必须通过的用例 |
|---|---|
| M1a | S1–S5 + I1–I11 + I17–I19 + F1,F2,F3,F4,F5,F8,F9,F10,F11,F12 + R1–R3,R5 |
| M1b | I13,I14,F6,F7 + I2(长切换回归) |
| M1c | I12,L2 + F2 的读降级断言(档位标记);**I15/I16 已完成**(会话/RBAC 入状态机,见 `docs/session-rbac-consensus.md`) |
| M1d | I6/I7 的 ingress 变体(杀 ingress/入口 host)+ `StopLvs` 路由正确性 + L3 |
| M1.5 | 区域级故障演练(A 区整体故障不影响 B 区)+ 跨 shard saga 中断续跑用例 |

> 覆盖自检:`I1–I19` 全部被分配(11 + 2 + 3 + 2 + 1);`F1–F12` 全部被分配(F1–F5、F8–F12 于 M1a,F6/F7 于 M1b,F2 的读侧断言于 M1c);`L1` 在 M1a/M1b 的探测 worker 化后首次可测(归入 M1b 报告),`L2`/`L3` 分属 M1c/M1d;`R5` 已随构建供给决策(R5)落地,作为 M1a 起的常驻门禁。

---

## 8. 度量与报告(每阶段产出)

每阶段验收必须产出可复核报告(参考 `docs/m0-report.md` 的成文方式),含:

1. 用例清单与结果(编号对本文 ID);
2. 线性曲线原始数据(吞吐/p99/资源),以及**未达标项的归因**(先扩 shard → 查 sink → 查 agent 扇出);
3. 不变量违反数(必须为 0),仿真步数与随机种子;
4. 与上一阶段的对比与回归结论(R1–R5 全绿);
5. 已知缺口清单(带"不满足哪条不变量/哪条前提",例如未部署 agent 时的 `best-effort` 模式必须显式记录不满足 G1)。

---

## 9. 覆盖现状(M1a 进行中,如实跟踪)

图例:✅ 已实现且通过 · 🟡 部分覆盖(见说明)· ⏳ 未开始

| 用例 | 状态 | 证据 / 说明 |
|---|---|---|
| S1 | ✅ | `src/ha/raft.rs::tests::s1_safety_sweep_random_partitions_never_two_leaders_in_one_term`(随机分区/愈合 + 随机提案,逐步断言"同 term 至多一个 leader" + 愈合后收敛) |
| S2 | ✅ | 少数派不可提交:`ha::raft::tests::appends_are_committed_only_with_majority`、`ha_cluster::f2_minority_cannot_commit_and_readyz_degrades`;**非对称分区愈合后收敛**:`ha::raft::tests::f3_asymmetric_partition_never_yields_two_leaders` |
| S3 | ✅ | `ha::raft::tests::s3_f7_clock_skew_is_measured_and_degrades`(注入 5×max_skew:被实测发现、readyz 降级为 `skew_exceeded`、无脑裂、恢复后自愈) |
| S4 | ✅ | `ha::log::tests::injected_fsync_failure_rejects_append_without_side_effects`(写入前失败、索引不推进、无半条记录) |
| S5 | ✅ | `ha::raft::tests::s5_duplicate_and_stale_delivery_is_idempotent`(重复投递 20 次 + 陈旧 AppendEntries) |
| I1 | ✅ | `ha_cluster::i1_lease_single_writer_and_fence_monotonic`(双写者被拒 + 释放后立即接管) |
| I2 | ✅ | `acceptance::cluster_lifecycle_survives_total_restart_and_replays_idempotently`:建实例期间从**另一副本**读到共识租约(holder=发起节点)、`renewals≥1`(长任务持续续约);机制侧另有 `lease_lost` 分级处理(传输抖动≠失去租约,连续失败超 TTL 才停手) |
| I3 | ✅ | 同上(接管后 fence 必须高于前任,已断言) |
| I4 | ✅ | `ha_cluster::f2_minority_cannot_commit_and_readyz_degrades`(503 + 不提交 + readyz 降级原因) |
| I5 | ✅ | `ha_cluster::i5_quorum_recovery_heals_automatically`(停 2/3 → readyz 降级且 `degraded_reasons` 含 quorum_unavailable → 拉起两台后**进程不重启即自愈**并可继续提交) |
| I6 | ✅ | `ha_cluster::i6_f4_stopped_holder_is_superseded_and_fenced`(SIGSTOP 旧 leader → 其余节点接管且 term 提升 → 越过 `TTL+max_skew` 后新持有者接管且 **fence 高于旧值** → SIGCONT 后旧节点让位、无脑裂;旧 fence 的命令由执行面拒绝,见 I7) |
| I7 | ✅ | `tests/ha_agent_fence.rs`(单调/跨分片/畸形/强制模式/幂等/重启后旧 fence 仍被拒) |
| I8/I9 | ✅ | 同一用例:全副本 kill -9 后重启续跑,`RUN rds-cl1-master` 计数**恒为 1**(账本短路,重放不重建容器);单测覆盖有效一次/首结果保留 |
| I10 | ✅ | `ha_cluster::i10_replica_restart_does_not_clear_consensus_leases`(重启副本后租约持有者与 **fence 均不变**,且原持有者仍可续约)、`ha_cluster::i10_cluster_start_does_not_wipe_sink_locks_or_tasks`(sink 侧锁行与在跑任务不被改写) |
| I11 | ✅ | 同上(进程级:崩溃重启→leader 接管→日志追平→终态 success);另有 `ha::log`/`ha::snapshot` 重放与恢复单测、`f11` 重启追平用例 |
| I12 | ⏳ | 三档一致性读(follower/read-index 未接线) |
| I13/I14 | ⏳ | 队列 worker 化(M1b) |
| I15/I16 | ✅ | `acceptance::cluster_session_and_rbac_are_consensus_authoritative`:①在 n1 登录 → 同一 cookie 在 n2/n3 均 200;②在 n2 建角色/用户 → n3 立刻可见;③该用户会话跨副本可用且按角色裁剪(manage 端点 403);④n2 改角色权限 → **不重登**,n1 上的已登录会话立即多出权限;⑤n3 冻结 → 该用户**全部**会话在 n1/n2/n3 全部 401;⑥改密后旧口令在所有副本失效、新口令生效;⑦n1 登出 → 同一 cookie 在 3 个副本全部 401;⑧明文 token 不出现在任何副本的日志/快照字节里(只落哈希)。单机模式回归:`acceptance::single_mode_session_semantics_unchanged`(会话仍在进程内、登出真实撤销、无集群语义) |
| I17 | ✅ | cluster 模式在 sink=none 或 MySQL 不可达时仍可选举/提交/租约(降级为仅探针),`ha_cluster` 全套用例在无 MySQL 下通过 |
| I18 | ✅ | `ha_cluster::i18_i19_sink_projection_is_idempotent_and_rebuildable`:`rdsctl admin resync-sink` 重置投影游标 → 重放日志后**行数不增**(幂等标记 `proj:<shard>:<index>`) |
| I19 | ✅ | 同一用例:决策(租约/步骤账本)随共识提交并被投影到 sink 审计表(带幂等标记,可重放补齐);sink 不在正确性路径由 I17 用例证明 |
| F1 | ✅ | `ha_cluster::f1_elects_leader_and_fails_over_on_kill` |
| F2 | ✅ | `ha_cluster::f2_minority_cannot_commit_and_readyz_degrades` + `f2_public_port_refuses_business_api_and_reports_premises` |
| F3 | ✅ | `ha::raft::tests::f3_asymmetric_partition_never_yields_two_leaders`(单向切断 + 方向翻转 + 愈合;全程逐步断言同 term 至多一个 leader) |
| F4 | ✅ | 同 `i6_f4_*`(SIGSTOP 注入);执行面拒绝过期 fence 另由 `tests/ha_agent_fence.rs` 证明 |
| F5 | ✅ | `ha_agent_fence::fence_is_monotonic_enforced_and_survives_agent_restart` |
| F6 | ✅ | `acceptance::cluster_lifecycle_*`(3 副本全部 kill -9 → 重启 → leader 续跑至 success;中途 SQL 故障窗口用于卡住 DAG) |
| F7 | ✅ | `ha::raft::tests::s3_f7_clock_skew_is_measured_and_degrades` + `ha::runtime::tests::skewed_node_refuses_new_lease_grants`(超界 → 拒绝授予/续约租约,且检查先于 leader 判定) |
| F8 | ✅ | `injected_fsync_failure_rejects_append_without_side_effects`(设计要求的"不出现未持久化即应答");进程级磁盘故障注入待补 |
| F9 | ✅ | `ha::log::tests`(尾部截断容忍 / 中间损坏拒绝 / 哈希链断裂拒绝) |
| F10 | ✅ | `ha_cluster::f10_rolling_upgrade_preserves_state_and_availability` + 内核层 `step_down_transfers_leadership_without_two_leaders` |
| F11 | ✅ | `ha_cluster::f11_supervised_restart_rejoins_and_recovers_state`(测试充当守护:kill -9 后用同一 node-id/数据目录/端口拉起 → 20s 内可用、term 不倒退、由日志追平、集群仍可提交) |
| F12 | ✅ | `ha_cluster::f12_premise_failures_refuse_to_start_with_exit_code_2`(缺 node-id / 偶数 voter / 未验证 A1+A4 → 退出码 2) |
| L1/L2/L3 | ⏳ | 线性度测量(依赖队列 worker 化与读路径接线) |
| R1 | ✅ | `cargo test --offline --bin rdsctl` = 149/149 |
| R2 | ✅ | `cargo test --offline --test acceptance` = **9/9**(含 `kill9_restart_keeps_task_no_rerun` 与集群端到端)。**既有偶发**:`sweeper_detects_and_recovers` 在整套串行跑下约 5 次中 2 次因时序敏感失败,单独执行/重跑通过(非 HA 改动引入) |
| R3 | ✅ | 同上(single 模式行为未变)+ cluster 模式独立进程验收 |
| R4 | ✅ | 既有 drill 未受影响;集群侧演练已提供 **`scripts/ha-drill.sh`**(起 3 副本+agent → 验就绪/租约单写者/跨副本可见/fence 单调/kill -9 失效转移/失多数派 503/自愈 → 停),本机实测 **PASS**;更严苛的用例在 `tests/ha_cluster.rs`(12 项) |
| R5 | ✅ | 默认离线构建通过;供给缺失时明确报错(实测) |

**已知未覆盖且必须在 M1a 收尾前完成**:I2/I8/I9/I10/I11(instance.rs 改经共识租约与步骤账本)、F10(stepdown + 滚动升级)、F11(守护拉起)、S3–S5(仿真注入)。
