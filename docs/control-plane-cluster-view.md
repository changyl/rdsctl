# 管控集群页(control-plane-cluster-view)

> 目标:给 **rdsctl 管控面自身的 HA 集群**(`RDSCTL_MODE=cluster`,设计见
> [control-plane-ha-design.md](./control-plane-ha-design.md))一个可操作的运维页面 ——
> 看得到"谁是谁、谁在带、日志追到哪、前提是否可信、租约在谁手上",并且能做两个**语义明确**的动作。
> 前端 `src/rds.html`(`view-cluster`);后端 `GET /api/rds/cluster` + 两个 POST。
> 状态:已实现并通过验收(`tests/acceptance.rs::cluster_view_api_and_ops` /
> `cluster_view_reports_single_mode_honestly`,真实 3 副本进程)。

---

## 1. 设计原则

| 原则 | 落地 |
|---|---|
| **服务端扇出,浏览器只连一个副本** | 页面向"当前副本"要全量视图;该副本用 cluster token 去问其它成员的 `/internal/view`。浏览器不需要(也不应该)直连其它副本的 RPC 端口 |
| **不冒充别人的视角** | 每个成员一行,字段来自**该成员自己**的上报;探测失败就 `reachable=false + error`,**不用本副本视角填空** |
| **"看不了" ≠ "坏了"** | 不可达成员的其余字段一律留空/null(不写成 0),前端整行显示"本次探测无响应" |
| **前提不粉饰** | `premises_ok=false` / `premises_unverified[]` / `lab_degraded` / `skew_measured_ms` / `fsync_ok` / `log_writable` 原样呈现,lab 放行不等于已满足 |
| **动作有明确边界** | 只有两个动作,且都对齐既有语义:`stepdown`(任意副本入口,自动转发)与 `resync-sink`(**仅 leader**,否则 409) |
| **只读不重** | 视图不写库、不改共识状态;`stepdown` 让 raft 自己选继任者(不允许人工指定目标) |

---

## 2. 后端契约

### 2.1 内部 RPC(新增,仅 RPC 端口,需 `X-Cluster-Token`)

`GET /internal/view` → `{ ok, status, ready, peers, leases }`
= 把 `/internal/status`、`/readyz` 与新增的 peers/leases 视图一次取回(扇出时少一半往返)。

`status`(`src/ha/runtime.rs::status`)给拓扑与状态机规模;`ready`(`::ready`)给角色/term/leader/
commit/applied/quorum/log_writable/fsync/前提/偏移/uptime/投递计数。

`peers[]`(`src/ha/raft.rs::peers_view`,新增):每个成员一行 ——
`id / is_self / match_index / next_index / log_last_index / repl_lag / last_ack_age_ms /
clock_offset_ms`(判定值,**已按最小延迟过滤**)/ `clock_offset_latest_ms`(最新一次采样,含单程延迟;
两者之差 = 被过滤掉的延迟量级,用于区分"真时钟不同步"与"消息延迟尖峰",见设计 §19 发现 22)。

- `match_index`/`next_index`/`repl_lag` **只有 leader 维护**,非 leader 返回 `null`(不是 0);
- `last_ack_age_ms = null` 表示**本进程从未收到过该成员响应**,与"刚刚收到"(0ms)必须区分。

`leases[]`(`src/ha/state.rs::leases_view`,新增):`instance / holder / fence_term / fence_index /
`expire_at_ms / ttl_ms / renewals / remaining_ms / expired`。`remaining_ms` 由调用方传入的 `now` 计算
(不读本地时钟,便于诊断时钟偏移)。

### 2.2 `GET /api/rds/cluster`(权限 `cluster.view`)

```jsonc
{
  "enabled": true,                 // 是否真的是 cluster 模式运行时
  "mode": "cluster",               // cluster | single
  "version": "1.0.0",
  "node_id": "n1",
  "generated_at_ms": 1757000000000,
  "probe_timeout_ms": 700,
  "quorum": {
    "voters": 3, "reachable": 3,
    "ok": true,                    // 本副本自己的 quorum 判定(不是别人替它说的)
    "leader": "n2", "leader_addr": "127.0.0.1:9332",
    "term": 7
  },
  "members": [ /* 见下 */ ],
  "leases": [ /* 本副本状态机的租约台账 */ ],
  "notes": ["读法说明…"]           // 读法/限制原样呈现
}
```

`members[]` 每行:身份(`id/addr/is_self/reachable/error`)+ 共识(`shard/role/term/leader/
commit_index/applied_index/snapshot_index/config_epoch`)+ 规模(`kv_len/lease_count/data_dir/voters`)+
就绪与前提(`ready/degraded_reason/degraded_reasons/quorum_ok/log_writable/fsync_ok/premises_ok/
premises_unverified/lab_degraded/skew_measured_ms`)+ 运行(`uptime_ms/delivered/transport_errors`)+
`peers[]` + `leases[]`。

取样口径:同一成员内重叠字段(role/term/leader/commit/applied/ready)以 `ready()` 那次取样为准,
保证一份 JSON 内不自相矛盾;非重叠字段是"某次取锁时刻"的真实值,不做平滑。

**并发与超时**:对 `voters - 1` 个成员并发 `GET /internal/view`,单成员超时
`MEMBER_PROBE_TIMEOUT_MS = 700ms`,因此整体延迟 ≈ 700ms 而非 × 成员数。一个卡住的 peer
不会把页面拖慢(这正是要用**短**超时的原因)。

### 2.3 `POST /api/rds/cluster/stepdown`(权限 `cluster.manage`)

主动让位。本副本是 leader → 就地 `Node::step_down()`;否则**转发**给已知 leader 的
`POST /internal/stepdown`(带 cluster token,3s 超时)。返回
`{ok, message, detail:{stepped_down, via: local|forward, former_leader}}`。

- 继任者由 raft 按「**日志已追平优先**,其次 `match_index` 最大」选择;未追平的候选只继续复制、
  不强制转移 —— **故意不提供"指定目标"参数**:人工把未追平的副本推成主是数据风险。
- 让位后本节点推迟自身竞选(宽限期),避免"抢回领导权"导致转移失败;宽限期过后仍可参选(不永久停摆)。
- 当前无 leader → 503 `internal`("可能正在选主中"),不假装成功。
- 全程审计(`cluster_stepdown`)。

### 2.4 `POST /api/rds/cluster/resync-sink`(权限 `cluster.manage`)

把 **leader** 的 sink 投影游标置 0 → 下一轮对账(≤5s)从共识日志幂等重放补齐(设计 §12/I18)。
**不写库**、不影响共识与租约,只影响审计投影的补齐进度。

- 非 leader → **409** `{code:"not_leader", leader:"n2", error:"本副本不是 leader(当前 leader=n2):该动作只在 leader 上生效"}`。
  理由:投影循环只在 leader 跑,在 follower 上重置本地游标既不会立刻生效也不会被使用,
  返回"成功"就是假成功。
- 未以 cluster 模式运行 → **409** `{code:"not_cluster"}`。
- 全程审计(`cluster_resync_sink`)。

错误体统一:机器可判的 `code` + 人可读的 `error`(`jpost` 只透出 `error`,自动化用 `code`)。

### 2.5 权限

`src/store.rs::PERMISSIONS` 新增 `cluster.view` / `cluster.manage`(独立分组,前端 `PERM_CN.cluster = "管控集群"`)。

- 观测与实例同源可见性,但**控制面运维动作不复用 `instances.manage`**:让位/重建投影是集群级操作,
  不应随"实例管理"权限一起被授予。
- 默认 fail-closed:非 super 角色不加就没有;super 角色按既有规则展开为全量权限。

---

## 3. 前端(`#view-cluster`)

- 侧边栏 `data-view="cluster" id="nav-cluster"`;`VIEW_IDS`/`VIEW_TITLE`/`navTo`/`routeTo`/侧栏权限表/
  `v.any` 全部登记。
- **单机模式**:显示"未启用管控集群(single 模式)+ 原因",按钮区不出现 —— 没有假集群、没有空表。
- 集群概览 KPI(复用 `kpiCard`):可达副本 / 当前 leader / term / 活跃实例租约 / 本副本 commit /
  状态机条目 / 本副本 ready;下面一行"本副本要点"把前提、降级、时钟偏移、投递计数、`data_dir` 摊开。
- **成员表**:节点(含 `kv/租约/epoch/snap`)、地址、角色、term、leader、提交/应用、
  复制进度(**统一取 leader 视角**:已追平/落后 N 条 + `match` + 最近 ack + 时钟偏移)、就绪(含降级原因)、
  前提/降级、运行时长。不可达成员整行合并显示"本次探测无响应 + error"。
- **租约台账**:实例、holder、fence(term,index)、TTL、剩余、续约次数;空态明确说明
  "互斥锁只在有实例操作持有期间存在"。
- **读法说明**:后端 `notes[]` 原样列出(超时口径、`match_index` 仅 leader、`remaining_ms` 用本副本时钟等)。
- 刷新:进页立即拉一次;`tick()` 在**本页可见**时以 **5s 节流**刷新(成员扇出有成本,不跟 2.5s);
  另有手动「刷新」。失败静默(手动刷新才提示错误)。
- 两个动作都用 `riskDlg`(需键入 `stepdown` / `resync`),文案写明后果与边界;动作后延时刷新等事实收敛。

---

## 4. 限制与后续(不粉饰)

1. **会话是"每副本进程内"的(M1a 现状)。** 每个副本有独立 session store:同一 cookie 换一个副本访问会
   `401`。因此本页必须服务端扇出;而"多副本 + 负载均衡"下用户会随机被要求重新登录
   (需 sticky session,或等 M1b/M1c 把会话/RBAC 搬进状态机)。这条是**实现本页时实测撞到的**,
   已记入 `control-plane-ha-design.md` §19 发现清单。
2. **sink 不可达时页面不可用。** cluster 模式下若 sink(元数据库)连不上,进程只起探针服务
   (`serve_public`),业务 API 不启动 —— 此时只能看 `/readyz`,看不到本页。这是 M1a 的既有降级口径,
   不是本页引入的。
3. **`resync-sink` 不能远程执行。** 它操作的是"leader 进程所在机器的本地游标文件",所以只能在
   leader 上生效;要跨副本触发需要新增 `/internal/resync-sink` 转发(未做,因为当前 CLI 语义就是
   "在本机跑")。
4. **成员可达性只是"本次探测"**。700ms 没回就记不可达;网络抖动会把瞬时失败显示出来。本页
   **不代替多数派判定**:`quorum.ok` 一律是本副本自己的判定,页面上也不做"多数派健康"的推断。
5. **`term` 抖动。** 选主期间不同成员可能在不同 term,页面按成员分别显示,不做跨成员的"最大 term"合成
   (合成会掩盖真实分歧)。
6. **过期租约的回收已收口(三层,见 `control-plane-ha-design.md` §19 发现 15)。**
   ①正常路径 `unlock_instance` 一定释放;②每个副本每 5s 自愈回收"自己持有 + 已过期 + 本进程未在操作"的条目;
   ③持有者永久离场的条目由 leader 用确定性 `Op::LeasePurge{cutoff_ms}` 回收
   (`cutoff = 提议时刻 - (max_skew + 宽限)`,宽限默认 60s / `RDSCTL_LEASE_REAP_GRACE_MS`)。
   代价:被回收条目若持有者其实还活着(长停顿),其迟到续约会变成 `lease_lost` 而 fail-closed 中止(不会双写)。
   页面上**已过期默认折叠**,右上「显示已过期(N)」可展开,计数单独标出,避免误读成异常。
7. 未做:成员订阅/长连接推送(仍是轮询)、历史 term 变迁时间线、按实例过滤租约、`stepdown` 指定继任者
   (**刻意不做**)。

---

## 5. 验收

| 用例 | 覆盖 |
|---|---|
| `tests/acceptance.rs::cluster_view_api_and_ops` | 真实 3 副本:①`enabled/mode` 与 3 个可达成员;②恰一个 leader,且**与 rpc 层面一致**、`quorum.leader_addr` 与成员表地址一致;③每个成员字段齐备且 `peers` 覆盖全员,leader 的 `match_index` 为数字;④follower 视角看到同一个 leader;⑤`resync-sink` 在 follower → 409 `not_leader`、在 leader → 200;⑥从 follower 发起 `stepdown` → `via=forward` 且**领导权确实转移**、无脑裂、换主后视图指向新 leader;⑦`cluster.view/manage` 在权限目录中;⑧页面含 `nav-cluster`/`view-cluster`/`cl-rows`/`cl-lease-rows`/`cl-stepdown`/`cl-resync` |
| `tests/acceptance.rs::cluster_view_reports_single_mode_honestly` | 单机模式:`enabled=false`、`members=[]`、`quorum=null`、`notes` 非空;两个 POST 均 409 `not_cluster`(不得静默成功) |
| `tests/acceptance.rs::expired_lease_reap_and_deterministic_purge` | 真实 3 副本:①本副本持有的过期条目被自愈回收;②非持有者释放被状态机拒绝(holder 门禁);③holder 永久离场的过期条目由 leader GC 回收;④未过期条目在 3 个副本上都保留 |
| `ha::state::tests::lease_purge_is_deterministic_and_equivalent_to_conflict_rule` | 同一 op 串在两台状态机上 replay 逐字一致;`at_ms >= cutoff + max_skew` 时 purge 前后准入结论一致;cutoff 早于 expire 时回收不到任何东西 |
