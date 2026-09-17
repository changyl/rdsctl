# 承载平台可插拔接入(执行面抽象)

> 状态:**P0 抽象层 + P1 外部驱动已落地**(`src/exec/`,2026-09)。
> 节点供给(`NodeProvider`)、Host 平台字段(`rds_hosts.runtime`)、端点抽象(`InstNode.endpoint`)
> 为后续期次,见 §8 分期;本文标注了每项的**落地状态**,未落地处不写成已完成。
> 接入操作步骤见 [ops-guide-container-platform.md](./ops-guide-container-platform.md)。
> 关联:[physical-multi-site-ops.md](./physical-multi-site-ops.md)(物理机/跨机)、
> [scaling-design.md](./scaling-design.md) §3「Executor/Agent 双实现」、[m0-report.md](./m0-report.md) M1 遗留项。

---

## 1. 为什么需要这一层

改造前:控制面(instance/dag/query/slow/capacity/api)在本机直接 `docker run/exec/inspect`,
物理机跨机靠 `NodeRoute::Agent` 把 **docker 命令**经 HTTP 转给远端 agent 执行。
结果是「承载平台 = docker」被写死在 60+ 处调用点里,接入 k8s / OpenStack / 自研平台
必须改控制面代码,且容易把「另一台机器上的容器」误判成本机对象。

改造后:控制面只认**一层平台无关接口**,承载平台是它的一个实现。

```
承载平台 Substrate —— 两层(各自可插拔)
┌─ ① 节点供给 NodeProvider ────────────────────────────────  ⏳ P3 规划 ─┐
│  产出「一个可执行节点」:NodeHandle{ip, region/az/rack, 容量, agent 接入} │
│  实现:ManualProvider(物理机纳管) · NovaProvider(OpenStack) · 云主机     │
└──────────────────────────────────────────────────────────────────────┘
                                   ↓ 节点就绪(agent 已在位、Host 已登记)
┌─ ② 工作负载执行 WorkloadRuntime ─────────────────────────  ✅ 已落地 ──┐
│  在节点上管理 MySQL 工作负载:workload_id 不透明(今天 = 容器名)        │
│  DockerRuntime · AgentRuntime · ExternalRuntime ·(SystemdRuntime ⏳)   │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 2. 载体矩阵(现状)

| 承载平台 | 节点供给 | 工作负载执行 | 接入方式 | 状态 |
|---|---|---|---|---|
| 本机 docker | — | `DockerRuntime` | 内置(默认) | ✅ |
| 远端物理机 + docker | 物理机上架 + Host 登记 | `AgentRuntime` → agent 侧 `DockerRuntime` | 部署 agent + 登记 `agent_port` | ✅ |
| 远端物理机 + podman/nerdctl | 同上 | 同上(agent 侧换 CLI) | **纯配置**:`RDSCTL_CONTAINER_CLI=podman` | ✅ |
| 远端物理机 + 直装(无容器) | 同上 | `SystemdRuntime` | 内置 | ⏳ P5 |
| k8s | — | `ExternalRuntime`(Pod) | 外部驱动 shim | ✅(驱动需自备) |
| OpenStack(虚机内跑负载) | ⏳ `NodeProvider` | 虚机内 agent | 虚机内 agent + 外部驱动 | 部分节点供给 ⏳ |
| 自研平台 | 任一外部驱动 | `ExternalRuntime` | 外部驱动 | ✅ |

关键取舍:**OpenStack 走「节点供给 + 节点内 agent」而非容器层对接**。
它是 IAAS,粒度是虚机;直接按容器对接会丢故障域与容量语义。建虚机 → 引导 agent →
之后所有工作负载操作复用物理机通道(今天需人工建机;自动化建机是 P3)。

---

## 3. 代码结构与契约

```
src/exec/mod.rs        WorkloadRuntime trait / RuntimeCaps / RuntimeError / check_caps / 路由与注册
src/exec/spec.rs       ContainerSpec + docker_args_to_spec()(历史 args 兼容解析)
src/exec/docker.rs     DockerRuntime(CLI 可配;错误文案与改造前逐字一致)
src/exec/agent.rs      AgentRuntime(经远端 agent 执行,fence/幂等穿过抽象)
src/exec/external.rs   ExternalRuntime(JSON over stdio;平台方自备驱动,无需重编译)
src/exec/fake.rs       #[cfg(test)] FakeRuntime(断言控制面发出的原语序列)
src/docker.rs          **兼容门面**:保留全部历史函数签名,内部经 route_for_workload 分发
```

### 3.1 WorkloadRuntime

必需原语(平台无关):

| 分类 | 方法 |
|---|---|
| 生命周期 | `create(spec, meta)` · `start/stop/restart(id, meta)` · `remove(id, purge_volumes, meta)` · `rename(from, to, meta)` |
| 事实 | `exists(id)` · `state(id) -> ContainerState` · `health(id)` · `logs(id, tail)` |
| 执行 | `exec(id, argv)`(非零退出即错) · `exec_raw(id, argv) -> ExecOutput`(保留退出码) · `write_file(id, path, content)` |
| 网络 | `network_ensure(name)` · `network_remove(name)` |
| 接入点 | `expose(spec) -> "ip:port"` |
| 元信息 | `kind()` · `caps()` |

trait **默认实现**(driver 无需重复写,新平台接入面因此很小):
`is_healthy` · `wait_healthy` · `exec_mysql_local` · `wait_mysql_ready` · `query_table`
· `restart`(默认 stop+start;docker 覆写为 `docker restart`)。

### 3.2 能力声明与 fail-closed

`RuntimeCaps{ host_ports, bind_mounts, named_volumes, networks, exec, logs, rename, systemd_unit, raw_flags }`

任何 driver 的 `create` 第一行必须调用 `crate::exec::check_caps(self, spec)?`。
规格用到未声明能力 → `RuntimeError::Unsupported{cap}`,
错误文案固定为 `unsupported capability: <cap>(<detail>)`,**绝不静默回落到本机 docker**。

`raw_flags`:能否透传未知 docker 参数。docker CLI 天然为 `true`;
外部驱动恒为 `false` —— 未知参数一律 fail-closed(见 §4 解析器语义)。

### 3.3 路由

```
route_for_workload(container):
  扫描实例的 node_hosts 命中该工作负载?
    否  → 进程默认后端(default_runtime = RDSCTL_RUNTIME,默认 docker)
    是  → resolve_route_of → Local(默认后端) / Agent(AgentRuntime) / Unmanaged(Err,拒绝代跑)
```

`Unmanaged`(绑定了宿主但 agent 未接入)= **管理盲区**:所有原语拒绝执行,
不假装本机可达(沿用改造前语义,避免误删/误建本机同名容器)。

`dk::*` 历史调用点经门面走同一路由,因此 `query.rs` / `slow.rs` / `capacity.rs`
等原先「永远打本机 docker」的读路径,现在对绑定远端的节点会自动走 agent。

---

## 4. docker CLI 参数 → 平台无关规格

历史任务在 MySQL `task_nodes` 里存的是 `Step::DockerRun{container, args}` 的**原始 CLI 参数**,
且「自定义功能模块」允许用户填任意 flag。直接改结构会破坏向后兼容与重启续跑,故:

- `docker_args_to_spec(name, args)` 解析出结构化规格;
- `ContainerSpec.legacy_args` 保留原文,**docker driver 逐字回放**(零行为变化);
- 解析覆盖仓库实际用到的全部 flag:`--network` `--hostname` `-p [ip:]host:容器[/proto]`
  `-v source:target[:ro|rw]` `-e K=V` `--restart` `--entrypoint` `--name`;
  `-v` 的 source 以 `/` `.` `~` 开头判为宿主路径,否则为命名卷;
  镜像之后的内容全部视为容器命令(与 docker 语义一致);
- **未知 flag 收进 `spec.extra`**:docker 照旧透传;声明 `raw_flags=false` 的驱动
  在 `check_caps` 处 fail-closed 并列出参数。

> 已知取舍:未知且带取值的写法(如 `--ulimit 65535`)只保住 flag 本身。
> 对 docker 无影响(回放原文);对其它驱动反正会因 `extra` 非空而拒绝。

---

## 5. 外部驱动契约(接入 k8s / 自研的首选路径)

控制面**不重编译**即可接入:平台方提供一个可执行文件,按固定契约回答原语请求。
每请求一个子进程(与既有 docker CLI 子进程风格一致,无常驻 daemon)。

**请求**(stdin 一行 JSON):

```json
{"action":"create","spec":{...},"meta":{"fence":"0:3:42","idem":"task-1-node2"}}
{"action":"exec","id":"rds-i1-master","argv":["mysql","-e","SELECT 1"]}
{"action":"remove","id":"rds-i1-master","purge":true,"meta":{}}
```

**响应**(stdout 一行 JSON):

```json
{"ok":true,"out":"..."}
{"ok":true,"err":"...","code":124}
{"ok":false,"error":"...","code":"unsupported","cap":"host_ports"}
```

支持的 `action`(✅ 已实现 / ⏳ 规划):

| action | 载荷 | 返回 | 状态 |
|---|---|---|---|
| `caps` | — | `{caps:{...}}` | ✅ |
| `create` | `spec`, `meta` | — | ✅ |
| `start` `stop` `restart` `remove` `rename` | `id`(`remove` 带 `purge`) | — | ✅ |
| `exists` | `id` | `{exists:bool}` | ✅ |
| `state` | `id` | `{state:"Status\|ExitCode\|RestartCount"}` 或 `{state:{...}}` | ✅ |
| `health` | `id` | `{health:"healthy\|none\|…"}` | ✅ |
| `logs` | `id`, `tail` | `{out:"…"}` | ✅ |
| `exec` / `exec_raw` | `id`, `argv` | `{out}` / `{out,err,code}` | ✅ |
| `write_file` | `id`, `path`, `content` | — | ✅ |
| `network_ensure` / `network_remove` | `name` | — | ✅ |
| `expose` | `spec` | `{addr:"ip:port"}` | ✅ |
| `host_probe` | — | 节点实测事实 | ⏳ P2 |
| `node_ensure` / `node_drain` / `node_remove` | `spec` | 节点句柄 | ⏳ P3 |

**spec 结构**(`src/exec/spec.rs`):

```json
{"name":"rds-i1-master","image":"mysql:8.0","hostname":"rds-i1-master",
 "network":"rds-i1",
 "ports":[{"host_ip":"127.0.0.1","host_port":35001,"container_port":3306,"proto":"tcp"}],
 "mounts":[{"source":"xenon-data-x","target":"/var/lib/mysql","kind":"named_volume","read_only":false}],
 "envs":[{"key":"MYSQL_ROOT_PASSWORD","value":"…"}],
 "command":["--server-id","1"],"entrypoint":null,"restart_policy":null,
 "extra":[]}
```

`extra` 非空 = 含驱动无法理解的 docker 参数;**驱动应当直接拒绝**,不要猜。

**安全要求**(外部驱动以控制面同等权限运行,等价 `docker.sock` 信任级):

- 必须是**绝对路径**、是普通文件、**不能对其它用户可写**(启动自检强制);
- 仅部署在可信内网;生产需自行加 TLS / 命令白名单(与 agent 同一安全模型);
- 平台凭据留在驱动侧,控制面不持有 k8s/OpenStack 凭据;
- `meta.idem` 存在时应做幂等短路(同键重复请求回放首次成功结果)。

参考实现:`scripts/runtime/k8s-driver.sh`(kubectl;未在真实集群验证,按契约自测)。

> fence/幂等经 `meta:{fence,idem}` 传入;按 §6.3 的二分,只读 action 不带 fence。

---

## 6. agent 协议(物理机通道平台无关化)

`rdsctl agent` 启动时选定本机执行后端(`--runtime` / `--runtime-cmd` / `RDSCTL_RUNTIME`),
控制面只发平台无关原语。**平台差异由此收敛在 agent 一处**。

新增端点(全部支持 fence/幂等头):

```
GET  /agent/ping    → {host, runtime, version, caps, fence_capable, fence_required, …}
GET  /agent/caps
POST /agent/create|start|stop|restart|remove|rename|exists|state|health|logs
POST /agent/exec|exec_raw|write_file|network/ensure|network/remove|expose
```

历史端点 `/agent/run` `/agent/rm` `/agent/sql` `/agent/exec` `/agent/state` 保留为**等价别名**
(旧控制面 / 既有测试不受影响);`/agent/docker`(裸 CLI 透传)**已废弃**,
仅在本机后端为 docker 时可用,其它后端返回 `unsupported`。

失败响应带 `code`/`cap`,控制面据此还原结构化 `Unsupported`(`fail-closed` 可跨进程传递)。

### 6.1 滚动升级兼容(新控制面 ↔ 旧 agent)

新旧两端不必同时升级:**agent 先行**是推荐顺序,但新控制面遇到旧 agent 会**自动退回历史端点**,
避免升级窗口内误判:

| 原语 | 新端点 | 旧 agent 回退 | 语义差异 |
|---|---|---|---|
| `create` | `/agent/create` | `/agent/run`(规格还原为 docker 参数) | 无(旧 agent 只有 docker 后端) |
| `remove` | `/agent/remove`(+`purge`) | `/agent/rm` | **`purge` 被忽略**:连卷删除需升级 agent 或人工清卷(会打 warn) |
| `rename` | `/agent/rename` | `/agent/docker`(裸 `rename`) | 无 |
| `logs` | `/agent/logs` | `/agent/docker`(裸 `logs --tail`) | 无 |
| `exists` | `/agent/exists` | `/agent/docker`(裸 `inspect`) | 无 —— **关键安全点**:协议不匹配绝不能被当成「工作负载不存在」,否则会误判节点缺失并可能触发错误的自动切换 |
| 其余 | `state`/`exec`/`sql` | 同名历史端点 | 无 |

判定依据是 agent 对未知路径的 `"未知路径 …"` 响应(`is_legacy_agent`);
回退只发生一次,不掩盖其它错误。测试:`exec::agent::tests`(进程内假旧 agent 验证回退与安全断言)。

### 6.2 cluster 模式:本机不再有直连特权(去特权)

改造前:cluster 模式下只有 3 个 fenced 步骤(`DockerRun`/`DockerRm`/`ExecSql`)在设置了
`RDSCTL_AGENT_URL` 时经本机 agent;巡检探针、query/slow/capacity、网络与等待类步骤
**直连本机 docker** —— 也就是 A4「执行面 fence 强制」实际只覆盖一部分,执行面存在旁路。

现在:

| 项 | 规则 |
|---|---|
| 触发条件 | `cluster 模式`(已注册共识运行时)**且** 配置了非空 `RDSCTL_AGENT_URL` → `NodeRoute::Local` 也解析为 `AgentRuntime` |
| 单机模式 | 不变(本机直连),历史行为零变化 |
| lab 降级 | `RDSCTL_ALLOW_NO_AGENT=1` 且未配 agent → 保持本机直连,`/readyz` 已标 `lab_degraded` |
| 判据 | `should_use_local_agent(cluster_active, agent_url)` 纯函数(有单测) |

### 6.3 只读 / 变更的分野(去特权能成立的前提)

agent 对**变更类**端点强制 fence。若把所有经 agent 的调用都当变更,巡检(在实例租约之外、
拿不到 fence)会被整体拒掉。因此契约明确二分:

| 类别 | 端点 | fence |
|---|---|---|
| 只读 | `/agent/exists` `/agent/state` `/agent/health` `/agent/logs` `/agent/exec_raw` `/agent/expose` `/agent/caps` `/agent/ping` | **不需要** |
| 变更 | `/agent/create` `/agent/start` `/agent/stop` `/agent/restart` `/agent/remove` `/agent/rename` `/agent/network/*` `/agent/exec` `/agent/sql` `/agent/write_file` `/agent/run` `/agent/rm` `/agent/docker` | **必须** |

控制面侧对应两条通道:

- **变更 SQL** → `exec_mysql_local` → `/agent/exec`,受 fence 约束;
- **只读 SQL(巡检/事实/证据)** → `exec_mysql_ro` → `/agent/exec_raw`,免 fence
  (`r_sql_ro` 用于 `probe_node`/`probe_xenon_node`/`wait_sql`/`replica_problem`/
  `capture_degrade_evidence`/`orch/facts`)。
  边界:会改数据的 SQL(`SET GLOBAL` / `CHANGE REPLICATION SOURCE` / `STOP REPLICA`…)
  **一律不得**走 `r_sql_ro`;目前仅 `replace_node`/`migrate_instance` 的 6 处变更使用 `r_sql`。

### 6.4 fence 自动附带(不是让 60+ 个调用点手传)

`ExecMeta` 由控制面在执行面边界统一合成,避免漏传导致的线上偶发 409:

```
meta_for_workload(container)
  → owner_instance(container)            // nodes / proxies / lvs / node_hosts 精确匹配
      └ 未命中 → prefix_owner(`rds-{instance}-…`, **唯一匹配**)   // 临时容器 rnx/mgn
  → manager.call_meta(instance)          // 该实例当前租约的 fence(无租约 → 空)
```

- 调用方**已给** fence(如 `exec_step` 用 `ctx.instance` 的租约)→ 原样使用;
- 未给(门面 / 巡检路径)→ 自动补齐;
- 无租约 → 拿不到 fence → agent **拒绝变更**:这正是 A4 的意图(失败即安全)。
- `owner_instance` 只读**已初始化**的管理器(`manager_opt`),不因一次查询在单测/agent
  进程里拉起 MySQL;前缀兜底遇歧义返回 None(fail-closed,宁可不给也不给错)。

### 6.5 验收(P2 去特权)

| 断言 | 位置 |
|---|---|
| 强 fence 模式下:只读端点无 fence 放行、变更端点无 fence 409、带 fence 放行、只读不受 fence 单调性影响 | `tests/ha_agent_fence.rs::strict_mode_separates_read_only_from_mutating_endpoints` |
| 去特权判据(仅 cluster + 配 agent 时生效) | `instance::exec_route_tests::local_agent_only_in_cluster_with_agent_url` |
| 前缀兜底唯一匹配 / 歧义 fail-closed / 不触发管理器初始化 | `exec::route_tests` |

> `ha_cluster` 与 `acceptance` 的集群用例都走 `RDSCTL_ALLOW_NO_AGENT=1` 且不设
> `RDSCTL_AGENT_URL` → 命中「lab 降级」分支,行为不变,故不受本次去特权影响。

---

## 7. 与 ha(fence / 幂等 / 账本)的关系

- 所有**变更类**原语都带 `ExecMeta{fence, idem}`(即原 `agent::CallMeta`),
  经 HTTP 头 `x-rdsctl-fence` / `x-rdsctl-idem` 下发,agent 侧 `ExecutorGuard` 强制执行;
- `Step` 级步骤账本(`ledger_begin/ledger_done`)与执行面抽象正交,未受影响;
- `Unmanaged` 节点不触发自动 ERS、标 `remote` 的管理盲区语义不变。

---

## 8. 分期与落地状态

| 期 | 内容 | 状态 |
|---|---|---|
| P0 | `src/exec/` 抽象层(trait/spec/docker/agent/fake)+ `docker.rs` 门面 + 关闭 5 处绕过路由的步骤(Network*/DockerExec/WaitHealthy/WaitMysql/DtsRun) | ✅ |
| P1 | `ExternalRuntime` + 契约 + `scripts/runtime/*` + 外部驱动测试 + 新控制面↔旧 agent 回退(§6.1) | ✅ |
| P2 | **cluster 模式去特权 ✅ 已落地**(见 §6.2);物理机纳入统一模型其余部分:`host_probe` 实测事实、`rds_hosts.runtime`/`agent_version` 列、纳管 preflight | 部分 |
| P3 | 节点供给层 `NodeProvider`(Manual + OpenStack Nova)+ `node_ensure`/`node_drain` | ⏳ |
| P4 | 端点抽象:`expose()` 驱动接入点 + `InstNode.endpoint`,`alloc_host_port` 门控(解锁无 host port 平台) | ⏳ |
| P5 | `SystemdRuntime`(物理机直装,无容器) | ⏳ |
| P6 | 文档收口与既有文档交叉引用更新 | 部分(P0/P1 文档已补) |

### 已知边界(诚实清单)

1. **未落地的仍是硬编码**:`Step::WriteHostFile` 目前仍写**控制主机**本地文件
   (远端绑定场景需 P2 补 `write_file` 路由);`Step::HostMysql` 是控制主机侧探测,语义如此。
2. **数据面仍依赖宿主端口**:`127.0.0.1:{host_port}` 语义未抽象(P4),
   因此 k8s 类**无宿主机端口**的平台当前需要驱动以 hostPort/NodePort 方式暴露,
   或等待 P4 的 `endpoint` 模型。
3. **Host 表无平台字段**:`rds_hosts` 尚无 `runtime` 列(P2);
   今天的平台选择来自 **agent 自身配置**(`--runtime`),控制面通过 `/agent/ping` 观察,
   而非按 Host 记录分发。
4. **外部驱动未做真实集群验证**:`scripts/runtime/k8s-driver.sh` 是契约参考实现。
5. **性能**:`route_for_workload` 仅在存在绑定节点时扫描实例表并查 Host 列表;
   未绑定(单机是常态)不产生额外查询。实例表反向索引为 P2 优化项。

---

## 9. 验收(P0/P1)

| 项 | 命令/断言 | 结果 |
|---|---|---|
| 单元测试 | `cargo test --bin rdsctl -- --test-threads=1` | **205 passed** |
| P0 三项验收 + 查询台/功能模块 | `cargo test --test acceptance` | **15 passed** |
| agent / fence 协议(含严格模式只读/变更分野) | `cargo test --test ha_agent_fence` | **4 passed** |
| 集群面(共识/租约/fence/投影) | `cargo test --test ha_cluster` | **12 passed** |
| 抽象层契约单测 | `exec::spec::tests`(真实 flag 全覆盖)、`exec::fake`(原语序列 + caps 门控)、`exec::external`(协议往返/超时/路径安全)、`instance::exec_route_tests`(Local/Agent/Unmanaged 映射 + 未知 runtime fail-closed)、`exec::agent::tests`(旧 agent 回退 + 「协议不匹配 ≠ 工作负载不存在」安全断言) | 全绿 |
| 端到端链路 | `RDSCTL_RUNTIME=external RDSCTL_RUNTIME_CMD=tests/fixtures/fake-driver.sh rdsctl agent --port 9191` → `/agent/ping` 报 `runtime=external`;`/agent/create`、`/agent/exec`、`/agent/state` 经驱动进程往返成功;`/agent/docker` 在非 docker 后端正确返回 `unsupported` | 手工已验证 |
| 编译告警 | `cargo check --all-targets`(src 内) | **0** |

> 说明:单线程运行是为了规避**既有的**端口占用类用例互相抢占
> (`instance::port_alloc_tests`、`lvs::tests` 会 bind 固定/临时 127.0.0.1 端口),
> 与本抽象层无关。
>
> 去特权(P2)改动后的复跑:单元 205 / acceptance 15 / ha_agent_fence 4 / ha_cluster 12 全绿;
> `acceptance` 与 `ha_cluster` 的集群用例走 `RDSCTL_ALLOW_NO_AGENT=1`(不设 agent URL),
> 命中 lab 降级分支,故不受去特权影响。
>
> 两处**与本改动无关的既有偶发**已在复跑中确认:
> ① `tests/acceptance.rs::cluster_lifecycle_survives_total_restart_and_replays_idempotently`
> 在同一次 shell 里先跑满 199 个单测再跑集群验收时偶发超时(单独运行 15/15 稳定通过);
> ② `tests/ha_cluster.rs::f10_rolling_upgrade_preserves_state_and_availability`
> 在前后台并发重负载下偶发失败(连续单独复跑 2 次 12/12 通过,此前 3 次全绿)。
> 两者均为测试对时序/负载敏感,不是功能回归。

### 验收要点(供复现)

```bash
# 1) 零回归
cargo test --bin rdsctl -- --test-threads=1
cargo test --test acceptance
cargo test --test ha_agent_fence
cargo test --test ha_cluster

# 2) 外部驱动链路(不接真实平台)
RDSCTL_RUNTIME=external \
RDSCTL_RUNTIME_CMD="$PWD/tests/fixtures/fake-driver.sh" \
  ./target/debug/rdsctl agent --port 9191 &
curl -s localhost:9191/agent/ping | python3 -m json.tool   # runtime=external

# 3) CLI 兼容平台(podman/nerdctl)等价性
RDSCTL_CONTAINER_CLI=podman ./target/release/rdsctl agent --port 9191
```
