# rdsctl — RDS 管控平台

> 基于容器的 MySQL 实例**全生命周期管理**平台:
> DAG 任务调度 + 平台无关的容器编排执行面 + 自带多数派仲裁的管控面高可用 + DBA 数据服务。

`rdsctl` 用一个 Rust 进程把「建实例 → 用实例 → 治实例」
串成一个闭环:所有变更动作都被建模为可序列化、可重放、可审计的 **DAG Step**,
掉电/被杀后任务状态与实例状态可恢复。

- **实例** = 专属 docker 网络 + MySQL 主/N 从 + (可选)newproxy 代理 + (可选)LVS 接入 VIP;
- **控制面持久化** = 本机/外部 MySQL(任务、节点、审计、实例、RBAC、快照……);

默认入口:`http://127.0.0.1:9113/rds`。管理员账号由部署配置(`RDSCTL_USER` / `RDSCTL_PASS`)决定。

---

## 1. 核心能力

### 1.1 实例生命周期

| 能力 | 说明 |
| --- | --- |
| 创建 | `single`(单节点)/ `async`(主从异步)/ `sync`(主从半同步)/ `xenon`(Raft 高可用,可选前置 newproxy);`OceanBase`(占位)|
| 多分片 | `shard_num=N` 生成 N 个独立分片组(每组主 + 从,按复制模式决定读从/离线从) |
| 扩容 | `scaleout` 追加 读从 / 离线从 / 统计节点,以主节点为复制源 |
| 销毁 / 删除 | `destroy`(停容器保记录)与 `instance/delete`(删记录)分离,页面二次确认 |
| 状态机 | `creating → running ⇄ paused/scaling/switchover/maintenance → running`,`failed`/`degraded`/`destroyed` 等终态明确 |
| 互斥 | 实例级操作锁(单机:进程内快路径 + 存储层 TTL 租约;集群:共识租约),控制并发操作 |
| 健康巡检 | 周期 sweeper(默认 30s)探测容器/复制/服务状态,异常时触发降级与自愈 |
| 暂停 / 启用 | 实例级 `enable` / `paused` 控制 |
| 替换 / 迁移 | `replace_node`(同名重建 + GTID 追平 + 切流 + 回滚)、`migrate`(跨机房) |

### 1.2 DAG 任务调度(`src/dag.rs`)

- 节点 = 一组**可序列化 Step**(`ExecSql` / `RunCommand` / `WriteFile` / `WaitMysqlReady` / `RemoveContainer` / `Noop` …);
- 无依赖节点并发执行(tokio JoinSet),依赖驱动下游;失败节点的下游全部跳过;
- 节点级重试 / 超时;任务级取消(节点间检查 cancel 标志);
- 任务、节点、状态全部落库:**进程被 `kill -9` 后重启,未完成任务标记 failed 且不重跑**;
  设 `RDSCTL_RESUME_TASKS=1` 则改为**续跑**(已完成节点不重跑,配合幂等 Step 安全);
- **功能模块**:页面上自定义「真实可执行、可复用」的步骤序列并一键对运行中实例执行
  (审计 `module_run`);与 backup 同类——加短锁、失败不改实例状态、可在任务中心展开重试单步。
  详见 [docs/func-modules.md](docs/func-modules.md)。

### 1.3 数据面拓扑与接入层

- **Proxy**:newproxy 容器编排(创建/配置下发/指标采集);
- **LVS/VIP**:离线环境无法拉取 haproxy 等镜像,接入层落地为 **rdsctl 进程内 4 层 TCP 转发器**
  (每实例一个 listener,round-robin 转发到各 Proxy 发布端口,后端故障自动跳过),生命周期由 DAG Step 驱动、
  巡检兜底重建。详见 [docs/lvs-v0.md](docs/lvs-v0.md);
- **DTS / canal**:占位容器链路 + 列表/创建/移除接口。

### 1.4 复制事实与受管切换

- `src/orch.rs` 解析 `SHOW SLAVE STATUS` / 半同步状态为结构化 **Fact**,按复制模式给出 loss 预算与候选取舍;
- 受管主从切换(PRS/ERS)+ 回滚(`orch/reparent`、`orch/rollback`)、Xenon Raft 的 `trytoleader`;
- 元数据权威分层(登记 / 事实 / 切换)与收敛机制见 [docs/meta-authority.md](docs/meta-authority.md)。

### 1.5 DBA 数据服务三件套

| 模块 | 能力 | 设计文档 |
| --- | --- | --- |
| Web 查询台 | 按实例直达数据面(`master`/`read`/`offline`),默认只读 + 独立写权限;词法护栏 + deny 名单、专用低权账号懒供给、超时/字节/行数/并发/长度护栏、敏感列掩码;结果行**永不落库** | [dba-console-design.md](docs/dba-console-design.md) |
| 全局慢查治理 | 采集 `performance_schema` digest,跨实例按 digest 归一、差分增量落库;规则聚合 → 治理队列(open/ack/resolved);**只存规范化 digest,绝不存原始 SQL** | [slow-query-design.md](docs/slow-query-design.md) |
| 备份节点注册联动 | 本地 outbox + 后台投递 worker,向外部备份平台发 register/heartbeat/backup_result 事件;幂等键 + 指数退避;平台不可达只积压重试,**不阻断实例状态机**;默认关闭=零行为 | [backup-link-design.md](docs/backup-link-design.md) |

### 1.6 可观测与运营治理

- **洞察(规则版,零 LLM)**:异常原因归一化 + 同因实例群聚类、playbook 目录(仅建议)、异常快照摘要;
- **容量与成本**:主节点磁盘采样(最小二乘外推增速 / `days_to_90pct`)+ 水位预警 + 回收候选;
- **自动报告**:UTC 定时的日报/周报(基础段 + 慢查 Top3 / 容量预警 / open 告警群),保留清理;
- **值班问答**:三层语料(静态 runbook + 现场证据)模板化中文回答,确定性纯函数,不访问网络;
- **告警中心**:告警列表 / 指派 / 处置 / 告警群治理;
- **审计**:全量 `audit_log`(操作人/实例/动作/参数/结果/任务),支持按时间线追溯。

### 1.7 安全

- RBAC:用户 / 角色 / 权限三张表 + 权限门禁按**每条路由**登记(`instances.view/create/destroy/scaleout/manage`、
  `tasks.view/manage/cancel`、`audit.view`、`users.manage`、`roles.manage`、`alerts.view/handle`、
  `cluster.view/manage`、`instances.query` …);
- 会话:8 小时 TTL,`HttpOnly` Cookie;单机模式为进程内会话 + 每请求查库校验冻结状态(冻结即时生效);
- 口令:`salt:SHA-256`,零依赖 `src/sha256.rs`;
- **实例口令不下发**——任务定义会落库并经 `GET /api/rds/task` 返回,因此内置步骤一律不写明文:
  步骤里 `pass` 留空 = 执行瞬间取实例统一口令(env `RDSCTL_ROOT_PASS`),配置模板用哨兵
  (`__RDSCTL_ROOT_PASSWORD__` 等)占位、落盘时才替换;备份命令直接读容器自身
  `MYSQL_ROOT_PASSWORD`。回归测试见 `src/instance.rs::secret_not_persisted`;
- **集群模式会话/RBAC 入共识状态机**——任一网关副本可服务任意 Cookie,冻结/改密/改权对所有副本立即生效,
  登录走 read-index 线性一致读,登出等待所有可达副本生效。详见 [docs/session-rbac-consensus.md](docs/session-rbac-consensus.md)。

### 1.8 执行面可插拔(`src/exec/`)

控制面对「工作负载跑在哪里」只认一层 `WorkloadRuntime` 接口,接入新平台**不改控制面代码**:

| 方式 | 适用 | 代价 |
| --- | --- | --- |
| A. 环境变量 | docker / podman / nerdctl 等 CLI 兼容平台 | 改一个变量,分钟级 |
| B. 远端 agent | 物理机/跨机(控制端经 HTTP 把 docker 命令交给目标宿主机上的 agent) | 部署一个 agent 进程 |
| C. 外部驱动可执行文件 | k8s / OpenStack / 自研平台 | 实现 stdin/stdout JSON 契约,无需重编译 rdsctl |

未实现的后端**一律 fail-closed**,绝不静默回落本机 docker(启动自检失败 → 退出码 2)。
详见 [docs/container-platform-abstraction.md](docs/container-platform-abstraction.md) 与
[ops-guide-container-platform.md](docs/ops-guide-container-platform.md)。

### 1.9 管控面高可用(`RDSCTL_MODE=cluster`, `src/ha/`)

- **自带多数派仲裁**:自研分片组 Raft(vote 持久化 / pre-vote / 快照 / 日志哈希链 + fsync),不依赖外部
  DB 高可用做仲裁;3/5 副本,少数派可读、写 fail-closed;
- **租约 + fence**:实例互斥权威从"进程内锁"升级为共识租约,执行面按 fence 单调拒绝过期持有者;
- **无状态网关**:角色拆分为 `gateway,controller,ingress`,任一网关副本可服务任意会话;
- **启动前提门禁**:时钟同步 / fsync / voter 奇数≥3 / agent 可达 / 网络档位实测,**不达标拒绝启动(退出码 2)**,
  lab 放行开关会体现在 `/readyz` 的 `premises_unverified` 标注上;
- **探针**:`/healthz`(进程存活)与 `/readyz`(ready / quorum_ok / leader / premises);
- **sink 降级**:元数据库不可达时降级为「仅探针 + 内部 RPC」模式,共识与租约不受影响(明确降级而非假装可用)。

设计与验收锚点:[control-plane-ha-design.md](docs/control-plane-ha-design.md)、
[control-plane-ha-acceptance.md](docs/control-plane-ha-acceptance.md)、
[control-plane-ha-topology.md](docs/control-plane-ha-topology.md)、
[ops-guide-cluster.md](docs/ops-guide-cluster.md)。

---

## 2. 项目结构

```
rdsctl/
├── Cargo.toml                 零外部服务依赖;依赖已全量 vendor
├── rdsctl.env.example         运行配置模板(复制为 rdsctl.env 生效)
├── src/                       管控服务源码(单 crate)
│   ├── main.rs                入口:模式解析 + 全局管理器 + 后台任务 + cluster 自检
│   ├── http.rs                手写 HTTP/1.1 服务:登录/会话/权限门禁/路由表/探针
│   ├── api.rs                 HTTP 接口实现(60+ 端点)
│   ├── instance.rs            实例模型 + 生命周期编排 + 巡检 + Step 执行器(最大模块)
│   ├── dag.rs                 通用 DAG 调度器(可持久化)
│   ├── docker.rs              容器执行**门面**(保留旧签名,内部路由到执行面抽象)
│   ├── exec/                  承载平台抽象层
│   │   ├── mod.rs             WorkloadRuntime 路由 + 启动自检
│   │   ├── spec.rs            ContainerSpec + docker CLI 参数解析(兼容历史任务 JSON)
│   │   ├── docker.rs          DockerRuntime(本机/物理机,CLI 名可配 podman/nerdctl)
│   │   ├── agent.rs           AgentRuntime(远端物理机端点)
│   │   ├── external.rs        外部驱动(子进程 stdin/stdout JSON 契约)
│   │   └── fake.rs            测试垫片
│   ├── ha/                    管控面 HA 正确性内核
│   │   ├── mod.rs             模块边界与约束
│   │   ├── raft.rs            分片组共识核心(与传输层解耦,可确定性仿真)
│   │   ├── runtime.rs         集群运行时:tick/RPC/探针/会话权威/租约/对账
│   │   ├── state.rs           状态机(KV 版本 CAS、租约、队列索引、步骤账本)
│   │   ├── log.rs             本地持久日志(哈希链 + fsync + 尾部截断容忍)
│   │   ├── snapshot.rs        快照(原子写 + 版本 + 恢复)
│   │   ├── projection.rs      审计投影(日志 → sink,幂等标记)
│   │   ├── fence.rs           执行面 fence 单调性
│   │   ├── auth.rs            会话/RBAC 的共识权威表示
│   │   └── clock.rs           时钟抽象(真实/手工;注入偏移与停顿)
│   ├── store.rs               持久化层(StoreBackend trait:MySQL / Memory 双后端)
│   ├── query.rs               DBA 查询台引擎(分类/护栏/掩码/审计)
│   ├── slow.rs                全局慢查采集与聚合
│   ├── backuplink.rs          备份平台注册联动(outbox + worker)
│   ├── capacity.rs            容量与成本(采样 + 外推 + 水位)
│   ├── insights.rs            规则版洞察(归一化/聚类/playbook/报告文本)
│   ├── report.rs              自动日报 / 周报
│   ├── ask.rs                 规则版值班问答
│   ├── orch.rs                vtorc 式复制事实层(P1 骨架)
│   ├── lvs.rs                 进程内 L4 接入转发器(LVS/VIP)
│   ├── agent.rs               远端执行 agent 进程
│   ├── auth.rs                操作人上下文(审计用)
│   ├── sha256.rs              零依赖 SHA-256
│   ├── rds.html               管控台单页(视图见 §4)
│   └── login.html             登录页
├── tests/                     集成/验收测试(真实 MySQL + 假 docker 垫片 + 真实多副本进程)
│   ├── acceptance.rs          P0/集群/页面 API 验收
│   ├── ha_cluster.rs          共识集群仿真与契约
│   ├── ha_agent_fence.rs      agent + fence 契约
│   └── fixtures/fake-driver.sh
├── scripts/                   本机交付链路(编译/MySQL/启停/集群/演练/清场)
│   ├── lib.sh build.sh mysql.sh rdsctl.sh deploy.sh
│   ├── cluster.sh ha-drill.sh agent-drill.sh orch-drill.sh
│   ├── migrate-drill.sh replace-drill.sh destroy-all-containers.sh
│   └── runtime/k8s-driver.sh  外部驱动参考实现(kubectl)
├── deploy/                    生产守护与自检
│   ├── bin/rdsctl-preflight.sh
│   ├── systemd/rdsctl@.service, rdsctl-agent@.service
│   └── launchd/com.rdsctl.node1.plist, com.rdsctl.agent.plist
├── docs/                      设计与运维文档(索引见 §7)
└── vendor/                    离线构建的依赖源码(配合 .cargo-home 的 source replacement)
```

---

## 3. 快速开始

```bash
# 一键:release 离线编译 + 捆绑 MySQL 就绪 + 启动服务 + 健康检查
./scripts/deploy.sh

# 追加 P0 验收(单元 + kill-9/并发/巡检 等集成)
./scripts/deploy.sh --test
```

完成后打开 <http://127.0.0.1:9113/rds>,用部署配置 `RDSCTL_USER` / `RDSCTL_PASS` 指定的管理员账号登录
(首次部署后请立即改密)。

常用命令:

```bash
./scripts/build.sh                 # 只编译(默认 --offline;--debug/--online/--clean)
./scripts/rdsctl.sh status|logs    # 服务状态 / 日志
./scripts/mysql.sh status|info     # 控制面 MySQL(自动识别外部实例 vs 捆绑实例)
./scripts/cluster.sh up --with-agent --lab   # 同机 3 副本集群 + agent(开发/演练)
./scripts/ha-drill.sh              # 端到端集群演练(租约/fence/失效转移/失多数派 fail-closed/自愈)
./scripts/deploy.sh clean          # 停全部本脚本管理的实例(保留 MySQL 数据)
```

> **离线可构建**:依赖源码已 vendored 在 `vendor/`,`.cargo-home/config.toml` 做 source replacement,
> `cargo build --offline` 可完整构建,无外网亦可交付。新增/升级依赖用 `./scripts/build.sh --online` 后重新 vendor。
>
> **两套托管不要混用**:开发/lab 用 `scripts/`(nohup + pid),生产用 `deploy/`(systemd / launchd)。
> 同一进程被两个 supervisor 抢会出问题。

---

## 4. 管控台页面

单页 `src/rds.html`,侧栏视图:

| 视图 | 内容 |
| --- | --- |
| 概览 | 全局统计与容量/告警摘要 |
| 实例 | 列表(状态/复制模式/标签筛选)、创建(单节点 / 高可用版 / Xenon Raft / OceanBase 占位)、详情与拓扑 |
| 详情 | 节点与复制拓扑、连接信息、Proxy/LVS、监控、备份、慢查、时间线、操作记录 |
| 分片 | 多分片实例的分片级视图与操作 |
| 任务 | 任务中心(DAG 节点状态/重试单步/取消)、节点组目录、**新建编排**、**功能模块** |
| 节点状态 | 跨实例汇总每个 DB / Proxy 节点的状态事实 |
| Proxy | Proxy 集群列表、配置下发、指标 |
| 主机 | Host 注册表、分配/清理、替换节点、迁移 |
| 集群 | 管控面自身 HA:副本与 leader、日志追进度、前提是否可信、租约归属;两个语义明确的操作(stepdown / resync-sink) |
| 洞察 | 异常聚类、playbook 建议、报告 |
| 告警 | 告警列表/处置/告警群治理 |
| 审计 | 全量审计检索 |
| 用户 | 用户 / 角色 / 权限管理 |

前端刷新:改 `src/rds.html` 后需重新编译(`include_str!` 编译进二进制)并重启服务。

---

## 5. 运行模式

```bash
rdsctl [--port 9113]                     # 单机管控服务(默认)
rdsctl agent [--port 9190]               # 远端执行 agent(物理机/跨机)
                [--runtime docker|external] [--runtime-cmd <绝对路径>]
rdsctl serve --node-id=N1 \
  --cluster=N1@ip:9330,N2@ip:9330,N3@ip:9330   # 管控面集群模式(见 docs/control-plane-ha-design.md)
rdsctl admin resync-sink                 # 重置 sink 投影游标(leader 下轮幂等重放补齐)
```

鉴权:agent 请求头 `x-agent-token`(`RDSCTL_AGENT_TOKEN` 或 `agent --token`;空=不鉴权,仅 lab)。

**关键环境变量**(完整清单见 `rdsctl.env.example`;优先级:环境变量 > `rdsctl.env` > 代码默认值):

| 变量 | 默认 | 含义 |
| --- | --- | --- |
| `RDSCTL_PORT` | `9113` | HTTP 端口 |
| `RDSCTL_MYSQL_HOST/PORT/USER/PASS/DB` | 见 `rdsctl.env.example` | 控制面持久化 MySQL。凭据不写入代码与文档,经环境变量或 `rdsctl.env` 注入(该文件权限 600 且不入库) |
| `RDSCTL_ROOT_PASS` / `RDSCTL_REPL_PASS` / `RDSCTL_PROXY_MNG_PASS` | lab 兼容默认值 | **受管实例**的 root / 复制账号 / newproxy 管理口口令。生产必须显式设置;轮换只影响新建实例(既有容器口令在创建时写死) |
| `RDSCTL_SWEEP_SECS` | `30` | 健康巡检周期 |
| `RDSCTL_RESUME_TASKS` | `0` | `1` = 启动续跑未终态任务(已完成节点不重跑) |
| `RDSCTL_STORE_BACKEND` | `mysql` | `memory` = 进程内内存后端(lab/合成) |
| `RDSCTL_RUNTIME` | `docker` | 执行后端 `docker` / `external` |
| `RDSCTL_CONTAINER_CLI` | `docker` | docker 驱动的 CLI 名(填 `podman`/`nerdctl` 即接入) |
| `RDSCTL_RUNTIME_CMD` / `_ARGS` / `_CAPS` / `_TIMEOUT_SECS` | 空/空/空/`60` | 外部驱动:路径 / 附加参数 / 声明不支持的能力 / 超时 |
| `RDSCTL_MODE` / `RDSCTL_NODE_ID` / `RDSCTL_CLUSTER` / `RDSCTL_RPC_PORT` | `single` / 空 / 空 / `9330` | 集群模式身份与成员表 |
| `RDSCTL_AGENT_URL` / `RDSCTL_CLUSTER_TOKEN` | 空 | 集群访问执行 agent 的地址与令牌 |
| `RDSCTL_DATA_DIR` | `./logs/ha/<node-id>` | 集群日志/快照/投影数据目录(**按节点隔离**) |
| `RDSCTL_CAP_SECS` / `RDSCTL_REPORT_ENABLED` | `0` / `0` | 容量采样与自动报告,默认关闭 = 零行为 |
| `RDSCTL_DEMO_SEED` | `0` | `1` = 空库时种入演示实例(不起容器) |

---

## 6. 测试

| 层 | 位置 | 说明 |
| --- | --- | --- |
| 单元测试 | `src/**`(`#[cfg(test)]`,170+ 个) | 纯函数与状态机;走内存后端,离线可跑 |
| P0 验收 | `tests/acceptance.rs` | kill-9 不重跑、并发操作拒绝、巡检恢复、多分片实体、查询台读写掩码、慢查治理、备份联动、功能模块 CRUD、集群生命周期重放、集群页/节点页 API、会话共识权威……(**真实 MySQL + 假 docker 垫片**) |
| 集群契约 | `tests/ha_cluster.rs` | 共识仿真:选举 / 提交 / 快照 / 分区 / fence 单调 |
| agent 契约 | `tests/ha_agent_fence.rs` | 远端 agent 路由与 fence 拒绝语义 |

```bash
./scripts/build.sh --test     # 离线编译 + 跑测试
```

> 验收环境注意:假 docker 垫片会劫持 `PATH`,必须显式设 `RDSCTL_MYSQL_CLI` 指向真实 mysql 客户端。

---

## 7. 文档索引

| 文档 | 内容 |
| --- | --- |
| [deployment-architecture.md](docs/deployment-architecture.md) | 部署架构图 + UML 风格部署图(进程、端口、制品、目录、正确性路径归属) |
| [scaling-design.md](docs/scaling-design.md) | 大规模/跨区域架构路线(M0 已落地;M1..M3 规划) |
| [m0-report.md](docs/m0-report.md) | M0 验收报告(backend trait / region-tenant / lease / 分页 / 续跑) |
| [control-plane-ha-design.md](docs/control-plane-ha-design.md) | 管控面 HA 与线性扩展设计(不变量 C1–C10、前提 A1–A7) |
| [control-plane-ha-acceptance.md](docs/control-plane-ha-acceptance.md) | 上述不变量的可执行验收锚点 |
| [control-plane-ha-topology.md](docs/control-plane-ha-topology.md) | 机制到函数的锚点表(含未实现项标注) |
| [control-plane-cluster-view.md](docs/control-plane-cluster-view.md) | 管控集群页设计 |
| [session-rbac-consensus.md](docs/session-rbac-consensus.md) | 会话与 RBAC 入共识状态机 |
| [ops-guide-cluster.md](docs/ops-guide-cluster.md) | 集群运维手册(含"今天可用 / 随 M1a 生效"状态标记) |
| [meta-authority.md](docs/meta-authority.md) | 元数据权威分层与 PRS/ERS 受管切换 |
| [ops-guide-dts-orchestrator.md](docs/ops-guide-dts-orchestrator.md) | DTS 与 vtorc 式切换的用法与演练 |
| [container-platform-abstraction.md](docs/container-platform-abstraction.md) | 执行面抽象契约 |
| [ops-guide-container-platform.md](docs/ops-guide-container-platform.md) | 接入新承载平台操作指南(30 秒决策表) |
| [physical-multi-site-ops.md](docs/physical-multi-site-ops.md) | 物理机混部 / 过保替换 / 机房迁移 |
| [lvs-v0.md](docs/lvs-v0.md) | 进程内 LVS/VIP 接入层 |
| [multishard-v0.md](docs/multishard-v0.md) | 多分片实体创建语义与边界 |
| [xenon-create-design.md](docs/xenon-create-design.md) | Xenon(Raft)实例创建设计 |
| [nodes-view-design.md](docs/nodes-view-design.md) | 创建入口分类与节点状态页 |
| [dba-console-design.md](docs/dba-console-design.md) | Web 查询台设计 |
| [slow-query-design.md](docs/slow-query-design.md) | 全局慢查治理设计 |
| [backup-link-design.md](docs/backup-link-design.md) | 备份节点注册联动设计 |
| [dba-features-milestone.md](docs/dba-features-milestone.md) | 上述三件套的实施顺序与验收 |
| [dba-ai-design.md](docs/dba-ai-design.md) | DBA AI 提能功能设计(性能诊断/监控处置/容量成本/问答/报告) |
| [ai-roadmap.md](docs/ai-roadmap.md) | AI 提能路线图 v2(AI-0..AI-4 与红线) |
| [ai0-impl-checklist.md](docs/ai0-impl-checklist.md) / [ai0-acceptance.md](docs/ai0-acceptance.md) | AI-0 确定性洞察基座:清单与验收记录 |
| [s-milestone.md](docs/s-milestone.md) | S-安全 / S-告警 / S-批量 里程碑 |
| [func-modules.md](docs/func-modules.md) | 功能模块(页面自定义真实步骤) |
| [dag-module-howto.md](docs/dag-module-howto.md) | 新增 DAG 功能模块的完整操作手册 |
| [docker-registry-cn.md](docs/docker-registry-cn.md) | 离线/受限网络的镜像源配置 |
| [../deploy/README.md](deploy/README.md) | 守护模板、启动自检、目录结构与脚本分工 |
| [../scripts/README.md](scripts/README.md) | 编译/部署脚本职责与常见场景 FAQ |

---

## 8. 边界与现状(诚实标注)

- **`OceanBase`** 仅创建入口占位(明确提示"近期开放"),无实体实现;
- **LVS/VIP** 是宿主进程内 4 层转发,不是内核 LVS/keepalived;适合 lab 与单宿主场景;
- **`orch.rs`** 为 P1 骨架:复制事实解析/决策已是纯函数并被巡检接线,完整 orchestrator 采集与 UI 双源角标为后续增量;
- **存储层** 经 `mysql` CLI 逐语句连接,面向 lab 规模;backend trait 已就位以便替换;
- **集群模式** 已落地共识/租约/fence/探针/会话权威/投影;生产多机部署需自行验证 systemd 路径(macOS 开发机只能实测脚本路径);
- **AI 相关模块**(insights / report / ask)均为**规则版、零 LLM、零网络**;LLM 增强层按 `ai-roadmap.md` 的 gate 分期接入,默认配置下行为零变化;
- 巡检、慢查采集、容量采样、自动报告、备份联动在默认配置下均为**关闭或零写入**,不会污染既有行为。
