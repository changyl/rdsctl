# 物理机/跨机房运维设计:混部 · 过保替换 · 机房迁移

> 适用版本:当前 rdsctl 单控制端 + 本机 docker 编排模型(下称「现状基线」)。
> 本文回答三个 on-prem 场景问题,并给出「模型改造」落地现状与后续工作流(P2+)路线。
> 关联文档:`docs/meta-authority.md`(注册/巡检/切换职责边界)、`docs/ops-guide-dts-orchestrator.md`(受管切换操作手册)、`docs/scaling-design.md`(实例生命周期与任务模型)、`docs/lvs-v0.md`(接入层)。

## 0. 结论速览

| 场景 | 一句话结论 | 依赖的能力 |
|---|---|---|
| 一:物理机混部 MySQL | 可行,但资源/端口/接入层/巡检通道需先「机器(host)事实化」,不能只按实例维度规划 | Host 注册表、按 Host 端口分配、执行面 seam(本阶段已落地) |
| 二:过保替换节点 | 以「同名容器身份重建 + GTID 追平 + 校验 + 切流 + 清理/回滚」状态机执行;主节点用受管 PRS | 节点身份稳定、受管切换 PRS/ERS、审计/证据快照(P2 补 `replace_node` 工作流) |
| 三:机房迁移实例 | 先在新 DC 建数据面并追平,再切接入层,最后迁元数据/观测;半同步与 RTT 是首要风险 | 跨 DC 数据面、接入层切换点、元数据 region/az 事实化(P2 补 `migrate_instance`) |

三者的公共地基 = **把「机器」从隐式假设升级为显式实体**:一台物理机是一个 Host(含
region/az/rack/容量/端口高水位/状态),实例节点登记到 Host,执行与巡检按 Host 路由
(本机直连 / 远端 agent)。本仓库已按该方向做了最小改造,见 §5。

---

## 1. 现状基线(本文讨论的起点)

- **部署**:单台宿主机的 docker;实例 = 一组 MySQL 容器(proxy/lvs/主从分片)+ 接入层;
  每个节点映射本机 `127.0.0.1:{host_port}`;LVS v0 是**进程内**转发器,监听本机
  `127.0.0.1:{lvs_mysql_port}`(见 `docs/lvs-v0.md`)。
- **机器**:没有 Host 实体;`region/az` 只是实例/节点上的标签,不是事实;`next_port`
  是**进程级单计数**,跨进程/跨机不复用同一分配域。
- **巡检/恢复**:控制端进程在本机直接 `docker inspect/exec/run`;容器缺失 → 节点 down
  → 降级/告警/自动 ERS(受管切换,见 meta-authority.md)。一切假设「容器就在这台机器」。
- **身份**:节点身份 = 容器名(`rds-{instance}-{role}` 语义),DTS 链路、复制链 `parent`、
  证据快照、审计都锚定容器名 → **节点身份与容器名必须一起迁移**(替换时尽量同名重建)。
- **单写者**:实例注册(创建/销毁/扩容)是结构单写者;健康/事实(vtorc 式 facts)与
  角色(PRS/ERS/rollback)是覆盖层;同一时刻只有一个操作持实例锁(meta-authority.md §5)。

由此推导:物理机/多机房场景不是「把 docker run 换到别的机器」这么简单,它同时改的是
**身份、端口、接入层、巡检通道、故障域与角色切换的假设**。

---

## 2. 场景一:物理机混部 MySQL(一台物理机跑多个/混合 RDS 实例)

### 2.1 混部本身可行,但要先回答的问题

混部(同机多实例、甚至与其它业务混部)是资源效率的选择,不是正确性问题。风险全在
**资源竞争与故障域**:实例之间共享 CPU/内存/磁盘/网络,一个实例的大查询或慢 IO 会
波及邻居;一台机器宕机 = 上面所有实例的节点一起消失(故障域=机器)。

### 2.2 决策清单(checklist)

1. **容量评估(按机器维度记账,而非按实例)**:
   - 每台机器登记:CPU 核数、内存、磁盘总量/已用、网卡带宽、IOPS 预算;
   - 新建/扩容节点时校验该 Host 的「已分配规格 + 新规格 ≤ 上限」,而不是只看实例自身;
   - 磁盘水位:数据目录独立分区,监控磁盘(容量采样已按实例/节点入库,需补机器维度)。
2. **端口分配收敛到 Host 高水位**:
   - 现状是进程级 `next_port` 探测分配;多实例同一控制端下其实已天然避让,但
     一旦有第二台机器/第二个控制端,两套计数互不知情 → 每台 Host 维护自己的端口高水位,
     分配时「先取 Host 高水位,再对 127.0.0.1 实测避让」(本阶段已落 Host 高水位,见 §5.3)。
   - 规范化:MySQL `3306` 容器内端口不变,只变宿主映射;业务经接入层,不直连映射端口。
3. **接入层**:LVS v0 的 VIP 是 `127.0.0.1`,**只对本机生效**——物理机/多机混部下,
   业务主机要跨机访问,接入层必须升级为「机器可路由的入口」(如 LB/域名或后续多机 LVS),
   本仓库短期可做法:业务侧通过代理域名访问,把 `127.0.0.1` VIP 视为单机诊断口。
4. **az/rack 事实化**:混部时「实例跨 az」必须落到节点所在 Host 的 az/rack;
   主从要尽量分布在**不同机器(不同故障域)**,避免同机主从同时消失。
   做法:绑定节点到 Host 时校验「同实例主从不落在同一 Host/rack(尽力而为,可配置放宽)」。
5. **巡检通道**:控制端只认得本机 docker。远端机器要进巡检体系,需要:
   - 本阶段:Host 登记 + 节点绑定,绑定远端的节点在巡检里被显式标记
     「远端 agent 未接入」,**不假装本机可达**,也不触发自动 ERS(见 §5.4/§6);
   - P2:远端 agent(控制端 → agent → 本机 docker),见 §7。
6. **混部隔离与运维卫生**:
   - 容器 `--cpus/--memory` 资源上限、`OOM` 后自愈策略(现 `--restart unless-stopped`);
   - 大查询/慢 SQL 治理已有(slow 治理),物理机混部下更要用:全局临时表、长事务会吃
     机器级资源;
   - 备份/日志目录:离线从与备份任务尽量与在线实例分机或分盘,避免备份 IO 挤兑在线。

### 2.3 本阶段能立刻用上的

- Host 注册表(`GET /api/rds/hosts`,见 §5.2):机器清单/容量/状态有了事实源;
- 按 Host 的端口分配器(§5.3):为将来同机多实例的端口规划提供机器级记账;
- 巡检 seam(§5.4):登记了 Host 绑定但无 agent 的节点,不会被误判为本机在线。
  真正的跨机执行与巡检由 §7 的 agent 补齐。

---

## 3. 场景二:过保/退役机器上的节点替换

### 3.1 目标形态

把「机器 X 上的节点」迁到「机器 Y(或新机器)」,**不丢数据、不停业务、可回滚**。
按角色分两类:

- **从节点替换**:简单——新机器上加新从 → 追平 → 摘旧从 → 清理(可随时回退);
- **主节点替换**:必须走受管 PRS(计划内主从切换),把主角色先切到健康从,
  再在旧机器上重建原主节点为新从(或彻底下线)。

### 3.2 节点替换状态机(从节点示例)

```
  准备      登记(目标 Host 容量/端口、本机资源)
    ↓
  1. drain    代理/读写分流摘除该节点(读流量切走;主节点场景 = 先做受管 PRS)
    ↓
  2. 重建      同实例新增节点/同名容器(identity 语义,见下)→ docker run
    ↓
  3. 追平      CHANGE MASTER … MASTER_AUTO_POSITION=1(GTID 自动追平);或备份+追增量
    ↓
  4. 校验      facts:IO/SQL 线程 ON、lag≈0、只读位一致、数据校验(库表行数/校验和抽样)
    ↓
  5. 切流      把该节点角色/读写流量切到新节点(从:改分流;主:PRS 已完成)
    ↓
  6. 清理      旧机器容器下线销毁 + 注册表/审计闭环
    ↓
  (失败任一步) 回滚:回到 drain 前快照(受管切换支持 rollback 到旧主)
```

### 3.3 为什么「同名重建」是默认偏好(身份稳定性)

- DTS 链路按 `instance+node(容器名)` 注册;证据快照、审计、拓扑都锚容器名;
- 复制链 `parent` 引用的是容器名;
- 因此**迁移优先保留容器名**(`rds-{inst}-master` 等),旧机销毁、新机同名重建;
  换名 = 需要一并改 DTS/复制链/监控引用(identity remap),复杂且易漏。

### 3.4 替换前检查清单(节选)

- [ ] 目标机器容量/端口/rack/az 已在 Host 注册表且非 retiring;
- [ ] 该实例主从不与目标 Host 同 rack/同机(尽力);
- [ ] 有离线从或备份承载「先全量」的场景,避免从零开始追增量;
- [ ] 低峰窗口;已通知业务;受管切换面板(in-flight)无进行中操作;
- [ ] 替换期间禁止其它生命周期操作(实例锁自动互斥);
- [ ] 演练脚本 + 回滚预案(受管切换快照在,可 rollback)。

### 3.5 本阶段已具备 / P2 补齐

已具备:受管 PRS/ERS/rollback(orch-drill 已真库演练)、scaleout/任务模型、审计、
degrade 证据快照、事实接口。
P2:把上述状态机封装成 `replace_node` 任务(Step 串成 DAG,复用 scaleout/切换 step),
并在拓扑上提供「节点 → 迁移」操作;届时执行层 seam(§5.4)正好是 step 的执行路由点。

---

## 4. 场景三:机房迁移(实例/实例组从一个 DC 迁到另一个 DC)

### 4.1 大原则

> **先建数据面、追平;再切接入层;最后迁元数据与观测。** 任何时刻都能回退到旧机房。

DC 迁移不是「把容器搬过去」(跨 DC 无共享 docker),而是**复制数据 + 重放增量 + 切入口**。

### 4.2 典型状态机

```
 规划      数据量/带宽/时间窗评估(全量大小、日均增量、网络 RTT)
    ↓
 1. 新机房建实例     在新 DC 建同规格实例(新实例=数据面;region/az 指向新机房)
    ↓
 2. 全量+增量        xtrabackup/逻辑备份 → 导入新机房;或以新从身份加入复制链追 GTID
                     (需新旧两机房 MySQL 版本/参数兼容;建议 从复制链追平 优于 反复全量)
    ↓
 3. 校验            追平且校验通过(lag≈0、数据校验、只读位、半同步 client 数)
    ↓
 4. 停写/切流        业务侧接入层切到新机房(域名/LB);主库停写窗口尽量短
    ↓
 5. 收尾            旧机房实例保留只读兜底 N 天 → 降级为备份/下线
    ↓
 失败 → 回滚:接入层切回旧机房(数据面未破坏,旧机房数据仍是权威)
```

### 4.3 迁移实例的三种执行形态(供 P2 `migrate_instance` 参考)

1. **加入复制链(推荐)**:新机房节点作为旧机房主库的跨 DC 从追平,
   lag≈0 后直接受管 PRS 把主切过去。缺点:半同步链路跨 DC,RTT 大时写延迟高
   (见 4.4 风险 1)。本模型天然支持(复制链 + 受管切换)。
2. **备份/恢复**:全量 + binlog/GTID 增量补;适合跨大版本/长停机窗口场景。
3. **混合**:先备份建底,再挂复制链补尾差(减少追平时间)。

### 4.4 风险清单(迁移前必读)

1. **半同步与 RTT**:sync 模板若启用 `rpl_semi_sync`,跨 DC RTT(几十 ms+)会显著抬高
   提交延迟;先评估延迟预算(meta-authority.md 有 RTO/RPO 预算),必要时迁移窗口内
   降级为异步复制(受管切换的事实接口能看到 `master_degraded`,即「开了半同步但无 ack」)。
2. **GTID 集合**:新机房节点必须完整拿到旧主 `gtid_executed` 全集,否则追平后
   `MASTER_AUTO_POSITION` 可能等待不存在的事务(卡死);校验阶段比对 `gtid_executed`。
3. **接入层切换点**:入口是域名/LB 还是本机 VIP 决定切换手段;当前 LVS v0 是单机
   进程内转发器 → **跨 DC 迁移前必须引入机器级入口**(见 §2.2.3),否则业务无法平滑切走。
4. **备份域**:全量备份要落在新机房或独立存储,旧机房数据源消失不丢恢复能力。
5. **监控/告警/审计**:实例元数据(region/az)在迁移后要更新;巡检、容量、慢查历史
   要能区分新旧机房节点;告警路由随 region/az 变化。
6. **周边依赖**:业务白名单/防火墙、DNS TTL、binlog 保留时长、DTS 链路上下游
   (DTS 消费在旧机房要一并迁或重配)。
7. **多实例批量迁移**:按实例串行/小批量,每批完成「校验 + 稳定期」再迁下一批;
   用审计/证据快照留档每次切换。

### 4.5 本阶段已具备 / P2 补齐

已具备:复制链(跨区从的 `parent` 引用)、受管切换、证据快照、审计、容量/慢查观测。
P2:`migrate_instance` 工作流(编排 §4.2 状态机)、region/az 由「标签」升级为「节点事实」、
接入层跨机方案、迁移专用校验步骤。

---

## 5. 模型改造落地(Host/Agent 最小可演进版)

为支撑 §2–§4,本阶段已落地以下最小改造(不引入真实 agent,先立模型与 seam):

### 5.1 Host 实体(机器注册表)

- 字段:`name`(唯一,如 `host-cn-north-01`)、`ip`、`region/az/rack`(故障域事实)、
  `cpu_cores/mem_gb/disk_gb`(容量)、`status(running|maintenance|retiring)`、
  `next_port`(该机器端口高水位,由分配器维护)、`created_at/updated_at`。
- 持久化:MySQL 表 `rds_hosts`(启动 DDL 自动建);内存后端同语义。
- API(权限:查看 `instances.view`,变更 `instances.manage`):
  - `GET  /api/rds/hosts` —— 机器清单;
  - `POST /api/rds/hosts?name=&ip=&region=&az=&rack=&cpu=&mem_gb=&disk_gb=&agent_port=` —— 登记/更新;
  - `POST /api/rds/hosts/delete?name=` —— 删除(仍被节点绑定则拒绝)。

### 5.2 节点 → 宿主绑定

- `RdsInstance.node_hosts: Map<container, host_name>`(默认空 = 未绑定/本机),随实例
  数据持久化,视图输出 `node_hosts`。
- 绑定/解绑:`host_assign_node(instance, node, host)` /
  `host_clear_node(instance, node)`:
  - 校验:实例/节点存在、Host 已登记且非 `retiring`;
  - 写 `node_hosts` + 持久化 + 审计(`action=host_assign/host_clear`)。
- **语义**:绑定是「规划/登记」事实,不代表控制端已能管理远端;执行路由见 §5.4。

### 5.3 按 Host 的端口分配

- `store.host_alloc_port(name) -> Option<u64>`:对该 Host 高水位 `next_port` 自增返回
  (MySQL 后端与内存后端均实现;单测验证递增)。
- 现状创建/扩容仍走进程级 `alloc_host_port()`(探测本机端口,向后兼容);
  绑定 Host 后的新节点端口将来由该 Host 高水位分配(供 P2 工作流使用)。

### 5.4 执行面抽象 seam(本机直连 / 远端预留)

- `node_host_binding(inst, container) -> Option<&str>`:节点绑定的 Host(空 = 本机);
- `host_exec_plan(host) -> "local"|"remote"`:执行路由判断(纯函数,供测试与后续 agent);
- **巡检行为变化**:绑定远端 Host 的节点——
  - 不执行本机 docker 探测(避免把「另一台机器上的容器」误判为缺失/可用);
  - `node_states[container] = "remote"`,问题文案明确「绑定宿主机 X,远端 agent 未接入」;
  - **自动 ERS 跳过**:主节点绑定远端时,不自动提升(避免无 agent 时对真正在跑的
    远端主做错误切换 → 脑裂),跳过原因写入受管切换面板。
- 事实接口(`/api/rds/orch/facts`)对远端绑定节点返回 `alive:false + host + remote:true`,
  不再尝试本机 docker exec。

> 设计取舍:远端状态用独立标记 `remote` 而不是 `down`,是因为语义不同——
> 「down」会喂给自动 ERS 做决策,而「remote(agent 未接入)」是**管理盲区告警**。
> 一旦 agent 接入(P2),同一绑定数据即可切换为真实执行路由。

### 5.5 边界与向后兼容

- 未绑定节点(`node_hosts` 为空)→ 全部行为与改造前一致(本机 docker);
- demo 种子不建 Host、不绑定,演示行为不变;
- Host 删除校验:被任一实例节点绑定则拒绝,防悬挂引用。

---

## 6. 三场景 × 现有能力/缺口映射

| 能力 | 现状 | 场景一 混部 | 场景二 替换 | 场景三 迁机房 |
|---|---|---|---|---|
| 机器清单/容量 | ✗ → Host 注册表(§5.1) | 规划基础 | 目标机校验 | 目标机房规划 |
| 节点身份稳定 | 容器名 | 依赖 | 同名重建的前提 | 迁移后身份不变 |
| 端口分配 | 进程级 | Host 级(§5.3) | Host 级 | 新机房 Host 级 |
| 执行/巡检通道 | 仅本机 docker | seam 盲区提示(§5.4) | 需 agent(P2) | 需 agent(P2) |
| 接入层 | 本机 VIP(v0) | 机器级入口缺口 | — | 机器级入口缺口 |
| 数据复制 | GTID 复制链 | — | 追平/重建 | 全量+增量/挂链 |
| 主从切换 | 受管 PRS/ERS/rollback(真库演练过) | — | 主节点替换用它 | 切主用它 |
| 替换/迁移工作流 | ✗ | ✗ | P2 `replace_node` | P2 `migrate_instance` |
| region/az | 标签 | rack/az 事实化(P2) | rack/az 事实化(P2) | 迁移后更新(P2) |

---

## 7. P2 路线图与落地状态

> 图例:✅ 已落地(真 drill 验收)· 🔜 下一项 · ⏳ 规划

1. **远端 agent(执行面补齐)** ✅
   - 落地:`rdsctl agent [--port <端口>]` 独立进程(默认 9190,鉴权 env `RDSCTL_AGENT_TOKEN`
     或 `--token`;控制端/agent 同 env 即自动携带 `x-agent-token` 头)。极简 HTTP+JSON
     协议:ping / docker 透传 / sql(exec_mysql_local)/ exec / state / run / rm(见 `src/agent.rs`)。
   - Host 注册表新增 `agent_port`(0=未接入);`POST /api/rds/hosts?…&agent_port=9191`。
   - 执行路由:`resolve_route_of()` → `Local | Agent{url} | Unmanaged`;巡检节点健康、
     `orch/facts`、degrade 快照、自动 ERS 全部按路由执行——绑定 Host 且 agent 可达的节点
     由 agent 跑 docker;agent 失联标记 `remote`(管理盲区,不误判容器缺失、不喂给 ERS)。
   - 验收:真实 drill `scripts/agent-drill.sh`(单机双进程):facts `via=agent`;kill agent →
     degraded + 节点 `remote`;重启 → 恢复 running;清理干净。`cargo test` 95 全绿。
2. **`replace_node` 工作流** ✅
   - API:`POST /api/rds/replace_node?instance=&node=&host=`(instances.manage;从节点专用,
     主节点替换先 `orch/reparent mode=planned` 切走主角色)。门槛:实例运行中、节点非主、
     目标宿主机已登记且 `agent_port>0` 非 retiring、节点未已在目标机。
   - 编排:DAG 任务(`kind=replace_node`)= `ReplaceNodeCore → ReplaceNodeCommit → 状态 running`;
     持实例操作锁,全程审计,失败 watcher 置 Failed。
   - 核心策略(**零停机 + 天然回滚**):目标宿主机用**临时名**(`{node}-rnx`)起新从 →
     挂复制链(GTID 自动追平,校验用 `GTID_SUBSET`——实测 8.0.46 的
     `WAIT_FOR_EXECUTED_GTID_SET` 即使已追上仍恒返回 0)→ 摘旧容器 →
     `docker rename` 临时名→正式节点名(**身份=容器名不变**,复制链/DTS/审计引用不动)→ 提交。
     失败发生在追平前:自动清理临时容器,旧节点不受影响(可重试/换目标)。
     续跑幂等:临时容器已存在则直接追平;rename 中断后再跑自动落位。
   - 验收:真实 drill `scripts/replace-drill.sh`:替换后 `node_hosts[节点]=目标机`、
     节点 host_port/server_id 已更新、正式容器换名上线且临时容器清理、主写→从读一致、
     facts 经 agent(`via=agent`)。`cargo test` 97 全绿。
   - 已知边界(单机 drill 已覆盖同 daemon 场景):跨物理机复制源地址为**容器名**
     (要求共享 docker 网络);独立机房需把 SOURCE_HOST 换为主机可达地址(ip+映射端口),
     属接入层跨机(④)模型,UI 已按 Host 登记 ip 呈现接入点。
    - 前端入口:实例详情操作区新增「迁移 / 换机」(整机迁移 + 替换节点;目标机下拉来自
      主机管理页,二次短语确认后提交任务,进度见任务中心)。
3. **`migrate_instance` 工作流** ✅
   - API:`POST /api/rds/migrate?instance=&region=&az=&hosts=a,b`(instances.manage)。
     目标机房事实 region/az + 目标宿主机组(逗号分隔;须登记且 agent 接入、非 retiring)。
   - 编排:DAG 任务(`kind=migrate`)= `MigrateInstance → 状态 running`,持实例操作锁、
     Switching 状态、全程审计、失败 watcher 置 Failed。
   - 核心状态机(**主节点最后切,失败可回退/续跑**):
     1. 从节点逐个同身份迁移(复用 ②:目标机临时名追平 → 摘旧 → 换名,绑定/端口提交);
     2. 主节点:目标机临时新主挂链追平 → **老主置只读定格** → 最终追平 → 停新从复制 →
        摘老主 → 换名上线(身份=容器名不变)→ 新主解除只读 → **其余从节点复制源重指向新主**;
     3. 全部成功后提交 region/az 事实(实例级 + 节点级)。续跑幂等:已在目标机的节点跳过。
   - 验收:真实 drill `scripts/migrate-drill.sh`:迁移后实例 region/az=目标、`node_hosts`
     全部指向目标机、主/从容器身份在线且无临时残留、**新主可写、主写→从读一致**、
     facts 全部经 agent。`cargo test` 98 全绿。
   - 边界:单机 drill 依赖共享 docker 网络(容器名可达);独立机房/独立 docker 网络时
     复制源地址需为主机可达地址,随 ④ 接入层跨机一并补齐。
4. **接入层跨机方案模型** ✅
   - 模型:实例视图新增 **接入点清单 `ingress[]`**(P2-④):本机 LVS/VIP 入口
     (`kind=lvs`) + 每个**绑定宿主机**一个接入点(`kind=host`:ip/region/az/rack/agent 状态 +
     其上节点逐一可达地址 `ip:host_port`)。即“业务入口 = LVS(本机)/宿主机 ip:映射端口(跨机)”,
     取代“接入 = 单机 VIP”的假设;独立机房各自登记 Host 后,入口自然落到主机 ip。
   - UI:实例详情头部 chips 呈现「主机 h · ip:port」接入点;连接信息卡新增
     **「跨机接入点(绑定宿主机)」**区块,逐 Host 列出其上节点 `container@addr`。
   - 复制链源地址边界:跨独立 docker 网络的复制源地址(SOURCE_HOST)需为主机可达地址,
     由 ②③ 的换名/重指向步骤在使用 `host_node.addr` 后即满足(本仓库 drill 共享网络,
     沿用容器名;UI/数据已按 Host 登记 ip)。
   - 验收:单测 + HTTP 冒烟(绑定后 `/instance` 返回节点 `host_name/host_ip/host_az/host_rack/
     exec_route/addr` 与 `ingress[]` 主机入口)。
5. **region/az/rack 节点事实化** ✅
   - 服务端:`enrich_host_facts`(instance.rs)在实例列表/详情视图把**绑定宿主机注册表事实**
     附到每个节点:`host_name/host_ip/host_region/host_az/host_rack/agent_port/addr/exec_route`
     (local|agent|unmanaged);未绑定节点 = 本机直连 local。实例 region/az 仍为登记事实,
     节点以绑定 Host 的 region/az/rack 为准渲染(拓扑/告警口径一致)。
   - 迁移/替换成功后节点 region/az 随目标机房更新;rack 事实来自 Host 注册表。
   - UI:chips/连接卡展示主机与 rack(见 ④);facts 接口(`orch/facts`)已带 `via/host`。
   - 验收:单测(绑定后视图断言 host 字段与 exec_route) + HTTP 冒烟。
6. **调度建议** ✅
   - 容量水位(硬):`schedule_check(host, extra)`——节点槽位按 `mem_gb/8` 估算,
     已用 = 全实例绑定计数;超水位拒绝(`host/assign`、`replace_node`、`migrate_instance`
     提交前均校验并随审计记录)。既有无 agent/retiring 校验保留。
   - 分布提示(建议级,不阻断):`schedule_advisories(instance, host)`——主从同机、
     同 rack、同 az 聚堆 → 提示文案写入审计(submitted),供 DBA 判断换机。
   - 创建/扩容:本机模式(无绑定)不受 Host 模型约束,沿用原流程;
     跨机建/扩一律走 replace/migrate 并自动受上述水位与分布约束。
   - 验收:单测(小内存机槽位上限拒绝溢出;同 az 提示)。

> **P2 六项全部落地(2026-09):**① 远端 agent · ② replace_node · ③ migrate_instance ·
> ④ 接入层跨机入口模型 · ⑤ Host 事实化渲染 · ⑥ 调度水位/分布约束。
> 真 drill:①②③(`scripts/{agent,replace,migrate}-drill.sh`);④⑤⑥ 以单测 + HTTP 冒烟验收。
> 主线: `cargo test` 100 全绿,构建 0 新警告。

---

## 8. 验收锚点(本阶段)

- `docs/physical-multi-site-ops.md`(本文)覆盖三场景 + 能力缺口表 + 模型改造说明;
- `rds_hosts` 表/内存后端:Host 登记/列表/删除/按 Host 端口自增分配(单测);
- `node_hosts` 绑定:assign 校验(节点属于实例、Host 存在、retiring 拒绝)+ 审计;
- seam:绑定远端 Host 的节点巡检不碰本机 docker、标记 `remote`、自动 ERS 跳过原因可见;
- `cargo build` 0 警告;`cargo test` 全绿(含新增用例)。
