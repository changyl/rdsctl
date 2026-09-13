# rdsctl 大规模 / 跨区域架构设计(scaling-design)

> 状态:已批准方案归档;**M0 已落地并通过验收**(backend trait / region-tenant 字段 /
> lease 锁 / 列表分页筛选 / 启动续跑),详见 docs/m0-report.md;M1 起按本文件路线推进。
> **M1 的管控面高可用/多副本细节已由 [control-plane-ha-design.md](./control-plane-ha-design.md)
> 收敛为可实现的契约**(自带多数派仲裁 + fence + 无状态网关 + ingress 角色 + sink 投影),
> 验收锚点见 [control-plane-ha-acceptance.md](./control-plane-ha-acceptance.md);本文 §6/§7/§12
> 为路线口径,与设计文档冲突时**以设计文档为准**。
> 目标:单 region ≤3 万实例、单 shard ≤1 万、全局 ~10 万实例;控制面可跨 region 部署,
> 数据面主从可跨 region;审计分级保留。基线决策 2026-09-02 与需求方对齐(见 §3)。

---

## 1. 结论摘要

现有 rdsctl 是**单机单进程管控**(本机 docker CLI + 单 MySQL + 进程内全量内存),
**不支持 10 万级,也不支持跨区域**。它不是参数问题,而是六类架构假设都要替换:
①数据面=本机 docker;②持久化=每句 SQL spawn 一个 mysql 进程;③热路径=进程内全量
内存(实例/任务/会话/操作锁);④调度=进程内 tokio 任务,无常驻队列;⑤巡检=单循环
顺序全表扫;⑥API/前端=全量列表 + 2s 全量轮询,无分页、无 region 模型。

演进分两条线并行:
- **A. 水平扩展控制面(region 内 shard 化 → 全局 10 万)**;
- **B. 跨区域**(控制面多 region + 数据面主从跨区域)。

保留可复用资产:可序列化 `Step`+`TaskNode`、实例状态机与操作规则(含 degraded/失败
可清理)、`StepExecutor` 抽象、巡检降级/恢复+审计语义、MySQL 控制面定位、P0 验收体系。

---

## 2. 现状事实与硬瓶颈(代码锚点)

| # | 现状 | 位置 | 瓶颈 |
|---|---|---|---|
| 1 | 实例=本机容器,1 网络/实例,主机端口 `35000+n`,每实例≥5 host port | `instance.rs` next_port/-p 映射 | 单机端口空间 → **单机硬顶 ≤5k 实例** |
| 2 | 复制 `SOURCE_HOST=容器名:3306`,同 docker 网络 | `instance.rs` CHANGE REPLICATION SOURCE… | 只能同机主从;跨区零支持 |
| 3 | 每句 SQL spawn `mysql` 进程 | `store.rs` q0/q_db | 单操作 10–50ms;无连接复用 |
| 4 | 实例/任务/审计/操作锁进程内 DashMap;`instances` 表存整 JSON | `instance.rs`;`store.rs` instances | 不能多进程/多活/shard/分布式锁 |
| 5 | 提交即 `tokio::spawn`,任务全量驻留;id=单表 `task_seq(kind)` | `dag.rs`;`store.rs` task_seq | 无持久队列/背压;重启仅能"标 failed"(mark_interrupted) |
| 6 | 巡检单循环顺序全表 + 每探针 spawn | `instance.rs` sweep_once | 数千实例即分钟级;不可跨区 |
| 7 | 列表全量;审计 ≤200、任务 ≤300,无游标 | `api.rs`/`store.rs` | 随 N 线性膨胀 |
| 8 | 前端 2s 拉全量 | `rds.html` setInterval | 10 万不可用 |
| 9 | 无 region/az/tenant | 全库 grep | 跨区/多租户/审计归属无表达 |
| 10 | 会话进程内内存 | `http.rs` SessionStore | 多活失效 |

---

## 3. 已确认决策(2026-09-02)

| 项 | 结论 |
|---|---|
| 数据面承载 | **双实现**:`Executor/Agent` trait 抽象先行;提供本机 docker 实现(保留)与 k8s 实现;控制面不绑定 k8s |
| 组件约束 | **仅 MySQL + 语言栈**:任务队列基于 MySQL(与锁/状态同库保证一致性);审计归档落文件/对象存储;不引入 Redis/Kafka(吞吐不足再议) |
| 规模基线 | 单 shard ≤1 万实例;单 region ≤3 万;全量 10 万;生命周期操作 ~百次/实例/月;审计热 30 天 + 归档 |
| 多租户 | M0 数据模型加入 `tenant/owner` 列与审计归属占位;RBAC 后续里程碑启用 |
| DAG 模型 | **节点组组合器(代码强类型)**:Step 库(原子、幂等键)→ 参数化 NodeGroup 注册表(自带依赖/状态机门槛)→ 任务=节点组组合;新任务类型通常零新 Step;dry-run 预览 + 合规检查 |
| 组合场景 | 同实例多步编排、跨实例/批量(配额+批次取消)、人工叠加步骤(模板+人工覆盖,全审计)、定时/自动运维;并显式支持**可重试(节点级与任务级策略)与任务依赖(工作流边)** |

---

## 4. 目标架构总览

```
┌──────────────── 全球控制面(Global Brain,只读汇聚/可选) ────────────────┐
│ 区域目录(region/shard 归属)/跨区拓扑与容灾视图/全局审计检索            │
└───────┬───────────────────────────────────────────────┬───────────────┘
┌───────▼─────────┐  Region A 控制面(每 region 一套,可多活)  ┌▼───────────┐
│ 控制面网关         │  ├ API/鉴权/分页/游标(无状态,多副本)   │ Region B   │
│ 控制器(shard worker)│  ├ 分片 MySQL(region×shard,独立故障域)  │  控制面 …   │
│ 持久任务/工作流队列 │  ├ lease(DB 行级 TTL,跨进程)           └────────────┘
│ reconciler/worker  │  └ 状态机 + 巡检(事件驱动)               region 间仅同步
└────┬────┬────┬─────┘                                           目录/审计视图
     │    │    └── Agent/Executor(每计算节点;本机 docker 实现 / k8s 实现)
     │    │           └ 实例运行时:MySQL 容器/组、代理/路由、副本与跨区复制链路
     └ shard 2 … 每 shard = 控制器组 + 自己的库分片(≤1 万实例)
```

关键:①数据面与控制面分离(控制进程不再直接跑容器);②每 region 独立故障域,region
间不做强一致事务(仅目录/审计最终一致);③主从跨区域作为数据面拓扑对象建模,见 §9。

---

## 5. 数据模型演进

### 5.1 新字段与概念(替换"整 JSON blob + 主机端口"式存储)
- `RdsInstance`:新增 `region`、`az`、`shard`、`tenant/owner`、`data_endpoint`(DNS/overlay,
  取代 host_port 直连);`status_label` 等展示字段不变。
- `InstNode`:新增 `region/az/host_id/endpoint`;`Role` 增补 `dr_standby`(跨区容灾从)、
  路由/代理归属由实例级 topology 表达。
- 新表(演进 `store.rs` DDL v2):
  - `regions`(region/az 元数据);
  - `instance_shards`(shard 归属、容量水位);
  - `topology_links`(master↔slave 复制链路,含跨区标记/SSL/GTID/lag 快照/状态);
  - `instance_locks`(分布式 lease,见 §6);
  - `queue`(任务/工作流队列,见 §7);
  - 审计按时间分区 + 保留策略。
- 实例 ID:`{region}-{shard}-{uuid7}`;日志/路由/分页携带 region/shard 前缀;`task_seq`
  演进为按 shard 的序列(或 DB `SEQUENCE` 思路的 per-shard 计数行)。

### 5.2 迁移策略
- DDL 版本化 + 启动迁移(信息模式检查后 `ALTER TABLE … ADD COLUMN`);`instances` 保留
  `data` JSON 作为扩展属性,查询字段(region/shard/status/tenant)拆成列并加复合索引;
- 双写/影子读过渡,单机 lab 版可回退。

---

## 6. 控制面伸缩与锁

- **分片**:控制器按 `instance_shard` 水平切分;实例归属 = `hash(id)` 或显式 region+shard;
  网关按 key 路由;每控制器仅常驻本 shard 缓存(≤1 万,沿用 DashMap 可接受)。
- **多活**:写走 leader(行锁 + lease);读可副本/缓存;会话落 DB/共享缓存。
- **锁**:实例级互斥改为 **lease 行**(`holder`、`lease_until`):获取=条件 UPDATE,TTL 续约,
  失联自动过期;状态机规则(仅 运行/降级/失败 可销毁 等)不变,落在控制器层;多副本下
  同一实例同时只有一个 holder。
- **收敛(M0 后)**:`Clear_all_locks`/`mark_interrupted` 属**单控制端语义**,多副本下不可用;
  租约的过期判定不得依赖各副本本地时钟;续约失败必须停手而非继续执行。上述三点的目标契约、
  多数派仲裁形态(分片组共识)、fence 与执行面强制点见
  [control-plane-ha-design.md](./control-plane-ha-design.md) §5/§9。

---

## 7. 执行引擎:队列、续跑与工作流

### 7.1 队列与续跑(替代"提交即 tokio::spawn")
- 任务写 `queue`(shard 分区):`queued → claimed(holder,lease_until) → running → terminal`;
  worker 领取带租约与可见性超时;崩溃后由其它 worker 接管。
- 启动恢复从"仅标 failed"升级为选项:**`RDSCTL_RESUME_TASKS=1` 时 requeue 未终态任务
  并按需续跑**(默认仍 mark_interrupted,保 P0 验收语义)。
- **续跑前提 = 幂等**:补齐所有 Step 幂等键(docker run/rm、network、WriteHostFile 已基本
  幂等;ExecSql/复制变更需幂等键 + 记录)。
- M1 登记:复制修复 / proxy 重跑 Step 入库(供 AI playbook 消费)随本项幂等补全落地(M1b);
  单任务重跑 / 任务级 retry 入口随 §7.2 队列语义落地(M1a)——详见 §12 M1 增补登记。

### 7.2 工作流层(DAG 组合器,§3 结论)
- **Step 库**:原子可序列化动作(现有 `dag::Step` + 幂等键)。
- **NodeGroup 注册表**:参数化、声明依赖与状态机门槛(如 `replica_group(master, region,
  role)`、`proxy_group(...)`、`data_check_group(...)`),返回 `Vec<TaskNode>`。
- **Task = 节点组组合**:builder/组合子函数按需拼装(现 create/destroy/scaleout 即首批
  内置组);组合器做 DAG 校验(环/依赖/状态机合规)与 **dry-run 视图**(预览每个节点
  将执行哪些 Step 与影响)。
- **任务级能力**(新增队列列/字段):
  - retry 策略:节点级(已有 attempts)+ 任务级重试/退避 + 总尝试上限;
  - **任务依赖**:`task_deps(task, dep_on, 时机=success/failure)` 持久化边,consumer 在
    上游终态后放行(支撑"创建成功后才扩容/备份""A 实例完成后联动 B"工作流);
  - 批次/批量:同一模板批量实例化 + 并发配额 + 批次取消(级联取消未启动下游);
  - **人工叠加**:任务模板生成后允许在运维窗口插入/追加节点(标记来源 human,全审计);
  - 定时/自动:调度器按 cron/间隔生成任务(复用同一队列,DAG 引擎不变);
  - 取消语义:现有节点间 cancel 标志保留,扩展到批次。

### 7.3 与状态机/锁的关系
- 组合器输出的每个任务须声明「持有哪个实例的 lease/状态门槛」;同实例生命周期任务仍
  互斥;跨实例任务靠任务依赖与批次配额控制并发,不再依赖单实例锁。

---

## 8. 巡检与可观测

- 巡检=**事件驱动 + 检测队列**:状态变更/心跳/复制链路异常入队,worker 池并发探测
  (复用容器存在性/代理连通/`performance_schema` 复制线程判定)。
- 分级降级:连续 N 次失败才 degrade(现为一次即降);degrade/recover 审计不变。
- 跨区链路单列 SLO/告警(WAN 延迟/抖动/半同步降级)。
- 心跳与指标(实例/agent/复制链路 lag)上报,聚合页与告警。

---

## 9. 跨区域

### 9.1 跨区控制面
- 每 region 独立控制面 + 独立库分片 = 独立故障域;region 间只同步目录(实例归属/状态
  快照)与审计视图(Global Brain 只读检索)。
- 跨区控制操作=向目标 region 下发请求(幂等、带 request-id),不跨区事务。

### 9.2 跨区主从(数据面)
- 复制链路建模(`topology_links`):跨区建从 = 目标区 agent 克隆/备份恢复 + GTID
  auto-position 追平 + 数据/复制校验(复用 verify/巡检 SQL 判定);`SOURCE_HOST` 指向
  跨区 endpoint(SSL/TLS),不再是容器名。
- 故障处理:链路抖动巡检阈值、脑裂 fence(切换前目标区 fence 源)、DR 提升入状态机
  (新增 `FailingOver`;现有 Switching 用于同区)。
- 路由层:就近读、跨区读流量;newproxy 面向本区拓扑,上层路由聚合跨区。

---

## 10. API 与前端

- API 一律:游标分页 + `region/az/shard/status/tenant/q` 筛选 + 字段投影;列表只返摘要,
  节点详情走 detail(列裁剪);审计/任务游标检索。
- 会话/权限:会话落 DB(多活);M0 先落 tenant 归属列与占位角色,后续 RBAC。
- 前端:按视图懒加载(实例列表/任务/审计分页,不再一次全拉);2s 轮询改为增量/SSE;
  卡片虚拟化;region/status 筛选;模板/批量/工作流视图(配合 §7.2)。

---

## 11. 容量模型(参数化,基线=§3)

| 参数 | 基线 | 说明 |
|---|---|---|
| 实例总量 | 100,000 | 全局 |
| region 数 | ≥3 | 每 region ≤30,000 |
| shard/实例数 | 10,000/shard | 每 region ≥3 shard |
| 每实例节点 | 主1+从2~4+路由 | 数据面分布多主机,无主机端口瓶颈 |
| 写密度 | ~百次生命周期/实例/月 | 队列/库压力低;批量高峰需配额 |
| 读密度 | 状态页/审计,分页 | P95 列表 <200ms(缓存+游标) |
| 审计 | 热30天 + 归档 | 分表/分区 + 保留任务 |
| 巡检 | 事件驱动 + worker 池 | 探测 ≤10ms 量级/次(长连接),非 spawn |
| 恢复 | RDSCTL_RESUME_TASKS 续跑 | lease+幂等保障不重副作用 |

---

## 12. 分期路线(M0–M3)

- **M0 架构底座(当前执行)**:backend trait(默认 MySQL CLI + 内存实现);region/shard/
  tenant 字段 + 迁移;lease 分布式锁;API 分页/筛选 + 前端懒加载;队列语义
  (claim/lease/requeue/续跑选项);NodeGroup 组合器骨架 + dry-run/校验;任务依赖边与
  retry 策略字段入队。验收:合成 1 万实例指标 + 原 P0 三项回归全绿。
- **M1 区域内多机**:Executor/Agent 双实现收口(本机 docker 保留 + k8s 实现占位)、
  端点 registry 化、队列 worker 多副本、事件驱动巡检、审计分表保留。
  **M1 增补登记(源自 docs/ai-roadmap.md v2 与 docs/ai0-impl-checklist.md;只登记排期,
  随对应子项落地,不做提前实现)**:
  - **M1a 任务级重试 / 单任务重跑入口**:随 §7.2 retry 策略与队列 claim/requeue 语义落地
    (旧 scheduler 不加独立重跑 API,避免被队列版取代);AI-1 playbook `retry_task` 消费;
  - **M1b 复制修复 / proxy 重跑 Step 入库**:随 §7.1 幂等补全进入 Step 库(ExecSql/复制变更
    幂等键 + 记录;proxy 重跑从 create_nodes 抽出复用);AI-1 playbook `start_replica` /
    `restart_proxy` 消费;
  - **M1c 事件驱动巡检携带 evidence 快照**:巡检事件载荷采用 ai0-impl-checklist §2
    `capture_degrade_evidence` 契约(容器状态/exit code/LAST_ERROR/日志尾/脱敏);
    AI-0 S2/S3 为可运行原型,M1 照单接入;
- **M1.5 跨区控制面**:每 region 独立控制面+库;Global Brain 只读目录/审计;网关按
  region 路由;会话共享。验收:A/B 区独立可用、A 区整体故障不影响 B 区。
  实现口径(每 region 一套分片组 + 区域目录只读汇聚 + 跨 shard saga)见
  [control-plane-ha-design.md](./control-plane-ha-design.md) §16 M1.5。
- **M2 跨区主从**:topology_links + 跨区建从/加从 + 跨区链路巡检 + DR 提升状态机与
  fence + 路由就近读。验收:跨区建从/校验/告警/DR 演练。
- **M3 十万级压测硬化**:故障注入(节点/库/队列/agent/时钟漂移)、容量参数化 SLO 文档。

## 13. 迁移与风险

- 单机 lab 版持续保留并跑 P0 验收;演进走新模块 + 版本化迁移/双写。
- 风险:幂等是续跑前提(M0 工作量与风险最大);lease 依赖时钟(租期 >> 漂移);审计膨胀
  (分表+保留);一致性取舍:状态变更=lease+行级强一致,列表/审计=最终一致,不做跨区事务。

## 14. 开放问题(待后续里程碑)
- k8s Executor 的具体资源模型(StatefulSet vs 自管 Pod + 共享存储)与调度策略;
- 批量/定时任务的配额模型与租户级隔离阈值;
- 跨区读路由的代理升级形态(newproxy 扩展 vs 独立 router);
- 归档存储格式与检索(文件/对象存储 + 元数据索引)。
