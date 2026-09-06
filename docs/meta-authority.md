# 元数据权威与受管切换设计(meta-authority)

> 状态:已评审定稿(AI 会话)。引入 orchestrator 前提下的元数据权威分层与 PRS/ERS 双轨切换设计。

## 1. 问题

引入 orchestrator(如 MySQL Orchestrator / Vitess vtorc)后,其故障切换会改变**真实 MySQL 复制拓扑**:
原主被降级、某从被提升为主。若 rdsctl 仍以控制面登记的角色/写入方向为准,将长期与实际不一致:
- `nodes[].role`(master/read)漂移;
- 拓扑树/连接信息里的“主节点”指向已降级原主;
- 扩容等操作以登记 master 为复制源会接到从节点;
- Proxy 写路由仍指向原主(orchestrator 不碰代理),即使角色改对写入口也未切。

单纯“两边各信一半”不可行——需要明确的**权威分层**与**收敛机制**。

## 2. 现状分层(rdsctl)

| 层 | 内容 | 维护方 | 持久化 | 展示 |
|---|---|---|---|---|
| ① 结构登记 | `nodes/proxies/shards`:节点是谁、角色、端口、分片归属、DTS 绑定 | create/scaleout/destroy 任务结果 | store | 拓扑结构/连接信息/DTS 归属 |
| ② 健康事实 | `node_states`(ok/stopped/down/missing/repl_down)+ 实例 `status` | 健康巡检 | 仅变化时 | 节点着色/角标、降级、操作可用性 |
| ③ LVS 接入层 | VIP→proxies 宿主端口转发 | 进程内 lvs.rs(由 ① 派生) | 不持久 | 接入层连通 |

## 3. 权威分层(决策)

1. **结构骨架 = rdsctl 登记权威**:实例由哪些容器组成、分片、规格/标签/租户、Proxy/DTS 归属。orchestrator 不提供这些业务概念;漂移只告警,不改结构。
2. **复制角色(master/read)与复制/连通事实 = orchestrator 权威**,但**只有受管 reparent 工作流写回登记角色**;不静默覆盖。
3. **健康 = 事实层权威**:巡检 node_states + orchestrator 事实合并;健康只标状态、触发流程,绝不直接改 `role`。
4. **发布参数(host_port/网络)、VIP→Proxy 后端 = 登记/LVS 派生权威**,不受 orchestrator host:port 反向覆盖;维护 container↔orchestrator host:port 映射(由巡检登记/推导)。
5. **离线/备份/读从的业务语义**由登记+切换规则定义;orchestrator 只给角色候选。

## 4. 受管切换:PRS / ERS 双轨(Vitess 式)

**单一写者**:登记 = 唯一权威拓扑;或ac 只做发现/触发(等同 vtorc)。切换一律由 reparent 工作流驱动,不允许“自由切换后长期不一致”。

### 4.1 ERS 快速通道(emergency,自动)
检测(主不可用)→ 选最优候选(lag/位点/semi-sync ack)→ 提升(最小 SQL 集)→ 代理热切换 → 收敛。
- **不等人工**;审计/快照照记;`failover.mode=auto`(默认)|`manual`(停在待确认提升)。
- 目标 RTO ≈ 5–10s(见 §5)。

### 4.2 PRS 计划通道(controlled,可慢)
计划维护/升级:优雅 drain → 降级旧主 → 提升 → 代理重指(可人工确认)→ 校验。

### 4.3 收敛动作(并发,不串行等容器)
1. 登记角色原子写回(候选→master、旧主→read;内存即时+审计+快照先落,persist 异步)。
2. Proxy 写入口:**优先配置热加载指向新 master**;无 reload 则快速重启(秒级)。
3. LVS/serving:refresh(由 proxies 派生)。
4. UI 切换窗内标“切换中/偏差”,收敛后一致。

## 5. 延迟预算与验收

切换耗时 ≈ 检测 + 提升 + 收敛:
- 检测 1–2s(半同步/心跳超时或 vtorc 事件,非长轮询)
- 提升 2–3s(STOP/RESET SLAVE、read_only 翻转等最小 SQL)
- 收敛 1–2s(代理热加载 + LVS refresh + 登记写回,并发)
- **RTO 验收 ≤ 10s(目标 5–8s)**:自“主不可写检测”起,至“新主可写 + 代理指向新主 + 登记一致”。
- 快照回滚:切换前登记 JSON+代理配置快照,失败/误切一键回滚旧主。

## 6. 复制模式(async / semi-sync)对 vtorc 式内嵌的影响(已确认纳入)

当前实例模板含**异步(async)**与**半同步(sync)**两种复制,对切换决策/检测有直接影响,已并入事实层与策略矩阵。

### 6.1 丢失与候选取舍
- async:主已提交、从可能未收到 → 提升默认**接受少量丢失**(或等候选追平换 RTO);告警标注 loss 可能。
- semi-sync:主在收到 ≥1 从 ack 前不返回 → 提升用 **ack 判定候选**,RPO≈0(要求“含全部 ack 的候选”存活)。
- 每从可独立开半同步:多从时通常构成“≥1 个半同步从”的安全组,其余异步。候选范围/排序须感知安全组。
- loss 预算按实例 `itype` 参数化:**sync → 近零丢(等追平/选 ack 位点);async → RTO 优先(可少量丢并告警)**。

### 6.2 检测与状态
- 半同步主有 `rpl_semi_sync_master_timeout`,超时后自动退化继续写 → 需识别「半同步已退化(无 ack 保护)」,避免误判为切换依据或错判安全组。
- ReplicaFacts 新增字段:`replication_mode(async|semi_sync)`、主侧 semi-sync(enabled/ack 数/是否退化)、从侧状态、lag/位点。

### 6.3 切换后恢复语义
- 新主开启半同步 + 其余从重连至新主(sync 模板必须;async 可选);
- 旧主降级后关写,按模板决定是否作为读从开启半同步;
- 半同步暂不可用时按模板处理:sync 严格模板 → 告警“无保护提升”或拒绝;async → 放行并告警。

## 7. 漂移检测与“采纳”

若出现“外部已自由切换(未走本流程)”:
1. diff(orchestrator 事实, 登记)→ 偏差清单;
2. 走 **ERS 采纳**:记录偏差 → 加实例锁 → 按事实校正角色 → 代理重指(`failover.mode`)→ 快照/审计;
3. 只影响状态/标记的差异自动收敛;结构/业务语义差异需显式确认动作。

冲突规则:
- master 冲突 → 展示层立即以 orchestrator 事实标红/角标;写回登记仅经受管流程。
- 容器缺失(node_states=missing)→ 只标灰,不删结构。
- 多从/孤立/GTID 不一致 → 事实高亮 + “纳入登记/忽略/清理”显式动作。

## 8. 展示(用户可见)

- 拓扑/详情保持登记骨架;节点卡叠加来源角标(登记/编排/巡检)与偏差高亮、切换时间戳。
- 列表 Proxy 版本灰度:版本归属 proxy 节点(登记);健康/连通归事实层。
- DTS 占位绑定从节点:登记权威;若该从已被切换掉,DTS 目标高亮“源已变化”并提示重建/移除。

## 9. 实施阶段(P0–P4)

- P0(本文档):口径、分层、PRS/ERS、RTO、适配器契约。
- P1 只读事实层:`ReplicaFacts` 适配器抽象(拉取/事件),采集 角色/lag/复制中断 + 复制模式(async/semi-sync)与半同步状态(主侧 enabled/ack 数/退化、从侧状态),扩展 node_states 事实键;UI 双源角标与“半同步保护中/退化/异步”标注;不写回登记。
- P2 reparent 引擎:PRS/ERS 状态机(idle→detecting→promoting→redirecting→done/failed),候选选择、提升最小 SQL、失败回滚、审计/快照;复用实例锁。
- P3 代理收敛:newproxy 配置热加载(无则快速重启)、LVS refresh、切换窗 UI 状态。
- P4 触发接线:orchestrator 事件→ERS、心跳/半同步超时检测、`failover.mode` 配置与超时兜底。

## 10. 适配器契约(占位,供 P1 实现)

```
ReplicaFacts {
  list(instance) -> Vec<Fact { container|host:port, role(master|slave), is_running,
                              replication_running, lag_secs, gtid, last_seen }>
  watch(instance, cb) // 事件推送(可选)
}
```
- 角色权威只来自该适配器;写回仅经 reparent 工作流。
- 品牌隔离:MySQL Orchestrator / Vitess vtorc 由具体 adapter 实现;rdsctl 侧不感知。

## 11. 边界/假设

- 不做全量重建登记;不做静默覆盖业务语义。
- ERS 默认 auto;proxy 无法热加载时退化为快速重启并计入 RTO。
- 发布参数(host_port/网络)不被 orchestrator 反向覆盖;container↔host:port 映射由巡检维护。
