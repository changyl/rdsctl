# Xenon(Raft 高可用)实例创建 · 管控面设计

> 需求:参考 xenon 项目 `deploy/xenon.sh` 部署脚手架,在 rdsctl 管控面实现 xenon 实例创建;
> xenon 只通过 raft 做故障转移,创建时可选在集群前部署 **rsproxy(newproxy)** 接入层
> (自动发现并跟随 raft leader,提供三种读一致性档位)。
> xenon 仓库(只读参考):`/Users/didi/CLionProjects/xenon`(Go,基于 MySQL 8.0 的 raft MySQL HA)。
> rsproxy 仓库(只读参考):`/Users/didi/CLionProjects/rsproxy`(Rust 代理;HA 适配见其 docs/15-xenon-ha.md)。

## 1. 参考实现读解(xenon/deploy + rsproxy/src/ha)

| 文件 | 语义 |
| --- | --- |
| `xenon/deploy/xenon.sh` | `up` = compose 拉起 N 节点 → `wait_cluster_ready`:轮询 `docker exec <node> xenoncli raft status`,出现 `"state":"LEADER"` 即就绪(≤180s);`clean` = `down -v`(连卷删除) |
| `xenon/deploy/gen-compose.sh` | per-service 模板:env(`NODE_NAME/RPC_PORT=8801/MYSQL_PORT=3306/MYSQL_ROOT_PASSWORD/REPL_USER/REPL_PASSWD/CLUSTER_PEERS/INIT_ROLE/LOG_LEVEL`)、端口、卷对 `xenon-data-N:/var/lib/mysql` + `xenon-meta-N:/data/raft.meta` |
| `xenon/deploy/entrypoint.sh` | 渲染 xenon.json/my.cnf;初始化 datadir;种 peers.json;`mysqld_safe` + `exec xenon -r ${INIT_ROLE}`(节点1=LEADER) |
| `xenon/src/mysql/status.go` | **xenon 二进制内建维护 `mysql.xenon_raft_status` 单行表**:leader 每 ~5s upsert(id=1, leader endpoint, view_id, epoch_id, updated_at);从库 super_read_only 挡写 → 任意节点可查得当前主 |
| `rsproxy/src/ha/*` | HA worker 按 `probe_interval` 探询成员的 `mysql.xenon_raft_status` → 判主(任期最大/多探源一致) → 拓扑自动跟随 leader 漂移;leader 失联→updated_at 老化→无主态保持旧拓扑 |
| `rsproxy/src/ha/consistency.rs` | 读分流档位引擎:Strong/Causal/Session/Eventual;事务/FOR UPDATE/会话 pin 强制 leader |
| `rsproxy conf` | `[XenonRaft_<cluster>_<tablet>]` 段:`members=<host>:3306,...`、`raft_endpoints=<host>:8801,...`、`read_consistency=strong|causal|session|eventual`、`barrier_wait_ms` 等 |

**关键结论**:1 节点 = 1 容器(mysqld + xenon 同容器);raft 端点 = 容器名:8801;就绪判定 = LEADER 出现;
数据面无 MySQL 复制线程;rsproxy 与 xenon 天然适配(发现契约即 xenon 内建表),可在集群前做统一接入层。

## 2. 管控面建模(rdsctl)

新增 `itype = "xenon"`(Xenon Raft 高可用),贯穿 CreateOpts → RdsInstance → DAG → 巡检 → UI。

### 2.1 数据模型增量(全部 serde default,旧记录兼容)

- `CreateOpts.xenon_nodes: u32`(默认 3;UI 3/5/1,上限 9)
- `CreateOpts.xenon_proxy: u32`(前置 rsproxy 实例数;0=业务直连节点,1..=4=部署代理接入层,推荐 2)
- `CreateOpts.xenon_consistency: String`(rsproxy 读一致性档位:strong/causal/session/eventual,默认 strong)
- `InstNode.rpc_host_port: u16`(xenon 节点宿主侧 RPC 8801 映射;非 xenon = 0)
- 节点角色复用既有枚举:节点1 = `Role::Master`(登记主/展示语义),其余 = `Role::Read`;raft 内部 leader/follower 由 xenon 自治
- 代理层复用既有字段:`proxies`/`proxy_container`/`proxy_mysql_port`/`proxy_mng_port`/`lvs*`
  → 巡检/代理指标/监控/视图自动获得与主从架构同等的代理展示能力
- 镜像:`RDSCTL_XENON_IMAGE`(默认 `xenon-local:latest`)与 `RDSCTL_PROXY_IMAGE`(rsproxy)

### 2.2 创建 DAG(build_xenon_inst)

无代理(xenon_proxy=0):

```
net → node1..N(并发 DockerRun+WaitMysql) → verify(VerifyXenon) → done
```

有代理(xenon_proxy=K,推荐):

```
net → node1..N(并发) → verify(VerifyXenon)
        ↓
     proxy_conf(WriteHostFile: 渲染含 [XenonRaft_0_t0] 的 newproxy.conf)
        ↓
     proxy1..K(并发 DockerRun,同实例网络) → lvs(EnsureLvs,进程内 VIP→K 代理) → done
```

- 代理与 xenon 节点**同一容器网络**(`--network rds-{name}`),`members` 用容器名:3306 内网直连;
- 宿主端口:代理 4051(MySQL 入口)/9111(管理面)与 xenon 节点双端口一样,全部 `127.0.0.1` +
  `alloc_host_port()` 动态探测分配;
- LVS 复用主从架构的进程内转发器(`lvs.rs`):VIP → K 个 rsproxy 宿主端口,业务单入口;
- 配置落盘 `logs/rds/{name}/newproxy.conf`(与主从架构同约定,管控面「代理配置」页可直接查看/改档)。

### 2.3 rsproxy 配置生成(xenon_proxy_config)

`[XenonRaft_0_t0]` 段注入:

- `members=rds-{name}-xenon{i}:3306,...`(容器网络内 MySQL 地址)
- `raft_endpoints=rds-{name}-xenon{i}:8801,...`(对齐 `mysql.xenon_raft_status.leader` 列的 raft 端点语义)
- `read_consistency={xenon_consistency}`(创建时选择的档位)
- `probe_interval=1000 / probe_timeout_ms=500 / leader_stale_ms=3000 / barrier_wait_ms=200 / gtid_sample_cache_ms=30`

凭据语义:proxy 的 `[DB_User_dbu]`/`[Product_User_pu]` 用统一 `root/ROOT_PASS`(与 xenon 节点一致),
HA worker(即 db_user)自动具备 `mysql.xenon_raft_status` SELECT 权限(root 全权)。
初始 `[Master_Host_g0]/[Slave_Host_g0]` 仅作首启占位,HA worker 启动后按 raft 实况覆盖拓扑。
causal/session 档依赖 `gtid-mode=ON`(xenon 镜像 my.cnf 已默认开启)。

### 2.4 读一致性档位(创建时可选;rsproxy 同源语义)

| 档 | 承诺 | 每读成本 |
|---|---|---|
| strong(默认) | 线性化,全走 leader | 0 |
| causal | 跨客户端因果(读 = 主库某提交前缀) | leader 采样 + WAIT 1 RTT |
| session | 读己之写 + 本会话单调读 | 写后才产生屏障;纯读会话 0 |
| eventual | 无承诺,直读 follower | 0 |

写/显式事务/FOR UPDATE/会话 pin 一律强制 leader(任何档位不绕过);档位选择优先级
产品用户 > 库 > 分片默认 > strong。管控面创建时选择的是**分片默认档**,后续可在代理配置页按库/按用户覆盖。

### 2.5 Step::VerifyXenon(执行器 verify_xenon)

1. 等 LEADER(180s,判定与 xenon.sh 一致:`"state":"LEADER"`,逐节点 `xenoncli raft status`);
2. 登记主(节点1)写 init 数据(`appdb.kv` `init=ok`);
3. 轮询 60s:全节点 `SELECT` 读回一致 → raft 复制真实可用;
4. 通过才算创建成功;超时报错保留现场(可销毁重建)。

### 2.6 销毁(destroy_nodes 的 xenon 分支)

有代理:`lvs_stop(StopLvs) → proxy_stop(删 K 个 rsproxy) → raft_stop(xenoncli raft stop) →
nodes_stop(rm -f -v 连卷) → net_rm → cleanup`;
无代理:从 `raft_stop` 直接开始。依赖链保证业务流量先摘除、再停数据层。

### 2.7 巡检(sweep_xenon)/ 事实层

- 节点级:容器存活 + MySQL `SELECT 1`(无 IO/SQL 复制线程,`replica_problem_route` 不适用);
- 接入层(有代理时):LVS 重建兜底 + 经 VIP(或直连代理)探活,与主从架构代理巡检同语义;
- 集群级:任一节点 `xenoncli raft status` 可读 = 管理面健康;全节点读不到 = 告警;
- `collect_replica_facts` 对 xenon 返回空(orchestrator 事实层不适用)。

### 2.8 MySQL 复制语义操作的显式拒绝(防误用)

| 操作 | xenon 行为 | 理由 |
| --- | --- | --- |
| scaleout(扩从) | 拒绝 | 节点数变更属 raft 成员运维 |
| replace_node | 拒绝 | 同上 |
| migrate_instance | 拒绝 | raft 成员迁移属引擎侧运维 |
| run_backup | 暂拒 | mysqldump 可用但备份一致性语义未验证,防伪备份 |
| dts_create / 创建时 dts | 拒绝 | 无 binlog 复制链,canal 伪 master 不适用 |
| orch_reparent(PRS/ERS) | 拒绝 | raft 自带选主;auto_failover_if_needed 亦加守卫 |

### 2.9 API / UI

- `POST /api/rds/create` 参数:`xenon_nodes`(默认 3)、`xenon_proxy`(默认 0)、`xenon_consistency`(默认 strong);
- 创建向导**两段式**:第一步「选择数据库架构」主入口(独立架构卡,进入页面默认停在此步),
  点选后展开第二步配置表单(基础信息/架构配置/规格与版本),表单底部提供「重选架构」返回;
  选中 xenon 卡片后展开专属选项区:Raft 节点数 / 前置 rsproxy 代理数 / 读一致性档位;
  同时隐藏通用 Proxy 区、锁定分片=1、禁用 DTS(旧 `c-itype` 卡组保留为隐藏状态载体,数据契约不变);
- 架构描述口径:**xenon 不是"强一致"**——后台复制仍是半同步(与主从同步同源),由 raft 共识协议
  自动选主/故障转移(创建卡/架构目录文案统一);
- `itypeLab` / `compositionParts` / 节点组与架构目录(nodegroups/architectures)同步收录;
- 有代理时实例详情/巡检/监控复用主从架构的代理卡与 LVS 展示;`rpc_host_port` 用于 raft 诊断入口标注。

### 2.10 xenon 运维语义:无受管切换/回滚,raft 角色实况

xenon 集群**不做 orchestrator 受管切换(PRS/ERS)与回滚** —— 选主由 raft 共识自主完成
(后端保留守卫:orch/reparent、orch/rollback 对 xenon 拒绝;auto_failover 恒关):

- 前端对 xenon 隐藏受管切换面板:分片详情顶部 rp-ui(候选/ERS/PRS)、详情拓扑「受管切换与回滚」
  区域、auto_failover 开关,均不出现;分片详情给出一行 raft 说明横幅;
- 实例详情 → 拓扑结构页新增 **Xenon raft 集群 · 角色实况** 面板:
  - `GET /api/rds/xenon/raft?instance=`(instances.view):逐节点 `xenoncli raft status`,
    汇总各节点状态(LEADER/FOLLOWER/CANDIDATE/IDLE/INVALID/down)与**共识 leader**(各成员
    自述 leader 的多数一致,去掉 :8801 → 容器名);前端 12s 节流缓存;
  - 面板按容器列出角色徽标(绿色 LEADER · 主写半同步 / 蓝 FOLLOWER 等);
  - **角色切换按钮**:对 FOLLOWER 发起 `POST /api/rds/xenon/raft/trytoleader?instance=&node=`
    (instances.manage)→ 容器内 `xenoncli raft trytoleader`,由 raft 共识完成让位与再选举
    (期间秒级不可写窗口,需多数派达成;失败保持原角色);全程审计(xenon_trytoleader);
  - 分片卡/分片列表在缓存存在时以 raft 实况标注 **LEADER / FOLLOWER** 徽标代替「主/从」,
    缓存未就绪时回退登记角色,避免拓扑闪烁。

### 2.11 导航层级

侧栏「主机管理」移入**代理管理**之下(次级缩进项,进入时父级同步高亮),与代理/接入层运维归组。

## 3. 边界与演进

- 镜像缺失时创建在首个 `DockerRun` 失败并按既有任务失败语义置 Failed(可销毁清理)。
- xenon 节点暂只支持本机(agent 路由模型已就绪,远端宿主绑定属后续工作)。
- rsproxy 档位仅注入分片默认;按库/按用户覆盖(read_consistency_db/user)可经代理配置页编辑 → 后续。
- raft 成员变更(add/remove learner)、xenon 侧备份、xenoncli 指标监控接入 → M1+。
