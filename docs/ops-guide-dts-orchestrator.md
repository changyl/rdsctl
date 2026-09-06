# DTS 与 vtorc 式受管切换:功能与维护指南

> 适用范围:DTS 占位容器链路、vtorc 式复制事实层、受管主从切换(PRS/ERS)与回滚。
> 设计权威分层见 [meta-authority.md](./meta-authority.md);本文件面向“怎么用、怎么维护、怎么演练”。

## 0. 快速命令

```bash
# 编译(release 离线)/ debug / 测试
./scripts/build.sh
./scripts/build.sh --debug
./scripts/build.sh --test

# 便捷:真实 MySQL 切换演练(自动起内存后端 + docker 主从 → auto 切换 → 回滚 → planned)
./scripts/build.sh --drill

# 便捷:内存后端 + demo 数据前台启动(端口 RDSCTL_PORT,默认 9123)
./scripts/build.sh --run-mem
```

---

## 1. DTS 占位空容器链路

### 1.1 概念与约定
- DTS = **占位空容器**:不运行 canal 等 DTS 引擎;仅登记链路记录 + 拉起一个常驻 sleep 容器占位。
- 绑定关系:**一实例一从节点一条 DTS**(instance+node 唯一);引擎字段固定 `canal`(语义占位)。
- 容器名:`rds-{实例}-dts-{节点}`;镜像候选按序:环境变量 `RDSCTL_DTS_IMAGE` → 本机已就绪 mysql/proxy 镜像(免拉取)→ busybox 官方/国内加速镜像。
- 规格(占位规格,如 `2C4G`)随记录持久化并展示。

### 1.2 入口
- **创建实例时**:「架构配置 → 随实例创建 DTS(canal) + DTS 规格(1C2G/2C4G/4C8G)」;async 会落到离线/备份从,无离线从时(sync)落到每分片一个读从;single 禁用。
- **单独登记/移除**:
  - 分片详情:从节点卡下「＋ 登记 DTS(canal)」/ 管道卡「移除」「重试」;
  - 实例「拓扑结构」页签下方 **DTS 空容器卡片区**(状态/规格/移除/重试);
  - 分片卡(拓扑视图)带 DTS 角标。
- 分片详情渲染签名守卫:增/删 DTS 轮询不再整页闪烁。

### 1.3 API
| 方法/路径 | 说明 |
|---|---|
| `GET /api/rds/dts?instance=` | 列出链路(全部或单实例);权限 instances.view |
| `POST /api/rds/dts/create?instance=&node=&target=&spec=` | 创建/重试占位链路(起空容器);instances.manage |
| `POST /api/rds/dts/remove?instance=&node=` | 移除(停删容器+清记录);instances.manage |

### 1.4 维护要点
- 存储:MySQL 表 `rds_dts`(instance+node 主键);启动自动迁移补 `spec` 列(旧表无 spec 会导致写入失败、界面不展示,详见常见问题)。
- 镜像选择:受限网络/内网可用 `RDSCTL_DTS_IMAGE=本机镜像` 覆盖;空容器无需外网。
- 常见问题:界面登记成功但拓扑/详情不显示 → 先查 `select * from rds_dts where instance='…'`;记录缺失多为旧表缺 `spec` 列(重启自动迁移)。历史遗留真实 canal 容器残留 → `docker rm -f rds-…-dts-…` 后重新登记。

---

## 2. vtorc 式复制事实层(只读)

### 2.1 端点与字段
`GET /api/rds/orch/facts?instance=`(instances.view;进程内采集 + ≤10s 缓存,巡检周期刷新)

每节点字段:
- `container / role(master|slave) / mode(async|semi_sync) / alive / repl_ok / io_running / sql_running / lag_secs`
- `semisync`: `master_enabled / master_ack / master_degraded / slave_enabled`
  - `master_degraded = master_enabled && ack==0`(半同步无 ack 保护仍在写)。

demo/非运行实例返回空(豁免)。采集走容器内 root 通道,失败只影响本节点事实,不影响巡检主流程。

### 2.2 界面标注
- 分片详情节点卡:主 `半同步 · ack n` / `半同步·退化(无ack)` / `异步`;从 `半同步从` / `异步从` / 未启用 / `复制中断`。
- 事实与登记主不一致时出现「拓扑偏差」琥珀横幅 + 「按事实切换(ERS)」。

---

## 3. 受管主从切换(PRS / ERS)与回滚

### 3.1 原则
- **登记为单写者**:谁 master 只经受管流程改;orchestrator/巡检只做发现与触发。
- 复制模式影响:半同步(sync 模板)候选要求 ack-safe(近零丢);异步允许按 lag 选(可少量丢)。
- 全程审计(action=`reparent` / `reparent_rollback`),切换前 evidence 快照 `reparent_snapshot`。

### 3.2 API
| 方法/路径 | 说明 |
|---|---|
| `GET /api/rds/orch/facts?instance=` | 见 §2(instances.view) |
| `GET /api/rds/orch/ops?instance=` | 受管切换动作视图:进行中 + 历史(instances.view) |
| `POST /api/rds/orch/reparent?instance=&target=&mode=auto|planned` | ERS/PRS 切换(instances.manage) |
| `POST /api/rds/orch/rollback?instance=` | 回滚到最近快照旧主(instances.manage) |

参数与守卫:
- `target` 缺省=按事实自动挑选候选(`sync` 走 ack-safe);显式目标必须是该实例从节点。
- `mode=auto`(ERS,旧主不可达也允许)/ `planned`(PRS,要求旧主在线并重挂为从)。
- 演示实例(demo-*)、非运行/停用实例、缺主、无存活候选 等均被拒。

执行序列(自动,全程审计;失败自动回滚旧主只读并降级告警):
1. 旧主存活 → `SET GLOBAL read_only=ON`;
2. 候选 `STOP SLAVE; RESET SLAVE ALL; SET GLOBAL read_only=OFF`(提升);
3. 旧主存活 → `CHANGE MASTER TO …MASTER_AUTO_POSITION=1; START SLAVE`(重挂,best-effort);
4. 切换前快照入库 → 登记角色互换(候选→master、旧主→read、其余从 parent 指向新主)+ persist;
5. 审计 ok;失败:审计 failed + critical 告警 + 实例 degraded。

### 3.3 UI
分片详情头部「受管切换(vtorc 式)」:候选下拉(自动/各从 · alive/复制中断 · 半同步/异步)、主从切换(auto)、刷新事实;
顶部偏差横幅:按事实切换(ERS)一键、最近切换(时间/结果)+ 回滚入口(均需输入短语确认)。
实例「拓扑结构」页签底部「受管切换与回滚(PRS/ERS)」面板:切换/回滚仍走直执行(不加长切换时间),面板展示 登记主状态/进行中(自动轮询至收敛)/最近结果/历史(审计)与一键回滚。

自动故障转移为**实例级配置**:
- 字段 `auto_failover`(默认 true;`#[serde(default=true)]`,旧实例自动开启);
- 开关入口:拓扑面板头部「自动故障转移 开/关」,或 `POST /api/rds/meta?name=&k=auto_failover&v=1|0`(instances.manage);
- 巡检按实例开关执行:登记主不可用且存在存活复制从 → 自动 ERS(60s 限频防抖,审计 auto_failover)。

### 3.4 演练
`./scripts/build.sh --drill` 或直接 `bash scripts/orch-drill.sh`(需 docker + 本地 mysql:8.0/perf-newproxy 镜像):
1. 内存后端起服务 → API 创建真实 async 实例(1 主 1 读从 1 离线+代理);
2. 核对事实层全 alive → `mode=auto` 切换 → 校验 DB 新主 read_only=0/旧主=1、登记角色互换;
3. 清理。
API 三阶段人工脚本(参考 scripts/orch-drill.sh)可加验:回滚、planned。

---

## 4. 数据/迁移与权限
- `rds_dts.spec` 列:MySQL 启动自动 `migrate_rds_dts_spec`(旧表补列,幂等)。
- 权限:查看=instances.view;创建/移除/切换/回滚=instances.manage。
- 审计/证据:切换与回滚走 `store.audit`;快照走 `evidence`(kind=`reparent_snapshot`)。
- 半同步插件未在实例创建模板中安装(当前 sync 模板实际为“半同步语义标注”);需要 ack-safe 演练时请先在真实库启用半同步插件。
