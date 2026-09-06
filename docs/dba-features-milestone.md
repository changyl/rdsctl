# rdsctl DBA 数据服务三件套里程碑(dba-features-milestone)

> 状态:设计评审稿汇总。三条需求(实例 Web 查询窗口 / 全局慢查治理 / 备份节点注册联动)
> 的设计已分别定稿于:
> - `docs/dba-console-design.md`(查询台)
> - `docs/slow-query-design.md`(慢查)
> - `docs/backup-link-design.md`(备份联动)
>
> 本文给出实施顺序、阶段拆分、跨特性改动清单、估量、验收与风险,供评审后按里程碑实施。

---

## 1. 结论摘要

| 特性 | 文档 | 阶段 | 估量(粗) |
|---|---|---|---|
| Web 查询窗口(只读默认 + 独立写权限 + 审计/防泄漏) | dba-console-design | **M-A** | 6–9 人日 |
| 全局慢查治理(采集/聚合/治理闭环) | slow-query-design | **M-B**(依赖 M-A 查询引擎) | 5–8 人日 |
| 备份节点注册联动(outbox + HTTP 适配) | backup-link-design | **M-C**(依赖生命周期钩子,与 M-B 并行安全) | 4–6 人日 |
| **合计** | | | 15–23 人日(不含共享前置,见 §3) |

依赖:**M-A → M-B**(慢查 EXPLAIN/诊断复用查询引擎与 `instances.query` 权限);**M-C**
只依赖既有生命周期终态钩子,与 A/B 无代码冲突,可并行排期。三特性共享的
http.rs body 读取与保留清理 ticker 归入 **M-A 前置**(共享成本,§3.3)。

---

## 2. 跨特性一致改动清单(实施时逐项核对)

### 2.1 权限目录(4 处同步)

| 新权限 | 分组 | 用途 |
|---|---|---|
| `instances.query` | instances | 只读 SQL 执行;慢查 digest 文本/趋势/explain |
| `instances.query.write` | instances | 写语句执行(隐含只读可用) |

同步点:`store.rs PERMISSIONS` → `http.rs perm_for` → MySQL super 展开(`auth_effective`)→
内存后端展开(`MemoryBackend::auth_effective`/`users_list` 同款循环)。处置动作复用既有
`tasks.manage`(慢查治理)与 `instances.manage`(备份联动 retry)。

### 2.2 新增表(全部进 `store.rs` DDL 常量,幂等建表,MySQL/Memory 双实现)

| 表 | 归属文档 | 用途 |
|---|---|---|
| `query_audit` | dba-console | SQL 查询全量审计(完整原文,结果不入库) |
| `slow_digest_snapshots` | slow-query | 节点级慢查差分快照 |
| `slow_baselines` | slow-query | 差分基线 |
| `slow_governance` | slow-query | 全局治理队列 |
| `backup_outbox` | backup-link | 备份注册联动发件箱 |

Store 方法按各文档 §「存储扩展/Store 扩展」新增;新增表/方法各需
`trait StoreBackend / MysqlBackend / MemoryBackend` 三处同步。

### 2.3 环境变量汇总(全部默认关闭/默认值保守,无配置零行为变化)

| env | 归属 | 默认 | 说明 |
|---|---|---|---|
| `RDSCTL_QUERY_TIMEOUT_SECS` | M-A | 30 | 查询超时 |
| `RDSCTL_QUERY_MAX_BYTES` | M-A | 1048576 | 结果字节上限 |
| `RDSCTL_QUERY_MAX_SQL` | M-A | 65536 | SQL 文本上限 |
| `RDSCTL_QUERY_MAX_ROWS` | M-A | 1000 | 返回行数上限 |
| `RDSCTL_QUERY_CONCURRENCY` | M-A | 4 | 并发查询配额 |
| `RDSCTL_QUERY_BLOCK_TABLES` | M-A | (空) | 全局 deny 表名单补充 |
| `RDSCTL_QUERY_ALLOW_ROOT` | M-A | 0 | lab 专用开关(fail-closed 的显式例外) |
| `RDSCTL_SLOW_SECS` | M-B | 60 | 慢查采集周期 |
| `RDSCTL_SLOW_NODES` | M-B | master+offline | all=追加 read |
| `RDSCTL_SLOW_RETENTION_DAYS` | M-B | 30 | 快照保留 |
| `RDSCTL_SLOW_AVG_MS / MAX_MS / MIN_COUNT / GROW_PCT` | M-B | 1000 / 5000 / 10 / 200 | 治理规则阈值 |
| `RDSCTL_BACKUP_REG_ENABLED` | M-C | 0 | 联动总开关 |
| `RDSCTL_BACKUP_REG_URL(_TMPL)` / `RDSCTL_BACKUP_REG_TOKEN` | M-C | 空 | 平台端点与凭据(仅 env) |
| `RDSCTL_BACKUP_REG_SCRIPT` | M-C | 空 | 自定义适配脚本(优先于 URL) |
| `RDSCTL_BACKUP_TIMEOUT_SECS / TICK_SECS` | M-C | 10 / 60 | curl 超时与投递/对账周期 |
| `RDSCTL_AUDIT_RETENTION_DAYS` | M-A/M-B | 30 | query_audit 等保留(可并入 slow ticker) |

env 同步到 `rdsctl.env.example` 与 `scripts/lib.sh`(仓库 env 约定,见 ai-roadmap §3.4 先例)。

### 2.4 前端改动汇总(`rds.html`)

| 视图 | 归属 | 权限裁剪 |
|---|---|---|
| 导航「SQL 查询」+ 实例详情页签「查询」 | M-A | `instances.query`;写开关 `instances.query.write` |
| 导航「慢查询」+ 实例详情监控页「慢查」跳转 | M-B | 列表 `instances.view`;文本/explain `instances.query`;处置 `tasks.manage` |
| 详情「联动状态」徽标 + retry | M-C | 查看 `instances.view`;retry `instances.manage` |

前端权限裁剪块(`hasPerm`)与 S-里程碑既有块同源修改;审计页 action 过滤追加
`query_sql / query_provision / slow_gov_* / slow_collect / slow_reset / backup_notify /
backup_reg_retry / retention_cleanup`(分类展示分组)。

### 2.5 后台任务汇总

| 任务 | 归属 | 周期 | 说明 |
|---|---|---|---|
| 保留清理(慢查快照/治理项/query_audit/outbox 完成态) | M-A 起(M-B 扩展) | 1h | 三表共用 ticker,一次审计摘要 |
| 慢查采集 + 规则 + 对账候选入队 | M-B | RDSCTL_SLOW_SECS | 独立于巡检;失败静默+审计 |
| 备份 outbox 投递 + 对账心跳 | M-C | RDSCTL_BACKUP_TICK_SECS | 仅 enabled 时启动 |

---

## 3. 阶段拆分与实施顺序

### 3.0 共享前置(计入 M-A,先行)

1. `http.rs` 请求体分段读取(Content-Length 循环读)+ `/api/rds/query` 分发;
2. 权限目录两新增项 4 处同步 + 前端权限树;
3. 保留清理 ticker 骨架(先只挂 query_audit)。

### M-A DBA 查询台(dba-console-design)

范围:分类器/护栏、docker `query_table` + TSV 解析、专用账号供给、后端掩码、
`query_audit`、`/api/rds/query` + `/api/rds/query/caps`、前端两入口、垫片扩展。
DoD:文档 §11 验收 1–10 + P0 回归。

### M-B 全局慢查治理(slow-query-design)

范围:采集器(快照/差分/基线)、四表落地(§2.2 中 slow_*)、全局/实例/趋势/gov 接口、
治理规则与队列、explain 复用、慢查页、保留清理扩展。
DoD:文档 §10 验收 1–7 + P0 回归。

### M-C 备份节点注册联动(backup-link-design)

范围:outbox 表 + worker + curl/script 适配、生命周期钩子(终态/备份/启停)、对账、
`/api/rds/backup/reg` + retry、详情徽标、垫片(fake curl/script)。
DoD:文档 §11 验收 1–7 + P0 回归;可与 M-B 并行。

---

## 4. 风险与护栏(实施与评审共同核对)

1. **审计容量/膨胀**:query_audit 存原文、快照表 60s 一档 —— 保留任务必须随功能上线即
   生效;表增长纳入 M3 压测口径(scaling-design §11 审计 30 天热)。
2. **查询打爆数据面**:超时 + 字节/行数/并发四重护栏;重度查询引导 offline;写强制 master。
3. **防泄漏不破窗**:结果永不落库;掩码后端实施;digest 不存 sample_text;root_password
   现状暴露(详情视图)**登记整改项,不在本里程碑改动**(避免破坏既有验收与兼容)——
   单独排期做"详情视图按角色裁敏感字段"。
4. **词法分类器的边界**:DB 级强制靠低权账号;文档如实声明,不做攻防完备承诺。
5. **出网安全**:token 仅 env + stdin;审计脱敏;默认关闭;平台故障只积压不阻断。
6. **双后端漂移**:每新增 Store 方法三处同步,接受现有 memory 后端语义对齐测试约束。
7. **验收隔离**:新增行为全部 env 默认关闭/默认值保守;`RDSCTL_STORE_BACKEND=memory` 与
   acceptance 垫片路径各自独立验证。

---

## 5. 与既有路线图/架构的衔接

- **scaling-design(M0–M3)**:本三件套全部落在当前单机形态内;表设计(mysql 库、幂等迁移、
  保留策略)与 M1 队列/审计归档方向一致——outbox 即"本地可靠队列"的雏形,迁 M1 队列时
  backup_outbox 可折叠进通用 queue(登记,不提前做)。
- **ai-roadmap / ai0**:慢查治理建议为确定性规则(insights 风格),可后置被 AI-1 洞察管道消费;
  快照数据与 evidence 契约同源同库,报告升级项登记不改现状。
- **S-安全/审计体系**:新权限、新审计 action 沿用既有目录与页面过滤;会话/冻结/即时校验
  语义不变。

---

## 6. 总体验收与回归门槛(每阶段结束时)

- 阶段 DoD(§3 各文档验收清单)+ 既有 **P0 验收三项**(kill-9 不丢不重跑 / 并发拒绝 / 巡检
  降级恢复)全绿;
- `cargo test --offline` 全绿;memory 后端与 MySQL 后端语义一致用例通过;
- 默认 env 全关闭状态行为零变化(慢查/备份联动不写表不调用;查询台无权限不可达);
- `./scripts/build.sh` + 部署冒烟(:9113 登录 → 各新视图按权限显示/隐藏)。

---

## 7. 开放问题(评审要点,不阻塞设计)

1. 实例/租户级数据可见域(查询、慢查文本均受影响)——依赖 M0 tenant 占位后的 RBAC
   演进,三文档同口径登记。
2. 备份平台真实端点/鉴权/字段映射(backup-link §10):联调只改适配层。
3. 平台回执确认 vs 2xx 确认(backup-link §10)。
4. root_password 视图整改项的独立排期与验收口径。
5. 慢查是否升级为 alerts 顶层告警(kind+ref 复合去重需先设计)。

---

## 8. 附:文档一致性护栏(写代码/评审时逐条核对)

1. Store 新方法三处同步;新表幂等 DDL;保留清理随表上线。
2. 权限新增四同步 + 前端权限树/导航/按钮裁剪。
3. 零新 crate(查询=容器内 mysql CLI;对外 HTTP=curl/脚本 CLI)。
4. 审计容量现实:完整 SQL 走专用表;`audit_log` 只存摘要;token/口令/结果行不入审计与日志。
5. 锚点一律符号名;本文档与三份设计文档交叉引用不漂移(改动后同步更新)。

---

## 实施偏差与回归记录(编码落地后,2026-09-03)

- 三段编码(查询台 M-A / 慢查 M-B / 备份联动 M-C)已完成并单测全绿(47 个,memory 后端)。
- 偏差集中在三份设计文档各自的「实施偏差登记」节;全局一致性护栏(权限 4 处同步、Store 三处同步、幂等 DDL、零新 crate、默认关闭零行为变化)已通过代码评审自查。
- `slow::start` / `backuplink::start` 默认行为:慢查采集器默认每 60s 轮询(running 实例才触碰容器);备份联动 worker 仅 `RDSCTL_BACKUP_REG_ENABLED=1` 启动;两者在无实例/无配置时均零落库。
- 前端 v1 页面:SQL 查询导航页、慢查询导航页、实例详情备份联动徽标;均按权限裁剪。
