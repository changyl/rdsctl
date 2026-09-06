# rdsctl 全局慢查询治理 slow-query-design

> 状态:设计定稿(需求方确认:平台能力 + 页面 + 治理闭环)。锚点用符号名。
> 配套:`docs/dba-console-design.md`(查询引擎/权限)、`docs/dba-features-milestone.md`(顺序)。

---

## 1. 结论摘要

慢查的**全局维度 = 跨实例、按 digest 归一后的集中治理**:同一形态的慢语句在不同实例
重复出现时,应合并统计、全局排序、统一治理,而不是逐个实例各看各的。

- **采集源**:MySQL 8 `performance_schema.events_statements_summary_by_digest`(默认开启),
  逐实例逐节点(默认 master + offline 离线从,读从可选)按周期取快照、与上轮差分后落库;
  **只存规范化 digest_text(无字面量),不存 sample_text/原始 SQL** —— 防泄漏设计。
- **存储**:`slow_digest_snapshots`(节点级差分快照,ts 建索引);`slow_governance`(全局治理
  队列:同一 digest 跨实例汇总的候选/处置状态)。
- **维护闭环**:周期采集 → 差分入库 → 全局排行/趋势查询 → 阈值规则产治理候选 →
  人工 EXPLAIN(复用查询引擎,审计)/忽略/解决 → 保留清理。全部动作确定性、不依赖 LLM;
  digest 文本可见性需要 `instances.query` 权限。

估量(粗,团队校准):采集器与差分 2–3 人日、存储与接口 1–2 人日、规则与治理队列 1–2
人日、前端页 1–2 人日、测试验收 1 人日,合计 **5–8 人日**(不含查询引擎复用成本,已在
dba-console-design 计)。

---

## 2. 现状与代码锚点

| 事实 | 锚点 | 影响 |
|---|---|---|
| 全局无慢查采集;巡检只做健康探针(容器/复制/连通),快照只在 degrade 跃迁时写 | `instance.rs sweep_once` / `capture_degrade_evidence`、`docker.rs exec_mysql_local` | 慢查采集为**新增后台周期任务**,与巡检并列独立 ticker |
| 现有调度/巡检以 tokio 后台任务 + env 周期可配 | `instance.rs start_sweeper`(`RDSCTL_SWEEP_SECS`)、`main.rs manager()` | 慢查采集器同构实现(`RDSCTL_SLOW_SECS`),不触碰执行引擎热路径 |
| 告警表语义按 (instance,kind) 去重,kind 现为 degraded/task_failed | `store.rs alert_open/alerts DDL` | 慢查"告警"不与 alerts 混用(避免同实例多 digest 互相顶掉),用治理队列表表达;如需顶层告警另议(§10) |
| 审计/evidence 双后端 trait、DDL 幂等、保留策略口径(热 30 天) | `store.rs StoreBackend`、`evidence_snapshots` | 新表与方法遵守同一模式 |
| 无 LLM 时已有确定性规则洞察体系与 playbook 目录 v0 | `insights.rs`、`docs/ai-roadmap.md` §3 | 治理建议=确定性规则(文档只建议、不自动执行),与 AI-0 哲学一致 |
| 查询引擎(只读执行/审计/EXPLAIN 通道)为 dba-console-design 交付 | `src/query.rs`(设计)、`docs/dba-console-design.md` §5 | 慢查 EXPLAIN/诊断复用该引擎与 `instances.query` 权限 |

---

## 3. 已确认决策

| # | 项 | 结论 |
|---|---|---|
| D1 | 数据源 | `performance_schema.events_statements_summary_by_digest`;不做慢日志文件解析(避免原始 SQL 落面) |
| D2 | 存储内容 | 只存 digest(哈希)+ digest_text(规范化、无字面量)+ 计数/耗时统计;不存 sample_text |
| D3 | 采集范围 | running 实例;节点默认 master + offline;`RDSCTL_SLOW_NODES=all` 时含 read |
| D4 | 差分 | 快照为累计值,控制面存上轮基线做差;差值为负视为节点/服务重置,丢弃该轮并重建基线 |
| D5 | 全局视图 | `GET /api/rds/slow` 按 digest 跨实例聚合;明细需 `instances.query`,列表 `instances.view` |
| D6 | 治理队列 | `slow_governance` 表承载候选与处置(open/ack/resolved),规则命中即入队,不去重互顶 |
| D7 | 保留 | 快照默认 30 天(`RDSCTL_SLOW_RETENTION_DAYS`),每小时清理;治理项保留至解决后 90 天清理 |
| D8 | 动作安全 | 只产生建议与 EXPLAIN(复用查询引擎,审计为真实操作人);不自动执行任何变更 |
| D9 | 告警耦合 | 不写 alerts 表(语义冲突);候选量/超阈值如需平台提醒,走治理队列 + 可选 summary 角标(§10) |

---

## 4. 采集器设计

### 4.1 周期与拓扑

- 独立后台任务,周期 env `RDSCTL_SLOW_SECS`(默认 60s,最小 10s),随 `manager()` 启动
  (与 `start_sweeper` 并列,注册于 `RdsManager::new` 之后)。
- 每轮扫描 running 实例(与巡检相同扫描口径,不打扰实例锁;若实例在生命周期操作中则跳过该轮)。
- 目标节点:master + offline(有则);`RDSCTL_SLOW_NODES=all` 追加 read。

### 4.2 采集 SQL(单条、自包含)

```sql
SELECT SCHEMA_NAME, DIGEST, DIGEST_TEXT,
       COUNT_STAR, SUM_TIMER_WAIT/1000000000 AS sum_ms,
       AVG_TIMER_WAIT/1000000000 AS avg_ms, MAX_TIMER_WAIT/1000000000 AS max_ms,
       FIRST_SEEN, LAST_SEEN
FROM performance_schema.events_statements_summary_by_digest
WHERE DIGEST IS NOT NULL AND SCHEMA_NAME IS NOT NULL
  AND COUNT_STAR > 0
ORDER BY SUM_TIMER_WAIT DESC
LIMIT 500;
```

- 经 `docker exec <node> mysql -N --raw -u root -p…`(容器内 root,管理面内部采集;仅本
  采集与账号供给使用 root,查询台不依赖 root)。
- digest_text 由 MySQL 生成:字面量已被 `?` 归一 —— 可安全落库/展示;**不在本模块任何
  环节写入 sample_text**。
- 拓扑表可能积累陈旧 digest:以 LAST_SEEN 距今 > 采集窗口×3 作为过期行,差分轮遇
  0 增量自然停止(见保留清理)。

### 4.3 差分与入库

- 控制面留存上轮基线(内存 + 落库 `slow_digest_snapshots` 每轮写入增量行):
  - 读当前累计 → 与基线(instance,node,digest)比对:
    `delta_count = cur - base`;`delta_sum_ms = cur_sum - base_sum`;
  - 任一差值为负 → 服务器/表重置(TRUNCATE/重启):丢弃本轮、写回基线、审计一次
    `slow_reset`(system,该实例节点,低噪音:同节点一天最多一条);
  - `delta_count > 0` 才写快照行;avg 以 `delta_sum_ms/delta_count` 计,写入快照的是本轮
    增量语义字段(count/sum/avg/max——max 用累计值近似,文档明示)。
  - 基线每轮覆盖(存于 `slow_digest_snapshots` 同键最新行或独立小表 `slow_baselines`;选
    独立 `slow_baselines(instance,node,digest,count,sum_ms,seen_at)`,避免大表全扫)。
- 写放大控制:单实例单轮 digest 数 > 阈值(默认 500)时仅保留 top 500(采集 SQL 已 LIMIT)。

---

## 5. 数据模型(新增表,MySQL + Memory 双实现)

### 5.1 `slow_digest_snapshots`

| 列 | 说明 |
|---|---|
| id | 自增 |
| ts | 采样秒(轮次时间) |
| instance / node / schema_name | 归属 |
| digest CHAR(64) / digest_text VARCHAR(1024) | 规范化 SQL(无字面量) |
| count_star / sum_ms / avg_ms / max_ms | 本轮增量计数 / 总耗时 ms / 均 ms / 累计 max ms |
| first_seen / last_seen | digest 在节点的首次/最近出现(采集 SQL 的 FIRST/LAST_SEEN) |

索引:`(ts)`、`(digest, ts)`、`(instance, ts)`;周期间隔即时间粒度(默认 60s 一档)。

### 5.2 `slow_baselines`

`(instance,node,digest)` 主键 + `count_star / sum_ms / seen_at`,采集差分读写;启动时无需恢复(空基线=丢弃首轮,见 §4.3)。

### 5.3 `slow_governance`(治理队列,全局 digest 口径)

| 列 | 说明 |
|---|---|
| id | 自增 |
| digest / digest_text | 全局归一的语句(跨实例合并用 digest;text 取最近样本) |
| total_count / total_ms / avg_ms / max_ms | 最近窗口(默认 24h)全局聚合(由快照实时计算后刷新本表或查询时实时算,取**查询时实时算**避免写放大) |
| instance_csv | 命中实例清单(最近窗口内,截断 20 个) |
| status | open / ack / resolved |
| assignee / note / created_at / updated_at / resolved_at | 处置记录 |

> 说明:本表只存**候选引用**(digest + 摘要),实时指标查询时聚合快照表计算;保留 90 天,
> 解决后 90 天清理。

### 5.4 Store 扩展(三处同步)

- `slow_snapshots_insert(...)`, `slow_baselines_update(...)`,
  `slow_baselines_get(instance,node)`(单轮批量语义:一次调用传整批,内部批量 INSERT/REPLACE);
- `slow_top_global(window_since, min_count, order, limit) -> Vec<Value>`(跨实例按 digest
  GROUP BY 聚合)、`slow_by_instance(instance, since, limit)`、`slow_digest_trend(digest, since)`(按 ts 分桶);
- `slow_gov_list(status?, q?)`、`slow_gov_action(id, action, assignee) -> bool`(ack/resolve);
- `slow_prune_snapshots(before_ts)`、`slow_prune_gov(before_ts)`。
- 各方法 MySQL 单语句 SQL(JSON_ARRAYAGG 风格与既有 store 一致);Memory 版对齐语义。

---

## 6. 规则与治理闭环

### 6.1 规则(确定性,候选入队)

每轮采集差分完成后,对全局窗口(默认 24h)聚合运行规则(env 可调):

- `RDSCTL_SLOW_AVG_MS`(默认 1000):`avg_ms ≥ 阈值 && count ≥ RDSCTL_SLOW_MIN_COUNT(默认 10)`;
- `RDSCTL_SLOW_MAX_MS`(默认 5000):`max_ms ≥ 阈值 && count ≥ 1`;
- 上升异常(可选):本窗口 vs 前窗口 total_ms 涨幅 ≥ `RDSCTL_SLOW_GROW_PCT`(默认 200%)且
  新窗口 count ≥ MIN_COUNT。

命中(digest 不在 open/ack 中)→ `slow_gov_list` 加 open 行(审计 `action=slow_gov_open`,
system);重复命中(已 open/ack)仅刷新 updated_at,不重复审计(防刷写,与 evidence 快照
"原因跃迁才写"哲学一致)。

### 6.2 处置与动作

- **EXPLAIN / 只读诊断**:页面按钮 → 复用 `src/query.rs` 引擎(dba-console-design),以
  `instances.query` 权限 + 真实操作人审计(`action=query_sql`),目标=该 digest 最近命中实例的
  master(或指定节点)。
- **忽略 / 解决**:`POST /api/rds/slow/gov?action=ack|resolve`,写 assignee;审计
  `action=slow_gov_ack/resolve`。权限:`tasks.manage`(处置动作归属运维,与既有治理动作
  权限口径一致)。
- 治理建议(仅展示,不自动执行):索引候选提示(对 `WHERE` 字段提示 EXPLAIN 结论由人
  判断)、SQL 改写方向(减少 `SELECT *`/避免函数列/参数化)、引流建议(只读走 offline)。
  建议文本由规则生成,注册为 insights 风格常量目录,不进 LLM。

### 6.3 保留清理

每小时:`slow_prune_snapshots(now - RDSCTL_SLOW_RETENTION_DAYS(默认 30)d)` +
`slow_prune_gov(90d)` + `query_audit` 30 天清理(跨特性共享,见 dba-console-design §8);
记录审计摘要一次(`action=retention_cleanup`)。

---

## 7. 接口契约

### GET /api/rds/slow?window=24h|7d&order_by=total_ms|count_star|avg_ms|max_ms&min_count=&instance=&q=&top=

全局 Top digest(默认 total_ms 降序,top=50):
```json
{ "items": [ { "digest": "…", "digest_text": "SELECT … FROM t WHERE a=?",
   "total_count": 120, "total_ms": 90000, "avg_ms": 750, "max_ms": 3000,
   "instances": ["x","y"], "instance_count": 2, "status": "open" } ],
  "window_since": 1720000000 }
```
权限:列表 `instances.view`;`digest_text` 明文项仅当调用者具备 `instances.query`(否则
`digest_text: "(需要 instances.query 权限)"`;实现按字段投影,见下)。

### GET /api/rds/slow/instance?instance=&since=&limit=

单实例 digest 明细(权限 `instances.view`,文本同上)。

### GET /api/rds/slow/trend?digest=&window=

某 digest 全局时间序列(按 ts 分桶 count/sum,权限 `instances.query`)。

### POST /api/rds/slow/explain?instance=&digest=

调查询引擎执行 `EXPLAIN <digest_text 还原为当前 schema 的语句>`——digest_text 是规范化
模板,EXPLAIN 前需以业务 schema 可用语句重填;v1 仅对**单表/无子查询模板**尝试替换
schema 名执行,失败返回引导语(文档如实声明限制)。

### POST /api/rds/slow/gov?id=&action=ack|resolve

治理处置(权限 `tasks.manage`;审计见 §6.2)。

> 权限实现:slow 接口在 `http.rs perm_for` 登记 `instances.view`(列表)与
> `instances.query`(文本/趋势/explain 细化在 api 层二次校验,与查询台同风格)。

---

## 8. 前端设计

- 导航「慢查询」(`data-view="slow"`,权限:`instances.view`):
  1. **全局 Top 面板**:窗口切换(24h/7d)、排序切换、min_count 过滤、实例下拉(任意/running
     实例)、Top N;行含 digest_text(脱敏后摘要展示,权限不足显示占位)、总耗时/次数/均/
     max、实例数徽标、状态徽标(open/ack/resolved);
  2. **治理队列**:open/ack 项列表,入口 = EXPLAIN/忽略/解决(权限按 §6.2 点亮);
  3. **趋势**:选中 digest 的折线(纯 CSS/SVG,无新依赖;数据 GET /api/rds/slow/trend);
  4. 每行“实例分布”展开 → 单实例 digest 明细(slow/instance)。
- 入口关系:实例详情「监控」页签可加“本实例慢查”跳转(复用同一视图带 instance 参数)。
- 文本可见性:`digest_text` 长度裁剪显示(默认 200 字符),完整文本 hover 提示仍需权限。

---

## 9. 边界与限制

- digest_text 是**模板化语句**:保留表/列名,可推断业务结构;其可见性按 `instances.query`
  授权(§7),与查询台同口径——租户级隔离仍为开放问题(引用 dba-console-design §13)。
- max 值取累计近似、avg 为窗口增量语义——不承诺与 MySQL 直查精确一致,文档明示。
- EXPLAIN 模板重填能力有限(v1 仅覆盖简单模板),复杂语句引导用户到查询台手工 EXPLAIN。
- 采集本身在容器内执行管理 SQL:低频率、小体量,不做并发放大;采集失败静默 + 审计留痕,
  不影响实例状态与查询台。

---

## 10. 测试与验收

单元:
1. 差分:正常增量 / 服务重置(负差)/ 空轮不写。
2. 聚合:跨实例同 digest 合并、窗口/排序/min_count、instance 过滤。
3. 规则:命中入队、重复命中不重复审计、ack/resolve 状态与 assignee。
4. Store 双后端:snapshots/baselines/gov/prune 语义一致;Memory 版差分行为对齐。
5. 文本权限投影:无 `instances.query` 时 digest_text 占位、趋势/explain 403。

协议级(扩展 acceptance 垫片:mysql 分支返回固定 digest 表格;fake 容器名可寻址):
6. 注入已知慢 digest(垫片返回)→ 全局 Top 正确、治理项生成、EXPLAIN 走查询引擎并有审计。
7. 保留清理删除过期行;`RDSCTL_SLOW_SECS` 未配置时零行为/零表写入(验收基线回归)。
8. 回归:既有 P0 三项 + `cargo test` 全绿。

---

## 11. 同步点与依赖

- 复用:`src/query.rs` 引擎(dba-console-design)、`instances.query/view` 权限、Store 扩展
  模式、保留清理 ticker(三表共享)。
- 衔接:AI-0 洞察/日报可后续消费 snapshots(规则报告升级项,登记不改当前行为);与
  scaling-design 事件驱动巡检(M1)不冲突(采集器届时随事件管道迁移)。

---

## 12. 开放问题

1. 租户/实例级慢查可见域(同上,全局 RBAC 尚无实例级授权)。
2. 是否需要平台级“慢查爆发”顶层告警(复用 alerts 语义需先解决同实例多 digest 去重冲突,
   或引入 kind+ref 复合去重键)。
3. 阈值参数按实例覆盖(实例 meta `slow_avg_ms` 等)是否首版即支持——建议 v1 仅全局 env,
   实例级后置。
4. EXPLAIN 模板重填的能力边界是否需要 SQL 解析器(远期,违背零依赖策略,登记不采纳)。

---

## 实施偏差登记(编码落地后,2026-09-03)

| 设计原稿 | 落地现状(锚点: src/slow.rs / src/store.rs) |
|---|---|
| 采集失败「审计留痕」 | 失败仅静默跳过/降级日志,不逐次写审计(避免噪音);关键治理动作(`slow_gov_open`)仍审计 |
| 规则命中后写 alerts | 不写 alerts(语义冲突);命中入 `slow_governance` 队列(与设计 D9 一致),页面「治理队列」展示 |
| 治理项保留 90 天(含 open/ack) | 清理条件=已 resolved 且 resolved_at 超期 90 天;open/ack 长期保留待处置(防误清) |
| 每实例可配阈值 | v1 全局 env(`RDSCTL_SLOW_AVG_MS / MAX_MS / MIN_COUNT / GROW_PCT`),`GROW_PCT` 上升异常规则暂未启用(登记) |
| 趋势分桶 | `trend_rows` 默认 3600s 桶(ts 对齐,count/sum) |
