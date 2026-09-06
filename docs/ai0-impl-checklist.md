# AI-0 确定性洞察基座 — 实施清单(docs/ai0-impl-checklist.md)

> 依据:`docs/ai-roadmap.md` v2 §4 AI-0 / §3.1–3.3 / §3.5。
> 范围:探针升级 + 异常快照落库、evidence 契约、规则版聚类与异常群报告、日报/周报(规则版)、
> playbook 目录 v0(仅建议不执行)、手工基线度量、`/api/rds/insights` 与 `/api/rds/report`、
> 前端"洞察"面板。
> **不含**(明确延后):任何 LLM 调用(AI-1 gate 后)、建议动作的 dry-run/执行(playbook 操作台,
> AI-1)、insights/ai_requests 持久化表(YAGNI,AI-1 事件管道落地)、跨实例因果推断(仅模式归因)。
> 估量:~11 人日(团队校准)。总红线见 ai-roadmap §5:无 AI 配置行为零变化;不进巡检热路径
> (快照仅在状态跃迁/原因变化时写)。

---

## 1. 数据与契约

### 1.1 evidence contract(本次定义并落地为代码结构)

分析/报告/聚类统一读:`EvidenceContext { instance_facts, audit_window, snapshots, task_outputs }`

- `instance_facts`:`{name,status,status_label,region,az,tenant,last_error,created_at}`(来自
  `RdsInstance::to_view`,剔除 `root_password`、`network` 内网细节可按配置隐去);
- `audit_window`:该实例近 N=30 条审计(`store.audit_list` 过滤 instance;params/result 已截断 240);
- `snapshots`:`evidence_snapshots` 中该实例最新 ≤3 条(kind=degrade);
- `task_outputs`:该实例最近失败任务节点 output(AI-1 聚类用,AI-0 报告仅计数)。

### 1.2 新表 `evidence_snapshots`(追加进 `store.rs` DDL 常量,IF NOT EXISTS 幂等)

```sql
CREATE TABLE IF NOT EXISTS evidence_snapshots (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts BIGINT NOT NULL,
    instance VARCHAR(96) NOT NULL,
    kind VARCHAR(32) NOT NULL DEFAULT 'degrade',
    reason VARCHAR(240) NOT NULL DEFAULT '',
    facts_json MEDIUMTEXT NOT NULL,
    KEY idx_ev_inst_ts (instance, ts),
    KEY idx_ev_ts (ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

- `kind` 预留 `task_fail` 等(AI-1 用),AI-0 只写 `degrade`。
- 保留策略:与审计归档对齐(热 30 天 + 归档,M1 审计分表时一并加清理任务)——AI-0 不实现清理。

### 1.3 `StoreBackend` trait 扩展(MysqlBackend + MemoryBackend 双实现)

```rust
fn evidence_insert(&self, instance: &str, kind: &str, reason: &str, facts_json: &str);
/// 某实例最新 n 条(新→旧)
fn evidence_latest(&self, instance: &str, n: usize) -> Vec<Value>;
/// 某时间窗后全部(新→旧;聚类/报告用)
fn evidence_since(&self, kind: Option<&str>, since_ts: u64, limit: usize) -> Vec<Value>;
```

- MysqlBackend:沿用现有 `q`/`q_db` 执行辅助(每语句 spawn mysql CLI,风格不变);
- MemoryBackend:内部 `Vec<(ts,instance,kind,reason,facts)>` + 排序,语义与 MySQL 一致;
- `Store` 加同名 pass-through(既有 `Arc<Store>` 用法不变)。

### 1.4 `audit_list` 扩展时间过滤(日报需要)

trait `audit_list(limit, instance, action, q)` → 增加 `since: Option<u64>`(参数化,Mysql:
`AND ts >= {since}`;Memory:过滤)。调用点:`RdsManager::audit`、`api::audit` 同步透传(None)。

### 1.5 单元测试(store)

- evidence_insert/latest/since 双后端语义一致(仿现有 `memory_backend_*` 测试);
- audit_list since 过滤生效。

---

## 2. 探针升级与异常快照(instance.rs + docker.rs)

### 2.1 docker.rs 新增辅助(保持单一 `docker()` CLI 封装风格)

```rust
/// 返回 "Status|ExitCode|RestartCount";容器缺失时返回 None
pub async fn container_state(container: &str) -> Option<String> {
    docker(&["inspect", "-f", "{{.State.Status}}|{{.State.ExitCode}}|{{.RestartCount}}", container]).await.ok()
}
```

`logs_tail(container, n)` 已支持任意 n(现 30)→ 快照用 n=40。

### 2.2 实例侧快照采集(instance.rs)

新增私有 `async fn capture_degrade_evidence(inst: &RdsInstance) -> String`(返回 facts_json),
在 degrade 前采集:

1. **容器事实**:proxy + 全部节点逐个 `container_state` → `{name, present, state}`(缺失记
   `present:false`),存在但异常的再 `logs_tail(c, 40)`;
2. **复制硬事实**(每从节点,SQL 用 CONCAT_WS('|') 单值化,与现有查法一致,避免引入解析器;
   实现时以现场 8.0 列校准):
   - `SELECT CONCAT_WS('|',SERVICE_STATE,IFNULL(LAST_ERROR_NUMBER,''),IFNULL(LAST_ERROR_MESSAGE,''))
     FROM performance_schema.replication_connection_status WHERE CHANNEL_NAME=''`
   - `SELECT CONCAT_WS('|',STATE,IFNULL(LAST_ERROR_NUMBER,''),IFNULL(LAST_ERROR_MESSAGE,''))
     FROM performance_schema.replication_applier_status_by_coordinator WHERE CHANNEL_NAME=''`
   - 空行 ⇒ `STOPPED`;查询失败 ⇒ 记错误文本;
   - 每节点 `@@GLOBAL.gtid_executed` 存档参考(**不作 lag 结论**,异步复制下快照比较无意义;
     AI-1 的追平判定已有 `WAIT_FOR_EXECUTED_GTID_SET`);
3. **代理事实**:`SELECT 1` 可达性 + 出错时日志尾;
4. **脱敏**:对采集的日志/错误文本替换 `ROOT_PASS`、`REPL_PASS`(及形如 `pass` 键值行),单测覆盖;
5. 各段总长封顶(如日志合计 ≤60 行),组装 `serde_json::json!` 输出。

### 2.3 sweep_once 集成(instance.rs,degrade 分支)

写入时机 = **状态跃迁或原因变化**(避免每 30s 对同一问题刷快照):

```text
if problems 为空 → 恢复逻辑不变(自愈后 audit recover)
else:
  msg = problems.join("; ")
  if inst.status == Running            // 降级跃迁
     || inst.last_error != msg {       // 降级原因变化
     facts = capture_degrade_evidence(&inst).await   // 失败仅 warn,不阻断降级
     store.evidence_insert(&name, "degrade", truncate(msg,240), &facts)
  }
  // 其后 set_status/audit degrade 逻辑保持原样(状态机与审计语义零变化)
```

- `op_locks` 跳过、audit action/result、`tracing::warn` 全部保持;P0 用例 3(容器缺失→degraded)
  语义不变,仅新增旁路快照写。
- 失败任务快照(kind=task_fail)属 AI-1 边界,本次不做。

### 2.4 单元测试

- capture 脱敏:注入含口令文本的假日志,断言落库无口令;
- 时间/去重:sweep 两次同问题只写 1 条,原因变化才新增。

---

## 3. 规则引擎与新模块 `src/insights.rs`(纯规则,无 LLM)

### 3.1 模块职责(后续 AI-1 的 LLM 走 `src/llm.rs`,insights 为宿主)

- `normalize_reason(msg) -> Pattern`(纯函数):
  小写 → 替换实例特有 token(`rds-<实例名>-master/slave-N/proxy` 归一到 `<node>` 类;连续数字/端口 →
  `<n>`)→ 压缩空白 → 输出 canonical pattern + 人工可读 label;单测覆盖(见下)。
- `cluster_anomalies() -> Vec<Cluster>`:
  遍历 `manager()` 中 degraded/failed 实例(复用 `list_filtered(status)` 语义),按
  `normalize_reason(last_error)` 分组;每组挂 `{pattern,label,count,members:[{name,region,az,
  tenant,status,last_error}],latest_snapshots, suggested_playbooks}`;按 count 降序;
  `suggested_playbooks` 由 §3.2 关键词表命中(如 `复制中断|IO 线程|applier`→start_replica;
  `代理.*缺失|不可达`→restart_proxy;实例 failed 且 last_error 前缀"任务 "→retry_task/
  destroy_residual)。
- `report(period) -> {period, text, counts}`:
  `text` 为纯文本中文摘要(创建/销毁/扩容计数、degraded/failed 清单、反复失败任务、聚类 Top N),
  数据源 `summary()` + `audit_list(…, since)` + `cluster_anomalies()`;`counts` 为结构化回显。

### 3.2 playbook 目录 v0(代码内强类型注册表)

```rust
struct Playbook { id, name, risk: Risk /*Low|High*/, state_gate: &str,
                  steps_hint: &str /*AI-0 展示用文本*/,
                  exec_map: ExecMap /*见下*/ }
```

| playbook_id | 触发关键词(证据特征) | risk | state_gate | exec_map(AI-0 只建议) |
|---|---|---|---|---|
| start_replica | 复制中断/IO 线程/applier 未运行 | Low | degraded | **缺原语 → 已登记 M1(M1b)**:随 §7.1 幂等补全入库(START REPLICA + 追平校验),AI-1 playbook 消费 |
| restart_proxy | 代理缺失/不可达 | Medium | degraded | **缺原语 → 已登记 M1(M1b)**:proxy 重跑 Step 从 create_nodes 抽出,AI-1 消费 |
| retry_task | 实例 failed + "任务 N 失败" | Low | failed | **缺接口 → 已登记 M1(M1a)**:任务级重试/单任务重跑随队列 retry 语义落地,AI-1 消费 |
| destroy_residual | failed/降级残留、重建提示 | High | degraded/failed | 现成:`POST /api/rds/destroy`(AI-1 操作台接入) |

> 清单价值点:AI-0 即暴露三个"缺失原语"(单任务重跑、proxy 重跑 Step、复制修复 Step),它们
> 是 AI-1 操作台与 M1 Step 幂等补全的输入——**已登记进 scaling-design §12 M1 增补登记
> (M1a/M1b),AI-0 不实现、M1 随对应子项落地**(登记闭环)。

### 3.3 单元测试

- normalize:相同语义不同实例名/端口 → 同 pattern;不同语义不合并;
- cluster:构造 m0 合成风格实例集(如 30 台同 `代理不可达`、5 台各不相关)→ 断言归群正确、
  批量建议只出现于群首;
- report:空库与样例库各生成一次,无 panic,text 含计数。

---

## 4. API 与路由(api.rs / http.rs)

```text
GET /api/rds/insights?scope=degraded|all&region=&az=&q=      → 200 {clusters:[…], generated_at}
GET /api/rds/report?period=today|week                         → 200 {period, text, counts}
```

- 前者挂 `api::insights`,后者 `api::report`;均走既有登录态(`http.rs` 路由表 + auth 不变);
- 响应 JSON 字段即 §1.1/§3.1 结构;`clusters[].members[].latest_snapshots` 只带 facts 的
  脱敏摘要字段(如容器状态/LAST_ERROR/日志头 3 行),不整包透传;
- 失败路径:store 不可用 → 500 JSON error;快照缺失 → members 该字段为空数组(不报错)。

---

## 5. 前端"洞察"面板(rds.html)

- 头部按钮"洞察/异常群"(与"审计日志"平级),点击**按需加载**(fetch 一次,不进 2s 轮询);
- 渲染:聚类卡片(Pattern + 命中实例数 + member 徽章可点进实例详情 + suggested_playbooks 提示
  文本与 risk 色标);每实例附快照摘要行;顶部"生成日报/周报"按钮 → 展示 report.text + 复制;
- 复用现有 DOM 辅助(`el/mk/toast`、status 色 class、卡片样式);空态文案对齐现有风格;
- 无数据时隐藏入口不报错(默认 env 全关的部署零变化)。

---

## 6. 手工基线度量(scripts/baseline.sh)——先于 AI-0 变更落地前执行并留档

- 用 lib.sh 的 mysql 客户端查 `audit_log`:
  - **MTTR**:每实例取 `ts` 最近一次 `(user='sweeper',action='degrade')` 到首个后续
    `(action IN ('recover','destroy'))` 的间隔(小时);
  - **处置步骤数**:同窗口内 admin 发起的 audit 行数(代理指标);
  - 输出 `logs/baseline-mttr-<date>.csv` + 摘要;AI-0 上线后再跑一次同脚本对比,写入验收记录。
- 语义备注:audit 天然含 degrade/recover/destroy 时间戳 → 基线可离线从历史审计算出,无需改代码。

---

## 7. 实施顺序(每步可独立提交、单测随行;≈11 人日)

| # | 步骤 | 涉及 | 估量 |
|---|---|---|---|
| S1 | DDL + trait 扩展(evidence_*/audit_list since)+ 双后端 + store 单测 | store.rs | 1.5 |
| S2 | docker::container_state + 复制/代理硬事实 SQL 辅助 | docker.rs | 1 |
| S3 | capture_degrade_evidence + sweep_once 集成 + 脱敏/去重单测 | instance.rs | 1 |
| S4 | insights.rs:normalize/cluster/playbook v0/report + 单测 | src/insights.rs | 2 |
| S5 | api.rs/h http.rs 路由 + JSON 契约 | api.rs, http.rs | 1 |
| S6 | rds.html 洞察面板(懒加载 + 快照摘要 + report 展示) | rds.html | 1.5 |
| S7 | baseline.sh + 手工基线留档(变更前) | scripts/baseline.sh | 0.5 |
| S8 | acceptance:快照断言 + insights/report 冒烟 + P0 三项回归 + 内存后端全绿 | tests/acceptance.rs | 1.5 |
| S9 | 文档:AI-0 验收记录(基线前后对比 + gate 数据供 AI-1) | docs/ | 0.5 |

---

## 8. 验收(Definition of Done)

1. 注入"同代理不可达 30 台"案例 → 聚类归 1 群并给出批量处置模板(§3.3 测试);degraded 实例
   均有 `evidence_snapshots` 且 facts 含失败细节(LAST_ERROR/exit code),非布尔结论;
2. `/api/rds/insights`、`/api/rds/report` 鉴权后返回契约 JSON,无 `root_password`/口令文本泄漏
   (红线下限:全包 grep 口令为空);
3. degrade 语义与审计与 AI-0 前一致(P0 用例 3 回归);无 AI 配置/内存后端下全部测试绿;
4. baseline.sh 前后各一份留档,报告差异;缺失原语(start_replica/restart_proxy/retry_task)
   已登记到 scaling-design §7.1 幂等/Step 清单;
5. UI:面板按需加载,不进轮询,无数据时入口隐藏。

## 9. 风险与备注

- sweep 写放大:仅状态跃迁/原因变化写快照,同问题不重复写(§2.3);
- 快照采集失败不得阻断降级/审计(仅 warn);
- 复制列以现场 8.0 为准校准(避免 5.7/8.0 表名差异,现有代码已固定 mysql:8.0);
- evidence_snapshots 膨胀由 M1 保留任务一并治理;AI-0 不加清理逻辑。
