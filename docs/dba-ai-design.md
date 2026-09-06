# rdsctl DBA AI 提能功能设计 — 性能诊断 · 监控处置 · 容量成本 · 知识问答 · 自动报告(dba-ai-design)

> 状态:设计评审稿(2026-09-04)。锚点用符号名(模块/函数/表/接口,编码落地后更新为行号)。
> 依据与关系:
> - `docs/ai-roadmap.md` v2(AI-0..AI-4 分期与红线)、`docs/ai0-impl-checklist.md` / `docs/ai0-acceptance.md`(AI-0 已交付)
> - `docs/slow-query-design.md` + 实施偏差登记(慢查治理已落地)、`docs/dba-console-design.md`(查询引擎)、
>   `docs/dba-features-milestone.md`(DBA 三件套已落地)、`docs/scaling-design.md`(M0 已交付,M1..M3 规划)
> - 本文按「功能维度」组织(1 性能诊断与调优 / 2 监控与故障处置 / 3 容量与成本 / 7-1 知识库问答 / 7-2
>   自动报告),与既有「分期编号」(AI-0..4 / M0..M3)是**两种坐标系**;每块功能内部拆「规则版」与
>   「LLM 增强」并各自标注归属,不新增分期编号、不与既有里程碑冲突。

---

## 1. 结论摘要

现状盘点:AI-0(证据快照 + 规则聚类 + playbook v0 + 即时日报/周报)与 DBA 三件套(查询台/慢查治理/
备份联动)均已交付;**零 LLM 代码、零指标时序、容量与问答空白**。本文为五块功能给出设计,核心原则:

- **规则版先行、LLM 可插拔增强**(沿袭 ai-roadmap D1–D6):每块功能先交付确定性、可单测、无 LLM 的
  规则版;LLM 增强统一在 **AI-1 gate(回放不劣于规则版 + 采纳率达标)+ 形态 B 网关**就绪后开启,
  gate 前入口标注实验性。
- **建议不产动作**:所有建议 = 规则/playbook 命中 + evidence_ref;LLM 只做解释/排序/填参;执行仍走
  既有状态机 + 人工确认 + 审计(红线不变)。
- **零新 crate / 零新出网路径**:采集与检索全部复用既有 mysql CLI、docker CLI、规则检索;LLM 请求经
  未来形态 A/B 通道(ai-roadmap §3.4),本文不引入 HTTP client。
- **无 AI 配置行为零变化**:全部新增功能 env 默认关闭或默认保守值;新表幂等 DDL + Memory 双实现。

| 块 | 规则版(近期可交付) | LLM 增强(gate 后) | 远期依赖 |
|---|---|---|---|
| 1 性能诊断 | EXPLAIN/结构启发索引建议、GROW_PCT 启用、gov 增列 | digest 中文解释与建议排序 | 参数调优=M1 指标 |
| 2 监控处置 | 告警群归聚 + 群处置 + 事件时间线 | 单实例 RCA 叙述 | 指标异常=M1 心跳 |
| 3 容量成本 | 磁盘/实例数采样外推 + 水位与建议 | 容量解读(并入 7-2/AI-4) | 负载容量=M1 指标;跨区=M1.5/M2 |
| 7-1 知识问答 | runbook 词表检索模板问答(值班 FAQ) | RAG 中文问答(形态 B) | 采纳闭环=AI-1 insights 落库 |
| 7-2 自动报告 | 定时生成 + 归档 + 慢查/容量/alerts 并入 | 报告解读摘要(后置) | — |

---

## 2. 现状差距矩阵(编码锚点核对)

| 块 | 可用现状(锚点) | 差距 | 本文补什么 |
|---|---|---|---|
| 1 | 慢查采集/差分/全局 Top/治理队列/EXPLAIN 走查询引擎(`slow.rs`, `slow_governance`);schema 与索引查询(`schema_index`);查询只读引擎(`query.rs` 分类器含 EXPLAIN) | 治理建议为静态文本常量;无索引候选生成;GROW_PCT 上升规则登记未启用;无"为什么慢"叙述 | 规则建议层 + digest 详情"建议卡" + LLM 解释增强(§4) |
| 2 | 探针升级 + degrade 快照(`instance.rs capture_degrade_evidence`);规则聚类/playbook v0(`insights.rs`);alerts 按 (instance,kind) 去重 + ack/resolve(`store.rs alert_open/alert_action`) | 告警逐条无群;无跨 alerts/快照/审计的时间线;无 RCA 叙述;无值班问答 | 群归聚与群处置 + 时间线(2a,规则)+ RCA 叙述(2b,LLM)(§5) |
| 3 | instances 表含 region/shard/status/data(meta:spec/data_size/buffer_pool 字符串);audit 含 create/destroy/scaleout 历史;proxy 实时指标(`monitor_*`/`proxy_metrics`,仅实时聚合) | 无任何指标时序与容量采样;无外推与水位;无成本口径 | capacity_samples 采样 + 磁盘外推 + 实例数水位 + 成本估算(规则)(§6) |
| 7-1 | playbook 注册表 v0(`insights.rs playbook_registry`);evidence_snapshots;审计含完整处置序列;insights 结果未落库(ai0-acceptance 偏差 2) | 无问答入口、无语料组织、无 FAQ 沉淀 | runbook 词表问答(规则)+ RAG 增强 + 采纳回写(AI-1 后)(§7) |
| 7-2 | 规则版日报/周报即时生成(`insights.rs compose_report` + `/api/rds/report`,权限 audit.view) | 无定时、无归档/历史、内容未并入慢查/容量/alerts | reports 表 + 定时器 + 历史查看 + 内容并表(§8) |

---

## 3. 共享设计原则(五块一致遵守)

1. **证据优先于模型**:所有 AI 产出必须带 `evidence_ref`(快照 id / digest / audit 行 / 采样行),
   无证据不出结论;质量瓶颈在证据不在模型。
2. **规则版 = 主路径,LLM = 可插拔解释层**:规则版先上线并回放评测;LLM 结果不得优于规则版结构,
   同构 JSON 契约(ai-roadmap §3.4: `{summary, hard_facts[], likely_causes[], actions[playbook]}`)。
3. **不进热路径**:巡检/采集 ticker 只写数据;聚类/预测/报告/问答全部按需计算或异步,失败静默降级
   不阻塞既有逻辑。
4. **动作最小面**:建议动作一律指向 playbook_id(insights 注册表 v0 起步),执行面为 AI-3 操作台;
   本文所有新增"处置"接口(群 ack 等)走既有动作语义与审计。
5. **脱敏与合规**:服务端组包;prompt/日志/落库一律剔除 `root_password`/`query_secret`;digest_text
   可见性沿用 `instances.query` 投影;LLM 请求只含脱敏摘要。
6. **零新依赖**:不新增 crate;检索/外推/归一化全部手写或复用现有辅助;Memory/MySQL 后端同步。
7. **默认关闭零变化**:新 env 默认 0/保守;新 ticker 仅在 enabled 时启动;验收基线回归沿用 P0 三项。

---

## 4. 块 1 — 性能诊断与调优(慢查建议层)

### 4.1 定位

在已落地的慢查治理闭环(slow-query-design)上追加**建议层**,不改采集/差分/治理队列主体语义。
诊断对象 = `slow_governance` open/ack 项与全局 Top digest;证据 = digest_text(脱敏模板) + 该 digest
窗口聚合(avg/max/count/趋势桶) + EXPLAIN 输出 + 目标表结构(列/现有索引,来自 `schema_index`)。
目标:**把"它慢"升级为"为什么慢 + 可以做什么(证据化建议)"**,全部建议只读展示,不自动执行。

### 4.2 规则版(R1–R4,确定性,近期交付)

| # | 规则 | 输入 | 输出建议类型 | 门槛 |
|---|---|---|---|---|
| R1 | EXPLAIN 信号 | 复用查询引擎执行 EXPLAIN(只读 + `query_sql` 审计);解析 `type`/`Extra`/`rows` | `index`:建议对命中的 `WHERE`/`ORDER BY` 列建索引;`rewrite`:提示 Using filesort/temporary/隐式转换 | 仅单表/无子查询模板可 EXPLAIN(slow-query §7 既有限制),失败出引导语 |
| R2 | 结构启发(离线版 R1) | digest_text 词法提取谓词列 vs `schema_index` 现有索引 | `index`:缺失索引候选(单列/组合前缀);`routing`:只读走 offline | 无 instances.query 权限者仅见文本提示,不见列名明细 |
| R3 | 上升异常 | 启用 slow.rs 预留 `GROW_PCT` 规则(本窗口 vs 前窗口 total_ms 涨幅 + min_count) | 治理项标记 `rising`,置顶 + 建议"优先核查最近变更/新流量" | 命中且 digest 非 open/ack 才入队(沿用防刷写) |
| R4 | 聚合异常 | 窗口 avg/max 阈值(复用现有 env) | 入治理队列(现状已具备) | 现状语义不变 |

- 建议文本注册为 insights 风格常量目录(`suggest_kind` + 模板),**不进 LLM**;
- **落库**:`slow_governance` 新增列 `advice_json MEDIUMTEXT`(幂等 `ALTER TABLE ... ADD COLUMN`,
  双后端同步;空值兼容既有行),建议实时生成 + 命中写入,重复命中仅刷新 updated_at;
- 与 EXPLAIN 一致性:EXPLAIN 输出不落库(敏感),建议引用其摘要文本字段。

### 4.3 LLM 增强(标注:AI-1 gate + 形态 B 后)

- 接口:`POST /api/rds/slow/advice`(入参 digest + 可选 instance;权限 `instances.query`);
- 输出(结构化,解析失败原文兜底):
  ```json
  { "summary": "中文一句话结论",
    "likely_causes": [ {"cause": "…", "evidence_ref": "…", "confidence": 0.8} ],
    "suggestions": [ {"type": "index|rewrite|routing|verify", "detail": "…", "risk": "low", "evidence_ref": "…"} ] }
  ```
- 输入组包:digest 聚合指标 + trend 桶 + EXPLAIN 摘要 + 表结构摘要 + 治理历史;全部脱敏;
- 页面:治理项/全局 Top 行内"AI 解释"卡(实验性标注)+ 采纳/不采纳反馈 → accepted 入示例池
  (ai-roadmap §3.5);只解释与排序,动作枚举仍由 playbook 目录给出。

### 4.4 明确不首版

- **参数自动调优**(buffer_pool/innodb 等):需实例负载时序 + 变更回滚语义,标注依赖 M1 指标心跳
  (§11 远期),本文仅登记方向;
- digest 全量重写(自动改写 SQL 并验证):高风险,不做;
- EXPLAIN 模板重填引入 SQL 解析器:违反零依赖策略,不采纳(slow-query §12 登记延续)。

---

## 5. 块 2 — 监控与故障处置

### 5.1 现状与目标

alerts 语义:按 (instance,kind) 去重 open、ack/resolve 人工处置;同因故障(如一台宿主机/代理异常牵连
几十台)会得到几十条独立 degrade 告警,缺乏"这是一件事"的表达。目标:
**2a 确定性告警群归聚 + 群处置 + 单实例事件时间线**(规则,近期);**2b 单实例 RCA 叙述**(LLM,gate 后)。

### 5.2 2a 告警群归聚与群处置(规则版)

- **不改 alerts DDL**(消息原文保留,去重语义不变);群归聚**实时计算**,不落物化:
  - 输入:open 告警(含 degraded/task_failed 等 kind)+ 各自实例 `last_error` + 最近 evidence 快照摘要;
  - 归一:`insights::normalize`(实例名/容器/端口 token 归一,复用 AI-0 聚类哲学)对
    (instance+message) 归群 → `{pattern, label, count, members[], latest_snapshots[], suggested_playbooks[]}`
    (playbook 命中复用 `playbook_hits`);
  - 语义输出:群卡标 `单点牵连`(同 pattern 多成员)vs `逐台独立`(成员 pattern 各异),批量处置模板
    仅出在群卡(与 AI-0 聚类一致);
- **群处置**:`POST /api/rds/alerts/gov?group=pattern&action=ack`(权限 `alerts.handle`):
  对群内 open 告警逐条走既有 `store.alert_action`(ack),**一次审计** `action=alert_group_ack`
  (params 带 pattern + 成员数),其余处置动作不新增;
- **单实例事件时间线**:按实例合并 `alerts`(open/ack/resolved)+ `evidence_snapshots` + 同窗
  `audit_log`(action/ts),按 ts 排序返回 → 实例详情「告警/洞察」页签渲染;数据源全部现成;
- **慢查爆发不写 alerts**(沿用 slow-query D9):治理队列表达 + 洞察群视图角标,不扩 alerts 去重键
  (顶层告警仍为开放问题,§14);
- 阈值自适应/漂移检测:无指标时序不可做,标注 M1(§11)。

### 5.3 2b 单实例 RCA 叙述(LLM,gate 后)

- 输入 = evidence 契约(ai-roadmap §3.3):instance_facts + 最新 ≤3 条 degrade 快照摘要 + audit_window
  (近 N 条,截断脱敏)+ 最近失败任务 output 摘要;
- 输出:中文根因排序 `{summary, likely_causes[{cause,evidence_ref,confidence}], playbook_hits}`,证据
  每条可点回快照/审计原文;
- gate:回放历史 degrade 案例,LLM 版不劣于规则版硬事实报告且采纳率 ≥ 阈值才开默认路径,否则入口
  标实验性(ai-roadmap AI-1 gate);
- 值班问答复用同一证据组包,见 §7。

---

## 6. 块 3 — 容量与成本

### 6.1 数据源分层(按确定性排优先级)

| 层 | 数据 | 来源 | 频率/保留 |
|---|---|---|---|
| L1 磁盘采样(核心) | 每 running 实例数据目录磁盘用量(df 一次/容器)+ data_gib 解析 | 新增采样 ticker:容器内 `df -P`(经 docker CLI,复用 exec 风格);`data_size` 字符串(如 `128GiB`)规格化 parse helper | `RDSCTL_CAP_SECS`(默认 300s)/ 90 天 |
| L2 结构水位 | 实例数按 region/shard/status;创建/销毁/扩容计数 | 现有 `instances` 表 + `audit_log`(create/destroy/scaleout) | 查询时实时算 |
| L3 负载趋势(远期) | CPU/连接/吞吐历史 | M1 心跳/指标上报 | 依赖 M1,本文不设计 |
| L4 成本口径 | spec × 运行时长;region/shard 维度 | instances.created_at + audit 终态事件;单价表 env(默认空=成本功能 off) | 查询时实时算 |

- 采样 ticker 与巡检并列(随 `manager()` 注册,同 slow::start 模式);实例生命周期操作中跳过该轮;
  采样失败静默 + 降级日志,不审计不告警(慢查采集同款策略);
- L1 落库新表 `capacity_samples`(见 §9);L2/L4 不新增表(审计与实例表即源)。

### 6.2 预测(规则版外推)

- 输入:某 (instance,node) 最近样本 ≥ 7 条且跨度 ≥ 72h,否则输出 `insufficient`;
- 方法:对 `disk_used_bytes` 线性最小二乘 + 保守上限(线性 vs 双点年化指数取**更早触线者**);
- 输出:`{ trend: {growth_bytes_per_day, slope_quality}, days_to_90pct, forecast_at_90pct }`;
  阈值 `RDSCTL_CAP_THRESHOLD_PCT`(默认 90);样本少于门槛/波动过大 → `insufficient`,不硬给数字;
- 水位:L2 按 region/shard 聚合实例数与近 30 天净增(create − destroy),输出 `days_to_shard_limit`
  (上限参数 `RDSCTL_CAP_SHARD_LIMIT`,默认 10000,对齐 scaling-design §11);
- 全部规则纯函数(可单测,零 LLM);不写 alerts,预警并入 7-2 报告与容量卡。

### 6.3 建议与成本(规则文本,动作挂 playbook)

| 场景 | 规则信号 | 建议(文本 + playbook 挂起) |
|---|---|---|
| 磁盘逼近 | days_to_90pct ≤ `RDSCTL_CAP_WARN_DAYS`(默认 14) | 归档/清理指引;扩容登记(执行=数据面伸缩,依赖 M1,仅登记) |
| 僵尸实例 | 持续 failed/disabled > 30 天(审计佐证) | 回收候选(对齐 destroy_residual playbook,不自动执行) |
| 水位逼近 | days_to_shard_limit ≤ 30 | shard 拆分/新 shard 规划提示(文本) |
| 成本(off 默认) | spec × 时长估算,region/shard 汇总 | 月度成本分布 + Top 成本实例;LLM 解读后置(AI-4) |

### 6.4 UI

- 洞察/概览新增**容量卡组**(实例 Top 逼近、region/shard 水位、样本不足提示);
- 实例详情「监控」页签容量小卡(磁盘趋势 mini 图,纯 CSS/SVG,复用 slow trend 画法);
- 数据入口:`GET /api/rds/capacity?scope=instance|group|global`(权限 `instances.view`),
  预测明细 `GET /api/rds/capacity/forecast?instance=`(同上);成本视图 `GET /api/rds/capacity/cost`
  (权限 `audit.view`,单价未配置返回 503 + 提示)。

---

## 7. 块 4(7-1)— 知识库问答(值班助手 / FAQ)

### 7.1 定位与语料三层

问题形态:值班场景的"为什么 X degraded""这类报错怎么处理""上个月同类问题怎么解决的"。
语料三层(按可控性排序):

| 层 | 内容 | 载体 | 就绪时点 |
|---|---|---|---|
| T1 静态 runbook | playbook 注册表描述 + 修复手册条目(代码内常量目录,AI-0 已有注册表 v0) | 代码常量(问答前组装) | 现在 |
| T2 运行知识 | 采纳案例(insights.actions accepted + outcome recover/fail)、治理处置记录 | insights 落库(AI-1)+ `slow_governance` 现有表 | AI-1 后 |
| T3 现场证据 | 目标实例最近 degrade 快照摘要 + audit_window + 状态 | evidence_snapshots/audit(现成) | 现在 |

### 7.2 规则版问答(确定性,近期交付,入口标注"规则版")

- 意图解析(纯规则,零 LLM):(a) 状态问询(实例 + degraded/failed)→ 状态 + 快照摘要;(b) 原因问询
  ("为什么")→ normalize(label) 命中 playbook + T3 证据摘要;(c) FAQ 问询("怎么处理/遇到过吗")→
  T1/T2 词表检索(关键词倒排,零依赖);
- 输出为模板化中文回答 + 结构化引用(evidence_ref/playbook pill/risk 色标),与洞察面板同风格;
- 实现落点:新模块 `src/ask.rs`(纯函数:`resolve(intent) -> Answer`),insights 为宿主;无 LLM、无网络。

### 7.3 LLM 增强(RAG,标注:形态 B 网关 + AI-1 gate 后)

- 组包 = T1/T2 检索命中条目(≤K 条)+ T3 脱敏 evidence JSON + 固定任务指令(输出协议同 ai-roadmap);
- 回答失败/超时/关闭 → **降级到规则版模板**,不空窗;答案带引用 id,可"采纳为 FAQ"(写回 kb_entries);
- 采纳闭环:治理动作 accepted → 示例池(ai-roadmap §3.5);问答采纳 → kb_entries 新条目(AI-1 后建表,
  §9 引用);
- 权限:问答只读组合(instances.view + audit.view 的既有权),**不新增权限**;含 digest_text 的问答按
  instances.query 投影。

---

## 8. 块 5(7-2)— 巡检报告自动生成(升级)

### 8.1 现状

AI-0 已交付规则版日报/周报:即时计算(`compose_report(period,…)`),`GET /api/rds/report`(audit.view),
内容 = 创建/销毁/扩容计数 + degraded/failed 清单 + 反复失败任务 + 聚类 Top N。缺口 = 无定时/无归档/
内容未并入新数据源。

### 8.2 设计

- **定时生成**:新增 report ticker(仅 `RDSCTL_REPORT_ENABLED=1` 启动):每天 `RDSCTL_REPORT_HHMM`
  (默认 0005)产日报;周一 00:10 额外产周报;调用扩展版 `compose_report` 的注入版
  (`report_v2(sources)`,period/审计/summary 现成 + 新增源见下),结果写 `reports` 表;
- **报告内容并表**(分阶段,默认全关逐个 env 开):
  | 内容 | 数据源 | env 开关 |
  |---|---|---|
  | 慢查 Top(≤3,文本按 instances.query 投影) | slow_top_view | `RDSCTL_REPORT_INCLUDE_SLOW` |
  | 容量预警(逼近 90% 实例清单) | capacity(§6) | `RDSCTL_REPORT_INCLUDE_CAP` |
  | alerts open 摘要 + 告警群 | alerts + §5.2 归聚 | `RDSCTL_REPORT_INCLUDE_ALERTS` |
  | 异常群 + playbook 命中 | insights cluster(现成) | 默认包含 |
  | 采纳/覆盖率指标(若有) | insights 落库后 | AI-1 后 |
- `reports` 表 + 历史查看:`GET /api/rds/reports?type=daily|weekly&since=&limit=`(audit.view);
  `/api/rds/report` 保留即时入口(period= now 语义不变,零破坏);
- **保留**:reports 90 天清理并入既有 retention ticker(与 slow/query_audit 同 ticker 扩展);
- **UI**:洞察面板增「报告历史」tab(查看/复制/下载),定时产物入口 = 该列表,不新增顶层导航;
- 邮件/IM 推送:不做(出网能力与端点未定,登记开放问题 §14)。

---

## 9. 数据与 Store 扩展汇总

### 9.1 新增表(幂等 DDL,MySQL + Memory 双实现,trait 三处同步)

```sql
-- 容量采样(L1,§6)
CREATE TABLE IF NOT EXISTS capacity_samples (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts BIGINT NOT NULL,
    instance VARCHAR(96) NOT NULL,
    node VARCHAR(96) NOT NULL DEFAULT 'master',
    disk_used_bytes BIGINT NOT NULL DEFAULT 0,
    disk_total_bytes BIGINT NOT NULL DEFAULT 0,
    data_gib DOUBLE NOT NULL DEFAULT 0,
    KEY idx_cap_inst_ts (instance, ts),
    KEY idx_cap_ts (ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 自动报告归档(§8)
CREATE TABLE IF NOT EXISTS reports (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ts BIGINT NOT NULL,
    period VARCHAR(16) NOT NULL,          -- today | week
    type VARCHAR(16) NOT NULL,            -- daily | weekly
    text MEDIUMTEXT NOT NULL,
    counts_json MEDIUMTEXT NOT NULL,
    KEY idx_reports_ts (ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

- `slow_governance` 增列:`advice_json MEDIUMTEXT`(幂等 ALTER,§4.2);
- **引用不重复设计**(就绪时点见 §7.1):`insights` 持久化(ai-roadmap §3.3 DDL 草案,AI-1 事件管道)、
  `kb_entries`(AI-1 后,问答采纳写回)——本文只在接口契约上消费/写回,不另立表;
- 保留策略统一并入既有 retention ticker(热期见各节;cleanup 审计摘要一次,沿用
  `action=retention_cleanup` 扩展参数)。

### 9.2 Store trait 新增方法(草案,全部双实现 + 单测)

| 方法 | 用途 |
|---|---|
| `capacity_insert(rows)/capacity_since(instance, since)/capacity_last(instance, node)` | §6 采样与预测读 |
| `report_insert(...)/reports_list(since, limit, type)` | §8 归档与历史 |
| `gov_set_advice(id, advice_json)` | §4 建议落库 |
| `alerts_open_window(since, limit)`(或复用现有 alerts 列表过滤) | §5.2 群归聚输入 |
| `kb_search(terms)/kb_add(...)` | §7(AI-1 后落地) |

---

## 10. API / 权限 / UI 汇总

### 10.1 新增接口(路由登记于 http.rs,权限登记 perm_for;列表 GET/处置 POST 与既有风格一致)

| 方法/路径 | 权限 | 功能 | 归属 |
|---|---|---|---|
| `GET /api/rds/slow/advice`(列表/规则版) | instances.view(文本按 instances.query 投影) | digest 建议卡数据 | §4.2 |
| `POST /api/rds/slow/advice`(LLM 解释) | instances.query | 实验性 AI 解释(未达 gate 返回 503+提示) | §4.3 |
| `GET /api/rds/alerts/groups` | alerts.view | 告警群归聚视图 | §5.2 |
| `POST /api/rds/alerts/gov`(group_ack) | alerts.handle | 群处置(逐条 ack + 单次审计) | §5.2 |
| `GET /api/rds/timeline?instance=` | instances.view | 事件时间线(alerts+快照+审计) | §5.2 |
| `GET /api/rds/capacity` / `/capacity/forecast` | instances.view | 容量视图与预测 | §6 |
| `GET /api/rds/capacity/cost` | audit.view | 成本估算(单价未配置 503) | §6 |
| `GET /api/rds/ask?q=&instance=` | instances.view(+audit 字段组合) | 值班问答(规则版主路径) | §7 |
| `GET /api/rds/reports` | audit.view | 报告历史 | §8 |
| `POST /api/rds/report/run?period=` | audit.view | 手动触发定时报告(测试/补跑) | §8 |

### 10.2 权限/前端变更汇总(rds.html)

- 新增**权限**:无(全部复用既有 5 组合);如需收紧,后置于 RBAC 里程碑;
- 导航/视图:不加顶层导航;扩展 洞察面板(报告历史 tab + 容量卡组 + 问答入口 + 告警群)、实例详情
  「监控/告警」页签(时间线 + 容量小卡 + digest 建议卡);
- 全部按需加载(不进 2s 轮询),无数据隐藏入口(与 AI-0 面板同规约)。

---

## 11. 分期与依赖(坐标系对齐:本文块 ↔ ai-roadmap / scaling-design)

### 11.1 先做集(纯规则、零 LLM、零 M1 依赖,可独立交付)

| 项 | 涉及 | 依赖 |
|---|---|---|
| 2a 告警群归聚 + 群处置 + 时间线 | insights/alerts/api/rds.html | 无(复用 AI-0 聚类) |
| 1 规则版建议(R1/R2/R4 + GROW_PCT 启用 + gov 增列) | slow/api | 无 |
| 3 采样 + 磁盘外推 + 结构水位 + 僵尸候选 | 新 ticker + capacity_samples + api | 无(仅本机容器,同 slow 采集模式) |
| 5(7-2) 定时 + 归档 + 慢查/容量/alerts 并入 | reports 表 + ticker + compose_report 注入版 | 无 |
| 7-1 规则版问答(T1/T3 + 词表) | 新模块 ask.rs | 无 |

> 建议顺序:① slow_governance 增列 + GROW_PCT → ② 告警群/时间线 → ③ 报告定时与归档 →
> ④ 容量采样外推 → ⑤ 问答规则版;每步独立提交、单测随行、acceptance 回归。

### 11.2 gate 后 LLM 增强集(统一条件:AI-1 事件管道 + 形态 B 网关 + 回放达标)

1 慢查 AI 解释、2b RCA 叙述、7-1 RAG 问答、7-2 报告解读摘要。全部遵守:同构 JSON 契约、失败降级
规则版、采纳回写示例池、入口实验性标注直至 gate 达标。

### 11.3 远期依赖集

| 能力 | 依赖 | 说明 |
|---|---|---|
| 参数调优、负载型容量/自适应异常阈值 | M1 心跳与指标上报 | 本文仅登记方向 |
| 扩容/回收等动作执行 | M1a/M1b 原语(playbook 消费)+ AI-3 操作台 | 建议先落地为 playbook 命中 |
| 跨区容量/容灾解读 | M1.5/M2(topology_links) | 对齐 AI-2 |
| 成本报告 LLM 解读 | AI-4 | 指标化后 |

---

## 12. 验收与测试要点

单元(规则版纯函数,memory 后端):
1. 归一/归聚:注入"同代理不可达 30 台"告警群 → 1 群 + 批量建议;逐台独立异常不误并;
2. 建议规则:R2 缺失索引候选命中/不命中;GROW_PCT 上升命中入队且不重复审计;
3. 外推:合成样本 ≥7 天 → days_to_90pct 合理;样本不足 → insufficient;`data_size` 字符串解析边界;
4. compose_report 注入版:各 include env 开关下文本含/不含对应段;
5. 问答:意图解析三分类 + 模板回答含引用;脱敏(注入口令文本断言输出为空);
6. Store 双后端:新表/方法语义一致。

协议/回归:
7. 零配置回归:全部新 env 默认关时零新表写入、P0 三项 + `cargo test` 全绿、memory 全绿;
8. 权限:无 instances.query 者 digest 文本占位/建议列名隐藏;群处置需 alerts.handle;
9. 脱敏红线:全包 grep `root_password` 为空;acceptance 垫片扩展(固定 df/EXPLAIN 输出分支);
10. UI:按需加载、无数据隐藏、权限不足隐藏入口。

## 13. 风险与红线

1. 幻觉/误建议 → LLM 不产动作、建议全部带 evidence_ref、规则版为主路径(gate);
2. 数据合规 → 组包脱敏、digest 权限投影、默认关;形态 A/B 密钥不出主进程(ai-roadmap §3.4);
3. 写放大 → 采样周期 ≥300s、快照/建议重复命中不重写、保留清理随表上线;
4. 热路径污染 → 全部新增为独立 ticker/按需计算;采集失败静默不阻塞;
5. 版本兼容 → 幂等 DDL + 增列(不改既有列语义)、/api/rds/report 与 alert 既有动作零破坏;
6. 效果不确定 → 规则版先行 + 回放评测 + 实验性标注,与 ai-roadmap gate 一致。

## 14. 开放问题(评审要点,不阻塞)

1. 慢查"顶层告警"(alerts 复合去重键)是否值得为爆发场景引入 —— 现决策:治理队列 + 群视图角标;
2. 报告/容量推送渠道(邮件/IM)端点与鉴权 —— 现决策:不入首版;
3. 容量单价表来源与口径(实例级 vs 规格级;跨 region 汇率) —— 现决策:env 单价、默认 off;
4. kb_entries 语料审核流(谁可把问答采纳为 FAQ;租户可见域) —— 依赖 RBAC 里程碑;
5. 容量预测算法参数(斜率质量阈值、指数年化窗口)以回放真实增长数据校准后再固定。

## 15. 实施偏差登记(编码落地后,2026-09-05)

> 范围:本文「先做集」五块已按仓库惯例落地(锚点:src/store.rs、src/slow.rs、src/insights.rs、
> src/capacity.rs、src/report.rs、src/ask.rs、src/api.rs、src/http.rs、src/main.rs、src/rds.html)。
> 单测 93 全绿(`cargo test --bin rdsctl`,内存后端);P0 验收三项未在本机重跑(需真实 MySQL +
> docker 垫片,按仓库验收流程另行执行);前端无浏览器环境未做运行时验证(静态词法自检通过)。

| 设计原稿 | 落地现状 |
|---|---|
| 块1 §4.2 R2「与 schema_index 现有索引对比」 | 规则版以 digest_text 谓词列候选做**提示**(`candidate_columns`,不做库级对比);按实例/表核验与 LLM 解释版同归 AI-1 gate 后(§4.3 未实现,登记延后) |
| 块1 §4.2 GROW_PCT「登记未启用」 | 已启用,`RDSCTL_SLOW_GROW_PCT` 默认 200(0=关),沿用 avg/max 规则默认即开的既有口径;`slow_governance` 增 `advice_json` 列(幂等迁移;**可空 MEDIUMTEXT NULL**,兼容省略该列的旧式 INSERT,视图层空值→null) |
| 块1 接口 | 仅 `GET /api/rds/slow/advice`(规则版,text 按权限投影);LLM `POST /api/rds/slow/advice` 未实现(AI-1 gate) |
| 块2 §5.2 群 key | `kind::归一化消息`;群处置 `POST /api/rds/alerts/gov` 逐条走既有 `alert_action` + 单次审计 `alert_group_action`;时间线截断 200 |
| 块3 §6.1 采样 | `RDSCTL_CAP_SECS` 默认 **0=关闭**(守「默认关闭零行为变化」红线;文档草案 300s 改为需显式开启);仅采样 running 实例 master 节点(`df -P /var/lib/mysql`);`data_gib` 解析自实例 `data_size`;未实现成本口径与 `capacity/cost` 接口(依赖单价表,延后) |
| 块4(7-1) §7 | 规则版 `src/ask.rs`(classify/faq_search/answer)落地;`kb_entries`/RAG/采纳回写依赖 AI-1,登记延后 |
| 块5(7-2) §8 | 定时器 UTC HHMM(默认 0005)日报 + 周一附加周报;`RDSCTL_REPORT_ENABLED=0` 默认关;慢查段仅 digest 前缀+统计,**不输出 SQL 文本**(自动化产物无用户上下文,保守处理);手动补跑 `POST /api/rds/report/run` |
| 权限 | 未新增权限(全部复用既有 5 组);新接口登记见 §10 表(ask/capacity/reports/timeline 已登记) |
| 前端 | rds.html(现 13062 行):洞察视图新增 AI 卡(告警群/报告历史+手动生成/容量预警/值班问答)、实例详情告警页签事件时间线、实例慢查行内「建议」面板;全部懒加载不轮询 |
| 其它 | `#![recursion_limit="512"]`(多层嵌套 json! 基线);store 新增表 `capacity_samples`/`reports` 与 `slow_governance.advice_json` 随 DDL+迁移幂等;`ReportRow` 结构未落地(报告以字符串直接落库,结构体删除) |
| 对接冒烟(2026-09-05) | 内存后端 + demo 实跑:8 个新接口全部连通且响应字段与前端解析一致;修复 2 处前端/后端口径不一致:① 慢查建议面板取 `advice` 响应 `items[0]`(原误把整包当单条);② `POST /api/rds/report/run` 兼容 `period=today|week` 与 `daily|weekly` 双口径 |

> LLM 增强与依赖 M1/M2 的远期项(§4.3/§5.3/§6.3 负载容量/§7.3 RAG/§8 推送渠道/成本)均按
> §11 标注延后,未实现——入口在 UI 标注实验性或直接不出现,行为零变化。
