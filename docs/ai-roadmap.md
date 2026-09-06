# rdsctl AI 提能路线图 v2(ai-roadmap)

> 问题:基于现有项目能力,AI 能否在运维排查与日常操作上提高效率?
> 结论:**可以,但更好的方案不是"LLM 驱动的助手",而是"证据与 playbook 驱动的自动化,
> LLM 只做可插拔的解释增强"。** 本文为 v2 主版本,取代 2026-09-02 初版(变更说明见 §8)。
> 日期:2026-09-02。锚点核对:src/instance.rs、src/dag.rs、src/store.rs、src/api.rs、
> src/docker.rs、src/http.rs、scripts/*.sh、docs/scaling-design.md(M0–M3)。

---

## 1. 结论摘要

rdsctl 的管控动作已全部 DAG/Step 化并幂等(`dag.rs` Step/TaskNode)、状态机 + 操作锁 +
lease 在 Rust(`instance.rs`)、全量审计(`store.rs` audit_log)、证据数据已结构化。AI 的
定位是**解释层与决策辅助层**,负责 ① 排障根因解释(RCA)、② 异常事件归纳聚类、③ 受控
动作建议与参数化编排、④ 例行报告与 runbook 沉淀。硬性前提:

- **AI 不进热路径**:30s 巡检与执行引擎保持确定性;AI 只做异步/按需解释层。
- **敏感数据不出内网**:`root_password` 与主机信息一律剔除;统一私有 LLM 端点 + 服务端
  白名单组包;最优形态下密钥/网络都不进 rdsctl 进程(见 §3-4)。
- **AI 不直接持有执行权**:所有动作来自 **playbook 实例化**(确定性匹配),经既有状态机/
  操作锁 + 人工确认 + dry-run + 审计(`user=ai`),LLM 不参与动作枚举(见 §3-2)。

### 1.1 v2 的核心判断(六个设计决定)

| # | 决定 | 相对初版 |
|---|---|---|
| D1 | **证据层做厚 + 异常快照化**:质量瓶颈在证据不在模型;探针从"布尔"升级为"带回原因的硬事实",degrade 瞬间的快照落库供事后分析 | 初版"按需重查证据" |
| D2 | **动作面 = playbook 实例化**:证据规则匹配修复目录,LLM 只做解释/排序/填参 | 初版"LLM 生成 + 白名单校验" |
| D3 | **共享证据契约 + insights 管道**:契约先行(RCA/聚类/报告共用),insights 落库可回溯可反馈 | 初版"每功能一个 REST 接口" |
| D4 | **先"群与模式"后"单实例 RCA"**:规则版聚类/异常群报告先行,LLM RCA 后置为增强层 | 初版"单实例 LLM RCA 最前" |
| D5 | **LLM 形态三态演进**:证据收集与 LLM 调用解耦(helper → 网关),lab 版离线零依赖 | 初版"内嵌 curl" |
| D6 | **AI-3 重心 = 批量/模板 + dry-run 合规**,NL 只是输入通道之一(与表单/CLI 平权) | 初版"NL 操作台为重心" |

**明确不上 AI**(保持确定性规则):容器存在性、复制线程 ON/OFF、代理连通等基础探针;
生命周期状态机与操作互斥判定;lease/锁语义。LLM 只做其上层的解释与建议。

---

## 2. 能力矩阵(现状摩擦 → AI 杠杆 → 归属阶段)

| # | 运维场景 | 现状摩擦(代码锚点) | AI 杠杆 | 归属 |
|---|---|---|---|---|
| 1 | 批量异常无聚类 | `instance.rs` summary 只给 top-5 reasons;同主机故障致几十台一起 degrade 无区分 | **规则版聚类**(关键词/同容器前缀/同网络):区分"单点牵连 vs 逐台独立",给批量处置模板 | AI-0 |
| 2 | 降级原因只一句话 | sweep_once 产出"容器缺失/复制中断"短句,无失败细节 | **探针升级 + 异常快照**:exit code / 容器日志尾 / `replication_connection_status` LAST_ERROR / GTID 差距随 degrade 落库 | AI-0 |
| 3 | 例行报告人工汇总 | 翻状态页/审计人工写 | 规则版日报/周报(创建量/降级清单/反复失败任务)→ insights 消费 | AI-0/AI-2 |
| 4 | 单实例排障靠人拼证据 | last_error + 审计 + 任务 output + 日志分散,人肉拼 | 硬事实报告(规则)先行,LLM 增强:中文根因排序 + 每条带 evidence | AI-1 |
| 5 | 失败任务解读 | DAG 节点 output 是裸报错(`dag.rs` NodeResult.output) | 规则分类(超时/连接/权限/磁盘类)→ playbook 建议;LLM 解释增强 | AI-1 |
| 6 | 巡检事件跟进 | 30s 顺序扫(设计 §8 将改事件驱动) | anomaly 事件 → 洞察 worker 异步产 insights(不进热路径) | AI-1 |
| 7 | 处置靠手工 | 只能 UI 点(销毁重建等),无"建议-执行"通道 | **playbook 操作台**:建议=playbook 实例化 → dry-run → 确认 → 走既有 scheduler,全审计 | AI-1/AI-3 |
| 8 | 跨区/拓扑诊断 | M2 才有 topology_links/lag(设计 §9) | 链路 lag/抖动分析、跨区容灾视图解读 | AI-2 |
| 9 | 批量/定时编排 | 设计 §7.2 组合器/批量为 M1+ 能力 | 模板/批量/定时 + dry-run 合规;NL 作为填充通道之一 | AI-3 |
| 10 | 经验沉淀 | 审计含完整处置序列但无归纳 | playbook 目录即 runbook 资产;采纳案例入示例池;FAQ 对答 | AI-3 |
| 11 | 十万级运营/演练 | M3 压测(设计 §12) | 演练报告解读;MTTR/采纳率闭环度量 | AI-4 |

---

## 3. v2 架构决定(落地为代码/数据对象)

### 3.1 探针升级 + 异常快照(D1)

- 巡检探针从布尔升级为"带回原因的硬事实"(M1 事件驱动巡检改版时一并落地):
  - 容器:存在性之外采集 状态/exit code/日志尾(扩 `docker.rs logs_tail` 行数参数);
  - 复制:现有 `replication_connection_status`/`applier_status` SERVICE_STATE 之外,采集
    `LAST_ERROR_NUMBER/LAST_ERROR_MESSAGE`、`@@GLOBAL.gtid_executed` 与从库
    `gtid_retrieved`/`gtid_executed` 差距(`replica_problem` 同步升级);
  - 代理:proxy 日志尾 + mng 端口查询结果。
- **快照落库**:degrade 判定瞬间将上述事实连同现 last_error 一并写
  `evidence_snapshots(ts, instance, kind, facts_json)`(store.rs 版本化迁移;保留策略与
  审计归档对齐,热 30 天)。事后分析读快照,零现场打扰、可回溯——30s 巡检窗口内现场
  可能已变化,快照才是根因现场。
- 巡检本身保持低成本(探测 ≤10ms 量级,快照仅在状态跃迁时写)。

### 3.2 动作面 = playbook 实例化(D2)

- **playbook 目录**(代码内强类型注册表,与 `dag::Step` 同语言):
  `{id, trigger(证据特征/规则条件), steps(Vec<Step> 或 NodeGroup 引用), risk, param_schema,
   state_gate(如仅 running/degraded), 幂等键}`;
  首批:`start_replica`(复制线程停)、`restart_proxy`、`retry_task`(续跑语义)、`destroy_residual`
  (failed 清理)、`scaleout` 等——参数校验复用 `api.rs`/`instance.rs` 同一套状态门槛与枚举。
- **AI-1 起,任何"建议动作"= playbook 命中后的实例化**,字段 `playbook_id+params+evidence_ref`;
  白名单从"事后校验 LLM 输出"变成**天然成立**;dry-run 天然可做;风险分级天然带出。
- LLM(若启用)只允许:解释硬事实、对命中 playbook 排序、按 param_schema 填参——不生成动作枚举。
- 副产品:playbook 目录即 runbook 资产(新人自服务 + 覆盖率可统计,见 §4 AI-1)。

### 3.3 共享证据契约 + insights 管道(D3)

- **evidence contract**(先于一切功能定义,供 AI-0 接口与 M1 事件管道共用):
  `instance_facts(状态/last_error/region·az·tenant) + audit_window(近 N 条 action/params
  截断 240) + task_outputs + evidence_snapshot + playbook_hits`。
- **insights 落库**(worker/接口产出统一写):`insights(ts, scope[instance|group|global], kind
  [rca|cluster|report|suggestion], evidence_ref, model[null=规则], body_脱敏, actions[],
  accepted_at, outcome)`——结果可回溯、可反馈。
- 消费端:前端"诊断/洞察"面板、日报周报、操作台、度量。
- AI-0 建在共享契约上(**契约先行,worker 不先行**):lab 规模按需接口即可;M1 事件驱动
  巡检就绪后,后台 worker 消费同一契约自动产 insights,零返工。

### 3.4 LLM 形态三态演进(D5)

| 形态 | 时机 | 形态 | 特性 |
|---|---|---|---|
| A | AI-0(lab,立即) | rdsctl 只渲染"证据 JSON + 任务指令"到管道/文件,外部 `scripts/ai-analyze.sh` 或本地小服务消费 | 主进程零网络/零密钥/零新依赖;离线可跑规则版;模型可随时换 |
| B | M1.5 多副本后 | 独立私有 AI 网关(litellm/one-api 类) | 密钥/限流/成本/多模型在网关;rdsctl 只做结构化请求/解析 |
| C | 远期评估 | 证据/playbook 体系稳定后,再评估轻量客户端内嵌 | 仅在确有必要时引入 HTTP client crate |

- 输出协议统一(与形态无关):规则版与 LLM 都产同构 JSON
  `{summary, hard_facts[], likely_causes[{cause,evidence,confidence}], actions[{playbook_id,
  params,risk,reason}]}`;解析失败 → 原文兜底,不阻断。
- env 配置:`RDSCTL_AI_ENABLED`(默认 0)/`RDSCTL_AI_ENDPOINT`/`RDSCTL_AI_MODEL`/`RDSCTL_AI_KEY`,
  经 `scripts/lib.sh` env 约定与 `rdsctl.env.example` 同步。

### 3.5 采纳反馈闭环与度量(先测手工基线)

- **AI-0 先行**:在现有环境量 当前 MTTR / 单次处置步骤数 / 日报耗时,作为提效基线(否则
  "AI 提效"无法被证明)。
- 闭环:insights.actions 的 `accepted/declined` + 处置后 `recover/fail` 配对落库;lab 数据量
  不支持微调,闭环产物为 ① few-shot 示例池(采纳案例进 prompt 示例)② **playbook 覆盖率
  统计**(哪些异常场景无命中 → 补 playbook)③ MTTR/采纳率指标进运营页。

---

## 4. 分期路线(与 scaling-design M0–M3 对齐;AI-0/1 需求契约前置进 M1)

### AI-0 确定性洞察基座(前置:仅现有单机能力;立即可做,无 LLM 依赖)

> 实施清单(DDL/trait 扩展/探针 SQL/模块与接口/UI/测试/顺序/DoD):见 `docs/ai0-impl-checklist.md`。

- **范围**:探针升级 + 异常快照(§3.1,先于事件驱动巡检落地为 sweep_once 增强);evidence
  contract 定义(§3.3);**规则版** 聚类(同因实例群)与异常群报告;日报/周报模板;playbook
  目录 v0(start_replica/restart_proxy/retry_task/destroy_residual);手工基线度量(§3.5)。
- **接口**:`GET /api/rds/insights?scope=...`(读 insights;AI-0 阶段由按需分析函数写);
  `rds.html` 增"洞察"面板(degraded/failed 卡片入口 + 异常群视图,懒加载,不加轮询)。
- **验收**:① 聚类在注入"同主机多实例 degrade"案例时正确归群并给出批量处置模板;② degrade
  记录带 evidence_snapshots 且含失败细节(LAST_ERROR 等),非仅布尔结论;③ 日报/周报可生成;
  ④ 基线 MTTR/处置步骤数已记录;⑤ `cargo test` 全绿、无 AI 配置时行为零变化。估量:8–12 人日。

### AI-1 事件洞察管道 + playbook 操作台(前置:M1 队列/事件驱动巡检/审计分表)

- **范围**:anomaly 事件 → 洞察 worker(消费 evidence contract)→ insights 落库;单实例
  **硬事实报告**(规则)+ **LLM 增强诊断**(解释/排序/填参,形态 A/B);失败任务分类→playbook
  建议;playbook 操作台(建议 → dry-run → 确认 → 提交既有 scheduler,审计 `user=ai`);
  采纳反馈记录(§3.5)。
- **前置到 M1 的规范**:M1 事件驱动巡检与队列设计时,把 anomaly 事件与 evidence_snapshots
  作为一等数据对象(§3.1/§3.3 契约),使 AI 管道在 M1 后零改造接入——该部分成本计入 M1。
- **gate(进入 LLM 增强的前提)**:回放历史 degrade/失败案例:规则版(无 LLM)准确率达标即可
  上线;**LLM 增强须在回放集上不劣于规则版且采纳率 ≥ 阈值**才开默认路径,否则入口标注
  实验性。同一 JSON 契约使 LLM 可后置替换。
- **验收**:异常群/单实例诊断在真实案例上无敏感字段泄漏;白名单动作全部走 playbook 注册表
  且校验与手工一致;采纳/结果配对落库;P0 验收三项回归全绿。估量:净增 6–10 人日(M1 主体
  另计)。

### AI-2 跨区与全局分析(前置:M1.5 跨区控制面/Global Brain、M2 topology_links/lag)

- **范围**:跨区复制链路诊断(读 topology_links + lag/抖动);多 region 聚合;周报升级(基于
  Global Brain 只读目录/审计 + insights)。估量:6–8 人日。
- **验收**:region A 故障演练时,对 A 区实例群输出"共同根因"结论;周报人工核对无事实错误。

### AI-3 参数化编排 + runbook 沉淀(前置:M2 组合器/dry-run/合规成熟,设计 §7.2)

- **范围**(工程重心在确定性编排,非 NL):
  - playbook 与 NodeGroup 组合器合流:建议/模板/批量/定时任务 = 组合器 dry-run → 合规检查
    → 确认 → 队列提交(并发配额、批次取消、任务依赖语义不变);
  - **输入通道平权**:表单 / CLI / NL 都只是模板填充入口(NL 经 LLM 形态 B 解析→参数校验,
    不高于其它通道权限);runbook/FAQ 沉淀:playbook 目录 + 采纳案例示例池 + "为什么 xx
    degraded"对答(证据契约 → insights 检索)。
- **验收**:口径化 20 条历史指令,任意输入通道 dry-run 结果与人工操作一致率 ≥ gate 阈值;
  playbook 覆盖率纳入周报;破坏性动作双确认。估量:净增 6–10 人日(组合器/批量主体在 M2)。

### AI-4 硬化与度量(M3 压测阶段并行)

- **范围**:洞察/AI 服务分级(SLO/限流/缓存;LLM 形态 B 网关内);故障注入(LLM 不可用/超时/
  幻觉建议注入)验证降级路径;演练报告解读;MTTR/采纳率/playbook 覆盖率指标化(数据源
  insights + 审计)。估量:5–8 人日。
- **验收**:故障注入下无任何误执行(playbook 注册表 + 状态机 + 确认兜底);指标进运营页。

---

## 5. 风险与护栏(全部落地为代码/流程约束)

1. **幻觉 / 误建议** → LLM 不产动作(§3.2 playbook 实例化);dry-run + 状态机/锁 + 高风险
   双确认;每条建议强制 evidence_ref。
2. **数据合规** → 统一私有端点;服务端白名单 + 截断脱敏;prompt/日志禁 root_password;
   insights/ai 请求只存脱敏摘要;形态 A 下密钥根本不进主进程。
3. **效果不确定(内网模型质量)** → AI-1 gate 回放评测;规则版同契约先上线,LLM 可后置替换。
4. **成本 / 延迟** → 仅按需/事件触发(不做全局周期扫描式 AI);结果截断;限流;失败快速降级。
5. **热路径污染** → AI 一律异步/按需;巡检只发事件与写快照,执行引擎保持确定性;LLM 不可用
   只降级不阻塞。
6. **版本兼容** → 新表(evidence_snapshots/insights 等)走 store.rs 版本化迁移;AI 默认 env
   关闭,单机 lab 版与 P0 验收零影响。

---

## 6. 工作量与依赖总览(人日,粗略,团队校准;含"前置进 M1/M2"的说明)

| 阶段 | 前置架构 | 估量 | 收益点 |
|---|---|---|---|
| AI-0 | 无(现有单机) | 8–12 | 探针/快照/契约/规则聚类/报告/playbook v0/基线——全部确定性资产 |
| AI-1 | M1(契约前置入 M1) | 净增 6–10 | 事件洞察管道 + playbook 操作台 + LLM 增强(gate 后) |
| AI-2 | M1.5 / M2 | 6–8 | 跨区诊断与周报 |
| AI-3 | M2(组合器主体在 M2) | 净增 6–10 | 批量/模板/定时 + dry-run 合规 + 知识沉淀 |
| AI-4 | M3 | 5–8 | 可靠性 / 度量闭环 |

依赖:AI-0 → AI-1 串行;AI-2 依赖跨区里程碑、与 AI-1 可并行;AI-3 依赖 M2 组合器;
AI-4 依赖 AI-1 采纳闭环与 M3 故障注入。

---

## 7. 假设与开放问题

- **假设**:存在可用私有/本地 OpenAI 兼容 LLM(形态 B 网关或形态 A helper);若无可信模型,
  AI-0/AI-1 规则版照常交付(契约不变),LLM 增强延后。
- **开放问题**:
  1. AI-1 gate 阈值(采纳率/准确率)具体取值——以 AI-0 回放集实测校准;
  2. insights/evidence_snapshots 保留策略与审计归档(设计 §13)精确对齐;
  3. playbook 参数与采纳动作的租户级权限表达(M0 tenant 占位;AI-3 前定谁可见/谁可采纳);
  4. 形态 C(内嵌客户端)是否值得引入 HTTP client crate;
  5. M1/M2 主体中"契约前置"的成本与排期如何并入 scaling-design 里程碑。

---

## 8. v1 → v2 变更说明(评审对照)

| 维度 | v1(初版) | v2(本文) |
|---|---|---|
| 范式 | LLM 驱动助手 | 证据/playbook 驱动自动化,LLM 可插拔解释增强 |
| 证据 | 按需重查(可能错过现场) | 探针升级 + degrade 快照落库(D1) |
| 动作 | LLM 生成 kind + 白名单校验 | playbook 规则实例化,LLM 不产动作(D2) |
| 架构 | 每功能一个 REST + prompt 烟囱 | 共享 evidence 契约 + insights 管道,契约先行(D3) |
| 顺序 | 单实例 LLM RCA 最前 | 规则版聚类/报告先行,LLM RCA 后置增强(D4) |
| LLM 接入 | 内嵌 curl | helper → 私有网关 三态演进,密钥/网络出主进程(D5) |
| AI-3 | NL 操作台为重心 | 批量/模板 + dry-run 合规为重心,NL 平权为通道(D6) |
| 度量 | 回放评测门槛 | + 手工基线先测 + 采纳/结果闭环 + playbook 覆盖率 |
| 成本 | 各阶段独立估量 | 契约/事件/组合器规范前置进 M1/M2,净增量单独标注 |

红线不变(§5):AI 任何路径不得绕过状态机/锁/审计;无 AI 配置时行为零变化;敏感字段不出
内网;AI 不进巡检与执行热路径。
