# AI-0 确定性洞察基座 — 验收记录(docs/ai0-acceptance.md)

> 日期:2026-09-03。对应 `docs/ai0-impl-checklist.md`(S1–S9)与 `docs/ai-roadmap.md` v2 AI-0。
> 环境:本机 Docker + 控制面 MySQL(127.0.0.1:3306,rdsctl_acc_* 独立库逐用例);
> 单元测试走内存后端(offline,`CARGO_HOME=.cargo-home`)。

## 1. 交付范围与代码改动

| 步骤 | 落地 | 位置 |
|---|---|---|
| S1 | `evidence_snapshots` 表 DDL(IF NOT EXISTS 幂等);trait 新增 `evidence_insert/latest/since` + `audit_since`;Store pass-through;**MySQL/Memory 双后端实现** + 单测 | src/store.rs |
| S2 | `docker::container_state`(Status\|ExitCode\|RestartCount);复制/代理硬事实 SQL 常量(连接/应用线程 LAST_ERROR,CONCAT_WS 单值化) | src/docker.rs;src/instance.rs |
| S3 | `capture_degrade_evidence`(容器状态/复制硬事实/GTID 存档/日志尾,总量封顶 + 口令脱敏);sweep degrade 分支按「状态跃迁或原因变化」写快照(防 30s 刷写) | src/instance.rs |
| S4 | 新模块 `src/insights.rs`:原因归一化 `normalize`/类别 `label_of`、playbook 目录 v0(仅建议)、`cluster_anomalies`、`snapshot_summary`、`compose_report`/`period_since`;6 项单测 | src/insights.rs |
| S5 | `GET /api/rds/insights`、`GET /api/rds/report`;http 路由 + 权限登记(insights=instances.view,report=audit.view) | src/api.rs,src/http.rs |
| S6 | 前端「洞察」导航视图(懒加载不轮询):聚类卡(成员/建议 pill/快照摘要行)+ 生成日报/周报 | src/rds.html |
| S7 | `scripts/baseline.sh`(只读审计离线算 MTTR/处置步骤/open 窗口;`--csv` 对比模式) | scripts/baseline.sh |
| S8 | acceptance `sweeper_detects_and_recovers` 扩展:快照落库直查、快照去重(2 条稳定)、insights 聚类+成员摘要、report 文本冒烟 | tests/acceptance.rs |

缺失原语(start_replica / restart_proxy / retry_task)已登记 `scaling-design.md` §12 M1 增补
(M1a/M1b),AI-0 仅建议不执行(清单 §3.2 exec_map 闭环)。

## 2. 测试结果

- 单元(`cargo test --bin rdsctl`):**24 passed,0 failed**——含
  store(evidence/audit_since 双后端语义)、instance(ai0_evidence 脱敏/去重规则)、insights 6 项、
  dag、m0 合成基准、sha256 等回归。
- 验收(`cargo test --test acceptance`,真实 MySQL + docker/mysql 垫片,独立库):
  **3 passed**——kill9_restart / concurrent_ops / sweeper_detects_and_recovers(AI-0 断言内嵌)。

## 3. checklist §8 DoD 逐条核验

| DoD | 结果 | 证据 |
|---|---|---|
| 1. 注入同因多实例 → 聚类归群并带批量处置建议 | ✅ | insights::tests::cluster_groups_same_cause_across_instances(3 台代理缺失=1 群,建议 restart_proxy) |
| 2. degraded 实例有 evidence 快照且含失败细节 | ✅ | acceptance:MySQL 直查 `evidence_snapshots` ≥1 条 + facts 含 `slave-1 present:false`;快照含 LAST_ERROR/exit code 探针 |
| 3. degrade 语义/审计不变,P0 回归绿,无 AI 配置零变化 | ✅ | acceptance 三项全绿;快照为旁路写,set_status/audit/alert 代码路径未改 |
| 4. baseline 前后留档;缺失原语已登记 M1 | ✅ | baseline 脚本运行留档 `logs/baseline-mttr-*.csv`;scaling-design §12 M1a/M1b/M1c |
| 5. UI 按需加载不轮询,无数据隐藏 | ✅ | 洞察视图仅 nav 点击触发 loadInsights;权限不足隐藏 nav;空态文案 |

## 4. 手工基线(变更前实测,2026-09-03)

运行 `./scripts/baseline.sh`:
- 已闭环降级窗口:0(控制库现存审计以 degrade 结尾为主,尚无 degrade→recover/destroy 配对);
- open(未闭环)降级窗口实例数:2(需人工排查);
- 产出 `logs/baseline-mttr-20260903-110005.csv`。

> 结论:当前库历史不足以给出 MTTR 基线;**上线 AI-0 后持续积累**,下次对比按同一脚本
> 重新执行并记录(作为 AI-1 gate 的采纳率/MTTR 分母来源之一)。

## 5. 对清单的偏差与说明

1. **audit 时间过滤**:未改 `audit_list` 签名,新增独立 `audit_since(since, limit)`(改动面更小,
   语义等价,报表/统计足够);
2. **insights 结果不落库**:AI-0 按需计算(契约先行,worker 形态与 insights 持久化留 AI-1 事件管道);
3. **快照摘要字段**:API 只透出摘要(容器状态/复制串/≤3 条日志截断),facts 全量仅在 DB,
   符合"脱敏最小化"红线(全包 grep 无 `root_password`);
4. 巡检扫描对象/告警(S-告警)等新近代码未受影响(快照独立旁路)。

## 6. AI-1 入口(下一步数据/接口就绪)

- 事件驱动巡检可直接携带 evidence 快照契约(M1c 已登记);
- playbook v0 已可给"同因实例群"出建议;动作执行(dry-run + 提交 scheduler)待 M1a/M1b 原语 + 操作台;
- gate:回放 degrade 案例集可复用 insights 聚类与 evidence_snapshots 数据。
