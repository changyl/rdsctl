//! AI-0 确定性洞察(规则版,无 LLM)——见 docs/ai0-impl-checklist.md §3 / ai-roadmap.md v2。
//!
//! 职责:异常原因归一化与聚类(同因实例群)、playbook 目录 v0(仅建议不执行)、
//! 异常快照摘要、日报/周报文本。LLM 增强层(AI-1)另立 `src/llm.rs`,本模块为其宿主;
//! 输出 JSON 契约与 LLM 版一致(可后置替换)。

use crate::instance::{InstStatus, RdsInstance};
use serde_json::{json, Value};

// ─── 时段 ───

/// 时段起点(AI-0 以 UTC 日界;本地化口径按部署对齐,后续可配)
pub fn period_since(period: &str, now: u64) -> Option<u64> {
    match period {
        "today" => Some(now - now % 86400),
        "week" => Some(now.saturating_sub(7 * 86400)),
        _ => None,
    }
}

// ─── 异常原因归一化 ───

fn is_num_token(t: &str) -> bool {
    let digits: String = t.chars().filter(|c| !c.is_ascii_punctuation()).collect();
    !digits.is_empty() && digits.chars().all(|c| c.is_ascii_digit())
}

/// 单 token 归一:实例容器名 → 角色类;数字/端口 → N;主机地址 → HOST
fn norm_token(t: &str) -> String {
    if is_num_token(t) {
        return "N".to_string();
    }
    let low = t.to_lowercase();
    if low.starts_with("rds-") {
        if low.contains("-master") {
            "MASTER".to_string()
        } else if low.contains("-slave-") {
            "SLAVE".to_string()
        } else if low.contains("-proxy") {
            "PROXY".to_string()
        } else {
            "CONTAINER".to_string()
        }
    } else if low.contains("127.0.0.1") || low.contains("localhost") {
        "HOST".to_string()
    } else {
        low
    }
}

/// 归一化原因:相同语义(不同实例名/端口/编号)得到相同 pattern
pub fn normalize(msg: &str) -> String {
    msg.split_whitespace()
        .map(norm_token)
        .collect::<Vec<String>>()
        .join(" ")
}

/// 人工可读的异常类别(聚类 label/报表展示)
pub fn label_of(msg: &str) -> String {
    if msg.contains("复制中断") {
        "复制中断".to_string()
    } else if msg.contains("复制状态查询失败") {
        "复制状态查询失败".to_string()
    } else if msg.contains("代理不可达") {
        "代理不可达".to_string()
    } else if msg.contains("代理容器") && msg.contains("缺失") {
        "代理容器缺失".to_string()
    } else if msg.contains("容器缺失") {
        "节点容器缺失".to_string()
    } else if msg.contains("不可达") {
        "从库不可达".to_string()
    } else if msg.contains("任务 ") && msg.contains("失败") {
        "任务失败".to_string()
    } else {
        "其他".to_string()
    }
}

// ─── playbook 目录 v0(仅建议;执行接入 AI-1 操作台) ───

pub struct Playbook {
    pub id: &'static str,
    pub name: &'static str,
    pub risk: &'static str, // low | medium | high
    pub desc: &'static str,
}

/// 注册表(强类型;AI-1 与组合器合流后扩展 execution 语义)
pub fn playbook_registry() -> Vec<Playbook> {
    vec![
        Playbook {
            id: "start_replica",
            name: "恢复从库复制",
            risk: "low",
            desc: "复制中断/线程停止 → START REPLICA + 追平校验(原语已登记 M1/M1b)",
        },
        Playbook {
            id: "restart_proxy",
            name: "重启代理",
            risk: "medium",
            desc: "代理容器缺失/不可达 → 重跑代理(原语已登记 M1/M1b)",
        },
        Playbook {
            id: "retry_task",
            name: "重跑失败任务",
            risk: "low",
            desc: "任务失败 → 单任务重跑/续跑(接口已登记 M1/M1a)",
        },
        Playbook {
            id: "destroy_residual",
            name: "清理残留并重建",
            risk: "high",
            desc: "failed/降级残留 → 走既有 destroy 清理后重建(现成 API)",
        },
    ]
}

/// 由证据特征命中 playbook(仅建议;AI-0 不执行)
pub fn playbook_hits(msg: &str) -> Vec<Value> {
    let class = label_of(msg);
    let mut hits = Vec::new();
    for p in playbook_registry() {
        let matched = match p.id {
            "start_replica" => class == "复制中断" || class == "复制状态查询失败",
            "restart_proxy" => class == "代理容器缺失" || class == "代理不可达",
            "retry_task" => class == "任务失败",
            "destroy_residual" => {
                msg.contains("残留") || msg.contains("销毁") || msg.contains("重建")
            }
            _ => false,
        };
        if matched {
            hits.push(json!({ "id": p.id, "name": p.name, "risk": p.risk, "reason": p.desc }));
        }
    }
    hits
}

// ─── 聚类(同因实例群;区分单点牵连 vs 逐台独立的第一步) ───

/// 对全部实例做异常聚类:仅统计 degraded/failed;按归一化原因分组、数量降序。
/// members 附最近一次快照摘要(evidence_snapshots 由调用方注入 → `snapshot_summary`)。
pub fn cluster_anomalies(insts: &[RdsInstance]) -> Vec<Value> {
    use std::collections::HashMap;
    let anomalies: Vec<&RdsInstance> = insts
        .iter()
        .filter(|i| matches!(i.status, InstStatus::Degraded | InstStatus::Failed))
        .collect();
    let mut groups: HashMap<String, Vec<&RdsInstance>> = HashMap::new();
    for inst in anomalies {
        groups
            .entry(normalize(&inst.last_error))
            .or_default()
            .push(inst);
    }
    let mut rows: Vec<(String, Vec<&RdsInstance>)> = groups.into_iter().collect();
    rows.sort_by(|a, b| b.1.len().cmp(&a.1.len()));
    rows.into_iter()
        .map(|(pattern, members)| {
            let label = label_of(&members[0].last_error);
            let member_rows: Vec<Value> = members
                .iter()
                .map(|i| {
                    json!({
                        "name": i.name,
                        "status": i.status,
                        "status_label": i.status.label(),
                        "region": i.region,
                        "az": i.az,
                        "tenant": i.tenant,
                        "last_error": i.last_error,
                    })
                })
                .collect();
            json!({
                "pattern": pattern,
                "label": label,
                "count": members.len(),
                "members": member_rows,
                "suggested_playbooks": playbook_hits(&members[0].last_error),
            })
        })
        .collect()
}

// ─── 快照摘要(insights 接口只带脱敏摘要,不整包透传 facts) ───

/// 从 evidence facts 提取前端/报表用摘要(截断日志、剥离冗长字段)
pub fn snapshot_summary(facts: &Value) -> Value {
    let containers: Vec<Value> = facts
        .get("containers")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .map(|c| {
                    let mut o = json!({ "container": c["container"].as_str().unwrap_or("") });
                    if let Some(s) = c["state"].as_str() {
                        o["state"] = json!(s);
                    }
                    if c.get("present") == Some(&Value::Bool(false)) {
                        o["present"] = json!(false);
                    }
                    o
                })
                .collect()
        })
        .unwrap_or_default();
    let replication: Vec<Value> = facts
        .get("slaves")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .map(|s| {
                    let repl = s.get("replication");
                    json!({
                        "container": s["container"].as_str().unwrap_or(""),
                        "reachable": s["reachable"].as_bool().unwrap_or(false),
                        "replication": repl.map(|r| r.to_string()).unwrap_or_default(),
                        "error": s["error"].as_str().unwrap_or(""),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    let logs: Vec<Value> = facts
        .get("logs")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .take(3)
                .map(|l| {
                    let lines = l["lines"].as_str().unwrap_or("");
                    let clipped: String = lines.chars().take(240).collect();
                    json!({ "container": l["container"].as_str().unwrap_or(""), "lines": clipped })
                })
                .collect()
        })
        .unwrap_or_default();
    json!({
        "containers": containers,
        "replication": replication,
        "logs": logs,
    })
}

// ─── 告警群归聚与事件时间线(dba-ai-design §5.2;规则版) ───

/// open 告警按 (kind, 归一化消息) 归群 → 群卡(区分"单点牵连"同因多实例;
/// 不改 alerts 去重语义,归聚实时计算不落库)。alerts = store.alert_list 原始行。
pub fn group_alerts(alerts: &[Value]) -> Vec<Value> {
    use std::collections::HashMap;
    let mut groups: HashMap<String, Vec<&Value>> = HashMap::new();
    for a in alerts {
        let kind = a["kind"].as_str().unwrap_or("");
        let msg = a["message"].as_str().unwrap_or("");
        let key = format!("{}::{}", kind, normalize(msg));
        groups.entry(key).or_default().push(a);
    }
    let mut rows: Vec<(String, Vec<&Value>)> = groups.into_iter().collect();
    rows.sort_by(|a, b| b.1.len().cmp(&a.1.len()));
    rows.into_iter()
        .map(|(key, members)| {
            let first = members[0];
            let msg = first["message"].as_str().unwrap_or("");
            let kind = first["kind"].as_str().unwrap_or("");
            let mut sev_rank = 0u8; // critical > warn > info
            for m in &members {
                let r = match m["severity"].as_str().unwrap_or("") {
                    "critical" => 3,
                    "warn" => 2,
                    _ => 1,
                };
                sev_rank = sev_rank.max(r);
            }
            let severity = match sev_rank {
                3 => "critical",
                2 => "warn",
                _ => "info",
            };
            let member_rows: Vec<Value> = members
                .iter()
                .map(|m| {
                    json!({
                        "id": m["id"], "ts": m["ts"], "instance": m["instance"],
                        "kind": m["kind"], "severity": m["severity"], "message": m["message"],
                        "status": m["status"],
                    })
                })
                .collect();
            json!({
                "key": key,
                "pattern": normalize(msg),
                "kind": kind,
                "label": label_of(msg),
                "severity": severity,
                "count": members.len(),
                "members": member_rows,
                "suggested_playbooks": playbook_hits(msg),
                "message": msg,
            })
        })
        .collect()
}

/// 单实例事件时间线:alert ∪ evidence ∪ audit 按 ts 合并、新→旧。
/// alerts = alert_list(instance) 行;evidence = evidence_latest 行;audit = audit_list(instance) 行。
pub fn timeline(alerts: &[Value], evidence: &[Value], audit: &[Value]) -> Vec<Value> {
    let mut events: Vec<Value> = Vec::new();
    for a in alerts {
        events.push(json!({
            "ts": a["ts"],
            "type": "alert",
            "summary": format!("告警[{}] {}", a["kind"].as_str().unwrap_or(""), a["message"].as_str().unwrap_or("")),
            "severity": a["severity"],
            "status": a["status"],
            "alert_id": a["id"],
        }));
    }
    for e in evidence {
        events.push(json!({
            "ts": e["ts"],
            "type": "evidence",
            "summary": format!("现场快照: {}", e["reason"].as_str().unwrap_or("")),
            "kind": e["kind"],
        }));
    }
    for u in audit {
        let action = u["action"].as_str().unwrap_or("");
        let result = u["result"].as_str().unwrap_or("");
        let user = u["user"].as_str().unwrap_or("");
        events.push(json!({
            "ts": u["ts"],
            "type": "audit",
            "summary": if result.is_empty() {
                format!("审计[{action}] by {user}")
            } else {
                format!("审计[{action}] {result} by {user}")
            },
            "action": action,
            "user": user,
        }));
    }
    events.sort_by(|a, b| {
        b["ts"]
            .as_u64()
            .unwrap_or(0)
            .cmp(&a["ts"].as_u64().unwrap_or(0))
    });
    events.truncate(200);
    events
}

// ─── 日报/周报(规则版) ───

/// 组装报告。audit_rows = store.audit_since(period_since, 上限) 原始行;
/// insts = 全部实例快照(内部按状态过滤统计)。
pub fn compose_report(
    period: &str,
    now: u64,
    audit_rows: &[Value],
    insts: &[RdsInstance],
) -> Option<Value> {
    let since = period_since(period, now)?;
    // 动作计数
    let mut by_action: Vec<(String, usize)> = Vec::new();
    let mut total_ops = 0usize;
    let mut degrade = 0usize;
    let mut recover = 0usize;
    let mut last_degrade: Vec<(String, u64, String)> = Vec::new(); // 最新降级事件(instance, ts, reason)
    for row in audit_rows.iter().rev() {
        let action = row["action"].as_str().unwrap_or("");
        total_ops += 1;
        match action {
            "degrade" => {
                degrade += 1;
                last_degrade.push((
                    row["instance"].as_str().unwrap_or("").to_string(),
                    row["ts"].as_u64().unwrap_or(0),
                    row["result"].as_str().unwrap_or("").to_string(),
                ));
            }
            "recover" => recover += 1,
            _ => {}
        }
        if let Some(e) = by_action.iter_mut().find(|(a, _)| a == action) {
            e.1 += 1;
        } else {
            by_action.push((action.to_string(), 1));
        }
    }
    by_action.sort_by(|a, b| b.1.cmp(&a.1));
    last_degrade.sort_by(|a, b| b.1.cmp(&a.1));
    last_degrade.truncate(10);

    let total = insts.len();
    let bad: Vec<&RdsInstance> = insts
        .iter()
        .filter(|i| matches!(i.status, InstStatus::Degraded | InstStatus::Failed))
        .collect();
    let clusters = cluster_anomalies(insts);
    let clusters_top = clusters.iter().take(5).cloned().collect::<Vec<_>>();

    let mut lines: Vec<String> = Vec::new();
    lines.push(format!(
        "= rdsctl 运维{}报告(生成于 ts={now},统计窗口 since={since})",
        if period == "today" { "日" } else { "周" }
    ));
    lines.push(format!(
        "实例:共 {total} 台,异常(degraded/failed){} 台",
        bad.len()
    ));
    lines.push(format!(
        "审计动作计数:共 {total_ops} 条(degrade {degrade} / recover {recover});{}",
        by_action
            .iter()
            .map(|(a, n)| format!("{a}×{n}"))
            .collect::<Vec<_>>()
            .join(", ")
    ));
    if !last_degrade.is_empty() {
        lines.push("最新降级事件(≤10):".to_string());
        for (inst, _ts, reason) in &last_degrade {
            lines.push(format!("  - {inst}: {reason}"));
        }
    }
    if !clusters_top.is_empty() {
        lines.push("异常模式 Top5:".to_string());
        for c in &clusters_top {
            let pbs = c["suggested_playbooks"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .map(|p| p["id"].as_str().unwrap_or("").to_string())
                        .collect::<Vec<_>>()
                        .join(" / ")
                })
                .unwrap_or_default();
            lines.push(format!(
                "  - {} ({} 台): {} → 建议: {}",
                c["label"].as_str().unwrap_or(""),
                c["count"].as_u64().unwrap_or(0),
                c["pattern"].as_str().unwrap_or(""),
                if pbs.is_empty() { "无" } else { pbs.as_str() }
            ));
        }
    } else if bad.is_empty() {
        lines.push("无异常实例。".to_string());
    }

    Some(json!({
        "period": period,
        "generated_at": now,
        "since": since,
        "text": lines.join("\n"),
        "counts": {
            "total": total,
            "anomalies": bad.len(),
            "audit_ops": total_ops,
            "degrade": degrade,
            "recover": recover,
            "by_action": by_action
                .into_iter()
                .map(|(a, n)| json!({ "action": a, "n": n }))
                .collect::<Vec<_>>(),
        },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instance::{InstNode, InstStatus, Role};

    fn fake(name: &str, status: InstStatus, last_error: &str) -> RdsInstance {
        RdsInstance {
            name: name.to_string(),
            status,
            region: "cn-bj".to_string(),
            az: "az1".to_string(),
            shard: "s0".to_string(),
            tenant: "t1".to_string(),
            enabled: true,
            lvs: vec![],
            lvs_container: String::new(),
            lvs_mysql_port: 0,
            proxies: vec![],
            shards: vec![],
            biz: String::new(),
            contact: String::new(),
            dba: String::new(),
            core: false,
            itype: "async".to_string(),
            mysql_version: String::new(),
            proxy_version: String::new(),
            spec: String::new(),
            shard_num: 1,
            data_size: String::new(),
            buffer_pool: String::new(),
            max_qps: 0,
            max_tps: 0,
            network: format!("rds-{name}"),
            nodes: vec![InstNode {
                container: format!("rds-{name}-master"),
                role: Role::Master,
                host: format!("rds-{name}-master"),
                port: 3306,
                host_port: 35001,
                server_id: 1,
                region: String::new(),
                az: String::new(),
                shard: String::new(),
                parent: String::new(),
                rpc_host_port: 0,
            }],
            proxy_container: format!("rds-{name}-proxy"),
            proxy_mysql_port: 35002,
            proxy_mng_port: 35003,
            created_at: 1,
            root_password: "x".to_string(),
            query_secret: String::new(),
            last_error: last_error.to_string(),
            node_states: std::collections::HashMap::new(),
            node_hosts: std::collections::HashMap::new(),
            auto_failover: true,
        }
    }

    fn run(name: &str) -> RdsInstance {
        fake(name, InstStatus::Running, "")
    }
    fn deg(name: &str, err: &str) -> RdsInstance {
        fake(name, InstStatus::Degraded, err)
    }
    fn fail(name: &str, err: &str) -> RdsInstance {
        fake(name, InstStatus::Failed, err)
    }

    #[test]
    fn normalize_same_semantics_same_pattern() {
        let a = normalize("代理容器 rds-app-1-proxy 缺失");
        let b = normalize("代理容器 rds-app-2-proxy 缺失");
        assert_eq!(a, b);
        assert!(a.contains("PROXY"));
        // 端口/编号归一
        let c = normalize("从库 rds-x-slave-1 复制中断(IO/SQL 线程未运行)");
        let d = normalize("从库 rds-y-slave-2 复制中断(IO/SQL 线程未运行)");
        assert_eq!(c, d);
        assert!(c.contains("SLAVE"));
        // 不同语义不合并
        assert_ne!(normalize("代理容器缺失"), normalize("复制中断"));
        // running 实例(空原因)不应归一成异常
        assert_eq!(normalize(""), "");
    }

    #[test]
    fn label_and_playbook_hits() {
        assert_eq!(label_of("从库 rds-a-slave-1 复制中断"), "复制中断");
        let hits = playbook_hits("从库 rds-a-slave-1 复制中断(IO/SQL 线程未运行)");
        let ids: Vec<&str> = hits.iter().map(|h| h["id"].as_str().unwrap()).collect();
        assert!(ids.contains(&"start_replica"));
        assert!(!ids.contains(&"restart_proxy"));
        let hits2 = playbook_hits("代理容器 rds-a-proxy 缺失");
        assert!(hits2.iter().any(|h| h["id"] == "restart_proxy"));
        let hits3 = playbook_hits("任务 t-create-1 失败");
        assert!(hits3.iter().any(|h| h["id"] == "retry_task"));
        let hits4 = playbook_hits("降级残留,可销毁清理后重建");
        assert!(hits4.iter().any(|h| h["id"] == "destroy_residual"));
    }

    #[test]
    fn cluster_groups_same_cause_across_instances() {
        let insts = vec![
            run("ok1"),
            deg("a1", "代理容器 rds-a1-proxy 缺失"),
            deg("a2", "代理容器 rds-a2-proxy 缺失"),
            deg("a3", "代理容器 rds-a3-proxy 缺失"),
            fail("b1", "任务 t-create-9 失败"),
            deg("c1", "从库 rds-c1-slave-1 复制中断(IO/SQL 线程未运行)"),
        ];
        let clusters = cluster_anomalies(&insts);
        assert_eq!(clusters.len(), 3);
        // 最大的群 = 3 台同因代理缺失
        assert_eq!(clusters[0]["count"], 3);
        assert_eq!(clusters[0]["label"], "代理容器缺失");
        assert_eq!(clusters[0]["members"].as_array().unwrap().len(), 3);
        assert!(clusters[0]["suggested_playbooks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["id"] == "restart_proxy"));
        // 单台异常各自成组
        assert!(clusters
            .iter()
            .any(|c| c["count"] == 1 && c["label"] == "任务失败"));
        assert!(clusters
            .iter()
            .any(|c| c["count"] == 1 && c["label"] == "复制中断"));
    }

    #[test]
    fn snapshot_summary_clips_logs() {
        let facts = json!({
            "containers": [{"container": "rds-a-proxy", "present": false}],
            "slaves": [{"container": "rds-a-slave-1", "reachable": false, "error": "err", "replication": null}],
            "logs": [
                {"container": "rds-a-proxy", "lines": "x".repeat(1000)},
                {"container": "rds-a-slave-1", "lines": "y"},
            ],
        });
        let s = snapshot_summary(&facts);
        assert_eq!(s["containers"][0]["present"], false);
        assert_eq!(s["logs"].as_array().unwrap().len(), 2);
        assert!(s["logs"][0]["lines"].as_str().unwrap().chars().count() <= 240);
        // 摘要不应包含整包 facts 的额外字段
        assert!(s.get("containers").is_some() && s.get("debug").is_none());
    }

    #[test]
    fn report_has_counts_and_text() {
        let now = 1_800_000_000u64;
        let audit = vec![
            json!({"action": "create", "instance": "a1", "result": "ok", "ts": now - 100}),
            json!({"action": "degrade", "instance": "a1", "result": "代理不可达: err", "ts": now - 50}),
            json!({"action": "recover", "instance": "a1", "result": "ok", "ts": now - 10}),
            json!({"action": "degrade", "instance": "a2", "result": "复制中断", "ts": now - 5}),
        ];
        let insts = vec![run("a1"), deg("a2", "从库 rds-a2-slave-1 复制中断")];
        let r = compose_report("today", now, &audit, &insts).expect("report");
        assert_eq!(r["period"], "today");
        assert_eq!(r["counts"]["total"], 2);
        assert_eq!(r["counts"]["anomalies"], 1);
        assert_eq!(r["counts"]["degrade"], 2);
        assert_eq!(r["counts"]["recover"], 1);
        let text = r["text"].as_str().unwrap();
        assert!(text.contains("复制中断"), "text 应含异常模式: {text}");
        assert!(text.contains("a2"));
        // 未知时段返回 None
        assert!(compose_report("month", now, &audit, &insts).is_none());
    }

    #[test]
    fn period_since_boundaries() {
        let now = 1_800_000_000u64;
        assert_eq!(period_since("today", now).unwrap(), now - now % 86400);
        assert_eq!(period_since("week", now).unwrap(), now - 7 * 86400);
        assert_eq!(period_since("bad", now), None);
    }

    #[test]
    fn alert_groups_cluster_same_cause_and_keep_severity() {
        let alert = |id: u64, inst: &str, kind: &str, sev: &str, msg: &str| {
            json!({
                "id": id, "ts": 100, "instance": inst, "kind": kind, "severity": sev,
                "message": msg, "status": "open",
            })
        };
        let rows = vec![
            alert(
                1,
                "a1",
                "degraded",
                "warn",
                "容器 rds-a1-master-1 缺失;代理不可达",
            ),
            alert(
                2,
                "a2",
                "degraded",
                "critical",
                "容器 rds-a2-master-1 缺失;代理不可达",
            ),
            alert(3, "b1", "task_failed", "info", "任务 12 失败"),
        ];
        let groups = group_alerts(&rows);
        assert_eq!(groups.len(), 2);
        let g = groups.iter().find(|g| g["kind"] == "degraded").unwrap();
        assert_eq!(g["count"], 2, "同类同因归 1 群(实例名/编号归一)");
        assert_eq!(g["severity"], "critical", "群严重度取成员最高");
        assert_eq!(g["members"].as_array().unwrap().len(), 2);
        assert!(g["suggested_playbooks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["id"] == "restart_proxy"));
        let single = groups.iter().find(|g| g["kind"] == "task_failed").unwrap();
        assert_eq!(single["count"], 1);
    }

    #[test]
    fn alert_groups_distinguish_different_causes() {
        let rows = vec![
            json!({"id": 1, "ts": 1, "instance": "a", "kind": "degraded", "severity": "warn", "message": "复制中断", "status": "open"}),
            json!({"id": 2, "ts": 2, "instance": "b", "kind": "degraded", "severity": "warn", "message": "容器缺失", "status": "open"}),
        ];
        let groups = group_alerts(&rows);
        assert_eq!(groups.len(), 2, "不同原因不合并");
    }

    #[test]
    fn timeline_merges_sorts_and_caps() {
        let alerts = vec![
            json!({"ts": 300, "id": 1, "kind": "degraded", "message": "代理不可达", "severity": "warn", "status": "open"}),
        ];
        let evidence =
            vec![json!({"ts": 200, "kind": "degrade", "reason": "容器 rds-a-master-1 缺失"})];
        let audit = vec![
            json!({"ts": 400, "action": "recover", "result": "ok", "user": "sweeper"}),
            json!({"ts": 100, "action": "create", "result": "ok", "user": "admin"}),
        ];
        let ev = timeline(&alerts, &evidence, &audit);
        assert_eq!(ev.len(), 4);
        // 新→旧
        let ts: Vec<u64> = ev.iter().map(|e| e["ts"].as_u64().unwrap()).collect();
        assert_eq!(ts, vec![400, 300, 200, 100]);
        assert_eq!(ev[0]["type"], "audit");
        assert_eq!(ev[1]["type"], "alert");
        assert_eq!(ev[2]["type"], "evidence");
        // 上限 200
        let many_alerts: Vec<Value> = (0..250u64).map(|i| json!({"ts": i, "kind": "degraded", "message": "x", "severity": "warn", "status": "open"})).collect();
        assert_eq!(timeline(&many_alerts, &[], &[]).len(), 200);
    }
}
