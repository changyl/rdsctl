// rdsctl — 规则版值班问答/FAQ(dba-ai-design §7;纯规则、零 LLM、零网络)
//
// 职责:
//   - 三层语料:T1 静态 runbook(代码内常量目录,对齐 insights::playbook_registry 哲学)、
//     T3 现场证据(调用方注入 evidence 行)→ 模板化中文回答 + 结构化 refs;
//   - classify 意图(状态问询 / 原因问询 / FAQ 处置问询 / 帮助)与 faq_search 关键词检索
//     均为确定性纯函数(可单测);LLM 增强(RAG)标注 AI-1 gate 后接入,本模块不含 LLM。
//   - 红线:不访问网络;不触碰 root_password;detail 一律 ≤240 字符;文本全中文。

use serde_json::{json, Value};

use crate::insights::{label_of, playbook_hits};
use crate::instance::RdsInstance;

/// runbook 静态词条(值班 FAQ 语料 T1;关键词任一命中即算命中)
pub struct Runbook {
    pub kw: &'static [&'static str],
    pub title: &'static str,
    pub body: &'static str,
}

pub fn runbook_entries() -> Vec<Runbook> {
    vec![
        Runbook {
            kw: &[
                "复制中断",
                "slave",
                "从库",
                "io 线程",
                "applier",
                "start replica",
            ],
            title: "复制中断处置",
            body: "核对 performance_schema 复制线程 SERVICE_STATE 与 LAST_ERROR;\
恢复复制建议走 playbook start_replica(低风险;经人工确认后执行,先追平 GTID 再启动)。",
        },
        Runbook {
            kw: &["代理", "proxy", "不可达", "重启代理"],
            title: "代理异常处置",
            body: "核对代理容器状态与 mng 端口连通;重建/重启建议走 playbook restart_proxy\
(中风险;人工确认后执行)。",
        },
        Runbook {
            kw: &["容器缺失", "容器", "节点没了", "容器被删"],
            title: "容器缺失处置",
            body: "先查 evidence 快照确认缺失节点与最近变更;按实例状态走 destroy_residual\
(高风险,双确认)或任务重跑。",
        },
        Runbook {
            kw: &["慢查询", "慢查", "slow", "性能", "索引"],
            title: "慢查询治理",
            body: "慢查询页按 digest 全局归并;对 open 治理项用查询台 EXPLAIN 复核,\
参考规则建议(索引候选/改写方向);无需改代码可先核对 schema 索引覆盖。",
        },
        Runbook {
            kw: &["磁盘", "容量", "空间", "满了", "扩容"],
            title: "容量与扩容",
            body: "容量卡给出磁盘外推与 days_to_90pct;逼近阈值先清理归档/日志;\
规格扩容属数据面伸缩(登记 M1),当前仅建议不自动执行。",
        },
        Runbook {
            kw: &["任务失败", "重跑", "retry", "失败任务"],
            title: "失败任务处置",
            body: "任务页展开失败节点查看 output;重跑入口登记 M1a(单任务重跑);\
可先人工确认失败原因后重试单步。",
        },
        Runbook {
            kw: &[
                "怎么",
                "如何",
                "处理",
                "解决",
                "修复",
                "怎么办",
                "步骤",
                "命令",
            ],
            title: "常规处置入口",
            body: "多数处置先看洞察面板:异常群给出同因批量处置模板;每个建议对应\
playbook 且需人工确认;所有动作留审计。",
        },
    ]
}

/// 意图分类(启发式;规则版)。返回 "status"|"why"|"faq"|"help"
pub fn classify(q: &str) -> String {
    let ql = q.to_lowercase();
    let has = |keys: &[&str]| keys.iter().any(|k| ql.contains(k));
    if has(&["为什么", "为何", "why", "根因", "原因是什么", "为啥"]) || ql.starts_with("为什么")
    {
        "why".to_string()
    } else if has(&["状态", "现在", "当前", "status", "情况"]) {
        "status".to_string()
    } else if has(&[
        "怎么",
        "如何",
        "处理",
        "解决",
        "修复",
        "怎么办",
        "步骤",
        "命令",
        "操作",
    ]) {
        "faq".to_string()
    } else {
        "help".to_string()
    }
}

/// 命中 runbook 下标(≤3;关键词任一命中,q 转小写后包含 kw)
pub fn faq_search(q: &str) -> Vec<usize> {
    let ql = q.to_lowercase();
    let mut hits: Vec<usize> = Vec::new();
    for (i, e) in runbook_entries().iter().enumerate() {
        if hits.len() >= 3 {
            break;
        }
        if e.kw.iter().any(|k| ql.contains(k)) {
            hits.push(i);
        }
    }
    hits
}

fn clip(s: &str, n: usize) -> String {
    s.chars().take(n).collect()
}

/// 规则版回答(纯函数;调用方注入实例与现场证据,输出不含敏感字段)。
/// evidence_rows = store.evidence_latest 行(含 reason;facts 摘要由调用方脱敏)。
pub fn answer(q: &str, inst_opt: Option<&RdsInstance>, evidence_rows: &[Value]) -> Value {
    let intent = classify(q);
    let mut refs: Vec<Value> = Vec::new();
    let text: String = match intent.as_str() {
        "status" => {
            match &inst_opt {
                Some(i) => {
                    let mut s = format!(
                        "实例 {} 当前状态:{}",
                        i.name,
                        i.status.label()
                    );
                    if !i.last_error.is_empty() {
                        s.push_str(&format!(";最近原因:{}", clip(&i.last_error, 120)));
                    }
                    s.push_str("。详见实例详情与现场快照。");
                    refs.push(json!({
                        "type": "instance", "label": i.name,
                        "detail": format!("{} / {}", i.status.label(), clip(&i.last_error, 160)),
                    }));
                    s
                }
                None => "未定位到目标实例(可加 instance 参数或在实例详情页提问)。".to_string(),
            }
        }
        "why" => {
            match &inst_opt {
                Some(i) => {
                    let class = label_of(&i.last_error);
                    let pb = playbook_hits(&i.last_error);
                    let mut s = format!(
                        "实例 {} 的异常类别判定为「{}」。\n规则版结论(无 LLM):\n- 现场原因:{}",
                        i.name,
                        class,
                        clip(&i.last_error, 160)
                    );
                    if pb.is_empty() {
                        s.push_str(
                            "\n- 无现成 playbook 命中:建议核对现场快照与审计时间线后手工处置。",
                        );
                    } else {
                        for p in &pb {
                            s.push_str(&format!(
                                "\n- 处置建议[{}]:{}(风险 {})",
                                p["id"].as_str().unwrap_or(""),
                                p["name"].as_str().unwrap_or(""),
                                p["risk"].as_str().unwrap_or("")
                            ));
                        }
                        s.push_str("\n- 以上仅为建议,执行前需人工确认并走既有状态机与审计。");
                    }
                    refs.push(json!({
                        "type": "instance", "label": i.name,
                        "detail": format!("{} / {}", i.status.label(), clip(&i.last_error, 160)),
                    }));
                    for p in pb {
                        refs.push(json!({
                            "type": "playbook", "label": p["name"].as_str().unwrap_or(""),
                            "detail": format!("{} (风险 {})", p["reason"].as_str().unwrap_or(""), p["risk"].as_str().unwrap_or("")),
                        }));
                    }
                    s
                }
                None => {
                    "未定位到目标实例,无法做原因归因(可加 instance 参数或在实例详情页提问)。"
                        .to_string()
                }
            }
        }
        "faq" => {
            let hits = faq_search(q);
            if hits.is_empty() {
                "知识库暂无直接命中;建议在查询台 EXPLAIN 或咨询值班同事。".to_string()
            } else {
                let mut s = String::new();
                for (k, i) in hits.iter().enumerate() {
                    let e = &runbook_entries()[*i];
                    if k > 0 {
                        s.push('\n');
                    }
                    s.push_str(&format!("- {}:{}\n  {}", e.title, e.body, ""));
                    refs.push(json!({
                        "type": "runbook", "label": e.title, "detail": clip(e.body, 240),
                    }));
                }
                s
            }
        }
        _ => {
            "值班助手(规则版)可回答:\n- 「X 现在什么状态/为什么降级」:实例状态与原因归因\n- 「复制中断/代理不可达/慢查询/磁盘 怎么处理」:FAQ 处置指引\n- 各回答带 refs 引用;动作类建议一律需人工确认。".to_string()
        }
    };

    // 现场证据(任意意图都附加,最多 3 条;避免重复已加 instance ref)
    if inst_opt.is_some() && intent != "why" && intent != "status" {
        for e in evidence_rows.iter().take(3) {
            let reason = e["reason"].as_str().unwrap_or("").to_string();
            if reason.is_empty() {
                continue;
            }
            refs.push(json!({
                "type": "evidence", "label": "现场快照",
                "detail": clip(&reason, 160),
            }));
        }
    }

    json!({ "intent": intent, "text": text, "refs": refs })
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

    #[test]
    fn classify_intents() {
        assert_eq!(classify("为什么实例 a1 降级"), "why");
        assert_eq!(classify("a1 现在什么状态"), "status");
        assert_eq!(classify("复制中断怎么处理"), "faq");
        assert_eq!(classify("你好"), "help");
        assert_eq!(classify("为什么慢查询变慢"), "why");
    }

    #[test]
    fn faq_search_hits_and_caps() {
        let hits = faq_search("复制中断 怎么处理 如何修复 磁盘满了 任务失败 怎么办");
        assert!(!hits.is_empty());
        assert!(hits.len() <= 3);
        // 关键词命中特定条目
        let h = faq_search("从库复制断了,怎么办");
        assert!(h.contains(&0), "应命中复制中断条目: {h:?}");
        let h2 = faq_search("mysql 慢查询索引优化");
        assert!(h2.contains(&3), "应命中慢查询条目: {h2:?}");
        assert!(faq_search("今天天气").is_empty());
    }

    #[test]
    fn answer_why_degrades_with_playbook_ref() {
        let inst = fake("a1", InstStatus::Degraded, "从库 rds-a1-slave-1 复制中断");
        let evidence = vec![json!({ "reason": "复制线程 IO 停止", "kind": "degrade" })];
        let v = answer("为什么 a1 degraded", Some(&inst), &evidence);
        assert_eq!(v["intent"], "why");
        let text = v["text"].as_str().unwrap();
        assert!(text.contains("复制中断"), "{text}");
        assert!(
            text.contains("start_replica") || text.contains("处置建议"),
            "{text}"
        );
        let refs = v["refs"].as_array().unwrap();
        assert!(refs.iter().any(|r| r["type"] == "instance"));
        assert!(refs.iter().any(|r| r["type"] == "playbook"));
        // 红线:任何回答不出现口令/敏感字段
        assert!(!v.to_string().contains("root_password"));
        assert!(!v.to_string().contains("rds_root"));
    }

    #[test]
    fn answer_status_and_missing_instance() {
        let inst = fake("ok1", InstStatus::Running, "");
        let v = answer("ok1 现在什么状态", Some(&inst), &[]);
        assert_eq!(v["intent"], "status");
        assert!(v["text"].as_str().unwrap().contains("运行中"));
        // 未定位实例给出引导,不 panic
        let v2 = answer("为什么降级了", None, &[]);
        assert_eq!(v2["intent"], "why");
        assert!(v2["text"].as_str().unwrap().contains("未定位到目标实例"));
        let v3 = answer("你好", None, &[]);
        assert_eq!(v3["intent"], "help");
        assert!(v3["text"].as_str().unwrap().contains("值班助手"));
    }

    #[test]
    fn answer_faq_lists_runbook_refs() {
        let v = answer("磁盘满了怎么处理", None, &[]);
        assert_eq!(v["intent"], "faq");
        let refs = v["refs"].as_array().unwrap();
        assert!(refs
            .iter()
            .any(|r| r["type"] == "runbook" && r["label"] == "容量与扩容"));
        // 无命中回退文案(classify 经「操作」入 faq,但 runbook 关键词不含 → 零 refs)
        let v2 = answer("给我看下系统操作说明", None, &[]);
        assert_eq!(v2["intent"], "faq");
        assert!(v2["refs"].as_array().unwrap().is_empty());
        assert!(v2["text"].as_str().unwrap().contains("暂无直接命中"));
    }
}
