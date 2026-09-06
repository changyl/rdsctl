// rdsctl — 备份节点注册联动(backup-link-design §4–§7)
//
// 形态:本地 outbox(backup_outbox)持久化事件 + 后台投递 worker,经 curl CLI(默认)
// 或自定义脚本(RDSCTL_BACKUP_REG_SCRIPT)调用外部备份平台 HTTP API,零新 crate。
//
// 事件:register_instance / register_node / deregister_instance / heartbeat / backup_result;
// 幂等键防重复投递;失败指数退避(封顶 10 次 → dead,可人工 requeue);平台不可达/失败
// 只积压重试,绝不阻断实例状态机。默认 RDSCTL_BACKUP_REG_ENABLED=0 → 全部 no-op,
// 无任何表写入与进程调用(零行为变化)。

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};

use crate::instance::{InstNode, InstStatus, RdsInstance, RdsManager};
use crate::manager;

const MAX_ATTEMPTS: u32 = 10;

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub fn enabled() -> bool {
    std::env::var("RDSCTL_BACKUP_REG_ENABLED").as_deref() == Ok("1")
}

fn env_str(key: &str, d: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| d.to_string())
}

fn env_u64(key: &str, d: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(d)
}

/// role 序列化口径与 instance.rs 一致(master/read/offline)
fn role_tag(r: crate::instance::Role) -> &'static str {
    match r {
        crate::instance::Role::Master => "master",
        crate::instance::Role::Read => "read",
        crate::instance::Role::Offline | crate::instance::Role::Stats | crate::instance::Role::Backup => "offline",
    }
}

fn status_str(s: crate::instance::InstStatus) -> String {
    serde_json::to_string(&s)
        .unwrap_or_default()
        .trim_matches('"')
        .to_string()
}

fn node_json(i: &RdsInstance, n: &InstNode) -> Value {
    json!({
        "container": n.container,
        "role": role_tag(n.role),
        "host_port": n.host_port,
        "server_id": n.server_id,
        "region": if n.region.is_empty() { i.region.clone() } else { n.region.clone() },
        "az": if n.az.is_empty() { i.az.clone() } else { n.az.clone() },
        "backup_capable": n.role.is_offline() || n.role == crate::instance::Role::Master,
    })
}

fn payload_register(i: &RdsInstance) -> String {
    json!({
        "instance": i.name,
        "region": i.region,
        "az": i.az,
        "tenant": i.tenant,
        "status": status_str(i.status),
        "enabled": i.enabled,
        "created_at": i.created_at,
        "nodes": i.nodes.iter().map(|n| node_json(i, n)).collect::<Vec<_>>(),
    })
    .to_string()
}

fn ikey(event: &str, instance: &str, node: &str) -> String {
    let key = format!("{event}-{instance}-{node}");
    key.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn enqueue(event: &str, instance: &str, node: &str, key: &str, payload: &str) {
    manager().store.backup_outbox_enqueue(event, instance, node, key, payload);
}

/// create/scaleout/destroy 任务成功联动(kind 与既有 scheduler 语义一致)
pub fn on_task_success(kind: &str, instance: &str) {
    if !enabled() {
        return;
    }
    let Some(i) = manager().instances.get(instance).map(|e| e.value().clone()) else {
        return;
    };
    match kind {
        "create" => {
            enqueue(
                "register_instance",
                instance,
                "master",
                &ikey("register_instance", instance, "master"),
                &payload_register(&i),
            );
        }
        "scaleout" => {
            // 对每个非主节点发 register_node(幂等键=container;老节点已 done 自动去重)
            for n in i.nodes.iter().filter(|n| n.role != crate::instance::Role::Master) {
                enqueue(
                    "register_node",
                    instance,
                    &n.container,
                    &ikey("register_node", instance, &n.container),
                    &json!({ "instance": i.name, "node": node_json(&i, n) }).to_string(),
                );
            }
        }
        "destroy" => {
            enqueue(
                "deregister_instance",
                instance,
                "master",
                &ikey("deregister_instance", instance, "master"),
                &json!({ "instance": i.name }).to_string(),
            );
        }
        _ => {}
    }
}

/// 备份任务终态(成功带产物字节数;失败带原因摘要,均通知平台)
pub fn on_backup_task_end(instance: &str, task_id: &str, ok: bool) {
    if !enabled() {
        return;
    }
    // 从任务节点输出中尽力解析 "backup-ok bytes=N"
    let mut bytes: u64 = 0;
    if let Some(v) = manager().scheduler.get(task_id) {
        if let Some(nodes) = v["nodes"].as_array() {
            for n in nodes {
                let out = n["output"].as_str().unwrap_or("");
                if let Some(p) = out.find("backup-ok bytes=") {
                    if let Some(rest) = out[p + "backup-ok bytes=".len()..].split_whitespace().next()
                    {
                        bytes = rest.parse().unwrap_or(0);
                    }
                }
            }
        }
    }
    let payload = json!({
        "instance": instance,
        "task_id": task_id,
        "ok": ok,
        "bytes": bytes,
    });
    let key = ikey("backup_result", instance, task_id);
    enqueue("backup_result", instance, "", &key, &payload.to_string());
}

/// 启停切换 → heartbeat(带 enabled 状态)
pub fn on_instance_enabled(instance: &str, on: bool) {
    let _ = on;
    if !enabled() {
        return;
    }
    let Some(i) = manager().instances.get(instance).map(|e| e.value().clone()) else {
        return;
    };
    let payload = json!({
        "instance": i.name,
        "enabled": i.enabled,
        "status": status_str(i.status),
        "nodes": i.nodes.iter().map(|n| json!({
            "container": n.container, "role": role_tag(n.role),
            "host_port": n.host_port, "backup_capable": n.role.is_offline() || n.role == crate::instance::Role::Master,
        })).collect::<Vec<_>>(),
    });
    // heartbeat 同分钟幂等(避免堆积)
    let key = ikey("heartbeat", instance, &(now() / 60).to_string());
    enqueue("heartbeat", instance, "", &key, &payload.to_string());
}

// ─── 投递 ───

enum Delivery {
    Ok,
    Retry,
    Permanent,
}

/// 实际调用:脚本优先;否则 curl(headers 经文件 -H @file,token 不进 argv)
async fn deliver(event: &str, key: &str, payload: &str) -> Delivery {
    let script = env_str("RDSCTL_BACKUP_REG_SCRIPT", "");
    if !script.is_empty() {
        let out = tokio::process::Command::new(&script)
            .args([event, key])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn();
        let Ok(mut child) = out else {
            return Delivery::Retry;
        };
        use tokio::io::AsyncWriteExt;
        if let Some(mut si) = child.stdin.take() {
            let _ = si.write_all(payload.as_bytes()).await;
            let _ = si.shutdown().await;
        }
        let res = child
            .wait_with_output()
            .await
            .map(|o| o.status.code().unwrap_or(3))
            .unwrap_or(3);
        return match res {
            0 => Delivery::Ok,
            2 => Delivery::Permanent,
            _ => Delivery::Retry,
        };
    }
    // curl 模式
    let url_base = env_str("RDSCTL_BACKUP_REG_URL", "");
    if url_base.is_empty() {
        return Delivery::Retry;
    }
    let token = env_str("RDSCTL_BACKUP_REG_TOKEN", "");
    let url = if url_base.contains("{event}") {
        url_base.replace("{event}", event)
    } else if url_base.contains('?') {
        format!("{url_base}&event={event}")
    } else {
        format!("{url_base}?event={event}")
    };
    let timeout = env_u64("RDSCTL_BACKUP_TIMEOUT_SECS", 10).clamp(1, 60);
    let dir = std::env::temp_dir();
    let tag = format!("rdsctl-br-{}-{}", std::process::id(), now());
    let hf = dir.join(format!("{tag}.hdr"));
    let pf = dir.join(format!("{tag}.payload"));
    let header_content = format!("Authorization: Bearer {token}");
    let _ = std::fs::write(&hf, &header_content);
    let _ = std::fs::write(&pf, payload);
    let out = tokio::process::Command::new("curl")
        .args([
            "-sS", "-m", &timeout.to_string(), "-X", "POST",
            "-H", "Content-Type: application/json",
            "-H", &format!("@{}", hf.display()),
            "--data-binary", &format!("@{}", pf.display()),
            "-o", "/dev/null", "-w", "%{http_code}",
            &url,
        ])
        .output()
        .await;
    let _ = std::fs::remove_file(&hf);
    let _ = std::fs::remove_file(&pf);
    let code = out
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_default();
    match code.as_str() {
        c if c.starts_with('2') => Delivery::Ok,
        c if c.starts_with('4') => Delivery::Permanent,
        _ => Delivery::Retry,
    }
}

fn backoff_secs(attempts: u32) -> u64 {
    let b = 60u64.saturating_mul(1u64 << attempts.min(5));
    b.min(600)
}

async fn process_one(row: &Value) {
    let id = row["id"].as_u64().unwrap_or(0);
    let event = row["event"].as_str().unwrap_or("").to_string();
    let instance = row["instance"].as_str().unwrap_or("").to_string();
    let key = row["idempotency_key"].as_str().unwrap_or("").to_string();
    let payload = row["payload_json"].as_str().unwrap_or("").to_string();
    let attempts = row["attempts"].as_u64().unwrap_or(0) as u32;
    if id == 0 {
        return;
    }
    let res = deliver(&event, &key, &payload).await;
    let attempts = attempts + 1;
    let (state, next_at, err): (&str, u64, String) = match res {
        Delivery::Ok => ("done", 0, String::new()),
        Delivery::Permanent => ("dead", 0, "HTTP 4xx/脚本码 2:永久失败".to_string()),
        Delivery::Retry => {
            if attempts >= MAX_ATTEMPTS {
                ("dead", 0, format!("连续 {MAX_ATTEMPTS} 次失败,转为 dead"))
            } else {
                ("pending", now() + backoff_secs(attempts), "可重试失败".to_string())
            }
        }
    };
    manager().store.backup_outbox_mark(id, state, attempts, next_at, &err);
    // 审计摘要(不含 token/凭据)
    let ok = state == "done";
    manager().store.audit(
        "system",
        &instance,
        "backup_notify",
        &format!("{event}:{instance}"),
        if ok { "ok" } else { state },
        "",
    );
}

/// 周期投递 + 对账(仅 enabled 时启动;默认关闭零行为变化)
pub fn start(_mgr: &Arc<RdsManager>) {
    if !enabled() {
        return;
    }
    let period = env_u64("RDSCTL_BACKUP_TICK_SECS", 60).clamp(10, 3600);
    tracing::info!("备份联动 worker 启动:每 {period}s 一次");
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(period)).await;
            let rows = manager().store.backup_outbox_poll(50, now());
            for row in &rows {
                process_one(row).await;
            }
            reconcile();
            let pruned = manager()
                .store
                .backup_outbox_prune(now().saturating_sub(7 * 86400));
            if pruned > 0 {
                tracing::info!("backup outbox 清理完成项 {pruned}");
            }
        }
    });
}

/// 对账:running 实例应注册而未确认 → 补发 register_instance;失败节点可人工 retry
fn reconcile() {
    if !enabled() {
        return;
    }
    let m = manager();
    let states = m.store.backup_outbox_reg_state(None);
    let registered: Vec<(String, String)> = states
        .iter()
        .filter(|s| s["reg_state"].as_str() == Some("registered"))
        .filter_map(|s| {
            Some((
                s["instance"].as_str()?.to_string(),
                s["node"].as_str()?.to_string(),
            ))
        })
        .collect();
    let scan: Vec<RdsInstance> = m
        .instances
        .iter()
        .filter(|e| e.value().status == InstStatus::Running)
        .map(|e| e.value().clone())
        .collect();
    for i in scan {
        let need = i
            .nodes
            .iter()
            .any(|n| !registered.contains(&(i.name.clone(), n.container.clone())));
        if need {
            on_task_success("create", &i.name);
        }
    }
}

// ─── HTTP 视图辅助(api.rs) ───

/// 注册状态视图:以 outbox done 事件为主,叠加实例当前节点清单(instance=none 时全局)
pub fn reg_view(instance: Option<&str>) -> Vec<Value> {
    let m = manager();
    let mut map: std::collections::BTreeMap<(String, String), Value> = std::collections::BTreeMap::new();
    for s in m.store.backup_outbox_reg_state(instance) {
        let inst = s["instance"].as_str().unwrap_or("").to_string();
        let node = s["node"].as_str().unwrap_or("").to_string();
        map.insert((inst, node), s);
    }
    // 叠加以 outbox 为准;instance 过滤
    if let Some(name) = instance {
        if let Some(i) = m.instances.get(name) {
            for n in i.value().nodes.iter() {
                let key = (name.to_string(), n.container.clone());
                map.entry(key).or_insert_with(|| {
                    json!({
                        "instance": name,
                        "node": n.container,
                        "role": role_tag(n.role),
                        "backup_capable": n.role.is_offline() || n.role == crate::instance::Role::Master,
                        "reg_state": "unknown",
                        "last_event": "",
                        "last_error": "",
                        "updated_at": 0,
                    })
                });
            }
        }
    }
    map.into_values().collect()
}

// ─── 单元测试(纯逻辑/入队语义;无网络) ───

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ikey_sanitizes() {
        assert_eq!(ikey("register_node", "a/b", "c:d"), "register_node-a_b-c_d");
        assert_eq!(ikey("register_instance", "demo", "master"), "register_instance-demo-master");
    }

    #[test]
    fn payload_flags_offline_backup_capable() {
        let i = RdsInstance {
            name: "x".into(),
            status: InstStatus::Running,
            region: "cn-bj".into(),
            az: "az1".into(),
            shard: String::new(),
            tenant: String::new(),
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
            itype: "async".into(),
            mysql_version: String::new(),
            proxy_version: String::new(),
            spec: String::new(),
            shard_num: 1,
            data_size: String::new(),
            buffer_pool: String::new(),
            max_qps: 0,
            max_tps: 0,
            network: "net".into(),
            nodes: vec![
                InstNode { container: "m".into(), role: crate::instance::Role::Master, host: "m".into(), port: 3306, host_port: 35001, server_id: 1, region: String::new(), az: String::new(), shard: String::new(), parent: String::new() },
                InstNode { container: "o".into(), role: crate::instance::Role::Offline, host: "o".into(), port: 3306, host_port: 35002, server_id: 2, region: String::new(), az: String::new(), shard: String::new(), parent: String::new() },
            ],
            proxy_container: "p".into(),
            proxy_mysql_port: 0,
            proxy_mng_port: 0,
            created_at: 1,
            root_password: "x".into(),
            query_secret: String::new(),
            last_error: String::new(),
            node_states: std::collections::HashMap::new(),
            node_hosts: std::collections::HashMap::new(),
            auto_failover: true,
        };
        let p: Value = serde_json::from_str(&payload_register(&i)).unwrap();
        assert_eq!(p["nodes"][0]["backup_capable"], json!(true));
        assert_eq!(p["nodes"][1]["role"], "offline");
        assert_eq!(p["nodes"][1]["backup_capable"], json!(true));
    }

    #[test]
    fn backoff_bounded() {
        assert_eq!(backoff_secs(0), 60);
        assert!(backoff_secs(10) <= 600);
    }
}
