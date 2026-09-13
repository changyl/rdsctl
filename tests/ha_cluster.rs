//! 管控面集群验收(M1a 骨架):F1/F2/F12 + I1/I3 的端到端证据。
//!
//! 形态:真实启动 **3 个 rdsctl 进程**(`serve` 子命令),通过真实 HTTP 观察选举、多数派、
//! 租约与 fence;不依赖 MySQL(cluster 模式的业务 API 尚未接线,公开端口只提供探针)。
//!
//! lab 前提:测试机没有 chrony/timedatectl,因此显式设置
//!   RDSCTL_PREFLIGHT_ALLOW_UNVERIFIED_CLOCK=1 + RDSCTL_ALLOW_NO_AGENT=1
//! —— 这会体现在 `/readyz` 的 `premises_unverified` 里(测试同时断言该标注存在,
//!    防止"lab 放行被当成生产就绪")。

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

type Resp = (u16, String);

fn free_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind");
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

struct NodeProc {
    child: Child,
    id: String,
    public: u16,
    rpc: u16,
    dir: PathBuf,
    /// false = 保留数据目录(用于"重启同一副本"场景)
    cleanup: bool,
}

impl Drop for NodeProc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if self.cleanup {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}

struct Cluster {
    root: PathBuf,
    spec: String,
    nodes: Vec<NodeProc>,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        self.nodes.clear();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

impl Cluster {
    /// n=3 的正常集群;premises=lab 时用放行开关
    fn start(tag: &str, n: usize) -> Cluster {
        let root = std::env::temp_dir().join(format!(
            "rdsctl-ha-cluster-{tag}-{}-{}",
            std::process::id(),
            Instant::now().elapsed().as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let ids: Vec<String> = (1..=n).map(|i| format!("n{i}")).collect();
        let rpc_ports: Vec<u16> = (0..n).map(|_| free_port()).collect();
        let spec = ids
            .iter()
            .zip(rpc_ports.iter())
            .map(|(id, p)| format!("{id}@127.0.0.1:{p}"))
            .collect::<Vec<_>>()
            .join(",");
        let mut nodes = Vec::new();
        for (i, id) in ids.iter().enumerate() {
            let public = free_port();
            let proc = spawn_node(&root, id, public, rpc_ports[i], &spec, true);
            nodes.push(proc);
        }
        let c = Cluster { root, spec, nodes };
        c
    }

    fn ids(&self) -> Vec<String> {
        self.nodes.iter().map(|n| n.id.clone()).collect()
    }

    /// 轮询直到出现**最高 term 上的唯一 leader**(返回 (id, term))。
    ///
    /// 注意:不同 term 同时各有 leader 是正常的选举抖动(旧 leader 尚未收到更高 term 的消息),
    /// 因此不能要求"全集群只有一个自认 leader 的节点",而要看**最高 term 上是否唯一**。
    fn wait_single_leader(&self, timeout: Duration) -> (String, u64) {
        let deadline = Instant::now() + timeout;
        loop {
            let mut leaders: Vec<(String, u64)> = Vec::new();
            for n in &self.nodes {
                if let Some(st) = node_status(n.rpc) {
                    if st["role"] == "leader" {
                        leaders.push((n.id.clone(), st["term"].as_u64().unwrap_or(0)));
                    }
                }
            }
            if !leaders.is_empty() {
                let max_term = leaders.iter().map(|(_, t)| *t).max().unwrap();
                let at_max: Vec<&(String, u64)> =
                    leaders.iter().filter(|(_, t)| *t == max_term).collect();
                if at_max.len() == 1 {
                    return at_max[0].clone();
                }
            }
            assert!(
                Instant::now() < deadline,
                "{} 内未选出最高 term 上的唯一 leader(当前 leaders={leaders:?})",
                timeout.as_secs()
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// 向**当前 leader** 提案(自动重试:抖动期间缓存下来的 leader 可能已过期)
    fn propose_to_leader(&self, op_json: &str, timeout: Duration) -> (u16, String) {
        let deadline = Instant::now() + timeout;
        let mut last = (0u16, String::new());
        loop {
            let mut leaders: Vec<(String, u64)> = Vec::new();
            for n in &self.nodes {
                if let Some(st) = node_status(n.rpc) {
                    if st["role"] == "leader" {
                        leaders.push((n.id.clone(), st["term"].as_u64().unwrap_or(0)));
                    }
                }
            }
            leaders.sort_by_key(|(_, t)| std::cmp::Reverse(*t));
            if let Some((id, _)) = leaders.first() {
                let r = propose(self.node(id).rpc, op_json);
                if r.0 == 200 {
                    return r;
                }
                last = r;
            }
            assert!(
                Instant::now() < deadline,
                "{} 内未能向 leader 完成提案(最后响应={last:?})",
                timeout.as_secs()
            );
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    fn node(&self, id: &str) -> &NodeProc {
        self.nodes.iter().find(|n| n.id == id).expect("节点不存在")
    }

    /// 重启同一副本(同一 id / 端口 / 数据目录)—— 用于验证"副本启动不破坏他人状态"
    fn restart(&mut self, id: &str) {
        let idx = self
            .nodes
            .iter()
            .position(|n| n.id == id)
            .expect("节点不存在");
        let (public, rpc, dir) = {
            let n = &self.nodes[idx];
            (n.public, n.rpc, n.dir.clone())
        };
        {
            let mut old = self.nodes.remove(idx);
            let _ = old.child.kill();
            let _ = old.child.wait();
            old.cleanup = false; // 保留目录
        }
        let spec = self.spec.clone();
        let p = spawn_node_at(&dir, id, public, rpc, &spec, true);
        self.nodes.push(p);
    }

    fn kill(&mut self, id: &str) {
        if let Some(pos) = self.nodes.iter().position(|n| n.id == id) {
            let mut n = self.nodes.remove(pos);
            let _ = n.child.kill();
            let _ = n.child.wait();
            // 保留目录以便可能的重启;Drop 里 node 已被移除,手工清理
            let _ = std::fs::remove_dir_all(&n.dir);
        }
    }
}

fn spawn_node(
    root: &std::path::Path,
    id: &str,
    public: u16,
    rpc: u16,
    spec: &str,
    lab: bool,
) -> NodeProc {
    spawn_node_at(&root.join(id), id, public, rpc, spec, lab)
}

fn spawn_node_at(
    dir: &std::path::Path,
    id: &str,
    public: u16,
    rpc: u16,
    spec: &str,
    lab: bool,
) -> NodeProc {
    spawn_node_env(dir, id, public, rpc, spec, lab, &[])
}

fn spawn_node_env(
    dir: &std::path::Path,
    id: &str,
    public: u16,
    rpc: u16,
    spec: &str,
    lab: bool,
    extra: &[(&str, &str)],
) -> NodeProc {
    let dir = dir.to_path_buf();
    std::fs::create_dir_all(&dir).unwrap();
    let out = std::fs::File::create(dir.join("node.log")).unwrap();
    let err = out.try_clone().unwrap();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rdsctl"));
    cmd.args([
        "serve",
        &format!("--node-id={id}"),
        &format!("--cluster={spec}"),
        &format!("--port={public}"),
        &format!("--rpc-port={rpc}"),
    ])
    .env("RDSCTL_MODE", "cluster")
    .env("RDSCTL_DATA_DIR", dir.join("data"))
    // 快速选举,缩短测试时间
    .env("RDSCTL_ELECTION_TIMEOUT_MS", "400")
    .env("RDSCTL_HA_TICK_MS", "25")
    // 默认不接 sink(探针模式):使集群用例与 MySQL 可用性解耦;
    // sink 相关的用例(I10-b)显式覆盖为 mysql。
    .env("RDSCTL_METADATA_SINK", "none")
    .stdout(out)
    .stderr(err);
    for (k, v) in extra {
        cmd.env(k, v);
    }
    if lab {
        cmd.env("RDSCTL_PREFLIGHT_ALLOW_UNVERIFIED_CLOCK", "1")
            .env("RDSCTL_ALLOW_NO_AGENT", "1");
    }
    let child = cmd.spawn().expect("启动 cluster 节点失败");
    let p = NodeProc {
        child,
        id: id.to_string(),
        public,
        rpc,
        dir,
        cleanup: true,
    };
    // 就绪:healthz 200
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        if let Ok((200, _)) = http(p.public, "GET", "/healthz", None, &[]) {
            return p;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("节点 {id} 未在 20s 内启动");
}

fn http(
    port: u16,
    method: &str,
    path: &str,
    body: Option<&str>,
    headers: &[(&str, &str)],
) -> Result<Resp, String> {
    let mut s = TcpStream::connect(("127.0.0.1", port)).map_err(|e| e.to_string())?;
    s.set_read_timeout(Some(Duration::from_secs(10))).ok();
    let payload = body.unwrap_or("");
    let mut req = format!(
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: {}\r\nConnection: close\r\n",
        payload.len()
    );
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
    req.push_str(payload);
    s.write_all(req.as_bytes()).map_err(|e| e.to_string())?;
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).map_err(|e| e.to_string())?;
    let text = String::from_utf8_lossy(&buf).into_owned();
    let status: u16 = text
        .split_whitespace()
        .nth(1)
        .and_then(|x| x.parse().ok())
        .ok_or_else(|| format!("响应无法解析:{text}"))?;
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();
    Ok((status, body))
}

fn node_status(rpc: u16) -> Option<serde_json::Value> {
    let (st, body) = http(rpc, "GET", "/internal/status", None, &[]).ok()?;
    if st != 200 {
        return None;
    }
    serde_json::from_str(&body).ok()
}

fn ready_of(public: u16) -> Option<serde_json::Value> {
    let (_, body) = http(public, "GET", "/readyz", None, &[]).ok()?;
    serde_json::from_str(&body).ok()
}

fn propose(rpc: u16, op_json: &str) -> Resp {
    http(rpc, "POST", "/internal/propose", Some(op_json), &[]).expect("propose 请求失败")
}

fn lease_grant_op(instance: &str, holder: &str, ttl_ms: u64) -> String {
    format!(
        r#"{{"op":"lease_grant","instance":"{instance}","holder":"{holder}","ttl_ms":{ttl_ms},"at_ms":0}}"#
    )
}

#[test]
fn f12_premise_failures_refuse_to_start_with_exit_code_2() {
    let root = std::env::temp_dir().join(format!("rdsctl-ha-f12-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();

    // 1) 缺 --node-id → 退出码 2
    let out = Command::new(env!("CARGO_BIN_EXE_rdsctl"))
        .args(["serve", "--cluster=n1@127.0.0.1:1,n2@127.0.0.1:2,n3@127.0.0.1:3"])
        .env("RDSCTL_MODE", "cluster")
        .env("RDSCTL_DATA_DIR", root.join("a"))
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2), "缺 node-id 必须以 2 退出");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("node-id"), "错误信息应指明原因:{err}");

    // 2) 偶数 voter → 退出码 2(结构性前提不可被 lab 放行)
    let out = Command::new(env!("CARGO_BIN_EXE_rdsctl"))
        .args([
            "serve",
            "--node-id=n1",
            "--cluster=n1@127.0.0.1:1,n2@127.0.0.1:2",
        ])
        .env("RDSCTL_MODE", "cluster")
        .env("RDSCTL_DATA_DIR", root.join("b"))
        .env("RDSCTL_PREFLIGHT_ALLOW_UNVERIFIED_CLOCK", "1")
        .env("RDSCTL_ALLOW_NO_AGENT", "1")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2), "偶数 voter 必须拒绝启动");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("奇") || err.contains("odd") || err.contains("voter"),
        "错误信息应说明 voter 问题:{err}"
    );

    // 3) 未放行时钟/agent → 退出码 2(前提 A1/A4 未验证不得静默启动)
    let out = Command::new(env!("CARGO_BIN_EXE_rdsctl"))
        .args([
            "serve",
            "--node-id=n1",
            "--cluster=n1@127.0.0.1:1,n2@127.0.0.1:2,n3@127.0.0.1:3",
        ])
        .env("RDSCTL_MODE", "cluster")
        .env("RDSCTL_DATA_DIR", root.join("c"))
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2), "未验证 A1/A4 必须拒绝启动");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("A1"), "应报出 A1 未验证:{err}");
    assert!(err.contains("A4"), "应报出 A4 未满足:{err}");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn f2_public_port_refuses_business_api_and_reports_premises() {
    let c = Cluster::start("public", 3);
    let n = c.node("n1");
    // 业务 API 未接线:必须 503 且说明原因(不假装可用)
    let (st, body) = http(n.public, "GET", "/api/rds/instances", None, &[]).unwrap();
    assert_eq!(st, 503, "cluster 模式业务 API 必须显式 503:{body}");
    assert!(body.contains("cluster"), "应说明是 cluster 模式未接线:{body}");
    // 内部端点不得从公开端口访问
    let (st, _) = http(n.public, "GET", "/internal/status", None, &[]).unwrap();
    assert_eq!(st, 403, "内部端点不得从公开端口暴露");
    // readyz 必须标注 lab 未验证前提(防止 lab 放行被当成生产就绪)
    let r = ready_of(n.public).expect("readyz 应可读");
    assert_eq!(r["premises_ok"], false, "lab 模式必须标注前提未验证:{r}");
    let unverified = r["premises_unverified"].as_array().unwrap();
    let names: Vec<String> = unverified
        .iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect();
    assert!(names.contains(&"A1_clock".to_string()), "{r}");
    assert!(names.contains(&"A4_agent_fence".to_string()), "{r}");
}

#[test]
fn f1_elects_leader_and_fails_over_on_kill() {
    let mut c = Cluster::start("f1", 3);
    let (leader, term1) = c.wait_single_leader(Duration::from_secs(20));
    // 等 follower 通过心跳学到 leader(否则它还不知道 leader 是谁,属正常 Raft 语义)
    let follower = c.ids().into_iter().find(|i| i != &leader).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let known = node_status(c.node(&follower).rpc)
            .and_then(|st| st["leader"].as_str().map(|s| s.to_string()));
        if known.as_deref() == Some(leader.as_str()) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "follower {follower} 未在 10s 内学到 leader({leader}),实际 {known:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    // 非 leader 提案必须被告知 leader(F1 可用性语义)
    let (st, body) = propose(c.node(&follower).rpc, &lease_grant_op("i1", "h1", 30_000));
    assert_eq!(st, 409, "非 leader 必须拒绝写入并指路:{body}");
    assert!(body.contains("not_leader"), "{body}");
    assert!(
        body.contains(&leader),
        "错误里应带当前 leader({leader}):{body}"
    );

    // leader 提案 → 应用
    let (st, body) = propose(
        c.node(&leader).rpc,
        &lease_grant_op("i1", "h1", 30_000),
    );
    assert_eq!(st, 200, "leader 提案应成功:{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["ok"], true);
    assert_eq!(v["applied"]["status"], "applied", "应被状态机接受:{body}");

    // 杀掉 leader → 其余两节点应选出新 leader(新的 term)
    let leader_rpc = c.node(&leader).rpc;
    c.kill(&leader);
    let deadline = Instant::now() + Duration::from_secs(25);
    let mut new_leader = None;
    while Instant::now() < deadline {
        let mut ls = Vec::new();
        for n in &c.nodes {
            if let Some(st) = node_status(n.rpc) {
                if st["role"] == "leader" {
                    ls.push((n.id.clone(), st["term"].as_u64().unwrap_or(0)));
                }
            }
        }
        if ls.len() == 1 && ls[0].1 > term1 {
            new_leader = Some(ls.remove(0));
            break;
        }
        std::thread::sleep(Duration::from_millis(120));
    }
    let (nl, term2) = new_leader.expect("杀掉 leader 后应选出新 leader 且 term 更高");
    assert!(term2 > term1, "新 leader 的 term 必须更高({term2} > {term1})");
    assert_ne!(nl, leader);

    // 新 leader 上租约仍可见(状态机已复制)
    let (st, body) = http(
        c.node(&nl).rpc,
        "GET",
        "/internal/lease?instance=i1",
        None,
        &[],
    )
    .unwrap();
    assert_eq!(st, 200, "新 leader 应能看到已提交的租约:{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["lease"]["holder"], "h1");
    let _ = leader_rpc;
}

#[test]
fn f2_minority_cannot_commit_and_readyz_degrades() {
    let mut c = Cluster::start("minority", 3);
    let (leader, _) = c.wait_single_leader(Duration::from_secs(20));
    let others: Vec<String> = c.ids().into_iter().filter(|i| i != &leader).collect();
    for o in &others {
        c.kill(o);
    }
    // 失去多数派:propose 必须 503 quorum_unavailable(绝不"本地写入成功")
    let (st, body) = propose(
        c.node(&leader).rpc,
        &lease_grant_op("solo", "h", 30_000),
    );
    assert_eq!(st, 503, "少数派不得接受写入:{body}");
    assert!(body.contains("quorum_unavailable"), "{body}");
    // /readyz 必须降级并给出原因
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut degraded = None;
    while Instant::now() < deadline {
        if let Some(r) = ready_of(c.node(&leader).public) {
            if r["quorum_ok"] == false {
                degraded = Some(r);
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(150));
    }
    let r = degraded.expect("/readyz 应在失去多数派后降级");
    assert_eq!(r["ready"], false, "{r}");
    assert_eq!(r["degraded_reason"], "quorum_unavailable", "{r}");
}

#[test]
fn i1_lease_single_writer_and_fence_monotonic() {
    let c = Cluster::start("lease", 3);
    let (leader, _) = c.wait_single_leader(Duration::from_secs(20));
    let rpc = c.node(&leader).rpc;

    // 第一次授予:h1
    let (st, body) = propose(rpc, &lease_grant_op("db1", "h1", 30_000));
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["applied"]["status"], "applied", "{body}");
    let fence1 = v["applied"].clone();
    let _ = fence1;

    let (st, body) = http(rpc, "GET", "/internal/lease?instance=db1", None, &[]).unwrap();
    assert_eq!(st, 200);
    let l: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(l["lease"]["holder"], "h1");
    let f1 = l["lease"]["fence"].as_str().unwrap().to_string();

    // 立即被他人抢占:必须被拒(旧租约未过期 + 未过 skew 边界)
    let (st, body) = propose(rpc, &lease_grant_op("db1", "h2", 30_000));
    assert_eq!(st, 200, "提案本身成功,但状态机应拒绝:{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["applied"]["status"], "rejected", "第二次授予必须被拒:{body}");
    let reason = v["applied"]["reason"].as_str().unwrap_or("");
    assert!(reason.contains("旧租约尚未安全过期"), "拒绝原因应说明安全边界:{reason}");

    // 持有者续约成功,fence 前移(单调)
    let renew = r#"{"op":"lease_renew","instance":"db1","holder":"h1","at_ms":0}"#;
    let (st, body) = propose(rpc, &renew);
    assert_eq!(st, 200);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["applied"]["status"], "applied", "{body}");

    let (_, body) = http(rpc, "GET", "/internal/lease?instance=db1", None, &[]).unwrap();
    let l: serde_json::Value = serde_json::from_str(&body).unwrap();
    let f2 = l["lease"]["fence"].as_str().unwrap().to_string();
    assert_eq!(l["lease"]["holder"], "h1");
    assert_ne!(f1, f2, "续约后 fence 必须前移");
    assert!(fence_gt(&f2, &f1), "fence 必须单调递增:{f1} → {f2}");
    assert_eq!(l["lease"]["renewals"], 1);

    // 非持有者续约:拒绝
    let bad = r#"{"op":"lease_renew","instance":"db1","holder":"h2","at_ms":0}"#;
    let (_, body) = propose(rpc, &bad);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["applied"]["status"], "rejected", "非持有者续约必须被拒:{body}");

    // 释放后他人可立即接管(无需等待 skew)
    let rel = r#"{"op":"lease_release","instance":"db1","holder":"h1"}"#;
    let (_, body) = propose(rpc, &rel);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["applied"]["status"], "applied", "{body}");
    let (_, body) = propose(rpc, &lease_grant_op("db1", "h2", 30_000));
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        v["applied"]["status"], "applied",
        "释放后应立即允许接管:{body}"
    );
    let (_, body) = http(rpc, "GET", "/internal/lease?instance=db1", None, &[]).unwrap();
    let l: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(l["lease"]["holder"], "h2");
    assert!(
        fence_gt(
            l["lease"]["fence"].as_str().unwrap(),
            &f2
        ),
        "接管后的 fence 必须高于前任"
    );
}

/// 比较 fence 线格式(shard:term:index)的字典序
fn fence_gt(a: &str, b: &str) -> bool {
    let parse = |s: &str| -> (u64, u64, u64) {
        let mut it = s.split(':');
        (
            it.next().and_then(|x| x.parse().ok()).unwrap_or(0),
            it.next().and_then(|x| x.parse().ok()).unwrap_or(0),
            it.next().and_then(|x| x.parse().ok()).unwrap_or(0),
        )
    };
    parse(a) > parse(b)
}

// ───────────────────────── I10:副本启动不得破坏他人状态 ─────────────────────────

/// I10-a(共识权威):副本启动/重启不得清掉他人持有的共识租约。
///
/// 对照组是今日缺陷:`Store::clear_all_locks()` 是无条件 `DELETE FROM instance_locks`,
/// 任何副本一启动就把别人的锁清空。cluster 模式下该动作被门禁禁止(设计 C5/I10)。
#[test]
fn i10_replica_restart_does_not_clear_consensus_leases() {
    let mut c = Cluster::start("i10a", 3);
    let (leader, _) = c.wait_single_leader(Duration::from_secs(20));
    // 持有者 h1 取得实例 i1 的租约
    let (st, body) = propose(c.node(&leader).rpc, &lease_grant_op("i1", "h1", 30_000));
    assert_eq!(st, 200, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["applied"]["status"], "applied", "{body}");
    let (_, body) = http(
        c.node(&leader).rpc,
        "GET",
        "/internal/lease?instance=i1",
        None,
        &[],
    )
    .unwrap();
    let before: serde_json::Value = serde_json::from_str(&body).unwrap();
    let fence_before = before["lease"]["fence"].as_str().unwrap().to_string();

    // 重启一个非 leader 副本(它会完整走一遍自己的启动路径)
    let victim = c
        .ids()
        .into_iter()
        .find(|i| i != &leader)
        .expect("应存在非 leader 副本");
    c.restart(&victim);
    std::thread::sleep(Duration::from_millis(600));

    // 断言:租约仍是 h1、fence 未变(未被新副本清空/抢占/重置)
    let (st, body) = http(
        c.node(&leader).rpc,
        "GET",
        "/internal/lease?instance=i1",
        None,
        &[],
    )
    .unwrap();
    assert_eq!(st, 200, "租约必须仍存在:{body}");
    let after: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        after["lease"]["holder"], "h1",
        "副本重启不得改变他人租约的持有者(design C5/I10)"
    );
    assert_eq!(
        after["lease"]["fence"].as_str().unwrap(),
        fence_before,
        "副本重启不得重置 fence(否则旧持有者的命令会重新变为有效)"
    );

    // 且原持有者仍可续约(未被"孤儿锁清理"误伤)
    let renew = r#"{"op":"lease_renew","instance":"i1","holder":"h1","at_ms":0}"#;
    let (_, body) = propose(c.node(&leader).rpc, renew);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["applied"]["status"], "applied", "原持有者应仍可续约:{body}");
}

/// 起一个节点但用"任意成员表"(用于 I10-a 的第三个副本场景)
fn spawn_node_raw(
    root: &std::path::Path,
    id: &str,
    public: u16,
    rpc: u16,
    spec: &str,
    lab: bool,
) -> NodeProc {
    // 与 spawn_node 相同,但不等待 healthz(新成员可能不达多数派或成员表不一致)
    let dir = root.join(format!("{id}-raw"));
    std::fs::create_dir_all(&dir).unwrap();
    let out = std::fs::File::create(dir.join("node.log")).unwrap();
    let err = out.try_clone().unwrap();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rdsctl"));
    cmd.args([
        "serve",
        &format!("--node-id={id}"),
        &format!("--cluster={spec}"),
        &format!("--port={public}"),
        &format!("--rpc-port={rpc}"),
    ])
    .env("RDSCTL_MODE", "cluster")
    .env("RDSCTL_DATA_DIR", dir.join("data"))
    .env("RDSCTL_ELECTION_TIMEOUT_MS", "400")
    .env("RDSCTL_HA_TICK_MS", "25")
    .env("RDSCTL_METADATA_SINK", "none")
    .stdout(out)
    .stderr(err);
    if lab {
        cmd.env("RDSCTL_PREFLIGHT_ALLOW_UNVERIFIED_CLOCK", "1")
            .env("RDSCTL_ALLOW_NO_AGENT", "1");
    }
    let child = cmd.spawn().expect("启动节点失败");
    NodeProc {
        child,
        id: id.to_string(),
        public,
        rpc,
        dir,
        cleanup: true,
    }
}

/// 直接用 mysql CLI 执行 SQL(供 sink 侧断言/准备)
fn mysql_exec(db: &str, sql: &str) -> Result<String, String> {
    let cli = std::env::var("RDSCTL_MYSQL_CLI").unwrap_or_else(|_| "mysql".to_string());
    let host = std::env::var("RDSCTL_MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = std::env::var("RDSCTL_MYSQL_PORT").unwrap_or_else(|_| "3306".to_string());
    let user = std::env::var("RDSCTL_MYSQL_USER").unwrap_or_else(|_| "root".to_string());
    let pass = std::env::var("RDSCTL_MYSQL_PASS").unwrap_or_default();
    let mut cmd = Command::new(cli);
    cmd.args([
        "-h", &host, "-P", &port, "-u", &user, "--protocol=tcp", "--batch", "--raw",
        "--skip-column-names", "--connect-timeout=5", db, "-e", sql,
    ]);
    if !pass.is_empty() {
        cmd.env("MYSQL_PWD", pass);
    }
    let out = cmd.output().map_err(|e| format!("mysql 执行失败:{e}"))?;
    if !out.status.success() {
        return Err(String::from_utf8_lossy(&out.stderr).trim().to_string());
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// I10-b(sink 侧):cluster 模式副本启动**不得**执行 `mark_interrupted`/`clear_all_locks`,
/// 即不得把其它副本正在跑的任务标 failed,也不得清掉他人租约行。
///
/// 需要真实 MySQL(与其他验收用例一致);用例自建独立库,跑完即删。
#[test]
fn i10_cluster_start_does_not_wipe_sink_locks_or_tasks() {
    // MySQL 不可用则跳过(不静默通过:打印明确原因)
    if mysql_exec("mysql", "SELECT 1").is_err() && mysql_exec("", "SELECT 1").is_err() {
        eprintln!("跳过 I10-b:本机 MySQL 不可用");
        return;
    }
    let db = format!("rdsctl_ha_i10_{}", std::process::id());
    let _ = mysql_exec("", &format!("DROP DATABASE IF EXISTS {db}"));
    mysql_exec("", &format!("CREATE DATABASE {db}"))
        .unwrap_or_else(|e| panic!("建库失败:{e}"));

    let root = std::env::temp_dir().join(format!("rdsctl-ha-i10b-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();

    // 先手动建表并种入"他人持有的锁 + 在跑任务"
    let ddl_ok = mysql_exec(
        &db,
        "CREATE TABLE IF NOT EXISTS instance_locks (name VARCHAR(96) PRIMARY KEY, holder VARCHAR(128) NOT NULL, lease_until BIGINT NOT NULL, updated_at BIGINT NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
         CREATE TABLE IF NOT EXISTS tasks (id VARCHAR(96) PRIMARY KEY, kind VARCHAR(32) NOT NULL, instance VARCHAR(96) NOT NULL, status VARCHAR(16) NOT NULL, created_at BIGINT NOT NULL, started_at BIGINT NULL, finished_at BIGINT NULL, KEY idx_tasks_instance (instance)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
         INSERT INTO instance_locks(name,holder,lease_until,updated_at) VALUES('i-seed','other-controller',9999999999,1) ON DUPLICATE KEY UPDATE holder='other-controller';
         INSERT INTO tasks(id,kind,instance,status,created_at,started_at,finished_at) VALUES('t-seed','create','i-seed','running',1,1,NULL) ON DUPLICATE KEY UPDATE status='running';",
    );
    assert!(ddl_ok.is_ok(), "准备 sink 数据失败:{ddl_ok:?}");

    // 起一个 cluster 节点(sink=mysql,指向该库)
    let public = free_port();
    let rpc = free_port();
    let spec = format!("n1@127.0.0.1:{rpc},n2@127.0.0.1:{},n3@127.0.0.1:{}", free_port(), free_port());
    let dir = root.join("n1");
    std::fs::create_dir_all(&dir).unwrap();
    let out = std::fs::File::create(dir.join("node.log")).unwrap();
    let err = out.try_clone().unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_rdsctl"))
        .args([
            "serve",
            "--node-id=n1",
            &format!("--cluster={spec}"),
            &format!("--port={public}"),
            &format!("--rpc-port={rpc}"),
        ])
        .env("RDSCTL_MODE", "cluster")
        .env("RDSCTL_DATA_DIR", dir.join("data"))
        .env("RDSCTL_MYSQL_DB", &db)
        .env("RDSCTL_METADATA_SINK", "mysql")
        .env("RDSCTL_PREFLIGHT_ALLOW_UNVERIFIED_CLOCK", "1")
        .env("RDSCTL_ALLOW_NO_AGENT", "1")
        .env("RDSCTL_ELECTION_TIMEOUT_MS", "400")
        .env("RDSCTL_HA_TICK_MS", "25")
        .stdout(out)
        .stderr(err)
        .spawn()
        .expect("启动 cluster 节点失败");

    // 等它起来(healthz 可达)
    let deadline = Instant::now() + Duration::from_secs(25);
    let mut up = false;
    while Instant::now() < deadline {
        if let Ok((200, _)) = http(public, "GET", "/healthz", None, &[]) {
            up = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(150));
    }
    assert!(up, "cluster 节点未在 25s 内启动");

    // 断言:种子数据未被启动动作破坏
    let lock = mysql_exec(&db, "SELECT holder FROM instance_locks WHERE name='i-seed'")
        .unwrap_or_default();
    assert_eq!(
        lock.trim(),
        "other-controller",
        "其它控制端的租约行不得被新副本清掉(clear_all_locks 必须被门禁禁止)"
    );
    let task = mysql_exec(&db, "SELECT status FROM tasks WHERE id='t-seed'").unwrap_or_default();
    assert_eq!(
        task.trim(),
        "running",
        "在跑任务不得被新副本标记 failed(mark_interrupted 必须被门禁禁止)"
    );

    // 完整 API 已接线(无 cookie → 401),但**登录本身必须 fail-closed**:
    // cluster 模式下建立会话要写入共识状态机 ⇒ 需要多数派;本用例只起了 3 voter 中的 1 个,
    // 属于少数派,因此登录必须是 503。若这里返回 200,就意味着"无多数派时也能凭空签发
    // 一个谁都撤销不掉的会话"(设计 C8 明确禁止)。
    let (probe_status, _) = http(public, "GET", "/api/auth/me", None, &[]).unwrap();
    assert_eq!(probe_status, 401, "API 应已接线(无 cookie → 401)");
    let (login_status, login_body) = http(
        public,
        "POST",
        "/login",
        Some("user=admin&password=admin"),
        &[],
    )
    .unwrap();
    assert_eq!(
        login_status, 503,
        "少数派下必须拒绝建立会话(fail-closed);实际 {}", login_body
    );

    let _ = child.kill();
    let _ = child.wait();
    let _ = mysql_exec("", &format!("DROP DATABASE IF EXISTS {db}"));
    let _ = std::fs::remove_dir_all(&root);
}

/// F11(守护拉起)+ I11(进程级:崩溃重启后由日志/硬状态恢复并重新加入)
///
/// 语义等价于 systemd `Restart=always` / launchd `KeepAlive`:进程被 kill -9 后,
/// 由守护用**同一 node-id / 数据目录 / 端口**拉起。CI 无 systemd 时,由测试充当守护
/// (设计 §17.1 R2 允许)。
#[test]
fn f11_supervised_restart_rejoins_and_recovers_state() {
    let mut c = Cluster::start("f11", 3);
    let (leader, term) = c.wait_single_leader(Duration::from_secs(20));

    // 先落一条已提交状态(租约),以便验证重启节点能追平
    let (st, body) = propose(c.node(&leader).rpc, &lease_grant_op("db-f11", "h1", 30_000));
    assert_eq!(st, 200, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["applied"]["status"], "applied", "{body}");

    // 挑一个非 leader 副本,模拟守护重启(同目录、同端口)
    let victim = c
        .ids()
        .into_iter()
        .find(|i| i != &leader)
        .expect("应存在非 leader 副本");
    c.restart(&victim);

    // 断言 1:在超时内重新可用(守护拉起语义)
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut st_after = None;
    while Instant::now() < deadline {
        if let Some(st) = node_status(c.node(&victim).rpc) {
            st_after = Some(st);
            break;
        }
        std::thread::sleep(Duration::from_millis(150));
    }
    let st_after = st_after.expect("重启副本应在 20s 内重新可用");

    // 断言 2:term 不倒退(硬状态从磁盘恢复,而不是从 0 重新开始)
    let t_after = st_after["term"].as_u64().unwrap_or(0);
    assert!(
        t_after >= term,
        "重启后 term 不得倒退:重启前 leader term={term},重启后该节点 term={t_after}"
    );

    // 断言 3:状态机追平(重启节点能看到已提交的租约)
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut caught_up = false;
    while Instant::now() < deadline {
        if let Ok((200, body)) = http(
            c.node(&victim).rpc,
            "GET",
            "/internal/lease?instance=db-f11",
            None,
            &[],
        ) {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&body) {
                if v["lease"]["holder"] == "h1" {
                    caught_up = true;
                    break;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(150));
    }
    assert!(caught_up, "重启副本应在 15s 内由日志追平到已提交状态");

    // 断言 4:集群仍能提交(重启副本不影响多数派可用性)
    let (st, body) = propose(c.node(&leader).rpc, &lease_grant_op("db-f11-b", "h2", 30_000));
    assert_eq!(st, 200, "重启一个副本后集群仍应可提交:{body}");
}


// ───────────────────────── F10:滚动升级 ─────────────────────────

impl Cluster {
    /// 停掉一个副本但保留数据目录与节点记录(模拟升级停服)
    fn stop(&mut self, id: &str) {
        if let Some(n) = self.nodes.iter_mut().find(|n| n.id == id) {
            let _ = n.child.kill();
            let _ = n.child.wait();
            n.cleanup = false;
        }
    }

    /// 用同一 id / 端口 / 数据目录重新拉起(模拟升级后启动新版本)
    fn start_stopped(&mut self, id: &str) {
        let idx = self
            .nodes
            .iter()
            .position(|n| n.id == id)
            .expect("节点不存在");
        let (public, rpc, dir) = {
            let n = &self.nodes[idx];
            (n.public, n.rpc, n.dir.clone())
        };
        {
            let mut old = self.nodes.remove(idx);
            let _ = old.child.try_wait();
        }
        let spec = self.spec.clone();
        let p = spawn_node_at(&dir, id, public, rpc, &spec, true);
        self.nodes.push(p);
    }

    /// 诊断:整体状态快照(失败信息里带上,便于定位)
    fn dump(&self) -> String {
        let mut out = Vec::new();
        for n in &self.nodes {
            let st = node_status(n.rpc);
            out.push(format!(
                "{}: role={} term={} leader={:?} ready={:?}",
                n.id,
                st.as_ref().and_then(|v| v["role"].as_str()).unwrap_or("<down>"),
                st.as_ref().and_then(|v| v["term"].as_u64()).unwrap_or(0),
                st.as_ref().and_then(|v| v["leader"].as_str()),
                ready_of(n.public).map(|r| r["ready"].clone()),
            ));
        }
        out.join(" | ")
    }

    fn node_status_opt(&self, id: &str) -> Option<serde_json::Value> {
        self.nodes
            .iter()
            .find(|n| n.id == id)
            .and_then(|n| node_status(n.rpc))
    }

    fn is_leader(&self, id: &str) -> bool {
        node_status(self.node(id).rpc)
            .map(|st| st["role"] == "leader")
            .unwrap_or(false)
    }

    fn current_leader(&self) -> Option<String> {
        self.nodes.iter().find_map(|n| {
            node_status(n.rpc)
                .filter(|st| st["role"] == "leader")
                .map(|_| n.id.clone())
        })
    }
}

/// F10 滚动升级:逐副本 stepdown → kill -9 → 重新拉起(同一 node-id/目录/端口)。
///
/// 断言:①任一时刻同一 term 至多一个 leader;②升级前已提交的状态在升级后各副本都还在;
/// ③每步之后集群仍可提交(说明多数派始终可用);④`config_epoch` 不变。
#[test]
fn f10_rolling_upgrade_preserves_state_and_availability() {
    let mut c = Cluster::start("f10", 3);
    let (leader, _) = c.wait_single_leader(Duration::from_secs(20));

    // 升级前写入可校验状态:两条租约
    for (inst, holder) in [("db-r1", "h1"), ("db-r2", "h2")] {
        let (st, body) = propose(c.node(&leader).rpc, &lease_grant_op(inst, holder, 30_000));
        assert_eq!(st, 200, "{body}");
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["applied"]["status"], "applied", "{body}");
    }
    let epoch_before = node_status(c.node(&leader).rpc).unwrap()["config_epoch"].clone();

    // 逐个副本升级:非 leader 先、leader 最后(标准滚动顺序)
    let ids = c.ids();
    let mut order: Vec<String> = ids.iter().filter(|i| **i != leader).cloned().collect();
    order.push(leader.clone());

    for (step, id) in order.iter().enumerate() {
        // 若目标当前是 leader:先优雅让位(避免写空窗)
        if c.is_leader(id) {
            let (st, body) = http(c.node(id).rpc, "POST", "/internal/stepdown", Some("{}"), &[])
                .unwrap();
            assert_eq!(st, 200, "stepdown 应成功:{body}");
            let v: serde_json::Value = serde_json::from_str(&body).unwrap();
            assert_eq!(v["stepped_down"], true, "leader 应报告已让位:{body}");
        }
        // 停服(用 kill -9 模拟最坏情况)再拉起
        c.stop(id);
        std::thread::sleep(Duration::from_millis(150));
        // 其余节点应能重新选出 leader 并继续提交(多数派仍在)
        let mut new_leader = None;
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline {
            if let Some(l) = c.current_leader() {
                if l != *id {
                    new_leader = Some(l);
                    break;
                }
            }
            std::thread::sleep(Duration::from_millis(120));
        }
        let nl = new_leader.unwrap_or_else(|| panic!("第 {} 步({id})后未选出新 leader", step + 1));
        let (st, body) = propose(
            c.node(&nl).rpc,
            &lease_grant_op(&format!("db-up{step}"), "hu", 30_000),
        );
        assert_eq!(st, 200, "第 {} 步后集群应仍可提交:{body}", step + 1);

        c.start_stopped(id);
        // 等它重新可用
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut up = false;
        while Instant::now() < deadline {
            if let Ok((200, _)) = http(c.node(id).public, "GET", "/healthz", None, &[]) {
                up = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(150));
        }
        assert!(up, "第 {} 步({id})拉起后未在 20s 内可用", step + 1);
        // 全程无脑裂
        assert_at_most_one_leader_per_term_proc(&c);
    }

    // 升级后:所有副本都应能看到升级前提交的租约(状态未丢)
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut all_seen = false;
    while Instant::now() < deadline {
        let mut ok_all = true;
        for id in c.ids() {
            match http(
                c.node(&id).rpc,
                "GET",
                "/internal/lease?instance=db-r1",
                None,
                &[],
            ) {
                Ok((200, body)) => {
                    let v: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
                    if v["lease"]["holder"] != "h1" {
                        ok_all = false;
                    }
                }
                _ => ok_all = false,
            }
        }
        if ok_all {
            all_seen = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    assert!(all_seen, "滚动升级后所有副本都应保有升级前已提交的状态");

    let l = c.current_leader().expect("升级完成后应有 leader");
    assert_eq!(
        node_status(c.node(&l).rpc).unwrap()["config_epoch"],
        epoch_before,
        "滚动升级不得改变 config_epoch"
    );
}

/// 进程级脑裂检测:同一 term 至多一个 leader
fn assert_at_most_one_leader_per_term_proc(c: &Cluster) {
    use std::collections::BTreeMap;
    let mut by_term: BTreeMap<u64, Vec<String>> = BTreeMap::new();
    for n in &c.nodes {
        if let Some(st) = node_status(n.rpc) {
            if st["role"] == "leader" {
                by_term
                    .entry(st["term"].as_u64().unwrap_or(0))
                    .or_default()
                    .push(n.id.clone());
            }
        }
    }
    for (t, ls) in by_term {
        assert_eq!(ls.len(), 1, "INV-1 违反:term={t} 出现多个 leader {ls:?}");
    }
}

// ───────────────── I5 多数派恢复自愈 / I6+F4 SIGSTOP 旧 holder ─────────────────

impl Cluster {
    /// 重启指定副本集合(同一 id/端口/数据目录)
    fn restart_many(&mut self, ids: &[String]) {
        for id in ids {
            self.restart(id);
        }
    }

    fn wait_ready(&self, id: &str, timeout: Duration) -> serde_json::Value {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(r) = ready_of(self.node(id).public) {
                if r["ready"] == serde_json::Value::Bool(true) {
                    return r;
                }
            }
            assert!(
                Instant::now() < deadline,
                "节点 {id} 未在 {:?} 内就绪",
                timeout
            );
            std::thread::sleep(Duration::from_millis(200));
        }
    }
}

fn sig(pid: u32, sig: &str) {
    let st = Command::new("kill")
        .args([format!("-{sig}"), pid.to_string()])
        .status()
        .expect("kill 调用失败");
    assert!(st.success(), "kill -{sig} {pid} 失败");
}

/// I5:失去多数派 → 停写并降级;恢复多数派 → **自动自愈**(无需重启进程)并可继续提交。
#[test]
fn i5_quorum_recovery_heals_automatically() {
    let mut c = Cluster::start("i5", 3);
    let (leader, _) = c.wait_single_leader(Duration::from_secs(20));
    let others: Vec<String> = c.ids().into_iter().filter(|i| i != &leader).collect();

    // 先确认健康:能提交
    let (st, _) = propose(c.node(&leader).rpc, &lease_grant_op("i5-a", "h", 30_000));
    assert_eq!(st, 200);

    // 失去多数派(停掉两台;用 stop 保留节点记录以便随后拉起)
    for o in &others {
        c.stop(o);
    }
    let alive = {
        let ids = c.ids();
        let alive: Vec<String> = ids
            .into_iter()
            .filter(|i| c.node_status_opt(i).is_some())
            .collect();
        assert_eq!(alive.len(), 1, "应只剩一台存活");
        alive[0].clone()
    };
    // readyz 必须降级,且写明原因
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut degraded = None;
    while Instant::now() < deadline {
        if let Some(r) = ready_of(c.node(&alive).public) {
            if r["quorum_ok"] == false {
                degraded = Some(r);
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(150));
    }
    let r = degraded.expect("失去多数派后 readyz 必须降级");
    assert_eq!(r["ready"], false, "{r}");
    let reasons: Vec<String> = r["degraded_reasons"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|v| v.as_str().map(|s| s.to_string()))
        .collect();
    assert!(reasons.contains(&"quorum_unavailable".to_string()), "{r}");

    // 恢复多数派:进程**不重启**,应自动回到就绪状态
    c.restart_many(&others);
    // 重启后 leader 可能已换;等出现最高 term 上的唯一 leader
    let (nl, _) = c.wait_single_leader(Duration::from_secs(30));
    let ready = c.wait_ready(&nl, Duration::from_secs(30));
    assert_eq!(ready["quorum_ok"], true, "多数派恢复后应自愈:{ready}");
    // 自愈后应能继续提交(向当前 leader 提案,容忍抖动)
    let (st, body) = c.propose_to_leader(&lease_grant_op("i5-b", "h", 30_000), Duration::from_secs(30));
    assert_eq!(st, 200, "自愈后应可继续提交:{body}");
}

/// I6 + F4:SIGSTOP 旧 leader(等价 GC 长停顿/挂起)→ 其余节点接管 →
/// SIGCONT 恢复后:①不得出现两个 leader;②旧节点不再自认 leader;
/// ③新持有者的 fence 高于旧值(旧 holder 的命令会被执行面按 fence 拒绝,见 I7)。
#[test]
fn i6_f4_stopped_holder_is_superseded_and_fenced() {
    let mut c = Cluster::start("i6", 3);
    let (leader, term0) = c.wait_single_leader(Duration::from_secs(20));

    // 建立"旧持有者":短 TTL(10s,= 10×max_skew 下限),便于在测试期内跨过安全边界
    let (st, body) = propose(c.node(&leader).rpc, &lease_grant_op("i6-db", "h-old", 10_000));
    assert_eq!(st, 200, "{body}");
    let (_, body) = http(
        c.node(&leader).rpc,
        "GET",
        "/internal/lease?instance=i6-db",
        None,
        &[],
    )
    .unwrap();
    let before: serde_json::Value = serde_json::from_str(&body).unwrap();
    let fence_old = before["lease"]["fence"].as_str().unwrap().to_string();

    // SIGSTOP 旧 leader(它既不能心跳也不能提交)
    let pid = c.node(&leader).child.id();
    sig(pid, "STOP");

    // 其余两节点应在选举超时后选出新 leader(term 更高)
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut new_leader = None;
    while Instant::now() < deadline {
        let mut ls = Vec::new();
        for n in &c.nodes {
            if n.id == leader {
                continue;
            }
            if let Some(st) = node_status(n.rpc) {
                if st["role"] == "leader" && st["term"].as_u64().unwrap_or(0) > term0 {
                    ls.push((n.id.clone(), st["term"].as_u64().unwrap_or(0)));
                }
            }
        }
        if ls.len() == 1 {
            new_leader = Some(ls.remove(0));
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    let (nl, term1) = new_leader.expect("挂起旧 leader 后其余节点应选出新 leader");
    assert_ne!(nl, leader);
    assert!(term1 > term0);

    // 越过"旧租约过期 + 一个 max_skew"安全边界后,由新 leader 授予新持有者
    let wait_ms = 10_000 + 1_000 + 500; // TTL + max_skew + 余量
    std::thread::sleep(Duration::from_millis(wait_ms));
    let (st, body) = c.propose_to_leader(&lease_grant_op("i6-db", "h-new", 30_000), Duration::from_secs(30));
    assert_eq!(st, 200, "新 leader 应可提交:{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["applied"]["status"], "applied", "越过安全边界后应可接管:{body}");

    let (_, body) = http(
        c.node(&nl).rpc,
        "GET",
        "/internal/lease?instance=i6-db",
        None,
        &[],
    )
    .unwrap();
    let after: serde_json::Value = serde_json::from_str(&body).unwrap();
    let fence_new = after["lease"]["fence"].as_str().unwrap().to_string();
    assert_eq!(after["lease"]["holder"], "h-new");
    assert!(
        fence_gt(&fence_new, &fence_old),
        "接管后的 fence({fence_new})必须高于旧持有者({fence_old})—— 旧 holder 的命令据此被拒"
    );

    // SIGCONT 恢复旧 leader:不得出现两个 leader,旧节点必须让位
    sig(pid, "CONT");
    std::thread::sleep(Duration::from_secs(3));
    assert_at_most_one_leader_per_term_proc(&c);
    assert!(
        !c.is_leader(&leader),
        "恢复后的旧 leader 必须已让位(不得同 term 双主)"
    );
    // 旧节点仍可服务:内部 RPC 端点**刻意不转发**(避免成环),因此对非 leader 的
    // `/internal/propose` 必须返回**可用的重定向信息**(not_leader + 真实 leader),
    // 让调用方能立刻改投;而业务/权威路径(`propose_op`)会自动转发给 leader
    // —— 该转发能力由集群端到端用例 `acceptance::cluster_lifecycle_*` 证明
    //    (create 落在 follower 上仍成功)。
    let (st, body) = propose(c.node(&leader).rpc, &lease_grant_op("i6-db2", "h", 30_000));
    assert_eq!(st, 409, "非 leader 的内部提案端点应返回可重定向的 409:{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["error"], "not_leader", "{body}");
    let actual = c.current_leader().expect("应存在 leader");
    assert_eq!(
        v["leader"].as_str(),
        Some(actual.as_str()),
        "重定向信息必须指向真实 leader({actual}):{body}"
    );
}

/// I18 + I19:sink 投影(决策审计)落库幂等,且**可从日志重建**(重置游标后重放不产生重复)。
#[test]
fn i18_i19_sink_projection_is_idempotent_and_rebuildable() {
    // 需要真实 MySQL
    if mysql_exec("", "SELECT 1").is_err() {
        eprintln!("跳过 I18/I19:本机 MySQL 不可用");
        return;
    }
    let db = format!("rdsctl_ha_proj_{}", std::process::id());
    let _ = mysql_exec("", &format!("DROP DATABASE IF EXISTS {db}"));
    mysql_exec("", &format!("CREATE DATABASE {db}")).unwrap_or_else(|e| panic!("建库失败:{e}"));

    let root = std::env::temp_dir().join(format!("rdsctl-ha-proj-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();

    let n = 3usize;
    let rpc_ports: Vec<u16> = (0..n).map(|_| free_port()).collect();
    let pub_ports: Vec<u16> = (0..n).map(|_| free_port()).collect();
    let spec = (0..n)
        .map(|i| format!("n{}@127.0.0.1:{}", i + 1, rpc_ports[i]))
        .collect::<Vec<_>>()
        .join(",");
    let mut nodes = Vec::new();
    for i in 0..n {
        let id = format!("n{}", i + 1);
        let dir = root.join(&id);
        let p = spawn_node_env(
            &dir,
            &id,
            pub_ports[i],
            rpc_ports[i],
            &spec,
            true,
            &[
                ("RDSCTL_METADATA_SINK", "mysql"),
                ("RDSCTL_MYSQL_DB", &db),
            ],
        );
        nodes.push(p);
    }
    let root_data_dir = root.join("n1"); // 与被查节点一致(游标文件在该目录下)

    // 等 leader
    let deadline = Instant::now() + Duration::from_secs(25);
    let mut leader_rpc = None;
    while Instant::now() < deadline {
        let mut ls = Vec::new();
        for p in &nodes {
            if let Some(st) = node_status(p.rpc) {
                if st["role"] == "leader" {
                    ls.push(p.rpc);
                }
            }
        }
        if ls.len() == 1 {
            leader_rpc = Some(ls[0]);
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    let rpc = leader_rpc.expect("应选出 leader");

    // 提交两条决策(租约授予 + 续约)→ 应被投影到 sink
    let (st, body) = propose(rpc, &lease_grant_op("p-db", "h1", 30_000));
    assert_eq!(st, 200, "{body}");
    let renew = r#"{"op":"lease_renew","instance":"p-db","holder":"h1","at_ms":0}"#;
    let (st, body) = propose(rpc, renew);
    assert_eq!(st, 200, "{body}");

    let count_marker_rows = |db: &str| -> i64 {
        mysql_exec(
            db,
            "SELECT COUNT(*) FROM audit_log WHERE params LIKE 'proj:%'",
        )
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(-1)
    };

    // 等投影落库(对账循环 5s 一轮)
    let deadline = Instant::now() + Duration::from_secs(40);
    let mut first = -1i64;
    while Instant::now() < deadline {
        let c = count_marker_rows(&db);
        if c >= 2 {
            first = c;
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    assert!(first >= 2, "决策应在 40s 内被投影到 sink(实际 {first} 行)");
    // 内容抽查:租约决策带 holder 与幂等标记
    let sample = mysql_exec(
        &db,
        "SELECT action, params FROM audit_log WHERE params LIKE 'proj:%' ORDER BY id LIMIT 1",
    )
    .unwrap_or_default();
    assert!(sample.contains("lease_"), "投影行应为租约决策:{sample}");
    assert!(sample.contains("proj:0:"), "投影行必须带幂等标记:{sample}");

    // I18:模拟"投影损坏/丢失" → 重置游标 → 重放同一段日志
    let bin = env!("CARGO_BIN_EXE_rdsctl");
    let out = Command::new(bin)
        .args(["admin", "resync-sink"])
        .env("RDSCTL_DATA_DIR", &root_data_dir)
        .env("RDSCTL_SHARD_ID", "0")
        .output()
        .expect("resync-sink 执行失败");
    assert!(
        out.status.success(),
        "resync-sink 应成功:{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("投影游标已重置"), "{stdout}");

    // 重放窗口:等两轮对账(>10s),断言行数**不增加**(幂等)
    std::thread::sleep(Duration::from_secs(14));
    let after = count_marker_rows(&db);
    assert_eq!(
        after, first,
        "重置游标后重放不得产生重复行(幂等标记应生效):{first} → {after}"
    );

    for p in &mut nodes {
        let _ = p.child.kill();
        let _ = p.child.wait();
    }
    let _ = mysql_exec("", &format!("DROP DATABASE IF EXISTS {db}"));
    let _ = std::fs::remove_dir_all(&root);
}
