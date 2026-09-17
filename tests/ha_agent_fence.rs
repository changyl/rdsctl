//! 执行面 fence 验收(I7 / F5):agent 强制拒绝过期 fence,且 `fence_seen` 跨进程存活。
//!
//! 为什么单独一个测试文件:这是"正确性在**资源侧**强制"的唯一端到端证据 —— 单元测试
//! 只能证明存储语义,这里证明真实 HTTP 服务端在真实进程里执行该语义(设计 §9.2)。
//!
//! 不依赖 MySQL、不依赖 docker(用 `docker rm` 打一个不存在的容器名,失败无害且无副作用)。

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

const TOKEN: &str = "ha-fence-test-token";
const PROBE_CONTAINER: &str = "rdsctl-fence-probe-does-not-exist";

struct AgentProc {
    child: Child,
    port: u16,
    dir: std::path::PathBuf,
    /// stop() 之后保留目录:用于验证 fence_seen 跨进程存活
    cleanup: bool,
}

impl AgentProc {
    /// 只杀进程、保留数据目录
    fn stop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.cleanup = false;
    }
}

impl Drop for AgentProc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if self.cleanup {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}

fn free_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind");
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

fn tmp_dir(tag: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "rdsctl-ha-agent-{tag}-{}-{}",
        std::process::id(),
        Instant::now().elapsed().as_nanos()
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

/// 起一个 agent 进程(fence_required 决定是否强制 fence 头)
fn start_agent(tag: &str, fence_required: bool, dir: std::path::PathBuf, port: u16) -> AgentProc {
    let bin = env!("CARGO_BIN_EXE_rdsctl");
    let log = dir.join("agent.log");
    let out = std::fs::File::create(&log).unwrap();
    let err = out.try_clone().unwrap();
    let mut cmd = Command::new(bin);
    cmd.args(["agent", "--port", &port.to_string()])
        .env("RDSCTL_AGENT_TOKEN", TOKEN)
        .env("RDSCTL_AGENT_DATA_DIR", dir.join("data"))
        .env(
            "RDSCTL_AGENT_FENCE_REQUIRED",
            if fence_required { "1" } else { "0" },
        )
        .stdout(out)
        .stderr(err);
    let child = cmd.spawn().expect("启动 agent 失败");
    let proc = AgentProc {
        child,
        port,
        dir,
        cleanup: true,
    };
    // 就绪:ping 返回 200
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        if let Ok((200, _)) = http(port, "GET", "/agent/ping", None, &[]) {
            return proc;
        }
        std::thread::sleep(Duration::from_millis(150));
    }
    panic!("agent({tag}) 未在 20s 内就绪");
}

type Resp = (u16, String);

fn http(
    port: u16,
    method: &str,
    path: &str,
    body: Option<&str>,
    headers: &[(&str, &str)],
) -> Result<Resp, String> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).map_err(|e| e.to_string())?;
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .map_err(|e| e.to_string())?;
    let payload = body.unwrap_or("");
    let mut req = format!(
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nX-Agent-Token: {TOKEN}\r\nContent-Length: {}\r\nConnection: close\r\n",
        payload.len()
    );
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
    req.push_str(payload);
    stream.write_all(req.as_bytes()).map_err(|e| e.to_string())?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).map_err(|e| e.to_string())?;
    let text = String::from_utf8_lossy(&buf).into_owned();
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or_else(|| format!("响应无法解析:{text}"))?;
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_string())
        .unwrap_or_default();
    Ok((status, body))
}

fn rm_call(port: u16, fence: Option<&str>, idem: Option<&str>) -> Resp {
    let body = format!(r#"{{"container":"{PROBE_CONTAINER}"}}"#);
    let mut hs: Vec<(&str, &str)> = Vec::new();
    if let Some(f) = fence {
        hs.push(("X-Rdsctl-Fence", f));
    }
    if let Some(i) = idem {
        hs.push(("X-Rdsctl-Idem", i));
    }
    http(port, "POST", "/agent/rm", Some(&body), &hs).expect("agent 请求失败")
}

#[test]
fn agent_ping_reports_fence_capability() {
    let dir = tmp_dir("ping");
    let port = free_port();
    let a = start_agent("ping", true, dir, port);
    let (status, body) = http(port, "GET", "/agent/ping", None, &[]).unwrap();
    assert_eq!(status, 200);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["ok"], true);
    assert_eq!(v["fence_capable"], true, "必须声明 fence 能力(A4 前提检查依赖它)");
    assert_eq!(v["fence_required"], true);
    drop(a);
}

#[test]
fn fence_is_monotonic_enforced_and_survives_agent_restart() {
    let dir = tmp_dir("mono");
    let data = dir.join("data");
    let port = free_port();
    let a = start_agent("mono", true, dir.clone(), port);

    // 1) 首次 fence 被接受(容器不存在 → 执行失败但 fence 语义通过,不能是 409)
    let (st, body) = rm_call(port, Some("0:2:10"), None);
    assert_eq!(st, 200, "首个 fence 不应被拒:{body}");
    assert_ne!(
        serde_json::from_str::<serde_json::Value>(&body).unwrap()["error"],
        "fence_stale"
    );

    // 2) 同一 fence 重发:幂等放行
    let (st, _) = rm_call(port, Some("0:2:10"), None);
    assert_eq!(st, 200, "相同 fence 必须幂等放行");

    // 3) 更高的 fence:放行并抬高
    let (st, _) = rm_call(port, Some("0:3:1"), None);
    assert_eq!(st, 200);

    // 4) 过期的 fence:必须 409 fence_stale(僵尸副本兜底)
    let (st, body) = rm_call(port, Some("0:2:99"), None);
    assert_eq!(st, 409, "过期 fence 必须被资源侧拒绝(C3)");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["error"], "fence_stale", "错误码必须是 fence_stale:{body}");
    assert_eq!(v["seen"], "0:3:1", "应报告已见的最大 fence");

    // 5) 跨分片 fence:不可比 → 拒绝
    let (st, _) = rm_call(port, Some("1:9:9"), None);
    assert_eq!(st, 409, "跨分片 fence 不可比,必须拒绝");

    // 6) 缺 fence + 强制模式 → 409 fence_required
    let (st, body) = rm_call(port, None, None);
    assert_eq!(st, 409);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["error"], "fence_required", "A4 强制模式必须拒绝无 fence 请求");

    // 7) 非法格式 → 400
    let (st, _) = rm_call(port, Some("garbage"), None);
    assert_eq!(st, 400, "畸形 fence 头必须 400");

    // 杀掉并重启(F5:落盘必须跨进程存活)
    let mut a = a;
    a.stop();
    assert!(data.join("fence_seen").exists(), "fence_seen 目录应已落盘");
    let port2 = free_port();
    let b = start_agent("mono2", true, dir, port2);
    let (st, body) = rm_call(port2, Some("0:2:10"), None);
    assert_eq!(
        st, 409,
        "重启后旧 fence 必须继续被拒(否则僵尸副本可重复执行):{body}"
    );
    // 而更高的 fence 仍然可用
    let (st, _) = rm_call(port2, Some("0:4:1"), None);
    assert_eq!(st, 200, "更高 fence 在重启后仍应放行");
    drop(b);
}

#[test]
fn fence_optional_mode_allows_legacy_clients() {
    let dir = tmp_dir("optional");
    let port = free_port();
    let a = start_agent("optional", false, dir, port);
    // 未强制 fence:老客户端(不带 fence 头)仍可用 —— single 模式向后兼容
    let (st, _) = rm_call(port, None, None);
    assert_eq!(st, 200, "非强制模式下应保持向后兼容");
    // 但一旦带了 fence,依然强制单调性
    let (st, _) = rm_call(port, Some("0:5:5"), None);
    assert_eq!(st, 200);
    let (st, _) = rm_call(port, Some("0:4:5"), None);
    assert_eq!(st, 409, "只要带了 fence 就必须单调");
    drop(a);
}

/// 只读 / 变更两类端点在**强制 fence** 模式下的分野。
///
/// 这是 cluster 模式「本机也经 agent」的前提:巡检/事实是只读的,在实例租约之外
/// 拿不到 fence,必须能过;而变更类在无 fence 时必须被拒(否则 A4 形同虚设)。
#[test]
fn strict_mode_separates_read_only_from_mutating_endpoints() {
    let dir = tmp_dir("ro_split");
    let port = free_port();
    let mut a = start_agent("ro_split", true, dir, port);

    // ── 只读类:无 fence 也必须放行(巡检靠这些)──
    let read_calls: &[(&str, &str)] = &[
        ("/agent/state", r#"{"container":"c1"}"#),
        ("/agent/health", r#"{"id":"c1"}"#),
        ("/agent/logs", r#"{"id":"c1","tail":5}"#),
        ("/agent/exists", r#"{"id":"c1"}"#),
        // exec_raw = 只读通道(exec_mysql_ro / query_table 走它)
        ("/agent/exec_raw", r#"{"id":"c1","argv":["mysql","-e","SELECT 1"]}"#),
    ];
    for (path, body) in read_calls {
        let (st, resp) = http(port, "POST", path, Some(body), &[]).expect("请求失败");
        assert_eq!(st, 200, "{path} 只读类不应因缺 fence 被拒: {resp}");
        assert!(
            !resp.contains("fence_required"),
            "{path} 不应要求 fence: {resp}"
        );
    }

    // ── 变更类:无 fence 必须拒绝 ──
    let mut_calls: &[(&str, &str)] = &[
        ("/agent/exec", r#"{"container":"c1","args":["sh","-c","true"]}"#),
        ("/agent/sql", r#"{"container":"c1","user":"root","pass":"p","sql":"SET GLOBAL read_only=ON"}"#),
        ("/agent/start", r#"{"id":"c1"}"#),
        ("/agent/network/ensure", r#"{"name":"rds-n1"}"#),
    ];
    for (path, body) in mut_calls {
        let (st, resp) = http(port, "POST", path, Some(body), &[]).expect("请求失败");
        assert_eq!(st, 409, "{path} 变更类在缺 fence 时必须拒绝: {resp}");
        assert!(resp.contains("fence_required"), "{path} 应报 fence_required: {resp}");
    }

    // ── 带上 fence 后,变更类放行 ──
    let f = "0:7:100";
    let (st, resp) = http(
        port,
        "POST",
        "/agent/exec",
        Some(r#"{"container":"c1","args":["sh","-c","true"]}"#),
        &[("X-Rdsctl-Fence", f)],
    )
    .expect("请求失败");
    assert_eq!(st, 200, "带 fence 的变更应放行: {resp}");
    assert!(!resp.contains("fence_required"), "{resp}");

    // ── 只读类不受 fence 单调性影响:过期 fence 也不该让读失败 ──
    let (st, resp) = http(
        port,
        "POST",
        "/agent/state",
        Some(r#"{"container":"c1"}"#),
        &[("X-Rdsctl-Fence", "0:1:1")],
    )
    .expect("请求失败");
    assert_eq!(st, 200, "只读类不应被 fence 单调性拒绝: {resp}");

    a.stop();
}
