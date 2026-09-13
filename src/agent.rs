// rdsctl — 远端执行 agent(物理机/跨机模型 P2-①,见 docs/physical-multi-site-ops.md §7)
//
// 职责:
//   - agent 模式:独立进程跑在目标宿主机上,持有该机 docker CLI;
//     控制端(管控服务)对「绑定到该宿主机」的节点不再直接调本机 docker,
//     而是经 HTTP 把 docker 命令交给 agent 执行(节点容器在 agent 所在机器上)。
//   - 单机 drill:控制端与 agent 同机双进程(共享 docker daemon),验证路由与
//     agent 断开时的降级语义;多机部署时 agent 部署到对应物理机即可,协议不变。
//
// 协议(极简 HTTP/1.1 + JSON,零外部 crate):
//   GET  /agent/ping                     → {"ok":true,"host":"<hostname>"}
//   POST /agent/docker {"args":[...]}    → 在本机执行 `docker <args...>`(docker.rs 同语义)
//   POST /agent/sql {"container","user","pass","sql"}      → 等价 exec_mysql_local
//   POST /agent/exec {"container","args":[...]}            → 等价 exec_in
//   POST /agent/state {"container"}       → 容器状态 "Status|ExitCode|RestartCount"(缺失 null)
//   POST /agent/run {"container","args":[...]}             → 等价 docker.rs::run
//   POST /agent/rm {"container"}
// 鉴权:两端相同 token(RDSCTL_AGENT_TOKEN 或 agent 模式 --token)。
//   请求携带头 X-Agent-Token 或 ?token=;不匹配 → 403。token 为空 = 不鉴权(lab)。
//
// 安全边界:agent 暴露任意 docker 执行能力,须仅部署在可信内网并配置强 token;
// 本仓库为 lab 模型,文档中已注明生产需接入更细的授权(命令白名单)与 TLS。

use serde_json::{json, Value};
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const HEADER_TOKEN: &str = "x-agent-token";
/// 执行面 fence 令牌头(设计 §9.2):`<shard>:<term>:<index>`
const HEADER_FENCE: &str = "x-rdsctl-fence";
/// 幂等键头:同键重复请求直接回放首次结果
const HEADER_IDEM: &str = "x-rdsctl-idem";

/// 控制端/agent 共用的 token(env RDSCTL_AGENT_TOKEN;空 = 不鉴权)
pub fn token_opt() -> Option<String> {
    match std::env::var("RDSCTL_AGENT_TOKEN") {
        Ok(t) if !t.is_empty() => Some(t),
        _ => None,
    }
}

/// 由宿主机 ip + agent_port 拼 agent 基址
pub fn agent_url(ip: &str, port: u16) -> String {
    format!("http://{ip}:{port}")
}

// ═════════════════════════ 服务端(agent 模式) ═════════════════════════

pub async fn serve(port: u16) -> io::Result<()> {
    let listener = TcpListener::bind(("0.0.0.0", port)).await?;
    let token = token_opt();
    // 执行面守卫:检测并落地 fence_seen / 幂等缓存(设计 §9.2)
    let guard = std::sync::Arc::new(crate::ha::fence::ExecutorGuard::from_env());
    let fence_required = fence_required();
    tracing::info!(
        "rdsctl agent listening on 0.0.0.0:{port} (token: {}, fence: {})",
        if token.is_some() { "on" } else { "off" },
        if fence_required { "required" } else { "optional" }
    );
    tracing::info!("执行面数据目录:{}", guard.root().display());
    loop {
        let (mut stream, _) = listener.accept().await?;
        let token = token.clone();
        let guard = std::sync::Arc::clone(&guard);
        tokio::spawn(async move {
            if let Err(e) = handle(&mut stream, token.as_deref(), &guard).await {
                tracing::debug!("agent conn error: {e}");
            }
        });
    }
}

/// cluster 模式下必须强制 fence(A4):`RDSCTL_AGENT_FENCE_REQUIRED=1`
pub fn fence_required() -> bool {
    std::env::var("RDSCTL_AGENT_FENCE_REQUIRED").as_deref() == Ok("1")
}

async fn handle(
    stream: &mut TcpStream,
    token: Option<&str>,
    guard: &crate::ha::fence::ExecutorGuard,
) -> io::Result<()> {
    let mut buf = [0u8; 8192];
    let n = stream.read(&mut buf).await?;
    if n == 0 {
        return Ok(());
    }
    let text = String::from_utf8_lossy(&buf[..n]);
    let head_end = text.find("\r\n\r\n").map(|p| p + 4).unwrap_or(0);
    let head = &text[..head_end];
    let mut lines = head.split("\r\n");
    let req_line = lines.next().unwrap_or("").to_string();
    let mut parts = req_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_uppercase();
    let target = parts.next().unwrap_or("/").to_string();
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (target.clone(), String::new()),
    };
    // token 校验(头或查询参数)
    let q_token = query.split('&').find_map(|kv| {
        let (k, v) = kv.split_once('=')?;
        (k == "token").then_some(v.to_string())
    });
    let header_of = |name: &str| -> Option<String> {
        lines
            .clone()
            .find(|l| l.to_ascii_lowercase().starts_with(name))
            .map(|l| l[name.len() + 1..].trim().to_string())
    };
    let h_token = header_of(HEADER_TOKEN);
    let h_fence = header_of(HEADER_FENCE);
    let h_idem = header_of(HEADER_IDEM).filter(|s| !s.is_empty());
    let got = q_token.or(h_token);
    if let Some(expect) = token {
        if got.as_deref() != Some(expect) {
            return reply(stream, 403, r#"{"ok":false,"error":"token 不匹配"}"#).await;
        }
    }
    tracing::debug!("agent req {method} {path}");
    // 读 body(Content-Length;GET 无 body)
    let content_length: usize = head
        .lines()
        .find(|l| l.to_ascii_lowercase().starts_with("content-length:"))
        .and_then(|l| l.split(':').nth(1))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0);
    let mut body = text[head_end.min(n)..].to_string();
    while body.len() < content_length {
        let mut chunk = [0u8; 4096];
        let r = stream.read(&mut chunk).await?;
        if r == 0 {
            break;
        }
        body.push_str(&String::from_utf8_lossy(&chunk[..r]));
    }
    let payload: Value = if content_length > 0 {
        serde_json::from_str(&body).unwrap_or(json!({}))
    } else {
        json!({})
    };

    if method == "GET" && path == "/agent/ping" {
        let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "agent".into());
        return reply(
            stream,
            200,
            &json!({
                "ok": true,
                "host": host,
                "fence_capable": true,
                "fence_required": fence_required(),
                "fence_seen_max": guard.seen_max().map(|f| f.wire()),
                "data_dir": guard.root().display().to_string(),
            })
            .to_string(),
        )
        .await;
    }
    if method != "POST" {
        return reply(stream, 405, r#"{"ok":false,"error":"仅支持 POST"}"#).await;
    }
    // 需要 fence 的变更类端点:先做幂等短路,再做 fence 强制(设计 §9.2)
    let mutating = matches!(
        path.as_str(),
        "/agent/run" | "/agent/rm" | "/agent/exec" | "/agent/sql" | "/agent/docker"
    );
    if mutating {
        if let Some(idem) = &h_idem {
            if let Some(cached) = guard.idem_get(idem) {
                tracing::debug!("agent 幂等命中:{idem}");
                return reply(stream, 200, &cached).await;
            }
        }
        let key = payload["container"].as_str().unwrap_or("docker").to_string();
        match &h_fence {
            Some(raw) => match crate::ha::Fence::parse(raw) {
                Some(f) => {
                    if let Err(e) = guard.check_and_raise(&key, f) {
                        tracing::warn!("agent 拒绝过期 fence:{e}");
                        return reply(
                            stream,
                            409,
                            &json!({
                                "ok": false,
                                "error": "fence_stale",
                                "detail": e.to_string(),
                                "seen": e.seen.map(|s| s.wire()),
                            })
                            .to_string(),
                        )
                        .await;
                    }
                }
                None => {
                    return reply(
                        stream,
                        400,
                        &json!({ "ok": false, "error": "fence 头格式非法(应为 shard:term:index)" })
                            .to_string(),
                    )
                    .await;
                }
            },
            None => {
                // cluster 模式(A4)下缺 fence 一律拒绝;lab/单机模式允许(向后兼容)
                if fence_required() {
                    return reply(
                        stream,
                        409,
                        &json!({ "ok": false, "error": "fence_required", "detail": "该 agent 运行在强制 fence 模式(设计 A4)" })
                            .to_string(),
                    )
                    .await;
                }
            }
        }
    }

    let resp: Value = match path.as_str() {
        "/agent/docker" => {
            let args: Vec<&str> = payload
                .get("args")
                .and_then(|a| a.as_array())
                .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
                .unwrap_or_default();
            run_docker(&args).await
        }
        "/agent/sql" => {
            let c = payload["container"].as_str().unwrap_or("");
            let u = payload["user"].as_str().unwrap_or("");
            let p = payload["pass"].as_str().unwrap_or("");
            let s = payload["sql"].as_str().unwrap_or("");
            if c.is_empty() || s.is_empty() {
                json!({ "ok": false, "error": "缺少 container/sql" })
            } else {
                let r = crate::docker::exec_mysql_local(c, u, p, s).await;
                match r {
                    Ok(out) => json!({ "ok": true, "out": out }),
                    Err(e) => json!({ "ok": false, "error": e }),
                }
            }
        }
        "/agent/exec" => {
            let c = payload["container"].as_str().unwrap_or("");
            let args: Vec<String> = payload
                .get("args")
                .and_then(|a| a.as_array())
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default();
            if c.is_empty() {
                json!({ "ok": false, "error": "缺少 container" })
            } else {
                match crate::docker::exec_in(c, &args).await {
                    Ok(out) => json!({ "ok": true, "out": out }),
                    Err(e) => json!({ "ok": false, "error": e }),
                }
            }
        }
        "/agent/state" => {
            let c = payload["container"].as_str().unwrap_or("");
            match crate::docker::container_state(c).await {
                Some(st) => json!({ "ok": true, "state": st }),
                None => json!({ "ok": true, "state": Value::Null }),
            }
        }
        "/agent/run" => {
            let c = payload["container"].as_str().unwrap_or("");
            let args: Vec<&str> = payload
                .get("args")
                .and_then(|a| a.as_array())
                .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
                .unwrap_or_default();
            if c.is_empty() {
                json!({ "ok": false, "error": "缺少 container" })
            } else {
                match crate::docker::run(c, &args).await {
                    Ok(()) => json!({ "ok": true }),
                    Err(e) => json!({ "ok": false, "error": e }),
                }
            }
        }
        "/agent/rm" => {
            let c = payload["container"].as_str().unwrap_or("");
            if c.is_empty() {
                json!({ "ok": false, "error": "缺少 container" })
            } else {
                match crate::docker::rm(c).await {
                    Ok(()) => json!({ "ok": true }),
                    Err(e) => json!({ "ok": false, "error": e }),
                }
            }
        }
        _ => json!({ "ok": false, "error": format!("未知路径 {path}") }),
    };
    // 幂等缓存:仅缓存成功结果(失败必须可重试,不能把错误固化)
    if mutating && resp["ok"].as_bool() == Some(true) {
        if let Some(idem) = &h_idem {
            let _ = guard.idem_put(idem, &resp.to_string());
        }
    }
    reply(stream, 200, &resp.to_string()).await
}

async fn run_docker(args: &[&str]) -> Value {
    match crate::docker::docker(args).await {
        Ok(out) => json!({ "ok": true, "out": out }),
        Err(e) => json!({ "ok": false, "error": e }),
    }
}

async fn reply(stream: &mut TcpStream, status: u16, body: &str) -> io::Result<()> {
    let reason = match status {
        200 => "OK",
        403 => "Forbidden",
        405 => "Method Not Allowed",
        _ => "Error",
    };
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
}

// ═════════════════════════ 客户端(控制端) ═════════════════════════

/// 调用的 fence/幂等元信息(设计 §9.2 的 X-Rdsctl-Fence / X-Rdsctl-Idem)
///
/// 这些构造器供 M1a 后续接线(instance.rs 的 r_run/r_rm/r_exec/r_sql 传 fence)使用;
/// 未接线前保留以免每次改动都要重写签名。
#[allow(dead_code)]
#[derive(Clone, Debug, Default)]
pub struct CallMeta {
    pub fence: Option<crate::ha::Fence>,
    pub idem: Option<String>,
}

#[allow(dead_code)]
impl CallMeta {
    pub fn none() -> Self {
        Self::default()
    }

    pub fn fenced(fence: crate::ha::Fence) -> Self {
        Self {
            fence: Some(fence),
            idem: None,
        }
    }

    pub fn with_idem(mut self, idem: impl Into<String>) -> Self {
        self.idem = Some(idem.into());
        self
    }

    fn headers(&self) -> Vec<(String, String)> {
        let mut h = Vec::new();
        if let Some(f) = &self.fence {
            h.push((HEADER_FENCE.to_string(), f.wire()));
        }
        if let Some(i) = &self.idem {
            h.push((HEADER_IDEM.to_string(), i.clone()));
        }
        h
    }
}

/// 远端 agent 句柄(url + token)
#[derive(Clone, Debug)]
#[allow(dead_code)] // ping/run/rm/exec_in/wait_mysql_ready 由 P2 replace_node/migrate 工作流接入后启用
pub struct Agent {
    pub url: String,
    pub token: Option<String>,
}

#[allow(dead_code)] // ping/run/rm/exec_in/wait_mysql_ready 由 P2 replace_node/migrate 工作流接入后启用
impl Agent {
    pub fn new(url: String, token: Option<String>) -> Self {
        Agent { url, token }
    }

    async fn post(&self, path: &str, payload: &Value) -> Result<Value, String> {
        post_json(&self.url, self.token.as_deref(), path, payload).await
    }

    async fn post_meta(
        &self,
        path: &str,
        payload: &Value,
        meta: &CallMeta,
    ) -> Result<Value, String> {
        request(
            &self.url,
            self.token.as_deref(),
            "POST",
            path,
            Some(payload),
            &meta.headers(),
        )
        .await
    }

    /// 带 fence 的 docker run(执行面强制校验;M1a 起 cluster 模式必用)
    pub async fn run_fenced(
        &self,
        container: &str,
        args: &[&str],
        meta: &CallMeta,
    ) -> Result<(), String> {
        let args: Vec<Value> = args.iter().map(|a| json!(a)).collect();
        let v = self
            .post_meta(
                "/agent/run",
                &json!({ "container": container, "args": args }),
                meta,
            )
            .await?;
        if v["ok"].as_bool() == Some(true) {
            Ok(())
        } else {
            Err(v["error"].as_str().unwrap_or("run 失败").to_string())
        }
    }

    /// 带 fence 的 docker rm
    pub async fn rm_fenced(&self, container: &str, meta: &CallMeta) -> Result<(), String> {
        let v = self
            .post_meta("/agent/rm", &json!({ "container": container }), meta)
            .await?;
        if v["ok"].as_bool() == Some(true) {
            Ok(())
        } else {
            Err(v["error"].as_str().unwrap_or("rm 失败").to_string())
        }
    }

    /// 带 fence 的 exec
    pub async fn exec_in_fenced(
        &self,
        container: &str,
        args: &[String],
        meta: &CallMeta,
    ) -> Result<String, String> {
        let args: Vec<Value> = args.iter().map(|a| json!(a)).collect();
        let v = self
            .post_meta(
                "/agent/exec",
                &json!({ "container": container, "args": args }),
                meta,
            )
            .await?;
        if v["ok"].as_bool() == Some(true) {
            Ok(v["out"].as_str().unwrap_or("").to_string())
        } else {
            Err(v["error"].as_str().unwrap_or("exec 失败").to_string())
        }
    }

    /// 带 fence 的 SQL(切主/复制变更等副作用走这里)
    pub async fn exec_mysql_fenced(
        &self,
        container: &str,
        user: &str,
        pass: &str,
        sql: &str,
        meta: &CallMeta,
    ) -> Result<String, String> {
        let v = self
            .post_meta(
                "/agent/sql",
                &json!({ "container": container, "user": user, "pass": pass, "sql": sql }),
                meta,
            )
            .await?;
        if v["ok"].as_bool() == Some(true) {
            Ok(v["out"].as_str().unwrap_or("").to_string())
        } else {
            Err(v["error"].as_str().unwrap_or("sql 失败").to_string())
        }
    }

    /// ping 详情(用于检查 fence 能力与 A4 前提)
    pub async fn ping_info(&self) -> Option<Value> {
        http_get(&self.url, self.token.as_deref(), "/agent/ping")
            .await
            .ok()
    }

    /// 执行 docker 命令(与 docker::docker 同语义:成功返回 stdout 去尾空白;失败 Err)
    pub async fn docker(&self, args: &[&str]) -> Result<String, String> {
        let args: Vec<Value> = args.iter().map(|a| json!(a)).collect();
        let v = self.post("/agent/docker", &json!({ "args": args })).await?;
        if v["ok"].as_bool() == Some(true) {
            Ok(v["out"].as_str().unwrap_or("").to_string())
        } else {
            Err(v["error"].as_str().unwrap_or("agent 执行失败").to_string())
        }
    }

    pub async fn ping(&self) -> bool {
        match tokio::time::timeout(
            std::time::Duration::from_secs(3),
            http_get(&self.url, self.token.as_deref(), "/agent/ping"),
        )
        .await
        {
            Ok(Ok(v)) => v["ok"].as_bool() == Some(true),
            _ => false,
        }
    }

    pub async fn run(&self, container: &str, args: &[&str]) -> Result<(), String> {
        let args: Vec<Value> = args.iter().map(|a| json!(a)).collect();
        let v = self
            .post(
                "/agent/run",
                &json!({ "container": container, "args": args }),
            )
            .await?;
        if v["ok"].as_bool() == Some(true) {
            Ok(())
        } else {
            Err(v["error"].as_str().unwrap_or("run 失败").to_string())
        }
    }

    pub async fn rm(&self, container: &str) -> Result<(), String> {
        let v = self
            .post("/agent/rm", &json!({ "container": container }))
            .await?;
        if v["ok"].as_bool() == Some(true) {
            Ok(())
        } else {
            Err(v["error"].as_str().unwrap_or("rm 失败").to_string())
        }
    }

    pub async fn exists(&self, container: &str) -> bool {
        self.docker(&["inspect", container]).await.is_ok()
    }

    pub async fn container_state(&self, container: &str) -> Option<String> {
        let v = self
            .post("/agent/state", &json!({ "container": container }))
            .await
            .ok()?;
        if v["ok"].as_bool() == Some(true) {
            v["state"].as_str().map(|s| s.to_string())
        } else {
            None
        }
    }

    pub async fn exec_mysql_local(
        &self,
        container: &str,
        user: &str,
        pass: &str,
        sql: &str,
    ) -> Result<String, String> {
        let v = self
            .post(
                "/agent/sql",
                &json!({ "container": container, "user": user, "pass": pass, "sql": sql }),
            )
            .await?;
        if v["ok"].as_bool() == Some(true) {
            Ok(v["out"].as_str().unwrap_or("").to_string())
        } else {
            Err(v["error"].as_str().unwrap_or("sql 失败").to_string())
        }
    }

    pub async fn exec_in(&self, container: &str, args: &[String]) -> Result<String, String> {
        let args: Vec<Value> = args.iter().map(|a| json!(a)).collect();
        let v = self
            .post(
                "/agent/exec",
                &json!({ "container": container, "args": args }),
            )
            .await?;
        if v["ok"].as_bool() == Some(true) {
            Ok(v["out"].as_str().unwrap_or("").to_string())
        } else {
            Err(v["error"].as_str().unwrap_or("exec 失败").to_string())
        }
    }

    /// 轮询容器内 MySQL 就绪(agent 远端,与 docker.rs::wait_mysql_ready 同语义)
    pub async fn wait_mysql_ready(
        &self,
        container: &str,
        user: &str,
        pass: &str,
        timeout_secs: u64,
    ) -> Result<(), String> {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
        while std::time::Instant::now() < deadline {
            if self
                .exec_mysql_local(container, user, pass, "SELECT 1")
                .await
                .is_ok()
            {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(800)).await;
        }
        let logs = self
            .docker(&["logs", "--tail", "30", container])
            .await
            .unwrap_or_default();
        Err(format!(
            "agent 端容器 {container} 内 MySQL 未在 {timeout_secs}s 内就绪\n最近日志:\n{logs}"
        ))
    }
}

/// HTTP POST JSON(极简客户端,解析状态码 + Content-Length)
async fn post_json(
    base: &str,
    token: Option<&str>,
    path: &str,
    payload: &Value,
) -> Result<Value, String> {
    request(base, token, "POST", path, Some(payload), &[]).await
}

#[allow(dead_code)] // 当前仅 ping 使用(其余 HTTP 客户端方法经 Agent 使用);keep 语义对称
async fn http_get(base: &str, token: Option<&str>, path: &str) -> Result<Value, String> {
    request(base, token, "GET", path, None, &[]).await
}

async fn request(
    base: &str,
    token: Option<&str>,
    method: &str,
    path: &str,
    payload: Option<&Value>,
    extra_headers: &[(String, String)],
) -> Result<Value, String> {
    let (host, port) = split_base(base)?;
    let body = payload.map(|p| p.to_string()).unwrap_or_default();
    let mut head = format!(
        "{method} {path} HTTP/1.1\r\nHost: {host}\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    if let Some(t) = token {
        head.push_str(&format!("{HEADER_TOKEN}: {t}\r\n"));
    }
    for (k, v) in extra_headers {
        head.push_str(&format!("{k}: {v}\r\n"));
    }
    head.push_str("\r\n");
    let fut = async {
        let mut stream = tokio::net::TcpStream::connect((host.as_str(), port)).await?;
        stream.write_all(head.as_bytes()).await?;
        stream.write_all(body.as_bytes()).await?;
        stream.flush().await?;
        let mut buf = Vec::with_capacity(4096);
        let mut chunk = [0u8; 8192];
        loop {
            let r = stream.read(&mut chunk).await?;
            if r == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..r]);
        }
        Ok::<Vec<u8>, io::Error>(buf)
    };
    let raw = tokio::time::timeout(std::time::Duration::from_secs(30), fut)
        .await
        .map_err(|_| format!("agent {base} 请求超时(agent 未接入?)"))?
        .map_err(|e| format!("agent {base} 连接失败: {e}"))?;
    let text = String::from_utf8_lossy(&raw).into_owned();
    let head_end = text.find("\r\n\r\n").map(|p| p + 4).unwrap_or(0);
    let head = &text[..head_end];
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0);
    let content_length: usize = head
        .lines()
        .find(|l| l.to_ascii_lowercase().starts_with("content-length:"))
        .and_then(|l| l.split(':').nth(1))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0);
    let body_text = text[head_end.min(text.len())..]
        .chars()
        .take(content_length)
        .collect::<String>();
    if status != 200 {
        return Err(format!("agent {base} HTTP {status}: {}", body_text.trim()));
    }
    serde_json::from_str(body_text.trim())
        .map_err(|e| format!("agent 响应解析失败: {e}: {}", body_text.trim()))
}

/// 拆 "http://ip:port[/path...]" → (ip, port)
fn split_base(base: &str) -> Result<(String, u16), String> {
    let rest = base
        .strip_prefix("http://")
        .or_else(|| base.strip_prefix("https://"))
        .ok_or_else(|| format!("agent 基址需 http(s):// 前缀: {base}"))?
        .split('/')
        .next()
        .unwrap_or("");
    let (h, p) = match rest.rsplit_once(':') {
        Some((h, p)) => (h, p),
        None => (rest, "9190"),
    };
    let port = p
        .parse::<u16>()
        .map_err(|_| format!("agent 端口非法: {base}"))?;
    Ok((h.to_string(), port))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_base_parses_url() {
        let (h, p) = split_base("http://10.0.0.1:9190").unwrap();
        assert_eq!((h.as_str(), p), ("10.0.0.1", 9190));
        let (h2, _p2) = split_base("http://127.0.0.1:9113/agent/ping").unwrap();
        assert_eq!(h2, "127.0.0.1");
        let e = split_base("10.0.0.1:9190").expect_err("缺协议前缀应报错");
        assert!(e.contains("http"));
    }

    #[test]
    fn agent_url_join() {
        assert_eq!(agent_url("10.0.0.1", 9190), "http://10.0.0.1:9190");
    }
}
