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
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::exec::{self, ExecMeta, RuntimeError, WorkloadRuntime};

const HEADER_TOKEN: &str = "x-agent-token";
/// 执行面 fence 令牌头(设计 §9.2):`<shard>:<term>:<index>`
const HEADER_FENCE: &str = exec::HEADER_FENCE;
/// 幂等键头:同键重复请求直接回放首次结果
const HEADER_IDEM: &str = exec::HEADER_IDEM;

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

pub async fn serve(port: u16, rt: Arc<dyn WorkloadRuntime>) -> io::Result<()> {
    let listener = TcpListener::bind(("0.0.0.0", port)).await?;
    let token = token_opt();
    // 执行面守卫:检测并落地 fence_seen / 幂等缓存(设计 §9.2)
    let guard = Arc::new(crate::ha::fence::ExecutorGuard::from_env());
    let fence_required = fence_required();
    tracing::info!(
        "rdsctl agent listening on 0.0.0.0:{port} (token: {}, fence: {}, runtime: {})",
        if token.is_some() { "on" } else { "off" },
        if fence_required { "required" } else { "optional" },
        rt.kind()
    );
    tracing::info!("执行面数据目录:{}", guard.root().display());
    loop {
        let (mut stream, _) = listener.accept().await?;
        let token = token.clone();
        let guard = Arc::clone(&guard);
        let rt = Arc::clone(&rt);
        tokio::spawn(async move {
            if let Err(e) = handle(&mut stream, token.as_deref(), &guard, &rt).await {
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
    rt: &Arc<dyn WorkloadRuntime>,
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
                "runtime": rt.kind(),
                "version": env!("CARGO_PKG_VERSION"),
                "caps": rt.caps(),
                "fence_capable": true,
                "fence_required": fence_required(),
                "fence_seen_max": guard.seen_max().map(|f| f.wire()),
                "data_dir": guard.root().display().to_string(),
            })
            .to_string(),
        )
        .await;
    }
    if method == "GET" && path == "/agent/caps" {
        return reply(
            stream,
            200,
            &json!({ "ok": true, "runtime": rt.kind(), "caps": rt.caps() }).to_string(),
        )
        .await;
    }
    if method != "POST" {
        return reply(stream, 405, r#"{"ok":false,"error":"仅支持 POST"}"#).await;
    }
    // 需要 fence 的变更类端点:先做幂等短路,再做 fence 强制(设计 §9.2)
    let mutating = matches!(
        path.as_str(),
        "/agent/run"
            | "/agent/rm"
            | "/agent/remove"
            | "/agent/create"
            | "/agent/start"
            | "/agent/stop"
            | "/agent/restart"
            | "/agent/rename"
            | "/agent/exec"
            | "/agent/sql"
            | "/agent/write_file"
            | "/agent/network/ensure"
            | "/agent/network/remove"
            | "/agent/docker"
    );
    if mutating {
        if let Some(idem) = &h_idem {
            if let Some(cached) = guard.idem_get(idem) {
                tracing::debug!("agent 幂等命中:{idem}");
                return reply(stream, 200, &cached).await;
            }
        }
        let key = fence_key(&payload);
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

    // fence/幂等元信息:变更类原语经此透传到本机执行后端
    let meta = ExecMeta {
        fence: h_fence.as_deref().and_then(crate::ha::Fence::parse),
        idem: h_idem.clone(),
    };

    let resp: Value = match path.as_str() {
        // ── 平台无关原语(推荐)──
        "/agent/create" => match serde_json::from_value::<exec::ContainerSpec>(payload["spec"].clone()) {
            Ok(spec) => unit(rt.create(&spec, &meta).await),
            Err(e) => json!({ "ok": false, "error": format!("spec 解析失败: {e}") }),
        },
        "/agent/start" => unit(rt.start(id_of(&payload), &meta).await),
        "/agent/stop" => unit(rt.stop(id_of(&payload), &meta).await),
        "/agent/restart" => unit(rt.restart(id_of(&payload), &meta).await),
        "/agent/remove" | "/agent/rm" => {
            let id = id_of(&payload);
            if id.is_empty() {
                json!({ "ok": false, "error": "缺少 container/id" })
            } else {
                unit(rt.remove(id, payload["purge"].as_bool().unwrap_or(false), &meta).await)
            }
        }
        "/agent/rename" => {
            let from = payload["from"].as_str().unwrap_or("");
            let to = payload["to"].as_str().unwrap_or("");
            if from.is_empty() || to.is_empty() {
                json!({ "ok": false, "error": "缺少 from/to" })
            } else {
                unit(rt.rename(from, to, &meta).await)
            }
        }
        "/agent/exists" => json!({ "ok": true, "exists": rt.exists(id_of(&payload)).await }),
        "/agent/state" => match rt.state(id_of(&payload)).await {
            Some(st) => json!({ "ok": true, "state": st.wire(), "health": st.health }),
            None => json!({ "ok": true, "state": Value::Null }),
        },
        "/agent/health" => match rt.health(id_of(&payload)).await {
            Ok(h) => json!({ "ok": true, "health": h }),
            Err(e) => err_json(e),
        },
        "/agent/logs" => {
            let tail = payload["tail"].as_u64().unwrap_or(40) as usize;
            match rt.logs(id_of(&payload), tail).await {
                Ok(o) => ok_out(o),
                Err(e) => err_json(e),
            }
        }
        "/agent/exec" => {
            let c = id_of(&payload);
            let args = argv_of(&payload, "args");
            if c.is_empty() {
                json!({ "ok": false, "error": "缺少 container" })
            } else {
                match rt.exec(c, &args).await {
                    Ok(o) => ok_out(o),
                    Err(e) => err_json(e),
                }
            }
        }
        "/agent/exec_raw" => {
            let c = id_of(&payload);
            let args = argv_of(&payload, "argv");
            if c.is_empty() {
                json!({ "ok": false, "error": "缺少 container" })
            } else {
                match rt.exec_raw(c, &args).await {
                    Ok(o) => json!({ "ok": true, "out": o.stdout, "err": o.stderr, "code": o.exit_code }),
                    Err(e) => err_json(e),
                }
            }
        }
        "/agent/sql" => {
            let c = id_of(&payload);
            let u = payload["user"].as_str().unwrap_or("");
            let p = payload["pass"].as_str().unwrap_or("");
            let s = payload["sql"].as_str().unwrap_or("");
            if c.is_empty() || s.is_empty() {
                json!({ "ok": false, "error": "缺少 container/sql" })
            } else {
                match rt.exec_mysql_local(c, u, p, s).await {
                    Ok(o) => ok_out(o),
                    Err(e) => err_json(e),
                }
            }
        }
        "/agent/write_file" => {
            let c = id_of(&payload);
            let path = payload["path"].as_str().unwrap_or("");
            let content = payload["content"].as_str().unwrap_or("");
            if c.is_empty() || path.is_empty() {
                json!({ "ok": false, "error": "缺少 id/path" })
            } else {
                unit(rt.write_file(c, path, content).await)
            }
        }
        "/agent/network/ensure" => {
            let name = payload["name"].as_str().unwrap_or("");
            if name.is_empty() {
                json!({ "ok": false, "error": "缺少 name" })
            } else {
                unit(rt.network_ensure(name, &meta).await)
            }
        }
        "/agent/network/remove" => {
            let name = payload["name"].as_str().unwrap_or("");
            if name.is_empty() {
                json!({ "ok": false, "error": "缺少 name" })
            } else {
                unit(rt.network_remove(name, &meta).await)
            }
        }
        "/agent/expose" => match serde_json::from_value::<exec::ContainerSpec>(payload["spec"].clone()) {
            Ok(spec) => match rt.expose(&spec).await {
                Ok(addr) => json!({ "ok": true, "addr": addr }),
                Err(e) => err_json(e),
            },
            Err(e) => json!({ "ok": false, "error": format!("spec 解析失败: {e}") }),
        },
        // ── 历史别名(旧控制面 / 既有测试兼容;语义与上面完全一致)──
        "/agent/run" => {
            let c = payload["container"].as_str().unwrap_or("");
            let args = argv_of(&payload, "args");
            if c.is_empty() {
                json!({ "ok": false, "error": "缺少 container" })
            } else {
                let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
                match exec::spec::docker_args_to_spec(c, &refs) {
                    Ok(spec) => unit(rt.create(&spec, &meta).await),
                    Err(e) => err_json(e),
                }
            }
        }
        "/agent/docker" => {
            // **已废弃**:裸 CLI 透传只是 docker 后端的兼容出口,平台无关路径请用原语端点
            if rt.kind() != "docker" {
                json!({ "ok": false, "error": "/agent/docker 已废弃,且本机后端不是 docker", "code": "unsupported", "cap": "raw_flags" })
            } else {
                let args = argv_of(&payload, "args");
                let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
                run_docker(&refs).await
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

/// 工作负载标识:新端点用 `id`,历史端点用 `container`
fn id_of(payload: &Value) -> &str {
    payload["id"]
        .as_str()
        .or_else(|| payload["container"].as_str())
        .unwrap_or("")
}

/// fence/幂等**作用域键**:按资源标识取(设计 §9.2 的 per-resource 单调性)。
///
/// 必须覆盖各端点携带标识的写法,否则 `/agent/create`(标识在 `spec.name`)
/// 会退化成同一个键,导致不同容器的 fence 相互干扰。
fn fence_key(payload: &Value) -> String {
    payload["id"]
        .as_str()
        .or_else(|| payload["container"].as_str())
        .or_else(|| payload["spec"]["name"].as_str())
        .or_else(|| payload["from"].as_str())
        .or_else(|| payload["name"].as_str())
        .unwrap_or("docker")
        .to_string()
}

/// 字符串数组参数(历史端点 `args` / 新端点 `argv`)
fn argv_of(payload: &Value, key: &str) -> Vec<String> {
    payload
        .get(key)
        .and_then(|a| a.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

fn ok_out(out: String) -> Value {
    json!({ "ok": true, "out": out })
}

/// 结构化错误上线:保留文案,并带上 code/cap 让控制面还原 `Unsupported`(fail-closed 跨进程传递)
fn err_json(e: RuntimeError) -> Value {
    let (code, cap) = exec::error_code_of(&e);
    json!({ "ok": false, "error": e.to_string(), "code": code, "cap": cap })
}

fn unit(r: Result<(), RuntimeError>) -> Value {
    match r {
        Ok(()) => json!({ "ok": true }),
        Err(e) => err_json(e),
    }
}

/// 裸 CLI 透传(仅 `/agent/docker` 废弃端点使用;始终走本机配置的 CLI)
async fn run_docker(args: &[&str]) -> Value {
    let cli = exec::docker::DockerRuntime::from_env();
    match cli.cli_raw(args).await {
        Ok(out) => json!({ "ok": true, "out": out }),
        Err(e) => err_json(e),
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

/// 调用的 fence/幂等元信息(设计 §9.2 的 X-Rdsctl-Fence / X-Rdsctl-Idem)。
///
/// 与执行面抽象共用同一类型(`crate::exec::ExecMeta`):抽象化之后 fence 语义
/// 仍然穿过 `WorkloadRuntime`,不因平台无关化而丢。
pub type CallMeta = ExecMeta;

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

    /// 平台无关原语调用(`crate::exec::agent::AgentRuntime` 用):
    /// 成功返回响应 JSON;失败把 `code`/`cap` 还原成结构化 [`RuntimeError`],
    /// 使 `Unsupported` 这类 fail-closed 语义能跨进程传递。
    pub async fn rt_call(
        &self,
        path: &str,
        payload: Value,
        meta: &CallMeta,
    ) -> Result<Value, RuntimeError> {
        let v = self
            .post_meta(path, &payload, meta)
            .await
            .map_err(RuntimeError::Platform)?;
        if v["ok"].as_bool() == Some(true) {
            return Ok(v);
        }
        Err(exec::runtime_error_from_wire(
            v["code"].as_str().unwrap_or(""),
            v["cap"].as_str().unwrap_or(""),
            v["error"].as_str().unwrap_or("agent 执行失败"),
        ))
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

    /// fence 作用域键必须覆盖各端点的标识写法(否则不同资源共用一个键,
    /// fence 单调性会互相干扰 —— 尤其 `/agent/create` 的标识在 `spec.name`)
    #[test]
    fn fence_key_covers_all_endpoint_shapes() {
        assert_eq!(fence_key(&json!({"container": "c1"})), "c1"); // 历史端点
        assert_eq!(fence_key(&json!({"id": "c2"})), "c2"); // 新端点
        assert_eq!(fence_key(&json!({"spec": {"name": "c3"}})), "c3"); // create/expose
        assert_eq!(fence_key(&json!({"from": "c4", "to": "c5"})), "c4"); // rename
        assert_eq!(fence_key(&json!({"name": "n1"})), "n1"); // network/*
        assert_eq!(fence_key(&json!({})), "docker"); // 兜底(与改造前一致)
        // 标识优先级:id > container > spec.name
        assert_eq!(fence_key(&json!({"id": "a", "container": "b"})), "a");
    }

    #[test]
    fn id_of_does_not_invent_an_id() {
        // handler 依赖空串做参数校验,不能被兜底值污染
        assert_eq!(id_of(&json!({"container": "c1"})), "c1");
        assert_eq!(id_of(&json!({"spec": {"name": "c3"}})), "");
        assert_eq!(id_of(&json!({})), "");
    }

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
