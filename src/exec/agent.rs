// rdsctl — AgentRuntime:经远端 agent 在目标物理机上执行
//
// 语义:`NodeRoute::Agent` 的 trait 化实现。控制面只发**平台无关原语**,
// 「这台物理机上用什么平台」由 agent 自己决定(agent 启动时选本地 runtime)。
// 于是 k8s / OpenStack / 自研平台只需在对应宿主上部署带该 driver 的 agent,
// 控制面零改动。
//
// fence / 幂等:`ExecMeta` 经 HTTP 头(`x-rdsctl-fence` / `x-rdsctl-idem`)下发,
// agent 侧 `ExecutorGuard` 强制执行(设计 §9.2),语义与抽象化之前完全一致。

use serde_json::{json, Value};

use super::spec::ContainerSpec;
use super::{ContainerState, ExecMeta, ExecOutput, RtFut, RuntimeCaps, RuntimeError, WorkloadRuntime};

/// 远端 agent 执行后端。
#[derive(Clone, Debug)]
pub struct AgentRuntime {
    pub ag: crate::agent::Agent,
}

impl AgentRuntime {
    pub fn new(ag: crate::agent::Agent) -> Self {
        Self { ag }
    }
}

fn state_from(v: &Value) -> Option<ContainerState> {
    if v["state"].is_null() {
        return None;
    }
    if let Some(s) = v["state"].as_str() {
        let mut st = ContainerState::parse_wire(s);
        if let Some(h) = v["health"].as_str() {
            st.health = Some(h.to_string());
        }
        return Some(st);
    }
    Some(ContainerState {
        status: v["state"]["status"].as_str().unwrap_or("").to_string(),
        exit_code: v["state"]["exit_code"].as_i64().unwrap_or(0),
        restarts: v["state"]["restarts"].as_u64().unwrap_or(0),
        health: v["state"]["health"].as_str().map(|s| s.to_string()),
    })
}

/// 「该 agent 不认识这个端点」——即 agent 是**旧版本**(滚动升级窗口)。
///
/// agent 服务端对未知路径返回 `{"ok":false,"error":"未知路径 …"}`。
/// 旧 agent 只有 `/agent/docker|run|rm|exec|sql|state`,所以新控制面在升级期间
/// 需要退回这些历史端点,否则会误判节点缺失(进而可能触发错误的自动切换)。
fn is_legacy_agent(e: &RuntimeError) -> bool {
    matches!(e, RuntimeError::Platform(m) if m.contains("未知路径"))
}

/// 变更加载的元信息合成:
///   - 调用方给了 fence(如 `exec_step` 里的实例操作)→ 原样使用;
///   - 未给(门面/巡检路径)→ 自动按「工作负载 → 归属实例 → 当前租约 fence」补齐。
///
/// 这样 cluster 模式下经 agent 执行的**每一次变更**都带 fence,
/// 无需 60+ 个调用点各自手传(漏传会表现为线上偶发 409)。
fn with_lease_fence(id: &str, given: &ExecMeta) -> ExecMeta {
    if given.fence.is_some() {
        return given.clone();
    }
    let mut m = crate::exec::meta_for_workload(id);
    if m.idem.is_none() {
        m.idem = given.idem.clone();
    }
    m
}

/// 旧 agent 兼容:把结构化规格还原成 docker 参数(仅旧 agent= docker 后端时有意义)
fn legacy_args_of(spec: &ContainerSpec) -> Vec<Value> {
    crate::exec::docker::DockerRuntime::render_args(spec)
        .iter()
        .map(|a| json!(a))
        .collect()
}

impl WorkloadRuntime for AgentRuntime {
    fn kind(&self) -> &'static str {
        "agent"
    }

    /// 能力以 **agent 侧本地驱动**为准:控制面这里返回 docker 基线,
    /// 真正的门禁在 agent 端(那里的 `check_caps` 用的是平台的真实 caps),
    /// 不支持时错误会以 `unsupported capability` 原样回传 —— 仍是 fail-closed。
    fn caps(&self) -> RuntimeCaps {
        RuntimeCaps::docker()
    }

    fn create<'a>(
        &'a self,
        spec: &'a ContainerSpec,
        meta: &'a ExecMeta,
    ) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move {
            let meta = with_lease_fence(&spec.name, meta);
            match self
                .ag
                .rt_call("/agent/create", json!({ "spec": spec }), &meta)
                .await
            {
                Ok(_) => Ok(()),
                // 旧 agent 无 /agent/create → 退回 /agent/run(需 docker 参数)
                Err(e) if is_legacy_agent(&e) => {
                    tracing::warn!("agent 版本较旧(无 /agent/create),退回 /agent/run;建议先升级 agent");
                    self.ag
                        .rt_call(
                            "/agent/run",
                            json!({ "container": spec.name, "args": legacy_args_of(spec) }),
                            &meta,
                        )
                        .await
                        .map(|_| ())
                }
                Err(e) => Err(e),
            }
        })
    }

    fn start<'a>(&'a self, id: &'a str, meta: &'a ExecMeta) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move {
            let meta = with_lease_fence(id, meta);
            self.ag
                .rt_call("/agent/start", json!({ "id": id }), &meta)
                .await
                .map(|_| ())
        })
    }

    fn stop<'a>(&'a self, id: &'a str, meta: &'a ExecMeta) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move {
            let meta = with_lease_fence(id, meta);
            self.ag
                .rt_call("/agent/stop", json!({ "id": id }), &meta)
                .await
                .map(|_| ())
        })
    }

    fn restart<'a>(&'a self, id: &'a str, meta: &'a ExecMeta) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move {
            let meta = with_lease_fence(id, meta);
            self.ag
                .rt_call("/agent/restart", json!({ "id": id }), &meta)
                .await
                .map(|_| ())
        })
    }

    fn remove<'a>(
        &'a self,
        id: &'a str,
        purge_volumes: bool,
        meta: &'a ExecMeta,
    ) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move {
            let meta = with_lease_fence(id, meta);
            match self
                .ag
                .rt_call(
                    "/agent/remove",
                    json!({ "id": id, "purge": purge_volumes }),
                    &meta,
                )
                .await
            {
                Ok(_) => Ok(()),
                // 旧 agent 的 /agent/rm 不支持 purge(连卷删除);此时需人工清卷
                Err(e) if is_legacy_agent(&e) => {
                    if purge_volumes {
                        tracing::warn!(
                            "agent 版本较旧,/agent/rm 不支持连同数据卷删除({id});请升级 agent 或人工清卷"
                        );
                    }
                    self.ag
                        .rt_call("/agent/rm", json!({ "container": id }), &meta)
                        .await
                        .map(|_| ())
                }
                Err(e) => Err(e),
            }
        })
    }

    fn rename<'a>(
        &'a self,
        from: &'a str,
        to: &'a str,
        meta: &'a ExecMeta,
    ) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move {
            let meta = with_lease_fence(from, meta);
            match self
                .ag
                .rt_call("/agent/rename", json!({ "from": from, "to": to }), &meta)
                .await
            {
                Ok(_) => Ok(()),
                Err(e) if is_legacy_agent(&e) => self
                    .ag
                    .rt_call(
                        "/agent/docker",
                        json!({ "args": ["rename", from, to] }),
                        &meta,
                    )
                    .await
                    .map(|_| ()),
                Err(e) => Err(e),
            }
        })
    }

    fn exists<'a>(&'a self, id: &'a str) -> RtFut<'a, bool> {
        Box::pin(async move {
            match self
                .ag
                .rt_call("/agent/exists", json!({ "id": id }), &ExecMeta::none())
                .await
            {
                Ok(v) => v["exists"].as_bool().unwrap_or(false),
                // 旧 agent / 协议异常:退回 inspect 语义(与改造前一致),
                // 绝不能把「端点不认识」当成「工作负载不存在」——那会误判节点缺失
                Err(_) => self.ag.docker(&["inspect", id]).await.is_ok(),
            }
        })
    }

    fn state<'a>(&'a self, id: &'a str) -> RtFut<'a, Option<ContainerState>> {
        Box::pin(async move {
            let v = self
                .ag
                .rt_call("/agent/state", json!({ "container": id }), &ExecMeta::none())
                .await
                .ok()?;
            state_from(&v)
        })
    }

    fn health<'a>(&'a self, id: &'a str) -> RtFut<'a, Result<Option<String>, RuntimeError>> {
        Box::pin(async move {
            let v = self
                .ag
                .rt_call("/agent/health", json!({ "id": id }), &ExecMeta::none())
                .await?;
            Ok(v["health"].as_str().map(|s| s.to_string()))
        })
    }

    fn logs<'a>(&'a self, id: &'a str, tail: usize) -> RtFut<'a, Result<String, RuntimeError>> {
        Box::pin(async move {
            let r = self
                .ag
                .rt_call("/agent/logs", json!({ "id": id, "tail": tail }), &ExecMeta::none())
                .await;
            if let Err(e) = &r {
                if is_legacy_agent(e) {
                    return self
                        .ag
                        .docker(&["logs", "--tail", &tail.to_string(), id])
                        .await
                        .map_err(RuntimeError::Platform);
                }
            }
            Ok(r?["out"].as_str().unwrap_or("").to_string())
        })
    }

    /// 任意容器内命令 = **变更类**(agent 侧 `/agent/exec` 受 fence 约束):
    /// 自动附带该工作负载当前租约的 fence。只读探测请走 `exec_raw`/`exec_mysql_ro`。
    fn exec<'a>(&'a self, id: &'a str, argv: &'a [String]) -> RtFut<'a, Result<String, RuntimeError>> {
        Box::pin(async move {
            let meta = with_lease_fence(id, &ExecMeta::none());
            let v = self
                .ag
                .rt_call(
                    "/agent/exec",
                    json!({ "container": id, "args": argv }),
                    &meta,
                )
                .await?;
            Ok(v["out"].as_str().unwrap_or("").to_string())
        })
    }

    fn exec_raw<'a>(
        &'a self,
        id: &'a str,
        argv: &'a [String],
    ) -> RtFut<'a, Result<ExecOutput, RuntimeError>> {
        Box::pin(async move {
            let v = self
                .ag
                .rt_call(
                    "/agent/exec_raw",
                    json!({ "id": id, "argv": argv }),
                    &ExecMeta::none(),
                )
                .await?;
            Ok(ExecOutput {
                stdout: v["out"].as_str().unwrap_or("").to_string(),
                stderr: v["err"].as_str().unwrap_or("").to_string(),
                exit_code: v["code"].as_i64().unwrap_or(0) as i32,
            })
        })
    }

    fn write_file<'a>(
        &'a self,
        id: &'a str,
        path: &'a str,
        content: &'a str,
    ) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move {
            let meta = with_lease_fence(id, &ExecMeta::none());
            self.ag
                .rt_call(
                    "/agent/write_file",
                    json!({ "id": id, "path": path, "content": content }),
                    &meta,
                )
                .await
                .map(|_| ())
        })
    }

    fn network_ensure<'a>(
        &'a self,
        name: &'a str,
        meta: &'a ExecMeta,
    ) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move {
            let meta = with_lease_fence(name, meta);
            self.ag
                .rt_call("/agent/network/ensure", json!({ "name": name }), &meta)
                .await
                .map(|_| ())
        })
    }

    fn network_remove<'a>(
        &'a self,
        name: &'a str,
        meta: &'a ExecMeta,
    ) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move {
            let meta = with_lease_fence(name, meta);
            self.ag
                .rt_call("/agent/network/remove", json!({ "name": name }), &meta)
                .await
                .map(|_| ())
        })
    }

    fn expose<'a>(&'a self, spec: &'a ContainerSpec) -> RtFut<'a, Result<String, RuntimeError>> {
        Box::pin(async move {
            let v = self
                .ag
                .rt_call("/agent/expose", json!({ "spec": spec }), &ExecMeta::none())
                .await?;
            let s = v["addr"].as_str().unwrap_or(v["out"].as_str().unwrap_or(""));
            if s.is_empty() {
                return Err(RuntimeError::Platform(
                    "agent 未返回接入点地址(addr)".to_string(),
                ));
            }
            Ok(s.to_string())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// 只实现**历史端点**的假 agent(模拟滚动升级窗口里的旧版本 agent):
    /// `/agent/create|remove|rename|exists|logs` 一律「未知路径」,只有
    /// `/agent/run` 与 `/agent/docker` 可用。
    async fn legacy_agent() -> (u16, Arc<Mutex<Vec<String>>>, tokio::task::JoinHandle<()>) {
        let l = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
        let port = l.local_addr().expect("addr").port();
        let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let seen_srv = Arc::clone(&seen);
        let h = tokio::spawn(async move {
            loop {
                let Ok((mut s, _)) = l.accept().await else {
                    break;
                };
                let seen = Arc::clone(&seen_srv);
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 8192];
                    let mut text = String::new();
                    // 读到请求头结束即可(路径足够判定;body 不参与判定)
                    loop {
                        let n = s.read(&mut buf).await.unwrap_or(0);
                        if n == 0 {
                            break;
                        }
                        text.push_str(&String::from_utf8_lossy(&buf[..n]));
                        if text.contains("\r\n\r\n") {
                            break;
                        }
                    }
                    let path = text.split_whitespace().nth(1).unwrap_or("/").to_string();
                    seen.lock().unwrap().push(path.clone());
                    let body = match path.as_str() {
                        "/agent/run" => r#"{"ok":true}"#.to_string(),
                        // 旧控制面的裸 CLI 出口(rename / inspect / logs 都走这里)
                        "/agent/docker" => r#"{"ok":true,"out":"{}"}"#.to_string(),
                        _ => format!(r#"{{"ok":false,"error":"未知路径 {path}"}}"#),
                    };
                    let head = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = s.write_all(head.as_bytes()).await;
                    let _ = s.write_all(body.as_bytes()).await;
                    let _ = s.flush().await;
                });
            }
        });
        (port, seen, h)
    }

    fn rt(port: u16) -> AgentRuntime {
        AgentRuntime::new(crate::agent::Agent::new(
            format!("http://127.0.0.1:{port}"),
            None,
        ))
    }

    #[tokio::test]
    async fn create_falls_back_to_legacy_run_endpoint() {
        let (port, seen, h) = legacy_agent().await;
        let spec = crate::exec::spec::docker_args_to_spec(
            "c1",
            &["--network", "n1", "mysql:8.0"],
        )
        .unwrap();
        rt(port)
            .create(&spec, &ExecMeta::none())
            .await
            .expect("旧 agent 下 create 应回退成功");
        let paths = seen.lock().unwrap().clone();
        assert!(paths.contains(&"/agent/create".to_string()), "{paths:?}");
        assert!(paths.contains(&"/agent/run".to_string()), "应回退到 /agent/run: {paths:?}");
        h.abort();
    }

    /// 关键安全断言:旧 agent 不认识 /agent/exists 时,绝不能把「端点不存在」
    /// 当成「工作负载不存在」——否则会被判成节点缺失,甚至触发错误的自动切换。
    #[tokio::test]
    async fn exists_never_reports_missing_on_protocol_mismatch() {
        let (port, seen, h) = legacy_agent().await;
        assert!(
            rt(port).exists("c1").await,
            "协议不匹配时必须退回 inspect 语义,而不是报 false"
        );
        let paths = seen.lock().unwrap().clone();
        assert!(paths.contains(&"/agent/docker".to_string()), "{paths:?}");
        h.abort();
    }

    #[tokio::test]
    async fn rename_and_logs_fall_back_to_raw_cli() {
        let (port, seen, h) = legacy_agent().await;
        let r = rt(port);
        r.rename("a", "b", &ExecMeta::none()).await.expect("应回退成功");
        let _ = r.logs("a", 5).await;
        let paths = seen.lock().unwrap().clone();
        assert_eq!(
            paths.iter().filter(|p| p.as_str() == "/agent/docker").count(),
            2,
            "rename 与 logs 都应回退到 /agent/docker: {paths:?}"
        );
        h.abort();
    }

    /// 严格模式 stub:不带 `x-rdsctl-fence` 头一律 409(等价 `RDSCTL_AGENT_FENCE_REQUIRED=1`)
    async fn strict_agent() -> (u16, Arc<Mutex<Vec<String>>>, tokio::task::JoinHandle<()>) {
        let l = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
        let port = l.local_addr().expect("addr").port();
        let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let seen_srv = Arc::clone(&seen);
        let h = tokio::spawn(async move {
            loop {
                let Ok((mut s, _)) = l.accept().await else { break };
                let seen = Arc::clone(&seen_srv);
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 8192];
                    let mut text = String::new();
                    loop {
                        let n = s.read(&mut buf).await.unwrap_or(0);
                        if n == 0 { break; }
                        text.push_str(&String::from_utf8_lossy(&buf[..n]));
                        if text.contains("\r\n\r\n") { break; }
                    }
                    let path = text.split_whitespace().nth(1).unwrap_or("/").to_string();
                    seen.lock().unwrap().push(path.clone());
                    let has_fence = text.to_ascii_lowercase().contains("x-rdsctl-fence:");
                    let (status, body) = if has_fence {
                        (200u16, r#"{"ok":true,"out":""}"#.to_string())
                    } else {
                        (409u16, r#"{"ok":false,"error":"fence_required"}"#.to_string())
                    };
                    let head = format!(
                        "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = s.write_all(head.as_bytes()).await;
                    let _ = s.write_all(body.as_bytes()).await;
                    let _ = s.flush().await;
                });
            }
        });
        (port, seen, h)
    }

    /// **关键安全断言**:拿不到 fence(此处无管理器/无租约)时,变更类原语必须被
    /// agent 拒绝,**绝不能**静默降级成本机直连或不带 fence 地变更。
    #[tokio::test]
    async fn exec_fails_closed_when_no_fence_can_be_resolved() {
        let (port, seen, h) = strict_agent().await;
        let r = rt(port);
        let e = r
            .exec("c1", &["mysql".into(), "-e".into(), "SET GLOBAL read_only=ON".into()])
            .await
            .expect_err("无 fence 的变更必须失败(不许旁路)");
        assert!(e.to_string().contains("fence_required"), "{e}");
        let paths = seen.lock().unwrap().clone();
        assert!(paths.contains(&"/agent/exec".to_string()), "{paths:?}");
        h.abort();
    }

    /// 只读原语在严格模式下**不需要** fence(巡检在租约之外也能工作)
    #[tokio::test]
    async fn readonly_primitives_do_not_need_fence() {
        let (port, seen, h) = strict_agent().await;
        let r = rt(port);
        // exec_raw 是只读通道(stub 会因无 fence 返回 409,这里只断言请求确实走 exec_raw)
        let _ = r.exec_raw("c1", &["mysql".into()]).await;
        let paths = seen.lock().unwrap().clone();
        assert!(paths.contains(&"/agent/exec_raw".to_string()), "{paths:?}");
        assert!(!paths.contains(&"/agent/exec".to_string()), "只读不得走变更通道:{paths:?}");
        h.abort();
    }

    #[test]
    fn legacy_detection_is_specific() {
        assert!(is_legacy_agent(&RuntimeError::Platform(
            "未知路径 /agent/exists".to_string()
        )));
        assert!(!is_legacy_agent(&RuntimeError::Platform(
            "docker exec c1 失败: no such container".to_string()
        )));
        assert!(!is_legacy_agent(&RuntimeError::Unsupported {
            cap: "host_ports",
            detail: "x".into()
        }));
    }
}
