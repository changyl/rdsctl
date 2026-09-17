// rdsctl — ExternalRuntime:外部驱动(JSON over stdio)
//
// 定位:控制面**不重编译**即可接入 k8s / OpenStack / 自研平台 —— 平台方提供一个可执行文件
// (shim),按固定契约回答原语请求。凭据留在驱动侧,控制面不持有平台凭据。
//
// 调用模型:每个请求一个子进程(与仓库既有「docker CLI 子进程」风格一致,无常驻 daemon);
//   stdin  : 一行 JSON 请求
//   stdout : 一行 JSON 响应
//   exit 0 : 响应有效;非 0 且 stdout 无 JSON → Transport 错误
//
// 请求:`{"action":"<原语>", ...}`
//   create        {"spec":{...},"meta":{...}}
//   start/stop/restart/remove/rename/exists/state/logs/health/write_file
//                 {"id":"<工作负载>", ...} (remove 带 "purge":bool;logs 带 "tail":n)
//   exec/exec_raw {"id":"...","argv":[...]}
//   network_ensure/network_remove {"name":"..."}
//   expose        {"spec":{...}}
//   caps          {}
//
// 响应:`{"ok":true,"out":"..."}` 或 `{"ok":true,"err":"...","code":124}`
//       失败:`{"ok":false,"error":"...","code":"unsupported","cap":"host_ports"}`
//
// 安全:驱动以控制面同等权限运行(等价 docker.sock 信任级)。强制绝对路径 + 非 world-writable,
//   并只应部署在可信内网(docs/ops-guide-container-platform.md §安全)。

use std::process::Stdio;

use serde_json::{json, Value};

use super::spec::ContainerSpec;
use super::{ContainerState, ExecMeta, ExecOutput, RtFut, RuntimeCaps, RuntimeError, WorkloadRuntime};

/// 外部驱动驱动的执行后端。
#[derive(Clone, Debug)]
pub struct ExternalRuntime {
    cmd: String,
    args: Vec<String>,
    timeout_secs: u64,
    /// 能力声明(配置驱动:`RDSCTL_RUNTIME_CAPS` 列出**不支持**的能力)
    caps: RuntimeCaps,
}

impl ExternalRuntime {
    pub fn new(cmd: impl Into<String>, args: Vec<String>, timeout_secs: u64, caps: RuntimeCaps) -> Self {
        Self {
            cmd: cmd.into(),
            args,
            timeout_secs,
            caps,
        }
    }

    pub fn from_env() -> Self {
        let cmd = std::env::var("RDSCTL_RUNTIME_CMD").unwrap_or_default();
        let args = std::env::var("RDSCTL_RUNTIME_ARGS")
            .unwrap_or_default()
            .split_whitespace()
            .map(|s| s.to_string())
            .collect();
        let timeout_secs = std::env::var("RDSCTL_RUNTIME_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(60);
        let caps = caps_from_env();
        Self::new(cmd, args, timeout_secs, caps)
    }

    pub fn cmd(&self) -> &str {
        &self.cmd
    }

    /// 启动自检:驱动必须存在、是绝对路径、不可被无权限用户改写。
    pub fn preflight(&self) -> Result<(), RuntimeError> {
        if self.cmd.trim().is_empty() {
            return Err(RuntimeError::Platform(
                "RDSCTL_RUNTIME=external 但未配置 RDSCTL_RUNTIME_CMD(驱动可执行文件)".to_string(),
            ));
        }
        let p = std::path::Path::new(&self.cmd);
        if !p.is_absolute() {
            return Err(RuntimeError::Platform(format!(
                "外部驱动必须是绝对路径: {}",
                self.cmd
            )));
        }
        let md = std::fs::metadata(p).map_err(|e| {
            RuntimeError::Platform(format!("外部驱动不可访问 {}: {e}", self.cmd))
        })?;
        if !md.is_file() {
            return Err(RuntimeError::Platform(format!(
                "外部驱动不是普通文件: {}",
                self.cmd
            )));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if md.permissions().mode() & 0o002 != 0 {
                return Err(RuntimeError::Platform(format!(
                    "外部驱动对其它用户可写(权限过宽): {}",
                    self.cmd
                )));
            }
        }
        Ok(())
    }

    /// 向驱动发一个请求,返回解析后的响应(已确认 `ok`)
    async fn send(&self, req: Value) -> Result<Value, RuntimeError> {
        self.preflight()?;
        let mut child = tokio::process::Command::new(&self.cmd)
            .args(&self.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| RuntimeError::Transport(format!("外部驱动启动失败 {}: {e}", self.cmd)))?;
        {
            use tokio::io::AsyncWriteExt;
            let mut stdin = child.stdin.take().ok_or_else(|| {
                RuntimeError::Transport("外部驱动 stdin 不可用".to_string())
            })?;
            let mut line = req.to_string();
            line.push('\n');
            stdin
                .write_all(line.as_bytes())
                .await
                .map_err(|e| RuntimeError::Transport(format!("外部驱动写入失败: {e}")))?;
            stdin
                .shutdown()
                .await
                .map_err(|e| RuntimeError::Transport(format!("外部驱动关闭 stdin 失败: {e}")))?;
        }
        let fut = child.wait_with_output();
        let out = tokio::time::timeout(std::time::Duration::from_secs(self.timeout_secs), fut)
            .await
            .map_err(|_| {
                RuntimeError::Timeout(format!(
                    "外部驱动 {} 在 {}s 内无响应",
                    self.cmd, self.timeout_secs
                ))
            })?
            .map_err(|e| RuntimeError::Transport(format!("外部驱动等待失败: {e}")))?;
        let stdout = String::from_utf8_lossy(&out.stdout).trim().to_string();
        let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
        let v: Value = serde_json::from_str(stdout.lines().last().unwrap_or("")).map_err(|e| {
            RuntimeError::Transport(format!(
                "外部驱动响应非 JSON({e});exit={:?} stderr={}",
                out.status.code(),
                stderr
            ))
        })?;
        if v["ok"].as_bool() != Some(true) {
            let code = v["code"].as_str().unwrap_or("");
            let msg = v["error"].as_str().unwrap_or("外部驱动执行失败");
            return Err(super::runtime_error_from_wire(
                code,
                v["cap"].as_str().unwrap_or(""),
                msg,
            ));
        }
        Ok(v)
    }

    /// 请求驱动的能力声明(启动自检 / 一致性校验用;不是热路径)。
    pub async fn probe_caps(&self) -> Result<RuntimeCaps, RuntimeError> {
        let v = self.send(json!({"action":"caps"})).await?;
        let caps = v.get("caps").cloned().unwrap_or(Value::Null);
        if caps.is_null() {
            return Ok(self.caps);
        }
        let get = |k: &str| caps[k].as_bool();
        Ok(RuntimeCaps {
            host_ports: get("host_ports").unwrap_or(self.caps.host_ports),
            bind_mounts: get("bind_mounts").unwrap_or(self.caps.bind_mounts),
            named_volumes: get("named_volumes").unwrap_or(self.caps.named_volumes),
            networks: get("networks").unwrap_or(self.caps.networks),
            exec: get("exec").unwrap_or(self.caps.exec),
            logs: get("logs").unwrap_or(self.caps.logs),
            rename: get("rename").unwrap_or(self.caps.rename),
            systemd_unit: get("systemd_unit").unwrap_or(self.caps.systemd_unit),
            raw_flags: get("raw_flags").unwrap_or(self.caps.raw_flags),
        })
    }

    fn meta_json(meta: &ExecMeta) -> Value {
        let mut m = serde_json::Map::new();
        if let Some(f) = &meta.fence {
            m.insert("fence".into(), json!(f.wire()));
        }
        if let Some(i) = &meta.idem {
            m.insert("idem".into(), json!(i));
        }
        Value::Object(m)
    }

    async fn out_of(&self, req: Value) -> Result<String, RuntimeError> {
        let v = self.send(req).await?;
        Ok(v["out"].as_str().unwrap_or("").trim().to_string())
    }

    async fn unit_of(&self, req: Value) -> Result<(), RuntimeError> {
        self.send(req).await.map(|_| ())
    }
}

/// `RDSCTL_RUNTIME_CAPS` = 逗号分隔的**不支持**能力名(默认与 docker 对齐)。
///
/// `raw_flags` 恒为 false:平台驱动必须自己消化结构化 spec,无法识别的 docker 参数
/// 一律 fail-closed(不会静默落到本机 docker)。
fn caps_from_env() -> RuntimeCaps {
    let raw = std::env::var("RDSCTL_RUNTIME_CAPS").unwrap_or_default();
    let mut c = RuntimeCaps::docker();
    c.raw_flags = false;
    for item in raw.split(',').map(|s| s.trim().to_ascii_lowercase()) {
        match item.as_str() {
            "host_ports" => c.host_ports = false,
            "bind_mounts" => c.bind_mounts = false,
            "named_volumes" => c.named_volumes = false,
            "networks" => c.networks = false,
            "exec" => c.exec = false,
            "logs" => c.logs = false,
            "rename" => c.rename = false,
            "systemd_unit" => c.systemd_unit = false,
            _ => {}
        }
    }
    c
}

impl WorkloadRuntime for ExternalRuntime {
    fn kind(&self) -> &'static str {
        "external"
    }

    fn caps(&self) -> RuntimeCaps {
        self.caps
    }

    fn create<'a>(
        &'a self,
        spec: &'a ContainerSpec,
        meta: &'a ExecMeta,
    ) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move {
            super::check_caps(self, spec)?;
            let req = json!({
                "action": "create",
                "spec": spec,
                "meta": Self::meta_json(meta),
            });
            self.unit_of(req).await
        })
    }

    fn start<'a>(&'a self, id: &'a str, meta: &'a ExecMeta) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move {
            self.unit_of(json!({"action":"start","id":id,"meta":Self::meta_json(meta)}))
                .await
        })
    }

    fn stop<'a>(&'a self, id: &'a str, meta: &'a ExecMeta) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move {
            self.unit_of(json!({"action":"stop","id":id,"meta":Self::meta_json(meta)}))
                .await
        })
    }

    fn remove<'a>(
        &'a self,
        id: &'a str,
        purge_volumes: bool,
        meta: &'a ExecMeta,
    ) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move {
            self.unit_of(json!({
                "action":"remove","id":id,"purge":purge_volumes,"meta":Self::meta_json(meta)
            }))
            .await
        })
    }

    fn rename<'a>(
        &'a self,
        from: &'a str,
        to: &'a str,
        meta: &'a ExecMeta,
    ) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move {
            if !self.caps.rename {
                return Err(RuntimeError::unsupported("rename", "外部驱动声明不支持重命名"));
            }
            self.unit_of(json!({"action":"rename","from":from,"to":to,"meta":Self::meta_json(meta)}))
                .await
        })
    }

    fn exists<'a>(&'a self, id: &'a str) -> RtFut<'a, bool> {
        Box::pin(async move {
            match self.send(json!({"action":"exists","id":id})).await {
                Ok(v) => v["exists"].as_bool().unwrap_or(v["out"].as_str() == Some("true")),
                Err(_) => false,
            }
        })
    }

    fn state<'a>(&'a self, id: &'a str) -> RtFut<'a, Option<ContainerState>> {
        Box::pin(async move {
            let v = self.send(json!({"action":"state","id":id})).await.ok()?;
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
        })
    }

    fn health<'a>(&'a self, id: &'a str) -> RtFut<'a, Result<Option<String>, RuntimeError>> {
        Box::pin(async move {
            let v = self.send(json!({"action":"health","id":id})).await?;
            if v["health"].is_null() {
                return Ok(None);
            }
            Ok(v["health"].as_str().map(|s| s.to_string()))
        })
    }

    fn logs<'a>(&'a self, id: &'a str, tail: usize) -> RtFut<'a, Result<String, RuntimeError>> {
        Box::pin(async move {
            self.out_of(json!({"action":"logs","id":id,"tail":tail})).await
        })
    }

    fn exec<'a>(&'a self, id: &'a str, argv: &'a [String]) -> RtFut<'a, Result<String, RuntimeError>> {
        Box::pin(async move {
            self.out_of(json!({"action":"exec","id":id,"argv":argv})).await
        })
    }

    fn exec_raw<'a>(
        &'a self,
        id: &'a str,
        argv: &'a [String],
    ) -> RtFut<'a, Result<ExecOutput, RuntimeError>> {
        Box::pin(async move {
            let v = self.send(json!({"action":"exec_raw","id":id,"argv":argv})).await?;
            Ok(ExecOutput {
                stdout: v["out"].as_str().unwrap_or("").trim().to_string(),
                stderr: v["err"].as_str().unwrap_or("").trim().to_string(),
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
            self.unit_of(json!({"action":"write_file","id":id,"path":path,"content":content}))
                .await
        })
    }

    fn network_ensure<'a>(
        &'a self,
        name: &'a str,
        meta: &'a ExecMeta,
    ) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move {
            self.unit_of(json!({"action":"network_ensure","name":name,"meta":Self::meta_json(meta)}))
                .await
        })
    }

    fn network_remove<'a>(
        &'a self,
        name: &'a str,
        meta: &'a ExecMeta,
    ) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move {
            self.unit_of(json!({"action":"network_remove","name":name,"meta":Self::meta_json(meta)}))
                .await
        })
    }

    fn expose<'a>(&'a self, spec: &'a ContainerSpec) -> RtFut<'a, Result<String, RuntimeError>> {
        Box::pin(async move {
            let v = self.send(json!({"action":"expose","spec":spec})).await?;
            let s = v["addr"].as_str().unwrap_or(v["out"].as_str().unwrap_or(""));
            if s.is_empty() {
                return Err(RuntimeError::Platform(
                    "外部驱动未返回接入点地址(addr)".to_string(),
                ));
            }
            Ok(s.to_string())
        })
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;

    /// 写一个「假外部驱动」脚本(平台方 shim 的最小形态)
    fn write_driver(tag: &str, body: &str, mode: u32) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "rdsctl-extdrv-{}-{}",
            tag,
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("driver.sh");
        let mut f = std::fs::File::create(&p).unwrap();
        f.write_all(body.as_bytes()).unwrap();
        drop(f);
        let mut perm = std::fs::metadata(&p).unwrap().permissions();
        perm.set_mode(mode);
        std::fs::set_permissions(&p, perm).unwrap();
        p
    }

    const DRIVER: &str = r#"#!/bin/sh
read -r line
case "$line" in
  *'"action":"caps"'*) echo '{"ok":true,"caps":{"host_ports":false,"raw_flags":false}}' ;;
  *'"action":"create"'*) echo '{"ok":true,"out":""}' ;;
  *'"action":"start"'*) echo '{"ok":true}' ;;
  *'"action":"exists"'*) echo '{"ok":true,"exists":true}' ;;
  *'"action":"state"'*) echo '{"ok":true,"state":"running|0|0"}' ;;
  *'"action":"logs"'*) echo '{"ok":true,"out":"hello-logs"}' ;;
  *'"action":"exec_raw"'*) echo '{"ok":true,"out":"7","err":"","code":0}' ;;
  *'"action":"exec"'*) echo '{"ok":true,"out":"ok-exec"}' ;;
  *'"action":"network_ensure"'*) echo '{"ok":true}' ;;
  *) echo '{"ok":false,"error":"unknown action","code":"platform"}' ;;
esac
"#;

    fn rt_with(caps: RuntimeCaps) -> (ExternalRuntime, std::path::PathBuf) {
        let p = write_driver("ok", DRIVER, 0o755);
        (ExternalRuntime::new(p.to_string_lossy().to_string(), vec![], 20, caps), p)
    }

    #[tokio::test]
    async fn external_driver_roundtrip() {
        let (rt, _p) = rt_with(RuntimeCaps::docker());
        let spec = crate::exec::spec::docker_args_to_spec("c1", &["mysql:8.0"]).unwrap();
        rt.create(&spec, &ExecMeta::none()).await.unwrap();
        assert!(rt.exists("c1").await);
        assert_eq!(rt.logs("c1", 5).await.unwrap(), "hello-logs");
        assert_eq!(rt.exec("c1", &["mysql".into()]).await.unwrap(), "ok-exec");
        let st = rt.state("c1").await.expect("state");
        assert_eq!(st.status, "running");
        assert_eq!(st.wire(), "running|0|0");
        let o = rt.exec_raw("c1", &["mysql".into()]).await.unwrap();
        assert_eq!((o.stdout.as_str(), o.exit_code), ("7", 0));
    }

    #[tokio::test]
    async fn external_driver_caps_probe() {
        let (rt, _p) = rt_with(RuntimeCaps::docker());
        let caps = rt.probe_caps().await.unwrap();
        assert!(!caps.host_ports, "驱动上报 host_ports=false 应生效");
        assert!(caps.networks, "未上报的字段沿用配置默认");
    }

    #[tokio::test]
    async fn external_caps_gate_fails_closed() {
        // 不支持宿主端口 → 带 -p 的规格必须显式报错(不静默丢弃)
        let mut caps = RuntimeCaps::docker();
        caps.raw_flags = false;
        caps.host_ports = false;
        let (rt, _p) = rt_with(caps);
        let spec = crate::exec::spec::docker_args_to_spec("c1", &["-p", "127.0.0.1:35001:3306", "mysql:8.0"]).unwrap();
        let e = rt.create(&spec, &ExecMeta::none()).await.expect_err("应拒绝");
        assert!(matches!(e, RuntimeError::Unsupported { cap: "host_ports", .. }), "{e}");
        // 未知 docker 参数 → 同样 fail-closed
        let spec2 = crate::exec::spec::docker_args_to_spec("c1", &["--privileged", "mysql:8.0"]).unwrap();
        let e2 = rt.create(&spec2, &ExecMeta::none()).await.expect_err("应拒绝");
        assert!(matches!(e2, RuntimeError::Unsupported { cap: "docker_flag", .. }), "{e2}");
    }

    #[test]
    fn external_preflight_rejects_unsafe_paths() {
        let rel = ExternalRuntime::new("driver.sh", vec![], 5, RuntimeCaps::docker());
        assert!(rel.preflight().unwrap_err().to_string().contains("绝对路径"));

        let missing = ExternalRuntime::new("/no/such/rdsctl-driver", vec![], 5, RuntimeCaps::docker());
        assert!(missing.preflight().is_err());

        let p = write_driver("ww", "#!/bin/sh\necho '{\"ok\":true}'\n", 0o777);
        let ww = ExternalRuntime::new(p.to_string_lossy().to_string(), vec![], 5, RuntimeCaps::docker());
        assert!(ww.preflight().unwrap_err().to_string().contains("可写"));
    }

    #[tokio::test]
    async fn external_unreachable_driver_times_out() {
        let p = write_driver("hang", "#!/bin/sh\nsleep 30\n", 0o755);
        let rt = ExternalRuntime::new(p.to_string_lossy().to_string(), vec![], 1, RuntimeCaps::docker());
        let e = rt.exec("c1", &["true".into()]).await.expect_err("应超时");
        assert!(matches!(e, RuntimeError::Timeout(_)), "{e}");
    }
}
