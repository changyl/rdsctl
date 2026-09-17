// rdsctl — DockerRuntime:本机 / 物理机上的 docker CLI 执行后端
//
// 由原 `src/docker.rs` 迁移而来:**错误文案、参数顺序、stdin 写文件语义逐字保留**,
// 保证 P0 抽象层落地后 docker 路径行为零变化(既有 P0 验收 + docker 垫片测试不改断言)。
//
// CLI 名可配(`RDSCTL_CONTAINER_CLI`):填 `podman` / `nerdctl` 即接入 CLI 兼容的
// 容器平台,无需改代码。

use std::process::Stdio;

use super::spec::ContainerSpec;
use super::{check_caps, ContainerState, ExecMeta, ExecOutput, RtFut, RuntimeCaps, RuntimeError, WorkloadRuntime};

/// docker CLI 执行后端。
#[derive(Clone, Debug)]
pub struct DockerRuntime {
    cli: String,
}

impl DockerRuntime {
    pub fn new(cli: impl Into<String>) -> Self {
        Self { cli: cli.into() }
    }

    /// `RDSCTL_CONTAINER_CLI`(默认 docker);podman / nerdctl 等 CLI 兼容平台改此即接入。
    pub fn from_env() -> Self {
        let raw = std::env::var("RDSCTL_CONTAINER_CLI").unwrap_or_default();
        let cli = raw.trim();
        Self::new(if cli.is_empty() { "docker" } else { cli })
    }

    /// 执行 CLI 命令,成功返回 stdout(去尾空白),失败返回平台错误(含 stderr)。
    ///
    /// 错误文案与历史 `docker.rs::docker()` 完全一致(含 `args.join(" ")`),
    /// 保证既有日志/断言/前端提示不变。
    pub async fn cli_raw(&self, args: &[&str]) -> Result<String, RuntimeError> {
        let out = tokio::process::Command::new(&self.cli)
            .args(args)
            .stdin(Stdio::null())
            .output()
            .await
            .map_err(|e| RuntimeError::Transport(format!("docker 执行失败: {e}")))?;
        let stdout = String::from_utf8_lossy(&out.stdout).trim().to_string();
        let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
        if !out.status.success() {
            return Err(RuntimeError::Platform(format!(
                "docker {} 失败: {}",
                args.join(" "),
                if stderr.is_empty() { stdout } else { stderr }
            )));
        }
        Ok(stdout)
    }

    /// 执行 CLI 命令并保留退出码(非零退出码不视为错误;查询台超时判定 124 等)
    pub async fn cli_output(&self, args: &[&str]) -> Result<ExecOutput, RuntimeError> {
        let out = tokio::process::Command::new(&self.cli)
            .args(args)
            .stdin(Stdio::null())
            .output()
            .await
            .map_err(|e| RuntimeError::Transport(format!("docker 执行失败: {e}")))?;
        Ok(ExecOutput {
            stdout: String::from_utf8_lossy(&out.stdout).trim().to_string(),
            stderr: String::from_utf8_lossy(&out.stderr).trim().to_string(),
            exit_code: out.status.code().unwrap_or(-1),
        })
    }

    /// 把平台无关规格渲染成 `docker run` 参数。
    ///
    /// 有 `legacy_args`(历史任务/自定义模块)时**逐字回放**原文,保证零行为变化。
    pub fn render_args(spec: &ContainerSpec) -> Vec<String> {
        if !spec.legacy_args.is_empty() {
            return spec.legacy_args.clone();
        }
        let mut a: Vec<String> = Vec::new();
        if let Some(n) = &spec.network {
            a.push("--network".into());
            a.push(n.clone());
        }
        if let Some(h) = &spec.hostname {
            a.push("--hostname".into());
            a.push(h.clone());
        }
        for p in &spec.ports {
            a.push("-p".into());
            a.push(p.to_cli());
        }
        for m in &spec.mounts {
            a.push("-v".into());
            a.push(m.to_cli());
        }
        for e in &spec.envs {
            a.push("-e".into());
            a.push(e.to_cli());
        }
        if let Some(r) = &spec.restart_policy {
            a.push("--restart".into());
            a.push(r.clone());
        }
        if let Some(ep) = &spec.entrypoint {
            a.push("--entrypoint".into());
            a.push(ep.clone());
        }
        a.extend(spec.extra.iter().cloned());
        a.push(spec.image.clone());
        a.extend(spec.command.iter().cloned());
        a
    }

    /// 容器在指定网络内的 IP(诊断用;docker 专属事实,不进平台无关接口)
    #[allow(dead_code)] // 备用编排工具(未来 Step 可能使用)
    pub async fn ip_in_network(&self, container: &str, network: &str) -> Result<String, RuntimeError> {
        let s = self
            .cli_raw(&[
                "inspect",
                "-f",
                &format!("{{{{.NetworkSettings.Networks.{network}.IPAddress}}}}"),
                container,
            ])
            .await?;
        if s.is_empty() || s == "<no value>" {
            return Err(RuntimeError::NotFound(format!(
                "容器 {container} 不在网络 {network} 内"
            )));
        }
        Ok(s)
    }
}

impl WorkloadRuntime for DockerRuntime {
    fn kind(&self) -> &'static str {
        "docker"
    }

    fn caps(&self) -> RuntimeCaps {
        RuntimeCaps::docker()
    }

    fn create<'a>(
        &'a self,
        spec: &'a ContainerSpec,
        _meta: &'a ExecMeta,
    ) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move {
            check_caps(self, spec)?;
            // 同名残留容器先强制清除(节点重试/进程中断自愈)——与历史 run() 语义一致
            if self.exists(&spec.name).await {
                let _ = self.remove(&spec.name, false, &ExecMeta::none()).await;
            }
            let rendered = Self::render_args(spec);
            let mut cmd: Vec<&str> = vec!["run", "-d", "--name", spec.name.as_str()];
            cmd.extend(rendered.iter().map(|s| s.as_str()));
            self.cli_raw(&cmd).await.map(|_| ())
        })
    }

    fn start<'a>(&'a self, id: &'a str, _meta: &'a ExecMeta) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move { self.cli_raw(&["start", id]).await.map(|_| ()) })
    }

    fn stop<'a>(&'a self, id: &'a str, _meta: &'a ExecMeta) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move { self.cli_raw(&["stop", id]).await.map(|_| ()) })
    }

    /// docker restart(不拆成 stop+start:历史语义与退出码不同)
    fn restart<'a>(&'a self, id: &'a str, _meta: &'a ExecMeta) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move { self.cli_raw(&["restart", id]).await.map(|_| ()) })
    }

    fn remove<'a>(
        &'a self,
        id: &'a str,
        purge_volumes: bool,
        _meta: &'a ExecMeta,
    ) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move {
            // -f 强制;purge 时 -v 连同匿名/命名卷一起删除(xenon 节点销毁语义)
            if purge_volumes {
                self.cli_raw(&["rm", "-f", "-v", id]).await.map(|_| ())
            } else {
                self.cli_raw(&["rm", "-f", id]).await.map(|_| ())
            }
        })
    }

    fn rename<'a>(
        &'a self,
        from: &'a str,
        to: &'a str,
        _meta: &'a ExecMeta,
    ) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move { self.cli_raw(&["rename", from, to]).await.map(|_| ()) })
    }

    fn exists<'a>(&'a self, id: &'a str) -> RtFut<'a, bool> {
        Box::pin(async move { self.cli_raw(&["inspect", id]).await.is_ok() })
    }

    fn state<'a>(&'a self, id: &'a str) -> RtFut<'a, Option<ContainerState>> {
        Box::pin(async move {
            match self
                .cli_raw(&[
                    "inspect",
                    "-f",
                    "{{.State.Status}}|{{.State.ExitCode}}|{{.RestartCount}}",
                    id,
                ])
                .await
            {
                Ok(s) => Some(ContainerState::parse_wire(&s)),
                Err(_) => None,
            }
        })
    }

    fn health<'a>(&'a self, id: &'a str) -> RtFut<'a, Result<Option<String>, RuntimeError>> {
        Box::pin(async move {
            let s = self
                .cli_raw(&[
                    "inspect",
                    "-f",
                    "{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}",
                    id,
                ])
                .await?;
            Ok(Some(s))
        })
    }

    fn logs<'a>(&'a self, id: &'a str, tail: usize) -> RtFut<'a, Result<String, RuntimeError>> {
        Box::pin(async move { self.cli_raw(&["logs", "--tail", &tail.to_string(), id]).await })
    }

    fn exec<'a>(&'a self, id: &'a str, argv: &'a [String]) -> RtFut<'a, Result<String, RuntimeError>> {
        Box::pin(async move {
            let mut cmd: Vec<&str> = vec!["exec", id];
            cmd.extend(argv.iter().map(|s| s.as_str()));
            self.cli_raw(&cmd).await
        })
    }

    fn exec_raw<'a>(
        &'a self,
        id: &'a str,
        argv: &'a [String],
    ) -> RtFut<'a, Result<ExecOutput, RuntimeError>> {
        Box::pin(async move {
            let mut cmd: Vec<&str> = vec!["exec", id];
            cmd.extend(argv.iter().map(|s| s.as_str()));
            self.cli_output(&cmd).await
        })
    }

    fn write_file<'a>(
        &'a self,
        id: &'a str,
        path: &'a str,
        content: &'a str,
    ) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move {
            // 经 stdin 管道写入:docker exec -i container sh -c 'cat > path'
            let mut child = tokio::process::Command::new(&self.cli)
                .args(["exec", "-i", id, "sh", "-c", &format!("cat > {path}")])
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .spawn()
                .map_err(|e| RuntimeError::Transport(format!("docker exec spawn 失败: {e}")))?;
            use tokio::io::AsyncWriteExt;
            if let Some(mut stdin) = child.stdin.take() {
                stdin
                    .write_all(content.as_bytes())
                    .await
                    .map_err(|e| RuntimeError::Transport(format!("写入 stdin 失败: {e}")))?;
                stdin
                    .shutdown()
                    .await
                    .map_err(|e| RuntimeError::Transport(format!("关闭 stdin 失败: {e}")))?;
            }
            let out = child
                .wait_with_output()
                .await
                .map_err(|e| RuntimeError::Transport(format!("等待失败: {e}")))?;
            if !out.status.success() {
                return Err(RuntimeError::Platform(format!(
                    "写入 {path} 失败: {}",
                    String::from_utf8_lossy(&out.stderr)
                )));
            }
            Ok(())
        })
    }

    fn network_ensure<'a>(
        &'a self,
        name: &'a str,
        _meta: &'a ExecMeta,
    ) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move {
            // 已存在则跳过(任务重试/进程中断残留自愈)
            if self.cli_raw(&["network", "inspect", name]).await.is_ok() {
                return Ok(());
            }
            self.cli_raw(&["network", "create", name]).await.map(|_| ())
        })
    }

    fn network_remove<'a>(
        &'a self,
        name: &'a str,
        _meta: &'a ExecMeta,
    ) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move {
            // 不存在则跳过(进程中断残留清理的幂等)
            if self.cli_raw(&["network", "inspect", name]).await.is_ok() {
                self.cli_raw(&["network", "rm", name]).await?;
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::spec::docker_args_to_spec;

    #[test]
    fn legacy_args_are_replayed_verbatim() {
        let raw = vec!["--network", "rds-n1", "-p", "127.0.0.1:35001:3306", "mysql:8.0"];
        let spec = docker_args_to_spec("c1", &raw).unwrap();
        assert_eq!(DockerRuntime::render_args(&spec), raw);
    }

    #[test]
    fn structured_spec_renders_expected_flags() {
        let mut spec = ContainerSpec {
            name: "c1".into(),
            image: "mysql:8.0".into(),
            network: Some("rds-n1".into()),
            hostname: Some("c1".into()),
            command: vec!["--server-id".into(), "1".into()],
            ..Default::default()
        };
        spec.ports.push(crate::exec::spec::PortMapping {
            host_ip: Some("127.0.0.1".into()),
            host_port: Some(35001),
            container_port: 3306,
            proto: "tcp".into(),
        });
        spec.mounts.push(crate::exec::spec::Mount {
            source: "vol-c1".into(),
            target: "/var/lib/mysql".into(),
            kind: crate::exec::spec::MountKind::NamedVolume,
            read_only: false,
        });
        spec.envs.push(crate::exec::spec::EnvVar {
            key: "K".into(),
            value: "V".into(),
        });
        let a = DockerRuntime::render_args(&spec);
        assert_eq!(
            a,
            vec![
                "--network", "rds-n1", "--hostname", "c1", "-p", "127.0.0.1:35001:3306", "-v",
                "vol-c1:/var/lib/mysql", "-e", "K=V", "mysql:8.0", "--server-id", "1"
            ]
        );
    }

    #[test]
    fn docker_declares_full_cli_caps() {
        let rt = DockerRuntime::new("docker");
        let c = rt.caps();
        assert!(c.host_ports && c.bind_mounts && c.named_volumes && c.networks && c.raw_flags);
        assert!(!c.systemd_unit);
    }
}
