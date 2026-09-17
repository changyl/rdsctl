// rdsctl — 测试垫片运行时:记录控制面发出的原语序列(FakeRuntime)
//
// 用途:在不起真实 docker 的前提下,断言「抽象层把什么原语、什么规格」交给了执行后端。
// 这是 P0 的验收工具(见 docs/container-platform-abstraction.md §验收)。

#![cfg(test)]

use std::sync::{Arc, Mutex};

use super::spec::ContainerSpec;
use super::{check_caps, ContainerState, ExecMeta, ExecOutput, RtFut, RuntimeCaps, RuntimeError, WorkloadRuntime};

/// 记录调用序列的假后端。
#[derive(Clone, Default)]
pub struct FakeRuntime {
    /// 原语调用流水(如 `create:rds-i1-master`, `exec:rds-i1-master:mysql`)
    pub calls: Arc<Mutex<Vec<String>>>,
    /// 能力声明(默认 docker 全能力)
    pub caps: Option<RuntimeCaps>,
    /// 设置了就让所有原语失败(验证错误透传)
    pub fail: Option<String>,
}

impl FakeRuntime {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_caps(caps: RuntimeCaps) -> Self {
        Self {
            caps: Some(caps),
            ..Default::default()
        }
    }

    fn note(&self, what: &str) {
        if let Ok(mut v) = self.calls.lock() {
            v.push(what.to_string());
        }
    }

    pub fn calls(&self) -> Vec<String> {
        self.calls.lock().map(|v| v.clone()).unwrap_or_default()
    }

    fn err(&self) -> Option<RuntimeError> {
        self.fail.clone().map(RuntimeError::Platform)
    }
}

impl WorkloadRuntime for FakeRuntime {
    fn kind(&self) -> &'static str {
        "fake"
    }

    fn caps(&self) -> RuntimeCaps {
        self.caps.unwrap_or_else(RuntimeCaps::docker)
    }

    fn create<'a>(
        &'a self,
        spec: &'a ContainerSpec,
        _meta: &'a ExecMeta,
    ) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move {
            check_caps(self, spec)?;
            self.note(&format!("create:{}", spec.name));
            match self.err() {
                Some(e) => Err(e),
                None => Ok(()),
            }
        })
    }

    fn start<'a>(&'a self, id: &'a str, _meta: &'a ExecMeta) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move {
            self.note(&format!("start:{id}"));
            self.err().map_or(Ok(()), Err)
        })
    }

    fn stop<'a>(&'a self, id: &'a str, _meta: &'a ExecMeta) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move {
            self.note(&format!("stop:{id}"));
            self.err().map_or(Ok(()), Err)
        })
    }

    fn restart<'a>(&'a self, id: &'a str, _meta: &'a ExecMeta) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move {
            self.note(&format!("restart:{id}"));
            self.err().map_or(Ok(()), Err)
        })
    }

    fn remove<'a>(
        &'a self,
        id: &'a str,
        purge_volumes: bool,
        _meta: &'a ExecMeta,
    ) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move {
            self.note(&format!(
                "remove:{id}{}",
                if purge_volumes { ":purge" } else { "" }
            ));
            self.err().map_or(Ok(()), Err)
        })
    }

    fn rename<'a>(
        &'a self,
        from: &'a str,
        to: &'a str,
        _meta: &'a ExecMeta,
    ) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move {
            self.note(&format!("rename:{from}->{to}"));
            self.err().map_or(Ok(()), Err)
        })
    }

    fn exists<'a>(&'a self, id: &'a str) -> RtFut<'a, bool> {
        Box::pin(async move {
            self.note(&format!("exists:{id}"));
            true
        })
    }

    fn state<'a>(&'a self, id: &'a str) -> RtFut<'a, Option<ContainerState>> {
        Box::pin(async move {
            self.note(&format!("state:{id}"));
            self.err().map_or(
                Some(ContainerState {
                    status: "running".into(),
                    exit_code: 0,
                    restarts: 0,
                    health: Some("none".into()),
                }),
                |_| None,
            )
        })
    }

    fn logs<'a>(&'a self, id: &'a str, tail: usize) -> RtFut<'a, Result<String, RuntimeError>> {
        Box::pin(async move {
            self.note(&format!("logs:{id}:{tail}"));
            match self.err() {
                Some(e) => Err(e),
                None => Ok(format!("fake logs for {id}")),
            }
        })
    }

    fn exec<'a>(&'a self, id: &'a str, argv: &'a [String]) -> RtFut<'a, Result<String, RuntimeError>> {
        Box::pin(async move {
            self.note(&format!("exec:{id}:{}", argv.first().cloned().unwrap_or_default()));
            match self.err() {
                Some(e) => Err(e),
                None => Ok(String::new()),
            }
        })
    }

    fn exec_raw<'a>(
        &'a self,
        id: &'a str,
        argv: &'a [String],
    ) -> RtFut<'a, Result<ExecOutput, RuntimeError>> {
        Box::pin(async move {
            self.note(&format!("exec_raw:{id}:{}", argv.join(" ")));
            match self.err() {
                Some(e) => Err(e),
                None => Ok(ExecOutput::default()),
            }
        })
    }

    fn write_file<'a>(
        &'a self,
        id: &'a str,
        path: &'a str,
        _content: &'a str,
    ) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move {
            self.note(&format!("write_file:{id}:{path}"));
            self.err().map_or(Ok(()), Err)
        })
    }

    fn network_ensure<'a>(
        &'a self,
        name: &'a str,
        _meta: &'a ExecMeta,
    ) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move {
            self.note(&format!("network_ensure:{name}"));
            self.err().map_or(Ok(()), Err)
        })
    }

    fn network_remove<'a>(
        &'a self,
        name: &'a str,
        _meta: &'a ExecMeta,
    ) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move {
            self.note(&format!("network_remove:{name}"));
            self.err().map_or(Ok(()), Err)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::spec::docker_args_to_spec;

    #[tokio::test]
    async fn records_primitives_and_spec() {
        let rt = FakeRuntime::new();
        let spec = docker_args_to_spec("c1", &["--network", "n1", "mysql:8.0"]).unwrap();
        rt.create(&spec, &ExecMeta::none()).await.unwrap();
        rt.remove("c1", true, &ExecMeta::none()).await.unwrap();
        rt.exec("c1", &["mysql".to_string(), "-e".to_string(), "SELECT 1".to_string()])
            .await
            .unwrap();
        assert_eq!(
            rt.calls(),
            vec![
                "create:c1".to_string(),
                "remove:c1:purge".to_string(),
                "exec:c1:mysql".to_string()
            ]
        );
    }

    #[tokio::test]
    async fn caps_gate_rejects_unknown_flags_fail_closed() {
        // 声明不支持 raw_flags 的后端 → 未知 docker 参数必须报 unsupported(不静默透传)
        let mut caps = RuntimeCaps::docker();
        caps.raw_flags = false;
        let rt = FakeRuntime::with_caps(caps);
        let spec = docker_args_to_spec("c1", &["--privileged", "mysql:8.0"]).unwrap();
        let e = rt.create(&spec, &ExecMeta::none()).await.expect_err("应拒绝");
        assert!(matches!(e, RuntimeError::Unsupported { cap: "docker_flag", .. }), "{e}");
    }

    #[tokio::test]
    async fn caps_gate_rejects_host_port_without_support() {
        let mut caps = RuntimeCaps::docker();
        caps.host_ports = false;
        let rt = FakeRuntime::with_caps(caps);
        let spec = docker_args_to_spec("c1", &["-p", "127.0.0.1:35001:3306", "mysql:8.0"]).unwrap();
        let e = rt.create(&spec, &ExecMeta::none()).await.expect_err("应拒绝");
        assert!(matches!(e, RuntimeError::Unsupported { cap: "host_ports", .. }), "{e}");
    }

    #[tokio::test]
    async fn default_helpers_build_on_primitives() {
        // wait_mysql_ready 默认实现必须建立在 exec 上(driver 不必重复实现)
        let rt = FakeRuntime::new();
        rt.wait_mysql_ready("c1", "root", "p", 2).await.unwrap();
        let calls = rt.calls();
        assert!(calls.iter().any(|c| c.starts_with("exec:c1:mysql")), "{calls:?}");
    }
}
