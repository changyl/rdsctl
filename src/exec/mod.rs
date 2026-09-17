// rdsctl — 承载平台统一抽象层(WorkloadRuntime / NodeProvider)
//
// 目标(见 docs/container-platform-abstraction.md):
//   控制面对「实例工作负载跑在哪里」只认一层接口。承载平台可以是
//     物理机(本机/远端,容器或直装)· k8s · OpenStack · 自研平台。
//   接入新平台 = 新增一个 WorkloadRuntime 实现,或提供一个外部驱动可执行文件;
//   控制面(instance/dag/query/slow/capacity/api)不改动。
//
// 分层:
//   mod spec     ContainerSpec + docker CLI 参数解析器(兼容历史任务 JSON)
//   mod docker   DockerRuntime(本机/物理机 docker CLI;CLI 名可配 podman/nerdctl)
//   mod agent    AgentRuntime(任意远端物理机上的 WorkloadRuntime 端点)
//   mod external ExternalRuntime(JSON over stdio;平台方自备驱动,无需重编译控制面)
//   mod node     NodeProvider(节点供给:物理机纳管 / OpenStack 建虚机;见 P3)
//   mod fake     #[cfg(test)] 垫片(断言控制面发出的原语序列)
//
// 兼容:src/docker.rs 保留原有函数签名作为门面,内部经 `route_for_workload` 分发,
//   历史调用点零改动即获得平台无关性。

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

pub mod agent;
pub mod docker;
pub mod external;
pub mod spec;

#[cfg(test)]
pub mod fake;

pub use spec::{ContainerSpec, MountKind};

/// 执行面原语返回的 future(dyn 兼容:与 `Arc<dyn StoreBackend>` 的既有风格一致)
pub type RtFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// fence / 幂等元信息(设计 §9.2)。
///
/// 所有**变更类**原语都必须携带它:`ha` 的「拒绝过期持有者」语义不得因抽象而丢。
/// 物理机直连可忽略;agent / 外部驱动必须回传/校验。
#[derive(Clone, Debug, Default)]
pub struct ExecMeta {
    pub fence: Option<crate::ha::Fence>,
    pub idem: Option<String>,
}

/// fence 头(agent / 外部驱动线格式)
pub const HEADER_FENCE: &str = "x-rdsctl-fence";
/// 幂等键头
pub const HEADER_IDEM: &str = "x-rdsctl-idem";

impl ExecMeta {
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

    pub fn headers(&self) -> Vec<(String, String)> {
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

/// 驱动能力声明。
///
/// 驱动只声明、不实现的能力被调用时 → `RuntimeError::Unsupported`(fail-closed),
/// **绝不静默回落到本机 docker**。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeCaps {
    /// 宿主端口映射(docker `-p` / k8s hostPort / NodePort)
    pub host_ports: bool,
    /// 宿主路径 bind mount
    pub bind_mounts: bool,
    /// 引擎命名卷(docker volume;k8s 需 PVC 语义)
    pub named_volumes: bool,
    /// 自定义网络
    pub networks: bool,
    /// 进入工作负载执行命令(docker exec / kubectl exec)
    pub exec: bool,
    /// 读取日志
    pub logs: bool,
    /// 重命名工作负载(docker rename;k8s 通常不可变)
    pub rename: bool,
    /// systemd 直装形态(物理机无容器)
    pub systemd_unit: bool,
    /// 可透传原始 docker CLI 参数(`spec.extra`)。docker CLI 天然具备;
    /// 平台驱动必须显式声明,否则未知参数 fail-closed。
    pub raw_flags: bool,
}

impl RuntimeCaps {
    /// docker 语义:除 systemd 外全支持(与今天行为一致)
    pub fn docker() -> Self {
        Self {
            host_ports: true,
            bind_mounts: true,
            named_volumes: true,
            networks: true,
            exec: true,
            logs: true,
            rename: true,
            systemd_unit: false,
            raw_flags: true,
        }
    }

    /// 完全无能力(用于显式报错的后端)
    pub fn none() -> Self {
        Self {
            host_ports: false,
            bind_mounts: false,
            named_volumes: false,
            networks: false,
            exec: false,
            logs: false,
            rename: false,
            systemd_unit: false,
            raw_flags: false,
        }
    }
}

/// 执行面结构化错误。
///
/// `Platform(msg)` 的 `Display` 就是 `msg` 本身 —— docker driver 借此逐字保留历史
/// 错误文案(含 stderr),让既有验收断言与前端提示不变。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RuntimeError {
    /// 驱动未声明该能力(平台无关接入的 fail-closed 出口)
    Unsupported { cap: &'static str, detail: String },
    NotFound(String),
    Timeout(String),
    Transport(String),
    NodeNotReady(String),
    Platform(String),
}

impl RuntimeError {
    pub fn unsupported(cap: &'static str, detail: impl Into<String>) -> Self {
        RuntimeError::Unsupported {
            cap,
            detail: detail.into(),
        }
    }
}

impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RuntimeError::Unsupported { cap, detail } => {
                write!(f, "unsupported capability: {cap}({detail})")
            }
            RuntimeError::NotFound(m) => write!(f, "{m}"),
            RuntimeError::Timeout(m) => write!(f, "{m}"),
            RuntimeError::Transport(m) => write!(f, "{m}"),
            RuntimeError::NodeNotReady(m) => write!(f, "{m}"),
            RuntimeError::Platform(m) => write!(f, "{m}"),
        }
    }
}

impl std::error::Error for RuntimeError {}

/// 工作负载状态事实。
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContainerState {
    /// docker: running|exited|created|paused… ;k8s: Pending|Running|Succeeded|Failed
    pub status: String,
    pub exit_code: i64,
    pub restarts: u64,
    /// docker healthcheck 状态:healthy|unhealthy|starting|none;None = 平台无该概念
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health: Option<String>,
}

impl ContainerState {
    /// 线格式 `Status|ExitCode|RestartCount`(与历史 `/agent/state` 完全一致)
    pub fn wire(&self) -> String {
        format!("{}|{}|{}", self.status, self.exit_code, self.restarts)
    }

    /// 解析线格式(宽容:字段缺失按 0)
    pub fn parse_wire(s: &str) -> Self {
        let mut it = s.split('|');
        Self {
            status: it.next().unwrap_or("").trim().to_string(),
            exit_code: it.next().and_then(|v| v.trim().parse().ok()).unwrap_or(0),
            restarts: it.next().and_then(|v| v.trim().parse().ok()).unwrap_or(0),
            health: None,
        }
    }
}

/// 原生命令执行输出(不把非零退出码当错误 —— 调用方按码判定,如查询超时 124)
#[derive(Clone, Debug, Default)]
pub struct ExecOutput {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
}

/// 承载平台工作负载执行接口。
///
/// 必需方法即「平台无关原语」;`ext` 之上的默认实现(`is_healthy`/`wait_*`/SQL 原语/
/// 查询台)让 driver 尽量短 —— 新平台接入通常只需实现必需方法。
///
/// 新增 driver 的约定:`create` 第一行必须是 `crate::exec::check_caps(self, spec)?`,
/// 否则能力缺失会静默传到平台侧变成难查的行为差异。
pub trait WorkloadRuntime: Send + Sync {
    /// 驱动标识(docker / agent / external / …),用于日志与一致性校验
    fn kind(&self) -> &'static str;

    /// 能力声明
    fn caps(&self) -> RuntimeCaps;

    // ── 生命周期 ──
    fn create<'a>(
        &'a self,
        spec: &'a ContainerSpec,
        meta: &'a ExecMeta,
    ) -> RtFut<'a, Result<(), RuntimeError>>;

    fn start<'a>(&'a self, id: &'a str, meta: &'a ExecMeta) -> RtFut<'a, Result<(), RuntimeError>>;
    fn stop<'a>(&'a self, id: &'a str, meta: &'a ExecMeta) -> RtFut<'a, Result<(), RuntimeError>>;

    /// 重启类动作(默认 = stop + start)
    fn restart<'a>(&'a self, id: &'a str, meta: &'a ExecMeta) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move {
            self.stop(id, meta).await?;
            self.start(id, meta).await
        })
    }

    /// 删除工作负载;`purge_volumes` = 连数据卷一起删(对齐 `docker rm -f -v`)
    fn remove<'a>(
        &'a self,
        id: &'a str,
        purge_volumes: bool,
        meta: &'a ExecMeta,
    ) -> RtFut<'a, Result<(), RuntimeError>>;

    /// 重命名(默认:该平台不支持)
    fn rename<'a>(
        &'a self,
        _from: &'a str,
        _to: &'a str,
        _meta: &'a ExecMeta,
    ) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move {
            Err(RuntimeError::unsupported(
                "rename",
                format!("驱动 {} 不支持重命名工作负载", self.kind()),
            ))
        })
    }

    // ── 事实 ──
    fn exists<'a>(&'a self, id: &'a str) -> RtFut<'a, bool>;
    fn state<'a>(&'a self, id: &'a str) -> RtFut<'a, Option<ContainerState>>;
    /// healthcheck 事实:`Ok(None)` = 平台无健康检查概念(默认视为健康)
    fn health<'a>(&'a self, id: &'a str) -> RtFut<'a, Result<Option<String>, RuntimeError>> {
        let _ = id;
        Box::pin(async move { Ok(None) })
    }
    fn logs<'a>(&'a self, id: &'a str, tail: usize) -> RtFut<'a, Result<String, RuntimeError>>;

    // ── 容器内执行 ──
    /// 执行命令,非零退出码 → Err(平台各自措辞,保持历史文案)
    fn exec<'a>(&'a self, id: &'a str, argv: &'a [String]) -> RtFut<'a, Result<String, RuntimeError>>;
    /// 执行命令并保留退出码(查询台超时判定 124 等)
    fn exec_raw<'a>(
        &'a self,
        id: &'a str,
        argv: &'a [String],
    ) -> RtFut<'a, Result<ExecOutput, RuntimeError>>;
    fn write_file<'a>(
        &'a self,
        id: &'a str,
        path: &'a str,
        content: &'a str,
    ) -> RtFut<'a, Result<(), RuntimeError>>;

    // ── 网络(变更类:带 meta,cluster 模式经 agent 时受 fence 约束)──
    fn network_ensure<'a>(
        &'a self,
        name: &'a str,
        meta: &'a ExecMeta,
    ) -> RtFut<'a, Result<(), RuntimeError>>;
    fn network_remove<'a>(
        &'a self,
        name: &'a str,
        meta: &'a ExecMeta,
    ) -> RtFut<'a, Result<(), RuntimeError>>;

    // ── 接入点 ──
    /// 返回该规格的工作负载对外可达地址
    /// (`host_ip:host_port` 语义;无宿主端口的平台由驱动给出域名/NodePort)
    fn expose<'a>(&'a self, spec: &'a ContainerSpec) -> RtFut<'a, Result<String, RuntimeError>> {
        Box::pin(async move {
            let p = spec
                .ports
                .iter()
                .find(|p| p.host_port.is_some())
                .ok_or_else(|| RuntimeError::unsupported("host_ports", "该规格没有宿主端口映射"))?;
            let ip = p.host_ip.clone().unwrap_or_else(|| "127.0.0.1".to_string());
            Ok(format!("{ip}:{}", p.host_port.unwrap_or(0)))
        })
    }

    // ═════════════ 默认实现(平台无关,driver 无需重复实现) ═════════════

    /// 健康判定:有 healthcheck 要求 `healthy`;无 healthcheck(`none`/None)= 视为健康
    fn is_healthy<'a>(&'a self, id: &'a str) -> RtFut<'a, bool> {
        Box::pin(async move {
            match self.health(id).await {
                Ok(Some(h)) => h.eq_ignore_ascii_case("healthy") || h.eq_ignore_ascii_case("none"),
                Ok(None) => true,
                Err(_) => false,
            }
        })
    }

    /// 轮询健康,超时错误文案与历史 `docker.rs::wait_healthy` 一致
    fn wait_healthy<'a>(&'a self, id: &'a str, timeout_secs: u64) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
            while std::time::Instant::now() < deadline {
                if self.is_healthy(id).await {
                    return Ok(());
                }
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            let logs = self.logs(id, 30).await.unwrap_or_default();
            Err(RuntimeError::Timeout(format!(
                "容器 {id} 未在 {timeout_secs}s 内就绪\n最近日志:\n{logs}"
            )))
        })
    }

    /// 容器内执行 SQL(`mysql --protocol=TCP`;不依赖 socket 路径)。
    ///
    /// **变更类**:会改数据的 SQL(`SET GLOBAL`/`CHANGE REPLICATION SOURCE`/`STOP REPLICA`…)
    /// 走这里,受 fence 约束(agent 侧 `/agent/exec`、`/agent/sql` 属变更类)。
    fn exec_mysql_local<'a>(
        &'a self,
        id: &'a str,
        user: &'a str,
        pass: &'a str,
        sql: &'a str,
    ) -> RtFut<'a, Result<String, RuntimeError>> {
        Box::pin(async move {
            let argv = mysql_argv(user, pass, sql);
            self.exec(id, &argv).await
        })
    }

    /// 容器内执行**只读** SQL(巡检 / 事实 / 证据快照)。
    ///
    /// 与 `exec_mysql_local` 的 argv 完全相同,但经 `exec_raw` 落地:agent 侧
    /// `exec_raw` 属**只读类**(不要求 fence 头),因此巡检在实例租约之外
    /// (拿不到 fence)也能经 agent 工作。会改数据的 SQL 一律不得走这里。
    fn exec_mysql_ro<'a>(
        &'a self,
        id: &'a str,
        user: &'a str,
        pass: &'a str,
        sql: &'a str,
    ) -> RtFut<'a, Result<String, RuntimeError>> {
        Box::pin(async move {
            let argv = mysql_argv(user, pass, sql);
            let o = self.exec_raw(id, &argv).await?;
            if o.exit_code == 0 {
                Ok(o.stdout)
            } else if o.stderr.is_empty() {
                Err(RuntimeError::Platform(o.stdout))
            } else {
                Err(RuntimeError::Platform(o.stderr))
            }
        })
    }

    /// 等待容器内 MySQL 可接受连接(`SELECT 1` 探测;运行中 ≠ MySQL 就绪)
    fn wait_mysql_ready<'a>(
        &'a self,
        id: &'a str,
        user: &'a str,
        pass: &'a str,
        timeout_secs: u64,
    ) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
            while std::time::Instant::now() < deadline {
                if self.exec_mysql_local(id, user, pass, "SELECT 1").await.is_ok() {
                    return Ok(());
                }
                tokio::time::sleep(std::time::Duration::from_millis(800)).await;
            }
            let logs = self.logs(id, 30).await.unwrap_or_default();
            Err(RuntimeError::Timeout(format!(
                "容器 {id} 内 MySQL 未在 {timeout_secs}s 内就绪\n最近日志:\n{logs}"
            )))
        })
    }

    /// 容器内批量查询(dba-console-design §5.3):
    /// `mysql --batch --column-names` 输出 TSV,首行列名;
    /// 超时双保险:容器内 `timeout -s KILL`(退出码 124 → 超时)+ tokio 外层兜底。
    fn query_table<'a>(
        &'a self,
        id: &'a str,
        user: &'a str,
        pass: &'a str,
        sql: &'a str,
        timeout_secs: u64,
        default_db: &'a str,
    ) -> RtFut<'a, Result<String, RuntimeError>> {
        Box::pin(async move {
            let secs = timeout_secs.max(1);
            let sq = shell_sq(sql);
            let db_flag = if !default_db.is_empty()
                && default_db
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
            {
                format!(" -D {default_db}")
            } else {
                String::new()
            };
            let cmd = format!(
                "timeout -s KILL {secs} mysql --batch --column-names --default-character-set=utf8mb4 \
                 --connect-timeout=5 --protocol=TCP -u {user} -p{pass}{db_flag} -e {sq}"
            );
            let argv = vec!["sh".to_string(), "-c".to_string(), cmd];
            match tokio::time::timeout(
                std::time::Duration::from_secs(secs + 30),
                self.exec_raw(id, &argv),
            )
            .await
            {
                Ok(Ok(o)) => {
                    if o.exit_code == 0 {
                        Ok(o.stdout)
                    } else if o.exit_code == 124 {
                        Err(RuntimeError::Timeout("查询超时,已终止".to_string()))
                    } else if o.stderr.is_empty() {
                        Err(RuntimeError::Platform(o.stdout))
                    } else {
                        Err(RuntimeError::Platform(o.stderr))
                    }
                }
                // 原语层错误(如 CLI 缺失/远端不可达)
                Ok(Err(e)) => Err(RuntimeError::Platform(format!("查询进程执行失败: {e}"))),
                Err(_) => Err(RuntimeError::Timeout("查询超时,已终止".to_string())),
            }
        })
    }
}

/// 把驱动/agent 上报的能力名归一化到 `'static` 集合(cap 字段是 `&'static str`)
pub fn normalized_cap(raw: &str) -> &'static str {
    match raw {
        "host_ports" => "host_ports",
        "bind_mounts" => "bind_mounts",
        "named_volumes" => "named_volumes",
        "networks" => "networks",
        "exec" => "exec",
        "logs" => "logs",
        "rename" => "rename",
        "systemd_unit" => "systemd_unit",
        "raw_flags" => "raw_flags",
        "docker_flag" => "docker_flag",
        _ => "platform",
    }
}

/// 按 `{code, cap, error}` 构造结构化错误(外部驱动 / agent 协议共用)。
/// `code == "unsupported"` → `Unsupported`(fail-closed 语义可跨进程传递)。
pub fn runtime_error_from_wire(code: &str, cap: &str, msg: &str) -> RuntimeError {
    if code == "unsupported" {
        RuntimeError::unsupported(normalized_cap(cap), msg.to_string())
    } else if code == "timeout" {
        RuntimeError::Timeout(msg.to_string())
    } else if code == "not_found" {
        RuntimeError::NotFound(msg.to_string())
    } else if code == "node_not_ready" {
        RuntimeError::NodeNotReady(msg.to_string())
    } else {
        RuntimeError::Platform(msg.to_string())
    }
}

/// 把错误编码上线格式(agent 服务端返回给控制面)
pub fn error_code_of(e: &RuntimeError) -> (&'static str, &'static str) {
    match e {
        RuntimeError::Unsupported { cap, .. } => ("unsupported", cap),
        RuntimeError::Timeout(_) => ("timeout", ""),
        RuntimeError::NotFound(_) => ("not_found", ""),
        RuntimeError::Transport(_) => ("transport", ""),
        RuntimeError::NodeNotReady(_) => ("node_not_ready", ""),
        RuntimeError::Platform(_) => ("platform", ""),
    }
}

// ═════════════ 能力门禁(平台无关接入的 fail-closed 出口) ═════════════

/// 规格 × 驱动能力校验。任何 driver 的 `create` 都必须先调用它。
pub fn check_caps(rt: &dyn WorkloadRuntime, spec: &ContainerSpec) -> Result<(), RuntimeError> {
    let caps = rt.caps();
    if !caps.raw_flags && !spec.extra.is_empty() {
        return Err(RuntimeError::unsupported(
            "docker_flag",
            format!(
                "驱动 {} 无法透传参数 {:?}(如需支持请在驱动声明 raw_flags 后经 spec.extra 处理)",
                rt.kind(),
                spec.extra
            ),
        ));
    }
    if !caps.host_ports && spec.ports.iter().any(|p| p.host_port.is_some()) {
        return Err(RuntimeError::unsupported(
            "host_ports",
            format!("驱动 {} 不支持宿主端口映射(该平台需经 expose() 提供接入点)", rt.kind()),
        ));
    }
    if !caps.networks && spec.network.is_some() {
        return Err(RuntimeError::unsupported(
            "networks",
            format!("驱动 {} 不支持自定义网络", rt.kind()),
        ));
    }
    if !caps.bind_mounts && spec.mounts.iter().any(|m| m.kind == MountKind::HostPath) {
        return Err(RuntimeError::unsupported(
            "bind_mounts",
            format!("驱动 {} 不支持宿主路径挂载", rt.kind()),
        ));
    }
    if !caps.named_volumes && spec.mounts.iter().any(|m| m.kind == MountKind::NamedVolume) {
        return Err(RuntimeError::unsupported(
            "named_volumes",
            format!("驱动 {} 不支持命名卷", rt.kind()),
        ));
    }
    Ok(())
}

// ═════════════ 默认 / 路由 ═════════════

/// 进程默认执行后端:`RDSCTL_RUNTIME`(docker|external;默认 docker)。
pub fn default_runtime() -> Arc<dyn WorkloadRuntime> {
    let want = std::env::var("RDSCTL_RUNTIME").unwrap_or_default();
    runtime_for_kind(want.trim())
}

/// 按后端名构造执行后端(纯函数:便于单测与启动自检复用)。
///
/// 未知取值 → `BrokenRuntime`(每个原语都 fail-closed 报错),**不做静默回落**。
pub fn runtime_for_kind(want: &str) -> Arc<dyn WorkloadRuntime> {
    let w = want.to_ascii_lowercase();
    match w.as_str() {
        "" | "docker" | "local" => Arc::new(docker::DockerRuntime::from_env()),
        "external" => Arc::new(external_from_env()),
        _ => {
            tracing::error!(
                "RDSCTL_RUNTIME={w} 不是已实现的执行后端(docker|external);\
                 为避免误在本机 docker 上执行,本进程执行面一律拒绝(见 docs/ops-guide-container-platform.md)"
            );
            Arc::new(BrokenRuntime { want: w })
        }
    }
}

fn external_from_env() -> crate::exec::external::ExternalRuntime {
    crate::exec::external::ExternalRuntime::from_env()
}

/// 启动自检:默认执行后端是否可用。
///
/// - `docker`(默认):不探测(单机 lab 允许先起服务、后装 docker,保持历史行为)
/// - `external`:驱动必须是合法绝对路径;`caps` 探测失败只告警(能力按配置降级)
/// - 其它取值:直接失败(**不做静默回落**,避免误在本机 docker 上执行)
pub async fn preflight_default_runtime() -> Result<&'static str, RuntimeError> {
    let want = std::env::var("RDSCTL_RUNTIME")
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    match want.as_str() {
        "" | "docker" | "local" => Ok("docker"),
        "external" => {
            let rt = external::ExternalRuntime::from_env();
            rt.preflight()?;
            match rt.probe_caps().await {
                Ok(caps) => tracing::info!("外部驱动 {} 能力声明: {:?}", rt.cmd(), caps),
                Err(e) => tracing::warn!(
                    "外部驱动 {} caps 探测失败({e});按 RDSCTL_RUNTIME_CAPS/默认值继续",
                    rt.cmd()
                ),
            }
            Ok("external")
        }
        other => Err(RuntimeError::Platform(format!(
            "RDSCTL_RUNTIME={other} 未实现(已实现:docker|external);\
             接入指南见 docs/ops-guide-container-platform.md"
        ))),
    }
}

/// 按工作负载标识解析执行后端。
///
/// 只有**绑定了宿主机的**工作负载才需要特殊路由(其余一律走进程默认后端):
///   绑定 + agent 已接入 → AgentRuntime(在目标物理机上执行)
///   绑定 + agent 未接入 → Err(管理盲区,fail-closed)
///   未绑定             → 进程默认后端
pub fn route_for_workload(container: &str) -> Result<Arc<dyn WorkloadRuntime>, RuntimeError> {
    // 不触发管理器初始化:未初始化的进程(agent 模式 / 单测)按进程默认后端处理
    let Some(mgr) = crate::manager_opt() else {
        return Ok(default_runtime());
    };
    // 先克隆命中实例再查 host_list:避免持有 DashMap 分片读锁做 DB I/O(可能阻塞同分片写者)
    let bound = mgr
        .instances
        .iter()
        .find(|e| e.value().node_hosts.contains_key(container))
        .map(|e| e.value().clone());
    if let Some(inst) = bound {
        let hosts = mgr.store.host_list();
        let route = crate::instance::resolve_route_of(&inst, container, &hosts);
        return crate::instance::runtime_of_route(&route);
    }
    Ok(default_runtime())
}

/// 该工作负载归属的实例名(反查;仅读已初始化的管理器,不触发初始化)
pub fn owner_instance(container: &str) -> Option<String> {
    if container.is_empty() {
        return None;
    }
    let mgr = crate::manager_opt()?;
    if let Some(name) = mgr
        .instances
        .iter()
        .find(|e| owns_container(e.value(), container))
        .map(|e| e.value().name.clone())
    {
        return Some(name);
    }
    // 前缀兜底:replace_node 的临时容器 `{node}-rnx`、migrate 的 `{master}-mgn`
    // 不在实例 nodes/proxies 内,但命名仍锚定实例(`rds-{instance}-…`)——若不兜底,
    // 这些临时容器上的变更在 cluster+fence 模式下会拿不到 fence 而被拒绝。
    let names: Vec<String> = mgr.instances.iter().map(|e| e.value().name.clone()).collect();
    prefix_owner(names.iter().map(|s| s.as_str()), container)
}

/// 唯一的 `rds-{instance}-` 前缀匹配;歧义(一个实例名是另一个的 `-` 前缀)一律 None。
///
/// fail-closed 取向:宁可不给 fence(agent 会拒绝变更)也不能给错 fence
/// (错 fence 会被落到别的容器的守卫键上,削弱单调性保护)。
fn prefix_owner<'a>(names: impl Iterator<Item = &'a str>, container: &str) -> Option<String> {
    let mut hit: Option<String> = None;
    for n in names {
        let exact = format!("rds-{n}");
        let prefixed = format!("rds-{n}-");
        if container == exact || container.starts_with(&prefixed) {
            match &hit {
                None => hit = Some(n.to_string()),
                Some(prev) if prev == n => {}
                Some(_) => return None,
            }
        }
    }
    hit
}

fn owns_container(i: &crate::instance::RdsInstance, c: &str) -> bool {
    i.node_hosts.contains_key(c)
        || i.nodes.iter().any(|n| n.container == c)
        || i.proxies.iter().any(|p| p.container == c)
        || i.proxy_container == c
        || i.lvs_container == c
}

/// 当前持有该工作负载的 fence / 幂等元信息(无租约 → 全空)。
///
/// **为什么需要它**:cluster 模式下执行面经 agent 落地(设计 §9.1/A4,取消 `Local`
/// 直连特权),而 agent 对变更类原语强制 fence。若靠 60+ 个调用点各自手传 fence,
/// 必然漏传 → 线上表现为「某个操作突然 409」。因此变更加载统一在这里按
/// 「工作负载 → 归属实例 → 实例当前租约 fence」自动解析:
///   - 有租约(正在执行实例操作)→ 自动带上 fence,agent 放行;
///   - 无租约(操作之外)→ 拿不到 fence,agent **拒绝变更**(这正是 A4 的意图,失败即安全)。
pub fn meta_for_workload(container: &str) -> ExecMeta {
    let Some(inst) = owner_instance(container) else {
        return ExecMeta::none();
    };
    // owner_instance 命中 ⇒ 管理器必然已初始化,这里不会再触发初始化
    match crate::manager_opt() {
        Some(mgr) => mgr.call_meta(&inst),
        None => ExecMeta::none(),
    }
}

/// 容器内 mysql 客户端 argv(只读/变更两条路径共用,保证语义一致)
pub fn mysql_argv(user: &str, pass: &str, sql: &str) -> Vec<String> {
    vec![
        "mysql".to_string(),
        "-N".to_string(),
        "--protocol=TCP".to_string(),
        "-u".to_string(),
        user.to_string(),
        format!("-p{pass}"),
        "-e".to_string(),
        sql.to_string(),
    ]
}

/// shell 单引号转义(把 SQL 安全地嵌入 `sh -c` 作为单个 argv)
pub fn shell_sq(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        if c == '\'' {
            out.push_str("'\\''");
        } else {
            out.push(c);
        }
    }
    out.push('\'');
    out
}

// ═════════════ 显式报错后端(未知 RDSCTL_RUNTIME) ═════════════

/// 未知 `RDSCTL_RUNTIME` 的占位后端:所有原语 fail-closed,杜绝静默落到本机 docker。
pub struct BrokenRuntime {
    pub want: String,
}

impl BrokenRuntime {
    fn err(&self) -> RuntimeError {
        RuntimeError::Platform(format!(
            "RDSCTL_RUNTIME={} 未实现;本进程执行面拒绝所有操作(已实现:docker|external)",
            self.want
        ))
    }
}

impl WorkloadRuntime for BrokenRuntime {
    fn kind(&self) -> &'static str {
        "broken"
    }

    fn caps(&self) -> RuntimeCaps {
        RuntimeCaps::none()
    }

    fn create<'a>(
        &'a self,
        _spec: &'a ContainerSpec,
        _meta: &'a ExecMeta,
    ) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move { Err(self.err()) })
    }

    fn start<'a>(&'a self, _id: &'a str, _meta: &'a ExecMeta) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move { Err(self.err()) })
    }

    fn stop<'a>(&'a self, _id: &'a str, _meta: &'a ExecMeta) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move { Err(self.err()) })
    }

    fn remove<'a>(
        &'a self,
        _id: &'a str,
        _purge: bool,
        _meta: &'a ExecMeta,
    ) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move { Err(self.err()) })
    }

    fn exists<'a>(&'a self, _id: &'a str) -> RtFut<'a, bool> {
        Box::pin(async move { false })
    }

    fn state<'a>(&'a self, _id: &'a str) -> RtFut<'a, Option<ContainerState>> {
        Box::pin(async move { None })
    }

    fn logs<'a>(&'a self, _id: &'a str, _tail: usize) -> RtFut<'a, Result<String, RuntimeError>> {
        Box::pin(async move { Err(self.err()) })
    }

    fn exec<'a>(&'a self, _id: &'a str, _argv: &'a [String]) -> RtFut<'a, Result<String, RuntimeError>> {
        Box::pin(async move { Err(self.err()) })
    }

    fn exec_raw<'a>(
        &'a self,
        _id: &'a str,
        _argv: &'a [String],
    ) -> RtFut<'a, Result<ExecOutput, RuntimeError>> {
        Box::pin(async move { Err(self.err()) })
    }

    fn write_file<'a>(
        &'a self,
        _id: &'a str,
        _path: &'a str,
        _content: &'a str,
    ) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move { Err(self.err()) })
    }

    fn network_ensure<'a>(
        &'a self,
        _name: &'a str,
        _meta: &'a ExecMeta,
    ) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move { Err(self.err()) })
    }

    fn network_remove<'a>(
        &'a self,
        _name: &'a str,
        _meta: &'a ExecMeta,
    ) -> RtFut<'a, Result<(), RuntimeError>> {
        Box::pin(async move { Err(self.err()) })
    }
}

#[cfg(test)]
mod route_tests {
    use super::*;

    /// 前缀兜底必须**唯一匹配**才返回:fence 给错实例比不给更危险
    #[test]
    fn prefix_owner_requires_unique_match() {
        let names = ["a", "b"];
        assert_eq!(
            prefix_owner(names.iter().copied(), "rds-a-master"),
            Some("a".to_string())
        );
        assert_eq!(
            prefix_owner(names.iter().copied(), "rds-a-slave-1-rnx"),
            Some("a".to_string()),
            "replace_node 的临时容器也要能定位归属"
        );
        assert_eq!(prefix_owner(names.iter().copied(), "rds-a"), Some("a".to_string()));
        assert_eq!(prefix_owner(names.iter().copied(), "rds-c-master"), None);
        // 不加 '-' 前缀的相似名不算匹配(msh vs msh1)
        assert_eq!(prefix_owner(names.iter().copied(), "rds-a1-master"), None);

        // 歧义:一个实例名是另一个的 '-' 前缀 → 宁可不给
        let amb = ["a", "a-b"];
        assert_eq!(prefix_owner(amb.iter().copied(), "rds-a-b-master"), None);
        // 但明确的单匹配仍然工作
        assert_eq!(
            prefix_owner(amb.iter().copied(), "rds-a-xenon1"),
            Some("a".to_string())
        );
    }

    /// 元信息合成:无管理器(单测/agent 进程)时不得触发初始化,返回空元信息
    #[test]
    fn meta_for_workload_is_inert_without_manager() {
        let m = meta_for_workload("rds-nonexistent-master");
        assert!(m.fence.is_none() && m.idem.is_none());
        assert!(meta_for_workload("").fence.is_none());
    }

    #[test]
    fn readonly_and_mutating_sql_build_the_same_argv() {
        let a = mysql_argv("root", "pw", "SELECT 1");
        assert_eq!(
            a,
            vec!["mysql", "-N", "--protocol=TCP", "-u", "root", "-ppw", "-e", "SELECT 1"]
        );
    }
}
