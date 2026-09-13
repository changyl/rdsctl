// rdsctl — 实例模型与生命周期编排(Step 化,可持久化/恢复)
//
// 实例 = 专属 docker 网络 + MySQL 主 + N 从 + newproxy 代理。
// 生命周期(DAG 任务)由**可序列化 Step** 构成(见 dag::Step),执行器
// exec_step 在本文件实现;配合 SQLite 持久化,进程重启后任务与实例状态可恢复。
//
// 状态机:creating → running ⇄ paused/scaling/switchover/maintenance → running
//                ↘ failed(任务失败/巡检降级)      running → destroying → destroyed
// 实例级操作锁:任一生命周期任务提交即占锁,终态释放,拒绝并发操作。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use crate::dag::{Step, StepCtx, StepExecutor, TaskNode, TaskScheduler};
use crate::ha::Fence;
use crate::docker as dk;
use crate::store::Store;

/// 默认镜像(docker hub)。离线/受限网络可经环境变量覆盖为国内镜像源或内网 registry:
///   RDSCTL_MYSQL_IMAGE  RDSCTL_PROXY_IMAGE
/// 例(daocloud 加速):  RDSCTL_MYSQL_IMAGE=docker.m.daocloud.io/library/mysql:8.0
const MYSQL_IMAGE: &str = "mysql:8.0";
const PROXY_IMAGE: &str = "perf-2shard-newproxy:latest";
/// xenon(raft 高可用)节点镜像:xenon 项目 deploy/build.sh 产物(MySQL8 + xenon 同容器,
/// entrypoint 渲染 xenon.json/my.cnf、初始化 datadir、种 peers.json)。
/// 构建方式:在 xenon 项目下执行 deploy/xenon.sh build(产物镜像名 xenon-local:latest)。
const XENON_IMAGE: &str = "xenon-local:latest";
/// xenon 节点容器内 RPC 端口(raft endpoint 端口;对齐 xenon deploy/env.sh:容器内固定 8801)
const XENON_RPC_PORT: u16 = 8801;

/// 读取可覆盖镜像名(未设置时回退默认)
fn image_of(env_key: &str, def: &str) -> String {
    std::env::var(env_key).unwrap_or_else(|_| def.to_string())
}
pub(crate) const ROOT_PASS: &str = "rds_root_2024";
const REPL_PASS: &str = "rds_repl_2024";
pub(crate) const APP_DB: &str = "appdb";

/// 实例状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InstStatus {
    Creating,
    Running,
    Paused,
    Scaling,
    Switching,
    Maintenance,
    Degraded,
    Destroying,
    Destroyed,
    Failed,
}

impl InstStatus {
    pub fn label(self) -> &'static str {
        match self {
            InstStatus::Creating => "创建中",
            InstStatus::Running => "运行中",
            InstStatus::Paused => "已暂停",
            InstStatus::Scaling => "扩容中",
            InstStatus::Switching => "切换中",
            InstStatus::Maintenance => "维护中",
            InstStatus::Degraded => "降级(异常)",
            InstStatus::Destroying => "销毁中",
            InstStatus::Destroyed => "已销毁",
            InstStatus::Failed => "失败",
        }
    }
}

/// 节点角色
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Master,
    /// 在线读从(挂代理/读流量)
    Read,
    /// 离线从(备份/统计/大查询等离线任务共用;每实例仅一个)
    Offline,
    /// 历史兼容(旧数据);离线类,新扩容一律写入 Offline
    Stats,
    Backup,
}

impl Role {
    pub fn label(self) -> &'static str {
        match self {
            Role::Master => "主",
            Role::Read => "从(读)",
            Role::Offline => "从(离线:备份/统计/大查询)",
            Role::Stats => "从(离线)(历史)",
            Role::Backup => "从(离线)(历史)",
        }
    }

    /// 是否属于"离线任务用从"(备份/统计/大查询)——此类节点每实例仅允许一个
    pub fn is_offline(self) -> bool {
        matches!(self, Role::Offline | Role::Stats | Role::Backup)
    }
}

/// 实例内的 MySQL 节点
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstNode {
    pub container: String,
    pub role: Role,
    pub host: String,
    pub port: u16,
    pub host_port: u16,
    pub server_id: u64,
    /// 节点所属 region/az(跨区拓扑展示;缺省时取实例级,前端按需回退)
    #[serde(default)]
    pub region: String,
    #[serde(default)]
    pub az: String,
    /// 节点归属分片(多分片实例用;空=实例级 shard)
    #[serde(default)]
    pub shard: String,
    /// 复制上游(级联/跨区链;master 为空,从节点为复制源容器名)
    #[serde(default)]
    pub parent: String,
    /// xenon(raft)节点:宿主侧 RPC 端口映射(127.0.0.1:{rpc_host_port} → 容器 8801);
    /// 非 xenon 架构为 0(serde default 向后兼容旧记录)
    #[serde(default)]
    pub rpc_host_port: u16,
}

/// 代理实例(集群组内成员;1..N)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyNode {
    #[serde(default)]
    pub container: String,
    #[serde(default)]
    pub mysql_port: u16,
    #[serde(default)]
    pub mng_port: u16,
    // 运维指标(代理节点卡展示;缺省为 0/空 → 前端显示 “—”)
    #[serde(default)]
    pub spec: String,
    #[serde(default)]
    pub ip: String,
    #[serde(default)]
    pub qps: u64,
    #[serde(default)]
    pub conns: u64,
    #[serde(default)]
    pub cpu: f64,
    #[serde(default)]
    pub status: String, // running | stopped | unknown
    /// 该代理节点实际运行的 Proxy 版本(灰度发布时可能部分节点已升级;空 = 沿用实例 proxy_version)
    #[serde(default)]
    pub version: String,
}

/// 分片内的一个从节点引用(供拓扑/监控完整展示该分片全部从库)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardSlaveRef {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub role: String, // read | offline
}

/// 分片信息(分片集群:每分片 = 主 + 若干从;用于拓扑分片列表/监控)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardInfo {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub data_range: String,
    #[serde(default)]
    pub master: String,
    #[serde(default)]
    pub slave: String,
    #[serde(default)]
    pub slaves: Vec<ShardSlaveRef>, // 完整从节点列表(读从/离线从…),向后兼容缺失时为空
    #[serde(default)]
    pub lag_ms: u64,
    #[serde(default)]
    pub storage_pct: u8,
    #[serde(default)]
    pub health: String, // ok | lag | bad
}

/// 创建参数(创建时选择/填写;创建后仍可在详情页修改)
#[derive(Debug, Clone)]
pub struct CreateOpts {
    pub itype: String, // single | async | sync | xenon(raft 高可用) | distributed(需引擎模板)
    pub proxies: u32,  // 默认 2 = 代理集群
    pub region: String,
    pub az: String,
    pub biz: String,
    pub contact: String,
    pub dba: String,
    pub core: bool,
    pub spec: String,
    pub shard_num: u64,
    pub data_size: String,
    pub buffer_pool: String,
    pub max_qps: u64,
    pub max_tps: u64,
    pub mysql_version: String,
    pub proxy_version: String,
    /// 创建实例时是否同时为从节点登记 DTS 占位链路(canal)
    pub dts: bool,
    /// 随建 DTS 的占位规格(如 2C4G;空 = 默认 2C4G)
    pub dts_spec: String,
    /// xenon 架构:raft 集群节点数(默认 3;>=3 才有容错,奇数推荐)
    pub xenon_nodes: u32,
    /// xenon 架构:前置 rsproxy(newproxy)代理实例数(0=不部署代理,业务直连节点;
    /// 1..=4 = 在 xenon 集群前部署 rsproxy,自动发现 raft leader 并提供读分流)。
    /// 推荐 2(与主从架构的 proxy 集群对齐,组成高可用接入层)
    pub xenon_proxy: u32,
    /// xenon 架构:rsproxy 读一致性档位(strong|causal|session|eventual;默认 strong)。
    /// 只作用于"可分流纯读":strong=全走 leader(线性化);causal=高水位 GTID 屏障(跨客户端因果);
    /// session=会话水位屏障(读己之写/单调读);eventual=免屏障直读 follower(业务自担)。
    /// 见 rsproxy docs/15-xenon-ha.md §4
    pub xenon_consistency: String,
}

impl Default for CreateOpts {
    fn default() -> Self {
        Self {
            itype: "async".to_string(),
            proxies: 2,
            region: String::new(),
            az: String::new(),
            biz: String::new(),
            contact: String::new(),
            dba: String::new(),
            core: false,
            spec: String::new(),
            shard_num: 1,
            data_size: String::new(),
            buffer_pool: String::new(),
            max_qps: 0,
            max_tps: 0,
            mysql_version: String::new(),
            proxy_version: String::new(),
            dts: false,
            dts_spec: String::new(),
            xenon_nodes: 3,
            xenon_proxy: 0,
            xenon_consistency: "strong".to_string(),
        }
    }
}

/// RDS 实例
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RdsInstance {
    pub name: String,
    pub status: InstStatus,
    /// 所属可用区/机房(跨区主从数据面,见 docs/scaling-design.md)
    #[serde(default)]
    pub region: String,
    #[serde(default)]
    pub az: String,
    #[serde(default)]
    pub shard: String,
    /// 归属租户(占位;M0 仅落模型与审计归属,权限后续里程碑启用)
    #[serde(default)]
    pub tenant: String,
    /// 管理启停(S-批量):false=停用(禁止常规变更,仅允许启用/销毁清理);默认 true
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// LVS/接入 VIP(负载入口,1..N;创建时登记,形如 "127.0.0.1:{lvs_mysql_port}")
    #[serde(default)]
    pub lvs: Vec<String>,
    /// LVS 接入层标识(rds-{name}-lvs;v0 为进程内转发器而非容器,见 docs/lvs-v0.md)
    #[serde(default)]
    pub lvs_container: String,
    /// LVS 接入层端口(业务入口统一走该端口转发到 Proxy 集群)
    #[serde(default)]
    pub lvs_mysql_port: u16,
    /// 节点级健康状态事实(巡检写入): container → ok|stopped|down|missing|repl_down;
    /// 供拓扑/分片列表/监控显示“哪个节点故障”;空=尚未巡检/旧实例
    #[serde(default)]
    pub node_states: std::collections::HashMap<String, String>,
    /// 节点 → 宿主机绑定(物理机/跨机模型,见 docs/physical-multi-site-ops.md §5):
    /// container → host name;空/无键 = 未绑定(本机直连)。绑定为登记事实,执行路由见 node_host_binding。
    #[serde(default)]
    pub node_hosts: std::collections::HashMap<String, String>,
    /// 实例级自动故障转移开关(vtorc 式 ERS;默认开,可经 meta 接口关闭)
    #[serde(default = "default_true")]
    pub auto_failover: bool,
    /// 代理集群组成员(1..N;空时前端回退单代理 legacy 字段)
    #[serde(default)]
    pub proxies: Vec<ProxyNode>,
    /// 分片列表(分片集群;空=由前端按节点/实例级 shard 收敛展示)
    #[serde(default)]
    pub shards: Vec<ShardInfo>,
    // ── 运营元数据(可编辑,instances.manage) ──
    #[serde(default)]
    pub biz: String, // 业务线
    #[serde(default)]
    pub contact: String, // 业务线接口人
    #[serde(default)]
    pub dba: String, // DBA 负责人
    #[serde(default)]
    pub core: bool, // 是否核心实例
    #[serde(default)]
    pub itype: String, // 类型: single | async | sync | distributed(TiDB/OceanBase…)
    #[serde(default)]
    pub mysql_version: String,
    #[serde(default)]
    pub proxy_version: String,
    // ── 基础配置(展示/维护) ──
    #[serde(default)]
    pub spec: String, // 规格,如 8C16G
    #[serde(default)]
    pub shard_num: u64, // 分片数
    #[serde(default)]
    pub data_size: String, // 当前数据量,如 128GiB
    #[serde(default)]
    pub buffer_pool: String, // buffer pool,如 8GiB
    #[serde(default)]
    pub max_qps: u64,
    #[serde(default)]
    pub max_tps: u64,
    pub network: String,
    pub nodes: Vec<InstNode>,
    pub proxy_container: String,
    pub proxy_mysql_port: u16,
    pub proxy_mng_port: u16,
    pub created_at: u64,
    pub root_password: String,
    /// 查询台专用账号口令(懒生成并随实例记录持久化;任何视图/审计均不暴露,见 docs/dba-console-design.md)
    #[serde(default)]
    pub query_secret: String,
    /// 最近一次异常原因(巡检/任务失败)
    pub last_error: String,
}

impl RdsInstance {
    pub fn master(&self) -> Option<&InstNode> {
        self.nodes.iter().find(|n| n.role == Role::Master)
    }
    pub fn slaves(&self) -> Vec<&InstNode> {
        self.nodes
            .iter()
            .filter(|n| n.role != Role::Master)
            .collect()
    }
    pub fn to_view(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name,
            "status": self.status,
            "status_label": self.status.label(),
            "region": self.region,
            "az": self.az,
            "shard": self.shard,
            "tenant": self.tenant,
            "enabled": self.enabled,
            "network": self.network,
            "nodes": self.nodes.iter().map(|n| serde_json::json!({
                "container": n.container,
                "role": n.role,
                "role_label": n.role.label(),
                "host": n.host,
                "port": n.port,
                "host_port": n.host_port,
                "server_id": n.server_id,
                "region": n.region,
                "az": n.az,
                "shard": n.shard,
                "parent": n.parent,
                "rpc_host_port": n.rpc_host_port,
            })).collect::<Vec<_>>(),
            "proxy": {
                "container": self.proxy_container,
                "mysql_port": self.proxy_mysql_port,
                "mng_port": self.proxy_mng_port,
            },
            "lvs": self.lvs,
            "lvs_container": self.lvs_container,
            "lvs_mysql_port": self.lvs_mysql_port,
            "node_states": self.node_states,
            "node_hosts": self.node_hosts,
            "auto_failover": self.auto_failover,
            "biz": self.biz, "contact": self.contact, "dba": self.dba,
            "core": self.core, "itype": self.itype,
            "mysql_version": self.mysql_version, "proxy_version": self.proxy_version,
            "spec": self.spec, "shard_num": self.shard_num, "data_size": self.data_size,
            "buffer_pool": self.buffer_pool, "max_qps": self.max_qps, "max_tps": self.max_tps,
            "proxies": self.proxies.iter().map(|p| serde_json::json!({
                "container": p.container,
                "mysql_port": p.mysql_port,
                "mng_port": p.mng_port,
                "spec": p.spec, "ip": p.ip, "qps": p.qps, "conns": p.conns, "cpu": p.cpu, "status": p.status,
                "version": p.version,
            })).collect::<Vec<_>>(),
            "shards": self.shards,
            "created_at": self.created_at,
            "root_password": self.root_password,
            "last_error": self.last_error,
        })
    }
}

// ─── 执行面抽象:节点宿主绑定(物理机/跨机模型,见 docs/physical-multi-site-ops.md §5.4) ───

/// 节点绑定的宿主机名:读 instance.node_hosts(container → host);空/未绑定 = 本机直连。
pub fn node_host_binding<'a>(inst: &'a RdsInstance, container: &str) -> Option<&'a str> {
    inst.node_hosts
        .get(container)
        .map(|s| s.as_str())
        .filter(|s| !s.is_empty())
}

/// 执行路由计划:未绑定 → "local"(本机 docker);绑定宿主机 → "remote"(远端 agent,本阶段未接入)。
/// 纯函数,供巡检/事实层判断与后续 agent 路由复用。
#[allow(dead_code)] // 当前由测试断言语义;P2 远端 agent 路由启用
pub fn host_exec_plan(host: Option<&str>) -> &'static str {
    if host.is_some() {
        "remote"
    } else {
        "local"
    }
}

/// 节点执行路由(P2-① 远端 agent):决定一次容器操作在本机 docker 还是交给远端 agent。
#[derive(Clone, Debug)]
pub enum NodeRoute {
    /// 未绑定 → 本机 docker 直连
    Local,
    /// 绑定宿主机且该机登记了 agent(agent_port>0)→ 经 agent 执行
    Agent {
        host: String,
        ag: crate::agent::Agent,
    },
    /// 绑定宿主机但 agent 未接入(未登记/agent_port=0)→ 管理盲区,不做本机执行
    Unmanaged { host: String },
}

/// 解析节点执行路由:需要 hosts 清单(store.host_list),见 resolve_route_of。
pub fn resolve_route_of(
    inst: &RdsInstance,
    container: &str,
    hosts: &[serde_json::Value],
) -> NodeRoute {
    use crate::agent::Agent;
    match node_host_binding(inst, container) {
        None => NodeRoute::Local,
        Some(h) => match hosts.iter().find(|r| r["name"].as_str() == Some(h)) {
            Some(hr) => {
                let p = hr["agent_port"].as_u64().unwrap_or(0) as u16;
                if p == 0 {
                    NodeRoute::Unmanaged {
                        host: h.to_string(),
                    }
                } else {
                    let ip = hr["ip"].as_str().unwrap_or("127.0.0.1");
                    NodeRoute::Agent {
                        host: h.to_string(),
                        ag: Agent::new(crate::agent::agent_url(ip, p), crate::agent::token_opt()),
                    }
                }
            }
            None => NodeRoute::Unmanaged {
                host: h.to_string(),
            },
        },
    }
}

impl NodeRoute {
    pub fn label(&self) -> &'static str {
        match self {
            NodeRoute::Local => "local",
            NodeRoute::Agent { .. } => "agent",
            NodeRoute::Unmanaged { .. } => "unmanaged",
        }
    }
}

// ─── 按路由的容器/MySQL 原语(本机 dk::* ↔ 远端 agent 同语义) ───

async fn r_exists(r: &NodeRoute, container: &str) -> bool {
    match r {
        NodeRoute::Local => dk::exists(container).await,
        NodeRoute::Agent { ag, .. } => ag.exists(container).await,
        NodeRoute::Unmanaged { .. } => false,
    }
}

async fn r_state(r: &NodeRoute, container: &str) -> Option<String> {
    match r {
        NodeRoute::Local => dk::container_state(container).await,
        NodeRoute::Agent { ag, .. } => ag.container_state(container).await,
        NodeRoute::Unmanaged { .. } => None,
    }
}

async fn r_sql(
    r: &NodeRoute,
    container: &str,
    user: &str,
    pass: &str,
    sql: &str,
) -> Result<String, String> {
    match r {
        NodeRoute::Local => dk::exec_mysql_local(container, user, pass, sql).await,
        NodeRoute::Agent { ag, .. } => ag.exec_mysql_local(container, user, pass, sql).await,
        NodeRoute::Unmanaged { .. } => Err("节点绑定宿主机但 agent 未接入,无法执行".to_string()),
    }
}

/// 节点级健康探测(按路由;与巡检语义一致,见 sweep_once)
async fn probe_node(r: &NodeRoute, n: &InstNode) -> (String, Option<String>) {
    if !r_exists(r, &n.container).await {
        return (
            "missing".to_string(),
            Some(format!("节点 {} 容器缺失", n.container)),
        );
    }
    let state = r_state(r, &n.container).await.unwrap_or_default();
    let st0 = state.split('|').next().unwrap_or("").trim();
    // 状态不可读(空串,如测试垫片/旧 docker 插件)时以 SQL 探测为准;
    // 仅当明确返回非 running 状态(stopped/exited/paused…)才判为“服务已停止”
    if !st0.is_empty() && !st0.eq_ignore_ascii_case("running") {
        return (
            "stopped".to_string(),
            Some(format!("节点 {} 服务已停止({st0})", n.container)),
        );
    }
    if r_sql(r, &n.container, "root", ROOT_PASS, "SELECT 1")
        .await
        .is_err()
    {
        return (
            "down".to_string(),
            Some(format!("从库/节点 {} MySQL 不可达", n.container)),
        );
    }
    if n.role != Role::Master {
        if let Some(p) = replica_problem_route(r, &n.container).await {
            return ("repl_down".to_string(), Some(p));
        }
    }
    ("ok".to_string(), None)
}

/// 巡检用探测入口:Agent 路由先 ping,agent 失联 = 管理盲区(remote),不算容器缺失/down
async fn probe_route(route: &NodeRoute, n: &InstNode) -> (String, Option<String>) {
    if let NodeRoute::Agent { ag, host } = route {
        if !ag.ping().await {
            return (
                "remote".to_string(),
                Some(format!(
                    "节点 {} 绑定宿主机 {host},agent 不可达(已跳过本机检查)",
                    n.container
                )),
            );
        }
    }
    probe_node(route, n).await
}

/// xenon 节点健康探测(与 probe_node 同结构,但无复制线程探针——
/// raft 节点没有 MySQL IO/SQL 复制线程,主从语义的 repl_down 判定不适用)
async fn probe_xenon_node(r: &NodeRoute, n: &InstNode) -> (String, Option<String>) {
    if !r_exists(r, &n.container).await {
        return (
            "missing".to_string(),
            Some(format!("节点 {} 容器缺失", n.container)),
        );
    }
    let state = r_state(r, &n.container).await.unwrap_or_default();
    let st0 = state.split('|').next().unwrap_or("").trim();
    if !st0.is_empty() && !st0.eq_ignore_ascii_case("running") {
        return (
            "stopped".to_string(),
            Some(format!("节点 {} 服务已停止({st0})", n.container)),
        );
    }
    if r_sql(r, &n.container, "root", ROOT_PASS, "SELECT 1")
        .await
        .is_err()
    {
        return (
            "down".to_string(),
            Some(format!("节点 {} MySQL 不可达", n.container)),
        );
    }
    ("ok".to_string(), None)
}

// ─── 按路由的容器写原语(本机 dk::* ↔ 远端 agent 同语义;replace_node 等工作流用) ───

async fn r_run(r: &NodeRoute, container: &str, args: &[&str]) -> Result<(), String> {
    match r {
        NodeRoute::Local => dk::run(container, args).await,
        NodeRoute::Agent { ag, .. } => ag.run(container, args).await,
        NodeRoute::Unmanaged { host } => {
            Err(format!("节点绑定宿主机 {host},agent 未接入,无法执行"))
        }
    }
}

/// 删除容器(不存在则忽略;幂等)
async fn r_rm(r: &NodeRoute, container: &str) -> Result<(), String> {
    if !r_exists(r, container).await {
        return Ok(());
    }
    match r {
        NodeRoute::Local => dk::rm(container).await,
        NodeRoute::Agent { ag, .. } => ag.rm(container).await,
        NodeRoute::Unmanaged { host } => {
            Err(format!("节点绑定宿主机 {host},agent 未接入,无法执行"))
        }
    }
}

/// docker rename
async fn r_rename(r: &NodeRoute, from: &str, to: &str) -> Result<(), String> {
    match r {
        NodeRoute::Local => dk::docker(&["rename", from, to]).await.map(|_| ()),
        NodeRoute::Agent { ag, .. } => ag.docker(&["rename", from, to]).await.map(|_| ()),
        NodeRoute::Unmanaged { host } => {
            Err(format!("节点绑定宿主机 {host},agent 未接入,无法执行"))
        }
    }
}

/// 轮询容器内 MySQL 就绪(按路由)
async fn wait_mysql_route(
    r: &NodeRoute,
    container: &str,
    user: &str,
    pass: &str,
    timeout_secs: u64,
) -> Result<(), String> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
    while std::time::Instant::now() < deadline {
        if r_sql(r, container, user, pass, "SELECT 1").await.is_ok() {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(800)).await;
    }
    Err(format!(
        "节点 {container} 内 MySQL 未在 {timeout_secs}s 内就绪(按路由 {})",
        r.label()
    ))
}

/// 目标宿主机 → 执行路由(host 空 = 本机;非空须在 Host 注册表且 agent 已接入)
fn target_route(host: &str) -> Result<NodeRoute, String> {
    use crate::agent::Agent;
    let host = host.trim();
    if host.is_empty() {
        return Ok(NodeRoute::Local);
    }
    let hosts = crate::manager().store.host_list();
    match hosts.iter().find(|r| r["name"].as_str() == Some(host)) {
        Some(hr) => {
            let p = hr["agent_port"].as_u64().unwrap_or(0) as u16;
            if p == 0 {
                Err(format!("宿主机 {host} 未接入 agent(agent_port=0)"))
            } else {
                let ip = hr["ip"].as_str().unwrap_or("127.0.0.1");
                Ok(NodeRoute::Agent {
                    host: host.to_string(),
                    ag: Agent::new(crate::agent::agent_url(ip, p), crate::agent::token_opt()),
                })
            }
        }
        None => Err(format!("宿主机 {host} 未登记")),
    }
}

/// RDS 管理器(全局单例):实例注册表 + 任务调度 + 持久化 + 操作锁
/// 实例级互斥的权威来源(设计 §5/D2)
///
/// - `Store`:单机模式,DB 行锁(今天的行为,逐字保留);
/// - `Cluster`:集群模式,由**共识租约**裁定 holder,fence 随授予前移并被执行面强制。
#[derive(Clone)]
pub enum Authority {
    Store,
    Cluster(Arc<crate::ha::runtime::ClusterRuntime>),
}

pub struct RdsManager {
    pub scheduler: Arc<TaskScheduler>,
    pub store: Arc<Store>,
    pub instances: DashMap<String, RdsInstance>,
    next_port: AtomicU64,
    next_server_id: AtomicU64,
    /// 实例操作锁(生命周期任务提交时占用,终态释放)
    op_locks: DashMap<String, ()>,
    /// 本控制端标识(lease holder;多副本/多进程互斥,M0)
    holder: String,
    /// lease 时长(秒,默认 30;watch_task 每轮续约)
    lock_lease: u64,
    /// 互斥权威(单机=DB 行锁 / 集群=共识租约)
    authority: Authority,
    /// 当前实例的 fence(授予/续约时更新;执行面据此拒绝过期命令,设计 §5.5)
    fences: DashMap<String, Fence>,
    /// 已失去租约的实例 → 原因(必须立即停手,不再继续执行副作用,设计 §5.4)
    lease_lost: DashMap<String, String>,
    /// 续约首次失败时刻(用于把"传输抖动"与"租约真过期"区分开)
    lease_fail_since: DashMap<String, std::time::Instant>,
}

impl RdsManager {
    pub fn new(store: Arc<Store>) -> Arc<Self> {
        let executor = make_executor();
        let scheduler = Arc::new(TaskScheduler::new(executor, Some(store.clone())));
        let m = RdsManager {
            scheduler,
            store,
            instances: DashMap::new(),
            next_port: AtomicU64::new(35000),
            next_server_id: AtomicU64::new(1),
            op_locks: DashMap::new(),
            holder: controller_id(),
            lock_lease: env_str("RDSCTL_LOCK_LEASE", "30")
                .parse()
                .unwrap_or(30)
                .max(5),
            authority: match crate::ha::runtime::global() {
                Some(rt) => {
                    tracing::info!("实例互斥权威 = 共识租约(cluster 模式)");
                    Authority::Cluster(rt)
                }
                None => Authority::Store,
            },
            fences: DashMap::new(),
            lease_lost: DashMap::new(),
            lease_fail_since: DashMap::new(),
        };
        m.load_persisted();
        Arc::new(m)
    }

    /// 从 sink(元数据库)回灌实例视图:让**副本间视图收敛**。
    ///
    /// 定位(设计 §12):sink 只是投影/读模型,不是权威;但 cluster 模式下实例登记目前
    /// 仍由各副本内存 + sink 承载(M1c 才迁入状态机),因此需要定期回灌以免副本长期陈旧。
    /// 约束:本地正在执行生命周期操作(op_locks)的实例不回灌,避免覆盖在途变更。
    pub fn refresh_from_sink(&self) -> usize {
        let mut changed = 0usize;
        for (name, data) in self.store.instance_load_all() {
            if self.op_locks.contains_key(&name) {
                continue;
            }
            let Ok(fresh) = serde_json::from_str::<RdsInstance>(&data) else {
                continue;
            };
            match self.instances.get_mut(&name) {
                Some(mut cur) => {
                    let mut dirtied = false;
                    if cur.status != fresh.status {
                        cur.status = fresh.status;
                        dirtied = true;
                    }
                    if cur.last_error != fresh.last_error {
                        cur.last_error = fresh.last_error.clone();
                        dirtied = true;
                    }
                    if cur.node_states != fresh.node_states {
                        cur.node_states = fresh.node_states.clone();
                        dirtied = true;
                    }
                    if dirtied {
                        changed += 1;
                    }
                }
                None => {
                    // 其它副本创建的实例:补进本副本视图
                    self.instances.insert(name, fresh);
                    changed += 1;
                }
            }
        }
        changed
    }

    /// 探测式宿主端口分配:自 next_port 起逐个检查 127.0.0.1 端口可绑定,跳过已被占用
    /// (ssh 隧道转发 / 既有容器映射 / 其它进程)的端口,避免 docker 端口冲突导致任务失败。
    fn alloc_host_port(&self) -> u16 {
        loop {
            let p = self.next_port.fetch_add(1, Ordering::Relaxed);
            if p >= 60_000 {
                // 计数器到顶(异常场景)回绕到起始区间
                self.next_port.store(35_000, Ordering::Relaxed);
                continue;
            }
            let p16 = p as u16;
            if p16 < 35_000 {
                continue;
            }
            if host_port_free(p16) {
                return p16;
            }
            // 占用则继续取下一端口(计数已自增)
        }
    }

    // ─── 查询 ───

    pub fn list(&self) -> Vec<serde_json::Value> {
        let hosts = self.store.host_list();
        let mut v: Vec<_> = self
            .instances
            .iter()
            .map(|e| self.enrich_host_facts(e.value().to_view(), &hosts))
            .collect();
        v.sort_by_key(|t| t["created_at"].as_u64().unwrap_or(0));
        v.reverse();
        v
    }

    /// 带筛选与分页的实例列表(M0-4;返回 (页数据, 筛选后总数))
    pub fn list_filtered(
        &self,
        region: Option<&str>,
        az: Option<&str>,
        status: Option<&str>,
        tenant: Option<&str>,
        q: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> (Vec<serde_json::Value>, usize) {
        let q = q.map(|s| s.trim().to_lowercase()).filter(|s| !s.is_empty());
        let hosts = self.store.host_list();
        let mut all: Vec<serde_json::Value> = self
            .instances
            .iter()
            .filter(|e| {
                let i = e.value();
                if let Some(r) = region {
                    if !r.is_empty() && i.region != r {
                        return false;
                    }
                }
                if let Some(z) = az {
                    if !z.is_empty() && i.az != z {
                        return false;
                    }
                }
                if let Some(st) = status {
                    if !st.is_empty() && i.status.label() != st && status_tag(i.status) != st {
                        return false;
                    }
                }
                if let Some(t) = tenant {
                    if !t.is_empty() && i.tenant != t {
                        return false;
                    }
                }
                if let Some(kw) = &q {
                    if !i.name.to_lowercase().contains(kw) {
                        return false;
                    }
                }
                true
            })
            .map(|e| self.enrich_host_facts(e.value().to_view(), &hosts))
            .collect();
        let total = all.len();
        all.sort_by_key(|t| std::cmp::Reverse(t["created_at"].as_u64().unwrap_or(0)));
        all.truncate(offset.saturating_add(limit));
        (all.into_iter().skip(offset).collect(), total)
    }

    pub fn get(&self, name: &str) -> Option<serde_json::Value> {
        let hosts = self.store.host_list();
        self.instances
            .get(name)
            .map(|i| self.enrich_host_facts(i.to_view(), &hosts))
    }

    /// 为实例视图附挂 Host 事实与接入点(P2-④ 跨机入口模型 / P2-⑤ 节点事实化):
    ///   每节点: host_name/host_ip/host_region/host_az/host_rack/agent_port/addr/exec_route
    ///   (绑定宿主机时以 Host 注册表为事实;未绑定 = 本机直连 local);
    ///   顶层 ingress[]: 本机 LVS/VIP 入口 + 各绑定宿主机接入点(ip + 其上节点地址清单)。
    fn enrich_host_facts(
        &self,
        mut v: serde_json::Value,
        hosts: &[serde_json::Value],
    ) -> serde_json::Value {
        let nh = v["node_hosts"].as_object().cloned().unwrap_or_default();
        let inst_region = v["region"].as_str().unwrap_or("").to_string();
        let inst_az = v["az"].as_str().unwrap_or("").to_string();
        let mut per_host: std::collections::BTreeMap<String, Vec<serde_json::Value>> =
            std::collections::BTreeMap::new();
        if let Some(nodes) = v["nodes"].as_array_mut() {
            for n in nodes.iter_mut() {
                let c = n["container"].as_str().unwrap_or("").to_string();
                let h = nh
                    .get(&c)
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string();
                let hp = n["host_port"].as_u64().unwrap_or(0) as u16;
                if h.is_empty() {
                    // 未绑定:本机直连
                    n["host_name"] = serde_json::json!("");
                    n["addr"] = serde_json::json!(format!("127.0.0.1:{hp}"));
                    n["exec_route"] = serde_json::json!("local");
                    continue;
                }
                let row = hosts
                    .iter()
                    .find(|r| r["name"].as_str() == Some(h.as_str()));
                n["host_name"] = serde_json::json!(h);
                match row {
                    Some(r) => {
                        let ip = r["ip"].as_str().unwrap_or("127.0.0.1").to_string();
                        let ap = r["agent_port"].as_u64().unwrap_or(0);
                        n["host_ip"] = r["ip"].clone();
                        n["host_region"] = r["region"].clone();
                        n["host_az"] = r["az"].clone();
                        n["host_rack"] = r["rack"].clone();
                        n["agent_port"] = serde_json::json!(ap);
                        n["addr"] = serde_json::json!(format!("{ip}:{hp}"));
                        n["exec_route"] =
                            serde_json::json!(if ap > 0 { "agent" } else { "unmanaged" });
                        let ent = per_host.entry(h.clone()).or_default();
                        ent.push(serde_json::json!({
                            "container": c, "role": n["role"].clone(),
                            "addr": format!("{ip}:{hp}"), "host_port": hp,
                        }));
                    }
                    None => {
                        // 绑定了未登记/已删除的宿主机(数据残留)→ 登记为 unmanaged
                        n["host_ip"] = serde_json::Value::Null;
                        n["exec_route"] = serde_json::json!("unmanaged");
                        let ent = per_host.entry(h.clone()).or_default();
                        ent.push(serde_json::json!({
                            "container": c, "role": n["role"].clone(),
                            "addr": format!("(unknown):{hp}"), "host_port": hp,
                        }));
                    }
                }
            }
        }
        // 接入点汇总:本机 LVS/VIP 入口 + 绑定宿主机入口(按 Host 去重)
        let mut entries: Vec<serde_json::Value> = Vec::new();
        let lvs_port = v["lvs_mysql_port"].as_u64().unwrap_or(0);
        if lvs_port > 0 {
            entries.push(serde_json::json!({
                "kind": "lvs", "host": "本机(LVS)", "ip": "127.0.0.1", "port": lvs_port,
                "addr": format!("127.0.0.1:{lvs_port}"),
                "region": inst_region, "az": inst_az, "rack": "",
                "agent": "local", "nodes": [],
            }));
        }
        for (h, nds) in per_host {
            if h.is_empty() {
                continue;
            }
            let row = hosts
                .iter()
                .find(|r| r["name"].as_str() == Some(h.as_str()));
            let (ip, region, az, rack, agent) = match row {
                Some(r) => (
                    r["ip"].as_str().unwrap_or("127.0.0.1").to_string(),
                    r["region"].as_str().unwrap_or("").to_string(),
                    r["az"].as_str().unwrap_or("").to_string(),
                    r["rack"].as_str().unwrap_or("").to_string(),
                    if r["agent_port"].as_u64().unwrap_or(0) > 0 {
                        "agent"
                    } else {
                        "unmanaged"
                    },
                ),
                None => (
                    "(unknown)".to_string(),
                    String::new(),
                    String::new(),
                    String::new(),
                    "unmanaged",
                ),
            };
            let first_port = nds
                .iter()
                .find_map(|x| x["host_port"].as_u64())
                .unwrap_or(0);
            entries.push(serde_json::json!({
                "kind": "host", "host": h, "ip": ip, "port": first_port,
                "addr": format!("{ip}:{first_port}"),
                "region": region, "az": az, "rack": rack,
                "agent": agent, "nodes": nds,
            }));
        }
        v["ingress"] = serde_json::json!(entries);
        v
    }

    /// 查询台:返回实例级专用账号口令(为空则生成随机口令并持久化到实例记录;
    /// 口令不出现在任何视图/审计/日志;查询统一走该低权账号而非 root)。
    pub fn ensure_query_secret(&self, name: &str) -> Result<String, String> {
        let Some(i) = self.instances.get(name).map(|e| e.value().clone()) else {
            return Err(format!("实例 {name} 不存在"));
        };
        if !i.query_secret.is_empty() {
            return Ok(i.query_secret.clone());
        }
        let secret = crate::sha256::to_hex(&crate::sha256::digest(
            format!(
                "{}-{}-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos())
                    .unwrap_or(0),
                std::process::id(),
                QUERY_SECRET_CNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            )
            .as_bytes(),
        ));
        if let Some(mut e) = self.instances.get_mut(name) {
            e.query_secret = secret.clone();
        }
        self.persist();
        Ok(secret)
    }

    pub fn tasks(&self, instance: &str) -> Vec<serde_json::Value> {
        self.scheduler.instance_tasks(instance)
    }

    pub fn audit(
        &self,
        limit: usize,
        instance: Option<&str>,
        action: Option<&str>,
        q: Option<&str>,
    ) -> Vec<serde_json::Value> {
        self.store.audit_list(limit, instance, action, q)
    }

    // ─── 物理机(Host)注册表与节点绑定(instances.manage;见 docs/physical-multi-site-ops.md §5) ───

    /// 机器清单(host 列表)
    pub fn hosts(&self) -> Vec<serde_json::Value> {
        self.store.host_list()
    }

    /// 登记/更新物理机;返回错误文案。agent_port: 该机远端执行 agent 监听端口(0=未接入)
    pub fn host_create(
        &self,
        name: &str,
        ip: &str,
        region: &str,
        az: &str,
        rack: &str,
        cpu_cores: u32,
        mem_gb: u32,
        disk_gb: u32,
        agent_port: u16,
    ) -> Result<(), String> {
        let name = name.trim();
        if name.is_empty() {
            return Err("缺少机器名".to_string());
        }
        if name.len() > 96 {
            return Err("机器名过长(≤96)".to_string());
        }
        if cpu_cores == 0 || mem_gb == 0 {
            return Err("cpu/mem_gb 需 > 0".to_string());
        }
        self.store.host_upsert(
            name, ip, region, az, rack, cpu_cores, mem_gb, disk_gb, agent_port, "running",
        );
        self.store.audit(
            &crate::auth::current_user(),
            "",
            "host_create",
            &format!("host={name} ip={ip} region={region} az={az} rack={rack} cpu={cpu_cores} mem={mem_gb} disk={disk_gb} agent_port={agent_port}"),
            "ok",
            "",
        );
        Ok(())
    }

    /// 删除机器;仍被实例节点绑定则拒绝(防悬挂引用)
    pub fn host_delete(&self, name: &str) -> Result<(), String> {
        let name = name.trim();
        if name.is_empty() {
            return Err("缺少机器名".to_string());
        }
        // 防悬挂:任何实例任一节点绑定到该 Host 时拒绝删除
        let in_use: Vec<String> = self
            .instances
            .iter()
            .filter_map(|e| {
                let i = e.value();
                if i.node_hosts.values().any(|h| h == name) {
                    Some(i.name.clone())
                } else {
                    None
                }
            })
            .collect();
        if !in_use.is_empty() {
            return Err(format!(
                "机器 {name} 仍被实例 {} 的节点绑定,不可删除",
                in_use.join(",")
            ));
        }
        if !self.store.host_delete(name) {
            return Err(format!("机器 {name} 不存在"));
        }
        self.store.audit(
            &crate::auth::current_user(),
            "",
            "host_delete",
            &format!("host={name}"),
            "ok",
            "",
        );
        Ok(())
    }

    /// 机器状态切换(running | maintenance | retiring):写回注册表并审计。
    /// retiring 表示退役中——不能再作为绑定/替换/迁移目标。
    pub fn host_set_status(&self, name: &str, status: &str) -> Result<(), String> {
        let name = name.trim();
        if name.is_empty() {
            return Err("缺少机器名".to_string());
        }
        if !matches!(status, "running" | "maintenance" | "retiring") {
            return Err("status 需为 running / maintenance / retiring".to_string());
        }
        let rows = self.store.host_list();
        let Some(hr) = rows.iter().find(|r| r["name"].as_str() == Some(name)) else {
            return Err(format!("机器 {name} 不存在"));
        };
        let u = |k: &str| hr[k].as_u64().unwrap_or(0) as u32;
        let ap = hr["agent_port"].as_u64().unwrap_or(0) as u16;
        self.store.host_upsert(
            name,
            hr["ip"].as_str().unwrap_or(""),
            hr["region"].as_str().unwrap_or(""),
            hr["az"].as_str().unwrap_or(""),
            hr["rack"].as_str().unwrap_or(""),
            u("cpu_cores"),
            u("mem_gb"),
            u("disk_gb"),
            ap,
            status,
        );
        self.store.audit(
            &crate::auth::current_user(),
            "",
            "host_status",
            &format!("host={name} status={status}"),
            "ok",
            "",
        );
        Ok(())
    }

    /// 绑定实例节点到宿主机(登记事实;执行路由见 host_exec_plan)。
    /// 校验:实例/节点存在、Host 已登记且非 retiring。
    /// 绑定到指定宿主机(全部实例)的节点计数
    pub fn host_binding_count(&self, host: &str) -> usize {
        self.instances
            .iter()
            .filter(|e| e.value().node_hosts.values().any(|h| h == host))
            .map(|e| e.value().node_hosts.values().filter(|h| h == &host).count())
            .sum()
    }

    /// 调度建议/水位校验(P2-⑥):宿主机可承载节点槽位按 mem_gb/8 估算(每节点 8GiB);
    /// 返回 Err = 硬拒绝(超水位);Ok = 提示文案。
    /// 主从同机 / rack 分布是“建议提示”,见 schedule_advisories()。
    pub fn schedule_check(&self, host: &str, extra: usize) -> Result<String, String> {
        let rows = self.store.host_list();
        let Some(hr) = rows.iter().find(|r| r["name"].as_str() == Some(host)) else {
            return Err(format!("宿主机 {host} 未登记"));
        };
        let mem = hr["mem_gb"].as_u64().unwrap_or(0);
        let slots = ((mem / 8).max(1)) as usize; // 节点槽位(每 8GiB 一个)
        let used = self.host_binding_count(host);
        if used + extra > slots {
            return Err(format!(
                "宿主机 {host} 容量水位不足:已用 {used} 槽位,将再占 {extra},上限 {slots}(mem {mem}GiB,按每节点 8GiB 估算);请更换/扩容目标机器"
            ));
        }
        Ok(format!("宿主机 {host} 容量水位 ok: {used}/{slots} 槽位"))
    }

    /// 调度分布提示(P2-⑥,建议级):主从同机、同 rack 聚堆、az 未分开 → 返回提示文案
    pub fn schedule_advisories(&self, instance: &str, target_host: &str) -> Vec<String> {
        let mut out = Vec::new();
        let Some(i) = self.instances.get(instance).map(|e| e.value().clone()) else {
            return out;
        };
        let rows = self.store.host_list();
        let target_row = rows
            .iter()
            .find(|r| r["name"].as_str() == Some(target_host));
        let (t_rack, t_az) = match target_row {
            Some(r) => (
                r["rack"].as_str().unwrap_or("").to_string(),
                r["az"].as_str().unwrap_or("").to_string(),
            ),
            None => (String::new(), String::new()),
        };
        for n in &i.nodes {
            let nb = node_host_binding(&i, &n.container);
            if nb == Some(target_host) && n.role == Role::Master {
                out.push(format!(
                    "注意:目标机 {target_host} 与主节点同机(故障域未分开)"
                ));
            }
            if let Some(h) = nb {
                if let Some(r) = rows.iter().find(|x| x["name"].as_str() == Some(h)) {
                    let rack = r["rack"].as_str().unwrap_or("").to_string();
                    let az = r["az"].as_str().unwrap_or("").to_string();
                    if !rack.is_empty() && rack == t_rack && h != target_host {
                        out.push(format!("注意:宿主机 {target_host} 与 {h} 同 rack({rack})"));
                    }
                    if !az.is_empty() && az == t_az && h != target_host {
                        out.push(format!("注意:宿主机 {target_host} 与 {h} 同 az({az})"));
                    }
                }
            }
        }
        out
    }

    pub fn host_assign_node(&self, instance: &str, node: &str, host: &str) -> Result<(), String> {
        let instance = instance.trim();
        let node = node.trim();
        let host = host.trim();
        if instance.is_empty() || node.is_empty() || host.is_empty() {
            return Err("缺少 instance/node/host 参数".to_string());
        }
        let Some(i) = self.instances.get(instance).map(|e| e.value().clone()) else {
            return Err(format!("实例 {instance} 不存在"));
        };
        if i.status == InstStatus::Destroyed {
            return Err(format!("实例 {instance} 已销毁,不可绑定"));
        }
        if !i.nodes.iter().any(|n| n.container == node) {
            return Err(format!("节点 {node} 不属于实例 {instance}"));
        }
        let host_row = self
            .store
            .host_list()
            .into_iter()
            .find(|h| h["name"] == host);
        let Some(hr) = host_row else {
            return Err(format!("宿主机 {host} 未登记,请先创建机器"));
        };
        if hr["status"].as_str() == Some("retiring") {
            return Err(format!("宿主机 {host} 处于 retiring(退役),禁止新绑定"));
        }
        // 容量水位(P2-⑥):非重复绑定才占槽位
        let already_here = i
            .node_hosts
            .get(node)
            .map(|h| h.as_str() == host)
            .unwrap_or(false);
        if !already_here {
            self.schedule_check(host, 1)?;
        }
        if let Some(mut e) = self.instances.get_mut(instance) {
            e.node_hosts.insert(node.to_string(), host.to_string());
        }
        self.persist();
        self.store.audit(
            &crate::auth::current_user(),
            instance,
            "host_assign",
            &format!("node={node} host={host}"),
            "ok",
            "",
        );
        Ok(())
    }

    /// 解绑实例节点(回到本机直连语义);未绑定时报错
    pub fn host_clear_node(&self, instance: &str, node: &str) -> Result<(), String> {
        let instance = instance.trim();
        let node = node.trim();
        if instance.is_empty() || node.is_empty() {
            return Err("缺少 instance/node 参数".to_string());
        }
        let had = self
            .instances
            .get(instance)
            .map(|e| e.value().node_hosts.contains_key(node))
            .unwrap_or(false);
        if !had {
            return Err(format!("节点 {node} 未绑定宿主机(实例 {instance})"));
        }
        if let Some(mut e) = self.instances.get_mut(instance) {
            e.node_hosts.remove(node);
        }
        self.persist();
        self.store.audit(
            &crate::auth::current_user(),
            instance,
            "host_clear",
            &format!("node={node}"),
            "ok",
            "",
        );
        Ok(())
    }

    /// 按 Host 分配宿主端口(该 Host 端口高水位自增);Host 未登记返回 None。
    /// 现有创建/扩容仍走 alloc_host_port()(本机探测,向后兼容);
    /// 绑定 Host 后的新节点端口由本方法提供(P2 replace_node/migrate_instance 使用)。
    #[allow(dead_code)] // 当前由测试覆盖语义;P2 工作流接入后启用
    pub fn alloc_port_for_host(&self, host: &str) -> Option<u16> {
        self.store
            .host_alloc_port(host)
            .map(|p| p.min(u16::MAX as u64) as u16)
    }

    /// 运维视图汇总:实例状态分布 + 进行中任务数(M0 dashboard)
    pub fn summary(&self) -> serde_json::Value {
        use std::collections::HashMap;
        let mut by_status: HashMap<String, u64> = HashMap::new();
        let mut total = 0u64;
        let mut degraded: Vec<String> = Vec::new();
        // 概览统计口径:不含「已销毁/已删除」实例(destroyed 仍保留记录供历史查看,但不计入运营统计)
        let alive = |i: &RdsInstance| i.status != InstStatus::Destroyed;
        for e in self.instances.iter() {
            let i = e.value();
            if !alive(i) {
                continue;
            }
            total += 1;
            *by_status.entry(status_tag(i.status)).or_default() += 1;
            if matches!(i.status, InstStatus::Degraded | InstStatus::Failed)
                && !i.last_error.is_empty()
            {
                degraded.push(i.last_error.clone());
            }
        }
        let (ac, aw, ai) = self.store.alert_counts();
        // ── 运营分布(MySQL/Proxy 版本、业务线、DBA、核心) ──
        let mut mv: HashMap<String, u64> = HashMap::new();
        let mut pv: HashMap<String, u64> = HashMap::new();
        let mut biz: HashMap<String, u64> = HashMap::new();
        let mut dba: HashMap<String, (u64, u64)> = HashMap::new();
        let (mut core_n, mut noncore_n) = (0u64, 0u64);
        let mut types: HashMap<String, u64> = HashMap::new();
        for e in self.instances.iter() {
            let i = e.value();
            if !alive(i) {
                continue;
            }
            let k1 = if i.mysql_version.is_empty() {
                "未标注".to_string()
            } else {
                i.mysql_version.clone()
            };
            *mv.entry(k1).or_default() += 1;
            let k2 = if i.proxy_version.is_empty() {
                "未标注".to_string()
            } else {
                i.proxy_version.clone()
            };
            *pv.entry(k2).or_default() += 1;
            let kb = if i.biz.is_empty() {
                "未标注".to_string()
            } else {
                i.biz.clone()
            };
            *biz.entry(kb).or_default() += 1;
            if !i.dba.is_empty() {
                let e2 = dba.entry(i.dba.clone()).or_default();
                e2.0 += 1;
                if i.core {
                    e2.1 += 1;
                }
            }
            if i.core {
                core_n += 1;
            } else {
                noncore_n += 1;
            }
            let kt = if i.itype.is_empty() {
                "未标注".to_string()
            } else {
                i.itype.clone()
            };
            *types.entry(kt).or_default() += 1;
        }
        let mut dba_top: Vec<serde_json::Value> = dba
            .into_iter()
            .map(|(name, (n, core_cnt))| serde_json::json!({ "name": name, "count": n, "core": core_cnt }))
            .collect();
        // 数量降序;数量相同再按 DBA 名升序,保证排序确定、跨请求稳定(否则 DashMap 遍历序随机导致榜单抖动)
        dba_top.sort_by(|a, b| {
            let c = b["count"]
                .as_u64()
                .unwrap_or(0)
                .cmp(&a["count"].as_u64().unwrap_or(0));
            if c == std::cmp::Ordering::Equal {
                a["name"]
                    .as_str()
                    .unwrap_or("")
                    .cmp(&b["name"].as_str().unwrap_or(""))
            } else {
                c
            }
        });
        dba_top.truncate(10);
        serde_json::json!({
            "total": total,
            "by_status": by_status,
            "running_tasks": self.scheduler.running.load(Ordering::Relaxed),
            "degraded_reasons": degraded.iter().take(5).collect::<Vec<_>>(),
            "alerts": { "critical": ac, "warn": aw, "info": ai, "open": ac + aw + ai },
            "dist_mysql": mv, "dist_proxy": pv,
            "dist_biz": biz, "dba_top": dba_top,
            "core": core_n, "noncore": noncore_n,
            "dist_type": types,
        })
    }

    /// 巡检更新节点健康状态事实(container → ok|stopped|down|missing|repl_down);仅变化时持久化
    fn apply_node_states(&self, name: &str, states: std::collections::HashMap<String, String>) {
        let changed = self
            .instances
            .get_mut(name)
            .map(|mut e| {
                if e.node_states == states {
                    false
                } else {
                    e.node_states = states;
                    true
                }
            })
            .unwrap_or(false);
        if changed {
            self.persist();
        }
    }

    pub fn cancel_task(&self, task_id: &str) -> bool {
        self.scheduler.cancel(task_id)
    }

    /// 管理启停:停用后禁止常规变更(扩容等),允许启用与销毁清理
    pub fn set_instance_enabled(&self, name: &str, enabled: bool) -> Result<(), String> {
        let Some(i) = self.instances.get(name).map(|i| i.clone()) else {
            return Err(format!("实例 {name} 不存在"));
        };
        if i.status == InstStatus::Destroyed {
            return Err(format!("实例 {name} 已销毁,不可启停"));
        }
        if i.enabled == enabled {
            return Ok(());
        }
        let action = if enabled { "enable" } else { "disable" };
        if let Some(mut e) = self.instances.get_mut(name) {
            e.enabled = enabled;
        }
        self.persist();
        self.store.audit(
            &crate::auth::current_user(),
            name,
            action,
            if enabled { "enabled" } else { "disabled" },
            "ok",
            "",
        );
        // 备份注册联动:启停状态变化 → heartbeat(内部默认关闭,no-op)
        crate::backuplink::on_instance_enabled(name, enabled);
        Ok(())
    }

    /// 批量操作:逐实例执行并返回逐条结果(便于前端展示失败原因)
    pub fn batch(&self, action: &str, names: &[String]) -> Vec<serde_json::Value> {
        let actor = crate::auth::current_user();
        let mut out = Vec::new();
        for name in names {
            let res: Result<String, String> = match action {
                "destroy" => self.destroy(name),
                "enable" => {
                    let _ = actor;
                    self.set_instance_enabled(name, true).map(|_| String::new())
                }
                "disable" => self
                    .set_instance_enabled(name, false)
                    .map(|_| String::new()),
                "scaleout_read" => self
                    .scaleout(name, Role::Read, None, None)
                    .map(|tid| tid.clone()),
                "scaleout_offline" => self
                    .scaleout(name, Role::Offline, None, None)
                    .map(|tid| tid.clone()),
                _ => Err(format!("未知批量操作 {action}")),
            };
            let (ok, msg) = match res {
                Ok(t) => (true, t),
                Err(e) => (false, e),
            };
            out.push(serde_json::json!({ "name": name, "ok": ok, "message": msg }));
        }
        out
    }

    /// 告警列表(透传 store;S-告警)
    pub fn alerts(
        &self,
        limit: usize,
        severity: Option<&str>,
        status: Option<&str>,
        instance: Option<&str>,
    ) -> Vec<serde_json::Value> {
        self.store.alert_list(limit, severity, status, instance)
    }

    /// 告警处置 ack/resolve(记录处理人与时间)
    pub fn alert_action(&self, id: u64, action: &str) -> bool {
        self.store
            .alert_action(id, action, &crate::auth::current_user())
    }

    /// 写入运营元数据(单键;instances.manage)。合法键见 META_KEYS。
    pub fn set_meta(&self, name: &str, k: &str, v: &str) -> Result<(), String> {
        let Some(mut i) = self.instances.get_mut(name) else {
            return Err(format!("实例 {name} 不存在"));
        };
        match k {
            "biz" => i.biz = v.to_string(),
            "contact" => i.contact = v.to_string(),
            "dba" => i.dba = v.to_string(),
            "core" => {
                if v == "1" || v.eq_ignore_ascii_case("true") {
                    i.core = true;
                } else if v == "0" || v.eq_ignore_ascii_case("false") {
                    i.core = false;
                } else {
                    return Err("core 需为 true/false 或 1/0".to_string());
                }
            }
            "itype" => {
                if !matches!(
                    v,
                    "single" | "async" | "sync" | "distributed" | "xenon" | ""
                ) {
                    return Err("类型需为 single/async/sync/xenon/distributed".to_string());
                }
                i.itype = v.to_string();
            }
            "mysql_version" => i.mysql_version = v.to_string(),
            "proxy_version" => i.proxy_version = v.to_string(),
            "auto_failover" => {
                if v == "1" || v.eq_ignore_ascii_case("true") {
                    i.auto_failover = true;
                } else if v == "0" || v.eq_ignore_ascii_case("false") {
                    i.auto_failover = false;
                } else {
                    return Err("auto_failover 需为 true/false 或 1/0".to_string());
                }
            }
            "spec" => i.spec = v.to_string(),
            "shard_num" => {
                i.shard_num = v.trim().parse().map_err(|_| "分片数需为数字".to_string())?;
            }
            "data_size" => i.data_size = v.to_string(),
            "buffer_pool" => i.buffer_pool = v.to_string(),
            "max_qps" => {
                i.max_qps = v.trim().parse().map_err(|_| "QPS 需为数字".to_string())?;
            }
            "max_tps" => {
                i.max_tps = v.trim().parse().map_err(|_| "TPS 需为数字".to_string())?;
            }
            _ => {
                return Err(format!("未知元数据键 {k}"));
            }
        }
        drop(i);
        self.persist();
        self.store.audit(
            &crate::auth::current_user(),
            name,
            "meta_set",
            &format!("{k}={v}"),
            "ok",
            "",
        );
        Ok(())
    }

    /// 全实例代理聚合(代理维度统一管理列表)
    /// **跨实例节点清单**(侧边栏「节点」视图用):把各实例的 DB 节点与 Proxy 节点摊平成一张表。
    ///
    /// 设计取舍:
    ///   - 只读内存态 + 已登记的 Host 事实(与列表/详情同源),**不发起任何 docker/SQL 探针**,
    ///     因此可以安全地每轮轮询;真实状态来自巡检写入的 `node_states`(未探过的节点标记 unknown);
    ///   - 每行附带 `actions`:由实例类型/角色推导出**当前真正可用**的控制能力(见 api 层按钮映射),
    ///     避免前端硬编码规则、也避免给出后端不支持的按钮;
    ///   - `summary` 按状态与类型计数,供视图顶部概览。
    pub fn nodes_inventory(
        &self,
        instance: Option<&str>,
        kind: Option<&str>,
        q: Option<&str>,
        limit: usize,
        offset: usize,
    ) -> (Vec<serde_json::Value>, serde_json::Value) {
        let inst_f = instance.map(|s| s.trim()).filter(|s| !s.is_empty());
        let kind_f = kind.map(|s| s.trim()).filter(|s| !s.is_empty());
        let q_f = q
            .map(|s| s.trim().to_lowercase())
            .filter(|s| !s.is_empty());
        let hosts = self.store.host_list();

        let mut rows: Vec<serde_json::Value> = Vec::new();
        for e in self.instances.iter() {
            let i = e.value();
            if let Some(f) = inst_f {
                if i.name != f {
                    continue;
                }
            }
            // ── DB 节点 ──
            if kind_f.is_none() || kind_f == Some("db") {
                for n in &i.nodes {
                    let state = i
                        .node_states
                        .get(&n.container)
                        .cloned()
                        .unwrap_or_else(|| "unknown".to_string());
                    let host_name = i.node_hosts.get(&n.container).cloned().unwrap_or_default();
                    let mut row = serde_json::json!({
                        "kind": "db",
                        "instance": i.name,
                        "itype": i.itype,
                        "region": i.region, "az": i.az, "tenant": i.tenant,
                        "instance_status": i.status, "enabled": i.enabled,
                        "container": n.container,
                        "role": n.role, "role_label": n.role.label(),
                        "state": state,
                        "host_port": n.host_port,
                        "server_id": n.server_id,
                        "parent": n.parent,
                        "rpc_host_port": n.rpc_host_port,
                        "host_name": host_name,
                    });
                    // 复用实例视图的 Host 事实富化(host_ip/az/rack/agent_port/addr/exec_route)
                    self.enrich_node_host_facts(&mut row, &hosts);
                    row["actions"] = serde_json::json!(db_node_actions(i, n));
                    rows.push(row);
                }
            }
            // ── Proxy 节点 ──
            if kind_f.is_none() || kind_f == Some("proxy") {
                let list: Vec<(String, u16, u16)> = if i.proxies.is_empty() {
                    vec![(
                        i.proxy_container.clone(),
                        i.proxy_mysql_port,
                        i.proxy_mng_port,
                    )]
                } else {
                    i.proxies
                        .iter()
                        .map(|p| (p.container.clone(), p.mysql_port, p.mng_port))
                        .collect()
                };
                for (c, mp, gp) in list {
                    if c.is_empty() {
                        continue;
                    }
                    let state = i
                        .node_states
                        .get(&c)
                        .cloned()
                        .unwrap_or_else(|| "unknown".to_string());
                    let host_name = i.node_hosts.get(&c).cloned().unwrap_or_default();
                    let mut row = serde_json::json!({
                        "kind": "proxy",
                        "instance": i.name,
                        "itype": i.itype,
                        "region": i.region, "az": i.az, "tenant": i.tenant,
                        "instance_status": i.status, "enabled": i.enabled,
                        "container": c,
                        "role": "proxy", "role_label": "Proxy",
                        "state": state,
                        // host_port 参与 enrich_node_host_facts 的 addr 拼接(代理对外入口是 mysql_port)
                        "host_port": mp,
                        "mysql_port": mp, "mng_port": gp,
                        "host_name": host_name,
                    });
                    self.enrich_node_host_facts(&mut row, &hosts);
                    row["actions"] = serde_json::json!([
                        "proxy_restart", "proxy_stop", "proxy_start", "proxy_health", "detail"
                    ]);
                    rows.push(row);
                }
            }
        }

        // 关键词过滤(容器名 / 实例名 / host_port)
        if let Some(kw) = &q_f {
            rows.retain(|r| {
                let hay = format!(
                    "{} {} {} {}",
                    r["container"].as_str().unwrap_or(""),
                    r["instance"].as_str().unwrap_or(""),
                    r["host_port"].as_u64().unwrap_or(0),
                    r["host_name"].as_str().unwrap_or("")
                )
                .to_lowercase();
                hay.contains(kw)
            });
        }

        // summary 基于**过滤后的全量**(不受分页影响)
        let mut summary = serde_json::json!({
            "total": rows.len(),
            "db": 0, "proxy": 0,
            "ok": 0, "degraded": 0, "down": 0, "missing": 0, "stopped": 0,
            "repl_down": 0, "remote": 0, "unknown": 0,
            "not_enabled": 0,
        });
        for r in &rows {
            let k = r["kind"].as_str().unwrap_or("");
            if k == "db" {
                summary["db"] = serde_json::json!(summary["db"].as_u64().unwrap_or(0) + 1);
            } else if k == "proxy" {
                summary["proxy"] = serde_json::json!(summary["proxy"].as_u64().unwrap_or(0) + 1);
            }
            let st = r["state"].as_str().unwrap_or("unknown");
            if summary.get(st).is_some() {
                summary[st] = serde_json::json!(summary[st].as_u64().unwrap_or(0) + 1);
            }
            if r["enabled"].as_bool() == Some(false) {
                summary["not_enabled"] =
                    serde_json::json!(summary["not_enabled"].as_u64().unwrap_or(0) + 1);
            }
        }
        let total = rows.len();
        let limit = limit.clamp(1, 2000);
        let page: Vec<serde_json::Value> = rows.into_iter().skip(offset).take(limit).collect();
        summary["returned"] = serde_json::json!(page.len());
        summary["total_matched"] = serde_json::json!(total);
        (page, summary)
    }

    /// 把 Host 注册表事实(ip/region/az/rack/agent_port/addr/exec_route)补进一行节点记录
    fn enrich_node_host_facts(&self, row: &mut serde_json::Value, hosts: &[serde_json::Value]) {
        let port = row["host_port"].as_u64().unwrap_or(0);
        let host_name = row["host_name"].as_str().unwrap_or("").to_string();
        if host_name.is_empty() {
            // 未绑定登记机器 = 本机节点:监听在回环地址(管控进程直接执行 docker/mysql)
            row["exec_route"] = serde_json::json!("local");
            if port > 0 {
                row["addr"] = serde_json::json!(format!("127.0.0.1:{port}"));
            }
            return;
        }
        match hosts.iter().find(|h| h["name"].as_str() == Some(host_name.as_str())) {
            Some(h) => {
                row["host_ip"] = h["ip"].clone();
                row["host_region"] = h["region"].clone();
                row["host_az"] = h["az"].clone();
                row["host_rack"] = h["rack"].clone();
                row["agent_port"] = h["agent_port"].clone();
                let ip = h["ip"].as_str().unwrap_or("127.0.0.1");
                if port > 0 {
                    row["addr"] = serde_json::json!(format!("{ip}:{port}"));
                }
                let agent = h["agent_port"].as_u64().unwrap_or(0);
                row["exec_route"] = serde_json::json!(if agent > 0 { "agent" } else { "unmanaged" });
            }
            None => {
                row["exec_route"] = serde_json::json!("unmanaged");
            }
        }
    }

    pub fn proxies(&self) -> Vec<serde_json::Value> {
        let mut out = Vec::new();
        for e in self.instances.iter() {
            let i = e.value();
            let list: Vec<(String, u16, u16)> = if i.proxies.is_empty() {
                vec![(
                    i.proxy_container.clone(),
                    i.proxy_mysql_port,
                    i.proxy_mng_port,
                )]
            } else {
                i.proxies
                    .iter()
                    .map(|p| (p.container.clone(), p.mysql_port, p.mng_port))
                    .collect()
            };
            for (c, mp, gp) in list {
                if c.is_empty() {
                    continue;
                }
                out.push(serde_json::json!({
                    "container": c, "instance": i.name,
                    "region": i.region, "az": i.az,
                    "mysql_port": mp, "mng_port": gp,
                }));
            }
        }
        out
    }

    /// 代理动作(通用 Docker 实现;升级/降级/热加载预留执行模板扩展点)
    pub async fn proxy_action(&self, container: &str, action: &str) -> Result<String, String> {
        match action {
            "restart" => {
                crate::docker::restart(container).await?;
                Ok(format!("代理 {container} 已重启"))
            }
            "health" => {
                let ok = crate::docker::is_healthy(container).await;
                if ok {
                    Ok(format!("代理 {container} 健康(容器运行且健康检测通过)"))
                } else {
                    Err(format!("代理 {container} 不健康/容器不存在"))
                }
            }
            "stop" => {
                crate::docker::stop(container).await?;
                Ok(format!("代理 {container} 已停止"))
            }
            "start" => {
                crate::docker::start(container).await?;
                Ok(format!("代理 {container} 已启动"))
            }
            "upgrade" | "downgrade" | "hot_reload" | "scale" | "spec_upgrade" => Err(format!(
                "动作 {action} 的执行模板尚未接入(需要代理版本库/镜像与配置下发约定);参数与接口已预留"
            )),
            _ => Err(format!("未知动作 {action}")),
        }
    }

    // ─── 实例操作锁(进程内 DashMap 快路径 + 存储层 lease) ───

    fn lock_instance(&self, name: &str) -> Result<(), String> {
        if self.op_locks.contains_key(name) {
            return Err(format!("实例 {name} 有其他操作进行中,请等待完成"));
        }
        match &self.authority {
            Authority::Store => {
                if !self
                    .store
                    .lock_instance(name, &self.holder, self.lock_lease)
                {
                    return Err(format!(
                        "实例 {name} 的 lease 被其它控制端持有(可能有其它控制器在操作),请稍后重试"
                    ));
                }
            }
            Authority::Cluster(rt) => {
                match rt.acquire_lease_blocking(name, &self.holder, self.lock_lease * 1000) {
                    Ok(fence) => {
                        tracing::info!("实例 {name} 获得共识租约,fence={}", fence.wire());
                        self.fences.insert(name.to_string(), fence);
                    }
                    Err(e) => return Err(format!("实例 {name} 操作被拒:{e}")),
                }
            }
        }
        self.lease_lost.remove(name);
        self.op_locks.insert(name.to_string(), ());
        Ok(())
    }

    fn unlock_instance(&self, name: &str) {
        self.op_locks.remove(name);
        match &self.authority {
            Authority::Store => self.store.unlock_instance(name, &self.holder),
            Authority::Cluster(rt) => {
                // 主动释放:下一个持有者无需等待 skew 边界(设计 §5.4)
                if let Err(e) = rt.release_lease_blocking(name, &self.holder) {
                    tracing::warn!("实例 {name} 释放共识租约失败(将按 TTL 自然过期):{e}");
                }
                self.fences.remove(name);
            }
        }
    }

    /// 回收"本副本持有、已过期、且本进程当前并未在操作"的实例租约(崩溃/漏释放的自愈)。
    ///
    /// 为什么只回收**已过期**的:
    ///   · 未过期的租约可能正处于"共识已授予、`op_locks` 尚未插入"的极小窗口
    ///     (见 `lock_instance`),误释放会把自己正在进行的操作推出互斥;
    ///   · 正常路径每 100ms 续约(`watch_task`),所以"已过期"本身就意味着持有者已停止续约
    ///     (进程被杀 / 实例已销毁)。此时该实例的互斥保证本来就已经交回 TTL,
    ///     主动释放不削弱任何保证,只是把残留条目清掉(设计 §19 发现 15)。
    ///
    /// 只释放**自己**持有的条目:状态机强制 `holder` 匹配,别的副本残留由它自己收。
    /// 返回本次成功释放的条数(用于日志/观测)。
    pub async fn reap_own_expired_leases(self: &Arc<Self>) -> usize {
        let Authority::Cluster(rt) = &self.authority else {
            return 0; // 单机模式:DB 锁按 TTL 自然过期,行为逐字不变
        };
        let leases = rt.leases_view();
        let mut n = 0usize;
        for l in leases.as_array().cloned().unwrap_or_default() {
            if l["holder"].as_str() != Some(self.holder.as_str())
                || l["expired"].as_bool() != Some(true)
            {
                continue;
            }
            let Some(instance) = l["instance"].as_str() else {
                continue;
            };
            if self.op_locks.contains_key(instance) {
                continue; // 防御:理论上不会"正在操作且已过期"
            }
            // 直接走 propose_op 以拿到 apply 结果:被他人接管时会返回 rejected,
            // 这属于正常竞争(不该刷 warn),因此按结果分级记录。
            let op = crate::ha::state::Op::LeaseRelease {
                instance: instance.to_string(),
                holder: self.holder.clone(),
            };
            match rt.propose_op(op).await {
                Ok((_, applied)) if applied["status"] == serde_json::Value::String("applied".into()) => {
                    self.fences.remove(instance);
                    n += 1;
                    tracing::info!(
                        "回收过期实例租约:{instance}(holder={},已过期,本进程未在操作)",
                        self.holder
                    );
                }
                Ok((_, applied)) => tracing::debug!(
                    "回收过期租约 {instance} 未生效(已被接管或状态已变):{applied}"
                ),
                Err(e) => tracing::debug!("回收过期租约 {instance} 提案失败:{e}"),
            }
        }
        n
    }

    /// watch_task 轮询期间续约租约(长任务防租约过期)
    ///
    /// 集群模式下**续约失败即视为失去租约**:记录 `lease_lost` 并立即停手,
    /// 不再有"续约失败仍继续执行"的路径(设计 §5.4;today 的 DB 路径保持原样)。
    fn renew_instance_lease(&self, name: &str) {
        match &self.authority {
            Authority::Store => {
                if !self
                    .store
                    .renew_instance_lock(name, &self.holder, self.lock_lease)
                {
                    tracing::warn!("实例 {name} lease 续约失败(可能被其它控制端抢占);仍由本进程执行任务");
                }
            }
            Authority::Cluster(rt) => match rt.renew_lease_blocking(name, &self.holder) {
                Ok(fence) => {
                    self.fences.insert(name.to_string(), fence);
                    self.lease_fail_since.remove(name);
                }
                // 失败必须分类处理:租约是**时间**保证,一次网络/转发抖动并不等于失去租约。
                // 只有"状态机明确说租约不在我手上"才立即停手;传输类故障按 TTL 到期升级。
                Err(e) => {
                    let fatal = matches!(
                        e,
                        crate::ha::runtime::LeaseError::Held(_)
                            | crate::ha::runtime::LeaseError::Skew(_)
                    );
                    if fatal {
                        tracing::error!("实例 {name} 已失去共识租约:{e} —— 立即停手(设计 §5.4)");
                        self.lease_lost.insert(name.to_string(), e.to_string());
                    } else {
                        // 传输/多数派/换主类故障:记录首次失败时刻,持续超过 TTL 才判定失去
                        let since = self
                            .lease_fail_since
                            .entry(name.to_string())
                            .or_insert_with(std::time::Instant::now)
                            .to_owned();
                        let held_ms = since.elapsed().as_millis() as u64;
                        if held_ms > self.lock_lease.saturating_mul(1000) {
                            let msg = format!(
                                "共识租约已连续续约失败 {held_ms}ms(> TTL {}s):按过期处理:{e}",
                                self.lock_lease
                            );
                            tracing::error!("实例 {name} {msg} —— 停手");
                            self.lease_lost.insert(name.to_string(), msg);
                        } else {
                            tracing::warn!(
                                "实例 {name} 续约暂时失败({held_ms}ms < TTL {}s),下一轮重试:{e}",
                                self.lock_lease
                            );
                        }
                    }
                }
            },
        }
    }

    /// 该实例当前的 fence(执行面强制所需;无租约/单机模式返回 None)
    pub fn fence_of(&self, instance: &str) -> Option<Fence> {
        self.fences.get(instance).map(|f| *f.value())
    }

    /// 是否已失去租约(worker 必须据此中止)
    pub fn lease_lost_reason(&self, instance: &str) -> Option<String> {
        self.lease_lost.get(instance).map(|v| v.value().clone())
    }

    /// 调用元信息:fence 感知的 agent 调用(设计 §9.2)
    pub fn call_meta(&self, instance: &str) -> crate::agent::CallMeta {
        match self.fence_of(instance) {
            Some(f) => crate::agent::CallMeta::fenced(f),
            None => crate::agent::CallMeta::none(),
        }
    }

    fn assert_state(&self, name: &str, want: InstStatus) -> Result<RdsInstance, String> {
        let Some(i) = self.instances.get(name).map(|i| i.clone()) else {
            return Err(format!("实例 {name} 不存在"));
        };
        if i.status != want {
            return Err(format!("实例 {name} 当前状态 {:?},不允许该操作", i.status));
        }
        Ok(i)
    }

    fn set_status(&self, name: &str, status: InstStatus, last_error: &str) {
        if let Some(mut i) = self.instances.get_mut(name) {
            i.status = status;
            if !last_error.is_empty() {
                i.last_error = last_error.to_string();
            }
        }
        self.persist();
    }

    // ─── 创建实例(proxy 集群默认 2 实例;db 架构类型可选) ───

    pub fn create(&self, name: &str, o: &CreateOpts) -> Result<String, String> {
        let name = name.trim().to_string();
        if name.is_empty()
            || !name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            return Err("实例名仅允许字母/数字/-/_".into());
        }
        if !matches!(o.itype.as_str(), "single" | "async" | "sync" | "xenon") {
            return Err(
                "架构类型需为 single/async/sync/xenon(distributed 需分布式引擎模板,暂不可创建)"
                    .into(),
            );
        }
        let proxies_n = o.proxies.clamp(1, 4);
        if self.instances.contains_key(&name) {
            return Err(format!("实例 {name} 已存在"));
        }
        let shard_n = o.shard_num.max(1).min(128);
        let multi = shard_n > 1 && matches!(o.itype.as_str(), "async" | "sync");
        if shard_n > 1 && o.itype == "single" {
            return Err("单节点(single)不支持多分片,请选主从异步/同步或保持 1 分片".into());
        }
        if shard_n > 1 && o.itype == "xenon" {
            return Err("xenon(raft)架构本身即多节点高可用,不支持再分片".into());
        }
        // xenon 一致性档位校验(rsproxy ReadConsistency::parse 同源)
        if o.itype == "xenon" && !o.xenon_consistency.is_empty() {
            if !matches!(
                o.xenon_consistency.as_str(),
                "strong" | "causal" | "session" | "eventual"
            ) {
                return Err("xenon 读一致性档位需为 strong/causal/session/eventual".into());
            }
        }
        if shard_n > 1 && !matches!(o.itype.as_str(), "async" | "sync") {
            return Err(
                "架构类型需为 single/async/sync/xenon(distributed 需分布式引擎模板,暂不可创建)"
                    .into(),
            );
        }
        self.lock_instance(&name)?;

        let network = format!("rds-{name}");
        let master_c = format!("rds-{name}-master");
        let master_sid = self.next_server_id.fetch_add(10, Ordering::Relaxed) as u64;
        // proxy 集群:1..N,首成员承载 legacy 字段
        let mut proxy_list: Vec<(String, u16, u16)> = Vec::new();
        for i in 0..proxies_n {
            let c = format!("rds-{name}-proxy-{}", i + 1);
            let mp = self.alloc_host_port();
            let gp = self.alloc_host_port();
            proxy_list.push((c, mp, gp));
        }
        // LVS 接入层:实例网络内起一个 4 层转发容器,业务入口统一走 VIP(LVS 发布端口)
        let lvs_c = format!("rds-{name}-lvs");
        let lvs_port = self.alloc_host_port();
        // 多分片(N>1,async/sync):每分片 = 独立主从复制组(1 主 + 1 读从)。
        // v0 语义:分片间数据路由/每分片独立入口代理属「引擎模板/代理层」范围,见代码注释与
        // docs/scaling-design §5.1(instance_shards 为 M1+ 演进),本步只保证「创建出的实体
        // 就是 N 分片(各自容器/端口/复制/校验)」,与 UI 分片列表数据(shards)对齐。
        let (inst, mut dag_nodes) = if o.itype == "xenon" {
            // xenon(raft 高可用):N 个节点容器(mysqld+xenon 同容器),无 proxy/LVS/复制链
            self.build_xenon_inst(&name, o, &network)?
        } else if multi {
            self.build_multi_inst(&name, o, shard_n, &network, &proxy_list, &lvs_c, lvs_port)?
        } else {
            self.build_single_inst(
                &name,
                o,
                &network,
                &master_c,
                master_sid,
                &proxy_list,
                &lvs_c,
                lvs_port,
            )
        };
        // 可选:创建实例时一并登记 DTS 占位链路(canal,不实际运行引擎;规格见 dts_spec)
        // 目标:离线/备份/统计从节点;无此类节点(async 以外)时退回「每分片选 1 个读从」。
        // 作为 DAG 尾节点(依赖最后一个节点:从属节点均已就绪),纯注册即时完成、非阻断。
        if o.dts {
            let spec = if o.dts_spec.is_empty() {
                "2C4G".to_string()
            } else {
                o.dts_spec.clone()
            };
            let tail_dep: Vec<String> = dag_nodes
                .last()
                .map(|n| vec![n.id.clone()])
                .unwrap_or_default();
            let mut targets: Vec<&InstNode> = inst
                .nodes
                .iter()
                .filter(|n| matches!(n.role, Role::Offline | Role::Backup | Role::Stats))
                .collect();
            if targets.is_empty() {
                let mut seen_shard: Vec<String> = Vec::new();
                for n in inst.nodes.iter().filter(|n| n.role == Role::Read) {
                    let sh = n.shard.clone();
                    if !seen_shard.contains(&sh) {
                        seen_shard.push(sh);
                        targets.push(n);
                    }
                }
            }
            for n in targets {
                let dts_c = dts_container(&name, &n.container);
                let label = if matches!(n.role, Role::Offline | Role::Backup | Role::Stats) {
                    "数据仓库(BigData)"
                } else {
                    "下游业务(Downstream)"
                };
                dag_nodes.push(TaskNode {
                    id: format!("dts-{}", n.container),
                    name: format!("占位 DTS(canal) {spec} ← {}(随实例创建)", n.container),
                    deps: tail_dep.clone(),
                    retries: 0,
                    timeout_secs: Some(30),
                    steps: vec![Step::DtsRun {
                        instance: name.clone(),
                        node: n.container.clone(),
                        dts_container: dts_c.clone(),
                        target_label: label.to_string(),
                        spec: spec.clone(),
                    }],
                });
            }
        }
        self.instances.insert(name.clone(), inst);
        self.persist();
        self.store.audit(
            &crate::auth::current_user(),
            &name,
            "create",
            "submitted",
            "",
            "",
        );

        let tid = self
            .scheduler
            .submit("create", &name, &crate::auth::current_user(), dag_nodes);
        watch_task(&name, tid.clone());
        Ok(tid)
    }

    /// 单分片(legacy)实例规划:架构 roles 映射 + create_nodes2 DAG(行为与历史完全一致)
    fn build_single_inst(
        &self,
        name: &str,
        o: &CreateOpts,
        network: &str,
        master_c: &str,
        master_sid: u64,
        proxy_list: &[(String, u16, u16)],
        lvs_c: &str,
        lvs_port: u16,
    ) -> (RdsInstance, Vec<TaskNode>) {
        let (proxy0_c, proxy0_mysql, proxy0_mng) = proxy_list[0].clone();
        // db 节点按架构类型
        let mut roles: Vec<(&str, Role)> = Vec::new(); // (kind, role)
        match o.itype.as_str() {
            "single" => roles.push(("master", Role::Master)),
            "sync" => {
                roles.push(("master", Role::Master));
                roles.push(("read", Role::Read));
            }
            _ => {
                roles.push(("master", Role::Master));
                roles.push(("read", Role::Read));
                roles.push(("offline", Role::Offline));
            }
        }
        let l_region = if o.region.is_empty() {
            sys_region()
        } else {
            o.region.clone()
        };
        let l_az = if o.az.is_empty() {
            sys_az()
        } else {
            o.az.clone()
        };
        let l_shard = sys_shard();
        let mut nodes: Vec<InstNode> = Vec::new();
        let mut slave_idx = 0usize;
        for (kind, role) in roles {
            let container = if kind == "master" {
                master_c.to_string()
            } else {
                slave_idx += 1;
                format!("rds-{name}-slave-{slave_idx}")
            };
            let hp = self.alloc_host_port();
            let sid = master_sid + (nodes.len() as u64);
            let parent = if kind == "master" {
                String::new()
            } else {
                master_c.to_string()
            };
            nodes.push(InstNode {
                container: container.clone(),
                role,
                host: container.clone(),
                port: 3306,
                host_port: hp,
                server_id: sid,
                region: l_region.clone(),
                az: l_az.clone(),
                shard: l_shard.clone(),
                parent,
                rpc_host_port: 0,
            });
        }
        let proxies = proxy_node_list(proxy_list);
        // 从节点规格(供 DAG 构建;与 registry 一致)
        let mut slave_i = 0u64;
        let slave_specs: Vec<(String, String, u64)> = nodes
            .iter()
            .filter(|n| n.role != Role::Master)
            .map(|n| {
                slave_i += 1;
                let kind = if n.role == Role::Read {
                    "read"
                } else {
                    "offline"
                }
                .to_string();
                (n.container.clone(), kind, master_sid + slave_i)
            })
            .collect();
        let inst = RdsInstance {
            name: name.to_string(),
            status: InstStatus::Creating,
            region: l_region.clone(),
            az: l_az.clone(),
            shard: l_shard.clone(),
            tenant: sys_tenant(),
            enabled: true,
            lvs: vec![format!("127.0.0.1:{lvs_port}")],
            lvs_container: lvs_c.to_string(),
            lvs_mysql_port: lvs_port,
            node_states: std::collections::HashMap::new(),
            node_hosts: std::collections::HashMap::new(),
            auto_failover: true,
            proxies: proxies.clone(),
            shards: Vec::new(),
            biz: o.biz.clone(),
            contact: o.contact.clone(),
            dba: o.dba.clone(),
            core: o.core,
            itype: o.itype.clone(),
            mysql_version: if o.mysql_version.is_empty() {
                "5.7".to_string() // 默认 MySQL 5.7(未指定版本时)
            } else {
                o.mysql_version.clone()
            },
            proxy_version: o.proxy_version.clone(),
            spec: o.spec.clone(),
            shard_num: 1,
            data_size: o.data_size.clone(),
            buffer_pool: o.buffer_pool.clone(),
            max_qps: o.max_qps,
            max_tps: o.max_tps,
            network: network.to_string(),
            nodes,
            proxy_container: proxy0_c,
            proxy_mysql_port: proxy0_mysql,
            proxy_mng_port: proxy0_mng,
            created_at: now(),
            root_password: ROOT_PASS.to_string(),
            query_secret: String::new(),
            last_error: String::new(),
        };
        let dag = create_nodes2(
            name,
            network,
            master_c,
            &slave_specs,
            proxy_list,
            master_sid,
            lvs_c,
            lvs_port,
        );
        (inst, dag)
    }

    /// 多分片(N>1)实例规划:每分片独立主 + 从节点组 + shards 元数据 + 分片化 DAG。
    /// 从节点语义对齐单分片模板:sync 每分片 1 个读从;async 每分片 1 读从 + 1 离线从(备份/统计)。
    fn build_multi_inst(
        &self,
        name: &str,
        o: &CreateOpts,
        shard_n: u64,
        network: &str,
        proxy_list: &[(String, u16, u16)],
        lvs_c: &str,
        lvs_port: u16,
    ) -> Result<(RdsInstance, Vec<TaskNode>), String> {
        let (proxy0_c, proxy0_mysql, proxy0_mng) = proxy_list[0].clone();
        let l_region = if o.region.is_empty() {
            sys_region()
        } else {
            o.region.clone()
        };
        let l_az = if o.az.is_empty() {
            sys_az()
        } else {
            o.az.clone()
        };
        let async_multi = o.itype.as_str() == "async"; // async 语义含离线从
        let mut nodes: Vec<InstNode> = Vec::new();
        let mut groups: Vec<MultiGroup> = Vec::new(); // (shard_id, master_c, read_c, offline_c, master_sid)
        for k in 1..=shard_n {
            let shard_id = format!("s{k}");
            let master_c = format!("rds-{name}-s{k}-master");
            let read_c = format!("rds-{name}-s{k}-slave-1");
            let offline_c = if async_multi {
                Some(format!("rds-{name}-s{k}-slave-2"))
            } else {
                None
            };
            let master_sid = self.next_server_id.fetch_add(10, Ordering::Relaxed) as u64;
            let read_sid = master_sid + 1;
            let m_hp = self.alloc_host_port();
            let r_hp = self.alloc_host_port();
            nodes.push(InstNode {
                container: master_c.clone(),
                role: Role::Master,
                host: master_c.clone(),
                port: 3306,
                host_port: m_hp,
                server_id: master_sid,
                region: l_region.clone(),
                az: l_az.clone(),
                shard: shard_id.clone(),
                parent: String::new(),
                rpc_host_port: 0,
            });
            nodes.push(InstNode {
                container: read_c.clone(),
                role: Role::Read,
                host: read_c.clone(),
                port: 3306,
                host_port: r_hp,
                server_id: read_sid,
                region: l_region.clone(),
                az: l_az.clone(),
                shard: shard_id.clone(),
                parent: master_c.clone(),
                rpc_host_port: 0,
            });
            if let Some(oc) = &offline_c {
                let o_hp = self.alloc_host_port();
                nodes.push(InstNode {
                    container: oc.clone(),
                    role: Role::Offline,
                    host: oc.clone(),
                    port: 3306,
                    host_port: o_hp,
                    server_id: master_sid + 2,
                    region: l_region.clone(),
                    az: l_az.clone(),
                    shard: shard_id.clone(),
                    parent: master_c.clone(),
                    rpc_host_port: 0,
                });
            }
            groups.push(MultiGroup {
                shard_id: shard_id.clone(),
                master_c: master_c.clone(),
                read_c,
                offline_c,
                master_sid,
            });
        }
        let proxies = proxy_node_list(proxy_list);
        let shards: Vec<ShardInfo> = groups
            .iter()
            .map(|g| {
                let mut sl: Vec<ShardSlaveRef> = vec![ShardSlaveRef {
                    name: g.read_c.clone(),
                    role: "read".into(),
                }];
                if let Some(oc) = &g.offline_c {
                    sl.push(ShardSlaveRef {
                        name: oc.clone(),
                        role: "offline".into(),
                    });
                }
                ShardInfo {
                    id: g.shard_id.clone(),
                    data_range: String::new(),
                    master: g.master_c.clone(),
                    slave: g.read_c.clone(),
                    slaves: sl,
                    lag_ms: 0,
                    storage_pct: 0,
                    health: String::new(),
                }
            })
            .collect();
        let inst = RdsInstance {
            name: name.to_string(),
            status: InstStatus::Creating,
            region: l_region.clone(),
            az: l_az.clone(),
            shard: String::new(), // 多分片以节点/分片列表的 shard 字段为准
            tenant: sys_tenant(),
            enabled: true,
            lvs: vec![format!("127.0.0.1:{lvs_port}")],
            lvs_container: lvs_c.to_string(),
            lvs_mysql_port: lvs_port,
            node_states: std::collections::HashMap::new(),
            node_hosts: std::collections::HashMap::new(),
            auto_failover: true,
            proxies: proxies.clone(),
            shards,
            biz: o.biz.clone(),
            contact: o.contact.clone(),
            dba: o.dba.clone(),
            core: o.core,
            itype: o.itype.clone(),
            mysql_version: if o.mysql_version.is_empty() {
                "5.7".to_string() // 默认 MySQL 5.7(未指定版本时)
            } else {
                o.mysql_version.clone()
            },
            proxy_version: o.proxy_version.clone(),
            spec: o.spec.clone(),
            shard_num: shard_n,
            data_size: o.data_size.clone(),
            buffer_pool: o.buffer_pool.clone(),
            max_qps: o.max_qps,
            max_tps: o.max_tps,
            network: network.to_string(),
            nodes,
            proxy_container: proxy0_c,
            proxy_mysql_port: proxy0_mysql,
            proxy_mng_port: proxy0_mng,
            created_at: now(),
            root_password: ROOT_PASS.to_string(),
            query_secret: String::new(),
            last_error: String::new(),
        };
        let dag = create_nodes_multi(name, network, &groups, proxy_list, lvs_c, lvs_port);
        Ok((inst, dag))
    }

    // ─── 创建实例:xenon(raft 高可用,见 docs/xenon-create-design.md) ───
    //
    // 对齐 xenon 项目 deploy/(xenon.sh + gen-compose.sh + entrypoint.sh)的部署语义:
    //   1 节点 = 1 容器(镜像 xenon-local:latest,内含 mysqld + xenon 同进程组),
    //   entrypoint 负责渲染 xenon.json/my.cnf、初始化 datadir、建账号、种 peers.json;
    //   raft 集群经 CLUSTER_PEERS(容器名:8801)互联,节点1 INIT_ROLE=LEADER 其余 FOLLOWER。
    // 管控差异(vs 主从架构):
    //   - 无 proxy/LVS 接入层,无 MySQL 复制链;业务可直连任一节点 MySQL 宿主端口。
    //   - 高可用由 xenon raft 内建(leader 选举/自动故障转移),管控面不启用 orchestrator ERS。
    //   - root 口令沿用实例统一 ROOT_PASS(entrypoint MYSQL_ROOT_PASSWORD),巡检/查询台语义不变。
    fn build_xenon_inst(
        &self,
        name: &str,
        o: &CreateOpts,
        network: &str,
    ) -> Result<(RdsInstance, Vec<TaskNode>), String> {
        // 节点数:>=3 才有 raft 容错(xenon deploy/env.sh 注释);允许 1/2 仅限实验,
        // 默认与推荐值 3;上限 9(lab 规模)
        let n_nodes = if o.xenon_nodes == 0 { 3 } else { o.xenon_nodes };
        if n_nodes > 9 {
            return Err("xenon 节点数上限 9(lab 规模)".into());
        }
        let n_nodes = n_nodes as usize;
        // 前置 rsproxy(newproxy)代理:0=业务直连节点;1..=4 = 集群前部署代理接入层
        let n_proxy = o.xenon_proxy.clamp(0, 4) as usize;
        // 读一致性档位(rsproxy ReadConsistency 同源;strong 内置默认 = 全走 leader)
        let consistency = if o.xenon_consistency.is_empty() {
            "strong".to_string()
        } else {
            o.xenon_consistency.clone()
        };
        // CLUSTER_PEERS:容器名:8801 逗号分隔(容器网络内互通,与 gen-compose.sh 一致)
        let peers: Vec<String> = (1..=n_nodes)
            .map(|i| format!("{}:{}", xenon_node_c(name, i), XENON_RPC_PORT))
            .collect();
        let peers_s = peers.join(",");
        let mut nodes: Vec<InstNode> = Vec::with_capacity(n_nodes);
        let mut proxies: Vec<ProxyNode> = Vec::new();
        let mut dag_nodes: Vec<TaskNode> = Vec::new();
        // 1) 专属网络
        dag_nodes.push(TaskNode {
            id: "net".into(),
            name: "创建专属网络".into(),
            deps: vec![],
            retries: 1,
            timeout_secs: Some(60),
            steps: vec![Step::NetworkCreate {
                name: network.to_string(),
            }],
        });
        // 2) N 个 xenon 节点(无依赖 → 调度器并发拉起;节点1 = 种子 LEADER)
        for i in 1..=n_nodes {
            let c = xenon_node_c(name, i);
            let role = if i == 1 { "LEADER" } else { "FOLLOWER" };
            // MySQL 宿主端口(管控/业务直连);RPC 宿主端口( raft 诊断,可控面直接 xenoncli)
            let mysql_hp = self.alloc_host_port();
            let rpc_hp = self.alloc_host_port();
            let sid = self.next_server_id.fetch_add(10, Ordering::Relaxed) as u64;
            nodes.push(InstNode {
                container: c.clone(),
                // 节点1 登记 Master(展示/主写语义),其余 Read;raft 内部角色由 xenon 管理
                role: if i == 1 { Role::Master } else { Role::Read },
                host: c.clone(),
                port: 3306,
                host_port: mysql_hp,
                server_id: sid,
                region: String::new(),
                az: String::new(),
                shard: String::new(),
                parent: String::new(),
                rpc_host_port: rpc_hp,
            });
            dag_nodes.push(TaskNode {
                id: format!("node{i}"),
                name: format!("启动 xenon 节点 #{i}({role})"),
                deps: vec!["net".into()],
                retries: 1,
                timeout_secs: Some(600),
                steps: vec![
                    Step::DockerRun {
                        container: c.clone(),
                        args: xenon_node_args(network, &c, &peers_s, role, mysql_hp, rpc_hp),
                    },
                    // entrypoint 内部:渲染配置 → 初始化 datadir → 建账号 → mysqld_safe → xenon;
                    // mysqld 就绪即 xenon 管理链路就绪(xenon 监听 IsRunning 探测)
                    Step::WaitMysql {
                        container: c.clone(),
                        user: "root".into(),
                        pass: ROOT_PASS.into(),
                        timeout_secs: 300,
                    },
                ],
            });
        }
        // 3) 集群就绪校验(等 LEADER + 主写 init + raft 复制一致;全节点依赖)
        let member_ids: Vec<String> = (1..=n_nodes).map(|i| format!("node{i}")).collect();
        let members: Vec<String> = (1..=n_nodes).map(|i| xenon_node_c(name, i)).collect();
        dag_nodes.push(TaskNode {
            id: "verify".into(),
            name: "xenon raft 集群就绪校验".into(),
            deps: member_ids,
            retries: 2,
            timeout_secs: Some(300),
            steps: vec![Step::VerifyXenon { members: members.clone() }],
        });
        // 3.5) 可选:前置 rsproxy 代理接入层(与 xenon 节点同网络;自动跟随 raft leader)
        //      链:proxy_conf(渲染配置,含 XenonRaft 段) → proxy×N → lvs(进程内 VIP 转发)
        let mut tail_dep = "verify".to_string();
        let (proxy_container, proxy_mysql_port, proxy_mng_port, lvs_container, lvs_mysql_port) =
            if n_proxy > 0 {
                // members = raft 节点容器名:3306(代理与节点同容器网络,内网直连);
                // raft_endpoints = 容器名:8801(对齐 mysql.xenon_raft_status.leader 列语义)
                let conf_path = format!(
                    "{}/logs/rds/{name}/newproxy.conf",
                    std::env::current_dir().unwrap_or_default().display()
                );
                let conf_content = xenon_proxy_config(
                    &members,
                    &consistency,
                    n_proxy,
                );
                dag_nodes.push(TaskNode {
                    id: "proxy_conf".into(),
                    name: "生成 rsproxy 配置(含 XenonRaft leader 发现)".into(),
                    deps: vec!["verify".into()],
                    retries: 0,
                    timeout_secs: Some(30),
                    steps: vec![Step::WriteHostFile {
                        path: conf_path.clone(),
                        content: conf_content,
                    }],
                });
                let mut proxy_ids: Vec<String> = Vec::new();
                for k in 1..=n_proxy {
                    let c = format!("rds-{name}-proxy-{k}");
                    let mp = self.alloc_host_port();
                    let gp = self.alloc_host_port();
                    proxies.push(ProxyNode {
                        container: c.clone(),
                        mysql_port: mp,
                        mng_port: gp,
                        spec: String::new(),
                        ip: String::new(),
                        qps: 0,
                        conns: 0,
                        cpu: 0.0,
                        status: String::new(),
                        version: String::new(),
                    });
                    let id = if k == 1 { "proxy".to_string() } else { format!("proxy{k}") };
                    proxy_ids.push(id.clone());
                    dag_nodes.push(TaskNode {
                        id,
                        name: format!("启动 rsproxy 代理 #{k}({consistency})"),
                        deps: vec!["proxy_conf".into()],
                        retries: 1,
                        timeout_secs: Some(180),
                        steps: vec![Step::DockerRun {
                            container: c.clone(),
                            args: vec![
                                "--network".into(),
                                network.to_string(),
                                "-p".into(),
                                format!("127.0.0.1:{mp}:4051"),
                                "-p".into(),
                                format!("127.0.0.1:{gp}:9111"),
                                "-v".into(),
                                format!("{}:/app/conf/newproxy.conf:rw", conf_path),
                                image_of("RDSCTL_PROXY_IMAGE", PROXY_IMAGE),
                            ],
                        }],
                    });
                }
                // LVS 接入层(进程内转发器:VIP → 全部 Proxy 宿主端口),复用主从架构实现
                let lvs_c = format!("rds-{name}-lvs");
                let lvs_port = self.alloc_host_port();
                let last_proxy = proxy_ids.last().cloned().unwrap_or_else(|| "proxy".into());
                dag_nodes.push(TaskNode {
                    id: "lvs".into(),
                    name: "启动 LVS 接入(VIP 转发 rsproxy 集群)".into(),
                    deps: vec![last_proxy],
                    retries: 2,
                    timeout_secs: Some(60),
                    steps: vec![Step::EnsureLvs {
                        instance: name.to_string(),
                    }],
                });
                tail_dep = "lvs".to_string();
                (
                    proxies.first().map(|p| p.container.clone()).unwrap_or_default(),
                    proxies.first().map(|p| p.mysql_port).unwrap_or(0),
                    proxies.first().map(|p| p.mng_port).unwrap_or(0),
                    lvs_c,
                    lvs_port,
                )
            } else {
                (String::new(), 0, 0, String::new(), 0)
            };
        // 4) 全部就绪 → running(有代理时经 LVS → rsproxy → raft leader 全链路验证)
        dag_nodes.push(TaskNode {
            id: "done".into(),
            name: "全部就绪".into(),
            deps: vec![tail_dep],
            retries: 0,
            timeout_secs: Some(30),
            steps: vec![Step::UpdateInstanceStatus {
                instance: name.to_string(),
                status: "running".into(),
            }],
        });
        let inst = RdsInstance {
            name: name.to_string(),
            status: InstStatus::Creating,
            region: if o.region.is_empty() {
                sys_region()
            } else {
                o.region.clone()
            },
            az: if o.az.is_empty() {
                sys_az()
            } else {
                o.az.clone()
            },
            shard: sys_shard(),
            tenant: if o.dts { String::new() } else { sys_tenant() },
            enabled: true,
            lvs: if lvs_mysql_port > 0 {
                vec![format!("127.0.0.1:{lvs_mysql_port}")]
            } else {
                Vec::new()
            },
            lvs_container,
            lvs_mysql_port,
            node_states: std::collections::HashMap::new(),
            node_hosts: std::collections::HashMap::new(),
            auto_failover: false, // raft 自带故障转移,管控面 ERS 关闭
            proxies,
            shards: Vec::new(),
            biz: o.biz.clone(),
            contact: o.contact.clone(),
            dba: o.dba.clone(),
            core: o.core,
            itype: "xenon".to_string(),
            mysql_version: if o.mysql_version.is_empty() {
                "8.0".to_string()
            } else {
                o.mysql_version.clone()
            },
            proxy_version: if n_proxy > 0 { "newproxy".to_string() } else { String::new() },
            spec: o.spec.clone(),
            shard_num: 1,
            data_size: o.data_size.clone(),
            buffer_pool: o.buffer_pool.clone(),
            max_qps: o.max_qps,
            max_tps: o.max_tps,
            network: network.to_string(),
            nodes,
            proxy_container,
            proxy_mysql_port,
            proxy_mng_port,
            created_at: now(),
            root_password: ROOT_PASS.to_string(),
            query_secret: String::new(),
            last_error: String::new(),
        };
        Ok((inst, dag_nodes))
    }

    // ─── 销毁实例 ───

    /// 允许销毁的状态:运行中 / 降级(巡检异常)/ 失败(进程中断/任务失败的残留)。
    /// 状态机不允许从 creating/scaling/destroying 等操作中状态销毁(由操作锁+状态双保险拒绝)。
    pub fn destroy(&self, name: &str) -> Result<String, String> {
        let Some(inst) = self.instances.get(name).map(|i| i.clone()) else {
            return Err(format!("实例 {name} 不存在"));
        };
        if !matches!(
            inst.status,
            InstStatus::Running | InstStatus::Degraded | InstStatus::Failed
        ) {
            return Err(format!(
                "实例 {name} 当前状态 {},仅 运行/降级/失败 状态可销毁",
                inst.status.label()
            ));
        }
        self.lock_instance(name)?;
        let mut inst = inst;
        inst.status = InstStatus::Destroying;
        self.instances.insert(name.to_string(), inst.clone());
        self.persist();
        self.store.audit(
            &crate::auth::current_user(),
            name,
            "destroy",
            "submitted",
            "",
            "",
        );

        let nodes = destroy_nodes(&name, &inst);
        let tid = self
            .scheduler
            .submit("destroy", name, &crate::auth::current_user(), nodes);
        watch_task(&name.to_string(), tid.clone());
        Ok(tid)
    }

    // ─── 扩容从节点 ───

    pub fn scaleout(
        &self,
        name: &str,
        role: Role,
        region: Option<&str>,
        az: Option<&str>,
    ) -> Result<String, String> {
        self.scaleout_opt(name, role, region, az, None)
    }

    /// 分片级扩容:把新从节点挂到目标分片(shard=s1..sN)的 master 下。
    /// 多分片实例必须显式指定分片;单分片实例忽略 shard(行为与实例级一致,向后兼容)。
    pub fn scaleout_shard(
        &self,
        name: &str,
        shard: &str,
        role: Role,
        region: Option<&str>,
        az: Option<&str>,
    ) -> Result<String, String> {
        self.scaleout_opt(name, role, region, az, Some(shard))
    }

    fn scaleout_opt(
        &self,
        name: &str,
        role: Role,
        region: Option<&str>,
        az: Option<&str>,
        shard: Option<&str>,
    ) -> Result<String, String> {
        let inst = self.assert_state(name, InstStatus::Running)?;
        if inst.itype == "xenon" {
            return Err(
                "xenon(raft)实例不支持从节点扩容(节点数变更属 raft 成员运维,暂未开放)".into(),
            );
        }
        if !inst.enabled {
            return Err(format!("实例 {name} 已停用(管理暂停),请先启用再操作"));
        }
        let master_cnt = inst.nodes.iter().filter(|n| n.role == Role::Master).count();
        let single = master_cnt <= 1;
        // 分片级扩容:目标分片 master 为复制源,从节点编号延续 s{k}-slave-{n}(n 取该分片现有最大值+1)
        let (mc, sc, sid, shard_arg, parent_arg): (String, String, u64, String, String) = if single
        {
            if role.is_offline() && inst.nodes.iter().any(|n| n.role.is_offline()) {
                return Err(format!(
                    "实例 {name} 已存在离线从节点(备份/统计/大查询共用,仅允许一个);如需第二个请先销毁再扩容"
                ));
            }
            let m = inst
                .master()
                .map(|m| (m.container.clone(), m.host.clone(), m.server_id))
                .ok_or_else(|| format!("实例 {name} 缺少主节点,无法扩容"))?;
            let idx = inst.slaves().len() as u64 + 1;
            (
                m.1,
                format!("rds-{name}-slave-{idx}"),
                m.2 + idx,
                String::new(),
                String::new(),
            )
        } else {
            let s = shard.filter(|s| !s.is_empty()).ok_or_else(|| {
                format!("实例 {name} 为 {master_cnt} 分片集群,请指定目标分片(shard=s1..sN)")
            })?;
            let exists =
                inst.nodes.iter().any(|n| n.shard == s) || inst.shards.iter().any(|x| x.id == s);
            if !exists {
                return Err(format!("实例 {name} 不存在分片 {s}"));
            }
            if role.is_offline()
                && inst
                    .nodes
                    .iter()
                    .any(|n| n.shard == s && n.role.is_offline())
            {
                return Err(format!(
                    "分片 {s} 已存在离线从节点(备份/统计/大查询共用,每分片仅允许一个)"
                ));
            }
            let sm = inst
                .nodes
                .iter()
                .find(|n| n.role == Role::Master && n.shard == s)
                .cloned()
                .ok_or_else(|| format!("分片 {s} 缺少主节点,无法扩容"))?;
            let next = Self::next_shard_slave_idx(&inst.nodes, &s);
            let sc = format!("rds-{name}-{s}-slave-{next}");
            (
                sm.host.clone(),
                sc,
                sm.server_id + next,
                s.to_string(),
                sm.container,
            )
        };
        // 并发保护:与 create/destroy/实例级扩容共用同一把实例操作锁(终态由任务 watcher 释放)
        self.lock_instance(name)?;
        let hp = self.alloc_host_port();

        let mut inst = inst;
        let net = inst.network.clone();
        inst.status = InstStatus::Scaling;
        self.instances.insert(name.to_string(), inst);
        self.persist();
        self.store.audit(
            &crate::auth::current_user(),
            name,
            "scaleout",
            &role.label(),
            "submitted",
            "",
        );

        let region_s = region.unwrap_or("").to_string();
        let az_s = az.unwrap_or("").to_string();
        let nodes = scaleout_nodes(
            name,
            &mc,
            &net,
            &sc,
            sid,
            hp,
            role,
            &region_s,
            &az_s,
            &shard_arg,
            &parent_arg,
        );
        let tid = self
            .scheduler
            .submit("scaleout", name, &crate::auth::current_user(), nodes);
        watch_task(&name.to_string(), tid.clone());
        Ok(tid)
    }

    /// 某分片下已存在的从节点序号最大值+1(容器命名 rds-{name}-s{k}-slave-{n})
    fn next_shard_slave_idx(nodes: &[InstNode], shard: &str) -> u64 {
        let mut max = 0u64;
        for n in nodes {
            if n.shard != shard {
                continue;
            }
            if let Some(tail) = n.container.rsplit('-').next() {
                if let Ok(v) = tail.parse::<u64>() {
                    max = max.max(v);
                }
            }
        }
        max + 1
    }

    // ─── 节点替换(replace_node,P2-②;见 docs/physical-multi-site-ops.md §3/§7-②) ───

    /// 过保/退役机器节点替换:把从节点迁到目标宿主机(身份=容器名不变)。
    /// 提交为 DAG 任务(单节点 = ReplaceNodeCore→Commit→状态 running),持实例操作锁;
    /// 全程审计;失败由 watcher 置实例 Failed(核心阶段失败自动清理临时容器,旧节点不受影响)。
    /// 主节点替换请先受管 PRS 把主角色切走(orch_reparent planned),再替换该从节点。
    pub fn replace_node(&self, instance: &str, node: &str, host: &str) -> Result<String, String> {
        let name = instance.trim().to_string();
        let node = node.trim().to_string();
        let host = host.trim().to_string();
        if node.is_empty() || host.is_empty() {
            return Err("缺少 node/host 参数(目标宿主机须已登记且 agent 接入)".to_string());
        }
        let inst = self.assert_state(&name, InstStatus::Running)?;
        if !inst.enabled {
            return Err(format!("实例 {name} 已停用(管理暂停),请先启用再操作"));
        }
        if inst.itype == "xenon" {
            return Err(
                "xenon(raft)实例不支持节点替换工作流(raft 成员变更属引擎侧运维,暂未开放)".into(),
            );
        }
        let Some(n) = inst.nodes.iter().find(|n| n.container == node) else {
            return Err(format!("节点 {node} 不属于实例 {name}"));
        };
        if n.role == Role::Master {
            return Err(
                "主节点不可直接替换:请先用受管 PRS(planned)把主角色切到健康从,再替换该节点"
                    .to_string(),
            );
        }
        if inst.master().is_none() {
            return Err(format!("实例 {name} 缺少主节点,无法执行替换"));
        }
        // 目标宿主机校验:已登记、非 retiring、agent 已接入
        let hosts = self.store.host_list();
        let Some(hr) = hosts
            .iter()
            .find(|r| r["name"].as_str() == Some(host.as_str()))
        else {
            return Err(format!("宿主机 {host} 未登记,请先创建机器"));
        };
        if hr["status"].as_str() == Some("retiring") {
            return Err(format!(
                "宿主机 {host} 处于 retiring(退役),禁止作为替换目标"
            ));
        }
        if hr["agent_port"].as_u64().unwrap_or(0) == 0 {
            return Err(format!(
                "宿主机 {host} 未接入 agent(agent_port=0),无法在其上执行容器操作"
            ));
        }
        // 调度水位(P2-⑥):硬校验容量;分布提示随审计记录
        let cap_note = self.schedule_check(&host, 1)?;
        let advise = self.schedule_advisories(&name, &host);
        if matches!(
            resolve_route_of(&inst, &node, &hosts),
            NodeRoute::Agent { host: h, .. } | NodeRoute::Unmanaged { host: h } if h == host
        ) {
            return Err(format!("节点 {node} 已在宿主机 {host},无需替换"));
        }
        // 实例操作锁 + 状态(切换中)+ 审计(终态由 watch_task 释放)
        self.lock_instance(&name)?;
        if let Some(mut i) = self.instances.get_mut(&name) {
            i.status = InstStatus::Switching;
        }
        self.persist();
        self.store.audit(
            &crate::auth::current_user(),
            &name,
            "replace_node",
            &format!(
                "submitted node={node} target_host={host}; {cap_note}; {}",
                advise.join("; ")
            ),
            "submitted",
            "",
        );
        // 新容器参数:host 端口(本机探测分配)+ 全实例最大 server_id+1
        let hp = self.alloc_host_port();
        let max_sid = inst.nodes.iter().map(|n| n.server_id).max().unwrap_or(0);
        let sid = max_sid + 1;
        let n = inst.name.clone();
        let nodes = vec![crate::dag::TaskNode {
            id: "replace".into(),
            name: format!("替换节点 {node} → 宿主机 {host}"),
            deps: vec![],
            retries: 1,
            timeout_secs: Some(600),
            steps: vec![
                crate::dag::Step::ReplaceNodeCore {
                    instance: n.clone(),
                    node: node.clone(),
                    host: host.clone(),
                    host_port: hp,
                    server_id: sid,
                },
                crate::dag::Step::ReplaceNodeCommit {
                    instance: n.clone(),
                    node: node.clone(),
                    host: host.clone(),
                    host_port: hp,
                    server_id: sid,
                },
                crate::dag::Step::UpdateInstanceStatus {
                    instance: n,
                    status: "running".into(),
                },
            ],
        }];
        let tid = self
            .scheduler
            .submit("replace_node", &name, &crate::auth::current_user(), nodes);
        watch_task(&name.to_string(), tid.clone());
        Ok(tid)
    }

    // ─── 实例整体迁移(migrate_instance,P2-③;见 docs/physical-multi-site-ops.md §7-③) ───

    /// 把整个实例迁到目标 region/az 的宿主机组(hosts=a,b,…逗号分隔;须已登记且 agent 接入)。
    /// 从节点逐个同身份替换(轮转目标机),主节点最后受管切主;成功后提交 region/az 事实。
    /// 提交为 DAG 任务(kind=migrate),持实例操作锁 + Switching 状态,失败 watcher 置 Failed。
    pub fn migrate_instance(
        &self,
        instance: &str,
        region: &str,
        az: &str,
        hosts_csv: &str,
    ) -> Result<String, String> {
        let name = instance.trim().to_string();
        let region = region.trim().to_string();
        let az = az.trim().to_string();
        let hosts: Vec<String> = hosts_csv
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if region.is_empty() || az.is_empty() {
            return Err("缺少 region/az(迁移目标机房事实)".to_string());
        }
        if hosts.is_empty() {
            return Err("缺少目标宿主机列表(hosts=a,b…)".to_string());
        }
        let inst = self.assert_state(&name, InstStatus::Running)?;
        if !inst.enabled {
            return Err(format!("实例 {name} 已停用(管理暂停),请先启用再操作"));
        }
        if inst.network.starts_with("demo-") {
            return Err("演示实例无真实容器,不支持迁移".to_string());
        }
        if inst.itype == "xenon" {
            return Err(
                "xenon(raft)实例不支持跨机迁移工作流(raft 成员迁移属引擎侧运维,暂未开放)"
                    .to_string(),
            );
        }
        if inst.master().is_none() {
            return Err(format!("实例 {name} 缺少主节点,无法迁移"));
        }
        let rows = self.store.host_list();
        for h in &hosts {
            match rows.iter().find(|r| r["name"].as_str() == Some(h.as_str())) {
                Some(hr)
                    if hr["agent_port"].as_u64().unwrap_or(0) > 0
                        && hr["status"].as_str() != Some("retiring") => {}
                Some(hr) if hr["status"].as_str() == Some("retiring") => {
                    return Err(format!("宿主机 {h} 处于 retiring,不可作为迁移目标"));
                }
                _ => return Err(format!("宿主机 {h} 未登记或未接入 agent")),
            }
        }
        // 已全部在目标(绑定+region/az 一致)时是幂等 no-op → 友好报错避免空转
        let already = {
            let all_nodes: Vec<&InstNode> = inst.nodes.iter().collect();
            all_nodes.len() == 1
                && node_host_binding(&inst, &all_nodes[0].container)
                    .map(|s| s == hosts[0])
                    .unwrap_or(false)
                && inst.region == region
                && inst.az == az
        };
        if already && hosts.len() == 1 {
            return Err(format!(
                "实例 {name} 已在目标机房(region={region} az={az}),无需迁移"
            ));
        }
        // 调度水位(P2-⑥):各目标机容量粗校验;分布提示随审计记录
        let mut cap_notes: Vec<String> = Vec::new();
        for h in &hosts {
            cap_notes.push(self.schedule_check(h, 1)?);
        }
        let mut advisories: Vec<String> = Vec::new();
        for h in &hosts {
            advisories.extend(self.schedule_advisories(&name, h));
        }
        self.lock_instance(&name)?;
        if let Some(mut i) = self.instances.get_mut(&name) {
            i.status = InstStatus::Switching;
        }
        self.persist();
        self.store.audit(
            &crate::auth::current_user(),
            &name,
            "migrate_instance",
            &format!(
                "submitted region={region} az={az} hosts={}; {}; {}",
                hosts.join(","),
                cap_notes.join("; "),
                advisories.join("; ")
            ),
            "submitted",
            "",
        );
        let nodes = vec![crate::dag::TaskNode {
            id: "migrate".into(),
            name: format!("实例 {name} 迁移 → {region}/{az}"),
            deps: vec![],
            retries: 0,
            timeout_secs: Some(1800),
            steps: vec![
                crate::dag::Step::MigrateInstance {
                    instance: name.clone(),
                    region: region.clone(),
                    az: az.clone(),
                    hosts: hosts.join(","),
                },
                crate::dag::Step::UpdateInstanceStatus {
                    instance: name.clone(),
                    status: "running".into(),
                },
            ],
        }];
        let tid = self
            .scheduler
            .submit("migrate", &name, &crate::auth::current_user(), nodes);
        watch_task(&name.to_string(), tid.clone());
        Ok(tid)
    }

    // ─── 彻底删除实例记录(仅「已销毁」;任务/审计留痕保留) ───

    /// 把已销毁(destroyed)实例从控制台内存与持久化层一并移除。
    /// 前置:状态必须为 Destroyed —— 销毁动作本身由 destroy() 负责,二者不可混用;
    /// 已销毁实例不允许任何带锁操作(启停/扩容/再次销毁均被各自门槛拒绝),故此处不再取实例锁。
    /// 历史任务列表与审计日志仍保留该实例名作为留痕。
    pub fn delete_instance(&self, name: &str) -> Result<(), String> {
        let Some(i) = self.instances.get(name).map(|i| i.clone()) else {
            return Err(format!("实例 {name} 不存在"));
        };
        if i.status != InstStatus::Destroyed {
            return Err(format!(
                "仅「已销毁」的实例可彻底删除记录(当前状态 {:?});请先销毁实例",
                i.status
            ));
        }
        self.instances.remove(name);
        self.store.instance_delete(name);
        self.store.audit(
            &crate::auth::current_user(),
            name,
            "instance_delete",
            "removed",
            "ok",
            "",
        );
        Ok(())
    }

    // ─── 逻辑备份(样例功能模块模板;全流程见 docs/dag-module-howto.md) ───

    /// 运行中实例 → 主节点容器内 mysqldump 逻辑备份。
    /// 提交侧模板:状态门槛 → 停用检查 → 操作锁 → 审计 → 组合子造节点 → submit → 终态联动。
    /// 与生命周期任务不同:备份不改实例状态;失败时由 watch_backup_task 仅告警留痕、释放锁(可重试)。
    pub fn run_backup(self: &Arc<Self>, name: &str) -> Result<String, String> {
        let inst = self.assert_state(name, InstStatus::Running)?;
        if !inst.enabled {
            return Err(format!("实例 {name} 已停用(管理暂停),请先启用再操作"));
        }
        if inst.itype == "xenon" {
            // mysqldump 经 mysql CLI 在容器内执行,理论上可用;但 dump 一致性视角/从库选择
            // 与主从复制语义耦合,未经验证前先显式拒绝,避免产出伪备份
            return Err("xenon(raft)实例的备份链路尚未接入(主从复制语义的备份任务不适用)".into());
        }
        let mc = inst
            .master()
            .map(|m| m.container.clone())
            .unwrap_or_default();
        if mc.is_empty() {
            return Err(format!("实例 {name} 缺少主节点,无法执行备份"));
        }
        self.lock_instance(name)?;
        self.store.audit(
            &crate::auth::current_user(),
            name,
            "backup",
            "submitted",
            "",
            "",
        );
        let nodes = backup_nodes(name, &mc);
        let tid = self
            .scheduler
            .submit("backup", name, &crate::auth::current_user(), nodes);
        watch_backup_task(self, name, tid.clone());
        Ok(tid)
    }

    // ─── 功能模块:一键运行(见 docs/func-modules.md) ───

    /// 运行一个已保存的用户功能模块:把模块步骤实例化(占位符 {instance}/{master} →
    /// 目标实例名/主节点容器名)提交为一个 module 任务。沿用 backup 语义:
    /// 加实例锁、终态解锁,失败只告警留痕,**不修改实例状态**。
    pub fn run_module(
        self: &Arc<Self>,
        module_name: &str,
        instance: &str,
    ) -> Result<String, String> {
        let name = instance.trim();
        let inst = self.assert_state(name, InstStatus::Running)?;
        if !inst.enabled {
            return Err(format!("实例 {name} 已停用(管理暂停),请先启用再操作"));
        }
        let rec = self
            .store
            .module_list()
            .into_iter()
            .find(|m| m["name"].as_str().unwrap_or("") == module_name)
            .ok_or_else(|| format!("功能模块 {module_name} 不存在"))?;
        let steps_raw = rec["steps_json"].as_str().unwrap_or("");
        let raw_json = steps_raw.replace("{instance}", &inst.name).replace(
            "{master}",
            inst.master().map(|m| m.container.as_str()).unwrap_or(""),
        );
        let steps: Vec<crate::dag::Step> = serde_json::from_str(&raw_json)
            .map_err(|e| format!("模块 {module_name} 步骤解析失败: {e}"))?;
        if steps.is_empty() {
            return Err(format!("模块 {module_name} 没有步骤"));
        }
        let master_c = inst
            .master()
            .map(|m| m.container.clone())
            .unwrap_or_default();
        self.lock_instance(&inst.name)?;
        self.store.audit(
            &crate::auth::current_user(),
            &inst.name,
            "module_run",
            &format!("module={module_name}"),
            "submitted",
            "",
        );
        let nodes = vec![TaskNode {
            id: "m".into(),
            name: format!("执行模块:{module_name}"),
            deps: vec![],
            retries: 0,
            timeout_secs: None,
            steps,
        }];
        let tid = self
            .scheduler
            .submit("module", &inst.name, &crate::auth::current_user(), nodes);
        // master 为空理论上不可达(assert_state 保证节点存在),此处保留字段避免悬空
        let _ = master_c;
        watch_backup_task(self, &inst.name, tid.clone());
        Ok(tid)
    }

    // ─── DTS 链路(canal;dts-design §3):单独为某从节点创建/移除 ───
    //   语义:引擎容器真实拉起(占位),不做数据出口;一从一 DTS;失败不改实例状态,可重试。

    /// 全部/某实例的 DTS 链路
    pub fn dts_list(&self, instance: Option<&str>) -> Vec<serde_json::Value> {
        self.store.dts_list(instance)
    }

    /// 复制/半同步事实(P1,vtorc 式事实层):缓存 ≤10s 直读,否则即时采集
    pub async fn orch_facts(&self, name: &str) -> Vec<serde_json::Value> {
        let name = name.trim();
        if let Some((ts, facts)) = orch_cache_get(name) {
            if now().saturating_sub(ts) < 10 && !facts.is_empty() {
                return facts;
            }
        }
        let Some(inst) = self.instances.get(name).map(|e| e.value().clone()) else {
            return Vec::new();
        };
        let hosts = self.store.host_list();
        let facts = collect_replica_facts(&inst, &hosts).await;
        orch_cache_set(name, facts.clone());
        facts
    }

    /// 自动故障转移(ERS):登记主不可用且存在存活从 → 自动提升。
    /// 30s 巡检节奏触发;限频 60s;跳过原因写入 in-flight(面板可见)。
    async fn auto_failover_if_needed(&self, inst: &RdsInstance) {
        if inst.network.starts_with("demo-") {
            return;
        }
        // xenon(raft):leader 选举由 xenon 内建,管控面 ERS 不介入(巡检走 sweep_xenon,不会到达此处;
        // 此守卫防御未来其他调用路径)
        if inst.itype == "xenon" {
            return;
        }
        let facts = self.orch_facts(&inst.name).await;
        let Some(master_c) = inst
            .nodes
            .iter()
            .find(|n| n.role == Role::Master)
            .map(|n| n.container.clone())
        else {
            return;
        };
        // 主节点绑定远端宿主机:agent 已接入且可达 → 事实层可真实判定,允许自动 ERS;
        // agent 未接入/不可达 → 控制端无法触碰真实主库,不触发自动 ERS
        // (避免对仍在运行的远端主做错误提升造成脑裂),原因写入面板
        let hosts = self.store.host_list();
        let mroute = resolve_route_of(inst, &master_c, &hosts);
        let blind: Option<String> = match &mroute {
            NodeRoute::Unmanaged { host } => Some(format!(
                "主节点绑定宿主机 {host}(agent 未接入),不执行自动 ERS"
            )),
            NodeRoute::Agent { ag, host } if !ag.ping().await => Some(format!(
                "主节点绑定宿主机 {host}(agent 不可达),不执行自动 ERS"
            )),
            _ => None,
        };
        if let Some(msg) = blind {
            orch_op_set(
                &inst.name,
                serde_json::json!({
                    "state": "skipped", "kind": "ERS", "mode": "auto",
                    "target": "auto", "op_id": "", "ts": now(), "message": msg,
                }),
            );
            return;
        }
        let master_alive = facts.iter().any(|f| {
            f["container"].as_str() == Some(master_c.as_str()) && f["alive"].as_bool() == Some(true)
        });
        if master_alive {
            return; // 主可用
        }
        // 主不可用后的前置检查 → 跳过原因写入面板(in-flight)
        let skip = |msg: &str| {
            orch_op_set(
                &inst.name,
                serde_json::json!({
                    "state": "skipped", "kind": "ERS", "mode": "auto",
                    "target": "auto", "op_id": "", "ts": now(), "message": msg,
                }),
            );
        };
        if !inst.enabled || !inst.auto_failover {
            skip("实例未启用自动故障转移(auto_failover=off)或已停用");
            return;
        }
        if self.op_locks.contains_key(&inst.name) {
            skip("实例操作锁占用(有任务进行中)");
            return;
        }
        let slaves_ok = facts
            .iter()
            .filter(|f| f["role"].as_str() == Some("slave") && f["alive"].as_bool() == Some(true))
            .count();
        if slaves_ok == 0 {
            skip("无存活从节点候选(事实层无 alive 从)");
            return;
        }
        // 限频:60s 内已尝试过则跳过
        let nowt = now();
        {
            let mut g = AUTO_TRY.lock().unwrap();
            let m = g.get_or_insert_with(std::collections::HashMap::new);
            if m.get(&inst.name).map(|t| nowt - *t < 60).unwrap_or(false) {
                skip("60s 限频防抖,上次尝试后未满 60s");
                return;
            }
            m.insert(inst.name.clone(), nowt);
        }
        self.store.audit(
            "sweeper",
            &inst.name,
            "auto_failover",
            "登记主不可用,触发自动 ERS",
            "submitted",
            "",
        );
        let m = crate::manager();
        if let Err(e) = m.orch_reparent(&inst.name, None, "auto") {
            self.store
                .audit("sweeper", &inst.name, "auto_failover", &e, "failed", "");
            orch_op_set(
                &inst.name,
                serde_json::json!({
                    "state": "failed", "kind": "ERS", "mode": "auto",
                    "target": "auto", "op_id": "", "ts": now(), "finished": now(), "message": format!("自动切换提交失败:{e}"),
                }),
            );
        }
    }

    /// 受管主从切换(PRS/ERS 统一入口,见 docs/meta-authority.md):
    ///   auto  = ERS 快速通道(旧主不可达也可执行;候选自动/指定)
    ///   planned = PRS(要求旧主可达并重挂为新主从)
    /// 切换只允许从「登记」单写者流程改角色;全程审计;失败尝试回滚并降级告警。
    /// xenon(raft) 实例不适用:raft 自带 leader 选举/故障转移,拒绝混用 MySQL 复制切换语义。
    /// xenon raft 集群实况:逐节点 `xenoncli raft status`,汇总各节点状态与共识 leader。
    /// 说明:xenon 选主由 raft 共识自主完成(后台复制仍为半同步),管控面只读实况,
    /// 不做 orchestrator 受管切换/回滚。GET /api/rds/xenon/raft?instance=
    pub async fn xenon_raft_view(&self, name: &str) -> Result<serde_json::Value, String> {
        let name = name.trim();
        let Some(inst) = self.instances.get(name).map(|e| e.value().clone()) else {
            return Err(format!("实例 {name} 不存在"));
        };
        if inst.itype != "xenon" {
            return Err(format!("实例 {name} 非 xenon(raft)架构"));
        }
        let mut states = Vec::new();
        let mut votes: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        use serde_json::json;
        for n in &inst.nodes {
            let cell = match dk::exec_in(
                &n.container,
                &["xenoncli".into(), "raft".into(), "status".into()],
            )
            .await
            {
                Ok(out) => {
                    match serde_json::from_str::<serde_json::Value>(out.trim()) {
                        Ok(v) => {
                            if let Some(ep) = v.get("leader").and_then(|x| x.as_str()) {
                                if !ep.is_empty() {
                                    *votes.entry(ep.to_string()).or_insert(0) += 1;
                                }
                            }
                            v
                        }
                        Err(_) => {
                            json!({"state":"unknown","raw":out.trim(),"reachable":true})
                        }
                    }
                }
                Err(e) => json!({"state":"down","error":e,"reachable":false}),
            };
            states.push(json!({ "container": n.container, "raft": cell }));
        }
        // 共识 leader = 各节点自述中出现最多的 raft 端点(去掉 :8801 → 容器名)
        let leader = votes
            .iter()
            .max_by_key(|(_, c)| **c)
            .map(|(ep, _)| ep.split(':').next().unwrap_or(ep).to_string())
            .unwrap_or_default();
        Ok(json!({
            "ok": true,
            "leader": leader,
            "nodes": states,
            "updated": now(),
        }))
    }

    /// 让某节点尝试成为 raft leader(xenon 唯一的人工干预通道;仍经 raft 共识生效)。
    /// 目标必须是实例成员;已经是 leader 时返回其原状态(幂等)。
    /// POST /api/rds/xenon/raft/trytoleader?instance=&node=
    pub async fn xenon_raft_trytoleader(
        &self,
        instance: &str,
        node: &str,
    ) -> Result<String, String> {
        let name = instance.trim();
        let node = node.trim();
        let Some(inst) = self.instances.get(name).map(|e| e.value().clone()) else {
            return Err(format!("实例 {name} 不存在"));
        };
        if inst.itype != "xenon" {
            return Err(format!("实例 {name} 非 xenon(raft)架构"));
        }
        if !inst.nodes.iter().any(|n| n.container == node) {
            return Err(format!("节点 {node} 不属于实例 {name}"));
        }
        let out = dk::exec_in(
            node,
            &["xenoncli".into(), "raft".into(), "trytoleader".into()],
        )
        .await
        .map_err(|e| format!("执行切换失败: {e}"))?;
        let bad = out.to_lowercase().contains("error");
        self.store.audit(
            &crate::auth::current_user(),
            name,
            "xenon_trytoleader",
            &format!("node={node}"),
            if bad { "failed" } else { "ok" },
            "",
        );
        if bad {
            return Err(out.trim().to_string());
        }
        Ok(out.trim().to_string())
    }

    pub fn orch_reparent(
        self: &Arc<Self>,
        instance: &str,
        target: Option<&str>,
        mode: &str,
    ) -> Result<String, String> {
        let name = instance.trim().to_string();
        if name.is_empty() {
            return Err("缺少实例名".into());
        }
        if let Some(i) = self.instances.get(&name) {
            if i.itype == "xenon" {
                return Err(
                    "xenon(raft)实例由 xenon 自动选主,不支持管控面 orchestrator 切换".into(),
                );
            }
        }
        if !matches!(mode, "auto" | "planned") {
            return Err("mode 需为 auto(ERS) / planned(PRS)".to_string());
        }
        let inst = self.assert_state(&name, InstStatus::Running)?;
        if !inst.enabled {
            return Err(format!("实例 {name} 已停用(管理暂停),请先启用再操作"));
        }
        if inst.network.starts_with("demo-") {
            return Err("演示实例无真实容器,不支持受管切换".to_string());
        }
        if target.is_some() && target != Some("") {
            let t = target.as_deref().unwrap_or("");
            if !inst
                .nodes
                .iter()
                .any(|n| n.container == t && n.role != Role::Master)
            {
                return Err(format!("目标 {t} 不是该实例的从节点"));
            }
        }
        self.lock_instance(&name)?;
        self.store.audit(
            &crate::auth::current_user(),
            &name,
            "reparent",
            &format!("mode={mode},target={target:?}"),
            "submitted",
            "",
        );
        let mgr = self.clone();
        let t = target.map(|s| s.to_string()).filter(|s| !s.is_empty());
        let opid = format!("reparent-{name}");
        let opid2 = opid.clone();
        let m2 = mode.to_string();
        tokio::spawn(async move {
            run_reparent(&mgr, &name, t, &m2, &opid2).await;
            mgr.unlock_instance(&name);
        });
        Ok(opid)
    }

    /// 回滚到最近一次受管切换前的旧主(按 evidence 快照定位;仍走受管 auto 切换,审计留痕)
    pub fn orch_rollback(self: &Arc<Self>, instance: &str) -> Result<String, String> {
        let name = instance.trim().to_string();
        if name.is_empty() {
            return Err("缺少实例名".into());
        }
        let inst = self.assert_state(&name, InstStatus::Running)?;
        if inst.network.starts_with("demo-") {
            return Err("演示实例不支持回滚".to_string());
        }
        let snaps = self.store.evidence_latest(&name, 10);
        let snap = snaps
            .iter()
            .find(|e| e["kind"].as_str() == Some("reparent_snapshot"))
            .cloned()
            .ok_or_else(|| "无可用切换快照(尚未进行过受管切换)".to_string())?;
        let prev = snap["facts"]["prev_master"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| "快照缺少 prev_master".to_string())?;
        if !inst.nodes.iter().any(|n| n.container == prev) {
            return Err(format!("快照旧主 {prev} 已不在实例节点中,无法回滚"));
        }
        let cur = inst
            .nodes
            .iter()
            .find(|n| n.role == Role::Master)
            .map(|n| n.container.clone())
            .ok_or_else(|| "当前缺主节点,无法回滚".to_string())?;
        if cur == prev {
            return Ok(format!("noop:{prev} 已是当前主,无需回滚"));
        }
        self.lock_instance(&name)?;
        self.store.audit(
            &crate::auth::current_user(),
            &name,
            "reparent_rollback",
            &format!("prev_master={prev}"),
            "submitted",
            "",
        );
        let mgr = self.clone();
        let opid = format!("reparent-rollback-{name}");
        let opid2 = opid.clone();
        tokio::spawn(async move {
            run_reparent(&mgr, &name, Some(prev), "auto", &opid2).await;
            mgr.unlock_instance(&name);
        });
        Ok(opid)
    }

    /// 受管切换动作视图:in-flight(最新)+ 操作历史(每次一条;进程内保留,审计为持久留痕)
    pub fn orch_ops_view(&self, instance: &str) -> serde_json::Value {
        let name = instance.trim();
        let in_flight = orch_op_get(name).unwrap_or(serde_json::Value::Null);
        let mut rows = orch_hist_get(name);
        if rows.is_empty() {
            // 进程重启后回退审计(保留动作/时间/结果,kind 由 op_id 推断)
            rows = self
                .audit(60, Some(name), None, None)
                .into_iter()
                .filter(|a| {
                    matches!(
                        a["action"].as_str(),
                        Some("reparent") | Some("reparent_rollback")
                    ) && a["result"].as_str() != Some("submitted")
                })
                .map(|a| {
                    let opid = a["task_id"].as_str().unwrap_or("");
                    let kind = if opid.contains("rollback") {
                        "rollback"
                    } else if a["result"].as_str() == Some("ok") {
                        "ERS"
                    } else {
                        "ERS"
                    };
                    serde_json::json!({
                        "kind": kind, "mode": "auto", "target": "auto",
                        "op_id": opid, "state": a["result"], "ts": a["ts"],
                        "message": a["params"],
                    })
                })
                .collect();
        }
        serde_json::json!({ "in_flight": in_flight, "history": rows })
    }

    /// 为该实例的某个从节点登记 DTS 占位链路(canal;占位规格 spec 可空默认 2C4G)
    pub fn dts_create(
        self: &Arc<Self>,
        instance: &str,
        node: &str,
        target: Option<&str>,
        spec: Option<&str>,
    ) -> Result<String, String> {
        let name = instance.trim();
        let inst = self.assert_state(name, InstStatus::Running)?;
        if !inst.enabled {
            return Err(format!("实例 {name} 已停用(管理暂停),请先启用再操作"));
        }
        let node = node.trim();
        if node.is_empty() {
            return Err("请选择要挂载 DTS 的从节点".to_string());
        }
        if inst.itype == "xenon" {
            return Err("xenon(raft)实例无主从复制链路,DTS(canal 伪 master)链路不适用".to_string());
        }
        let Some(n) = inst.nodes.iter().find(|x| x.container == node) else {
            return Err(format!("实例 {name} 不存在节点 {node}"));
        };
        if n.role == Role::Master {
            return Err("DTS 只能挂到从节点(读从/离线从)".to_string());
        }
        if self
            .store
            .dts_list(Some(name))
            .iter()
            .any(|d| d["node"].as_str() == Some(node))
        {
            return Err(format!(
                "节点 {node} 已有 DTS 链路(一从一 DTS,请先移除再重建)"
            ));
        }
        self.lock_instance(name)?;
        let target_label = target.unwrap_or("数据仓库(BigData)").to_string();
        let dts_spec = spec.unwrap_or("2C4G").to_string();
        let dts_c = dts_container(name, &n.container);
        self.store.audit(
            &crate::auth::current_user(),
            name,
            "dts_create",
            &format!("node={node},engine=canal,target={target_label},spec={dts_spec}"),
            "submitted",
            "",
        );
        // 创建「空容器」占位 DTS:轻量镜像常驻(不跑 DTS 引擎);由 DtsRun 步骤注册记录 + 起容器
        let nodes = vec![TaskNode {
            id: "dts".into(),
            name: format!("创建 DTS 空容器 {dts_c}"),
            deps: vec![],
            retries: 1,
            timeout_secs: Some(600),
            steps: vec![Step::DtsRun {
                instance: name.to_string(),
                node: n.container.clone(),
                dts_container: dts_c.clone(),
                target_label,
                spec: dts_spec,
            }],
        }];
        let tid = self
            .scheduler
            .submit("dts", name, &crate::auth::current_user(), nodes);
        watch_dts_lifecycle(self, name, &n.container, tid.clone(), "create");
        Ok(tid)
    }

    /// 移除某从节点的 DTS(停/删空容器 + 清注册)
    pub fn dts_remove(self: &Arc<Self>, instance: &str, node: &str) -> Result<String, String> {
        let name = instance.trim();
        let node = node.trim();
        let inst = self.assert_state(name, InstStatus::Running)?;
        if !inst.enabled {
            return Err(format!("实例 {name} 已停用(管理暂停),请先启用再操作"));
        }
        let rec = self
            .store
            .dts_list(Some(name))
            .into_iter()
            .find(|d| d["node"].as_str() == Some(node))
            .ok_or_else(|| format!("节点 {node} 没有 DTS 链路"))?;
        let dts_c = rec["container"].as_str().unwrap_or("").to_string();
        let engine = rec["engine"].as_str().unwrap_or("canal").to_string();
        let target = rec["target_label"].as_str().unwrap_or("").to_string();
        let spec = rec["spec"].as_str().unwrap_or("").to_string();
        self.lock_instance(name)?;
        self.store.audit(
            &crate::auth::current_user(),
            name,
            "dts_remove",
            &format!("node={node},container={dts_c}"),
            "submitted",
            "",
        );
        self.store
            .dts_upsert(name, node, &engine, &dts_c, &target, "removing", "", &spec);
        let nodes = vec![TaskNode {
            id: "dts_rm".into(),
            name: format!("移除 DTS 空容器 {dts_c}"),
            deps: vec![],
            retries: 1,
            timeout_secs: Some(120),
            steps: vec![Step::DockerRm {
                container: dts_c.clone(),
            }],
        }];
        let tid = self
            .scheduler
            .submit("dts_rm", name, &crate::auth::current_user(), nodes);
        watch_dts_lifecycle(self, name, node, tid.clone(), "remove");
        Ok(tid)
    }

    // ─── 持久化(MySQL) ───

    // ─── 演示测试数据(env RDSCTL_DEMO_SEED=1 且库中无实例时种入) ───

    /// 仅当 `enabled && 实例库为空` 时,种入一批带 业务线/DBA/版本/规格/容量 标签的
    /// 演示实例记录(控制面标签,不实际起容器;network 以 `demo-` 开头,健康巡检自动跳过,
    /// 不会因容器缺失而降级刷告警)。用于概览分布图表、代理汇总等页面的演示数据。
    pub fn seed_demo_if_empty(&self, enabled: bool) {
        if !enabled || !self.instances.is_empty() {
            return;
        }
        if !self.store.instance_load_all().is_empty() {
            tracing::info!("演示种子:实例库非空,跳过(设 RDSCTL_DEMO_SEED=1 且空库生效)");
            return;
        }
        // (name, biz, dba, contact, region, az, itype, mysql_version, spec, data_size_gib, core)
        const ROWS: &[(
            &str,
            &str,
            &str,
            &str,
            &str,
            &str,
            &str,
            &str,
            &str,
            u64,
            bool,
        )] = &[
            (
                "demo-pay-01",
                "支付交易",
                "li.si",
                "wang.wu",
                "cn-east",
                "az1",
                "async",
                "5.7",
                "8C16G",
                128,
                true,
            ),
            (
                "demo-order-01",
                "订单中心",
                "zhang.san",
                "zhao.liu",
                "cn-north",
                "az1",
                "async",
                "8.0",
                "16C32G",
                512,
                true,
            ),
            (
                "demo-user-02",
                "会员营销",
                "chen.er",
                "li.si",
                "cn-north",
                "az2",
                "sync",
                "5.7",
                "8C16G",
                96,
                true,
            ),
            (
                "demo-risk-03",
                "风控模型",
                "zhao.liu",
                "sun.qi",
                "cn-north",
                "az3",
                "single",
                "5.7",
                "4C8G",
                32,
                true,
            ),
            (
                "demo-trade-04",
                "电商交易",
                "wang.wu",
                "zhou.bing",
                "cn-south",
                "az1",
                "async",
                "5.7",
                "32C64G",
                1024,
                true,
            ),
            (
                "demo-coupon-05",
                "优惠券",
                "li.si",
                "zhang.san",
                "cn-east",
                "az2",
                "async",
                "5.7",
                "8C16G",
                128,
                false,
            ),
            (
                "demo-inv-06",
                "库存物流",
                "zhou.bing",
                "chen.er",
                "cn-south",
                "az2",
                "async",
                "5.7",
                "16C32G",
                256,
                true,
            ),
            (
                "demo-search-07",
                "搜索推荐",
                "sun.qi",
                "wang.wu",
                "cn-north",
                "az1",
                "sync",
                "8.0",
                "16C32G",
                256,
                true,
            ),
            (
                "demo-msg-08",
                "消息推送",
                "zhang.san",
                "li.si",
                "cn-east",
                "az3",
                "async",
                "5.7",
                "4C8G",
                16,
                false,
            ),
            (
                "demo-log-09",
                "日志分析",
                "chen.er",
                "zhao.liu",
                "cn-south",
                "az1",
                "async",
                "8.4",
                "32C64G",
                512,
                false,
            ),
            (
                "demo-bi-10",
                "BI 报表",
                "feng.jiu",
                "sun.qi",
                "cn-east",
                "az2",
                "single",
                "8.4",
                "8C16G",
                64,
                false,
            ),
            (
                "demo-acc-11",
                "账务结算",
                "zhao.liu",
                "zhou.bing",
                "cn-north",
                "az2",
                "async",
                "5.7",
                "16C32G",
                128,
                true,
            ),
            (
                "demo-content-12",
                "内容社区",
                "wang.wu",
                "chen.er",
                "cn-east",
                "az1",
                "async",
                "5.7",
                "8C16G",
                96,
                false,
            ),
            (
                "demo-dev-13",
                "研发测试",
                "zheng.shi",
                "feng.jiu",
                "cn-south",
                "az3",
                "single",
                "5.7",
                "2C4G",
                8,
                false,
            ),
            (
                "demo-fin-14",
                "信贷金融",
                "zhou.bing",
                "zhang.san",
                "cn-north",
                "az1",
                "async",
                "5.7",
                "32C64G",
                1024,
                true,
            ),
            (
                "demo-internal-15",
                "内部平台",
                "sun.qi",
                "wang.wu",
                "cn-north",
                "az2",
                "async",
                "8.0",
                "4C8G",
                32,
                false,
            ),
        ];
        let now = now();
        let mut max_port = 0u64;
        let mut max_sid = 0u64;
        for (i, (name, biz, dba, contact, region, az, itype, ver, spec, data_gib, core)) in
            ROWS.iter().enumerate()
        {
            let name = name.to_string();
            let base_px = 44_000u64 + i as u64 * 2;
            let base_hp = 55_000u64 + i as u64 * 3;
            let proxies_n: u32 = if *itype == "single" { 1 } else { 2 };
            let mut proxies = Vec::new();
            for k in 0..proxies_n {
                // 演示「Proxy 灰度发布」:demo-order-01 的首个 proxy 已升级到 2024.2-rc(其余仍实例基线 2024.1)
                let gray_ver = if name == "demo-order-01" && k == 0 {
                    "2shard-2024.2-rc".to_string()
                } else {
                    String::new()
                };
                proxies.push(ProxyNode {
                    container: format!("rds-{name}-proxy-{}", k + 1),
                    mysql_port: (base_px + k as u64) as u16,
                    mng_port: (base_px + 20_000 + k as u64) as u16,
                    spec: spec.to_string(),
                    ip: String::new(),
                    qps: 0,
                    conns: 0,
                    cpu: 0.0,
                    status: String::new(),
                    version: gray_ver,
                });
            }
            let mut nodes = vec![InstNode {
                container: format!("rds-{name}-master"),
                role: Role::Master,
                host: format!("rds-{name}-master"),
                port: 3306,
                host_port: base_hp as u16,
                server_id: 1000 + i as u64 * 10,
                region: region.to_string(),
                az: az.to_string(),
                shard: "s0".into(),
                parent: String::new(),
                rpc_host_port: 0,
            }];
            if *itype != "single" {
                nodes.push(InstNode {
                    container: format!("rds-{name}-slave-1"),
                    role: Role::Read,
                    host: format!("rds-{name}-slave-1"),
                    port: 3306,
                    host_port: (base_hp + 1) as u16,
                    server_id: 1000 + i as u64 * 10 + 1,
                    region: region.to_string(),
                    az: az.to_string(),
                    shard: "s0".into(),
                    parent: format!("rds-{name}-master"),
                    rpc_host_port: 0,
                });
            }
            // buffer_pool 与前端一致:规格内存 × 75%
            let ram = spec
                .split('G')
                .next()
                .and_then(|s| s.rsplit('C').next())
                .and_then(|s| s.trim().parse::<u64>().ok())
                .unwrap_or(8);
            let pool = (ram as f64 * 0.75).round().max(1.0) as u64;
            let inst = RdsInstance {
                lvs: Vec::new(),
                lvs_container: String::new(),
                lvs_mysql_port: 0,
                node_states: std::collections::HashMap::new(),
                node_hosts: std::collections::HashMap::new(),
                auto_failover: true,
                proxies: proxies.clone(),
                shards: Vec::new(),
                biz: biz.to_string(),
                contact: contact.to_string(),
                dba: dba.to_string(),
                core: *core,
                itype: itype.to_string(),
                mysql_version: ver.to_string(),
                proxy_version: "2shard-2024.1".into(),
                spec: spec.to_string(),
                shard_num: if *data_gib >= 500 { 8 } else { 1 },
                data_size: format!("{data_gib}GiB"),
                buffer_pool: format!("{pool}GiB"),
                max_qps: 0,
                max_tps: 0,
                name: name.clone(),
                status: InstStatus::Running,
                region: region.to_string(),
                az: az.to_string(),
                shard: "s0".into(),
                tenant: "demo".into(),
                enabled: true,
                network: format!("demo-net-{name}"),
                nodes,
                proxy_container: proxies[0].container.clone(),
                proxy_mysql_port: proxies[0].mysql_port,
                proxy_mng_port: proxies[0].mng_port,
                created_at: now - (ROWS.len() as u64 - i as u64) * 3600,
                root_password: ROOT_PASS.to_string(),
                query_secret: String::new(),
                last_error: String::new(),
            };
            max_port = max_port
                .max(inst.proxy_mysql_port as u64)
                .max(inst.proxy_mng_port as u64)
                .max(base_hp);
            max_sid = max_sid.max(1000 + i as u64 * 10 + 1);
            self.instances.insert(name, inst);
        }
        self.next_port.store(max_port + 1, Ordering::Relaxed);
        self.next_server_id.store(max_sid + 1, Ordering::Relaxed);
        // 演示 DTS 占位链路:按固定 ROWS 顺序给前 4 个非 single 实例的从节点登记 running,便于拓扑展示(确定性)
        let mut dts_seeded = 0usize;
        for row in ROWS {
            if dts_seeded >= 4 {
                break;
            }
            if row.6 == "single" {
                continue;
            }
            let Some(ins) = self.instances.get(row.0).map(|e| e.value().clone()) else {
                continue;
            };
            if let Some(slave) = ins.nodes.iter().find(|n| n.role != Role::Master) {
                let label = if ins.name.contains("bi") || ins.name.contains("log") {
                    "数据仓库(BigData)"
                } else {
                    "下游业务(Downstream)"
                };
                self.store.dts_upsert(
                    &ins.name,
                    &slave.container,
                    "canal",
                    &dts_container(&ins.name, &slave.container),
                    label,
                    "running",
                    "",
                    "2C4G",
                );
                dts_seeded += 1;
            }
        }
        self.persist();
        tracing::info!(
            "演示种子:已种入 {} 个演示实例(demo-*,network=demo-net-* 巡检豁免)",
            ROWS.len()
        );
    }

    fn load_persisted(&self) {
        let mut max_port = 35000u64;
        let mut max_sid = 1u64;
        for (name, data) in self.store.instance_load_all() {
            let Ok(mut i) = serde_json::from_str::<RdsInstance>(&data) else {
                tracing::warn!("实例 {name} 记录解析失败,跳过");
                continue;
            };
            // 旧记录无 region/shard → 归一到本进程默认(向后兼容)
            if i.region.is_empty() {
                i.region = sys_region();
            }
            if i.az.is_empty() {
                i.az = sys_az();
            }
            if i.shard.is_empty() {
                i.shard = sys_shard();
            }
            // 上次进程中断时处于操作中的实例 → 失败(不允许悬空状态)。
            // **cluster 模式例外**:该实例可能正被 leader 续跑/接管,若本副本在内存里强判
            // Failed,会出现"同一实例在不同副本状态不同"(实测:leader 已把实例跑成 running,
            // follower 仍显示 failed)。集群模式保留持久化状态,由续跑与巡检收敛。
            if !crate::ha::runtime::cluster_mode()
                && matches!(
                    i.status,
                    InstStatus::Creating
                        | InstStatus::Scaling
                        | InstStatus::Switching
                        | InstStatus::Destroying
                )
            {
                i.status = InstStatus::Failed;
                i.last_error = "进程中断,操作未完成(可销毁残留后重建)".into();
            }
            for n in &i.nodes {
                max_port = max_port.max(n.host_port as u64);
                max_sid = max_sid.max(n.server_id);
            }
            max_port = max_port
                .max(i.proxy_mysql_port as u64)
                .max(i.proxy_mng_port as u64);
            self.instances.insert(name, i);
        }
        // 端口/自增 id 从已加载实例之后继续,避免重启后新实例与存活实例端口冲突
        self.next_port.store(max_port + 1, Ordering::Relaxed);
        self.next_server_id.store(max_sid + 1, Ordering::Relaxed);
    }

    fn persist(&self) {
        let now = now();
        for e in self.instances.iter() {
            let i = e.value();
            let data = serde_json::to_string(&*i).unwrap_or_default();
            self.store.instance_upsert(
                &i.name,
                &i.region,
                &i.tenant,
                &data,
                &status_tag(i.status),
                now,
            );
        }
    }

    // ─── 健康巡检 ───

    /// 启动后台巡检(须在 Arc 构造完成后调用:持有自引用 Arc,不经过全局 manager,避免启动期重入)
    pub fn start_sweeper(self: &Arc<Self>) {
        // 周期可用 RDSCTL_SWEEP_SECS 覆盖(验收/演示用短周期),默认 30s
        let secs: u64 = std::env::var("RDSCTL_SWEEP_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(30)
            .max(1);
        tracing::info!("健康巡检启动:每 {secs}s 一次");
        let mgr = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(secs)).await;
                mgr.sweep_once().await;
            }
        });
    }

    /// 巡检 running/degraded 实例:容器存在 + 代理连通 + 从库可达 + 复制线程;
    /// 异常 → degraded(带审计),恢复 → running(带审计)
    async fn sweep_once(&self) {
        let scan: Vec<RdsInstance> = self
            .instances
            .iter()
            .filter(|e| {
                // demo 种子(演示标签数据,无真实容器)→ 巡检豁免,避免降级刷告警
                if e.value().network.starts_with("demo-") {
                    return false;
                }
                matches!(e.value().status, InstStatus::Running | InstStatus::Degraded)
            })
            .map(|e| e.value().clone())
            .collect();
        for inst in scan {
            if self.op_locks.contains_key(&inst.name) {
                continue; // 生命周期操作进行中,不打扰
            }
            // xenon(raft)架构:无 proxy/LVS、无 MySQL 复制链;raft 自带故障转移。
            // 探测语义 = 节点容器存活 + MySQL 可达 + raft 视角多数派(节点健康即集群健康)
            if inst.itype == "xenon" {
                self.sweep_xenon(&inst).await;
                continue;
            }
            // 接入层(LVS)兜底:进程重启后按持久化实例重建转发器(幂等,已存在则跳过)
            if inst.lvs_mysql_port > 0 && !inst.lvs_container.is_empty() {
                if let Err(e) = crate::lvs::ensure(&inst) {
                    tracing::warn!("实例 {} 接入层恢复失败: {e}", inst.name);
                }
            }
            let mut problems: Vec<String> = Vec::new();
            if !dk::exists(&inst.proxy_container).await {
                problems.push(format!("代理容器 {} 缺失", inst.proxy_container));
            }
            // 节点级健康事实:容器缺失/服务停止/MySQL 不可达/复制中断 → 写入 node_states 并形成问题。
            // 执行按节点路由(P2-①):绑定宿主机且 agent 已接入 → 经 agent 探测;未接入 → remote 标记
            let hosts = self.store.host_list();
            let mut ns: std::collections::HashMap<String, String> =
                std::collections::HashMap::new();
            for n in &inst.nodes {
                let route = resolve_route_of(&inst, &n.container, &hosts);
                let st = match &route {
                    NodeRoute::Unmanaged { host } => {
                        problems.push(format!(
                            "节点 {} 绑定宿主机 {host},agent 未接入(已跳过本机检查)",
                            n.container
                        ));
                        "remote".to_string()
                    }
                    _ => {
                        let (st, prob) = probe_route(&route, n).await;
                        if let Some(p) = prob {
                            problems.push(p);
                        }
                        st
                    }
                };
                ns.insert(n.container.clone(), st);
            }
            self.apply_node_states(&inst.name, ns);
            // vtorc 式事实层:每个巡检周期刷新复制/半同步事实缓存(供 UI 双源标注)
            let _ = self.orch_facts(&inst.name).await;
            // 自动故障转移(ERS):登记主不可用且存在存活复制从 → 自动提升(限频防抖)
            self.auto_failover_if_needed(&inst).await;
            if problems.is_empty() {
                // 连通性探测优先走 LVS 接入 VIP(真实穿过转发器到达 Proxy);
                // 未登记接入层的旧实例回退直连诊断端口
                let via_vip = inst.lvs_mysql_port > 0 && !inst.lvs_container.is_empty();
                let probe_port = if via_vip {
                    inst.lvs_mysql_port
                } else {
                    inst.proxy_mysql_port
                };
                match dk::host_mysql(probe_port, "root", ROOT_PASS, "SELECT 1").await {
                    Ok(_) => {}
                    Err(e) => {
                        problems.push(if via_vip {
                            format!(
                                "接入层 VIP 127.0.0.1:{probe_port} 不可达(可直连诊断 :{} 复核): {e}",
                                inst.proxy_mysql_port
                            )
                        } else {
                            format!("代理不可达: {e}")
                        });
                    }
                }
            }
            if problems.is_empty() {
                // 全部正常:若此前 degraded → 恢复 running,并自动关闭该实例告警
                if inst.status == InstStatus::Degraded {
                    tracing::info!("实例 {} 巡检恢复正常", inst.name);
                    self.set_status(&inst.name, InstStatus::Running, "");
                    self.store
                        .audit("sweeper", &inst.name, "recover", "", "ok", "");
                    self.store.alert_resolve_instance(&inst.name);
                }
            } else {
                let msg = problems.join("; ");
                // 快照仅在「降级跃迁」或「原因变化」时写,同问题不每 30s 刷(AI-0,防写放大)
                if snapshot_write_needed(inst.status == InstStatus::Running, &inst.last_error, &msg)
                {
                    let facts = capture_degrade_evidence(&inst, &hosts).await;
                    self.store
                        .evidence_insert(&inst.name, "degrade", &msg, &facts);
                }
                if inst.status == InstStatus::Running {
                    self.set_status(&inst.name, InstStatus::Degraded, &msg);
                    self.store
                        .audit("sweeper", &inst.name, "degrade", &msg, "degraded", "");
                    // 告警:降级(巡检异常)按严重度 warn;复制中断/代理不可达等提升提示
                    let sev = if msg.contains("复制中断") {
                        "critical"
                    } else {
                        "warn"
                    };
                    self.store.alert_open(&inst.name, "degraded", sev, &msg);
                    tracing::warn!("实例 {} 降级: {msg}", inst.name);
                } else if inst.last_error != msg {
                    // 已降级:仅更新原因,不重复刷审计
                    self.set_status(&inst.name, InstStatus::Degraded, &msg);
                }
            }
        }
    }

    /// xenon(raft)实例巡检(sweep_once 的 xenon 分支):
    ///   1. 节点级:容器存活 + MySQL 可达(xenon 无 MySQL 复制线程,跳过 repl 探针);
    ///   2. 集群级:经任一可达节点 `xenoncli raft status` 取 raft 视角(state/leader),
    ///      读不到 = xenon 管理面不可达(告警);能读到 = 记录当前 leader 供 UI 展示。
    /// 降级/恢复/告警/快照语义与主从架构 sweep_once 完全一致。
    async fn sweep_xenon(&self, inst: &RdsInstance) {
        let hosts = self.store.host_list();
        let mut problems: Vec<String> = Vec::new();
        let mut ns: std::collections::HashMap<String, String> = std::collections::HashMap::new();
        for n in &inst.nodes {
            let route = resolve_route_of(inst, &n.container, &hosts);
            let st = match &route {
                NodeRoute::Unmanaged { host } => {
                    problems.push(format!(
                        "节点 {} 绑定宿主机 {host},agent 未接入(已跳过本机检查)",
                        n.container
                    ));
                    "remote".to_string()
                }
                _ => {
                    // 复用通用探测但剥离复制探针:xenon 节点无 IO/SQL 复制线程,
                    // replica_problem_route 会把正常 raft 从节点误报为"复制中断"
                    let (st, prob) = probe_xenon_node(&route, n).await;
                    if let Some(p) = prob {
                        problems.push(p);
                    }
                    st
                }
            };
            ns.insert(n.container.clone(), st);
        }
        self.apply_node_states(&inst.name, ns);
        // 集群级 raft 视角(尽力而为:节点全挂时上面已报 missing/down)
        if problems.is_empty() {
            // 前置 rsproxy 接入层:经 VIP(或直连首个 proxy)探活,与主从架构代理巡检同语义
            let via_vip = inst.lvs_mysql_port > 0 && !inst.lvs_container.is_empty();
            if via_vip {
                if let Err(e) = crate::lvs::ensure(inst) {
                    tracing::warn!("实例 {} 接入层恢复失败: {e}", inst.name);
                }
            }
            let probe_port = if via_vip { inst.lvs_mysql_port } else { inst.proxy_mysql_port };
            if probe_port > 0 {
                if let Err(e) = dk::host_mysql(probe_port, "root", ROOT_PASS, "SELECT 1").await {
                    problems.push(if via_vip {
                        format!(
                            "接入层 VIP 127.0.0.1:{probe_port} 不可达(可直连代理 :{} 复核): {e}",
                            inst.proxy_mysql_port
                        )
                    } else {
                        format!("代理不可达: {e}")
                    });
                }
            }
        }
        if problems.is_empty() {
            let mut raft_seen = false;
            for n in &inst.nodes {
                if let Ok(out) = dk::exec_in(
                    &n.container,
                    &["xenoncli".into(), "raft".into(), "status".into()],
                )
                .await
                {
                    raft_seen = true;
                    if out.contains("\"state\":\"LEADER\"") {
                        tracing::debug!("实例 {} raft leader = {}", inst.name, n.container);
                    }
                    break;
                }
            }
            if !raft_seen {
                problems.push("xenon 管理面不可达(xenoncli raft status 全节点失败)".to_string());
            }
        }
        if problems.is_empty() {
            if inst.status == InstStatus::Degraded {
                tracing::info!("实例 {} 巡检恢复正常(xenon)", inst.name);
                self.set_status(&inst.name, InstStatus::Running, "");
                self.store
                    .audit("sweeper", &inst.name, "recover", "", "ok", "");
                self.store.alert_resolve_instance(&inst.name);
            }
        } else {
            let msg = problems.join("; ");
            if snapshot_write_needed(inst.status == InstStatus::Running, &inst.last_error, &msg) {
                let facts = capture_degrade_evidence(inst, &hosts).await;
                self.store
                    .evidence_insert(&inst.name, "degrade", &msg, &facts);
            }
            if inst.status == InstStatus::Running {
                self.set_status(&inst.name, InstStatus::Degraded, &msg);
                self.store
                    .audit("sweeper", &inst.name, "degrade", &msg, "degraded", "");
                let sev = if msg.contains("节点") && msg.contains("缺失") {
                    "critical"
                } else {
                    "warn"
                };
                self.store.alert_open(&inst.name, "degraded", sev, &msg);
                tracing::warn!("实例 {} 降级(xenon): {msg}", inst.name);
            } else if inst.last_error != msg {
                self.set_status(&inst.name, InstStatus::Degraded, &msg);
            }
        }
    }
}

/// 从库复制健康检查:两个复制线程(IO connection / SQL applier)均 ON 才算健康。
/// 按节点路由执行(P2-①:agent 接入节点经 agent 查询)
async fn replica_problem_route(r: &NodeRoute, container: &str) -> Option<String> {
    let sql = "SELECT IF(EXISTS(SELECT 1 FROM performance_schema.replication_connection_status \
               WHERE CHANNEL_NAME='' AND SERVICE_STATE='ON') \
               AND EXISTS(SELECT 1 FROM performance_schema.replication_applier_status \
               WHERE CHANNEL_NAME='' AND SERVICE_STATE='ON'),'OK','STOPPED')";
    match r_sql(r, container, "root", ROOT_PASS, sql).await {
        Ok(o) if o.trim() == "OK" => None,
        Ok(_) => Some(format!("从库 {container} 复制中断(IO/SQL 线程未运行)")),
        Err(e) => Some(format!("从库 {container} 复制状态查询失败: {e}")),
    }
}

// ─── AI-0 异常快照(见 docs/ai0-impl-checklist.md §2) ───

/// 复制线程连接侧硬事实:ServiceState|LAST_ERROR_NUMBER|LAST_ERROR_MESSAGE
const EVIDENCE_SQL_CONN: &str = "SELECT CONCAT_WS('|', IFNULL(SERVICE_STATE,''), \
    IFNULL(LAST_ERROR_NUMBER,''), IFNULL(LAST_ERROR_MESSAGE,'')) \
    FROM performance_schema.replication_connection_status WHERE CHANNEL_NAME=''";
/// 复制线程应用侧硬事实:State|LAST_ERROR_NUMBER|LAST_ERROR_MESSAGE
const EVIDENCE_SQL_APPLIER: &str = "SELECT CONCAT_WS('|', IFNULL(STATE,''), \
    IFNULL(LAST_ERROR_NUMBER,''), IFNULL(LAST_ERROR_MESSAGE,'')) \
    FROM performance_schema.replication_applier_status_by_coordinator WHERE CHANNEL_NAME=''";

/// 采集 degrade 现场的硬事实(容器状态/exit code/复制线程 LAST_ERROR/日志尾),脱敏后返回 JSON 串。
/// 采集失败不阻断降级:内部全部降级为字段值/空。
async fn capture_degrade_evidence(inst: &RdsInstance, hosts: &[serde_json::Value]) -> String {
    let mut containers: Vec<serde_json::Value> = Vec::new();
    let mut logs_budget = 6000usize; // 日志总量字符封顶(≈60 行量级)
    let mut logs: Vec<serde_json::Value> = Vec::new();
    // 节点路由表(容器状态/从库硬事实/日志尾按路由;P2-① agent)
    let routes: Vec<(String, NodeRoute)> = inst
        .nodes
        .iter()
        .map(|n| {
            (
                n.container.clone(),
                resolve_route_of(inst, &n.container, hosts),
            )
        })
        .collect();
    // 代理容器(接入层 v0 为进程内/本机,恒走本机)
    match dk::container_state(&inst.proxy_container).await {
        Some(st) => containers.push(serde_json::json!({
            "container": inst.proxy_container, "role": "proxy", "state": st })),
        None => containers.push(serde_json::json!({
            "container": inst.proxy_container, "role": "proxy", "present": false })),
    }
    for (c, route) in &routes {
        match route {
            // 绑定宿主机但 agent 未接入:不留伪造的“容器缺失”事实,标记 remote
            NodeRoute::Unmanaged { host } => containers.push(serde_json::json!({
                "container": c, "role": inst.nodes.iter().find(|n| &n.container == c)
                    .map(|n| n.role.label()).unwrap_or("node"),
                "host": host, "remote": true, "agent": "not_connected",
            })),
            _ => match r_state(route, c).await {
                Some(st) => containers.push(serde_json::json!({
                    "container": c, "role": inst.nodes.iter().find(|n| &n.container == c)
                        .map(|n| n.role.label()).unwrap_or("node"),
                    "state": st })),
                None => containers.push(serde_json::json!({
                    "container": c, "role": inst.nodes.iter().find(|n| &n.container == c)
                        .map(|n| n.role.label()).unwrap_or("node"),
                    "present": false })),
            },
        }
    }
    // 代理连通
    let proxy = match dk::host_mysql(inst.proxy_mysql_port, "root", ROOT_PASS, "SELECT 1").await {
        Ok(o) => serde_json::json!({
            "mysql_port": inst.proxy_mysql_port, "reachable": true, "select1": o.trim() }),
        Err(e) => {
            let msg = redact_secret(&e);
            logs.push(serde_json::json!({
                "container": inst.proxy_container, "lines": clip(&msg, 400) }));
            serde_json::json!({
                "mysql_port": inst.proxy_mysql_port, "reachable": false,
                "error": clip(&msg, 200) })
        }
    };
    // 从库硬事实(按路由)
    let mut slaves: Vec<serde_json::Value> = Vec::new();
    for n in inst.slaves() {
        let mut rec = serde_json::json!({ "container": n.container, "role": n.role.label() });
        let route = routes
            .iter()
            .find(|(c, _)| c == &n.container)
            .map(|(_, r)| r)
            .unwrap_or(&NodeRoute::Local);
        if matches!(route, NodeRoute::Unmanaged { .. }) {
            rec["reachable"] = serde_json::json!(false);
            rec["error"] = serde_json::json!("agent 未接入(本机执行已跳过)");
            slaves.push(rec);
            continue;
        }
        match r_sql(route, &n.container, "root", ROOT_PASS, "SELECT 1").await {
            Ok(_) => {
                rec["reachable"] = serde_json::json!(true);
                let conn = r_sql(route, &n.container, "root", ROOT_PASS, EVIDENCE_SQL_CONN)
                    .await
                    .unwrap_or_default();
                let app = r_sql(route, &n.container, "root", ROOT_PASS, EVIDENCE_SQL_APPLIER)
                    .await
                    .unwrap_or_default();
                rec["replication"] = serde_json::json!({
                    "connection": conn.trim(),
                    "applier": app.trim(),
                });
                let gtid = r_sql(
                    route,
                    &n.container,
                    "root",
                    ROOT_PASS,
                    "SELECT @@GLOBAL.gtid_executed",
                )
                .await
                .unwrap_or_default();
                rec["gtid_executed"] = serde_json::json!(gtid.trim());
            }
            Err(e) => {
                rec["reachable"] = serde_json::json!(false);
                let msg = redact_secret(&e);
                rec["error"] = serde_json::json!(clip(&msg, 200));
                logs.push(serde_json::json!({
                    "container": n.container, "lines": clip(&msg, 400) }));
            }
        }
        slaves.push(rec);
    }
    // 日志尾:仅异常容器/代理(总量封顶);agent 未接入的远端容器无日志通道,跳过
    for c in std::iter::once(inst.proxy_container.as_str())
        .chain(inst.nodes.iter().map(|n| n.container.as_str()))
    {
        let bad = containers.iter().any(|v| {
            v["container"] == c
                && (v.get("present") == Some(&serde_json::json!(false))
                    || v.get("state").is_none()
                    || !v["state"].as_str().unwrap_or("").starts_with("running"))
        });
        if !bad && proxy["reachable"] != serde_json::json!(false) && c != inst.proxy_container {
            continue;
        }
        if logs.len() >= 4 {
            break;
        }
        let route = routes
            .iter()
            .find(|(cc, _)| cc == c)
            .map(|(_, r)| r)
            .unwrap_or(&NodeRoute::Local);
        if matches!(route, NodeRoute::Unmanaged { .. }) {
            continue;
        }
        let tail = match route {
            NodeRoute::Local => dk::logs_tail(c, 40).await,
            NodeRoute::Agent { ag, .. } => ag.docker(&["logs", "--tail", "40", c]).await,
            NodeRoute::Unmanaged { .. } => Ok(String::new()),
        };
        if let Ok(l) = tail {
            let l = redact_secret(&l);
            let take = logs_budget.min(l.len());
            logs_budget -= take;
            logs.push(serde_json::json!({ "container": c, "lines": clip(&l, take) }));
            if logs_budget == 0 {
                break;
            }
        }
    }
    serde_json::json!({
        "containers": containers,
        "proxy": proxy,
        "slaves": slaves,
        "logs": logs,
    })
    .to_string()
}

/// 采集文本脱敏(口令等敏感常量)
fn redact_secret(s: &str) -> String {
    s.replace(ROOT_PASS, "***").replace(REPL_PASS, "***")
}

/// 按字符截断(边界安全)
fn clip(s: &str, n: usize) -> String {
    s.chars().take(n).collect()
}

/// 快照写入时机判定:降级跃迁(running→degraded)或降级原因变化才写,同问题不重复写
fn snapshot_write_needed(is_running_before: bool, last_error: &str, msg: &str) -> bool {
    is_running_before || last_error != msg
}

fn status_tag(s: InstStatus) -> String {
    serde_json::to_string(&s)
        .unwrap_or_default()
        .trim_matches('"')
        .to_string()
}

/// 宿主端口是否可绑定(空闲探测:bind 成功即空闲,随即释放)
pub(crate) fn host_port_free(p: u16) -> bool {
    std::net::TcpListener::bind(("127.0.0.1", p)).is_ok()
}

// ─── 生命周期 DAG 构建(Step 化) ───

/// 内置编排模板目录(节点组;仅目录/展示与向导用,不改变执行语义)
pub fn nodegroups() -> Vec<serde_json::Value> {
    vec![
        serde_json::json!({
            "kind": "create", "label": "创建实例", "desc": "按架构类型创建 MySQL 实例(含网络/主从/复制/代理集群/校验)",
            "params": [
                {"name": "itype", "label": "架构类型", "values": ["single", "async", "sync", "xenon"]},
                {"name": "proxies", "label": "Proxy 集群实例数", "values": ["1", "2", "3", "4"]},
                {"name": "shard_num", "label": "分片数", "values": ["1", "2", "8", "128"]}
            ],
            "modules": [{"module": "net", "count": 1}, {"module": "master", "count": 1}, {"module": "slave", "count": 2},
                        {"module": "repl", "count": 1}, {"module": "proxy", "count": 2}, {"module": "verify", "count": 1}]
        }),
        serde_json::json!({
            "kind": "destroy", "label": "销毁实例", "desc": "删除实例全部容器与网络(不可恢复)",
            "params": [{"name": "instance", "label": "目标实例", "values": []}],
            "modules": [{"module": "proxy", "count": 1}, {"module": "slave", "count": 1}, {"module": "master", "count": 1},
                        {"module": "net", "count": 1}, {"module": "cleanup", "count": 1}]
        }),
        serde_json::json!({
            "kind": "scaleout", "label": "扩容从节点", "desc": "为运行中实例扩容在线读从/离线从",
            "params": [
                {"name": "role", "label": "节点角色", "values": ["read", "offline"]},
                {"name": "region", "label": "region(可选)", "values": []},
                {"name": "az", "label": "az(可选)", "values": []}
            ],
            "modules": [{"module": "slave", "count": 1}, {"module": "repl", "count": 1}, {"module": "verify", "count": 1}]
        }),
        serde_json::json!({
            "kind": "backup", "label": "主库逻辑备份", "desc": "对运行中实例的主节点执行 mysqldump 逻辑备份(容器内落盘 + 宿主登记产物说明)",
            "params": [{"name": "instance", "label": "目标实例", "values": []}],
            "modules": [{"module": "backup", "count": 1}]
        }),
    ]
}

/// 架构组合方案模板(前端"模块组合方案"参考;仅展示,不改变创建语义)
pub fn architectures() -> Vec<serde_json::Value> {
    vec![
        serde_json::json!({
            "key": "single", "label": "单节点", "desc": "仅单个数据库节点,无高可用,适用于开发测试等非生产环境",
            "when_use": "开发 / 测试 / 非核心", "modules": [
                {"module": "net", "count": 1}, {"module": "master", "count": 1}, {"module": "proxy", "count": 1}
            ]
        }),
        serde_json::json!({
            "key": "async", "label": "主从异步", "desc": "主-从异步复制,适用于大多数业务场景,具备高可用容灾能力",
            "when_use": "生产通用(默认)", "modules": [
                {"module": "net", "count": 1}, {"module": "master", "count": 1}, {"module": "slave", "count": 2},
                {"module": "repl", "count": 1}, {"module": "proxy", "count": 2}, {"module": "verify", "count": 1}
            ]
        }),
        serde_json::json!({
            "key": "sync", "label": "主从同步", "desc": "主从同步复制架构:主库写入等待从库确认(半同步),数据一致性更强",
            "when_use": "数据一致性要求高的核心业务", "modules": [
                {"module": "net", "count": 1}, {"module": "master", "count": 1}, {"module": "slave", "count": 1},
                {"module": "repl", "count": 1}, {"module": "proxy", "count": 2}, {"module": "verify", "count": 1}
            ]
        }),
        serde_json::json!({
            "key": "xenon", "label": "Xenon Raft 高可用", "desc": "基于 Xenon 的 MySQL raft 集群:后台复制仍为半同步,由 raft 共识协议自动选主/故障转移;可前置 rsproxy 代理(读分流档位可选)",
            "when_use": "需自动容灾的核心业务,主从同步一致性 + raft 自动选主(需 xenon-local 镜像)", "modules": [
                {"module": "net", "count": 1}, {"module": "master", "count": 1}, {"module": "slave", "count": 2},
                {"module": "verify", "count": 1}
            ]
        }),
        serde_json::json!({
            "key": "distributed", "label": "分片集群", "desc": "多分片主从复制,支撑大容量与高吞吐",
            "when_use": "大数据量 / 高 QPS", "modules": [
                {"module": "net", "count": 1}, {"module": "proxy", "count": 2}, {"module": "shard", "count": 8},
                {"module": "repl", "count": 1}, {"module": "verify", "count": 1}
            ]
        }),
    ]
}

fn mysql_args(
    network: &str,
    host_port: Option<u16>,
    server_id: u64,
    extra: &[&str],
) -> Vec<String> {
    let mut args = vec!["--network".to_string(), network.to_string()];
    if let Some(hp) = host_port {
        args.push("-p".to_string());
        args.push(format!("127.0.0.1:{hp}:3306"));
    }
    args.push("-e".to_string());
    args.push(format!("MYSQL_ROOT_PASSWORD={ROOT_PASS}"));
    args.push("-e".to_string());
    args.push("MYSQL_ALLOW_EMPTY_PASSWORD=yes".to_string());
    args.push(image_of("RDSCTL_MYSQL_IMAGE", MYSQL_IMAGE));
    args.push("--server-id".to_string());
    args.push(server_id.to_string());
    if server_id % 10 != 1 {
        args.push("--read-only=1".to_string());
    }
    args.push("--gtid-mode=ON".to_string());
    args.push("--enforce-gtid-consistency=ON".to_string());
    args.push("--default-authentication-plugin=mysql_native_password".to_string());
    for e in extra {
        args.push(e.to_string());
    }
    args
}

// ─── xenon(raft 高可用)节点(对齐 xenon deploy/gen-compose.sh 的 per-node 容器定义) ───

/// xenon 节点容器名:rds-{inst}-xenon{i}(i 从 1 起;同时作为容器网络内 raft endpoint 主机名)
fn xenon_node_c(instance: &str, i: usize) -> String {
    format!("rds-{instance}-xenon{i}")
}

/// rsproxy(newproxy)配置生成(xenon raft 前置代理;格式对齐 rsproxy conf/newproxy.conf):
///   [MySQL_Proxy_Layer] 服务端口;[Cluster_0]/[CTablet_0_t0] 单分片拓扑;
///   [Master_Host_g0]/[Slave_Host_g0] 初始成员(首启占位,HA worker 启动后按 raft 实况覆盖拓扑);
///   [DB_User_dbu]/[Product_User_pu] 管控统一 root/ROOT_PASS(与直连节点同凭据);
///   [XenonRaft_0_t0] leader 自动发现 + 读一致性档位(members=容器名:3306,
///   raft_endpoints=容器名:8801 对齐 mysql.xenon_raft_status.leader 列)。
/// 档位语义见 rsproxy docs/15-xenon-ha.md §4:strong 全走 leader / causal 高水位 GTID /
/// session 会话水位 / eventual 直读 follower;causal|session 依赖 gtid-mode=ON
/// (xenon 镜像 my.cnf 已默认开启)。
fn xenon_proxy_config(members: &[String], consistency: &str, _n_proxy: usize) -> String {
    let members_s = members
        .iter()
        .map(|m| format!("{m}:3306"))
        .collect::<Vec<_>>()
        .join(",");
    let raft_eps = members
        .iter()
        .map(|m| format!("{m}:{XENON_RPC_PORT}"))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        r#"[MySQL_Proxy_Layer]
port=4051
mng_port=9111
mng_user=admin
mng_password=admin
max_threads=4
log_dir=logs
log_level=warn
client_timeout=28800
server_timeout=28800
conn_pool_socket_max_serve_client_times=100000
max_sql_size=16777216
max_query_size=16777216
default_charset=33
stream_transport_enable=0
slow_query_ms=200

[Cluster_0]
name=rds

[CTablet_0_t0]
name=t0

[Master_Host_g0]
host={master0}
port=3306
max_conn_pool_size=16
max_connections=256
connect_timeout=5
weight=1
cluster_tablet_name=t0

[Slave_Host_g0]
host={slave0}
port=3306
max_conn_pool_size=16
max_connections=256
connect_timeout=5
weight=1
cluster_tablet_name=t0

[DB_User_dbu]
db_username=root
db_password={ROOT_PASS}
default_db={APP_DB}
cluster_name=rds

[Product_User_pu]
username=root
password={ROOT_PASS}
db_username=root
max_connections=256
cluster_name=rds

[XenonRaft_0_t0]
members={members}
raft_endpoints={raft_eps}
probe_interval=1000
probe_timeout_ms=500
leader_stale_ms=3000
read_consistency={consistency}
barrier_wait_ms=200
gtid_sample_cache_ms=30
"#,
        master0 = members.first().map(|s| s.as_str()).unwrap_or("127.0.0.1"),
        slave0 = members.get(1).map(|s| s.as_str()).unwrap_or("127.0.0.1"),
        members = members_s,
        raft_eps = raft_eps,
        consistency = consistency,
    )
}

/// xenon 节点 docker run 参数(语义对齐 gen-compose.sh 的 service 定义):
///   --hostname = 容器名(peers 用容器名互联,hostname 必须一致才能解析)
///   CLUSTER_PEERS / INIT_ROLE / MYSQL_ROOT_PASSWORD 等 entrypoint 注入变量;
///   数据卷 xenon-data/raft.meta 各自独占命名卷(清理时随容器一起删除,见 destroy_nodes);
///   MySQL 端口发布 127.0.0.1(管控/业务直连),RPC 8801 发布 127.0.0.1(诊断用)。
fn xenon_node_args(
    network: &str,
    node_c: &str,
    peers: &str,
    init_role: &str,
    mysql_hp: u16,
    rpc_hp: u16,
) -> Vec<String> {
    vec![
        "--network".into(),
        network.into(),
        "--hostname".into(),
        node_c.into(),
        "-p".into(),
        format!("127.0.0.1:{mysql_hp}:3306"),
        "-p".into(),
        format!("127.0.0.1:{rpc_hp}:{XENON_RPC_PORT}"),
        "-v".into(),
        format!("xenon-data-{node_c}:/var/lib/mysql"),
        "-v".into(),
        format!("xenon-meta-{node_c}:/data/raft.meta"),
        "-e".into(),
        format!("NODE_NAME={node_c}"),
        "-e".into(),
        format!("RPC_PORT={XENON_RPC_PORT}"),
        "-e".into(),
        "MYSQL_PORT=3306".into(),
        "-e".into(),
        // 统一管控口令语义:巡检/查询台/慢查均用 root/ROOT_PASS 直连容器内 mysqld
        format!("MYSQL_ROOT_PASSWORD={ROOT_PASS}"),
        "-e".into(),
        "REPL_USER=repl".into(),
        "-e".into(),
        format!("REPL_PASSWD={REPL_PASS}"),
        "-e".into(),
        format!("CLUSTER_PEERS={peers}"),
        "-e".into(),
        format!("INIT_ROLE={init_role}"),
        "-e".into(),
        "LOG_LEVEL=INFO".into(),
        image_of("RDSCTL_XENON_IMAGE", XENON_IMAGE),
    ]
}

/// xenon 集群就绪校验(Step::VerifyXenon 执行体;对齐 xenon deploy/xenon.sh wait_cluster_ready):
///   1. 轮询各节点 `xenoncli raft status` 直到出现 LEADER(与 xenon.sh 相同的判定字符串);
///   2. 在「登记主」(节点1,LEADER 种子)写 init 数据;
///   3. 轮询其余节点读回同值 → raft 复制链路真实可用(替代主从架构的 GTID 校验)。
/// 全部通过才算创建完成;超时/不一致报错(节点保留现场,失败可销毁重建)。
async fn verify_xenon(members: &[String]) -> Result<String, String> {
    if members.is_empty() {
        return Err("xenon 集群无节点".into());
    }
    // 1) 等 LEADER(最多 180s,与 xenon.sh wait_cluster_ready 一致)
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(180);
    let mut leader: Option<String> = None;
    let mut last: String = String::new();
    while std::time::Instant::now() < deadline {
        for c in members {
            // xenoncli 在容器 PATH(/usr/local/bin);raft status 输出 JSON 含 "state":"LEADER"
            if let Ok(out) =
                dk::exec_in(c, &["xenoncli".into(), "raft".into(), "status".into()]).await
            {
                if out.contains("\"state\":\"LEADER\"") {
                    leader = Some(c.clone());
                    break;
                }
                last = out.chars().take(200).collect();
            }
        }
        if leader.is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    }
    let leader = leader.ok_or_else(|| {
        format!("180s 内未见 raft LEADER(对齐 xenon.sh wait_cluster_ready 判定);节点回显: {last}")
    })?;
    // 2) 主上写 init 数据(复用统一 init kv 语义,便于复制校验与后续运维一致)
    dk::exec_mysql_local(
        &leader,
        "root",
        ROOT_PASS,
        &format!(
            "CREATE DATABASE IF NOT EXISTS {APP_DB}; CREATE TABLE IF NOT EXISTS {APP_DB}.kv (k VARCHAR(64) PRIMARY KEY, v VARCHAR(128)); INSERT INTO {APP_DB}.kv (k, v) VALUES ('init', 'ok') ON DUPLICATE KEY UPDATE v='ok';"
        ),
    )
    .await
    .map_err(|e| format!("leader {leader} 写 init 数据失败: {e}"))?;
    // 3) 全节点读回一致(raft 复制;重试窗口容忍选举/日志回放)
    let mut last_err = String::new();
    for _ in 0..30 {
        let mut ok = 0usize;
        let mut cur = String::new();
        for c in members {
            match dk::exec_mysql_local(
                c,
                "root",
                ROOT_PASS,
                &format!("SELECT v FROM {APP_DB}.kv WHERE k='init'"),
            )
            .await
            {
                Ok(v) if v.trim() == "ok" => ok += 1,
                Ok(v) => cur = format!("{c} 读回异常: {v}"),
                Err(e) => cur = format!("{c} 查询失败: {e}"),
            }
        }
        if ok == members.len() {
            return Ok(format!(
                "xenon 集群就绪:leader={leader},raft 复制在 {} 节点全部一致",
                members.len()
            ));
        }
        last_err = cur;
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
    Err(format!("raft 复制未在超时前追平: {last_err}"))
}

/// 创建实例节点
fn create_nodes2(
    name: &str,
    network: &str,
    master_c: &str,
    slaves: &[(String, String, u64)], // (container, read|offline, sid)
    proxies: &[(String, u16, u16)],   // (container, px端口, 管理端口)
    master_sid: u64,
    _lvs_c: &str,
    lvs_port: u16,
) -> Vec<TaskNode> {
    let n = name.to_string();
    let net = network.to_string();
    let mc = master_c.to_string();
    let conf_path = conf_abs_path(&n);
    let mut out: Vec<TaskNode> = Vec::new();
    // 1) 网络
    out.push(TaskNode {
        id: "net".into(),
        name: "创建专属网络".into(),
        deps: vec![],
        retries: 1,
        timeout_secs: Some(60),
        steps: vec![Step::NetworkCreate { name: net.clone() }],
    });
    // 2) 主库
    out.push(TaskNode {
        id: "master".into(),
        name: "启动 MySQL 主节点".into(),
        deps: vec!["net".into()],
        retries: 1,
        timeout_secs: Some(300),
        steps: vec![
            Step::DockerRun {
                container: mc.clone(),
                args: mysql_args(&net, None, master_sid, &["--log-bin=mysql-bin", "--binlog-format=ROW"]),
            },
            Step::WaitMysql {
                container: mc.clone(),
                user: "root".into(),
                pass: ROOT_PASS.into(),
                timeout_secs: 120,
            },
            Step::ExecSql {
                container: mc.clone(),
                user: "root".into(),
                pass: ROOT_PASS.into(),
                sql: format!(
                    "CREATE USER IF NOT EXISTS 'repl'@'%' IDENTIFIED WITH mysql_native_password BY '{REPL_PASS}'; GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl'@'%'; FLUSH PRIVILEGES;"
                ),
            },
        ],
    });
    // 3) 从节点(按架构类型可变;可为空=单实例)
    let mut slave_ids: Vec<String> = Vec::new();
    for (idx, (cc, kind, sid)) in slaves.iter().enumerate() {
        let id = format!("slave{}", idx + 1);
        slave_ids.push(id.clone());
        let title = if kind == "offline" {
            "启动从节点(统计/备份)".to_string()
        } else {
            "启动从节点(读流量)".to_string()
        };
        out.push(TaskNode {
            id: id.clone(),
            name: title,
            deps: vec!["master".into()],
            retries: 1,
            timeout_secs: Some(300),
            steps: slave_steps(cc, &net, &mc, *sid),
        });
    }
    // 4) 代理配置(等主库+从库就绪;无从时只等主库)
    let mut conf_deps = vec!["master".to_string()];
    conf_deps.extend(slave_ids.clone());
    let read_host = slaves
        .iter()
        .find(|(_, k, _)| k == "read")
        .map(|(c, _, _)| c.clone())
        .unwrap_or_else(|| mc.clone());
    out.push(TaskNode {
        id: "proxy_conf".into(),
        name: "生成 newproxy 配置".into(),
        deps: conf_deps,
        retries: 0,
        timeout_secs: Some(30),
        steps: vec![Step::WriteHostFile {
            path: format!("logs/rds/{n}/newproxy.conf"),
            content: proxy_config(&mc, &read_host),
        }],
    });
    // 5) 代理集群(1..N 各自挂载同一配置)
    let mut proxy_ids: Vec<String> = Vec::new();
    for (idx, (cc, pxp, mngp)) in proxies.iter().enumerate() {
        let id = if idx == 0 {
            "proxy".to_string()
        } else {
            format!("proxy{}", idx + 1)
        };
        proxy_ids.push(id.clone());
        let cname = cc.clone();
        out.push(TaskNode {
            id: id.clone(),
            name: format!("启动 newproxy 代理 #{}", idx + 1),
            deps: vec!["proxy_conf".into()],
            retries: 1,
            timeout_secs: Some(180),
            steps: vec![Step::DockerRun {
                container: cname,
                args: vec![
                    "--network".into(),
                    net.clone(),
                    "-p".into(),
                    format!("127.0.0.1:{pxp}:4051"),
                    "-p".into(),
                    format!("127.0.0.1:{mngp}:9111"),
                    "-v".into(),
                    format!("{}:/app/conf/newproxy.conf:rw", conf_path),
                    image_of("RDSCTL_PROXY_IMAGE", PROXY_IMAGE),
                ],
            }],
        });
    }
    // 6) LVS 接入层(进程内转发器:VIP → 全部 Proxy 宿主端口;不依赖镜像/外网)
    let last = proxy_ids
        .last()
        .cloned()
        .unwrap_or_else(|| "proxy".to_string());
    out.push(TaskNode {
        id: "lvs".into(),
        name: "启动 LVS 接入(VIP 转发 Proxy 集群)".into(),
        deps: vec![last.clone()],
        retries: 2,
        timeout_secs: Some(60),
        steps: vec![Step::EnsureLvs {
            instance: n.clone(),
        }],
    });
    // 7) 验证(无复制从节点则仅收尾状态);连通性经 LVS 接入端口验证
    let verify_steps = if slaves.is_empty() {
        vec![Step::UpdateInstanceStatus {
            instance: n.clone(),
            status: "running".into(),
        }]
    } else {
        vec![
            Step::VerifyReplication {
                master: mc.clone(),
                slaves: slaves.iter().map(|(c, _, _)| c.clone()).collect(),
                proxy_port: lvs_port,
            },
            Step::UpdateInstanceStatus {
                instance: n.clone(),
                status: "running".into(),
            },
        ]
    };
    out.push(TaskNode {
        id: "verify".into(),
        name: "连通性验证".into(),
        deps: vec!["lvs".into()],
        retries: 2,
        timeout_secs: Some(300),
        steps: verify_steps,
    });
    out
}

// ─── 多分片(N>1)规划与 DAG(AI-0 多分片 v0:每分片独立主从组) ───

/// proxy 元数据列表(供 RdsInstance.proxies)
fn proxy_node_list(proxy_list: &[(String, u16, u16)]) -> Vec<ProxyNode> {
    proxy_list
        .iter()
        .map(|(c, mp, gp)| ProxyNode {
            container: c.clone(),
            mysql_port: *mp,
            mng_port: *gp,
            spec: String::new(),
            ip: String::new(),
            qps: 0,
            conns: 0,
            cpu: 0.0,
            status: String::new(),
            version: String::new(),
        })
        .collect()
}

/// 多分片单个分片的规划(主 + 读从;async 另加离线从)
struct MultiGroup {
    shard_id: String,          // s1..sN
    master_c: String,          // rds-{name}-s{k}-master
    read_c: String,            // rds-{name}-s{k}-slave-1
    offline_c: Option<String>, // rds-{name}-s{k}-slave-2(async 多分片)
    master_sid: u64,
}

/// 多分片创建 DAG:每分片 master → read(复制)→ 分片级校验;代理指向分片1(入口)。
/// v0 边界:分片间数据路由/每分片独立入口属引擎模板与代理层,创建侧只保证 N 组实体齐全。
fn create_nodes_multi(
    name: &str,
    network: &str,
    groups: &[MultiGroup],
    proxies: &[(String, u16, u16)],
    _lvs_c: &str,
    lvs_port: u16,
) -> Vec<TaskNode> {
    let n = name.to_string();
    let net = network.to_string();
    let conf_path = conf_abs_path(&n);
    let mut out: Vec<TaskNode> = Vec::new();
    // 1) 网络
    out.push(TaskNode {
        id: "net".into(),
        name: "创建专属网络".into(),
        deps: vec![],
        retries: 1,
        timeout_secs: Some(60),
        steps: vec![Step::NetworkCreate { name: net.clone() }],
    });
    // 2) 每分片:master → 读从(async 另 + 离线从,均指向组内 master 复制)
    for g in groups {
        let master_id = format!("{}_master", g.shard_id);
        let read_id = format!("{}_read", g.shard_id);
        let mc = g.master_c.clone();
        out.push(TaskNode {
            id: master_id.clone(),
            name: format!("启动主节点(分片 {} )", g.shard_id),
            deps: vec!["net".into()],
            retries: 1,
            timeout_secs: Some(300),
            steps: vec![
                Step::DockerRun {
                    container: mc.clone(),
                    args: mysql_args(&net, None, g.master_sid, &["--log-bin=mysql-bin", "--binlog-format=ROW"]),
                },
                Step::WaitMysql {
                    container: mc.clone(),
                    user: "root".into(),
                    pass: ROOT_PASS.into(),
                    timeout_secs: 120,
                },
                Step::ExecSql {
                    container: mc.clone(),
                    user: "root".into(),
                    pass: ROOT_PASS.into(),
                    sql: format!(
                        "CREATE USER IF NOT EXISTS 'repl'@'%' IDENTIFIED WITH mysql_native_password BY '{REPL_PASS}'; GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl'@'%'; FLUSH PRIVILEGES;"
                    ),
                },
            ],
        });
        out.push(TaskNode {
            id: read_id.clone(),
            name: format!("启动从节点(分片 {} · 读从)", g.shard_id),
            deps: vec![master_id.clone()],
            retries: 1,
            timeout_secs: Some(300),
            steps: slave_steps(&g.read_c, &net, &mc, g.master_sid + 1),
        });
        if let Some(oc) = &g.offline_c {
            let offline_id = format!("{}_offline", g.shard_id);
            out.push(TaskNode {
                id: offline_id,
                name: format!("启动从节点(分片 {} · 离线从)", g.shard_id),
                deps: vec![master_id],
                retries: 1,
                timeout_secs: Some(300),
                steps: slave_steps(oc, &net, &mc, g.master_sid + 2),
            });
        }
    }
    // 3) 代理配置(入口=分片1 主从;代理集群共用一份)
    let first = &groups[0];
    let first_master = format!("{}_master", first.shard_id);
    let first_read = format!("{}_read", first.shard_id);
    out.push(TaskNode {
        id: "proxy_conf".into(),
        name: "生成 newproxy 配置(分片1 入口)".into(),
        deps: vec![first_master.clone(), first_read.clone()],
        retries: 0,
        timeout_secs: Some(30),
        steps: vec![Step::WriteHostFile {
            path: format!("logs/rds/{n}/newproxy.conf"),
            content: proxy_config(&first.master_c, &first.read_c),
        }],
    });
    // 4) 代理集群(1..N)
    let mut proxy_ids: Vec<String> = Vec::new();
    for (idx, (cc, pxp, mngp)) in proxies.iter().enumerate() {
        let id = if idx == 0 {
            "proxy".to_string()
        } else {
            format!("proxy{}", idx + 1)
        };
        proxy_ids.push(id.clone());
        let cname = cc.clone();
        out.push(TaskNode {
            id: id.clone(),
            name: format!("启动 newproxy 代理 #{}", idx + 1),
            deps: vec!["proxy_conf".into()],
            retries: 1,
            timeout_secs: Some(180),
            steps: vec![Step::DockerRun {
                container: cname,
                args: vec![
                    "--network".into(),
                    net.clone(),
                    "-p".into(),
                    format!("127.0.0.1:{pxp}:4051"),
                    "-p".into(),
                    format!("127.0.0.1:{mngp}:9111"),
                    "-v".into(),
                    format!("{}:/app/conf/newproxy.conf:rw", conf_path),
                    image_of("RDSCTL_PROXY_IMAGE", PROXY_IMAGE),
                ],
            }],
        });
    }
    // 5) LVS 接入层(进程内转发器:VIP → 全部 Proxy 宿主端口;不依赖镜像/外网)
    let last_proxy = proxy_ids
        .last()
        .cloned()
        .unwrap_or_else(|| "proxy".to_string());
    out.push(TaskNode {
        id: "lvs".into(),
        name: "启动 LVS 接入(VIP 转发 Proxy 集群)".into(),
        deps: vec![last_proxy.clone()],
        retries: 2,
        timeout_secs: Some(60),
        steps: vec![Step::EnsureLvs {
            instance: n.clone(),
        }],
    });
    // 6) 每分片校验(主写 init → 本分片全部从追平 + LVS/VIP 连通)
    let mut verify_ids: Vec<String> = Vec::new();
    for g in groups {
        let vid = format!("{}_verify", g.shard_id);
        verify_ids.push(vid.clone());
        let mut slaves: Vec<String> = vec![g.read_c.clone()];
        let mut deps: Vec<String> = vec![format!("{}_read", g.shard_id)];
        if let Some(oc) = &g.offline_c {
            slaves.push(oc.clone());
            deps.push(format!("{}_offline", g.shard_id));
        }
        deps.push("lvs".to_string());
        out.push(TaskNode {
            id: vid,
            name: format!("分片 {} 复制/连通校验", g.shard_id),
            deps,
            retries: 2,
            timeout_secs: Some(300),
            steps: vec![Step::VerifyReplication {
                master: g.master_c.clone(),
                slaves,
                proxy_port: lvs_port,
            }],
        });
    }
    // 6) 全部就绪 → running
    out.push(TaskNode {
        id: "done".into(),
        name: "全部就绪".into(),
        deps: verify_ids,
        retries: 0,
        timeout_secs: Some(30),
        steps: vec![Step::UpdateInstanceStatus {
            instance: n.clone(),
            status: "running".into(),
        }],
    });
    out
}

fn slave_steps(slave_c: &str, net: &str, master_c: &str, sid: u64) -> Vec<Step> {
    vec![
        Step::DockerRun {
            container: slave_c.to_string(),
            args: mysql_args(net, None, sid, &[]),
        },
        Step::WaitMysql {
            container: slave_c.to_string(),
            user: "root".into(),
            pass: ROOT_PASS.into(),
            timeout_secs: 120,
        },
        Step::ExecSql {
            container: slave_c.to_string(),
            user: "root".into(),
            pass: ROOT_PASS.into(),
            sql: format!(
                "CHANGE REPLICATION SOURCE TO SOURCE_HOST='{master_c}', SOURCE_PORT=3306, SOURCE_USER='repl', SOURCE_PASSWORD='{REPL_PASS}', SOURCE_AUTO_POSITION=1; START REPLICA;"
            ),
        },
    ]
}

fn destroy_nodes(name: &str, inst: &RdsInstance) -> Vec<TaskNode> {
    let n = name.to_string();
    let xenon = inst.itype == "xenon";
    let proxy_containers: Vec<String> = if inst.proxies.is_empty() {
        vec![inst.proxy_container.clone()]
    } else {
        inst.proxies.iter().map(|p| p.container.clone()).collect()
    };
    let node_containers: Vec<String> = inst.nodes.iter().map(|x| x.container.clone()).collect();
    let mc = inst
        .master()
        .map(|m| m.container.clone())
        .unwrap_or_default();
    let net = inst.network.clone();

    // xenon(raft)架构:无 proxy/LVS 接入层,xenoncli raft stop 优雅停集群后直接删节点容器
    if xenon {
        let mut out = Vec::new();
        // 前置 rsproxy 接入层:先停 LVS 转发与代理容器(业务流量先摘除)
        let has_proxy = !proxy_containers.iter().all(|c| c.is_empty());
        if has_proxy {
            out.push(TaskNode {
                id: "lvs_stop".into(),
                name: "停止接入(LVS)".into(),
                deps: vec![],
                retries: 1,
                timeout_secs: Some(30),
                steps: vec![Step::StopLvs { instance: n.clone() }],
            });
            out.push(TaskNode {
                id: "proxy_stop".into(),
                name: "停止 rsproxy 代理".into(),
                deps: vec!["lvs_stop".into()],
                retries: 1,
                timeout_secs: Some(150),
                steps: proxy_containers
                    .iter()
                    .filter(|c| !c.is_empty())
                    .map(|c| Step::DockerRm { container: c.clone() })
                    .collect(),
            });
        }
        if !mc.is_empty() {
            // 集群优雅下线:xenoncli raft stop(忽略失败——节点可能已宕,rm -f 兜底)
            out.push(TaskNode {
                id: "raft_stop".into(),
                name: "停止 xenon raft 集群".into(),
                deps: if has_proxy { vec!["proxy_stop".into()] } else { vec![] },
                retries: 0,
                timeout_secs: Some(60),
                steps: vec![Step::DockerExec {
                    container: mc.clone(),
                    args: vec!["xenoncli".into(), "raft".into(), "stop".into()],
                }],
            });
        }
        let head_dep = if !mc.is_empty() {
            "raft_stop"
        } else if has_proxy {
            "proxy_stop"
        } else {
            ""
        };
        out.push(TaskNode {
            id: "nodes_stop".into(),
            name: "停止 xenon 节点".into(),
            deps: if head_dep.is_empty() {
                vec![]
            } else {
                vec![head_dep.to_string()]
            },
            retries: 1,
            timeout_secs: Some(180),
            steps: node_containers
                .iter()
                .map(|c| Step::DockerRm {
                    container: c.clone(),
                })
                .collect(),
        });
        out.push(TaskNode {
            id: "net_rm".into(),
            name: "移除网络".into(),
            deps: vec!["nodes_stop".into()],
            retries: 1,
            timeout_secs: Some(60),
            steps: vec![Step::NetworkRm { name: net }],
        });
        out.push(TaskNode {
            id: "cleanup".into(),
            name: "清理记录".into(),
            deps: vec!["net_rm".into()],
            retries: 0,
            timeout_secs: Some(30),
            steps: vec![
                Step::UpdateInstanceStatus {
                    instance: n.clone(),
                    status: "destroyed".into(),
                },
                Step::Audit {
                    instance: n,
                    action: "destroy".into(),
                    params: "done".into(),
                },
            ],
        });
        return out;
    }

    vec![
        TaskNode {
            id: "lvs_stop".into(),
            name: "停止接入(LVS)".into(),
            deps: vec![],
            retries: 1,
            timeout_secs: Some(30),
            steps: vec![Step::StopLvs {
                instance: n.clone(),
            }],
        },
        TaskNode {
            id: "proxy_stop".into(),
            name: "停止代理".into(),
            deps: vec!["lvs_stop".into()],
            retries: 1,
            timeout_secs: Some(150),
            steps: proxy_containers
                .iter()
                .map(|c| Step::DockerRm {
                    container: c.clone(),
                })
                .collect(),
        },
        TaskNode {
            id: "slaves_stop".into(),
            name: "停止从节点".into(),
            deps: vec!["proxy_stop".into()],
            retries: 1,
            timeout_secs: Some(180),
            steps: node_containers
                .iter()
                .map(|c| Step::DockerRm {
                    container: c.clone(),
                })
                .collect(),
        },
        TaskNode {
            id: "master_stop".into(),
            name: "停止主节点".into(),
            deps: vec!["slaves_stop".into()],
            retries: 1,
            timeout_secs: Some(120),
            steps: vec![Step::DockerRm {
                container: mc.clone(),
            }],
        },
        TaskNode {
            id: "net_rm".into(),
            name: "移除网络".into(),
            deps: vec!["master_stop".into()],
            retries: 1,
            timeout_secs: Some(60),
            steps: vec![Step::NetworkRm { name: net.clone() }],
        },
        TaskNode {
            id: "cleanup".into(),
            name: "清理记录".into(),
            deps: vec!["net_rm".into()],
            retries: 0,
            timeout_secs: Some(30),
            steps: vec![
                Step::UpdateInstanceStatus {
                    instance: n.clone(),
                    status: "destroyed".into(),
                },
                Step::Audit {
                    instance: n.clone(),
                    action: "destroy".into(),
                    params: "done".into(),
                },
            ],
        },
    ]
}

fn scaleout_nodes(
    name: &str,
    master_host: &str,
    network: &str,
    sc: &str,
    sid: u64,
    hp: u16,
    role: Role,
    region: &str,
    az: &str,
    shard: &str,
    parent: &str,
) -> Vec<TaskNode> {
    let n = name.to_string();
    let mc = master_host.to_string();
    let net = network.to_string();
    let sc_name = sc.to_string();
    let shard_s = shard.to_string();
    let parent_s = parent.to_string();
    let role_name = match role {
        Role::Read => "read".to_string(),
        Role::Offline | Role::Stats | Role::Backup => "offline".to_string(),
        Role::Master => "read".to_string(),
    };
    let head = if shard_s.is_empty() {
        format!("启动新从节点 {}", sc.rsplit('-').next().unwrap_or("?"))
    } else {
        format!("启动从节点(分片 {shard_s} · {role_name})")
    };

    vec![
        TaskNode {
            id: "new_slave".into(),
            name: head.into(),
            deps: vec![],
            retries: 1,
            timeout_secs: Some(300),
            steps: vec![
                Step::DockerRun {
                    container: sc_name.clone(),
                    args: mysql_args(&net, Some(hp), sid, &[]),
                },
                Step::WaitMysql {
                    container: sc_name.clone(),
                    user: "root".into(),
                    pass: ROOT_PASS.into(),
                    timeout_secs: 120,
                },
            ],
        },
        TaskNode {
            id: "repl".into(),
            name: "配置并启动复制".into(),
            deps: vec!["new_slave".into()],
            retries: 2,
            timeout_secs: Some(120),
            steps: vec![Step::ExecSql {
                container: sc_name.clone(),
                user: "root".into(),
                pass: ROOT_PASS.into(),
                sql: format!(
                    "CHANGE REPLICATION SOURCE TO SOURCE_HOST='{mc}', SOURCE_PORT=3306, SOURCE_USER='repl', SOURCE_PASSWORD='{REPL_PASS}', SOURCE_AUTO_POSITION=1; START REPLICA;"
                ),
            }],
        },
        TaskNode {
            id: "verify".into(),
            name: "复制状态与数据校验".into(),
            deps: vec!["repl".into()],
            retries: 2,
            timeout_secs: Some(300),
            steps: vec![
                Step::VerifyScaleout {
                    master: mc.clone(),
                    new_slave: sc_name.clone(),
                },
                Step::UpdateInstanceTopology {
                    instance: n.clone(),
                    container: sc_name.clone(),
                    role: role_name,
                    host_port: hp,
                    server_id: sid,
                    region: region.to_string(),
                    az: az.to_string(),
                    shard: shard_s,
                    parent: parent_s,
                },
                Step::UpdateInstanceStatus {
                    instance: n.clone(),
                    status: "running".into(),
                },
            ],
        },
    ]
}

// ─── 备份模块(样例功能模块;模板文档: docs/dag-module-howto.md) ───
//
// 与 create/destroy/scaleout 同一套「组合子 → TaskNode → scheduler.submit」模式:
//   - 节点 = 若干原子 Step 的顺序序列(本模块引入新原子 Step::DockerExec);
//   - 步骤全部幂等(重跑覆盖旧产物、不产生重复副作用),Deserialize 可跨重启恢复;
//   - 备份不动实例状态机(提交侧由 run_backup 加锁/审计;失败侧只告警不改状态)。

fn backup_nodes(name: &str, master_c: &str) -> Vec<TaskNode> {
    let n = name.to_string();
    let mc = master_c.to_string();
    let dump = format!("/tmp/rds-{n}-backup.sql");
    let cmd = format!(
        "set -e; mysqldump -u root -p'{ROOT_PASS}' --single-transaction --quick --databases {APP_DB} > {dump} 2> /tmp/rds-{n}-dump.err; echo 'backup-ok bytes='$(wc -c < {dump})"
    );
    vec![TaskNode {
        id: "backup".into(),
        name: "执行逻辑备份(mysqldump)".into(),
        deps: vec![],
        retries: 2,
        timeout_secs: Some(600),
        steps: vec![
            Step::DockerExec {
                container: mc.clone(),
                args: vec!["sh".into(), "-c".into(), cmd],
            },
            Step::WriteHostFile {
                path: format!("logs/rds/{n}/backup/README.txt"),
                content: format!(
                    "rdsctl 逻辑备份\n实例: {n}\n备份主节点: {mc}\n容器内产物: {dump}\n下载: docker cp {mc}:{dump} .\n"
                ),
            },
        ],
    }]
}

// ─── 步骤执行器 ───

pub fn make_executor() -> StepExecutor {
    Arc::new(|ctx: &StepCtx, step: &Step| {
        let step = step.clone();
        let ctx = ctx.clone();
        Box::pin(async move { exec_step(&ctx, step).await })
    })
}

/// 解析本次步骤应使用的执行面 agent。
///
/// - 绑定宿主机且 agent 已接入 → 该 agent;
/// - 未绑定(本机):**cluster 模式下也走 agent**(A4/§9.1),否则回退本机 docker(single 模式行为不变);
/// - `Unmanaged`(绑定了宿主机但 agent 未接入)→ None(调用方必须拒绝执行,不得误在本机执行)。
fn exec_agent(route: &NodeRoute) -> Option<crate::agent::Agent> {
    match route {
        NodeRoute::Agent { ag, .. } => Some(ag.clone()),
        NodeRoute::Local => crate::ha::runtime::global().and_then(|_| {
            std::env::var("RDSCTL_AGENT_URL")
                .ok()
                .filter(|s| !s.trim().is_empty())
                .map(|u| crate::agent::Agent::new(u, crate::agent::token_opt()))
        }),
        NodeRoute::Unmanaged { .. } => None,
    }
}

/// DB 节点当前可用的控制能力(token 由前端映射为按钮;必须与后端真实支持的接口一致)
fn db_node_actions(i: &RdsInstance, n: &InstNode) -> Vec<&'static str> {
    let mut a: Vec<&'static str> = Vec::new();
    if i.itype == "xenon" {
        // raft 自治选主:唯一的人工干预通道是 trytoleader(受管切换对 xenon 一律拒绝)
        if n.role != Role::Master {
            a.push("xenon_trytoleader");
        }
    } else if n.role != Role::Master {
        // 非 xenon:可对该从节点做受管计划切换(PRS)
        a.push("reparent_to_this");
    }
    a.push("detail");
    a
}

/// 步骤账本状态(设计 §8.4:有效一次的落地方式)
enum Ledger {
    /// 单机模式:无账本(行为与今天逐字一致)
    Disabled,
    /// 已在共识状态机登记;执行成功后必须写 StepDone
    Armed {
        rt: Arc<crate::ha::runtime::ClusterRuntime>,
        ctx: StepCtx,
        idem_key: String,
    },
    /// 命中历史:该步骤已完成,直接短路(不再执行副作用)
    AlreadyDone(String),
}

/// 参数摘要(用于幂等键:内容相同 = 同一步骤;内容不同 = 不同操作,不得互相短路)
fn args_digest(parts: &[&str]) -> String {
    let joined = parts.join("\u{1f}");
    crate::sha256::to_hex(&crate::sha256::digest(joined.as_bytes()))[..16].to_string()
}

/// 登记步骤开始。**无法登记时拒绝执行副作用**(fail-closed:宁可不做,不可无账本重放)
async fn ledger_begin(ctx: &StepCtx, idem_key: &str) -> Result<Ledger, String> {
    let Some(rt) = crate::ha::runtime::global() else {
        return Ok(Ledger::Disabled);
    };
    let op = crate::ha::state::Op::StepBegin {
        task_id: ctx.task_id.clone(),
        node: ctx.node_id.clone(),
        step: ctx.step_idx as u32,
        idem_key: idem_key.to_string(),
    };
    match rt.propose_op(op).await {
        Ok((_, applied)) => match applied["status"].as_str() {
            Some("step_already_done") => {
                let prev = applied["result"].as_str().unwrap_or("").to_string();
                tracing::info!(
                    "步骤账本命中({}/{}/{}):短路执行",
                    ctx.task_id,
                    ctx.node_id,
                    ctx.step_idx
                );
                Ok(Ledger::AlreadyDone(prev))
            }
            Some("applied") => Ok(Ledger::Armed {
                rt,
                ctx: ctx.clone(),
                idem_key: idem_key.to_string(),
            }),
            other => Err(format!(
                "步骤登记被拒({other:?}):{}",
                applied["reason"].as_str().unwrap_or("")
            )),
        },
        Err(e) => Err(format!(
            "无法登记步骤账本,拒绝执行副作用(fail-closed):{e}"
        )),
    }
}

/// 步骤成功后写入结果(结果用于重放短路时回放)
async fn ledger_done(ledger: Ledger, result: &str) -> Result<(), String> {
    match ledger {
        Ledger::Disabled | Ledger::AlreadyDone(_) => Ok(()),
        Ledger::Armed {
            rt,
            ctx,
            idem_key,
        } => {
            let op = crate::ha::state::Op::StepDone {
                task_id: ctx.task_id.clone(),
                node: ctx.node_id.clone(),
                step: ctx.step_idx as u32,
                idem_key,
                result: serde_json::Value::String(result.to_string()),
            };
            rt.propose_op(op)
                .await
                .map(|_| ())
                .map_err(|e| format!("步骤执行成功但账本写入失败(重启后可能重放):{e}"))
        }
    }
}

async fn exec_step(ctx: &StepCtx, step: Step) -> Result<String, String> {
    match step {
        Step::DockerRun { container, args } => {
            let mut key_parts: Vec<&str> = vec!["docker_run", &container];
            key_parts.extend(args.iter().map(|s| s.as_str()));
            let ledger = ledger_begin(ctx, &args_digest(&key_parts)).await?;
            if let Ledger::AlreadyDone(prev) = ledger {
                return Ok(format!("[账本命中] 容器 {container} 已按同一规格创建过:{prev}"));
            }
            let args_ref: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
            // 单机模式:today 的行为逐字保留(不触碰全局 manager、不引入新分支)
            if crate::ha::runtime::global().is_none() {
                dk::run(&container, &args_ref).await?;
                let out = format!("容器 {container} 已启动");
                ledger_done(ledger, &out).await?;
                return Ok(out);
            }
            let mgr = crate::manager();
            let route = mgr
                .instances
                .get(&ctx.instance)
                .map(|i| i.value().clone())
                .map(|i| resolve_route_of(&i, &container, &mgr.store.host_list()))
                .unwrap_or(NodeRoute::Local);
            if matches!(route, NodeRoute::Unmanaged { .. }) {
                return Err(format!(
                    "节点 {container} 绑定宿主机但 agent 未接入,拒绝执行(避免误在本机创建)"
                ));
            }
            match exec_agent(&route) {
                Some(ag) => {
                    let meta = mgr.call_meta(&ctx.instance);
                    ag.run_fenced(&container, &args_ref, &meta).await?;
                }
                None => dk::run(&container, &args_ref).await?,
            }
            let out = format!("容器 {container} 已启动");
            ledger_done(ledger, &out).await?;
            Ok(out)
        }
        Step::DockerRm { container } => {
            let ledger = ledger_begin(ctx, &format!("docker_rm:{}", args_digest(&[&container]))).await?;
            if let Ledger::AlreadyDone(prev) = ledger {
                return Ok(format!("[账本命中] 容器 {container} 已移除:{prev}"));
            }
            if crate::ha::runtime::global().is_none() {
                if dk::exists(&container).await {
                    if container.contains("-xenon") {
                        dk::rm_v(&container).await?;
                    } else {
                        dk::rm(&container).await?;
                    }
                }
                let out = format!("容器 {container} 已移除");
                ledger_done(ledger, &out).await?;
                return Ok(out);
            }
            let mgr = crate::manager();
            let route = mgr
                .instances
                .get(&ctx.instance)
                .map(|i| i.value().clone())
                .map(|i| resolve_route_of(&i, &container, &mgr.store.host_list()))
                .unwrap_or(NodeRoute::Local);
            let meta = mgr.call_meta(&ctx.instance);
            match exec_agent(&route) {
                Some(ag) => {
                    // 远端:存在才删(与本地语义一致)
                    if ag.exists(&container).await {
                        ag.rm_fenced(&container, &meta).await?;
                    }
                }
                None if matches!(route, NodeRoute::Unmanaged { .. }) => {
                    tracing::warn!("节点 {container} 绑定宿主机但 agent 未接入,跳过删除(不误删本机同名容器)");
                }
                None => {
                    if dk::exists(&container).await {
                        // xenon 节点容器:连命名卷(xenon-data/raft.meta)一并删除
                        if container.contains("-xenon") {
                            dk::rm_v(&container).await?;
                        } else {
                            dk::rm(&container).await?;
                        }
                    }
                }
            }
            let out = format!("容器 {container} 已移除");
            ledger_done(ledger, &out).await?;
            Ok(out)
        }
        Step::NetworkCreate { name } => {
            dk::network_create(&name).await?;
            Ok(format!("网络 {name} 就绪"))
        }
        Step::NetworkRm { name } => {
            dk::network_rm(&name).await?;
            Ok(format!("网络 {name} 已移除"))
        }
        Step::ExecSql {
            container,
            user,
            pass,
            sql,
        } => {
            // 复制变更/切主是最高风险的副作用:必须走账本(重放短路)+ fence(拒绝过期持有者)
            let key = args_digest(&["exec_sql", &container, &user, &sql]);
            let ledger = ledger_begin(ctx, &key).await?;
            if let Ledger::AlreadyDone(prev) = ledger {
                return Ok(format!("[账本命中] {container}: 同一 SQL 已执行过:{prev}"));
            }
            if crate::ha::runtime::global().is_none() {
                let out = dk::exec_mysql_local(&container, &user, &pass, &sql).await?;
                let out = if out.trim().is_empty() {
                    format!("{container}: SQL 执行成功")
                } else {
                    format!("{container}: {out}")
                };
                ledger_done(ledger, &out).await?;
                return Ok(out);
            }
            let mgr = crate::manager();
            let route = mgr
                .instances
                .get(&ctx.instance)
                .map(|i| i.value().clone())
                .map(|i| resolve_route_of(&i, &container, &mgr.store.host_list()))
                .unwrap_or(NodeRoute::Local);
            let out = match exec_agent(&route) {
                Some(ag) => {
                    let meta = mgr.call_meta(&ctx.instance);
                    ag.exec_mysql_fenced(&container, &user, &pass, &sql, &meta)
                        .await?
                }
                None if matches!(route, NodeRoute::Unmanaged { .. }) => {
                    return Err(format!(
                        "{container} 绑定宿主机但 agent 未接入,拒绝执行 SQL(避免误在本机执行)"
                    ));
                }
                None => dk::exec_mysql_local(&container, &user, &pass, &sql).await?,
            };
            let out = if out.trim().is_empty() {
                format!("{container}: SQL 执行成功")
            } else {
                format!("{container}: {out}")
            };
            ledger_done(ledger, &out).await?;
            Ok(out)
        }
        Step::DockerExec { container, args } => {
            let out = dk::exec_in(&container, &args).await?;
            Ok(if out.trim().is_empty() {
                format!("{container}: docker exec 完成")
            } else {
                format!("{container}: {out}")
            })
        }
        Step::HostMysql {
            port,
            user,
            pass,
            sql,
        } => {
            let out = dk::host_mysql(port, &user, &pass, &sql).await?;
            Ok(out)
        }
        Step::WaitHealthy {
            container,
            timeout_secs,
        } => {
            dk::wait_healthy(&container, timeout_secs).await?;
            Ok(format!("容器 {container} 健康"))
        }
        Step::WaitMysql {
            container,
            user,
            pass,
            timeout_secs,
        } => {
            dk::wait_mysql_ready(&container, &user, &pass, timeout_secs).await?;
            Ok(format!("{container} MySQL 就绪"))
        }
        Step::WriteHostFile { path, content } => {
            if let Some(dir) = std::path::Path::new(&path).parent() {
                std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
            }
            std::fs::write(&path, &content).map_err(|e| format!("写入 {path} 失败: {e}"))?;
            Ok(format!("配置已写入 {path}"))
        }
        Step::VerifyXenon { members } => verify_xenon(&members).await,
        Step::UpdateInstanceStatus { instance, status } => {
            let st = match status.as_str() {
                "running" => InstStatus::Running,
                "destroyed" => InstStatus::Destroyed,
                "failed" => InstStatus::Failed,
                "degraded" => InstStatus::Degraded,
                _ => InstStatus::Failed,
            };
            let mgr = crate::manager();
            if let Some(mut i) = mgr.instances.get_mut(&instance) {
                i.status = st;
            }
            mgr.persist();
            Ok(format!("实例 {instance} 状态 → {status}"))
        }
        Step::Audit {
            instance,
            action,
            params,
        } => {
            crate::manager().store.audit(
                &crate::auth::current_user(),
                &instance,
                &action,
                &params,
                "ok",
                "",
            );
            Ok(format!("audit:{action} {instance}"))
        }
        Step::Noop { note } => Ok(note.clone()),
        Step::DtsRun {
            instance,
            node,
            dts_container,
            target_label,
            spec,
        } => {
            // DTS = 空容器占位:轻量镜像常驻(不运行 DTS 引擎),记录 + 容器都在;失败不阻断主流程
            let m = crate::manager();
            let s = if spec.is_empty() { "2C4G" } else { &spec };
            m.store.dts_upsert(
                &instance,
                &node,
                "canal",
                &dts_container,
                &target_label,
                "creating",
                "",
                s,
            );
            let mut last_err = String::new();
            let mut started = false;
            for img in dts_placeholder_images() {
                let args: Vec<String> = vec![
                    "--restart".into(),
                    "unless-stopped".into(),
                    "--entrypoint".into(),
                    "/bin/sh".into(),
                    img.clone(),
                    "-c".into(),
                    "sleep 315360000".into(), // 常驻空容器,占位不跑 DTS 引擎(十年后结束;届时由移除流程删除)
                ];
                let args_ref: Vec<&str> = args.iter().map(|x| x.as_str()).collect();
                match dk::run(&dts_container, &args_ref).await {
                    Ok(()) => {
                        started = true;
                        break;
                    }
                    Err(e) => last_err = format!("镜像 {img}:{e}"),
                }
            }
            if started {
                m.store.dts_upsert(
                    &instance,
                    &node,
                    "canal",
                    &dts_container,
                    &target_label,
                    "running",
                    "",
                    s,
                );
                Ok(format!(
                    "DTS 空容器 {dts_container} 已创建(running,← {node})"
                ))
            } else {
                let hint = "创建空容器失败:请确认 docker 可用,或设 RDSCTL_DTS_IMAGE 指向本机已就绪的轻量镜像后重试";
                let msg = format!("{last_err};{hint}");
                m.store.dts_upsert(
                    &instance,
                    &node,
                    "canal",
                    &dts_container,
                    &target_label,
                    "failed",
                    &msg,
                    s,
                );
                m.store.audit(
                    "system",
                    &instance,
                    "dts_create",
                    &format!("node={node}"),
                    "failed",
                    "",
                );
                m.store.alert_open(
                    &instance,
                    "task_failed",
                    "info",
                    &format!("DTS 空容器创建失败:{node}(可重试)"),
                );
                Ok(format!("DTS 空容器创建失败(已登记可重试):{msg}"))
            }
        }
        Step::VerifyReplication {
            master,
            slaves,
            proxy_port,
        } => verify_replication(&master, &slaves, proxy_port).await,
        Step::VerifyScaleout { master, new_slave } => verify_scaleout(&master, &new_slave).await,
        Step::EnsureLvs { instance } => {
            let i = crate::manager()
                .instances
                .get(&instance)
                .map(|e| e.value().clone())
                .ok_or_else(|| format!("实例 {instance} 不存在,无法启动接入层"))?;
            let port = i.lvs_mysql_port;
            crate::lvs::ensure(&i)?;
            Ok(format!("接入层就绪: 127.0.0.1:{port} → Proxy 集群"))
        }
        Step::StopLvs { instance } => {
            crate::lvs::stop(&instance);
            Ok(format!("实例 {instance} 接入层已停止"))
        }
        Step::UpdateInstanceTopology {
            instance,
            container,
            role,
            host_port,
            server_id,
            region,
            az,
            shard,
            parent,
        } => {
            let role_obj = match role.as_str() {
                "read" => Role::Read,
                "offline" | "stats" | "backup" => Role::Offline,
                _ => Role::Read,
            };
            let mgr = crate::manager();
            if let Some(mut i) = mgr.instances.get_mut(&instance) {
                let master_c = i.master().map(|m| m.container.clone()).unwrap_or_default();
                // 分片级扩容:节点归入目标分片并挂到分片 master;单分片沿用实例默认
                let shard_c = if shard.is_empty() {
                    i.shard.clone()
                } else {
                    shard.clone()
                };
                let parent_c = if parent.is_empty() {
                    master_c
                } else {
                    parent.clone()
                };
                let node_region = if region.is_empty() {
                    i.region.clone()
                } else {
                    region.clone()
                };
                let node_az = if az.is_empty() {
                    i.az.clone()
                } else {
                    az.clone()
                };
                i.nodes.push(InstNode {
                    container: container.clone(),
                    role: role_obj,
                    host: container.clone(),
                    port: 3306,
                    host_port,
                    server_id,
                    region: node_region,
                    az: node_az,
                    shard: shard_c.clone(),
                    parent: parent_c,
                    rpc_host_port: 0,
                });
                // 分片元数据同步:目标分片行追加该从节点(保证拓扑/监控能展示全部从)
                if !i.shards.is_empty() && !shard_c.is_empty() {
                    let rn = role.as_str().to_string();
                    let role_k = if rn == "read" { "read" } else { "offline" };
                    if let Some(row) = i.shards.iter_mut().find(|x| x.id == shard_c) {
                        if !row.slaves.iter().any(|s| s.name == container) {
                            row.slaves.push(ShardSlaveRef {
                                name: container.clone(),
                                role: role_k.to_string(),
                            });
                        }
                        if row.slave.is_empty() {
                            row.slave = container.clone();
                        }
                    }
                }
            }
            mgr.persist();
            Ok(format!("实例 {instance} 拓扑已更新(+{container})"))
        }
        Step::ReplaceNodeCore {
            instance,
            node,
            host,
            host_port,
            server_id,
        } => exec_replace_node_core(&instance, &node, &host, host_port, server_id).await,
        Step::ReplaceNodeCommit {
            instance,
            node,
            host,
            host_port,
            server_id,
        } => {
            let m = crate::manager();
            commit_replace_node(m.as_ref(), &instance, &node, &host, host_port, server_id)
        }
        Step::MigrateInstance {
            instance,
            region,
            az,
            hosts,
        } => exec_migrate_instance_core(&instance, &region, &az, &hosts).await,
    }
}

/// 创建收尾验证:主写 → GTID 追平 → 从数据一致 → 代理连通
async fn verify_replication(
    master: &str,
    slaves: &[String],
    proxy_port: u16,
) -> Result<String, String> {
    dk::exec_mysql_local(
        master,
        "root",
        ROOT_PASS,
        &format!(
            "CREATE DATABASE IF NOT EXISTS {APP_DB}; CREATE TABLE IF NOT EXISTS {APP_DB}.kv (k VARCHAR(64) PRIMARY KEY, v VARCHAR(128)); INSERT INTO {APP_DB}.kv (k, v) VALUES ('init', 'ok') ON DUPLICATE KEY UPDATE v='ok';"
        ),
    )
    .await?;
    let gtid = gtid_of(master).await?;
    for s in slaves {
        if !gtid.is_empty() {
            wait_gtid(s, &gtid).await?;
        }
        let v = dk::exec_mysql_local(
            s,
            "root",
            ROOT_PASS,
            &format!("SELECT v FROM {APP_DB}.kv WHERE k='init'"),
        )
        .await
        .map_err(|e| format!("{s} 校验失败: {e}"))?;
        if v.trim() != "ok" {
            return Err(format!("{s} 复制数据不一致: {v}"));
        }
    }
    // 代理连通(重试预热)
    let mut last = String::new();
    for _ in 0..8 {
        match dk::host_mysql(proxy_port, "root", ROOT_PASS, "SELECT 1").await {
            Ok(o) if o.trim() == "1" => {
                return Ok(format!(
                    "验证通过:主+{} 从复制正常,接入层(LVS) 127.0.0.1:{proxy_port} 真实可查",
                    slaves.len()
                ))
            }
            Ok(o) => last = format!("代理返回异常: {o}"),
            Err(e) => last = e,
        }
        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
    }
    Err(format!("代理查询失败: {last}"))
}

async fn verify_scaleout(master: &str, new_slave: &str) -> Result<String, String> {
    let gtid = gtid_of(master).await?;
    if !gtid.is_empty() {
        wait_gtid(new_slave, &gtid).await?;
    }
    let v = dk::exec_mysql_local(
        new_slave,
        "root",
        ROOT_PASS,
        &format!("SELECT v FROM {APP_DB}.kv WHERE k='init'"),
    )
    .await
    .map_err(|e| format!("数据校验失败: {e}"))?;
    if v.trim() != "ok" {
        return Err(format!("新从数据不一致: {v}"));
    }
    Ok(format!("扩容验证通过:新从 {new_slave} 复制正常"))
}

async fn gtid_of(container: &str) -> Result<String, String> {
    let out = dk::exec_mysql_local(
        container,
        "root",
        ROOT_PASS,
        "SELECT @@GLOBAL.gtid_executed",
    )
    .await
    .map_err(|e| format!("获取主库 GTID 失败: {e}"))?;
    Ok(out.lines().last().unwrap_or("").trim().to_string())
}

async fn wait_gtid(slave: &str, gtid: &str) -> Result<(), String> {
    dk::exec_mysql_local(
        slave,
        "root",
        ROOT_PASS,
        &format!("SELECT WAIT_FOR_EXECUTED_GTID_SET('{gtid}', 15)"),
    )
    .await
    .map_err(|e| format!("{slave} 等待复制追平失败: {e}"))?;
    Ok(())
}

// ─── 节点替换(replace_node,P2-②;见 docs/physical-multi-site-ops.md §3/§7) ───
//
// 方案:「临时名容器先就绪,后切换」——近乎零停机、天然回滚:
//   1) 目标宿主机用临时名起新从 → 挂复制链(SOURCE=当前主,GTID 自动追平)→ 校验;
//   2) 追平后:摘旧节点容器 → docker rename 临时名→正式节点名 → 提交绑定/端口/自增 id。
//   失败点全部落在第 1 阶段:自动清理临时容器,旧节点保持可用(可重试/换目标)。
// 幂等:临时容器已存在则跳过重建直接追平;rename 前检查正式名是否已就位。

/// 从节点追上主库:IO/SQL 线程 ON + WAIT_FOR_EXECUTED_GTID_SET 命中(超时报错)
async fn wait_slave_caught(
    sr: &NodeRoute,
    sname: &str,
    mr: &NodeRoute,
    mc: &str,
    timeout_secs: u64,
) -> Result<(), String> {
    const CHECK: &str =
        "SELECT IF(EXISTS(SELECT 1 FROM performance_schema.replication_connection_status \
         WHERE CHANNEL_NAME='' AND SERVICE_STATE='ON') \
         AND EXISTS(SELECT 1 FROM performance_schema.replication_applier_status \
         WHERE CHANNEL_NAME='' AND SERVICE_STATE='ON'),'OK','STOPPED')";
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
    let mut last = String::from("尚未开始");
    while std::time::Instant::now() < deadline {
        // 1) 复制线程
        match r_sql(sr, sname, "root", ROOT_PASS, CHECK).await {
            Ok(o) if o.trim() == "OK" => {}
            Ok(_) => {
                last = format!("{sname} 复制线程未运行(IO/SQL)");
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                continue;
            }
            Err(e) => {
                last = format!("{sname} 复制状态查询失败: {e}");
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                continue;
            }
        }
        // 2) 主库 GTID 全集 → 新从是否已追平(GTID_SUBSET=1 即已应用全集;
        //    不用 WAIT_FOR_EXECUTED_GTID_SET:实测 8.0.46 上即使已追上该函数也恒返回 0)
        let gtid = match r_sql(mr, mc, "root", ROOT_PASS, "SELECT @@GLOBAL.gtid_executed").await {
            Ok(o) => o.lines().last().unwrap_or("").trim().to_string(),
            Err(e) => {
                last = format!("获取主库 {mc} GTID 失败: {e}");
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                continue;
            }
        };
        if gtid.is_empty() {
            return Ok(()); // 空实例(无任何事务)
        }
        match r_sql(
            sr,
            sname,
            "root",
            ROOT_PASS,
            &format!("SELECT GTID_SUBSET('{gtid}', @@GLOBAL.gtid_executed)"),
        )
        .await
        {
            Ok(o) if o.trim() == "1" => return Ok(()),
            Ok(_) => last = format!("{sname} 仍在追平中(主 {mc} gtid={})", clip(&gtid, 40)),
            Err(e) => last = format!("{sname} 追平判定查询失败: {e}"),
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
    Err(format!(
        "{sname} 未在 {timeout_secs}s 内追平主库 GTID: {last}"
    ))
}

/// Step::ReplaceNodeCore 执行体(见 dag.rs 变体注释)
async fn exec_replace_node_core(
    instance: &str,
    node: &str,
    host: &str,
    host_port: u16,
    server_id: u64,
) -> Result<String, String> {
    let mgr = crate::manager();
    let inst = mgr
        .instances
        .get(instance)
        .map(|e| e.value().clone())
        .ok_or_else(|| format!("实例 {instance} 不存在"))?;
    let tgt = target_route(host)?;
    let hosts = mgr.store.host_list();
    let old_route = resolve_route_of(&inst, node, &hosts);
    if let NodeRoute::Agent { host: h, .. } | NodeRoute::Unmanaged { host: h } = &old_route {
        if h.as_str() == host.trim() {
            return Err(format!("节点 {node} 已在宿主机 {host},无需替换"));
        }
    }
    let master = inst
        .master()
        .map(|m| m.container.clone())
        .ok_or_else(|| format!("实例 {instance} 缺少主节点,无法替换"))?;
    if master == node {
        return Err("主节点不可直接替换:请先用受管 PRS 把主角色切到健康从,再替换该节点(见 docs/meta-authority.md)".to_string());
    }
    let net = inst.network.clone();
    let temp = format!("{node}-rnx");
    let mut logs: Vec<String> = Vec::new();

    // 1) 目标宿主机起临时从(幂等:已存在则跳过重建,直接追平)
    if !r_exists(&tgt, &temp).await {
        let args = mysql_args(&net, Some(host_port), server_id, &[]);
        let args_ref: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        r_run(&tgt, &temp, &args_ref)
            .await
            .map_err(|e| format!("目标宿主机启动临时容器 {temp} 失败: {e}"))?;
        logs.push(format!("临时容器 {temp} 已启动(目标 {host})"));
        wait_mysql_route(&tgt, &temp, "root", ROOT_PASS, 120).await?;
        r_sql(
            &tgt,
            &temp,
            "root",
            ROOT_PASS,
            &format!(
                "CHANGE REPLICATION SOURCE TO SOURCE_HOST='{master}', SOURCE_PORT=3306, SOURCE_USER='repl', SOURCE_PASSWORD='{REPL_PASS}', SOURCE_AUTO_POSITION=1; START REPLICA;"
            ),
        )
        .await
        .map_err(|e| format!("临时从 {temp} 配置复制失败: {e}"))?;
    } else {
        logs.push(format!("临时容器 {temp} 已存在,跳过重建(续跑)"));
        wait_mysql_route(&tgt, &temp, "root", ROOT_PASS, 120).await?;
    }
    // 2) 追平校验(失败 → 清理临时容器后报错,旧节点不受影响)
    let mr = resolve_route_of(&inst, &master, &hosts);
    if let Err(e) = wait_slave_caught(&tgt, &temp, &mr, &master, 300).await {
        let _ = r_rm(&tgt, &temp).await;
        return Err(e);
    }
    logs.push(format!("临时从 {temp} 已追平主库 {master}"));
    // 3) 切换:先摘旧容器(释放容器名;同机/跨机皆安全),
    //    再检查目标侧是否已有正式名(上次续跑已落位)→ 有则清理临时即可,无则 rename 上线。
    //    注意:共享 docker daemon 时「同名=旧容器」,必须先摘旧再判定,否则会误入续跑分支。
    let _ = r_rm(&old_route, node).await;
    if r_exists(&tgt, node).await {
        // 跨机续跑:目标侧已存在正式容器(上次 rename 后中断),清理临时容器即可
        let _ = r_rm(&tgt, &temp).await;
        logs.push(format!("节点 {node} 已存在于宿主机 {host}(续跑落位)"));
    } else {
        r_rename(&tgt, &temp, node).await.map_err(|e| {
            format!("临时容器换名 {temp} → {node} 失败(旧节点已下线,请重跑本任务恢复): {e}")
        })?;
        wait_mysql_route(&tgt, node, "root", ROOT_PASS, 120).await?;
        logs.push(format!("节点 {node} 已在宿主机 {host} 换名上线"));
    }
    Ok(logs.join("; "))
}

/// Step::ReplaceNodeCommit 执行体:提交节点绑定/端口/自增 id(替换成功后由调度步骤调用)
fn commit_replace_node(
    mgr: &RdsManager,
    instance: &str,
    node: &str,
    host: &str,
    host_port: u16,
    server_id: u64,
) -> Result<String, String> {
    let Some(mut i) = mgr.instances.get_mut(instance) else {
        return Err(format!("实例 {instance} 不存在"));
    };
    let h = host.trim();
    if h.is_empty() {
        i.node_hosts.remove(node);
    } else {
        i.node_hosts.insert(node.to_string(), h.to_string());
    }
    if let Some(n) = i.nodes.iter_mut().find(|n| n.container == node) {
        n.host_port = host_port;
        n.server_id = server_id;
        n.host = node.to_string(); // 身份=容器名(不变)
    }
    drop(i);
    mgr.persist();
    mgr.store.audit(
        &crate::auth::current_user(),
        instance,
        "replace_node",
        &format!("commit node={node} host={host} host_port={host_port} server_id={server_id}"),
        "ok",
        "",
    );
    Ok(format!(
        "节点 {node} 替换提交完成(host={}, host_port={host_port})",
        if h.is_empty() {
            "本机".into()
        } else {
            h.to_string()
        }
    ))
}

// ─── 实例整体迁移(migrate_instance,P2-③;见 docs/physical-multi-site-ops.md §4/§7-③) ───
//
// 状态机:先逐从节点迁(复用 replace:目标机临时名追平 → 摘旧 → 换名,身份不变),
// 再迁主节点(目标机新主追平 → 老主置只读 → 摘老主 → 换名上线 → 新主可写);
// 全部成功后提交 region/az 事实与绑定。失败中断于当前节点,已迁节点保持可用、
// 主仍为权威源 → 可续跑(幂等:已在目标机的节点跳过)。

/// 迁移一个从节点(同身份替换;目标机已就位则跳过续跑)
async fn migrate_move_slave(
    mgr: &RdsManager,
    instance: &str,
    node: &str,
    host: &str,
) -> Result<String, String> {
    let inst = mgr
        .instances
        .get(instance)
        .map(|e| e.value().clone())
        .ok_or_else(|| format!("实例 {instance} 不存在"))?;
    // 已在目标机 → 跳过(续跑幂等)
    if node_host_binding(&inst, node)
        .map(|s| s == host)
        .unwrap_or(false)
    {
        return Ok(format!("节点 {node} 已在宿主机 {host}(跳过)"));
    }
    let max_sid = inst.nodes.iter().map(|n| n.server_id).max().unwrap_or(0);
    let sid = max_sid + 1;
    let hp = mgr.alloc_host_port();
    exec_replace_node_core(instance, node, host, hp, sid).await?;
    commit_replace_node(mgr, instance, node, host, hp, sid)?;
    Ok(format!("节点 {node} → {host} 迁移完成(端口 {hp})"))
}

/// 迁移主节点:目标机临时新主追平 → 老主只读定格 → 摘老主 → 换名上线 → 新主可写 → 提交
async fn migrate_move_master(
    mgr: &RdsManager,
    instance: &str,
    host: &str,
) -> Result<String, String> {
    let inst = mgr
        .instances
        .get(instance)
        .map(|e| e.value().clone())
        .ok_or_else(|| format!("实例 {instance} 不存在"))?;
    let master_c = inst
        .master()
        .map(|m| m.container.clone())
        .ok_or_else(|| format!("实例 {instance} 缺少主节点"))?;
    if node_host_binding(&inst, &master_c)
        .map(|s| s == host)
        .unwrap_or(false)
    {
        return Ok(format!("主节点 {master_c} 已在宿主机 {host}(跳过)"));
    }
    let hosts_all = mgr.store.host_list();
    let old_mr = resolve_route_of(&inst, &master_c, &hosts_all);
    let tgt = target_route(host)?;
    let max_sid = inst.nodes.iter().map(|n| n.server_id).max().unwrap_or(0);
    let sid = max_sid + 1;
    let hp = mgr.alloc_host_port();
    let net = inst.network.clone();
    let temp = format!("{master_c}-mgn");
    let mut logs: Vec<String> = Vec::new();
    // 1) 目标机临时新主(幂等:已存在则跳过重建)
    if !r_exists(&tgt, &temp).await {
        let args = mysql_args(&net, Some(hp), sid, &[]);
        let args_ref: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        r_run(&tgt, &temp, &args_ref)
            .await
            .map_err(|e| format!("目标机启动临时新主 {temp} 失败: {e}"))?;
        wait_mysql_route(&tgt, &temp, "root", ROOT_PASS, 120).await?;
        r_sql(
            &tgt,
            &temp,
            "root",
            ROOT_PASS,
            &format!(
                "CHANGE REPLICATION SOURCE TO SOURCE_HOST='{master_c}', SOURCE_PORT=3306, SOURCE_USER='repl', SOURCE_PASSWORD='{REPL_PASS}', SOURCE_AUTO_POSITION=1; START REPLICA;"
            ),
        )
        .await
        .map_err(|e| format!("临时新主 {temp} 挂复制链失败: {e}"))?;
    } else {
        wait_mysql_route(&tgt, &temp, "root", ROOT_PASS, 120).await?;
    }
    logs.push(format!("临时新主 {temp} 已启动并挂链(目标 {host})"));
    // 2) 追平(失败清临时,老主不受影响)
    if let Err(e) = wait_slave_caught(&tgt, &temp, &old_mr, &master_c, 300).await {
        let _ = r_rm(&tgt, &temp).await;
        return Err(e);
    }
    // 3) 切主:老主只读定格 → 新从最终追平 → 停新从复制 → 摘老主 → 换名 → 新主可写
    r_sql(
        &old_mr,
        &master_c,
        "root",
        ROOT_PASS,
        "SET GLOBAL read_only=ON",
    )
    .await
    .map_err(|e| format!("老主 {master_c} 置只读失败: {e}"))?;
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;
    wait_slave_caught(&tgt, &temp, &old_mr, &master_c, 60).await?;
    let _ = r_sql(&tgt, &temp, "root", ROOT_PASS, "STOP REPLICA").await;
    let _ = r_rm(&old_mr, &master_c).await; // 摘老主(数据面以临时新主为准;同机先释放容器名)
    if r_exists(&tgt, &master_c).await {
        let _ = r_rm(&tgt, &temp).await; // 续跑:目标机已存在正式主(上次换名后中断)
    } else {
        r_rename(&tgt, &temp, &master_c).await.map_err(|e| {
            format!("新主换名 {temp} → {master_c} 失败(老主已下线,请重跑迁移恢复): {e}")
        })?;
        wait_mysql_route(&tgt, &master_c, "root", ROOT_PASS, 120).await?;
    }
    r_sql(
        &tgt,
        &master_c,
        "root",
        ROOT_PASS,
        "SET GLOBAL read_only=OFF; SET GLOBAL super_read_only=OFF",
    )
    .await
    .map_err(|e| format!("新主 {master_c} 解除只读失败: {e}"))?;
    commit_replace_node(mgr, instance, &master_c, host, hp, sid)?;
    // 其余从节点复制源重指向新主(容器名不变:同 docker 网络别名已切换;独立网络时此即重指向点)
    let fresh = mgr
        .instances
        .get(instance)
        .map(|e| e.value().clone())
        .ok_or_else(|| format!("实例 {instance} 不存在"))?;
    let hosts_all = mgr.store.host_list();
    let mut repointed = 0usize;
    for n in fresh.nodes.iter().filter(|n| n.role != Role::Master) {
        let route = resolve_route_of(&fresh, &n.container, &hosts_all);
        let ok = r_sql(
            &route,
            &n.container,
            "root",
            ROOT_PASS,
            &format!(
                "STOP REPLICA; CHANGE REPLICATION SOURCE TO SOURCE_HOST='{master_c}', SOURCE_PORT=3306, SOURCE_USER='repl', SOURCE_PASSWORD='{REPL_PASS}', SOURCE_AUTO_POSITION=1; START REPLICA;"
            ),
        )
        .await;
        if ok.is_ok() {
            repointed += 1;
        }
    }
    logs.push(format!(
        "主节点 {master_c} 已切到宿主机 {host}(新主可写;重指向从 {repointed} 个)"
    ));
    Ok(logs.join("; "))
}

/// Step::MigrateInstance 执行体:从节点逐个迁(轮转目标机)→ 主节点最后切 → 提交 region/az 事实
async fn exec_migrate_instance_core(
    instance: &str,
    region: &str,
    az: &str,
    hosts_csv: &str,
) -> Result<String, String> {
    let mgr = crate::manager();
    let inst = mgr
        .instances
        .get(instance)
        .map(|e| e.value().clone())
        .ok_or_else(|| format!("实例 {instance} 不存在"))?;
    let hosts: Vec<&str> = hosts_csv
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();
    if hosts.is_empty() {
        return Err("缺少目标宿主机列表(hosts=a,b)".to_string());
    }
    let rows = mgr.store.host_list();
    for h in &hosts {
        match rows.iter().find(|r| r["name"].as_str() == Some(h)) {
            Some(hr)
                if hr["agent_port"].as_u64().unwrap_or(0) > 0
                    && hr["status"].as_str() != Some("retiring") => {}
            Some(hr) if hr["status"].as_str() == Some("retiring") => {
                return Err(format!("宿主机 {h} 处于 retiring,不可作为迁移目标"));
            }
            _ => return Err(format!("宿主机 {h} 未登记或未接入 agent")),
        }
    }
    let mut logs: Vec<String> = Vec::new();
    // 主节点放最后切(失败时主仍是权威源,整体可回退/续跑)
    if inst.master().is_none() {
        return Err("实例缺少主节点,无法迁移".to_string());
    }
    let slaves: Vec<InstNode> = inst
        .nodes
        .iter()
        .filter(|n| n.role != Role::Master)
        .cloned()
        .collect();
    let mut i = 0usize;
    for n in &slaves {
        let host = hosts[(i + 1) % hosts.len()];
        i += 1;
        logs.push(migrate_move_slave(&mgr, instance, &n.container, host).await?);
    }
    logs.push(migrate_move_master(&mgr, instance, hosts[0]).await?);
    // 提交 region/az 事实(实例级 + 节点级;绑定/端口已在各 commit 写入)
    if let Some(mut e) = mgr.instances.get_mut(instance) {
        e.region = region.to_string();
        e.az = az.to_string();
        for n in e.nodes.iter_mut() {
            n.region = region.to_string();
            n.az = az.to_string();
        }
    }
    mgr.persist();
    mgr.store.audit(
        &crate::auth::current_user(),
        instance,
        "migrate_instance",
        &format!("region={region} az={az} hosts={hosts_csv}"),
        "ok",
        "",
    );
    logs.push(format!(
        "实例 {instance} 迁移完成:region={region} az={az} 宿主机={hosts_csv}"
    ));
    Ok(logs.join("; "))
}

// ─── 端口/路径辅助(从持久化实例记录读取,避免与调度步骤耦合) ───

fn conf_abs_path(name: &str) -> String {
    format!(
        "{}/logs/rds/{name}/newproxy.conf",
        std::env::current_dir().unwrap_or_default().display()
    )
}

// ─── 任务终态联动(释放操作锁 + 失败置状态) ───

fn watch_task(instance: &String, task_id: String) {
    let mgr = crate::manager();
    let inst = instance.clone();
    tokio::spawn(async move {
        loop {
            if let Some(v) = mgr.scheduler.get(&task_id) {
                let st = v["status"].as_str().unwrap_or("");
                if st == "success" || st == "failed" {
                    mgr.unlock_instance(&inst);
                    if st == "failed" {
                        let msg = format!("任务 {task_id} 失败(实例状态机已置为失败)");
                        if let Some(mut i) = mgr.instances.get_mut(&inst) {
                            if !matches!(i.status, InstStatus::Destroyed) {
                                i.status = InstStatus::Failed;
                                i.last_error = msg.clone();
                            }
                        }
                        mgr.persist();
                        // 生命周期任务失败 → 告警(info)
                        mgr.store.alert_open(&inst, "task_failed", "info", &msg);
                    } else {
                        // 成功终态:若实例已销毁则关闭全部未决告警
                        if let Some(i) = mgr.instances.get(&inst) {
                            if i.status == InstStatus::Destroyed {
                                mgr.store.alert_resolve_instance(&inst);
                            }
                        }
                        // 备份注册联动:create/scaleout/destroy 终态(内部默认关闭)
                        let kind = v["kind"].as_str().unwrap_or("");
                        crate::backuplink::on_task_success(kind, &inst);
                    }
                    return;
                }
            }
            // 任务执行期间续约实例 lease(默认 30s,此处 100ms 轮询按需续约)
            mgr.renew_instance_lease(&inst);
            // 失去租约(共识模式下续约失败)= 可能有他人接管:必须立即止步,
            // 不再继续执行副作用(残留步骤由步骤账本保证幂等,可由新持有者续跑)
            if let Some(reason) = mgr.lease_lost_reason(&inst) {
                tracing::error!("实例 {inst} 已失去租约({reason}),中止本任务并把状态置为失败");
                mgr.store.audit(
                    "controller",
                    &inst,
                    "lease_lost",
                    &reason,
                    "failed",
                    &task_id,
                );
                if let Some(mut i) = mgr.instances.get_mut(&inst) {
                    if !matches!(i.status, InstStatus::Destroyed) {
                        i.status = InstStatus::Failed;
                        i.last_error = format!("失去共识租约:{reason}");
                    }
                }
                mgr.persist();
                mgr.unlock_instance(&inst);
                return;
            }
            // 100ms 轮询:任务终态到实例状态/running 的窗口尽量短,
            // 避免"实例已 running 但操作锁未释放"的可见拒绝期
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    });
}

/// 非生命周期任务(如 backup)的终态联动:只负责释放实例锁 + 失败告警留痕,
/// 不修改实例状态(备份失败不应把运行中的实例标记为 failed)。
fn watch_backup_task(mgr: &Arc<RdsManager>, instance: &str, task_id: String) {
    let inst = instance.to_string();
    let mgr = mgr.clone();
    tokio::spawn(async move {
        loop {
            if let Some(v) = mgr.scheduler.get(&task_id) {
                let st = v["status"].as_str().unwrap_or("");
                if st == "success" || st == "failed" {
                    mgr.unlock_instance(&inst);
                    if st == "failed" {
                        let msg = format!("备份任务 {task_id} 失败(实例状态不变,可重试)");
                        mgr.store.alert_open(&inst, "task_failed", "info", &msg);
                    }
                    // 备份产物通知(内部默认关闭,no-op)
                    crate::backuplink::on_backup_task_end(&inst, &task_id, st == "success");
                    return;
                }
            }
            mgr.renew_instance_lease(&inst);
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    });
}

// ─── DTS(canal)工具与任务终态联动 ───

fn dts_container(instance: &str, node: &str) -> String {
    let clean: String = node
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect();
    format!("rds-{instance}-dts-{clean}")
}

/// DTS 空容器镜像候选(按序尝试,命中即用):
///  env RDSCTL_DTS_IMAGE > 本机已就绪的 mysql/proxy 镜像(运行环境必然已本地化,免拉取)
///  > busybox 官方 / 国内加速镜像(极小,需外网时兜底)。
fn dts_placeholder_images() -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    if let Ok(ov) = std::env::var("RDSCTL_DTS_IMAGE") {
        let t = ov.trim().to_string();
        if !t.is_empty() {
            out.push(t);
        }
    }
    out.push(image_of("RDSCTL_MYSQL_IMAGE", MYSQL_IMAGE));
    out.push(image_of("RDSCTL_PROXY_IMAGE", PROXY_IMAGE));
    out.push("busybox:1.36".into());
    out.push("docker.m.daocloud.io/library/busybox:1.36".into());
    let mut seen = std::collections::HashSet::new();
    out.into_iter().filter(|x| seen.insert(x.clone())).collect()
}

/// DTS 任务生命周期:持锁续租,终态解锁并审计。
///  - create:记录状态由 Step::DtsRun 写入(creating→running/failed),此处不覆盖
///  - remove:成功 → 删注册;失败 → 记录标记 failed(可重试)
fn watch_dts_lifecycle(
    mgr: &Arc<RdsManager>,
    instance: &str,
    node: &str,
    task_id: String,
    action: &str,
) {
    let inst = instance.to_string();
    let node = node.to_string();
    let act = action.to_string();
    let mgr = mgr.clone();
    tokio::spawn(async move {
        loop {
            if let Some(v) = mgr.scheduler.get(&task_id) {
                let st = v["status"].as_str().unwrap_or("");
                if st == "success" || st == "failed" {
                    let ok = st == "success";
                    if act == "remove" {
                        if ok {
                            mgr.store.dts_remove(&inst, &node);
                        } else {
                            let rec = mgr
                                .store
                                .dts_list(Some(&inst))
                                .into_iter()
                                .find(|d| d["node"].as_str() == Some(node.as_str()));
                            let err = format!("DTS 移除任务 {task_id} 失败(可重试)");
                            mgr.store.dts_upsert(
                                &inst,
                                &node,
                                rec.as_ref()
                                    .and_then(|r| r["engine"].as_str())
                                    .unwrap_or("canal"),
                                rec.as_ref()
                                    .and_then(|r| r["container"].as_str())
                                    .unwrap_or(""),
                                rec.as_ref()
                                    .and_then(|r| r["target_label"].as_str())
                                    .unwrap_or(""),
                                "failed",
                                &err,
                                rec.as_ref().and_then(|r| r["spec"].as_str()).unwrap_or(""),
                            );
                        }
                    }
                    if !ok {
                        mgr.store.alert_open(
                            &inst,
                            "task_failed",
                            "info",
                            &format!(
                                "DTS {}失败:{node}(可重试)",
                                if act == "create" { "创建" } else { "移除" }
                            ),
                        );
                    }
                    mgr.store.audit(
                        "system",
                        &inst,
                        if act == "create" {
                            "dts_create"
                        } else {
                            "dts_remove"
                        },
                        &format!("node={node}"),
                        if ok { "ok" } else { "failed" },
                        &task_id,
                    );
                    mgr.unlock_instance(&inst);
                    return;
                }
            }
            mgr.renew_instance_lease(&inst);
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    });
}

// ─── 工具 ───

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

// ═══ vtorc 式事实层(P1,见 docs/meta-authority.md):复制/半同步事实采集与缓存 ═══

/// 事实缓存:实例名 → (ts, facts);巡检周期写入,API 读不到过期值时可即时刷新
static ORCH_FACTS_CACHE: std::sync::Mutex<
    Option<std::collections::HashMap<String, (u64, Vec<serde_json::Value>)>>,
> = std::sync::Mutex::new(None);

/// 受管切换 in-flight/最近结果(进程内;历史持久以审计为准)
static ORCH_OPS: std::sync::Mutex<Option<std::collections::HashMap<String, serde_json::Value>>> =
    std::sync::Mutex::new(None);

fn orch_op_get(name: &str) -> Option<serde_json::Value> {
    ORCH_OPS
        .lock()
        .unwrap()
        .as_ref()
        .and_then(|m| m.get(name).cloned())
}
fn orch_op_set(name: &str, v: serde_json::Value) {
    let mut g = ORCH_OPS.lock().unwrap();
    let m = g.get_or_insert_with(std::collections::HashMap::new);
    m.insert(name.to_string(), v);
}

/// 受管切换动作历史(进程内,每次操作一条;审计为持久留痕)
static ORCH_HIST: std::sync::Mutex<
    Option<std::collections::HashMap<String, Vec<serde_json::Value>>>,
> = std::sync::Mutex::new(None);

fn orch_hist_push(name: &str, v: serde_json::Value) {
    let mut g = ORCH_HIST.lock().unwrap();
    let m = g.get_or_insert_with(std::collections::HashMap::new);
    let list = m.entry(name.to_string()).or_default();
    list.push(v);
    if list.len() > 20 {
        list.remove(0);
    }
}
fn orch_hist_get(name: &str) -> Vec<serde_json::Value> {
    ORCH_HIST
        .lock()
        .unwrap()
        .as_ref()
        .and_then(|m| m.get(name).cloned())
        .unwrap_or_default()
}

/// 自动故障转移限频(60s 内同实例只尝试一次)
static AUTO_TRY: std::sync::Mutex<Option<std::collections::HashMap<String, u64>>> =
    std::sync::Mutex::new(None);

fn orch_cache_get(name: &str) -> Option<(u64, Vec<serde_json::Value>)> {
    ORCH_FACTS_CACHE
        .lock()
        .unwrap()
        .as_ref()
        .and_then(|m| m.get(name).cloned())
}

fn orch_cache_set(name: &str, facts: Vec<serde_json::Value>) {
    let mut g = ORCH_FACTS_CACHE.lock().unwrap();
    let m = g.get_or_insert_with(std::collections::HashMap::new);
    m.insert(name.to_string(), (now(), facts));
}

/// 对实例每个节点采集复制/半同步事实(复用容器内 root 探测通道;demo/非运行返回空)
async fn collect_replica_facts(
    inst: &RdsInstance,
    hosts: &[serde_json::Value],
) -> Vec<serde_json::Value> {
    if inst.network.starts_with("demo-") {
        return Vec::new(); // 演示数据无真实容器
    }
    // xenon(raft):无 MySQL 复制语义,orchestrator 事实层不适用(raft 视角由巡检 sweep_xenon 采集)
    if inst.itype == "xenon" {
        return Vec::new();
    }
    if !matches!(inst.status, InstStatus::Running | InstStatus::Degraded) {
        return Vec::new();
    }
    let mode = crate::orch::ReplMode::of_itype(&inst.itype);
    let mut out = Vec::new();
    for n in &inst.nodes {
        // 执行按节点路由(P2-①):绑定宿主机 + agent 接入 → 经 agent 探测(事实真实);
        // agent 未接入/不可达 → alive=false + remote 标记(本机/agent 均不执行,避免误判)
        let route = resolve_route_of(inst, &n.container, hosts);
        if let NodeRoute::Agent { ag, host } = &route {
            if !ag.ping().await {
                out.push(serde_json::json!({
                    "container": n.container,
                    "role": if n.role == Role::Master { "master" } else { "slave" },
                    "mode": mode.label(), "alive": false,
                    "repl_ok": false, "io_running": false, "sql_running": false,
                    "lag_secs": serde_json::Value::Null, "semisync": serde_json::Value::Null,
                    "host": host, "via": "unmanaged", "remote": true,
                    "reason": "远端 agent 不可达(本机执行已跳过)",
                }));
                continue;
            }
        }
        if let NodeRoute::Unmanaged { host } = &route {
            out.push(serde_json::json!({
                "container": n.container,
                "role": if n.role == Role::Master { "master" } else { "slave" },
                "mode": mode.label(), "alive": false,
                "repl_ok": false, "io_running": false, "sql_running": false,
                "lag_secs": serde_json::Value::Null, "semisync": serde_json::Value::Null,
                "host": host, "via": "unmanaged", "remote": true,
                "reason": "远端 agent 未接入(本机执行已跳过)",
            }));
            continue;
        }
        let (alive, io, sql, semisync) = if n.role == Role::Master {
            let alive = r_sql(&route, &n.container, "root", ROOT_PASS, "SELECT 1")
                .await
                .is_ok();
            let (en, ack) = r_sql(
                &route,
                &n.container,
                "root",
                ROOT_PASS,
                "SELECT @@global.rpl_semi_sync_master_enabled, IFNULL((SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Rpl_semi_sync_master_clients'),0)",
            )
            .await
            .ok()
            .and_then(|o| {
                let mut it = o.split('\t');
                let en = it.next().and_then(|x| x.trim().parse::<u8>().ok()).unwrap_or(0) == 1;
                let ack = it.next().and_then(|x| x.trim().parse::<u64>().ok()).unwrap_or(0);
                Some((en, ack))
            })
            .unwrap_or((false, 0));
            (
                alive,
                true,
                true,
                Some(crate::orch::SemiSync {
                    master_enabled: en,
                    master_ack: ack,
                    master_degraded: en && ack == 0,
                    slave_enabled: false,
                }),
            )
        } else {
            let probe = r_sql(&route, &n.container, "root", ROOT_PASS, "SELECT 1").await;
            let alive = probe.is_ok();
            // IO/SQL 线程(P_S,与 replica_problem 同源)
            let (io, sql) = if alive {
                r_sql(
                    &route,
                    &n.container,
                    "root",
                    ROOT_PASS,
                    "SELECT CONCAT_WS('|', IF(EXISTS(SELECT 1 FROM performance_schema.replication_connection_status WHERE CHANNEL_NAME='' AND SERVICE_STATE='ON'),'1','0'), IF(EXISTS(SELECT 1 FROM performance_schema.replication_applier_status WHERE CHANNEL_NAME='' AND SERVICE_STATE='ON'),'1','0'))",
                )
                .await
                .ok()
                .map(|o| {
                    let mut it = o.split('|');
                    (
                        it.next().map(|x| x.trim() == "1").unwrap_or(false),
                        it.next().map(|x| x.trim() == "1").unwrap_or(false),
                    )
                })
                .unwrap_or((false, false))
            } else {
                (false, false)
            };
            // 从侧半同步开关
            let slv_en = if alive {
                r_sql(
                    &route,
                    &n.container,
                    "root",
                    ROOT_PASS,
                    "SELECT @@global.rpl_semi_sync_slave_enabled",
                )
                .await
                .ok()
                .map(|o| o.trim() == "1")
                .unwrap_or(false)
            } else {
                false
            };
            (
                alive,
                io,
                sql,
                Some(crate::orch::SemiSync {
                    master_enabled: false,
                    master_ack: 0,
                    master_degraded: false,
                    slave_enabled: slv_en,
                }),
            )
        };
        let fact = crate::orch::Fact {
            container: n.container.clone(),
            role: if n.role == Role::Master {
                "master"
            } else {
                "slave"
            }
            .to_string(),
            mode,
            alive,
            io_running: io,
            sql_running: sql,
            lag_secs: None,
            semisync,
        };
        let semi = fact.semisync.as_ref().map(|s| {
            serde_json::json!({
                "master_enabled": s.master_enabled, "master_ack": s.master_ack,
                "master_degraded": s.master_degraded, "slave_enabled": s.slave_enabled,
            })
        });
        out.push(serde_json::json!({
            "container": fact.container, "role": fact.role,
            "mode": fact.mode.label(), "alive": fact.alive,
            "repl_ok": fact.repl_ok(), "io_running": io, "sql_running": sql,
            "lag_secs": fact.lag_secs, "semisync": semi,
            "via": route.label(),
            "host": match &route {
                NodeRoute::Agent { host, .. } => host,
                _ => "",
            },
        }));
    }
    out
}

/// 受管切换 worker:执行核心 → 审计/降级/告警;持锁期间续租由外层(调用前 lock)保证
async fn run_reparent(
    mgr: &Arc<RdsManager>,
    name: &str,
    target: Option<String>,
    mode: &str,
    opid: &str,
) {
    let kind = if opid.contains("rollback") {
        "rollback"
    } else if mode == "planned" {
        "PRS"
    } else {
        "ERS"
    };
    let target_label = target.clone().unwrap_or_else(|| "auto".to_string());
    orch_op_set(
        name,
        serde_json::json!({
            "state": "running", "kind": kind, "mode": mode,
            "target": target_label, "op_id": opid, "ts": now(),
        }),
    );
    match reparent_core(mgr, name, target, mode).await {
        Ok(logs) => {
            mgr.store
                .audit("system", name, "reparent", &logs.join("; "), "ok", opid);
            let rec = serde_json::json!({
                "state": "ok", "kind": kind, "mode": mode,
                "target": target_label, "op_id": opid, "ts": now(),
                "finished": now(), "message": logs.join("; "),
            });
            orch_op_set(name, rec.clone());
            orch_hist_push(name, rec);
            tracing::info!("实例 {name} 受管切换成功({mode}): {}", logs.join("; "));
        }
        Err(e) => {
            mgr.store
                .audit("system", name, "reparent", &e, "failed", opid);
            mgr.store.alert_open(
                name,
                "task_failed",
                "critical",
                &format!("受管切换失败:{e}"),
            );
            let rec = serde_json::json!({
                "state": "failed", "kind": kind, "mode": mode,
                "target": target_label, "op_id": opid, "ts": now(),
                "finished": now(), "message": e,
            });
            orch_op_set(name, rec.clone());
            orch_hist_push(name, rec);
            if let Some(mut i) = mgr.instances.get_mut(name) {
                i.status = InstStatus::Degraded;
                if !i.last_error.contains("切换") {
                    i.last_error = format!("受管切换失败:{e}");
                }
            }
            mgr.persist();
            tracing::error!("实例 {name} 受管切换失败: {e}");
        }
    }
}

/// 核心:读旧主 → 选/验候选 → (旧主存活则只读) → 提升候选 → 旧主重挂(planned/best-effort) → 登记角色互换
async fn reparent_core(
    mgr: &Arc<RdsManager>,
    name: &str,
    target: Option<String>,
    mode: &str,
) -> Result<Vec<String>, String> {
    let mut log: Vec<String> = Vec::new();
    let inst = mgr
        .instances
        .get(name)
        .map(|e| e.value().clone())
        .ok_or_else(|| format!("实例 {name} 不存在"))?;
    let old = inst
        .nodes
        .iter()
        .find(|n| n.role == Role::Master)
        .map(|n| n.container.clone())
        .ok_or_else(|| format!("实例 {name} 缺主节点,无法切换"))?;
    // 候选:优先显式目标;否则按事实层候选挑选(async 取 lag 最小;sync 半同步取 ack-safe)
    let cand = match target {
        Some(t) if !t.is_empty() => t,
        _ => {
            let facts = mgr.orch_facts(name).await;
            let zero = inst.itype == "sync";
            let cands: Vec<crate::orch::Candidate> = facts
                .iter()
                .filter(|f| {
                    f["role"].as_str() == Some("slave") && f["alive"].as_bool() == Some(true)
                })
                .map(|f| crate::orch::Candidate {
                    name: f["container"].as_str().unwrap_or("").to_string(),
                    lag_secs: 0,
                    ack_safe: f["semisync"]
                        .as_object()
                        .map(|s| {
                            s.get("slave_enabled")
                                .and_then(|x| x.as_bool())
                                .unwrap_or(false)
                        })
                        .unwrap_or(false),
                })
                .collect();
            if cands.is_empty() {
                return Err(format!("实例 {name} 无可用候选从(事实层无存活从)"));
            }
            let idx = crate::orch::pick_candidate(&cands, zero).ok_or_else(|| {
                format!(
                    "实例 {name} 无 {}(近零丢)候选",
                    if zero { "ack-safe" } else { "可用" }
                )
            })?;
            cands[idx].name.clone()
        }
    };
    if cand == old {
        return Err("候选不能是当前主节点".into());
    }
    // 旧主可达性(auto 允许不可达)
    let old_alive = dk::exec_mysql_local(&old, "root", ROOT_PASS, "SELECT 1")
        .await
        .is_ok();
    if !old_alive && mode == "planned" {
        return Err("旧主不可达,planned(PRS)要求旧主在线;请用 auto(ERS)".into());
    }
    if old_alive {
        // 停写
        match dk::exec_mysql_local(&old, "root", ROOT_PASS, "SET GLOBAL read_only=ON").await {
            Ok(_) => log.push(format!("旧主 {old} 已置只读")),
            Err(e) => return Err(format!("旧主 {old} 置只读失败:{e}")),
        }
    } else {
        log.push(format!("旧主 {old} 不可达,跳过只读(ERS)"));
    }
    // 提升候选
    let promote_sqls = ["STOP SLAVE", "RESET SLAVE ALL", "SET GLOBAL read_only=OFF"];
    for s in promote_sqls {
        if let Err(e) = dk::exec_mysql_local(&cand, "root", ROOT_PASS, s).await {
            // 提升失败:若旧主已只读则回滚其可写,避免两端只读
            if old_alive {
                let _ =
                    dk::exec_mysql_local(&old, "root", ROOT_PASS, "SET GLOBAL read_only=OFF").await;
            }
            return Err(format!("提升候选 {cand} 失败({s}): {e}"));
        }
    }
    log.push(format!("候选 {cand} 提升为主(STOP/RESET SLAVE,可写)"));
    // 旧主(存活)重挂为新主的从(best-effort;GTID 关闭环境会失败,仅告警不阻断)
    if old_alive {
        let change = format!(
            "STOP SLAVE; CHANGE MASTER TO MASTER_HOST='{cand}', MASTER_PORT=3306, MASTER_USER='repl', MASTER_PASSWORD='{REPL_PASS}', MASTER_AUTO_POSITION=1; START SLAVE;"
        );
        match dk::exec_mysql_local(&old, "root", ROOT_PASS, &change).await {
            Ok(_) => log.push(format!("旧主 {old} 已重挂到新主 {cand}")),
            Err(e) => log.push(format!("旧主 {old} 重挂失败(可后续手工修复):{e}")),
        }
    }
    // 切换前快照入库(evidence,供回滚定位 prev/cur master)
    {
        let snap = serde_json::json!({
            "prev_master": old, "cur_master": cand,
            "itype": inst.itype, "proxy_version": inst.proxy_version,
        });
        mgr.store.evidence_insert(
            name,
            "reparent_snapshot",
            "受管切换前快照",
            &snap.to_string(),
        );
    }
    // 登记角色互换(单写者;审计已由外层完成)
    {
        let mut i2 = mgr
            .instances
            .get_mut(name)
            .ok_or_else(|| format!("实例 {name} 不存在"))?;
        let mut swap_done = false;
        for n in i2.nodes.iter_mut() {
            if n.container == old && n.role == Role::Master {
                n.role = Role::Read;
                n.parent = cand.clone();
                swap_done = true;
            } else if n.container == cand && n.role != Role::Master {
                n.role = Role::Master;
                n.parent.clear();
                swap_done = true;
            } else if n.role != Role::Master && n.container != old && n.parent == old {
                // 其余从改为从新主复制(登记侧;SQL 侧为旧主已重挂到新主的链式,演示/受控可接受)
                n.parent = cand.clone();
            }
        }
        if !swap_done {
            return Err(format!("登记节点未匹配(旧主 {old}/候选 {cand}),未切换"));
        }
    }
    mgr.persist();
    log.push(format!("登记角色互换完成:master {old}→{cand}"));
    Ok(log)
}

// ─── region/shard/tenant(控制面归属,M0 起可配;env: RDSCTL_REGION/RDSCTL_AZ/RDSCTL_SHARD/RDSCTL_TENANT) ───
fn default_true() -> bool {
    true
}

fn env_str(key: &str, d: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| d.to_string())
}

/// 查询口令生成用计数器(时间+pid+计数混拼,与 store::salt_bytes 同思路)
static QUERY_SECRET_CNT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn sys_region() -> String {
    env_str("RDSCTL_REGION", "default")
}

fn sys_az() -> String {
    env_str("RDSCTL_AZ", "default")
}

fn sys_shard() -> String {
    env_str("RDSCTL_SHARD", "default")
}

fn sys_tenant() -> String {
    env_str("RDSCTL_TENANT", "")
}

/// 控制端标识(lease holder);跨副本唯一
fn controller_id() -> String {
    if let Ok(v) = std::env::var("RDSCTL_CONTROLLER_ID") {
        if !v.trim().is_empty() {
            return v.trim().to_string();
        }
    }
    // cluster 模式:holder = 集群节点 id(跨重启**稳定**)。
    // 为什么重要:holder 决定租约归属与 fence 归属;若用 host:pid,进程重启就变成"另一个控制端",
    // 只能等旧租约过期 + skew 才能接管自己刚才的实例(可用性损失),审计里也认不出是谁。
    if let Ok(n) = std::env::var("RDSCTL_NODE_ID") {
        if !n.trim().is_empty() {
            return format!("cluster:{}", n.trim());
        }
    }
    let host = env_str("HOSTNAME", "node");
    format!("{host}:{}", std::process::id())
}

fn proxy_config(master_host: &str, slave_host: &str) -> String {
    format!(
        r#"[MySQL_Proxy_Layer]
port=4051
mng_port=9111
mng_user=admin
mng_password=admin
max_threads=4
log_dir=logs
log_level=warn
client_timeout=28800
server_timeout=28800
conn_pool_socket_max_serve_client_times=100000
max_sql_size=16777216
max_query_size=16777216
default_charset=33
stream_transport_enable=0
slow_query_ms=200

[Cluster_0]
name=rds

[CTablet_0_t0]
name=t0

[Master_Host_g0]
host={master_host}
port=3306
max_conn_pool_size=16
max_connections=256
connect_timeout=5
weight=1
cluster_tablet_name=t0

[Slave_Host_g0]
host={slave_host}
port=3306
max_conn_pool_size=16
max_connections=256
connect_timeout=5
weight=1
cluster_tablet_name=t0

[DB_User_dbu]
db_username=root
db_password={ROOT_PASS}
default_db={APP_DB}
cluster_name=rds

[Product_User_pu]
username=root
password={ROOT_PASS}
db_username=root
max_connections=256
cluster_name=rds
"#,
        master_host = master_host,
        slave_host = slave_host,
    )
}

#[cfg(test)]
mod m0_bench {
    use super::*;
    use crate::store::{MemoryBackend, Store};
    use std::time::Instant;

    fn row(i: u32) -> String {
        let region = if i % 3 == 0 { "cn-bj" } else { "cn-sh" };
        let status = if i % 5 == 0 { "degraded" } else { "running" };
        serde_json::json!({
            "name": format!("bench-{i}"),
            "status": status,
            "region": region,
            "az": "az1",
            "shard": "s0",
            "tenant": "t1",
            "network": "net", "nodes": [], "proxy_container": "px",
            "proxy_mysql_port": 0, "proxy_mng_port": 0,
            "created_at": 1700000000u64 + i as u64,
            "root_password": "x", "last_error": "",
        })
        .to_string()
    }

    #[test]
    fn m0_synthetic_10k_instances() {
        let n = 10_000u32;
        let t0 = Instant::now();
        let store = Arc::new(Store::from_backend(MemoryBackend::new()));
        for i in 0..n {
            let region = if i % 3 == 0 { "cn-bj" } else { "cn-sh" };
            store.instance_upsert(
                &format!("bench-{i}"),
                region,
                "t1",
                &row(i),
                if i % 5 == 0 { "degraded" } else { "running" },
                1,
            );
        }
        let t1 = Instant::now();
        let mgr = RdsManager::new(store);
        let t2 = Instant::now();
        let (page, total) = mgr.list_filtered(Some("cn-bj"), None, None, Some("t1"), None, 0, 200);
        let t3 = Instant::now();
        // i%3==0 → cn-bj(0..=9999 共 3334 条);其它 cn-sh
        assert_eq!(total, 3334, "cn-bj 实例数应约 1/3");
        assert_eq!(page.len(), 200);
        assert!(page.iter().all(|v| v["region"] == "cn-bj"));
        eprintln!("[m0-bench] instance_upsert x{n}: {:?}", t1 - t0);
        eprintln!("[m0-bench] RdsManager::new + 载入 {n} 行: {:?}", t2 - t1);
        eprintln!(
            "[m0-bench] list_filtered(region=cn-bj,page200): {:?} total={total}",
            t3 - t2
        );
        // 第二页 + 其它筛选仍正确
        let (p2, t2n) =
            mgr.list_filtered(Some("cn-sh"), None, Some("running"), None, None, 200, 200);
        assert_eq!(p2.len(), 200);
        // cn-sh(6666) 中 running(i%5!=0 且非 3 倍数):6666 - 1333 = 5333
        assert_eq!(t2n, 5333);
        let (_, degraded) = mgr.list_filtered(None, None, Some("degraded"), None, None, 0, 10_000);
        assert_eq!(degraded, 2000, "degraded = i%5==0");
        // 分页 offset 越过结尾
        let (p3, _) = mgr.list_filtered(Some("cn-bj"), None, None, None, None, 100_000, 200);
        assert!(p3.is_empty());
    }

    #[test]
    fn offline_scaleout_single_only() {
        // 离线从(备份/统计/大查询)每实例仅一个:已有离线从时再扩容离线类角色应被拒
        let store = Arc::new(Store::from_backend(MemoryBackend::new()));
        let mgr = RdsManager::new(store);
        let inst = RdsInstance {
            lvs: Vec::new(),
            lvs_container: String::new(),
            lvs_mysql_port: 0,
            proxies: Vec::new(),
            shards: Vec::new(),
            biz: String::new(),
            contact: String::new(),
            dba: String::new(),
            core: false,
            itype: "async".to_string(),
            mysql_version: String::new(),
            proxy_version: String::new(),
            spec: String::new(),
            shard_num: 0,
            data_size: String::new(),
            buffer_pool: String::new(),
            max_qps: 0,
            max_tps: 0,
            name: "x".into(),
            status: InstStatus::Running,
            region: "cn-bj".into(),
            az: "az1".into(),
            shard: "s0".into(),
            tenant: "t1".into(),
            enabled: true,
            network: "rds-x".into(),
            nodes: vec![
                InstNode {
                    container: "rds-x-master".into(),
                    role: Role::Master,
                    host: "rds-x-master".into(),
                    port: 3306,
                    host_port: 1,
                    server_id: 10,
                    region: String::new(),
                    az: String::new(),
                    shard: String::new(),
                    parent: String::new(),
                    rpc_host_port: 0,
                },
                InstNode {
                    container: "rds-x-slave-1".into(),
                    role: Role::Read,
                    host: "rds-x-slave-1".into(),
                    port: 3306,
                    host_port: 2,
                    server_id: 11,
                    region: String::new(),
                    az: String::new(),
                    shard: String::new(),
                    parent: String::new(),
                    rpc_host_port: 0,
                },
                InstNode {
                    container: "rds-x-slave-2".into(),
                    role: Role::Offline,
                    host: "rds-x-slave-2".into(),
                    port: 3306,
                    host_port: 3,
                    server_id: 12,
                    region: String::new(),
                    az: String::new(),
                    shard: String::new(),
                    parent: String::new(),
                    rpc_host_port: 0,
                },
            ],
            proxy_container: "px".into(),
            proxy_mysql_port: 4,
            proxy_mng_port: 5,
            created_at: 1,
            root_password: "p".into(),
            query_secret: String::new(),
            last_error: String::new(),
            node_states: std::collections::HashMap::new(),
            node_hosts: std::collections::HashMap::new(),
            auto_failover: true,
        };
        mgr.instances.insert("x".into(), inst);
        let err = mgr
            .scaleout("x", Role::Offline, None, None)
            .expect_err("已有离线从,再扩容离线应被拒");
        assert!(
            err.contains("已存在离线从"),
            "错误信息应说明离线唯一: {err}"
        );
        // 历史角色 Stats/Backup 同属离线类,同样被拒
        let err2 = mgr
            .scaleout("x", Role::Stats, None, None)
            .expect_err("Stats 属离线类,应被拒");
        assert!(err2.contains("已存在离线从"));
    }

    #[test]
    fn m0_synthetic_persistence_writes() {
        // 持续写入模拟(任务/节点/审计高频写路径,M0 内存后端上限量级)
        let store = Arc::new(Store::from_backend(MemoryBackend::new()));
        let t0 = Instant::now();
        for i in 0..5000u32 {
            let tid = format!("t-create-{i}");
            store.upsert_task(
                &tid,
                "create",
                &format!("i{i}"),
                "pending",
                i as u64,
                None,
                None,
            );
            store.upsert_node(
                &tid,
                "n1",
                "node",
                "[]",
                "[]",
                "pending",
                "",
                0,
                1,
                Some(60),
                None,
                None,
            );
            store.audit("admin", &format!("i{i}"), "create", "submitted", "", &tid);
        }
        let t1 = Instant::now();
        eprintln!("[m0-bench] 5000 任务×节点×审计写(内存后端): {:?}", t1 - t0);
        assert_eq!(store.task_views(10_000).len(), 5000);
    }
}

#[cfg(test)]
mod ai0_evidence {
    use super::*;

    #[test]
    fn redact_and_clip_work() {
        // 口令类常量被脱敏
        let s = format!("mysql error near {ROOT_PASS} and {REPL_PASS}");
        let r = redact_secret(&s);
        assert!(
            !r.contains(ROOT_PASS) && !r.contains(REPL_PASS),
            "口令必须脱敏: {r}"
        );
        assert!(r.contains("***"));
        // 截断按字符安全(含中文)
        assert_eq!(clip("abcde", 3), "abc");
        assert_eq!(clip("你好世界", 2), "你好");
        assert_eq!(clip("short", 100), "short");
    }

    #[test]
    fn snapshot_write_rule_no_write_storm() {
        // running→degraded:写
        assert!(snapshot_write_needed(true, "", "代理不可达"));
        // 已 degraded 且原因相同:不写(防每 30s 刷)
        assert!(!snapshot_write_needed(false, "代理不可达", "代理不可达"));
        // 已 degraded 但原因变化:写
        assert!(snapshot_write_needed(
            false,
            "代理不可达",
            "从库 x 复制中断"
        ));
        // 已 degraded 原因由空到有/有到空:写
        assert!(snapshot_write_needed(false, "", "容器缺失"));
        assert!(snapshot_write_needed(false, "容器缺失", ""));
    }
}

#[cfg(test)]
mod module_backup {
    // 样例功能模块(backup)的后端行为测试:
    // 门槛 → 提交 → 终态不改实例状态 → 解锁可重试 → 失败告警留痕
    use super::*;
    use crate::store::{MemoryBackend, Store};

    fn mem_mgr() -> Arc<RdsManager> {
        let store = Arc::new(Store::from_backend(MemoryBackend::new()));
        RdsManager::new(store)
    }

    fn inst(name: &str, status: InstStatus, enabled: bool, with_master: bool) -> RdsInstance {
        let nodes = if with_master {
            vec![InstNode {
                container: format!("rds-{name}-master"),
                role: Role::Master,
                host: format!("rds-{name}-master"),
                port: 3306,
                host_port: 35001,
                server_id: 10,
                region: "cn-bj".into(),
                az: "az1".into(),
                shard: "s0".into(),
                parent: String::new(),
                rpc_host_port: 0,
            }]
        } else {
            Vec::new()
        };
        RdsInstance {
            lvs: Vec::new(),
            lvs_container: String::new(),
            lvs_mysql_port: 0,
            proxies: Vec::new(),
            shards: Vec::new(),
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
            name: name.to_string(),
            status,
            region: "cn-bj".into(),
            az: "az1".into(),
            shard: "s0".into(),
            tenant: "t1".into(),
            enabled,
            network: format!("rds-{name}"),
            nodes,
            proxy_container: String::new(),
            proxy_mysql_port: 0,
            proxy_mng_port: 0,
            created_at: 1,
            root_password: "p".into(),
            query_secret: String::new(),
            last_error: String::new(),
            node_states: std::collections::HashMap::new(),
            node_hosts: std::collections::HashMap::new(),
            auto_failover: true,
        }
    }

    #[test]
    fn guards() {
        let mgr = mem_mgr();
        let err = mgr.run_backup("ghost").expect_err("实例不存在应被拒");
        assert!(err.contains("不存在"), "{err}");
        mgr.instances
            .insert("x".into(), inst("x", InstStatus::Running, false, true));
        let err = mgr.run_backup("x").expect_err("停用实例应被拒");
        assert!(err.contains("已停用"), "{err}");
        mgr.instances
            .insert("y".into(), inst("y", InstStatus::Degraded, true, true));
        let err = mgr.run_backup("y").expect_err("非运行状态应被拒");
        assert!(err.contains("状态"), "{err}");
        mgr.instances
            .insert("z".into(), inst("z", InstStatus::Running, true, false));
        let err = mgr.run_backup("z").expect_err("缺少主节点应被拒");
        assert!(err.contains("主节点"), "{err}");
    }

    #[tokio::test]
    async fn submit_fails_gracefully_keeps_running_and_releases_lock() {
        let mgr = mem_mgr();
        mgr.instances
            .insert("x".into(), inst("x", InstStatus::Running, true, true));
        let tid = mgr.run_backup("x").expect("运行中实例应可提交备份");
        assert!(tid.starts_with("t-backup-"), "{tid}");
        let v = mgr.scheduler.get(&tid).expect("任务应可见");
        assert_eq!(v["kind"], "backup");
        assert_eq!(v["instance"], "x");
        assert_eq!(v["nodes"][0]["module"], "backup", "节点模块徽章应为 backup");

        // 等待任务终态(容器不存在/docker 不可用 → 优雅失败)
        let mut terminal = false;
        for _ in 0..400 {
            if let Some(t) = mgr.scheduler.get(&tid) {
                let st = t["status"].as_str().unwrap_or("");
                if st == "success" || st == "failed" {
                    terminal = true;
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        assert!(terminal, "备份任务应在数秒内到达终态");
        // 终态联动:实例锁释放(由 watch_backup_task)
        for _ in 0..200 {
            if !mgr.op_locks.contains_key("x") {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        assert!(!mgr.op_locks.contains_key("x"), "终态后实例锁应释放");
        assert_eq!(
            mgr.instances.get("x").unwrap().status,
            InstStatus::Running,
            "备份失败不得把运行中实例置为 failed"
        );
        // 锁已释放 → 可再次提交(失败后重试路径)
        let tid2 = mgr.run_backup("x").expect("锁释放后应可再次备份");
        assert!(tid2.starts_with("t-backup-"), "{tid2}");
        // 失败留痕:该实例有 task_failed 告警
        let alerts = mgr.store.alert_list(50, None, None, Some("x"));
        assert!(
            alerts
                .iter()
                .any(|a| a["kind"].as_str() == Some("task_failed")),
            "备份失败应有告警留痕"
        );
    }

    // ── 销毁后可「彻底删除实例记录」 ──

    #[test]
    fn delete_record_only_after_destroyed() {
        let mgr = mem_mgr();
        mgr.instances
            .insert("run".into(), inst("run", InstStatus::Running, true, true));
        mgr.instances
            .insert("bad".into(), inst("bad", InstStatus::Degraded, true, true));
        mgr.instances.insert(
            "gone".into(),
            inst("gone", InstStatus::Destroyed, true, true),
        );

        let err = mgr.delete_instance("ghost").expect_err("不存在实例应被拒");
        assert!(err.contains("不存在"), "{err}");
        for n in ["run", "bad"] {
            let e = mgr.delete_instance(n).expect_err("未销毁实例应被拒");
            assert!(e.contains("请先销毁"), "{n}: {e}");
        }
        // 成功:已销毁实例 → 内存 + 持久化记录一并移除 + 审计留痕
        mgr.delete_instance("gone").expect("已销毁实例可彻底删除");
        assert!(!mgr.instances.contains_key("gone"), "内存记录应移除");
        let all = mgr.store.instance_load_all();
        assert!(!all.iter().any(|(n, _)| n == "gone"), "持久化记录应移除");
        let audit = mgr.store.audit_list(100, None, None, None);
        assert!(
            audit
                .iter()
                .any(|a| a["action"].as_str() == Some("instance_delete")
                    && a["instance"].as_str() == Some("gone")),
            "应留审计 instance_delete"
        );
        // 删除后再次删除 → 不存在
        let e2 = mgr
            .delete_instance("gone")
            .expect_err("删除后再次删除应报不存在");
        assert!(e2.contains("不存在"), "{e2}");
    }
}

#[cfg(test)]
mod port_alloc_tests {
    use super::*;

    #[test]
    fn host_port_free_detects_occupation() {
        // 空闲
        let p = 35_900u16;
        assert!(host_port_free(p), "空闲端口应可绑定");
        // 占住后再测
        let _l = std::net::TcpListener::bind(("127.0.0.1", p)).expect("bind");
        assert!(!host_port_free(p), "被占用端口探测应失败");
    }

    #[test]
    fn alloc_skips_occupied_ports() {
        let store = Arc::new(crate::store::Store::from_backend(
            crate::store::MemoryBackend::new(),
        ));
        let m = RdsManager::new(store);
        // 动态挑选一个当前空闲的端口占用,再断言分配跳过它(避免依赖本机空闲段)
        let mut p = 37_000u16;
        while !host_port_free(p) {
            p += 1;
            assert!(p < 38_000, "找不到空闲测试端口");
        }
        let occupy = std::net::TcpListener::bind(("127.0.0.1", p)).unwrap();
        let got = m.alloc_host_port();
        assert_ne!(got, p, "应跳过被占用的端口 {p}");
        assert!(got >= 35_000, "分配结果应在起始区间, got {got}");
        assert!(host_port_free(got), "分配结果应真实可用");
        drop(occupy);
    }
}

#[cfg(test)]
mod demo_seed {
    use super::*;
    use crate::store::{MemoryBackend, Store};

    fn mem_mgr() -> Arc<RdsManager> {
        let store = Arc::new(Store::from_backend(MemoryBackend::new()));
        RdsManager::new(store)
    }

    #[test]
    fn seeds_only_when_enabled_and_empty() {
        // 空库 + enabled → 种入演示数据
        let mgr = mem_mgr();
        assert!(mgr.instances.is_empty());
        mgr.seed_demo_if_empty(true);
        assert!(
            mgr.instances.len() >= 14,
            "应种入 14+ 演示实例, got {}",
            mgr.instances.len()
        );
        // 持久化层同样有记录
        assert!(!mgr.store.instance_load_all().is_empty());
        // 每个演示实例都带业务线/DBA/版本/规格标签,且 network 以 demo- 开头(巡检豁免)
        for e in mgr.instances.iter() {
            let i = e.value();
            assert!(
                !i.biz.is_empty() && !i.dba.is_empty(),
                "{} 缺 biz/dba",
                i.name
            );
            assert!(!i.mysql_version.is_empty(), "{} 缺版本", i.name);
            assert!(
                i.network.starts_with("demo-"),
                "{} network 应为 demo-*",
                i.name
            );
        }
        // disabled 时不种入
        let mgr2 = mem_mgr();
        mgr2.seed_demo_if_empty(false);
        assert!(mgr2.instances.is_empty(), "enabled=false 不应种入");
        // 已有实例时不种入
        let mgr3 = mem_mgr();
        mgr3.seed_demo_if_empty(true);
        let n1 = mgr3.instances.len();
        mgr3.seed_demo_if_empty(true);
        assert_eq!(mgr3.instances.len(), n1, "二次调用不重复种入");
    }

    #[test]
    fn dba_rank_stable_when_counts_tie() {
        let mgr = mem_mgr();
        mgr.seed_demo_if_empty(true);
        let s1 = mgr.summary();
        let s2 = mgr.summary();
        assert_eq!(
            s1["dba_top"].to_string(),
            s2["dba_top"].to_string(),
            "同数量 DBA 榜单应跨请求稳定(次级按名排序)"
        );
        let arr = s1["dba_top"].as_array().unwrap();
        for w in arr.windows(2) {
            let a = w[0]["count"].as_u64().unwrap_or(0);
            let b = w[1]["count"].as_u64().unwrap_or(0);
            assert!(a >= b, "榜单应按负责数量降序: {a} < {b}");
        }
    }
}

// ═══ DB 节点实时监控(差分采样 SHOW GLOBAL STATUS;与慢查/巡检同走 root 容器通道) ═══
static DB_MON_SAMPLES: std::sync::Mutex<
    Option<std::collections::HashMap<String, (u64, u64, u64)>>,
> = std::sync::Mutex::new(None); // container -> (ts, questions, bytes)

/// GET /api/rds/monitor/dbs?instance= —— 每个 DB 节点当前连接数/QPS(差分)/吞吐等
#[allow(dead_code)] // 无过滤快捷入口(monitor_db_metrics_filtered 为实际使用路径)
pub async fn monitor_db_metrics(name: &str) -> Result<Vec<serde_json::Value>, String> {
    monitor_db_metrics_filtered(name, None, None).await
}

/// GET /api/rds/monitor/dbs?instance=&node=<container>&limit=N
///  - 不传 node:轮询实例全部 DB 节点(建议配合 limit 控制采样量)
///  - 传 node:仅对该节点采样(单节点下钻,开销固定)
pub async fn monitor_db_metrics_filtered(
    name: &str,
    node: Option<&str>,
    limit: Option<usize>,
) -> Result<Vec<serde_json::Value>, String> {
    use serde_json::json;
    let m = crate::manager();
    let Some(inst) = m.instances.get(name).map(|e| e.value().clone()) else {
        return Err(format!("实例 {name} 不存在"));
    };
    if inst.status != InstStatus::Running {
        return Ok(vec![]); // 非运行:无采样
    }
    let sql = "SHOW GLOBAL STATUS WHERE Variable_name IN \
               ('Threads_connected','Threads_running','Questions','Bytes_received','Bytes_sent','Uptime')";
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let targets: Vec<InstNode> = inst
        .nodes
        .iter()
        .filter(|n| node.map_or(true, |c| n.container == c))
        .cloned()
        .collect();
    let mut out = Vec::new();
    for n in &targets {
        if let Some(lim) = limit {
            if out.len() >= lim {
                break;
            }
        }
        let role = match n.role {
            Role::Master => "master",
            Role::Read => "read",
            _ => "offline",
        };
        let raw = dk::query_table(&n.container, "root", ROOT_PASS, sql, 10, "")
            .await
            .unwrap_or_default();
        let mut vals: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
        for line in raw.lines() {
            let mut it = line.splitn(2, '\t');
            let (Some(k), Some(v)) = (it.next(), it.next()) else {
                continue;
            };
            if let Ok(n2) = v.trim().parse::<u64>() {
                vals.insert(k.trim().to_uppercase(), n2);
            }
        }
        let conns = vals.get("THREADS_CONNECTED").copied().unwrap_or(0);
        let threads_running = vals.get("THREADS_RUNNING").copied().unwrap_or(0);
        let uptime = vals.get("UPTIME").copied().unwrap_or(0);
        let questions = vals.get("QUESTIONS").copied().unwrap_or(0);
        let bytes = vals
            .get("BYTES_RECEIVED")
            .zip(vals.get("BYTES_SENT"))
            .map(|(a, b)| a.saturating_add(*b))
            .unwrap_or(0);
        let mut qps: Option<u64> = None;
        let mut throughput: Option<u64> = None;
        {
            let g = DB_MON_SAMPLES.lock().unwrap();
            if let Some((pts, pq, pb)) = g.as_ref().and_then(|m2| m2.get(&n.container)).copied() {
                let dt = now.saturating_sub(pts).max(1);
                if questions >= pq {
                    qps = Some((questions - pq) / dt);
                }
                if bytes >= pb {
                    throughput = Some((bytes - pb) / dt);
                }
            }
        }
        {
            let mut g = DB_MON_SAMPLES.lock().unwrap();
            g.get_or_insert_with(Default::default)
                .insert(n.container.clone(), (now, questions, bytes));
        }
        out.push(json!({
            "container": n.container, "role": role, "shard": n.shard,
            "conns": conns, "threads_running": threads_running,
            "qps": qps, "throughput_bps": throughput,
            "uptime": uptime, "port": n.host_port,
        }));
    }
    Ok(out)
}

// ═══ Proxy 配置读写(conf 挂载文件;保存后可重启生效) ═══

/// 由代理容器名反查所属实例与配置路径
fn proxy_conf_target(container: &str) -> Result<(String, String), String> {
    let m = crate::manager();
    for e in m.instances.iter() {
        let i = e.value();
        let owns = if !i.proxies.is_empty() {
            i.proxies.iter().any(|p| p.container == container)
        } else {
            i.proxy_container == container
        };
        if owns {
            let path = format!(
                "{}/logs/rds/{}/newproxy.conf",
                std::env::current_dir().unwrap_or_default().display(),
                i.name
            );
            return Ok((i.name.clone(), path));
        }
    }
    Err(format!("代理 {container} 不属于任何实例"))
}

/// GET /api/rds/proxy/conf?container= —— 读取当前配置内容
pub fn proxy_conf_get(container: &str) -> Result<String, String> {
    let (_inst, path) = proxy_conf_target(container)?;
    std::fs::read_to_string(&path).map_err(|e| format!("读取 {path} 失败: {e}"))
}

/// POST /api/rds/proxy/conf?container=&content= —— 写回配置并重启该代理容器
pub async fn proxy_conf_apply(container: &str, content: &str) -> Result<String, String> {
    if content.len() > 200_000 {
        return Err("配置过大(>200KB)".to_string());
    }
    let (inst, path) = proxy_conf_target(container)?;
    if let Some(dir) = std::path::Path::new(&path).parent() {
        std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
    }
    std::fs::write(&path, content).map_err(|e| format!("写入 {path} 失败: {e}"))?;
    dk::restart(container).await?;
    crate::manager().store.audit(
        &crate::auth::current_user(),
        &inst,
        "proxy_conf_apply",
        container,
        "ok",
        "",
    );
    Ok(format!("已更新 {container} 配置并重启"))
}

/// 由代理容器名反查其管理端口
fn proxy_mng_of(container: &str) -> Result<(String, u16), String> {
    let m = crate::manager();
    for e in m.instances.iter() {
        let i = e.value();
        let hit = if !i.proxies.is_empty() {
            i.proxies.iter().find(|p| p.container == container)
        } else if i.proxy_container == container {
            Some(&ProxyNode {
                container: i.proxy_container.clone(),
                mysql_port: i.proxy_mysql_port,
                mng_port: i.proxy_mng_port,
                spec: String::new(),
                ip: String::new(),
                qps: 0,
                conns: 0,
                cpu: 0.0,
                status: String::new(),
                version: String::new(),
            })
        } else {
            None
        };
        if let Some(p) = hit {
            return Ok((i.name.clone(), p.mng_port));
        }
    }
    Err(format!("代理 {container} 不属于任何实例"))
}

/// 代理管理端口代拉 /metrics 文本(浏览器跨端口会被 CORS 拦,统一走管控端出站拉取)
/// 简单 base64 编码(无外部依赖)
fn b64(data: &[u8]) -> String {
    const T: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in data.chunks(3) {
        let b0 = chunk[0];
        let b1 = chunk.get(1).copied().unwrap_or(0);
        let b2 = chunk.get(2).copied().unwrap_or(0);
        out.push(T[(b0 >> 2) as usize] as char);
        out.push(T[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char);
        if chunk.len() > 1 {
            out.push(T[(((b1 & 0x0f) << 2) | (b2 >> 6)) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(T[(b2 & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

/// 从实例 newproxy.conf 中提取管理端口 Basic Auth 凭据(mng_user/mng_password)
fn conf_basic_auth(conf: &str) -> Option<String> {
    let mut user: Option<String> = None;
    let mut pass: Option<String> = None;
    for line in conf.lines() {
        let t = line.trim();
        if t.is_empty() || t.starts_with('#') {
            continue;
        }
        if t.starts_with('[') {
            // 允许任意 section 内出现 mng_user/mng_password
            continue;
        }
        if let Some((k, v)) = t.split_once('=') {
            let k = k.trim();
            let v = v.trim();
            if k.eq_ignore_ascii_case("mng_user") && user.is_none() {
                user = Some(v.to_string());
            } else if k.eq_ignore_ascii_case("mng_password") && pass.is_none() {
                pass = Some(v.to_string());
            }
        }
    }
    user.map(|u| b64(format!("{u}:{}", pass.unwrap_or_default()).as_bytes()))
}

pub async fn proxy_metrics_text(container: &str) -> Result<String, String> {
    let (_inst, mng) = proxy_mng_of(container)?;
    if mng == 0 {
        return Err(format!("代理 {container} 未配置管理端口"));
    }
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let addr = format!("127.0.0.1:{mng}");
    let mut s = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        tokio::net::TcpStream::connect(&addr),
    )
    .await
    .map_err(|_| format!("连接代理管理端口 {mng} 超时"))?
    .map_err(|e| format!("连接代理管理端口 {mng} 失败: {e}"))?;
    // 读取同实例 newproxy.conf 中管理端 Basic Auth;未配置则免认证
    let auth = proxy_conf_get(container)
        .ok()
        .and_then(|c| conf_basic_auth(&c));
    let auth_hdr = match &auth {
        Some(a) => format!("Authorization: Basic {a}\r\n"),
        None => String::new(),
    };
    let req = format!(
        "GET /metrics HTTP/1.1\r\nHost: 127.0.0.1:{mng}\r\n{auth_hdr}Connection: close\r\nAccept: text/plain\r\n\r\n"
    );
    s.write_all(req.as_bytes())
        .await
        .map_err(|e| format!("发送请求失败: {e}"))?;
    let _ = s.shutdown().await;
    let mut buf = Vec::with_capacity(64 * 1024);
    let read = tokio::time::timeout(std::time::Duration::from_secs(4), s.read_to_end(&mut buf));
    let _ = read.await.map_err(|_| ());
    let raw = String::from_utf8_lossy(&buf);
    let Some((_head, body)) = raw.split_once("\r\n\r\n") else {
        return Err("管理端口响应不完整(非 HTTP)".to_string());
    };
    if !raw.starts_with("HTTP/1.1 200") && !raw.starts_with("HTTP/1.0 200") {
        return Err(format!(
            "管理端口返回: {}",
            raw.lines().next().unwrap_or("?")
        ));
    }
    Ok(body.to_string())
}

/// 解析 newproxy /metrics 文本中「新增/关键」指标(见 docs: §4.1/§8 实施记录)
/// 返回: connections_active / queries_total / queries_slow / queries_errors /
///        backend_errors / pool_idle / cpu_percent / p50_us / p95_us / duration_count
pub fn parse_proxy_metrics(text: &str) -> serde_json::Value {
    use serde_json::json;
    let mut active: Option<u64> = None;
    let mut q_total: Option<u64> = None;
    let mut q_slow: Option<u64> = None;
    let mut q_err: Option<u64> = None;
    let mut be_err: u64 = 0;
    let mut pool_idle: u64 = 0;
    let mut cpu: Option<f64> = None;
    // 延迟直方图:le(秒)→累计
    let mut buckets: Vec<(f64, u64)> = Vec::new();
    for line in text.lines() {
        let ln = line.trim();
        if ln.is_empty() || ln.starts_with('#') {
            continue;
        }
        let mut it = ln.splitn(2, ' ');
        let Some(name) = it.next() else { continue };
        let val = it.next().unwrap_or("").trim();
        let num = |s: &str| s.parse::<u64>().ok();
        match name {
            "newproxy_connections_active" => active = num(val),
            "newproxy_queries_total" => q_total = num(val),
            "newproxy_queries_slow_total" => q_slow = num(val),
            "newproxy_queries_errors_total" => q_err = num(val),
            "newproxy_process_cpu_percent" => cpu = val.parse::<f64>().ok(),
            _ => {}
        }
        if name.starts_with("newproxy_backend_errors_total") {
            be_err += num(val).unwrap_or(0);
        } else if name.starts_with("newproxy_pool_idle_connections") {
            pool_idle += num(val).unwrap_or(0);
        } else if name.starts_with("newproxy_queries_duration_seconds_bucket") {
            // {le="1e-05"}/.../ le 值在花括号里
            let le = ln[ln.find('"').map(|i| i + 1).unwrap_or(0)..]
                .split('"')
                .next()
                .unwrap_or("")
                .parse::<f64>()
                .ok();
            let cum = num(val).unwrap_or(0);
            if let Some(l) = le {
                buckets.push((l, cum));
            }
        }
    }
    buckets.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    let duration_count = buckets.last().map(|b| b.1).unwrap_or(0);
    let quantile = |q: f64| -> Option<u64> {
        let total = duration_count as f64;
        if total <= 0.0 {
            return None;
        }
        let target = total * q;
        for (le, cum) in &buckets {
            if *cum as f64 >= target {
                // le 单位为秒 → µs
                return Some((*le * 1_000_000.0) as u64);
            }
        }
        buckets.last().map(|b| (b.0 * 1_000_000.0) as u64)
    };
    json!({
        "connections_active": active,
        "queries_total": q_total,
        "queries_slow": q_slow,
        "queries_errors": q_err,
        "backend_errors": be_err,
        "pool_idle": pool_idle,
        "cpu_percent": cpu,
        "p50_us": quantile(0.5),
        "p95_us": quantile(0.95),
        "duration_count": duration_count,
    })
}

#[cfg(test)]
mod metrics_parse {
    use super::*;

    const SAMPLE: &str = r#"# HELP newproxy_connections_active x
newproxy_connections_active 12
newproxy_queries_total 38420
newproxy_queries_slow_total 3
newproxy_queries_errors_total 1
newproxy_backend_errors_total{code="1064"} 1
newproxy_backend_errors_total{code="1146"} 2
newproxy_process_cpu_percent 8.2
newproxy_pool_idle_connections{cluster="c",tablet="t",user="u",db="d",role="rw"} 5
# TYPE newproxy_queries_duration_seconds histogram
newproxy_queries_duration_seconds_bucket{le="0.001"} 30000
newproxy_queries_duration_seconds_bucket{le="0.01"} 38000
newproxy_queries_duration_seconds_bucket{le="1"} 38419
newproxy_queries_duration_seconds_bucket{le="+Inf"} 38420
"#;

    #[test]
    fn parses_new_metrics_fields() {
        let v = parse_proxy_metrics(SAMPLE);
        assert_eq!(v["connections_active"], 12);
        assert_eq!(v["queries_total"], 38420);
        assert_eq!(v["queries_slow"], 3);
        assert_eq!(v["queries_errors"], 1);
        assert_eq!(v["backend_errors"], 3);
        assert_eq!(v["pool_idle"], 5);
        assert!((v["cpu_percent"].as_f64().unwrap() - 8.2).abs() < 1e-9);
        // 38420 累计,p50≈10ms 桶(38420*0.5=19210 <30000? 否,30000>=19210→1ms?)
        assert_eq!(v["duration_count"], 38420);
        assert!(v["p50_us"].as_u64().unwrap() > 0);
        // p95 = 38420*0.95 ≈ 36499 → 桶 38000? 但 30000>=36499 否 → 38000桶(0.01s)=10000µs
        let p95 = v["p95_us"].as_u64().unwrap();
        assert!(p95 >= 10_000 && p95 <= 1_000_000, "p95={p95}");
    }
}

#[cfg(test)]
mod proxy_auth_parse {
    use super::*;
    #[test]
    fn b64_and_conf_creds() {
        assert_eq!(b64(b"admin:admin"), "YWRtaW46YWRtaW4=");
        let conf =
            "[MySQL_Proxy_Layer]\nport=4051\nmng_user=admin\nmng_password=admin\nlog_level=info\n";
        assert_eq!(conf_basic_auth(conf).as_deref(), Some("YWRtaW46YWRtaW4="));
        // 未配置 → None(免认证路径)
        assert_eq!(
            conf_basic_auth("[MySQL_Proxy_Layer]\nmng_port=9111\n"),
            None
        );
    }
}

/// GET /api/rds/monitor/proxies?instance= —— 并发代拉该实例全部 Proxy /metrics 并聚合
pub async fn monitor_proxies_batch(name: &str) -> Result<serde_json::Value, String> {
    use serde_json::json;
    let m = crate::manager();
    let Some(inst) = m.instances.get(name).map(|e| e.value().clone()) else {
        return Err(format!("实例 {name} 不存在"));
    };
    let proxies = px_list_owned(&inst);
    let mut js = tokio::task::JoinSet::new();
    for p in &proxies {
        let c = p.0.clone();
        js.spawn(async move {
            let parsed = proxy_metrics_text(&c)
                .await
                .ok()
                .map(|t| parse_proxy_metrics(&t));
            (c, parsed)
        });
    }
    let mut items: Vec<serde_json::Value> = Vec::new();
    while let Some(res) = js.join_next().await {
        if let Ok((container, parsed)) = res {
            let pinfo = proxies
                .iter()
                .find(|p| p.0 == container)
                .cloned()
                .unwrap_or_default();
            let mut it =
                json!({ "container": container, "mysql_port": pinfo.1, "mng_port": pinfo.2 });
            if let Some(v) = parsed {
                it["parsed"] = v;
            }
            items.push(it);
        }
    }
    // 聚合(数值类求和;p95 取已有样本最大值)
    let mut agg = json!({
        "connections_active": 0u64, "queries_total": 0u64, "queries_slow": 0u64,
        "queries_errors": 0u64, "backend_errors": 0u64, "pool_idle": 0u64,
        "cpu_avg": null, "p95_us": null, "proxy_count": items.len()
    });
    let mut cpu_sum = 0.0f64;
    let mut cpu_n = 0u32;
    let mut p95_max: Option<u64> = None;
    for it in &items {
        let p = &it["parsed"];
        if p.is_null() {
            continue;
        }
        macro_rules! add {
            ($k:literal, $to:literal) => {
                if let Some(v) = p[$k].as_u64() {
                    agg[$to] = json!(agg[$to].as_u64().unwrap_or(0) + v);
                }
            };
        }
        add!("connections_active", "connections_active");
        add!("queries_total", "queries_total");
        add!("queries_slow", "queries_slow");
        add!("queries_errors", "queries_errors");
        add!("backend_errors", "backend_errors");
        add!("pool_idle", "pool_idle");
        if let Some(c) = p["cpu_percent"].as_f64() {
            cpu_sum += c;
            cpu_n += 1;
        }
        if let Some(v) = p["p95_us"].as_u64() {
            p95_max = Some(p95_max.map_or(v, |m| m.max(v)));
        }
    }
    if cpu_n > 0 {
        agg["cpu_avg"] = json!(cpu_sum / cpu_n as f64);
    }
    if let Some(v) = p95_max {
        agg["p95_us"] = json!(v);
    }
    Ok(json!({ "ok": true, "instance": name, "items": items, "agg": agg }))
}

fn px_list_owned(i: &RdsInstance) -> Vec<(String, u16, u16)> {
    if !i.proxies.is_empty() {
        i.proxies
            .iter()
            .map(|p| (p.container.clone(), p.mysql_port, p.mng_port))
            .collect()
    } else if !i.proxy_container.is_empty() {
        vec![(
            i.proxy_container.clone(),
            i.proxy_mysql_port,
            i.proxy_mng_port,
        )]
    } else {
        Vec::new()
    }
}

#[cfg(test)]
mod host_model {
    // 物理机(Host)模型:执行面 seam + 绑定/解绑守卫 + 按 Host 端口分配
    // (见 docs/physical-multi-site-ops.md §5)
    use super::*;
    use crate::store::{MemoryBackend, Store};

    fn mem_mgr() -> Arc<RdsManager> {
        let store = Arc::new(Store::from_backend(MemoryBackend::new()));
        RdsManager::new(store)
    }

    fn inst(name: &str) -> RdsInstance {
        serde_json::from_value(serde_json::json!({
            "name": name,
            "status": "running",
            "network": format!("rds-{name}"),
            "nodes": [
                {"container": format!("rds-{name}-master"), "role": "master",
                 "host": format!("rds-{name}-master"), "port": 3306, "host_port": 35001, "server_id": 1},
                {"container": format!("rds-{name}-slave-1"), "role": "read",
                 "host": format!("rds-{name}-slave-1"), "port": 3306, "host_port": 35002, "server_id": 2},
            ],
            "proxy_container": "", "proxy_mysql_port": 0, "proxy_mng_port": 0,
            "created_at": 1, "root_password": "x", "last_error": "",
        }))
        .unwrap()
    }

    #[test]
    fn exec_seam_local_vs_remote_binding() {
        // 空绑定 = 本机直连;绑定宿主机 = remote(agent 未接入阶段,本机执行跳过/标记 remote 的依据)
        let i = inst("x");
        assert_eq!(node_host_binding(&i, "rds-x-master"), None);
        assert_eq!(
            host_exec_plan(node_host_binding(&i, "rds-x-master")),
            "local"
        );
        let mut i2 = i.clone();
        i2.node_hosts
            .insert("rds-x-slave-1".into(), "host-a".into());
        assert_eq!(node_host_binding(&i2, "rds-x-slave-1"), Some("host-a"));
        assert_eq!(
            host_exec_plan(node_host_binding(&i2, "rds-x-slave-1")),
            "remote"
        );
        // 未绑定节点不受影响
        assert_eq!(node_host_binding(&i2, "rds-x-master"), None);
        assert_eq!(
            host_exec_plan(node_host_binding(&i2, "rds-x-master")),
            "local"
        );
        // 解绑(清键)= 回到本机直连
        i2.node_hosts.remove("rds-x-slave-1");
        assert_eq!(node_host_binding(&i2, "rds-x-slave-1"), None);
    }

    #[test]
    fn assign_clear_guards_and_delete_protection() {
        let mgr = mem_mgr();
        mgr.instances.insert("x".into(), inst("x"));
        // 未登记 Host → 拒绝
        let e = mgr
            .host_assign_node("x", "rds-x-slave-1", "host-a")
            .expect_err("未登记 Host 应被拒");
        assert!(e.contains("未登记"), "{e}");
        // 登记 Host(agent_port=9191,agent 接入);节点不属于实例 → 拒绝
        mgr.host_create(
            "host-a", "10.0.0.1", "cn-bj", "az1", "rack-a", 16, 32, 1000, 9191,
        )
        .unwrap();
        let e = mgr
            .host_assign_node("x", "ghost", "host-a")
            .expect_err("节点不属于实例应被拒");
        assert!(e.contains("不属于"), "{e}");
        // 正常绑定 → 视图携带 node_hosts;审计落 host_assign
        mgr.host_assign_node("x", "rds-x-slave-1", "host-a")
            .unwrap();
        let v = mgr.get("x").unwrap();
        assert_eq!(v["node_hosts"]["rds-x-slave-1"], "host-a");
        assert!(!mgr.audit(50, None, Some("host_assign"), None).is_empty());
        // 绑定中的 Host 不可删除(防悬挂)
        let e = mgr
            .host_delete("host-a")
            .expect_err("被绑定 Host 删除应被拒");
        assert!(e.contains("仍被实例"), "{e}");
        // retiring Host 禁止新绑定
        mgr.store.host_upsert(
            "host-b", "10.0.0.2", "cn-bj", "az1", "rack-b", 16, 32, 1000, 0, "retiring",
        );
        let e = mgr
            .host_assign_node("x", "rds-x-master", "host-b")
            .expect_err("retiring Host 应被拒");
        assert!(e.contains("retiring"), "{e}");
        // 解绑后可删除 host-a;host-b(retiring,未绑定)也可删除 → 最终清空
        mgr.host_clear_node("x", "rds-x-slave-1").unwrap();
        mgr.host_delete("host-a").unwrap();
        mgr.host_delete("host-b").unwrap();
        assert!(mgr.hosts().is_empty());
    }

    #[test]
    fn alloc_port_per_host_watermark() {
        // 按 Host 端口高水位分配:连续自增(store 层语义,manager 透传)
        let mgr = mem_mgr();
        mgr.host_create(
            "host-c", "10.0.0.3", "cn-bj", "az1", "rack-c", 16, 32, 1000, 9192,
        )
        .unwrap();
        assert_eq!(mgr.alloc_port_for_host("host-c"), Some(35_001));
        assert_eq!(mgr.alloc_port_for_host("host-c"), Some(35_002));
        assert_eq!(mgr.alloc_port_for_host("ghost"), None);
    }

    #[test]
    fn replace_node_guards() {
        // 替换门槛:主节点/节点不存在/目标未登记/无 agent/retiring/已同主机 → 各自拒绝
        let mgr = mem_mgr();
        mgr.instances.insert("x".into(), inst("x"));
        // 主节点 → 拒绝(须先受管 PRS)
        let e = mgr
            .replace_node("x", "rds-x-master", "host-a")
            .expect_err("主节点替换应被拒");
        assert!(e.contains("PRS"), "{e}");
        // 节点不存在
        let e = mgr
            .replace_node("x", "ghost", "host-a")
            .expect_err("节点不存在应被拒");
        assert!(e.contains("不属于"), "{e}");
        // 目标未登记
        let e = mgr
            .replace_node("x", "rds-x-slave-1", "host-a")
            .expect_err("未登记 Host 应被拒");
        assert!(e.contains("未登记"), "{e}");
        // agent_port=0(未接入 agent)
        mgr.host_create(
            "host-no-agent",
            "10.0.0.9",
            "cn-bj",
            "az1",
            "rack-z",
            8,
            16,
            500,
            0,
        )
        .unwrap();
        let e = mgr
            .replace_node("x", "rds-x-slave-1", "host-no-agent")
            .expect_err("无 agent 应被拒");
        assert!(e.contains("agent"), "{e}");
        // retiring 目标拒绝
        mgr.store.host_upsert(
            "host-ret", "10.0.0.8", "cn-bj", "az1", "rack-y", 8, 16, 500, 9193, "retiring",
        );
        let e = mgr
            .replace_node("x", "rds-x-slave-1", "host-ret")
            .expect_err("retiring 应被拒");
        assert!(e.contains("retiring"), "{e}");
        // 已绑定同目标 → 拒绝
        mgr.host_create(
            "host-a", "10.0.0.1", "cn-bj", "az1", "rack-a", 16, 32, 1000, 9191,
        )
        .unwrap();
        mgr.host_assign_node("x", "rds-x-slave-1", "host-a")
            .unwrap();
        let e = mgr
            .replace_node("x", "rds-x-slave-1", "host-a")
            .expect_err("同主机应被拒");
        assert!(e.contains("已在宿主机"), "{e}");
        // 注:提交(调度执行)路径由真 drill(scripts/replace-drill.sh)覆盖(需 tokio + docker)
    }

    #[test]
    fn replace_commit_updates_binding_and_port() {
        // Commit 语义:绑定写入 node_hosts、节点 host_port/server_id 更新、审计留痕
        let mgr = mem_mgr();
        mgr.instances.insert("x".into(), inst("x"));
        let r = commit_replace_node(&mgr, "x", "rds-x-slave-1", "host-a", 35123, 44);
        assert!(r.is_ok(), "{r:?}");
        let v = mgr.get("x").unwrap();
        assert_eq!(v["node_hosts"]["rds-x-slave-1"], "host-a");
        let sl = v["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|n| n["container"] == "rds-x-slave-1")
            .unwrap();
        assert_eq!(sl["host_port"], 35123);
        assert_eq!(sl["server_id"], 44);
        assert!(!mgr.audit(50, None, Some("replace_node"), None).is_empty());
    }

    #[test]
    fn migrate_instance_guards() {
        // 迁移门槛:参数缺失/实例不存在/目标未登记/无 agent/retiring → 各自拒绝
        let mgr = mem_mgr();
        // 缺参
        let e = mgr
            .migrate_instance("x", "", "azN", "host-m")
            .expect_err("缺 region 应被拒");
        assert!(e.contains("region"), "{e}");
        let e = mgr
            .migrate_instance("x", "cn-east", "azN", "")
            .expect_err("缺 hosts 应被拒");
        assert!(e.contains("hosts"), "{e}");
        mgr.instances.insert("x".into(), inst("x"));
        // 目标未登记 / 无 agent / retiring
        let e = mgr
            .migrate_instance("x", "cn-east", "azN", "host-m")
            .expect_err("未登记应被拒");
        assert!(e.contains("未登记"), "{e}");
        mgr.host_create(
            "host-m", "10.0.0.7", "cn-east", "azN", "rack-e", 16, 32, 1000, 0,
        )
        .unwrap();
        let e = mgr
            .migrate_instance("x", "cn-east", "azN", "host-m")
            .expect_err("无 agent 应被拒");
        assert!(e.contains("agent"), "{e}");
        mgr.store.host_upsert(
            "host-m2", "10.0.0.6", "cn-east", "azN", "rack-e2", 16, 32, 1000, 0, "retiring",
        );
        let e = mgr
            .migrate_instance("x", "cn-east", "azN", "host-m2")
            .expect_err("retiring 应被拒");
        assert!(e.contains("retiring"), "{e}");
        // 注:提交(调度执行)路径由真 drill(scripts/migrate-drill.sh)覆盖(需 tokio + docker)
    }

    #[test]
    fn host_facts_and_ingress_in_view() {
        // P2-④/⑤:绑定后视图附挂 Host 事实(ip/region/az/rack/addr/exec_route)+ 接入点清单
        let mgr = mem_mgr();
        mgr.instances.insert("x".into(), inst("x"));
        mgr.host_create(
            "host-e", "10.9.8.7", "cn-east", "azE", "rack-9", 16, 32, 1000, 9199,
        )
        .unwrap();
        mgr.host_assign_node("x", "rds-x-slave-1", "host-e")
            .unwrap();
        let v = mgr.get("x").unwrap();
        let sl = v["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|n| n["container"] == "rds-x-slave-1")
            .unwrap();
        assert_eq!(sl["host_name"], "host-e");
        assert_eq!(sl["host_ip"], "10.9.8.7");
        assert_eq!(sl["host_az"], "azE");
        assert_eq!(sl["host_rack"], "rack-9");
        assert_eq!(sl["exec_route"], "agent");
        assert_eq!(sl["addr"], "10.9.8.7:35002"); // 绑定机 ip + 节点 host_port
        let master = v["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|n| n["role"] == "master")
            .unwrap();
        assert_eq!(master["exec_route"], "local"); // 未绑定 = 本机
        assert_eq!(master["addr"], "127.0.0.1:35001");
        // ingress 含绑定宿主机入口(节点清单),不含未绑定本机
        let ig = v["ingress"].as_array().unwrap();
        assert!(ig
            .iter()
            .any(|e| e["kind"] == "host" && e["host"] == "host-e" && e["ip"] == "10.9.8.7"));
        let host_entry = ig.iter().find(|e| e["kind"] == "host").unwrap();
        assert_eq!(host_entry["nodes"].as_array().unwrap().len(), 1);
        // 无绑定实例:仅 lvs 类入口或空(实例无 LVS 端口则无条目)
        let mgr2 = mem_mgr();
        mgr2.instances.insert("y".into(), inst("y"));
        assert!(mgr2.get("y").unwrap()["ingress"]
            .as_array()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn schedule_capacity_guard_blocks_overflow() {
        // P2-⑥:容量水位(mem/8 槽位估算)——超水位拒绝绑定;分布提示为建议(不阻断)
        let mgr = mem_mgr();
        mgr.instances.insert("x".into(), inst("x"));
        mgr.host_create(
            "host-small",
            "10.0.0.4",
            "cn-bj",
            "az1",
            "rack-s",
            8,
            8,
            500,
            9193,
        )
        .unwrap(); // 8GiB → 1 槽位
        mgr.host_assign_node("x", "rds-x-master", "host-small")
            .unwrap();
        let e = mgr
            .host_assign_node("x", "rds-x-slave-1", "host-small")
            .expect_err("超水位应被拒");
        assert!(e.contains("容量水位"), "{e}");
        // 分布提示:同机/同 rack/同 az 为建议(advisories),不阻断
        mgr.host_create(
            "host-ok", "10.0.0.5", "cn-bj", "az1", "rack-1", 32, 64, 2000, 9194,
        )
        .unwrap();
        mgr.host_assign_node("x", "rds-x-slave-1", "host-ok")
            .unwrap();
        let adv = mgr.schedule_advisories("x", "host-ok");
        // master 在 host-small(cn-bj/az1/rack-s),host-ok cn-bj/az1/rack-1:同 az 提示
        assert!(
            adv.iter().any(|s| s.contains("同 az")),
            "advisories: {adv:?}"
        );
    }
}

#[cfg(test)]
mod xenon_create {
    // xenon(raft 高可用)实例创建:参数校验 → DAG 形状 → 持久化/视图 → 复制语义操作拒绝
    // (设计见 docs/xenon-create-design.md;参考 xenon deploy/xenon.sh + gen-compose.sh)
    use super::*;
    use crate::store::{MemoryBackend, Store};

    fn mem_mgr() -> Arc<RdsManager> {
        let store = Arc::new(Store::from_backend(MemoryBackend::new()));
        RdsManager::new(store)
    }

    fn xenon_opts(name: &str, nodes: u32) -> CreateOpts {
        CreateOpts {
            itype: "xenon".into(),
            xenon_nodes: nodes,
            spec: "1C1G".into(),
            ..Default::default()
        }
        .tap_name(name)
    }

    // 小工具:CreateOpts 无 name 字段,create(name, opts) 传入;这里仅为可读性包装
    trait OptName {
        fn tap_name(self, _: &str) -> Self;
    }
    impl OptName for CreateOpts {
        fn tap_name(self, _n: &str) -> Self {
            self
        }
    }

    #[tokio::test]
    async fn create_validates_and_builds_dag() {
        let mgr = mem_mgr();
        // 非法节点数:0 → 视为默认 3;>9 → 拒绝
        let tid = mgr.create("xn1", &xenon_opts("xn1", 3)).expect("3 节点应可提交");
        assert!(tid.starts_with("t-create-"), "{tid}");
        let inst = mgr.instances.get("xn1").unwrap().clone();
        assert_eq!(inst.itype, "xenon");
        assert_eq!(inst.status, InstStatus::Creating);
        assert_eq!(inst.nodes.len(), 3, "默认 3 节点");
        assert_eq!(inst.proxies.len(), 0, "xenon 无 proxy");
        assert_eq!(inst.proxy_container, "", "xenon 无 proxy 容器");
        assert!(!inst.auto_failover, "raft 自带故障转移,管控 ERS 应关闭");
        // 角色与命名:节点1 = Master 种子,其余 Read;容器名 rds-{name}-xenon{i}
        assert_eq!(inst.nodes[0].container, "rds-xn1-xenon1");
        assert_eq!(inst.nodes[0].role, Role::Master);
        assert_eq!(inst.nodes[1].role, Role::Read);
        assert_eq!(inst.nodes[2].container, "rds-xn1-xenon3");
        // RPC 端口登记且与 MySQL 端口不同
        for n in &inst.nodes {
            assert!(n.rpc_host_port > 0, "xenon 节点应登记 rpc_host_port");
            assert_ne!(n.rpc_host_port, n.host_port);
        }
        // DAG 形状:net → node1..3(无相互依赖,可并发)→ verify → done(nodes 为 HashMap,按集合断言)
        let v = mgr.scheduler.get(&tid).unwrap();
        assert_eq!(v["kind"], "create");
        let ids: Vec<&str> = v["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n["id"].as_str().unwrap())
            .collect();
        let want: Vec<&str> = vec!["net", "node1", "node2", "node3", "verify", "done"];
        let mut got = ids.clone();
        got.sort();
        let mut want_sorted = want.clone();
        want_sorted.sort();
        assert_eq!(got, want_sorted);
        let node = |id: &str| {
            v["nodes"]
                .as_array()
                .unwrap()
                .iter()
                .find(|n| n["id"] == id)
                .unwrap()
                .clone()
        };
        let n1 = node("node1");
        assert_eq!(n1["deps"][0], "net");
        // node2/node3 不依赖 node1(并发拉起)
        assert!(!n1["deps"].as_array().unwrap().iter().any(|d| d == "node2"));
        let verify = node("verify");
        let vd: Vec<&str> = verify["deps"]
            .as_array()
            .unwrap()
            .iter()
            .map(|d| d.as_str().unwrap())
            .collect();
        assert_eq!(vd, vec!["node1", "node2", "node3"], "verify 应等全部节点");
        // 步骤内容:DockerRun(xenon 镜像) + WaitMysql;verify = VerifyXenon
        assert_eq!(n1["module"], "other", "节点模块徽章(容器+等待)");
        let done = node("done");
        assert_eq!(done["deps"][0], "verify");
    }

    #[tokio::test]
    async fn create_rejects_bad_node_count_and_shards() {
        let mgr = mem_mgr();
        let mut o9 = xenon_opts("xn9", 10);
        o9.xenon_nodes = 10;
        let e = mgr.create("xn9", &o9).expect_err(">9 节点应拒绝");
        assert!(e.contains("上限"), "{e}");
        let mut os = xenon_opts("xns", 3);
        os.shard_num = 2;
        let e = mgr.create("xns", &os).expect_err("xenon 多分片应拒绝");
        assert!(e.contains("不支持再分片"), "{e}");
        assert!(!mgr.instances.contains_key("xn9"), "失败后不应留下实例记录");
        assert!(!mgr.instances.contains_key("xns"));
    }

    #[tokio::test]
    async fn instance_view_and_serde_roundtrip() {
        // 视图:rpc_host_port 透出;serde:旧格式记录(无 rpc_host_port)加载为 0
        let mgr = mem_mgr();
        mgr.create("xn2", &xenon_opts("xn2", 3)).unwrap();
        let inst = mgr.instances.get("xn2").unwrap().clone();
        let view = inst.to_view();
        assert_eq!(view["nodes"][0]["rpc_host_port"].as_u64(), Some(inst.nodes[0].rpc_host_port as u64));
        // 旧记录(缺 rpc_host_port)serde default → 0,不破坏 load_persisted
        let old: RdsInstance = serde_json::from_str(
            &serde_json::json!({
                "name": "legacy", "status": "running", "network": "rds-legacy",
                "nodes": [{"container": "rds-legacy-master", "role": "master", "host": "rds-legacy-master",
                            "port": 3306, "host_port": 35001, "server_id": 1}],
                "proxy_container": "", "proxy_mysql_port": 0, "proxy_mng_port": 0,
                "created_at": 1, "root_password": "x", "last_error": ""
            })
            .to_string(),
        )
        .unwrap();
        assert_eq!(old.nodes[0].rpc_host_port, 0);
    }

    #[tokio::test]
    async fn replication_semantics_ops_rejected() {
        let mgr = mem_mgr();
        mgr.create("xn3", &xenon_opts("xn3", 3)).unwrap();
        // 直接置 running 模拟创建完成(跳过真实 docker)
        mgr.set_status("xn3", InstStatus::Running, "");
        // 等待 op_lock 释放(create watcher 因任务失败会解锁)
        for _ in 0..400 {
            if !mgr.op_locks.contains_key("xn3") {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        let e = mgr
            .scaleout("xn3", Role::Read, None, None)
            .expect_err("xenon 扩从应拒绝");
        assert!(e.contains("不支持从节点扩容"), "{e}");
        let e = mgr
            .replace_node("xn3", "rds-xn3-xenon2", "host-a")
            .expect_err("xenon 节点替换应拒绝");
        assert!(e.contains("不支持节点替换"), "{e}");
        let e = mgr
            .migrate_instance("xn3", "cn-sh", "az1", "h1")
            .expect_err("xenon 迁移应拒绝");
        assert!(e.contains("不支持跨机迁移"), "{e}");
        let e = mgr.run_backup("xn3").expect_err("xenon 备份应暂拒");
        assert!(e.contains("尚未接入"), "{e}");
        let e = mgr
            .dts_create("xn3", "rds-xn3-xenon2", None, None)
            .expect_err("xenon DTS 应拒绝");
        assert!(e.contains("不适用"), "{e}");
        let e = mgr
            .orch_reparent("xn3", None, "auto")
            .expect_err("xenon 切换应拒绝");
        assert!(e.contains("不支持管控面 orchestrator 切换"), "{e}");
    }

    #[tokio::test]
    async fn destroy_dag_for_xenon_uses_volume_aware_rm() {
        let mgr = mem_mgr();
        mgr.create("xn4", &xenon_opts("xn4", 3)).unwrap();
        mgr.set_status("xn4", InstStatus::Running, "");
        // 直接构造销毁 DAG 检查形状(不经真实 docker)
        let inst = mgr.instances.get("xn4").unwrap().clone();
        let nodes = destroy_nodes("xn4", &inst);
        let ids: Vec<&str> = nodes.iter().map(|n| n.id.as_str()).collect();
        assert_eq!(ids, vec!["raft_stop", "nodes_stop", "net_rm", "cleanup"]);
        let raft = &nodes[0];
        assert!(
            raft.steps.iter().any(|s| matches!(s, Step::DockerExec { args, .. } if args.contains(&"xenoncli".to_string()) && args.contains(&"raft".to_string()) && args.contains(&"stop".to_string()))),
            "应先优雅停 raft 集群"
        );
        let rm_steps: Vec<&Step> = nodes[1].steps.iter().collect();
        assert_eq!(rm_steps.len(), 3);
        // 容器删除走 DockerRm(执行器对 xenon 容器自动 -v 删卷)
        assert!(rm_steps.iter().all(|s| matches!(s, Step::DockerRm { .. })));
        assert!(nodes[1].steps.iter().all(|s| match s {
            Step::DockerRm { container } => container.starts_with("rds-xn4-xenon"),
            _ => false,
        }));
        // 对照组:主从架构销毁 DAG 仍走 lvs/proxy 分层
        let a = crate::instance::test_support::legacy_inst_for_destroy("lg");
        let legacy = destroy_nodes("lg", &a);
        assert!(legacy.iter().any(|n| n.id == "lvs_stop"), "主从路径不应被破坏");
    }

    #[tokio::test]
    async fn create_with_rsproxy_layer_and_consistency() {
        let mgr = mem_mgr();
        let mut o = xenon_opts("xnp", 3);
        o.xenon_proxy = 2;
        o.xenon_consistency = "causal".to_string();
        let tid = mgr.create("xnp", &o).expect("xenon+rsproxy 应可提交");
        let inst = mgr.instances.get("xnp").unwrap().clone();
        // 代理层登记:proxy×2 + LVS,首代理承载 legacy 字段
        assert_eq!(inst.proxies.len(), 2);
        assert_eq!(inst.proxy_container, "rds-xnp-proxy-1");
        assert!(inst.proxy_mysql_port > 0 && inst.proxy_mng_port > 0);
        assert_eq!(inst.lvs_container, "rds-xnp-lvs");
        assert!(inst.lvs_mysql_port > 0);
        assert_eq!(inst.proxy_version, "newproxy");
        // DAG 形状:verify → proxy_conf → proxy×2 → lvs → done
        // (task_definition 暴露 steps_json;scheduler.get 的 to_view 不含 steps)
        let v = mgr.store.task_definition(&tid).expect("任务定义应可见");
        let node = |id: &str| {
            v["nodes"]
                .as_array()
                .unwrap()
                .iter()
                .find(|n| n["node_id"] == id)
                .unwrap()
                .clone()
        };
        for id in ["proxy_conf", "proxy", "proxy2", "lvs"] {
            assert!(v["nodes"].as_array().unwrap().iter().any(|n| n["node_id"] == id), "缺少节点 {id}");
        }
        assert_eq!(node("proxy_conf")["deps"][0], "verify");
        assert_eq!(node("proxy")["deps"][0], "proxy_conf");
        assert_eq!(node("lvs")["deps"][0], "proxy2");
        assert_eq!(node("done")["deps"][0], "lvs");
        // 代理容器:同实例网络 + 挂载实例配置 + 双端口发布(Step 为 externally-tagged 序列化)
        let proxy_node = node("proxy");
        let prun = &proxy_node["steps"].as_array().unwrap()[0]["DockerRun"];
        let args = prun["args"]
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x.as_str().unwrap().to_string())
            .collect::<Vec<_>>();
        assert!(args.contains(&"--network".to_string()));
        assert!(args.iter().any(|a| a.contains(":/app/conf/newproxy.conf:rw")));
        assert!(args.iter().any(|a| a.ends_with(":4051")));
        assert!(args.iter().any(|a| a.ends_with(":9111")));
        // 销毁 DAG:代理层先摘除(lvs_stop → proxy_stop → raft_stop → nodes)
        mgr.set_status("xnp", InstStatus::Running, "");
        let inst = mgr.instances.get("xnp").unwrap().clone();
        let dnodes = destroy_nodes("xnp", &inst);
        let ids: Vec<&str> = dnodes.iter().map(|n| n.id.as_str()).collect();
        assert_eq!(
            ids,
            vec!["lvs_stop", "proxy_stop", "raft_stop", "nodes_stop", "net_rm", "cleanup"]
        );
        // 销毁代理容器含 2 个 rsproxy
        let pstop = &dnodes[1];
        assert_eq!(pstop.steps.len(), 2);
        assert!(pstop.steps.iter().all(|s| match s {
            Step::DockerRm { container } => container.starts_with("rds-xnp-proxy-"),
            _ => false,
        }));
    }

    #[tokio::test]
    async fn create_rejects_bad_consistency_and_proxy_cap() {
        let mgr = mem_mgr();
        let mut oc = xenon_opts("xnc", 3);
        oc.xenon_consistency = "linearizable".to_string();
        let e = mgr.create("xnc", &oc).expect_err("未知档位应拒绝");
        assert!(e.contains("strong/causal/session/eventual"), "{e}");
        let mut op = xenon_opts("xnp2", 3);
        op.xenon_proxy = 5;
        // 上限 4(clamp)→ 提交成功但只部署 4 个代理
        let tid = mgr.create("xnp2", &op).expect("代理数 >4 应回退到 4");
        let inst = mgr.instances.get("xnp2").unwrap().clone();
        assert_eq!(inst.proxies.len(), 4, "代理数应 clamp 到 4");
        let _ = tid;
    }

    #[test]
    fn xenon_proxy_config_shape() {
        let members = vec!["rds-f-xenon1".to_string(), "rds-f-xenon2".to_string(), "rds-f-xenon3".to_string()];
        let conf = xenon_proxy_config(&members, "causal", 2);
        // XenonRaft 段:成员映射与档位注入
        assert!(conf.contains("[XenonRaft_0_t0]"));
        assert!(conf.contains("members=rds-f-xenon1:3306,rds-f-xenon2:3306,rds-f-xenon3:3306"));
        assert!(conf.contains(&format!("raft_endpoints=rds-f-xenon1:{XENON_RPC_PORT},rds-f-xenon2:{XENON_RPC_PORT},rds-f-xenon3:{XENON_RPC_PORT}")));
        assert!(conf.contains("read_consistency=causal"));
        // 初始拓扑占位 = 前 2 个成员(首启后由 HA worker 按 raft 实况覆盖)
        assert!(conf.contains("host=rds-f-xenon1\n"));
        assert!(conf.contains("host=rds-f-xenon2\n"));
        // 统一管控凭据与默认库
        assert!(conf.contains(&format!("db_password={ROOT_PASS}")));
        assert!(conf.contains(&format!("default_db={APP_DB}")));
        // 服务端口与既有代理镜像约定一致
        assert!(conf.contains("port=4051"));
        assert!(conf.contains("mng_port=9111"));
        // strong 档(默认)同样可生成
        let strong = xenon_proxy_config(&members, "strong", 1);
        assert!(strong.contains("read_consistency=strong"));
    }
}

/// 测试支撑(仅 cfg(test) 可见)
#[cfg(test)]
pub(crate) mod test_support {
    use super::*;

    /// 构造一个传统主从(async)实例(用于销毁 DAG 对照)
    pub(crate) fn legacy_inst_for_destroy(name: &str) -> RdsInstance {
        serde_json::from_value(serde_json::json!({
            "name": name, "status": "running", "network": format!("rds-{name}"),
            "nodes": [
                {"container": format!("rds-{name}-master"), "role": "master",
                 "host": format!("rds-{name}-master"), "port": 3306, "host_port": 35001, "server_id": 1},
            ],
            "proxy_container": format!("rds-{name}-proxy"), "proxy_mysql_port": 35002, "proxy_mng_port": 35003,
            "lvs_container": format!("rds-{name}-lvs"), "lvs_mysql_port": 35004,
            "created_at": 1, "root_password": "x", "last_error": "",
        }))
        .unwrap()
    }
}
