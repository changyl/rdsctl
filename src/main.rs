// RDS 管控 — 独立管控服务(基于容器的 MySQL 实例全生命周期管理)
//
// 架构:
//   dag.rs      DAG 任务调度器(节点/依赖/状态机/并发执行)
//   docker.rs   容器编排执行器(docker CLI 封装)
//   instance.rs RDS 实例模型 + create/destroy/scaleout 生命周期编排 + 健康巡检
//   store.rs    持久化层(本机 MySQL:任务/节点/审计/实例记录)
//   api.rs      RDS HTTP 接口
//   http.rs     管理 HTTP 服务(登录/会话/路由 + 静态页面)
//
// 启动: rdsctl [--port <端口>]   (默认 9113)
// 持久化:MySQL(env RDSCTL_MYSQL_HOST/PORT/USER/PASS/DB,默认 127.0.0.1:3306 root/rdsctl)

// 洞察/容量等视图组装含多层嵌套 json! 宏;抬高展开递归上限(仓库既有代码基线)
#![recursion_limit = "512"]

mod agent;
mod api;
mod ask;
mod auth;
mod backuplink;
mod capacity;
mod dag;
mod docker;
mod ha;
mod http;
mod insights;
mod instance;
mod lvs;
mod orch;
mod query;
mod report;
mod sha256;
mod slow;
mod store;

use std::sync::Arc;
use std::sync::OnceLock;

use instance::RdsManager;
use store::Store;

static MANAGER: OnceLock<Arc<RdsManager>> = OnceLock::new();

/// 全局 RDS 管理器单例(MySQL 持久化;启动恢复:默认中断任务标 failed,
/// 设 RDSCTL_RESUME_TASKS=1 时改为「续跑」——未终态任务重新入执行,已完成节点不重跑)
pub fn manager() -> &'static Arc<RdsManager> {
    MANAGER.get_or_init(|| {
        let store = Arc::new(Store::open());
        let resume = std::env::var("RDSCTL_RESUME_TASKS").as_deref() == Ok("1");
        if resume {
            // 续跑模式:不清孤儿锁、不改任务状态(未终态任务将重新入执行)
            tracing::info!("启动恢复模式:续跑(不清孤儿锁,未终态任务重新执行)");
        } else if ha::runtime::cluster_mode() {
            // cluster 模式:**禁止**这两个破坏性动作(设计 C5/I10):
            //   - mark_interrupted 会把其它副本正在跑的任务标 failed;
            //   - clear_all_locks 会删掉其它副本仍有效的租约(脑裂风险)。
            // 集群模式下租约由共识权威管理,随 TTL 自然过期,无需"清空"。
            tracing::info!(
                "cluster 模式:跳过 mark_interrupted/clear_all_locks(不破坏其它副本的租约与在跑任务)"
            );
        } else {
            let n = store.mark_interrupted();
            if n > 0 {
                tracing::warn!("检测到 {n} 个中断任务,已标记失败(设 RDSCTL_RESUME_TASKS=1 可续跑)");
            }
            // 单控制端 lab 模型:启动即清空宕机控制端的孤儿实例锁
            store.clear_all_locks();
        }
        let m = RdsManager::new(store);
        // 演示测试数据:env RDSCTL_DEMO_SEED=1 且实例库为空时种入(标签演示,不起容器)
        let demo = std::env::var("RDSCTL_DEMO_SEED").as_deref() == Ok("1");
        m.seed_demo_if_empty(demo);
        if resume {
            let n = m.scheduler.resume_pending();
            tracing::info!("启动恢复模式:续跑任务数 = {n}");
        }
        m.start_sweeper();
        crate::slow::start(&m);
        crate::backuplink::start(&m);
        crate::capacity::start(&m); // 容量采样(RDSCTL_CAP_SECS=0 时内部直接返回,零行为)
        crate::report::start(&m); // 自动报告(RDSCTL_REPORT_ENABLED=0 时内部直接返回,零行为)
        m
    })
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let mut port: u16 = 9113;
    let mut agent_mode = false;
    let mut cluster_mode = std::env::var("RDSCTL_MODE").as_deref() == Ok("cluster");
    let mut rpc_port: u16 = 9330;
    let mut node_id = std::env::var("RDSCTL_NODE_ID").unwrap_or_default();
    let mut cluster_spec = std::env::var("RDSCTL_CLUSTER").unwrap_or_default();
    let mut _shards = std::env::var("RDSCTL_SHARDS").unwrap_or_default();
    let mut roles = std::env::var("RDSCTL_ROLES")
        .unwrap_or_else(|_| "gateway,controller,ingress".to_string());
    let mut raw: Vec<String> = std::env::args().skip(1).collect();
    // 子命令:
    //   `rdsctl agent [--port <端口>]` → 远端执行 agent(默认 9190)
    //   `rdsctl serve [--node-id ... --cluster ... --rpc-port ...]` → 集群模式(M1a 骨架)
    if let Some(first) = raw.first() {
        if first == "agent" {
            agent_mode = true;
            raw.remove(0);
            if raw.is_empty() {
                port = 9190;
            }
        } else if first == "serve" {
            cluster_mode = true;
            raw.remove(0);
        } else if first == "admin" {
            // `rdsctl admin resync-sink`:重置 sink 投影游标 → 运行中的 leader 下一轮从日志
            // 幂等重放补齐(设计 §12/I18)。命令本身不写库,只改本地游标。
            raw.remove(0);
            let sub = raw.first().cloned().unwrap_or_default();
            if sub == "resync-sink" {
                let dir = std::env::var("RDSCTL_DATA_DIR")
                    .unwrap_or_else(|_| "./logs/ha".to_string());
                let shard: u16 = std::env::var("RDSCTL_SHARD_ID")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0);
                let mut proj = ha::projection::Projector::open(
                    std::path::Path::new(&dir),
                    shard,
                );
                let before = proj.cursor();
                match proj.reset(0) {
                    Ok(()) => {
                        println!(
                            "投影游标已重置:{before} → 0(data_dir={dir} shard={shard})"
                        );
                        println!(
                            "运行中的 leader 将在下一轮对账(≤5s)从日志重放并按幂等标记补齐;重复行不会产生。"
                        );
                    }
                    Err(e) => {
                        eprintln!("重置投影游标失败:{e}");
                        std::process::exit(1);
                    }
                }
                return Ok(());
            }
            eprintln!("未知 admin 子命令:{sub}(可用:resync-sink)");
            std::process::exit(3);
        }
    }
    let mut args = raw.into_iter();
    while let Some(a) = args.next() {
        let (k, inline) = match a.split_once('=') {
            Some((k, v)) => (k.to_string(), Some(v.to_string())),
            None => (a.clone(), None),
        };
        let take = |args: &mut std::vec::IntoIter<String>| inline.clone().or_else(|| args.next());
        match k.as_str() {
            "--port" | "-p" => {
                if let Some(v) = take(&mut args) {
                    port = v.parse().unwrap_or(if agent_mode { 9190 } else { 9113 });
                }
            }
            "--rpc-port" => {
                if let Some(v) = take(&mut args) {
                    rpc_port = v.parse().unwrap_or(9330);
                }
            }
            "--node-id" => {
                if let Some(v) = take(&mut args) {
                    node_id = v;
                }
            }
            "--cluster" => {
                if let Some(v) = take(&mut args) {
                    cluster_spec = v;
                }
            }
            "--roles" => {
                if let Some(v) = take(&mut args) {
                    roles = v;
                }
            }
            "--shards" => {
                if let Some(v) = take(&mut args) {
                    _shards = v;
                }
            }
            "--help" | "-h" => {
                println!("rdsctl — RDS 管控服务(基于容器)");
                println!("用法: rdsctl [--port <端口>]             管控服务(默认 9113,单机模式)");
                println!("      rdsctl agent [--port <端口>]       远端执行 agent(默认 9190)");
                println!("      rdsctl serve --node-id=N1 --cluster=N1@ip:9330,N2@ip:9330,N3@ip:9330");
                println!("                                     管控面集群模式(见 docs/control-plane-ha-design.md)");
                println!(
                    "鉴权: env RDSCTL_AGENT_TOKEN 或 agent --token(agent 请求头 x-agent-token)"
                );
                println!("页面: http://127.0.0.1:{port}/rds  (admin/admin)");
                return Ok(());
            }
            "--token" => {
                if let Some(v) = args.next() {
                    std::env::set_var("RDSCTL_AGENT_TOKEN", v);
                }
            }
            _ => {}
        }
    }

    if agent_mode {
        return agent::serve(port).await;
    }

    if cluster_mode {
        return serve_cluster(port, rpc_port, node_id, cluster_spec, roles).await;
    }

    tracing::info!("rdsctl listening on 0.0.0.0:{port} (页面 /rds;mode=single)");
    http::serve(port).await
}

/// 管控面集群模式(设计 §4/§7/§16 M1a 骨架)
///
/// 启动自检不通过 → **退出码 2**(与 deploy/bin/rdsctl-preflight.sh 的约定一致):
/// 拒绝在没有多数派仲裁/可信 fsync/可验证时钟/执行面 fence 的前提下"看起来高可用"。
async fn serve_cluster(
    port: u16,
    rpc_port: u16,
    node_id: String,
    cluster_spec: String,
    roles: String,
) -> std::io::Result<()> {
    use ha::raft::{Node, RaftConfig};
    use ha::runtime::{ClusterRuntime, MemberTable};

    if node_id.trim().is_empty() {
        eprintln!("cluster 模式必须指定 --node-id(或 RDSCTL_NODE_ID)");
        std::process::exit(2);
    }
    let table = match MemberTable::parse(&cluster_spec) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("集群成员表解析失败:{e}");
            std::process::exit(2);
        }
    };
    let shard: u16 = std::env::var("RDSCTL_SHARD_ID")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let mut cfg = RaftConfig::new(&node_id, shard, table.ids());
    if let Ok(v) = std::env::var("RDSCTL_MAX_SKEW_MS") {
        if let Ok(n) = v.parse::<u64>() {
            cfg.max_skew_ms = n;
        }
    }
    if let Ok(v) = std::env::var("RDSCTL_SNAPSHOT_ENTRIES") {
        if let Ok(n) = v.parse::<usize>() {
            cfg.snapshot_entry_threshold = n;
        }
    }
    if let Ok(v) = std::env::var("RDSCTL_ELECTION_TIMEOUT_MS") {
        if let Ok(n) = v.parse::<u64>() {
            cfg.election_timeout_ms = (n, n.saturating_mul(2));
        }
    }
    let data_dir = std::env::var("RDSCTL_DATA_DIR")
        .unwrap_or_else(|_| format!("./logs/ha/{node_id}"));
    let agent_url = std::env::var("RDSCTL_AGENT_URL").ok();
    let token = std::env::var("RDSCTL_CLUSTER_TOKEN").ok().filter(|s| !s.is_empty());

    // 自检先行:失败即拒绝启动(退出码 2)
    let check = ClusterRuntime::self_check(&cfg, std::path::Path::new(&data_dir), agent_url.as_deref());
    if !check.all_ok {
        let allow_clock =
            std::env::var("RDSCTL_PREFLIGHT_ALLOW_UNVERIFIED_CLOCK").as_deref() == Ok("1");
        let allow_agent = std::env::var("RDSCTL_ALLOW_NO_AGENT").as_deref() == Ok("1");
        let structural_ok = check.voters_ok && check.data_dir_ok && check.fsync_ok;
        let soft_ok = (check.clock_verified || allow_clock) && (check.agent_fence_ok || allow_agent);
        if !(structural_ok && soft_ok) {
            eprintln!("启动自检未通过,拒绝进入 cluster 模式(退出码 2):");
            for f in check.blocking_failures(allow_clock, allow_agent) {
                eprintln!("  - {f}");
            }
            // 已放行但未验证的前提单独提示(不算失败,但必须知情:readyz 会标注)
            let allowed: Vec<String> = check
                .failures()
                .into_iter()
                .filter(|f| !check.blocking_failures(allow_clock, allow_agent).contains(f))
                .collect();
            if !allowed.is_empty() {
                eprintln!("已由 lab 开关放行(未验证,readyz 会标注 premises_unverified):");
                for f in allowed {
                    eprintln!("  * {f}");
                }
            }
            eprintln!("详见 docs/control-plane-ha-design.md §1.3 与 deploy/README.md §6");
            std::process::exit(2);
        }
        tracing::warn!(
            "以 lab 降级模式启动(前提未全部满足,/readyz 将保持未就绪):{}",
            check.failures().join("; ")
        );
    }

    let rt = match ClusterRuntime::start(
        cfg,
        table,
        std::path::Path::new(&data_dir),
        agent_url.as_deref(),
        token,
    ) {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("cluster 启动失败:{e}");
            std::process::exit(2);
        }
    };
    tracing::info!(
        "cluster 模式启动:node_id={} roles={} data_dir={} rpc_port={}",
        node_id,
        roles,
        data_dir,
        rpc_port
    );
    let tick = std::env::var("RDSCTL_HA_TICK_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50);
    rt.spawn_background(tick);

    // 注册进程级句柄:此后 RdsManager 的实例互斥权威 = 共识租约(设计 D2)
    if !ha::runtime::set_global(std::sync::Arc::clone(&rt)) {
        tracing::warn!("集群运行时已注册(重复注册被忽略)");
    }

    let rpc = std::sync::Arc::clone(&rt);
    tokio::spawn(async move {
        if let Err(e) = rpc.serve_rpc(rpc_port).await {
            tracing::error!("RPC 服务退出:{e}");
        }
    });
    let _ = std::mem::size_of::<Node>();

    // sink(元数据库)可用性:sink 不在正确性路径(设计 C9/I17)。
    // 可用 → 起管理器(实例生命周期 API 完整可用,sink 只作投影/持久化);
    // 不可用 → 只提供探针与内部 RPC,明确降级而**不**影响共识与租约。
    let sink = std::env::var("RDSCTL_METADATA_SINK").unwrap_or_else(|_| "mysql".into());
    if sink == "none" {
        tracing::warn!("RDSCTL_METADATA_SINK=none:不起管理器,仅提供探针与内部 RPC");
        return rt.serve_public(port).await;
    }
    let mysql_host = std::env::var("RDSCTL_MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let mysql_port: u16 = std::env::var("RDSCTL_MYSQL_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3306);
    if std::net::TcpStream::connect((mysql_host.as_str(), mysql_port)).is_err() {
        tracing::error!(
            "sink({mysql_host}:{mysql_port})不可达:按设计降级为仅探针模式(共识/租约不受影响;实例生命周期 API 暂不可用)"
        );
        return rt.serve_public(port).await;
    }
    tracing::info!("sink 可达:启动管理器(实例互斥权威 = 共识租约)");
    let _ = crate::manager();
    // RBAC 引导灌入:**尽早**完成(否则窗口期内谁都登录不了,而登录失败会被误读成"密码错")。
    // leader-only、快速重试、幂等;会话不迁移(切模式必须重新登录,设计 §15)。
    {
        let rt = std::sync::Arc::clone(&rt);
        tokio::spawn(async move {
            loop {
                if rt.auth_hydrated() {
                    return;
                }
                if rt.is_leader() {
                    let m = crate::manager();
                    let users = m.store.users_raw();
                    let roles = m.store.roles_list();
                    match rt.auth_hydrate_from(&users, &roles).await {
                        Ok(n) => {
                            tracing::info!(
                                "RBAC 引导灌入完成({n} 条:sink → 共识状态机);会话不迁移,所有人需重新登录"
                            );
                            return;
                        }
                        Err(e) => tracing::debug!("RBAC 引导灌入待重试:{e}"),
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            }
        });
    }
    // 集群对账循环(每 5s):
    //   ① 所有副本从 sink 回灌实例视图(避免副本间长期不一致);
    //   ② 所有副本回收"自己持有但已过期、且本进程未在操作"的实例租约(残留自愈,见发现 15);
    //   ③ **仅 leader** 接管未完成任务(崩溃/换主后把中断任务续跑起来;设计 D5 串行化);
    //   ④ **仅 leader** 按确定性 cutoff 回收过期租约(持有者已永久离场的残留)。
    {
        let rt = std::sync::Arc::clone(&rt);
        let proj_dir = std::path::PathBuf::from(&data_dir);
        tokio::spawn(async move {
            let mut proj = ha::projection::Projector::open(&proj_dir, shard);
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                let m = crate::manager();
                let refreshed = m.refresh_from_sink();
                if refreshed > 0 {
                    tracing::debug!("从 sink 回灌 {refreshed} 个实例视图");
                }
                // ② 自己持有的过期残留 → 主动释放(每个副本只能释放自己持有的条目)
                let reaped = m.reap_own_expired_leases().await;
                let _ = reaped;
                if rt.is_leader() {
                    // ④ 过期租约 GC:确定性 cutoff,且没有可回收项时不写日志
                    match rt.purge_expired_leases().await {
                        Ok((0, _)) => {}
                        Ok((n, cutoff)) => tracing::info!(
                            "leader 回收过期租约 {n} 条(cutoff_ms={cutoff})"
                        ),
                        Err(e) => tracing::debug!("过期租约回收跳过:{e}"),
                    }
                    // ① 未完成任务接管
                    let n = m.scheduler.resume_pending();
                    if n > 0 {
                        tracing::info!("leader 接管 {n} 个未完成任务");
                    }
                    // ② sink 投影:日志里的决策 → 审计表(幂等标记 proj:<shard>:<index>)
                    let rows = rt.with_log(|log, applied| proj.pending(log, applied, 500));
                    let mut last = None;
                    for row in &rows {
                        let mk = ha::projection::marker(row.shard, row.index);
                        if !m.store.audit_marker_exists(&mk) {
                            m.store.audit(
                                &row.who,
                                &row.instance,
                                &row.action,
                                &row.params,
                                &row.result,
                                &row.task_id,
                            );
                        }
                        last = Some(row.index);
                    }
                    if let Some(upto) = last {
                        if let Err(e) = proj.commit(upto) {
                            tracing::warn!("投影游标写入失败:{e}");
                        } else {
                            tracing::debug!("sink 投影推进至 index={upto}");
                        }
                    }
                }
            }
        });
    }
    tracing::info!("cluster 模式完整 API listening on 0.0.0.0:{port}");
    http::serve(port).await
}
