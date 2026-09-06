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
mod dag;
mod docker;
mod http;
mod insights;
mod instance;
mod lvs;
mod capacity;
mod orch;
mod query;
mod report;
mod sha256;
mod slow;
mod store;

use std::sync::OnceLock;
use std::sync::Arc;

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
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let mut port: u16 = 9113;
    let mut agent_mode = false;
    let mut raw: Vec<String> = std::env::args().skip(1).collect();
    // 子命令: `rdsctl agent [--port <端口>]` → 远端执行 agent(默认 9190)
    if let Some(first) = raw.first() {
        if first == "agent" {
            agent_mode = true;
            raw.remove(0);
            if raw.is_empty() {
                port = 9190;
            }
        }
    }
    let mut args = raw.into_iter();
    while let Some(a) = args.next() {
        match a.as_str() {
            "--port" | "-p" => {
                if let Some(v) = args.next() {
                    port = v.parse().unwrap_or(if agent_mode { 9190 } else { 9113 });
                }
            }
            "--help" | "-h" => {
                println!("rdsctl — RDS 管控服务(基于容器)");
                println!("用法: rdsctl [--port <端口>]             管控服务(默认 9113)");
                println!("      rdsctl agent [--port <端口>]       远端执行 agent(默认 9190)");
                println!("鉴权: env RDSCTL_AGENT_TOKEN 或 agent --token(agent 请求头 x-agent-token)");
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

    tracing::info!("rdsctl listening on 0.0.0.0:{port} (页面 /rds)");
    http::serve(port).await
}
