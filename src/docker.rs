// rdsctl — 容器编排执行**门面**(平台无关执行面的兼容入口)
//
// 历史:本文件曾是 docker CLI 的直接封装(自由函数)。
// 抽象化之后(见 docs/container-platform-abstraction.md),它变成一层门面:
//   **保留全部原有函数签名**,内部经 `exec::route_for_workload` 解析
//   「这个工作负载跑在哪个承载平台」,再调用 `WorkloadRuntime` 原语。
//
// 为什么保留门面:控制面内 60+ 处 `dk::*` 调用点零改动即获得平台无关性
//   (绑定远端物理机的节点自动走 agent;绑定平台的工作负载自动走对应 driver),
//   同时 docker 路径的行为与错误文案逐字不变(既有验收断言不改)。

use std::sync::Arc;

use crate::exec::spec::docker_args_to_spec;
use crate::exec::{self, ExecMeta, WorkloadRuntime};

/// 解析某工作负载的执行后端(绑定远端 → agent;未绑定 → 进程默认后端)
async fn rt_of(container: &str) -> Result<Arc<dyn WorkloadRuntime>, String> {
    exec::route_for_workload(container).map_err(|e| e.to_string())
}

/// 本机 docker CLI 裸透传(**已废弃**:不可跨平台)。
///
/// 平台无关路径请使用 `WorkloadRuntime` 原语;本函数仅保留给历史调用点。
#[allow(dead_code)]
pub async fn docker(args: &[&str]) -> Result<String, String> {
    exec::docker::DockerRuntime::from_env()
        .cli_raw(args)
        .await
        .map_err(|e| e.to_string())
}

/// 创建工作负载(返回容器名);同名残留先强制清除(节点重试/进程中断自愈)
pub async fn run(container: &str, args: &[&str]) -> Result<(), String> {
    let rt = rt_of(container).await?;
    let spec = docker_args_to_spec(container, args).map_err(|e| e.to_string())?;
    let meta = ExecMeta::none();
    rt.create(&spec, &meta).await.map_err(|e| e.to_string())
}

/// 停止(代理/节点停止)
pub async fn stop(container: &str) -> Result<(), String> {
    let rt = rt_of(container).await?;
    let meta = ExecMeta::none();
    rt.stop(container, &meta).await.map_err(|e| e.to_string())
}

/// 启动(代理/节点启动)
pub async fn start(container: &str) -> Result<(), String> {
    let rt = rt_of(container).await?;
    let meta = ExecMeta::none();
    rt.start(container, &meta).await.map_err(|e| e.to_string())
}

/// 重启(代理/节点重启类动作)
pub async fn restart(container: &str) -> Result<(), String> {
    let rt = rt_of(container).await?;
    let meta = ExecMeta::none();
    rt.restart(container, &meta).await.map_err(|e| e.to_string())
}

pub async fn rm(container: &str) -> Result<(), String> {
    let rt = rt_of(container).await?;
    let meta = ExecMeta::none();
    rt.remove(container, false, &meta)
        .await
        .map_err(|e| e.to_string())
}

/// 连同匿名/命名卷一并删除(xenon 节点销毁用:数据卷与 raft.meta 随容器清除,
/// 与 xenon deploy xenon.sh clean 的 down -v 语义一致)
pub async fn rm_v(container: &str) -> Result<(), String> {
    let rt = rt_of(container).await?;
    let meta = ExecMeta::none();
    rt.remove(container, true, &meta)
        .await
        .map_err(|e| e.to_string())
}

pub async fn exists(container: &str) -> bool {
    match rt_of(container).await {
        Ok(rt) => rt.exists(container).await,
        Err(_) => false,
    }
}

/// 容器状态事实(供 AI-0 异常快照):`Status|ExitCode|RestartCount`;容器缺失返回 None
pub async fn container_state(container: &str) -> Option<String> {
    rt_of(container).await.ok()?.state(container).await.map(|s| s.wire())
}

/// 容器是否健康:
/// - 有 healthcheck 的镜像 → Status == healthy
/// - 无 healthcheck(mysql:8.0 官方镜像)→ 运行中即视为健康(输出 "none")
pub async fn is_healthy(container: &str) -> bool {
    match rt_of(container).await {
        Ok(rt) => rt.is_healthy(container).await,
        Err(_) => false,
    }
}

/// 轮询容器健康,超时返回错误
#[allow(dead_code)] // 兼容 API(Step::WaitHealthy 已直接走执行面 trait)
pub async fn wait_healthy(container: &str, timeout_secs: u64) -> Result<(), String> {
    let rt = rt_of(container).await?;
    rt.wait_healthy(container, timeout_secs)
        .await
        .map_err(|e| e.to_string())
}

/// 等待容器内 MySQL 可接受连接(mysql:8.0 官方镜像无 healthcheck,
/// 容器"运行中"≠ MySQL 就绪,需用 SELECT 1 探测)
#[allow(dead_code)] // 兼容 API(Step::WaitMysql 已直接走执行面 trait)
pub async fn wait_mysql_ready(
    container: &str,
    user: &str,
    pass: &str,
    timeout_secs: u64,
) -> Result<(), String> {
    let rt = rt_of(container).await?;
    rt.wait_mysql_ready(container, user, pass, timeout_secs)
        .await
        .map_err(|e| e.to_string())
}

pub async fn logs_tail(container: &str, n: usize) -> Result<String, String> {
    let rt = rt_of(container).await?;
    rt.logs(container, n).await.map_err(|e| e.to_string())
}

/// 容器在指定网络内的 IP(docker 专属诊断事实)
#[allow(dead_code)] // 备用编排工具(未来 Step 可能使用)
pub async fn ip_in_network(container: &str, network: &str) -> Result<String, String> {
    exec::docker::DockerRuntime::from_env()
        .ip_in_network(container, network)
        .await
        .map_err(|e| e.to_string())
}

// ─── 网络 ───

#[allow(dead_code)] // 兼容 API(步骤已直接走执行面 trait)
pub async fn network_create(name: &str) -> Result<(), String> {
    exec::default_runtime()
        .network_ensure(name, &ExecMeta::none())
        .await
        .map_err(|e| e.to_string())
}

#[allow(dead_code)] // 兼容 API(步骤已直接走执行面 trait)
pub async fn network_rm(name: &str) -> Result<(), String> {
    exec::default_runtime()
        .network_remove(name, &ExecMeta::none())
        .await
        .map_err(|e| e.to_string())
}

// ─── 容器内执行 ───

/// 容器内执行 mysql 客户端:host/port 为容器内可达地址
#[allow(dead_code)] // 备用编排工具(未来 Step 可能使用)
pub async fn exec_mysql(
    container: &str,
    host: &str,
    port: u16,
    user: &str,
    pass: &str,
    sql: &str,
) -> Result<String, String> {
    let rt = rt_of(container).await?;
    let argv: Vec<String> = vec![
        "mysql".to_string(),
        "-h".to_string(),
        host.to_string(),
        "-P".to_string(),
        port.to_string(),
        "-u".to_string(),
        user.to_string(),
        format!("-p{pass}"),
        "-N".to_string(),
        "-e".to_string(),
        sql.to_string(),
    ];
    rt.exec(container, &argv).await.map_err(|e| e.to_string())
}

/// 容器内执行任意命令(docker exec,回显 stdout)——Step::DockerExec 的执行后端
pub async fn exec_in(container: &str, args: &[String]) -> Result<String, String> {
    let rt = rt_of(container).await?;
    rt.exec(container, args).await.map_err(|e| e.to_string())
}

/// 在 MySQL 容器内执行 SQL(统一 TCP 方式,无表头输出)。
/// --protocol=TCP:不依赖容器内 socket 路径(mysql:8.0 官方镜像 socket 在
/// /var/lib/mysql/mysql.sock,xenon 镜像在 /var/run/mysqld/mysqld.sock);
/// mysqld 始终监听 3306/TCP,统一走 TCP 对两类镜像均成立。
pub async fn exec_mysql_local(
    container: &str,
    user: &str,
    pass: &str,
    sql: &str,
) -> Result<String, String> {
    let rt = rt_of(container).await?;
    rt.exec_mysql_local(container, user, pass, sql)
        .await
        .map_err(|e| e.to_string())
}

/// 容器内批量查询(dba-console-design §5.3):`mysql --batch --column-names` 输出 TSV,
/// 首行为列名,值中的 \t \n \\ 由客户端转义(不传 --raw),便于结构化解析。
/// 超时双保险:容器内 `timeout -s KILL` 到期强杀 mysql(返回码 124 → 超时错误);
/// tokio 外层 timeout(secs+30s)兜底 docker CLI 悬挂。
pub async fn query_table(
    container: &str,
    user: &str,
    pass: &str,
    sql: &str,
    timeout_secs: u64,
    default_db: &str,
) -> Result<String, String> {
    let rt = rt_of(container).await?;
    rt.query_table(container, user, pass, sql, timeout_secs, default_db)
        .await
        .map_err(|e| e.to_string())
}

/// shell 单引号转义(用于把 SQL 安全地嵌入 sh -c 内作为单个 argv)
#[allow(dead_code)]
fn shell_sq(s: &str) -> String {
    exec::shell_sq(s)
}

/// 宿主机 mysql 客户端连接(经映射端口,用于验证代理连通性)
///
/// 这是**控制主机侧**操作(不经过工作负载执行后端),故不受平台抽象影响。
pub async fn host_mysql(port: u16, user: &str, pass: &str, sql: &str) -> Result<String, String> {
    let out = tokio::process::Command::new("mysql")
        .args([
            "-h",
            "127.0.0.1",
            "-P",
            &port.to_string(),
            "-u",
            user,
            &format!("-p{pass}"),
            "-N",
            "-e",
            sql,
        ])
        .output()
        .await
        .map_err(|e| format!("宿主机 mysql 客户端执行失败: {e}"))?;
    let stdout = String::from_utf8_lossy(&out.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
    if !out.status.success() {
        return Err(format!(
            "mysql :{port} 查询失败: {}",
            if stderr.is_empty() { stdout } else { stderr }
        ));
    }
    Ok(stdout)
}

/// 判断容器内某路径文件是否存在(docker exec test)
#[allow(dead_code)] // 备用编排工具(未来 Step 可能使用)
pub async fn exec_test(container: &str, path: &str) -> bool {
    match rt_of(container).await {
        Ok(rt) => {
            let argv = vec!["test".to_string(), "-e".to_string(), path.to_string()];
            rt.exec(container, &argv).await.is_ok()
        }
        Err(_) => false,
    }
}

/// 向容器写入文件(经 stdin 管道:docker exec -i sh -c 'cat > path')
#[allow(dead_code)] // 备用编排工具(未来 Step 可能使用)
pub async fn write_file(container: &str, path: &str, content: &str) -> Result<(), String> {
    let rt = rt_of(container).await?;
    rt.write_file(container, path, content)
        .await
        .map_err(|e| e.to_string())
}
