// RDS 管控 —— 容器编排执行器
//
// 基于 docker CLI(tokio::process)执行容器生命周期操作,零外部依赖:
// run / rm / inspect / network / exec / cp / logs。
// 所有函数返回 Result<String, String>(错误含 docker stderr)。

use std::process::Stdio;

/// 执行 docker 命令,成功返回 stdout(去尾部空白),失败返回错误(含 stderr)
pub async fn docker(args: &[&str]) -> Result<String, String> {
    let out = tokio::process::Command::new("docker")
        .args(args)
        .stdin(Stdio::null())
        .output()
        .await
        .map_err(|e| format!("docker 执行失败: {e}"))?;
    let stdout = String::from_utf8_lossy(&out.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
    if !out.status.success() {
        return Err(format!("docker {} 失败: {}", args.join(" "), if stderr.is_empty() { stdout } else { stderr }));
    }
    Ok(stdout)
}

/// docker run(返回容器名);同名残留容器先强制清除(节点重试/进程中断自愈)
pub async fn run(container: &str, args: &[&str]) -> Result<(), String> {
    if exists(container).await {
        let _ = rm(container).await;
    }
    let mut cmd = vec!["run", "-d", "--name", container];
    cmd.extend_from_slice(args);
    docker(&cmd).await?;
    Ok(())
}

/// docker stop(代理/节点停止)
pub async fn stop(container: &str) -> Result<(), String> {
    docker(&["stop", container]).await?;
    Ok(())
}

/// docker start(代理/节点启动)
pub async fn start(container: &str) -> Result<(), String> {
    docker(&["start", container]).await?;
    Ok(())
}

/// docker restart(代理/节点重启类动作)
pub async fn restart(container: &str) -> Result<(), String> {
    docker(&["restart", container]).await?;
    Ok(())
}

pub async fn rm(container: &str) -> Result<(), String> {
    // -f 强制;不存在则忽略
    docker(&["rm", "-f", container]).await?;
    Ok(())
}

pub async fn exists(container: &str) -> bool {
    docker(&["inspect", container]).await.is_ok()
}

/// 容器状态事实(供 AI-0 异常快照):`Status|ExitCode|RestartCount`;容器缺失返回 None
pub async fn container_state(container: &str) -> Option<String> {
    docker(&[
        "inspect",
        "-f",
        "{{.State.Status}}|{{.State.ExitCode}}|{{.RestartCount}}",
        container,
    ])
    .await
    .ok()
}

/// 容器是否健康:
/// - 有 healthcheck 的镜像 → Status == healthy
/// - 无 healthcheck(mysql:8.0 官方镜像)→ 运行中即视为健康(输出 "none")
pub async fn is_healthy(container: &str) -> bool {
    let Ok(s) = docker(&[
        "inspect",
        "-f",
        "{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}",
        container,
    ])
    .await
    else {
        return false;
    };
    matches!(s.as_str(), "healthy" | "none")
}

/// 轮询容器健康,超时返回错误
pub async fn wait_healthy(container: &str, timeout_secs: u64) -> Result<(), String> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
    while std::time::Instant::now() < deadline {
        if is_healthy(container).await {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    let logs = logs_tail(container, 30).await.unwrap_or_default();
    Err(format!("容器 {container} 未在 {timeout_secs}s 内就绪\n最近日志:\n{logs}"))
}

/// 等待容器内 MySQL 可接受连接(mysql:8.0 官方镜像无 healthcheck,
/// 容器"运行中"≠ MySQL 就绪,需用 SELECT 1 探测)
pub async fn wait_mysql_ready(container: &str, user: &str, pass: &str, timeout_secs: u64) -> Result<(), String> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
    while std::time::Instant::now() < deadline {
        if exec_mysql_local(container, user, pass, "SELECT 1").await.is_ok() {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(800)).await;
    }
    let logs = logs_tail(container, 30).await.unwrap_or_default();
    Err(format!("容器 {container} 内 MySQL 未在 {timeout_secs}s 内就绪\n最近日志:\n{logs}"))
}

pub async fn logs_tail(container: &str, n: usize) -> Result<String, String> {
    docker(&["logs", "--tail", &n.to_string(), container]).await
}

/// 容器在指定网络内的 IP
#[allow(dead_code)] // 备用编排工具(未来 Step 可能使用)
pub async fn ip_in_network(container: &str, network: &str) -> Result<String, String> {
    let s = docker(&[
        "inspect",
        "-f",
        &format!("{{{{.NetworkSettings.Networks.{network}.IPAddress}}}}"),
        container,
    ])
    .await?;
    if s.is_empty() || s == "<no value>" {
        return Err(format!("容器 {container} 不在网络 {network} 内"));
    }
    Ok(s)
}

// ─── 网络 ───

pub async fn network_create(name: &str) -> Result<(), String> {
    // 已存在则跳过(任务重试/进程中断残留自愈)
    if docker(&["network", "inspect", name]).await.is_ok() {
        return Ok(());
    }
    docker(&["network", "create", name]).await?;
    Ok(())
}

pub async fn network_rm(name: &str) -> Result<(), String> {
    // 不存在则跳过(进程中断残留清理的幂等)
    if docker(&["network", "inspect", name]).await.is_ok() {
        docker(&["network", "rm", name]).await?;
    }
    Ok(())
}

// ─── 容器内执行 ───

/// docker exec 容器执行 mysql 客户端:host/port 为容器内可达地址
#[allow(dead_code)] // 备用编排工具(未来 Step 可能使用)
pub async fn exec_mysql(container: &str, host: &str, port: u16, user: &str, pass: &str, sql: &str) -> Result<String, String> {
    docker(&[
        "exec",
        container,
        "mysql",
        "-h", host,
        "-P", &port.to_string(),
        "-u", user,
        &format!("-p{pass}"),
        "-N",
        "-e", sql,
    ])
    .await
}

/// 容器内执行任意命令(docker exec,回显 stdout)——Step::DockerExec 的执行后端
pub async fn exec_in(container: &str, args: &[String]) -> Result<String, String> {
    let mut cmd: Vec<&str> = vec!["exec", container];
    cmd.extend(args.iter().map(|s| s.as_str()));
    docker(&cmd).await
}

/// 在 MySQL 容器内执行 SQL(本机 socket 方式,无表头输出)
pub async fn exec_mysql_local(container: &str, user: &str, pass: &str, sql: &str) -> Result<String, String> {
    docker(&[
        "exec",
        container,
        "mysql",
        "-N",
        "-u", user,
        &format!("-p{pass}"),
        "-e", sql,
    ])
    .await
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
    let secs = timeout_secs.max(1);
    let sq = shell_sq(sql);
    // 默认库(修复 "No database selected"):仅当是合法标识符时注入 -D,否则忽略
    let db_flag = if !default_db.is_empty()
        && default_db.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
    {
        format!(" -D {default_db}")
    } else {
        String::new()
    };
    // 用户/口令为受控值(固定账号名/hex 口令/root),不做 shell 引号处理;
    // SQL 需完整经单引号转义后作为 mysql -e 的单个 argv。
    let cmd = format!(
        "timeout -s KILL {secs} mysql --batch --column-names --default-character-set=utf8mb4 \
         --connect-timeout=5 -u {user} -p{pass}{db_flag} -e {sq}"
    );
    let fut = tokio::process::Command::new("docker")
        .args(["exec", container, "sh", "-c", cmd.as_str()])
        .output();
    match tokio::time::timeout(std::time::Duration::from_secs(secs + 30), fut).await {
        Ok(Ok(out)) => {
            let stdout = String::from_utf8_lossy(&out.stdout).trim().to_string();
            let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
            if out.status.success() {
                Ok(stdout)
            } else if out.status.code() == Some(124) {
                Err("查询超时,已终止".to_string())
            } else {
                Err(if stderr.is_empty() { stdout } else { stderr })
            }
        }
        Ok(Err(e)) => Err(format!("查询进程执行失败: {e}")),
        Err(_) => Err("查询超时,已终止".to_string()),
    }
}

/// shell 单引号转义(用于把 SQL 安全地嵌入 sh -c 内作为单个 argv)
fn shell_sq(s: &str) -> String {
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


/// 宿主机 mysql 客户端连接(经映射端口,用于验证代理连通性)
pub async fn host_mysql(port: u16, user: &str, pass: &str, sql: &str) -> Result<String, String> {
    let out = tokio::process::Command::new("mysql")
        .args([
            "-h", "127.0.0.1",
            "-P", &port.to_string(),
            "-u", user,
            &format!("-p{pass}"),
            "-N", "-e", sql,
        ])
        .output()
        .await
        .map_err(|e| format!("宿主机 mysql 客户端执行失败: {e}"))?;
    let stdout = String::from_utf8_lossy(&out.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
    if !out.status.success() {
        return Err(format!("mysql :{port} 查询失败: {}", if stderr.is_empty() { stdout } else { stderr }));
    }
    Ok(stdout)
}

/// 判断容器内某路径文件是否存在(docker exec test)
#[allow(dead_code)] // 备用编排工具(未来 Step 可能使用)
pub async fn exec_test(container: &str, path: &str) -> bool {
    docker(&["exec", container, "test", "-e", path]).await.is_ok()
}

/// 向容器写入文件(docker cp stdin → /dev/stdin 不可用,用 exec tee 方案:
/// 配置类小文件直接经 sh -c 'cat > path' 写入)
#[allow(dead_code)] // 备用编排工具(未来 Step 可能使用)
pub async fn write_file(container: &str, path: &str, content: &str) -> Result<(), String> {
    // 经 stdin 管道写入:docker exec -i container sh -c 'cat > path'
    let mut child = tokio::process::Command::new("docker")
        .args(["exec", "-i", container, "sh", "-c", &format!("cat > {path}")])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("docker exec spawn 失败: {e}"))?;
    use tokio::io::AsyncWriteExt;
    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(content.as_bytes()).await.map_err(|e| format!("写入 stdin 失败: {e}"))?;
        stdin.shutdown().await.map_err(|e| format!("关闭 stdin 失败: {e}"))?;
    }
    let out = child.wait_with_output().await.map_err(|e| format!("等待失败: {e}"))?;
    if !out.status.success() {
        return Err(format!("写入 {path} 失败: {}", String::from_utf8_lossy(&out.stderr)));
    }
    Ok(())
}
