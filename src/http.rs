// RDS 管控 — 管理 HTTP 服务(手写极简 HTTP/1.1,零依赖)
//
// 登录与会话(S-安全):
//   - 账号凭据存 MySQL(users 表,盐+salt:pass 哈希),不再依赖环境变量比对;
//   - 会话(内存)绑定用户与登录时权限快照;每次请求校验账号未冻结(即时生效);
//   - /api/rds/* 按路由所需权限鉴权,不足返回 403;审计经 crate::auth 记录真实操作人。
// 页面: /rds(登录页/管控页);默认种子管理员见 store::rbac_seed_default_admin。

use std::sync::Arc;

use crate::{api, auth};
use dashmap::DashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const LOGIN_HTML: &str = include_str!("login.html");
const RDS_HTML: &str = include_str!("rds.html");

const SESSION_TTL_HOURS: u64 = 8;
const SESSION_COOKIE: &str = "rdsctl_session";
const SESSION_COOKIE_EQ: &str = "rdsctl_session=";

#[derive(Clone)]
struct SessionInfo {
    user: String,
    perms: Vec<String>,
    at: std::time::Instant,
}

type SessionStore = Arc<DashMap<String, SessionInfo>>;

struct AuthGuard;
impl Drop for AuthGuard {
    fn drop(&mut self) {
        auth::clear();
    }
}

pub async fn serve(port: u16) -> std::io::Result<()> {
    let listener = TcpListener::bind(("0.0.0.0", port)).await?;
    let sessions: SessionStore = Arc::new(DashMap::new());
    loop {
        let (mut stream, _) = listener.accept().await?;
        let sessions = sessions.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(&mut stream, &sessions).await {
                tracing::debug!("conn error: {e}");
            }
        });
    }
}

/// 读取会话;过期自动清理
fn get_session(sessions: &SessionStore, cookie: Option<&str>) -> Option<(String, SessionInfo)> {
    let token = cookie
        .and_then(|c| c.split(';').find_map(|kv| kv.trim().strip_prefix(SESSION_COOKIE_EQ)))?
        .to_string();
    let info = sessions.get(&token).map(|e| e.value().clone())?;
    let now = std::time::Instant::now();
    if info.at + std::time::Duration::from_secs(SESSION_TTL_HOURS * 3600) <= now {
        sessions.remove(&token);
        return None;
    }
    Some((token, info))
}

/// 当前实现的路由所需权限(S-安全);未来新端点在此登记
fn perm_for(method: &str, path: &str) -> Option<&'static str> {
    if !path.starts_with("/api/rds/") {
        return None;
    }
    match (method, path) {
        ("GET", "/api/rds/instances")
        | ("GET", "/api/rds/instance")
        | ("GET", "/api/rds/summary") => Some("instances.view"),
        ("GET", "/api/rds/tasks") | ("GET", "/api/rds/task") => Some("tasks.view"),
        ("POST", "/api/rds/create") => Some("instances.create"),
        ("POST", "/api/rds/destroy") => Some("instances.destroy"),
        ("POST", "/api/rds/instance/delete") => Some("instances.destroy"),
        ("POST", "/api/rds/scaleout") => Some("instances.scaleout"),
        ("POST", "/api/rds/backup") => Some("instances.manage"),
        ("POST", "/api/rds/cancel") => Some("tasks.cancel"),
        ("POST", "/api/rds/retry") => Some("tasks.cancel"),
        ("POST", "/api/rds/meta") => Some("instances.manage"),
        ("POST", "/api/rds/task/draft") | ("POST", "/api/rds/task/start") | ("POST", "/api/rds/task/edit") | ("POST", "/api/rds/task/delete") => Some("tasks.manage"),
        ("GET", "/api/rds/modules") => Some("tasks.view"),
        ("POST", "/api/rds/module") | ("POST", "/api/rds/module/delete") | ("POST", "/api/rds/module/run") => Some("tasks.manage"),
        ("GET", "/api/rds/proxies") => Some("instances.view"),
        ("GET", "/api/rds/dts") => Some("instances.view"),
        ("GET", "/api/rds/hosts") => Some("instances.view"),
        ("POST", "/api/rds/hosts") | ("POST", "/api/rds/hosts/delete") | ("POST", "/api/rds/hosts/status") | ("POST", "/api/rds/host/assign") | ("POST", "/api/rds/host/clear") | ("POST", "/api/rds/replace_node") | ("POST", "/api/rds/migrate") => Some("instances.manage"),
        ("GET", "/api/rds/orch/facts") | ("GET", "/api/rds/orch/ops") => Some("instances.view"),
        ("POST", "/api/rds/orch/reparent") | ("POST", "/api/rds/orch/rollback") => Some("instances.manage"),
        ("POST", "/api/rds/dts/create") | ("POST", "/api/rds/dts/remove") => Some("instances.manage"),
        ("GET", "/api/rds/architectures") => Some("instances.view"),
        ("GET", "/api/rds/nodegroups") => Some("tasks.view"),
        ("POST", "/api/rds/proxy") => Some("instances.manage"),
        ("GET", "/api/rds/proxy/conf") => Some("instances.view"),
        ("POST", "/api/rds/proxy/conf") => Some("instances.manage"),
        ("GET", "/api/rds/monitor/dbs") => Some("instances.view"),
        ("GET", "/api/rds/proxy/metrics") => Some("instances.view"),
        ("GET", "/api/rds/monitor/proxies") => Some("instances.view"),
        ("GET", "/api/rds/audit") => Some("audit.view"),
        ("GET", "/api/rds/users") | ("POST", "/api/rds/users") => Some("users.manage"),
        ("GET", "/api/rds/permissions") | ("GET", "/api/rds/roles") | ("POST", "/api/rds/roles") => {
            Some("roles.manage")
        }
        ("POST", "/api/rds/enable") => Some("instances.manage"),
        ("POST", "/api/rds/batch") => Some("instances.manage"),
        ("GET", "/api/rds/alerts") => Some("alerts.view"),
        ("POST", "/api/rds/alert") => Some("alerts.handle"),
        ("GET", "/api/rds/alerts/groups") => Some("alerts.view"),
        ("POST", "/api/rds/alerts/gov") => Some("alerts.handle"),
        ("GET", "/api/rds/timeline") => Some("instances.view"),
        ("GET", "/api/rds/capacity") | ("GET", "/api/rds/capacity/forecast") => Some("instances.view"),
        ("GET", "/api/rds/ask") => Some("instances.view"),
        ("GET", "/api/rds/insights") => Some("instances.view"),
        ("GET", "/api/rds/report") => Some("audit.view"),
        ("POST", "/api/rds/report/run") => Some("audit.view"),
        ("GET", "/api/rds/reports") => Some("audit.view"),
        ("POST", "/api/rds/query") => Some("instances.query"),
        ("POST", "/api/rds/query/save") => Some("instances.query"),
        ("GET", "/api/rds/query/caps") => Some("instances.view"),
        ("GET", "/api/rds/schema") | ("GET", "/api/rds/schema/index") | ("GET", "/api/rds/schema/routines") => Some("instances.view"),
        ("GET", "/api/rds/slow") | ("GET", "/api/rds/slow/instance") => Some("instances.view"),
        ("GET", "/api/rds/slow/trend") => Some("instances.query"),
        ("GET", "/api/rds/slow/gov") => Some("instances.view"),
        ("GET", "/api/rds/slow/advice") => Some("instances.view"),
        ("POST", "/api/rds/slow/gov") => Some("tasks.manage"),
        ("POST", "/api/rds/slow/collect") => Some("instances.manage"),
        ("GET", "/api/rds/backup/reg") => Some("instances.view"),
        ("POST", "/api/rds/backup/reg/retry") => Some("instances.manage"),
        _ => None,
    }
}

/// 按 Content-Length 读取 POST body(首个缓冲区已含部分/全部 body 时复用)
async fn read_body_full(
    stream: &mut tokio::net::TcpStream,
    first: &[u8],
    content_length: usize,
) -> std::io::Result<String> {
    let head_end = first
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|p| p + 4)
        .unwrap_or(0);
    let mut body: Vec<u8> = Vec::with_capacity(content_length);
    if head_end < first.len() {
        let avail = (first.len() - head_end).min(content_length);
        body.extend_from_slice(&first[head_end..head_end + avail]);
    }
    while body.len() < content_length {
        let mut chunk = [0u8; 4096];
        let r = stream.read(&mut chunk).await?;
        if r == 0 {
            break;
        }
        let need = content_length - body.len();
        body.extend_from_slice(&chunk[..r.min(need)]);
    }
    Ok(String::from_utf8_lossy(&body).into_owned())
}

async fn handle(stream: &mut tokio::net::TcpStream, sessions: &SessionStore) -> std::io::Result<()> {
    let mut buf = [0u8; 4096];
    let n = stream.read(&mut buf).await?;
    if n == 0 {
        return Ok(());
    }
    let text = String::from_utf8_lossy(&buf[..n]);
    let mut lines = text.split("\r\n");
    let request_line = lines.next().unwrap_or("").to_string();

    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_uppercase();
    let target = parts.next().unwrap_or("/").to_string();
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (target.clone(), String::new()),
    };
    let cookie = lines
        .find(|l| l.to_ascii_lowercase().starts_with("cookie:"))
        .map(|l| l[7..].trim().to_string());

    // 请求体(仅 /api/rds/query 需要;按 Content-Length 分段读完)
    let header_text = text.split("\r\n\r\n").next().unwrap_or("");
    let content_length: usize = header_text
        .lines()
        .find(|l| l.to_ascii_lowercase().starts_with("content-length:"))
        .and_then(|l| l.split(':').nth(1))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0);
    let need_body = method == "POST"
        && (path == "/api/rds/query" || path == "/api/rds/query/save");
    let post_body = if need_body {
        read_body_full(stream, &buf[..n], content_length).await?
    } else {
        String::new()
    };

    async fn reply(stream: &mut tokio::net::TcpStream, status: u16, ctype: &str, body: &str, extra: Option<&str>) -> std::io::Result<()> {
        let resp = response(status, ctype, body.as_bytes(), extra);
        stream.write_all(&resp).await?;
        stream.flush().await?;
        Ok(())
    }

    // ─── 登录(凭据在 MySQL;会话绑定用户) ───
    if path == "/login" && method == "POST" {
        let body_text = String::from_utf8_lossy(&buf[..n]);
        let body = body_text.split("\r\n\r\n").nth(1).unwrap_or("");
        let params = format!("{query}&{body}");
        let user = qparam(&params, "user");
        let pass = qparam(&params, "password");
        if user.is_empty() || pass.is_empty() {
            return reply(stream, 400, "text/plain", "请输入用户名与密码", None).await;
        }
        let store = crate::manager().store.clone();
        match store.auth_effective(&user, &pass) {
            Some((true, perms)) => {
                let token = format!("{:x}", rand_token());
                sessions.insert(
                    token.clone(),
                    SessionInfo { user: user.clone(), perms, at: std::time::Instant::now() },
                );
                let cookie_hdr = format!(
                    "Set-Cookie: {SESSION_COOKIE}={token}; Path=/; HttpOnly; Max-Age={}",
                    SESSION_TTL_HOURS * 3600
                );
                reply(stream, 200, "text/plain", "ok", Some(&cookie_hdr)).await
            }
            Some((false, _)) => {
                reply(stream, 403, "text/plain", "账号已冻结/禁用,请联系管理员", None).await
            }
            None => {
                reply(stream, 401, "text/plain", "登录失败: 用户名或密码错误", None).await
            }
        }
    } else {
        // ─── 常规请求:会话 → 冻结校验 → 权限门禁 → 路由 ───
        let session = get_session(sessions, cookie.as_deref());
        if let Some((token, info)) = session {
            // 冻结即时生效:每次请求校验账号启用状态
            if !crate::manager().store.user_enabled(&info.user) {
                sessions.remove(&token);
                let body = r#"{"error":"账号已冻结,请联系管理员"}"#;
                return reply(stream, 403, "application/json", body, None).await;
            }
            auth::set(info.user.clone(), info.perms.clone());
            let _guard = AuthGuard;

            // 权限门禁(API 路由所需权限)
            if let Some(perm) = perm_for(&method, &path) {
                if !auth::has_perm(perm) {
                    let body = format!(r#"{{"ok":false,"error":"权限不足:需要 {perm}"}}"#);
                    return reply(stream, 403, "application/json", &body, None).await;
                }
            }

            let (status, ctype, body): (u16, &str, String) = match (method.as_str(), path.as_str()) {
                ("GET", "/") | ("GET", "/rds") => (200, "text/html; charset=utf-8", RDS_HTML.to_string()),
                ("GET", "/api/auth/me") => {
                    let me = crate::auth::Actor::snapshot_json();
                    (200, "application/json", me)
                }
                ("GET", "/api/rds/instances") => (200, "application/json", api::instances(&query)),
                ("GET", "/api/rds/instance") => api::instance(&query),
                ("GET", "/api/rds/tasks") => (200, "application/json", api::tasks(&query)),
                ("GET", "/api/rds/task") => api::task(&query),
                ("POST", "/api/rds/create") => api::create(&query),
                ("POST", "/api/rds/destroy") => api::destroy(&query),
                ("POST", "/api/rds/instance/delete") => api::delete_instance(&query),
                ("POST", "/api/rds/scaleout") => api::scaleout(&query),
                ("POST", "/api/rds/backup") => api::backup(&query),
                ("POST", "/api/rds/query") => api::query(&query, &post_body).await,
                ("POST", "/api/rds/query/save") => api::query_save(&query, &post_body),
                ("GET", "/api/rds/query/caps") => api::query_caps(&query),
                ("GET", "/api/rds/schema") => api::schema(&query).await,
                ("GET", "/api/rds/schema/index") => api::schema_index(&query).await,
                ("GET", "/api/rds/schema/routines") => api::schema_routines(&query).await,
                ("GET", "/api/rds/slow") => api::slow_top(&query),
                ("GET", "/api/rds/slow/instance") => api::slow_inst(&query),
                ("GET", "/api/rds/slow/trend") => api::slow_trend(&query),
                ("GET", "/api/rds/slow/gov") => api::slow_gov_list(&query),
                ("GET", "/api/rds/slow/advice") => api::slow_advice(&query),
                ("POST", "/api/rds/slow/gov") => api::slow_gov_action(&query),
                ("POST", "/api/rds/slow/collect") => api::slow_collect(&query).await,
                ("GET", "/api/rds/backup/reg") => api::backup_reg(&query),
                ("POST", "/api/rds/backup/reg/retry") => api::backup_reg_retry(&query),
                ("GET", "/api/rds/audit") => (200, "application/json", api::audit(&query)),
                ("GET", "/api/rds/summary") => (200, "application/json", api::summary()),
                ("POST", "/api/rds/cancel") => api::cancel_task(&query),
                ("POST", "/api/rds/retry") => api::retry_task(&query).await,
                ("POST", "/api/rds/meta") => api::meta(&query),
                ("POST", "/api/rds/task/draft") => api::draft_task(&query),
                ("POST", "/api/rds/task/start") => api::start_task(&query).await,
                ("POST", "/api/rds/task/edit") => api::edit_task(&query),
                ("POST", "/api/rds/task/delete") => api::delete_task(&query),
                ("GET", "/api/rds/modules") => (200, "application/json", api::modules()),
                ("POST", "/api/rds/module") => api::module_save(&query),
                ("POST", "/api/rds/module/delete") => api::module_delete(&query),
                ("POST", "/api/rds/module/run") => api::module_run(&query),
                ("GET", "/api/rds/proxies") => (200, "application/json", api::proxies()),
                ("GET", "/api/rds/dts") => api::dts_list(&query),
                ("GET", "/api/rds/hosts") => api::hosts(&query),
                ("POST", "/api/rds/hosts") => api::host_create(&query),
                ("POST", "/api/rds/hosts/delete") => api::host_delete(&query),
                ("POST", "/api/rds/hosts/status") => api::host_status(&query),
                ("POST", "/api/rds/host/assign") => api::host_assign(&query),
                ("POST", "/api/rds/host/clear") => api::host_clear(&query),
                ("POST", "/api/rds/replace_node") => api::replace_node(&query),
                ("POST", "/api/rds/migrate") => api::migrate(&query),
                ("GET", "/api/rds/orch/facts") => api::orch_facts(&query).await,
                ("GET", "/api/rds/orch/ops") => api::orch_ops(&query),
                ("POST", "/api/rds/orch/reparent") => api::orch_reparent(&query),
                ("POST", "/api/rds/orch/rollback") => api::orch_rollback(&query),
                ("POST", "/api/rds/dts/create") => api::dts_create(&query),
                ("POST", "/api/rds/dts/remove") => api::dts_remove(&query),
                ("GET", "/api/rds/architectures") => (200, "application/json", api::architectures()),
                ("GET", "/api/rds/nodegroups") => (200, "application/json", api::nodegroups()),
                ("POST", "/api/rds/proxy") => api::proxy_action(&query).await,
                ("GET", "/api/rds/proxy/conf") => api::proxy_conf_get(&query),
                ("POST", "/api/rds/proxy/conf") => api::proxy_conf_set(&query).await,
                ("GET", "/api/rds/monitor/dbs") => api::monitor_dbs(&query).await,
                ("GET", "/api/rds/monitor/proxies") => api::monitor_proxies(&query).await,
                ("GET", "/api/rds/proxy/metrics") => api::proxy_metrics(&query).await,
                ("GET", "/api/rds/users") => (200, "application/json", api::users()),
                ("POST", "/api/rds/users") => api::user_action(&query),
                ("GET", "/api/rds/roles") => (200, "application/json", api::roles()),
                ("POST", "/api/rds/roles") => api::role_action(&query),
                ("GET", "/api/rds/permissions") => (200, "application/json", api::permissions()),
                ("POST", "/api/rds/enable") => api::set_enabled(&query),
                ("POST", "/api/rds/batch") => api::batch(&query),
                ("GET", "/api/rds/alerts") => (200, "application/json", api::alerts(&query)),
                ("POST", "/api/rds/alert") => api::alert_action(&query),
                ("GET", "/api/rds/alerts/groups") => api::alert_groups(&query),
                ("POST", "/api/rds/alerts/gov") => api::alert_group_action(&query),
                ("GET", "/api/rds/timeline") => api::timeline(&query),
                ("GET", "/api/rds/capacity") => api::capacity(&query),
                ("GET", "/api/rds/ask") => api::ask(&query),
                ("GET", "/api/rds/capacity/forecast") => api::capacity_forecast(&query),
                ("GET", "/api/rds/insights") => (200, "application/json", api::insights(&query)),
                ("GET", "/api/rds/report") => api::report(&query),
                ("POST", "/api/rds/report/run") => api::report_run(&query),
                ("GET", "/api/rds/reports") => api::reports_history(&query),
                _ => (404, "text/plain", "not found".to_string()),
            };
            let resp = response(status, ctype, body.as_bytes(), None);
            stream.write_all(&resp).await?;
            stream.flush().await?;
            Ok(())
        } else {
            // 未登录:页面给登录页,其余 401
            if path == "/" || path == "/rds" {
                reply(stream, 200, "text/html; charset=utf-8", LOGIN_HTML, None).await
            } else {
                reply(stream, 401, "text/plain", "401 Unauthorized: 请先登录", None).await
            }
        }
    }
}

fn response(status: u16, ctype: &str, body: &[u8], extra_header: Option<&str>) -> Vec<u8> {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        _ => "Unknown",
    };
    let mut head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nCache-Control: no-store\r\n",
        body.len()
    );
    if let Some(h) = extra_header {
        head.push_str(h);
        head.push_str("\r\n");
    }
    head.push_str("Connection: close\r\n\r\n");
    let mut resp = head.into_bytes();
    resp.extend_from_slice(body);
    resp
}

fn qparam(query: &str, key: &str) -> String {
    query
        .split('&')
        .find_map(|kv| {
            let (k, v) = kv.split_once('=')?;
            if k == key {
                Some(url_decode(v))
            } else {
                None
            }
        })
        .unwrap_or_default()
}

fn url_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let h = (bytes[i + 1] as char).to_digit(16);
                let l = (bytes[i + 2] as char).to_digit(16);
                if let (Some(h), Some(l)) = (h, l) {
                    out.push((h * 16 + l) as u8);
                    i += 3;
                    continue;
                }
                out.push(bytes[i]);
                i += 1;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn rand_token() -> u128 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let pid = std::process::id() as u128;
    t ^ (pid << 40) ^ ((t & 0xFFFF) << 80)
}
