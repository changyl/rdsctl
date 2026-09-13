// rdsctl — 会话与 RBAC 的**共识权威**表示(设计 §11.4 / 不变量 C8;验收 I15/I16)
//
// 为什么需要它:单机模式下"登录态 + 用户/角色"在 MySQL 的一张表里,进程内 `DashMap` 存会话;
// 多副本下每个副本各有自己的进程内会话(`src/http.rs` 的 SessionStore),于是
//   · 同一 cookie 换一个副本 → 401(负载均衡后面随机掉登录);
//   · 冻结/改密/改权限只在处理该请求的副本上生效。
// M1c 把这三类数据搬进**共识状态机**:任一网关副本都能服务任一 cookie,撤销/改权对所有副本生效。
//
// 键空间(沿用设计 §6 的 `s/ u/ r/` 前缀,占用为保留空间,不与业务 KV 混用):
//   `r/<role>`        → { description, perms[], ver }
//   `u/<user>`        → { salt, pass_hash, enabled, roles[], epoch, ver }
//   `s/<token_hash>`  → { user, epoch, issued_ms, expire_at_ms }
//   `a/hydrated`      → { at_ms }  首次从 sink 灌入 RBAC 的幂等标记
//
// 两个刻意的设计决定:
//
// 1. **物理写入复用通用 `Put{key, value, expect_ver}` / `Delete{key, expect_ver}`**,
//    不新增 op 变体。设计 §6 的物化操作名(SessionPut/UserUpsert/RolePermsSet…)正是
//    "key+ver" 语义的**逻辑**动作;复用通用 op 让(=零新 op、快照/日志格式零变化、
//    直接拿到 CAS 防丢更新)远大于(=日志里少了语义标签,但审计由 AuditAppend 另行留痕)的代价。
//
// 2. **用 `epoch` 而不是"扫描并删除该用户全部会话"来实现冻结/改密的即时失效**:
//    会话记录里带签发时的 `u/<user>.epoch`,校验要求 `s.epoch == u.epoch`。
//    改密/冻结 = 一次 `Put` 把 epoch +1 → 该用户所有会话一次性失效,原子、无需扫描、
//    也不需要"SessionRevokeUser"这种会随会话数线性增长的 op。
//
// 口令校验:与既有实现完全一致(sha256 十六进制(salt + ":" + pass),salt 为随机十六进制),
// 因此从 sink 灌入的既有口令记录可原样使用,不需要任何迁移或重设。

use std::collections::BTreeMap;

use serde_json::{json, Value};

/// 认证/RBAC 操作失败分类(映射到 HTTP 与前端提示)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthError {
    /// 用户名或口令不匹配
    BadCredentials,
    /// 账号被冻结/禁用
    Disabled,
    /// 认证数据尚未就绪(首次引导灌入未完成)。
    /// **不能**用 BadCredentials 代替:那等于对一个还没加载完的库说"你密码错了"。
    NotReady,
    /// 本副本认证状态未追平(读被拒,fail-closed)
    Lagged { applied: u64, commit: u64 },
    /// 无多数派
    Quorum,
    /// 其它内部错误(含 CAS 重试耗尽)
    Internal(String),
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthError::BadCredentials => write!(f, "用户名或口令错误"),
            AuthError::Disabled => write!(f, "账号已冻结/禁用,请联系管理员"),
            AuthError::NotReady => write!(
                f,
                "认证数据尚未就绪(首次引导灌入未完成):请稍后重试;若持续如此请检查 sink 可达性"
            ),
            AuthError::Lagged { applied, commit } => write!(
                f,
                "本副本认证状态未追平(applied_index={applied} < commit_index={commit}):已拒绝本次认证读,请重试"
            ),
            AuthError::Quorum => write!(f, "失去多数派,无法完成认证写操作"),
            AuthError::Internal(m) => write!(f, "{m}"),
        }
    }
}

impl std::error::Error for AuthError {}

/// 保留键前缀(不与业务 KV 混用)
pub const K_ROLE: &str = "r/";
pub const K_USER: &str = "u/";
pub const K_SESSION: &str = "s/";
/// 首次从 sink 灌入 RBAC 的幂等标记
pub const K_HYDRATED: &str = "a/hydrated";

pub fn role_key(role: &str) -> String {
    format!("{K_ROLE}{role}")
}
pub fn user_key(user: &str) -> String {
    format!("{K_USER}{user}")
}
pub fn session_key(token_hash: &str) -> String {
    format!("{K_SESSION}{token_hash}")
}

/// 认证读屏障的判据(纯函数,便于单测):本副本的 applied 是否已覆盖它**已知**的 commit。
///
/// 语义边界必须说清:这只能挡住"**已经知道** commit 推进但还没 apply"的窗口;
/// 对"还没从心跳里学到新 commit"的副本,本判据会通过 —— 那段窗口上界是**一次心跳**
/// (默认 300ms)。要抹掉它必须每请求 read-index(多一个 RTT),代价不可接受,
/// 因此:普通请求用本屏障 + 关键撤销在返回前等"全部可达副本 ack"(`wait_replicated_to_all`)。
pub fn auth_barrier_ok(applied: u64, commit: u64) -> bool {
    applied >= commit
}

/// 会话 token 的存储形式:**只存哈希**(快照/日志泄露不等于拿到可用凭据)
pub fn token_hash(token: &str) -> String {
    crate::sha256::to_hex(&crate::sha256::digest(token.as_bytes()))
}

/// 口令摘要(与 `src/store.rs` 的既有实现逐字一致,保证既有记录可直接沿用)
pub fn hash_password(salt: &str, pass: &str) -> String {
    crate::sha256::to_hex(&crate::sha256::digest(format!("{salt}:{pass}").as_bytes()))
}

/// 随机盐(十六进制);仅登录/改密时生成,写入值随 op 落日志 ⇒ 各副本一致
pub fn new_salt() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    // 不用 rand crate(零新依赖):时间 + 进程内计数器 + 地址熵,足够做盐;
    // 安全性来自"盐不需要不可预测",口令摘要的抗碰撞由 SHA-256 提供。
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let a = &N as *const _ as usize;
    crate::sha256::to_hex(&crate::sha256::digest(
        format!("{t}:{n}:{a}:{}", std::process::id()).as_bytes(),
    ))[..32]
        .to_string()
}

// ─── 记录构造 / 解析 ───

/// 角色记录
pub fn role_entry(description: &str, perms: &[String], ver: u64) -> Value {
    let mut p: Vec<String> = perms.to_vec();
    p.sort();
    p.dedup();
    json!({ "description": description, "perms": p, "ver": ver })
}

/// 用户记录。`epoch` 在**口令或启用态变化**时必须 +1(见模块头注释第 2 点)。
pub fn user_entry(
    salt: &str,
    pass_hash: &str,
    enabled: bool,
    roles: &[String],
    epoch: u64,
) -> Value {
    let mut r: Vec<String> = roles.to_vec();
    r.sort();
    r.dedup();
    json!({ "salt": salt, "pass_hash": pass_hash, "enabled": enabled, "roles": r, "epoch": epoch })
}

/// 会话记录
pub fn session_entry(user: &str, epoch: u64, issued_ms: u64, ttl_ms: u64) -> Value {
    json!({
        "user": user,
        "epoch": epoch,
        "issued_ms": issued_ms,
        "expire_at_ms": issued_ms.saturating_add(ttl_ms),
    })
}

pub fn entry_user(e: &Value) -> Option<&str> {
    e.get("user").and_then(|v| v.as_str())
}
pub fn entry_epoch(e: &Value) -> u64 {
    e.get("epoch").and_then(|v| v.as_u64()).unwrap_or(0)
}
pub fn entry_enabled(e: &Value) -> bool {
    e.get("enabled").and_then(|v| v.as_bool()).unwrap_or(false)
}
pub fn entry_roles(e: &Value) -> Vec<String> {
    e.get("roles")
        .and_then(|v| v.as_array())
        .map(|a| a.iter().filter_map(|x| x.as_str().map(|s| s.to_string())).collect())
        .unwrap_or_default()
}
pub fn entry_perms(e: &Value) -> Vec<String> {
    e.get("perms")
        .and_then(|v| v.as_array())
        .map(|a| a.iter().filter_map(|x| x.as_str().map(|s| s.to_string())).collect())
        .unwrap_or_default()
}
pub fn entry_expire_ms(e: &Value) -> u64 {
    e.get("expire_at_ms").and_then(|v| v.as_u64()).unwrap_or(0)
}

/// 口令校验:`entry` 必须是 `u/<user>` 的值
pub fn password_ok(entry: &Value, pass: &str) -> bool {
    let salt = entry.get("salt").and_then(|v| v.as_str()).unwrap_or("");
    let want = entry.get("pass_hash").and_then(|v| v.as_str()).unwrap_or("");
    !want.is_empty() && hash_password(salt, pass) == want
}

/// 权限展开:`super` 角色 = 全量目录;否则并集。与 `src/store.rs` 的语义逐字一致。
pub fn expand_perms(roles: &[String], role_perms: &BTreeMap<String, Vec<String>>, all: &[&str]) -> Vec<String> {
    if roles.iter().any(|r| r == "super") {
        let mut v: Vec<String> = all.iter().map(|s| s.to_string()).collect();
        v.sort();
        v.dedup();
        return v;
    }
    let mut v: Vec<String> = Vec::new();
    for r in roles {
        if let Some(ps) = role_perms.get(r) {
            v.extend(ps.iter().cloned());
        }
    }
    v.sort();
    v.dedup();
    v
}

/// 会话可用性:未过期 + 归属用户正确(用户名来自 `u/<user>` 的**键**,记录里不重复存)
/// + 账号启用 + epoch 一致(冻结/改密后旧会话立即失效)
pub fn session_usable(session: &Value, user_name: &str, user: &Value, now_ms: u64) -> bool {
    entry_user(session) == Some(user_name)
        && entry_expire_ms(session) > now_ms
        && entry_enabled(user)
        && entry_epoch(session) == entry_epoch(user)
}

/// `s/` 前缀下的 token 哈希(仅用于校验与撤销,不对外暴露)
pub fn session_token_hash_from_key(key: &str) -> Option<&str> {
    key.strip_prefix(K_SESSION)
}

/// 由"键空间快照"渲染用户列表(与 store 的 `users_list` 同形)
pub fn users_list_from(
    entries: &BTreeMap<String, Value>,
    role_perms: &BTreeMap<String, Vec<String>>,
    all: &[&str],
) -> Vec<Value> {
    let mut out = Vec::new();
    for (k, v) in entries {
        let Some(u) = k.strip_prefix(K_USER) else {
            continue;
        };
        let roles = entry_roles(v);
        out.push(json!({
            "user": u,
            "enabled": entry_enabled(v),
            "roles": roles,
            "perms": expand_perms(&roles, role_perms, all),
        }));
    }
    out.sort_by(|a, b| a["user"].as_str().unwrap_or("").cmp(b["user"].as_str().unwrap_or("")));
    out
}

/// 由"键空间快照"渲染角色列表(与 store 的 `roles_list` 同形)
pub fn roles_list_from(entries: &BTreeMap<String, Value>) -> Vec<Value> {
    let mut out = Vec::new();
    for (k, v) in entries {
        let Some(name) = k.strip_prefix(K_ROLE) else {
            continue;
        };
        out.push(json!({
            "name": name,
            "description": v.get("description").and_then(|x| x.as_str()).unwrap_or(""),
            "perms": entry_perms(v),
        }));
    }
    out.sort_by(|a, b| a["name"].as_str().unwrap_or("").cmp(b["name"].as_str().unwrap_or("")));
    out
}

/// 把键空间快照切成 (users, roles, role_perms)
pub fn split_entries(
    entries: BTreeMap<String, Value>,
) -> (
    BTreeMap<String, Value>,
    BTreeMap<String, Value>,
    BTreeMap<String, Vec<String>>,
) {
    let mut users = BTreeMap::new();
    let mut roles = BTreeMap::new();
    let mut role_perms = BTreeMap::new();
    for (k, v) in entries {
        if k.starts_with(K_USER) {
            users.insert(k, v);
        } else if k.starts_with(K_ROLE) {
            role_perms.insert(k.trim_start_matches(K_ROLE).to_string(), entry_perms(&v));
            roles.insert(k, v);
        }
    }
    (users, roles, role_perms)
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL: &[&str] = &["instances.view", "instances.manage", "cluster.view"];

    fn roles_map() -> BTreeMap<String, Vec<String>> {
        let mut m = BTreeMap::new();
        m.insert("dba".to_string(), vec!["instances.view".to_string()]);
        m.insert(
            "ops".to_string(),
            vec!["instances.view".to_string(), "instances.manage".to_string()],
        );
        m
    }

    #[test]
    fn password_roundtrip_matches_store_scheme() {
        let salt = "deadbeef";
        let h = hash_password(salt, "s3cret");
        let u = user_entry(salt, &h, true, &[], 1);
        assert!(password_ok(&u, "s3cret"));
        assert!(!password_ok(&u, "s3cret "), "口令必须精确匹配");
        assert!(!password_ok(&u, ""), "空口令不得通过");
        // 与 store.rs 的算法逐字一致(sha256 hex(salt:pass))
        assert_eq!(
            h,
            crate::sha256::to_hex(&crate::sha256::digest(format!("{salt}:s3cret").as_bytes()))
        );
    }

    #[test]
    fn super_role_expands_to_full_catalog_and_others_union() {
        let full = expand_perms(&["super".to_string()], &roles_map(), ALL);
        assert_eq!(full, vec!["cluster.view", "instances.manage", "instances.view"]);
        let dba = expand_perms(&["dba".to_string()], &roles_map(), ALL);
        assert_eq!(dba, vec!["instances.view"]);
        let both = expand_perms(&["dba".to_string(), "ops".to_string()], &roles_map(), ALL);
        assert_eq!(both, vec!["instances.manage", "instances.view"], "并集且去重排序");
        assert!(expand_perms(&["ghost".to_string()], &roles_map(), ALL).is_empty());
    }

    #[test]
    fn user_entry_is_sorted_deduped_and_deterministic() {
        let a = user_entry("s", "h", true, &["b".into(), "a".into(), "b".into()], 3);
        assert_eq!(entry_roles(&a), vec!["a", "b"]);
        let b = user_entry("s", "h", true, &["a".into(), "b".into()], 3);
        assert_eq!(a, b, "同内容必须产生逐字节相同的记录(确定性)");
    }

    #[test]
    fn session_usable_requires_epoch_match_and_not_expired() {
        let now = 1_000_000u64;
        let u = user_entry("s", "h", true, &[], 7);
        let good = session_entry("alice", 7, now - 1000, 60_000);
        assert!(session_usable(&good, "alice", &u, now));

        // 冻结(epoch+1)→ 旧会话立即失效
        let frozen = user_entry("s", "h", true, &[], 8);
        assert!(!session_usable(&good, "alice", &frozen, now), "epoch 不一致必须失效");

        // 改密同理(epoch+1)
        let changed = user_entry("s", "h2", true, &[], 8);
        assert!(!session_usable(&good, "alice", &changed, now));

        // 过期
        let expired = session_entry("alice", 7, now - 120_000, 60_000);
        assert!(!session_usable(&expired, "alice", &u, now));

        // 禁用
        let disabled = user_entry("s", "h", false, &[], 7);
        assert!(!session_usable(&good, "alice", &disabled, now));

        // 换人:会话属于 bob,但名字参数是 alice
        let other = session_entry("bob", 7, now - 1000, 60_000);
        assert!(!session_usable(&other, "alice", &u, now));
    }

    #[test]
    fn lists_render_in_store_shape() {
        let mut entries = BTreeMap::new();
        entries.insert(role_key("dba"), role_entry("DBA", &["instances.view".into()], 1));
        entries.insert(
            user_key("alice"),
            user_entry("s", "h", true, &["dba".into()], 1),
        );
        let (users, roles, rp) = split_entries(entries);
        let ul = users_list_from(&users, &rp, ALL);
        assert_eq!(ul.len(), 1);
        assert_eq!(ul[0]["user"], serde_json::json!("alice"));
        assert_eq!(ul[0]["perms"], serde_json::json!(["instances.view"]));
        let rl = roles_list_from(&roles);
        assert_eq!(rl[0]["name"], serde_json::json!("dba"));
        assert_eq!(rl[0]["description"], serde_json::json!("DBA"));
    }

    #[test]
    fn barrier_decision_is_exactly_applied_covers_commit() {
        assert!(auth_barrier_ok(10, 10));
        assert!(auth_barrier_ok(11, 10), "追过头也允许");
        assert!(!auth_barrier_ok(9, 10), "落后必须拒绝(fail-closed)");
        assert!(auth_barrier_ok(0, 0), "空日志的初始状态允许");
    }

    #[test]
    fn token_is_only_stored_hashed() {
        let h = token_hash("plain-token");
        assert_ne!(h, "plain-token");
        assert_eq!(h.len(), 64, "sha256 hex");
        assert_eq!(session_token_hash_from_key(&session_key(&h)), Some(h.as_str()));
        assert!(session_token_hash_from_key("u/alice").is_none());
    }
}
