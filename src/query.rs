// rdsctl — DBA Web 查询台引擎(dba-console-design §5)
//
// 职责(仅确定性逻辑,不直接持 HTTP):
//   - 语句分类(只读/写/拒绝 + 词法护栏)与 deny 名单;
//   - 容器内 mysql 批量输出(TSV)解析 → {columns, rows};敏感列后端掩码;
//   - 专用低权账号(rds_ro / rds_rw)懒供给(幂等,root 仅用于建号/授权);
//   - 超时/字节/行数/并发/长度护栏;执行审计入 query_audit + audit_log 摘要。
//
// 红线:查询结果行永不落库;专用口令不进入任何视图/审计;写语句强制 master;
// 无 RDSCTL_QUERY_ALLOW_ROOT=1(lab 专用)时绝不回退 root 执行用户 SQL。

use std::sync::{Arc, OnceLock};

use serde_json::{json, Value};

use crate::auth;
use crate::docker as dk;
use crate::instance::{root_pass, InstNode, InstStatus, RdsInstance};
use crate::manager;

// ─── 分类与词法护栏 ───

/// 分类结果 + 语义描述(写:需 instances.query.write;拒绝:带原因)
pub struct Classified {
    pub read_only: bool,
}

const READ_KW: &[&str] = &[
    "SELECT", "WITH", "SHOW", "EXPLAIN", "DESCRIBE", "DESC", "HELP",
];
const WRITE_KW: &[&str] = &["INSERT", "UPDATE", "DELETE", "REPLACE"];
/// DDL/管理类语句:查询台不开放(专用写账号仅 DML 授权),走实例生命周期/运维通道
const DENY_KW: &[&str] = &[
    "ALTER",
    "CREATE",
    "DROP",
    "TRUNCATE",
    "RENAME",
    "GRANT",
    "REVOKE",
    "LOCK",
    "UNLOCK",
    "CALL",
    "LOAD",
    "SET",
    "START",
    "STOP",
    "ANALYZE",
    "OPTIMIZE",
    "REPAIR",
    "FLUSH",
    "KILL",
    "RESET",
    "CHANGE",
    "INSTALL",
    "UNINSTALL",
    "USE",
    "BEGIN",
    "COMMIT",
    "ROLLBACK",
    "SAVEPOINT",
];
/// 词法级高危子串(出现在字符串外即拒绝;DB 级还有 FILE/GRANT 等权限兜底)
const FORBIDDEN: &[&str] = &[
    "OUTFILE",
    "DUMPFILE",
    "LOAD_FILE",
    "SYS_EXEC",
    "BENCHMARK",
    "SLEEP",
];

/// 默认 deny 表(前缀匹配,不区分大小写;DB 级由账号 grant 兜底)
fn block_prefixes() -> Vec<String> {
    let mut v = vec![
        "mysql.".to_string(),
        "performance_schema.".to_string(),
        "information_schema.user_privileges".to_string(),
        "information_schema.schema_privileges".to_string(),
        "information_schema.role_".to_string(),
        "sys.".to_string(),
    ];
    if let Ok(x) = std::env::var("RDSCTL_QUERY_BLOCK_TABLES") {
        for p in x.split(',') {
            let p = p.trim().to_lowercase();
            if !p.is_empty() {
                v.push(p);
            }
        }
    }
    v
}

/// 清洗扫描:移除注释,返回「去掉字符串/注释后的小写化骨架」与
/// 关键信息(first_keyword、顶层分号数、被引号包住的骨架标识)。
struct Scan {
    /// 注释剔除后的可执行文本小写(字符串字面量内容保留,便于识别;禁止词只查非串区)
    skeleton_lower: String,
    /// 第一个顶层关键词
    first_kw: String,
    /// 顶层(串外/注释外)分号数量(末尾一个分号允许)
    top_semicolons: usize,
}

fn scan_sql(sql: &str) -> Scan {
    let b = sql.as_bytes();
    let mut i = 0usize;
    let mut sk = String::new();
    let mut first_kw: Option<String> = None;
    let mut semis = 0usize;
    let mut pending_kw = String::new();
    // 词素收束:记录首词(大写),并把该词小写追加进骨架(供 deny/禁词匹配)
    macro_rules! flush_word {
        () => {
            if !pending_kw.is_empty() {
                if first_kw.is_none() {
                    first_kw = Some(pending_kw.clone());
                }
                sk.push_str(&pending_kw.to_lowercase());
                pending_kw.clear();
            }
        };
    }
    while i < b.len() {
        let c = b[i] as char;
        match c {
            '\'' | '"' => {
                flush_word!();
                let q = c;
                // 字符串字面量:整段跳过(不以内容做关键词判定),防止引号内注入词误判
                i += 1;
                let mut prev_bs = false;
                while i < b.len() {
                    let cc = b[i] as char;
                    if cc == '\\' && !prev_bs {
                        prev_bs = true;
                        i += 1;
                        continue;
                    }
                    if cc == q && !prev_bs {
                        break;
                    }
                    prev_bs = false;
                    i += 1;
                }
                i += 1; // 跳过闭合引号
                sk.push(' '); // 字面量位置占位
            }
            '`' => {
                flush_word!();
                i += 1;
                while i < b.len() && (b[i] as char) != '`' {
                    i += 1;
                }
                i += 1;
                sk.push(' ');
            }
            '-' if i + 1 < b.len() && (b[i + 1] as char) == '-' => {
                flush_word!();
                while i < b.len() && (b[i] as char) != '\n' {
                    i += 1;
                }
                sk.push(' ');
            }
            '#' => {
                flush_word!();
                while i < b.len() && (b[i] as char) != '\n' {
                    i += 1;
                }
                sk.push(' ');
            }
            '/' if i + 1 < b.len() && (b[i + 1] as char) == '*' => {
                flush_word!();
                i += 2;
                while i + 1 < b.len() && !((b[i] as char) == '*' && (b[i + 1] as char) == '/') {
                    i += 1;
                }
                i += 2;
                sk.push(' ');
            }
            ';' => {
                flush_word!();
                semis += 1;
                i += 1;
                sk.push(' ');
            }
            _ => {
                if c.is_ascii_alphanumeric() || c == '_' || c == '$' {
                    pending_kw.push(if c.is_ascii_alphabetic() {
                        c.to_ascii_uppercase()
                    } else {
                        c
                    });
                } else if c.is_ascii_whitespace() || c == '(' {
                    flush_word!();
                    sk.push(' ');
                } else if c == '.' && !pending_kw.is_empty() {
                    // 点号并入标识符(mysql.user / appdb.t1 保留结构)
                    pending_kw.push('.');
                } else if pending_kw.is_empty() {
                    sk.push(c);
                }
                i += 1;
            }
        }
    }
    flush_word!();
    Scan {
        skeleton_lower: sk,
        first_kw: first_kw.unwrap_or_default(),
        top_semicolons: semis,
    }
}

/// 语句分类。Err = 拒绝(带原因);Ok = Read/Write。
pub fn classify_sql(sql: &str) -> Result<Classified, String> {
    if sql.trim().is_empty() {
        return Err("SQL 为空".to_string());
    }
    if sql.len() > max_sql_len() {
        return Err(format!("SQL 超过长度上限({} 字符)", max_sql_len()));
    }
    let s = scan_sql(sql);
    // 多语句:顶层分号 > 1(末尾单个分号视为可容忍)
    let trailing = sql.trim_end().ends_with(';');
    let n = if trailing {
        s.top_semicolons.saturating_sub(1)
    } else {
        s.top_semicolons
    };
    if n > 0 {
        return Err("不支持多语句(一次仅一条 SQL)".to_string());
    }
    for f in FORBIDDEN {
        if contains_outside_quotes(sql, f) {
            return Err(format!("语句包含禁止词 {f}"));
        }
    }
    let kw = s.first_kw.clone();
    if !kw.is_empty() {
        if READ_KW.contains(&kw.as_str()) {
            return Ok(Classified { read_only: true });
        }
        if WRITE_KW.contains(&kw.as_str()) {
            return Ok(Classified { read_only: false });
        }
        if DENY_KW.contains(&kw.as_str()) {
            return Err(format!(
                "语句 {kw} 属 DDL/管理类,查询台不开放(请走实例生命周期或运维通道)"
            ));
        }
        return Err(format!(
            "首词 {kw} 不在允许范围(只读需 SELECT/SHOW/EXPLAIN/DESCRIBE 等)"
        ));
    }
    Err("无法识别语句首词,已拒绝(注释/特殊开头)".to_string())
}

/// 词法级 deny 名单匹配:提取「被引用的表名」做前缀比对。
/// 词法近似实现:对骨架文本找 `from <ident>` / `join <ident>` / `into <ident>` / `update <ident>` / `table <ident>`。
pub fn blocked_table(sql: &str) -> Option<String> {
    let s = scan_sql(sql).skeleton_lower;
    let bp = block_prefixes();
    for pat in [
        "from ", "join ", "into ", "update ", "table ", "alter ", "drop ",
    ] {
        let mut rest = s.as_str();
        while let Some(pos) = rest.find(pat) {
            let start = pos + pat.len();
            let tail = &rest[start..];
            let ident: String = tail
                .chars()
                .take_while(|ch| {
                    ch.is_ascii_alphanumeric() || *ch == '_' || *ch == '.' || *ch == '`'
                })
                .collect();
            let ident = ident.replace('`', "");
            for p in &bp {
                if ident.starts_with(p.as_str()) {
                    return Some(ident);
                }
            }
            rest = tail;
        }
    }
    None
}

fn contains_outside_quotes(sql: &str, upper: &str) -> bool {
    // 复用骨架(字符串已被占位为空格):在骨架里找关键词(前后非字母数字)
    let sk = scan_sql(sql).skeleton_lower;
    let kw = upper.to_lowercase();
    let bytes = sk.as_bytes();
    let mut i = 0usize;
    while i + kw.len() <= bytes.len() {
        if sk[i..i + kw.len()] == *kw {
            let before_ok = i == 0 || !(bytes[i - 1] as char).is_ascii_alphanumeric();
            let after_ok = i + kw.len() >= bytes.len()
                || !(bytes[i + kw.len()] as char).is_ascii_alphanumeric();
            if before_ok && after_ok {
                return true;
            }
        }
        i += 1;
    }
    false
}

// ─── 护栏参数 ───

fn env_num(key: &str, d: u64, lo: u64, hi: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(d)
        .clamp(lo, hi)
}

fn max_sql_len() -> usize {
    env_num("RDSCTL_QUERY_MAX_SQL", 65536, 1024, 262144) as usize
}
fn timeout_secs() -> u64 {
    env_num("RDSCTL_QUERY_TIMEOUT_SECS", 30, 1, 600)
}
fn max_rows() -> usize {
    env_num("RDSCTL_QUERY_MAX_ROWS", 1000, 1, 100_000) as usize
}
fn max_bytes() -> usize {
    env_num("RDSCTL_QUERY_MAX_BYTES", 1_048_576, 4096, 64 * 1024 * 1024) as usize
}
fn allow_root_fallback() -> bool {
    std::env::var("RDSCTL_QUERY_ALLOW_ROOT").as_deref() == Ok("1")
}

fn sem() -> Arc<tokio::sync::Semaphore> {
    static SEM: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();
    SEM.get_or_init(|| {
        let n = env_num("RDSCTL_QUERY_CONCURRENCY", 4, 1, 64) as usize;
        Arc::new(tokio::sync::Semaphore::new(n))
    })
    .clone()
}

// ─── 掩码规则 ───

fn mask_rules() -> Vec<String> {
    let mut v = vec![
        "password".to_string(),
        "passwd".to_string(),
        "secret".to_string(),
        "token".to_string(),
        "credential".to_string(),
        "private_key".to_string(),
        "salt".to_string(),
        "access_key".to_string(),
        "secret_key".to_string(),
        "api_key".to_string(),
    ];
    if let Ok(x) = std::env::var("RDSCTL_QUERY_MASK_COLS") {
        for p in x.split(',') {
            let p = p.trim().to_lowercase();
            if !p.is_empty() {
                v.push(p);
            }
        }
    }
    v
}

fn is_masked_col(name: &str) -> bool {
    let n = name.to_lowercase();
    mask_rules().iter().any(|r| n.contains(r.as_str()))
}

// ─── 业务库列表(供给账号授权范围;默认 appdb,env 追加) ───

fn query_dbs() -> Vec<String> {
    let mut dbs = vec![crate::instance::APP_DB.to_string()];
    if let Ok(x) = std::env::var("RDSCTL_QUERY_DBS") {
        for d in x.split(',') {
            let d = d.trim();
            if !d.is_empty()
                && d.chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
                && !dbs.iter().any(|e| e == d)
            {
                dbs.push(d.to_string());
            }
        }
    }
    dbs
}

// ─── TSV 解析(docker.rs query_table 输出:批量 + 列名 + 转义,非 --raw) ───

pub struct ParsedTable {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub truncated: bool,
}

fn unescape_field(f: &str) -> String {
    let b = f.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0usize;
    while i < b.len() {
        if b[i] == b'\\' && i + 1 < b.len() {
            let nxt = b[i + 1] as char;
            match nxt {
                't' => out.push(b'\t'),
                'n' => out.push(b'\n'),
                'r' => out.push(b'\r'),
                '0' => out.push(0u8),
                '\\' => out.push(b'\\'),
                '\'' => out.push(b'\''),
                '"' => out.push(b'"'),
                _ => {
                    out.push(b'\\');
                    out.push(b[i + 1]);
                }
            }
            i += 2;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// 解析 mysql --batch --column-names 输出。NULL 字面量 → JSON null;
/// 按行数/字节护栏截断并置 truncated。
pub fn parse_table(out: &str, cap_rows: usize, cap_bytes: usize) -> ParsedTable {
    let mut lines = out.split('\n');
    let header = lines.next().unwrap_or("");
    let columns: Vec<String> = header
        .split('\t')
        .filter(|s| !s.is_empty())
        .map(|s| {
            let s = s.trim_end_matches('\r');
            unescape_field(s)
        })
        .collect();
    let mut rows: Vec<Vec<Value>> = Vec::new();
    let mut bytes_used = 0usize;
    let mut truncated = false;
    for line in lines {
        if line.is_empty() {
            continue;
        }
        if rows.len() >= cap_rows {
            truncated = true;
            break;
        }
        let line = line.trim_end_matches('\r');
        let fields: Vec<&str> = line.split('\t').collect();
        let mut row: Vec<Value> = Vec::with_capacity(fields.len());
        for f in fields {
            let raw = if f == "NULL" {
                None
            } else {
                Some(unescape_field(f))
            };
            let v = match raw {
                None => Value::Null,
                Some(s) => Value::String(s),
            };
            if let Value::String(s) = &v {
                bytes_used += s.len() + 1;
            }
            row.push(v);
        }
        if bytes_used > cap_bytes {
            truncated = true;
            break;
        }
        rows.push(row);
    }
    ParsedTable {
        columns,
        rows,
        truncated,
    }
}

// ─── 账号与执行 ───

const ACCT_RO: &str = "rds_ro";
const ACCT_RW: &str = "rds_rw";

fn ident_ok(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
}

fn redact(s: &str) -> String {
    // 错误文本简单脱敏:去掉疑似口令字面量(与巡检快照同思路,此处最小实现)
    let mut out = s.to_string();
    let mut low = out.to_lowercase();
    while let Some(p) = low.find("using password") {
        out = out[..p].to_string();
        low = out.to_lowercase();
    }
    out.chars().take(600).collect()
}

/// 目标节点解析:node=master|read|offline,默认 master。
/// 返回 (container, 是否写节点)。
fn resolve_node(inst: &RdsInstance, node: &str) -> Result<(String, bool), String> {
    let find = |pred: &dyn Fn(&InstNode) -> bool| -> Option<String> {
        inst.nodes
            .iter()
            .find(|n| pred(n))
            .map(|n| n.container.clone())
    };
    let node = node.to_lowercase();
    match node.as_str() {
        "" | "master" => find(&|n| n.role == crate::instance::Role::Master)
            .map(|c| (c, true))
            .ok_or_else(|| "实例缺少主节点,无法查询".to_string()),
        "read" => find(&|n| n.role == crate::instance::Role::Read)
            .map(|c| (c, false))
            .ok_or_else(|| "实例无在线读从(role=read),可选 master/offline".to_string()),
        "offline" => find(&|n| n.role.is_offline())
            .map(|c| (c, false))
            .ok_or_else(|| "实例无离线从(role=offline),可选 master/read".to_string()),
        other => Err(format!("未知节点类型 {other}(支持 master/read/offline)")),
    }
}

/// 专用账号(只读/写)幂等供给;仅 root 用于建号与授权。
async fn provision(container: &str, secret: &str, write: bool) -> Result<(), String> {
    let dbs = query_dbs();
    if dbs.is_empty() {
        return Err("未配置可查询业务库(RDSCTL_QUERY_DBS 为空)".to_string());
    }
    let mut sql = String::new();
    for (acct, extra_grants) in [
        (ACCT_RO, ""),
        (ACCT_RW, if write { "INSERT, UPDATE, DELETE" } else { "" }),
    ] {
        sql.push_str(&format!(
            "CREATE USER IF NOT EXISTS '{acct}'@'%' IDENTIFIED BY '{secret}'; \
             ALTER USER '{acct}'@'%' IDENTIFIED BY '{secret}'; \
             REVOKE ALL, GRANT OPTION FROM '{acct}'@'%'; "
        ));
        let privs = if extra_grants.is_empty() {
            "SELECT".to_string()
        } else {
            format!("SELECT, {extra_grants}")
        };
        for db in &dbs {
            if !ident_ok(db) {
                return Err(format!("业务库名非法: {db}"));
            }
            sql.push_str(&format!("GRANT {privs} ON `{db}`.* TO '{acct}'@'%'; "));
        }
    }
    sql.push_str("FLUSH PRIVILEGES;");
    dk::exec_mysql_local(container, "root", root_pass(), &sql).await?;
    Ok(())
}

/// 执行一条 SQL(调用方已做权限判定与实例状态校验)。
/// 返回查询结果 JSON 视图;写语句强制 master 由调用方保证。
pub async fn run_query(
    instance_name: &str,
    sql: &str,
    node_param: &str,
    db: &str,
) -> Result<serde_json::Value, QueryFail> {
    // 默认库:优先用户指定(db),否则取首个业务库;解决 -D 缺失导致的 "No database selected"
    let use_db = if !db.is_empty()
        && db
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
    {
        db.to_string()
    } else {
        query_dbs().into_iter().next().unwrap_or_default()
    };
    // 1) 实例/节点
    let inst = manager()
        .instances
        .get(instance_name)
        .map(|e| e.value().clone())
        .ok_or_else(|| QueryFail::new(404, format!("实例 {instance_name} 不存在")))?;
    if inst.status != InstStatus::Running {
        return Err(QueryFail::new(
            400,
            format!(
                "实例 {} 当前状态 {},仅运行中可查询",
                instance_name,
                inst.status.label()
            ),
        ));
    }
    let (container, is_write_node) = resolve_node(&inst, node_param).map_err(QueryFail::new_400)?;

    // 2) 分类(写语句由调用方二次鉴权;此处以 node 兜底强制 master)
    let c = classify_sql(sql).map_err(QueryFail::new_400)?;
    if !c.read_only {
        if !is_write_node {
            return Err(QueryFail::new(
                400,
                "写语句仅允许在 master 节点执行".to_string(),
            ));
        }
        if !auth::has_perm("instances.query.write") {
            return Err(QueryFail::new(
                403,
                "权限不足:需要 instances.query.write".to_string(),
            ));
        }
    }
    if let Some(t) = blocked_table(sql) {
        return Err(QueryFail::new(400, format!("表 {t} 在 deny 名单内,已拒绝")));
    }

    // 3) 账号:专用低权账号;仅显式 RDSCTL_QUERY_ALLOW_ROOT=1(lab)回退 root
    let (user, pass) = if allow_root_fallback() {
        ("root".to_string(), root_pass().to_string())
    } else {
        let secret = manager()
            .ensure_query_secret(instance_name)
            .map_err(QueryFail::new_500)?;
        provision(&container, &secret, !c.read_only)
            .await
            .map_err(|e| {
                QueryFail::new(
                    500,
                    format!("查询账号准备失败(已拒绝执行,不回退 root): {}", redact(&e)),
                )
            })?;
        (
            if c.read_only {
                ACCT_RO.to_string()
            } else {
                ACCT_RW.to_string()
            },
            secret,
        )
    };

    // 4) 并发配额(立即失败而非排队,避免请求堆积打爆容器)
    let _permit = sem()
        .clone()
        .try_acquire_owned()
        .map_err(|_| QueryFail::new(429, "查询并发已达上限,请稍后重试".to_string()))?;

    // 5) 执行 + 解析(计时;超时由 query_table 内部处理)
    let t0 = std::time::Instant::now();
    let out = dk::query_table(&container, &user, &pass, sql, timeout_secs(), &use_db).await;
    let elapsed_ms = t0.elapsed().as_millis() as u64;

    let (columns, rows, truncated, ok, err_summary): (
        Vec<String>,
        Vec<Vec<Value>>,
        bool,
        bool,
        String,
    ) = match out {
        Ok(o) => {
            let p = parse_table(&o, max_rows(), max_bytes());
            (p.columns, p.rows, p.truncated, true, String::new())
        }
        Err(e) => {
            // 超时/执行错误:记空结果 + ok=false,err_summary 保留(状态码按文本判定)
            (Vec::new(), Vec::new(), false, false, redact(&e))
        }
    };
    let rows_returned = rows.len();
    // 掩码在后端统一实施
    let masked_cols: Vec<String> = columns
        .iter()
        .enumerate()
        .filter(|(_, c)| is_masked_col(c))
        .map(|(_, c)| c.clone())
        .collect();
    let rows: Vec<Vec<Value>> = if masked_cols.is_empty() {
        rows
    } else {
        rows.into_iter()
            .map(|r| {
                r.into_iter()
                    .enumerate()
                    .map(|(i, v)| {
                        if masked_cols.contains(&columns[i]) {
                            Value::String("***".to_string())
                        } else {
                            v
                        }
                    })
                    .collect()
            })
            .collect()
    };

    let sql_hash = crate::sha256::to_hex(&crate::sha256::digest(sql.as_bytes()));
    let user = auth::current_user();
    let node_name = node_param.to_lowercase();
    let node_label = if node_name.is_empty() {
        "master".to_string()
    } else {
        node_name.clone()
    };
    manager().store.query_audit_insert(
        &user,
        instance_name,
        &node_label,
        sql,
        &sql_hash,
        c.read_only,
        rows_returned as u64,
        truncated,
        elapsed_ms,
        ok,
        &err_summary,
    );
    // audit_log 摘要(既有审计页可见;截断语义由 store 保证)
    let summary = truncate_utf8(
        &if ok {
            format!("{} 行/{}ms", rows_returned, elapsed_ms)
        } else {
            err_summary.clone()
        },
        200,
    );
    manager().store.audit(
        &user,
        instance_name,
        "query_sql",
        &summary,
        if ok { "ok" } else { "fail" },
        "",
    );

    if !ok {
        let msg = err_summary;
        let status = if msg.contains("查询超时") {
            408
        } else {
            400
        };
        return Err(QueryFail::new(status, msg));
    }

    // 结果列类型(工作台表头小字;仅只读且列名可解析时尽力而为,失败忽略)
    let mut column_types = serde_json::Map::new();
    if c.read_only && !columns.is_empty() && columns.len() <= 64 {
        let in_list = columns
            .iter()
            .map(|s| sql_str(s))
            .collect::<Vec<_>>()
            .join(",");
        let sql_ct = format!(
            "SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA={} AND COLUMN_NAME IN ({})",
            sql_str(&use_db),
            in_list
        );
        if let Ok(out) = dk::query_table(
            &container,
            &user,
            &pass,
            &sql_ct,
            timeout_secs().min(20),
            "",
        )
        .await
        {
            let p = parse_table(&out, 200, 512 * 1024);
            let cc: Vec<String> = p.columns.iter().map(|x| x.to_lowercase()).collect();
            if let (Some(i_n), Some(i_t)) =
                (col_index(&cc, "column_name"), col_index(&cc, "data_type"))
            {
                for r in &p.rows {
                    column_types.insert(val_cell(&r[i_n]), serde_json::json!(val_cell(&r[i_t])));
                }
            }
        }
    }

    Ok(json!({
        "ok": true,
        "instance": instance_name,
        "node": node_label,
        "read_only": c.read_only,
        "columns": columns,
        "rows": rows,
        "rows_returned": rows_returned,
        "truncated": truncated,
        "elapsed_ms": elapsed_ms,
        "masked_cols": masked_cols,
        "column_types": column_types,
    }))
}

fn sql_str(v: &str) -> String {
    format!("'{}'", v.replace('\\', "\\\\").replace('\'', "\\'"))
}

fn cells(parsed: &ParsedTable, idx: &[usize]) -> Vec<Vec<String>> {
    parsed
        .rows
        .iter()
        // 越界保护:畸形 TSV 行(注释换行/截断)不 panic,缺列补空串——避免连接被中断致前端 Failed to fetch
        .map(|r| {
            idx.iter()
                .map(|i| r.get(*i).map(val_cell).unwrap_or_default())
                .collect()
        })
        .collect()
}

fn val_cell(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        _ => v.to_string(),
    }
}

fn col_index(cols: &[String], name: &str) -> Option<usize> {
    cols.iter().position(|c| c.eq_ignore_ascii_case(name))
}

/// 工作台 schema 元数据(dba 工作台对象树/表结构):经专用只读账号查 information_schema。
/// scope 由 db/table 参数推导:均空→库列表;仅 db→表列表;db+table→列 + 粗略 DDL。
pub async fn schema(
    instance_name: &str,
    db: &str,
    table: &str,
) -> Result<serde_json::Value, QueryFail> {
    let inst = manager()
        .instances
        .get(instance_name)
        .map(|e| e.value().clone())
        .ok_or_else(|| QueryFail::new(404, format!("实例 {instance_name} 不存在")))?;
    if inst.status != InstStatus::Running {
        return Err(QueryFail::new(
            400,
            format!(
                "实例 {} 当前状态 {},仅运行中可查看 schema",
                instance_name,
                inst.status.label()
            ),
        ));
    }
    let (container, _) = resolve_node(&inst, "master").map_err(QueryFail::new_400)?;
    let secret = manager()
        .ensure_query_secret(instance_name)
        .map_err(QueryFail::new_500)?;
    // 元数据浏览只读;发现账号未供给则供给(与查询一致)
    let _ = provision(&container, &secret, false).await.map_err(|e| {
        QueryFail::new(500, format!("查询账号准备失败(已拒绝执行): {}", redact(&e)))
    })?;

    let timeout = timeout_secs().min(30);
    let use_master = container.as_str();
    // 元数据也走只读专用账号(仅授权业务库可见);root 仅用于账号供给
    let (user, pass) = if allow_root_fallback() {
        ("root".to_string(), root_pass().to_string())
    } else {
        (ACCT_RO.to_string(), secret)
    };

    let (scope, sql): (&str, String) = if db.is_empty() && table.is_empty() {
        (
            "libs",
            "SELECT s.SCHEMA_NAME, (SELECT COUNT(*) FROM information_schema.TABLES t \
              WHERE t.TABLE_SCHEMA = s.SCHEMA_NAME) AS object_count \
             FROM information_schema.SCHEMATA s \
             WHERE s.SCHEMA_NAME NOT IN ('mysql','performance_schema','information_schema','sys') \
             ORDER BY s.SCHEMA_NAME"
                .to_string(),
        )
    } else if !db.is_empty() && table.is_empty() {
        (
            "tables",
            format!(
                "SELECT TABLE_NAME, TABLE_ROWS, TABLE_TYPE, ENGINE, TABLE_COLLATION, \
                 TABLE_COMMENT FROM information_schema.TABLES \
                 WHERE TABLE_SCHEMA={} ORDER BY TABLE_NAME",
                sql_str(db)
            ),
        )
    } else {
        (
            "columns",
            format!(
                "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY, COLUMN_DEFAULT, \
                 COLUMN_COMMENT, EXTRA FROM information_schema.COLUMNS \
                 WHERE TABLE_SCHEMA={} AND TABLE_NAME={} ORDER BY ORDINAL_POSITION",
                sql_str(db),
                sql_str(table)
            ),
        )
    };

    let out = dk::query_table(use_master, &user, &pass, &sql, timeout, "")
        .await
        .map_err(|e| QueryFail::new(400, redact(&e).chars().take(400).collect::<String>()))?;
    let parsed = parse_table(&out, 5000, 4 * 1024 * 1024);
    let cols: Vec<String> = parsed.columns.iter().map(|s| s.to_lowercase()).collect();

    let actor = auth::current_user();
    manager().store.audit(
        &actor,
        instance_name,
        "schema_view",
        &format!("{db}/{table}"),
        "ok",
        "",
    );

    let mut v = serde_json::json!({
        "ok": true,
        "instance": instance_name,
        "scope": scope,
        "libs": [],
        "tables": [],
        "columns": [],
        "ddl": "",
    });
    match scope {
        "libs" => {
            if let (Some(i_n), Some(i_c)) = (
                col_index(&cols, "schema_name"),
                col_index(&cols, "object_count"),
            ) {
                v["libs"] = serde_json::json!(cells(&parsed, &[i_n, i_c])
                    .into_iter()
                    .map(|r| serde_json::json!({
                        "name": r[0],
                        "object_count": r[1].parse::<u64>().unwrap_or(0),
                    }))
                    .collect::<Vec<_>>());
            }
        }
        "tables" => {
            let idx: Vec<usize> = [
                "table_name",
                "table_rows",
                "table_type",
                "engine",
                "table_collation",
                "table_comment",
            ]
            .iter()
            .filter_map(|n| col_index(&cols, n))
            .collect();
            if idx.len() == 6 {
                v["tables"] = serde_json::json!(cells(&parsed, &idx)
                    .into_iter()
                    .map(|r| serde_json::json!({
                        "name": r[0],
                        "rows": r[1].parse::<u64>().unwrap_or(0),
                        "type": r[2],
                        "engine": r[3],
                        "collation": r[4],
                        "comment": r[5],
                    }))
                    .collect::<Vec<_>>());
            }
        }
        "columns" => {
            let wanted = [
                "column_name",
                "data_type",
                "is_nullable",
                "column_key",
                "column_default",
                "column_comment",
                "extra",
            ];
            let idx: Vec<usize> = wanted.iter().filter_map(|n| col_index(&cols, n)).collect();
            let rows = cells(&parsed, &idx);
            let defs: Vec<String> = rows
                .iter()
                .map(|r| {
                    let mut line = format!("  `{}` {}", r[0], r[1]);
                    if let Some(nn) = r.get(2) {
                        if nn.eq_ignore_ascii_case("NO") {
                            line.push_str(" NOT NULL");
                        }
                    }
                    if let Some(d) = r.get(4) {
                        if !d.is_empty() && !d.eq_ignore_ascii_case("NULL") {
                            line.push_str(&format!(
                                " DEFAULT {}",
                                if r[1].contains("int") {
                                    d.clone()
                                } else {
                                    sql_str(d)
                                }
                            ));
                        }
                    }
                    if let Some(k) = r.get(3) {
                        if !k.is_empty() && k != "MUL" {
                            line.push_str(&format!(" {k}")); // 精确 PRIMARY/UNIQUE
                        }
                    }
                    if let Some(c) = r.get(5) {
                        if !c.is_empty() {
                            line.push_str(&format!(" COMMENT {}", sql_str(c)));
                        }
                    }
                    line
                })
                .collect();
            let ddl = if rows.is_empty() {
                String::new()
            } else {
                format!(
                    "CREATE TABLE `{}`.`{}` (\n{}\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
                    db,
                    table,
                    defs.join(",\n")
                )
            };
            v["columns"] = serde_json::json!(rows
                .into_iter()
                .map(|r| serde_json::json!({
                    "name": r.get(0).cloned().unwrap_or_default(),
                    "type": r.get(1).cloned().unwrap_or_default(),
                    "nullable": r.get(2).map(|s| !s.eq_ignore_ascii_case("NO")).unwrap_or(true),
                    "key": r.get(3).cloned().unwrap_or_default(),
                    "default": r.get(4).cloned().unwrap_or_default(),
                    "comment": r.get(5).cloned().unwrap_or_default(),
                    "extra": r.get(6).cloned().unwrap_or_default(),
                }))
                .collect::<Vec<_>>());
            v["ddl"] = serde_json::json!(ddl);
        }
        _ => {}
    }
    Ok(v)
}

/// 真实索引定义(SHOW INDEX;右栏「索引」页签;db/table 白名单校验,反引号/空白拒绝)
pub async fn index(
    instance_name: &str,
    db: &str,
    table: &str,
) -> Result<serde_json::Value, QueryFail> {
    if db.is_empty()
        || table.is_empty()
        || db.chars().any(|c| c == '`' || c.is_whitespace())
        || table.chars().any(|c| c == '`' || c.is_whitespace())
    {
        return Err(QueryFail::new(
            400,
            "需 db 与 table,且标识符不含反引号/空白".to_string(),
        ));
    }
    let inst = manager()
        .instances
        .get(instance_name)
        .map(|e| e.value().clone())
        .ok_or_else(|| QueryFail::new(404, format!("实例 {instance_name} 不存在")))?;
    if inst.status != InstStatus::Running {
        return Err(QueryFail::new(
            400,
            format!(
                "实例 {} 当前状态 {},仅运行中可查看索引",
                instance_name,
                inst.status.label()
            ),
        ));
    }
    let (container, _) = resolve_node(&inst, "master").map_err(QueryFail::new_400)?;
    let secret = manager()
        .ensure_query_secret(instance_name)
        .map_err(QueryFail::new_500)?;
    let _ = provision(&container, &secret, false)
        .await
        .map_err(|e| QueryFail::new(500, format!("查询账号准备失败: {}", redact(&e))))?;
    let (user, pass) = if allow_root_fallback() {
        ("root".to_string(), root_pass().to_string())
    } else {
        (ACCT_RO.to_string(), secret)
    };
    let sql = format!("SHOW INDEX FROM `{db}`.`{table}`");
    let out = dk::query_table(&container, &user, &pass, &sql, timeout_secs().min(30), "")
        .await
        .map_err(|e| QueryFail::new(400, redact(&e).chars().take(400).collect::<String>()))?;
    let parsed = parse_table(&out, 500, 512 * 1024);
    let cols: Vec<String> = parsed.columns.iter().map(|s| s.to_lowercase()).collect();
    let wanted = [
        "key_name",
        "non_unique",
        "seq_in_index",
        "column_name",
        "index_type",
        "collation",
    ];
    let idx: Vec<usize> = wanted.iter().filter_map(|n| col_index(&cols, n)).collect();
    let rows = cells(&parsed, &idx);
    let list: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|r| {
            serde_json::json!({
                "key_name": r.get(0).cloned().unwrap_or_default(),
                "non_unique": r.get(1).cloned().unwrap_or_default(),
                "seq": r.get(2).cloned().unwrap_or_default(),
                "column": r.get(3).cloned().unwrap_or_default(),
                "index_type": r.get(4).cloned().unwrap_or_default(),
            })
        })
        .collect();
    manager().store.audit(
        &auth::current_user(),
        instance_name,
        "index_view",
        &format!("{db}.{table}"),
        "ok",
        "",
    );
    Ok(
        serde_json::json!({ "ok": true, "instance": instance_name, "db": db, "table": table, "indexes": list }),
    )
}

/// 例程列表(information_schema.ROUTINES;对象树「函数」分组)
pub async fn routines(instance_name: &str, db: &str) -> Result<serde_json::Value, QueryFail> {
    if db.is_empty() || db.chars().any(|c| c == '`' || c.is_whitespace()) {
        return Err(QueryFail::new(
            400,
            "需 db 且标识符不含反引号/空白".to_string(),
        ));
    }
    let inst = manager()
        .instances
        .get(instance_name)
        .map(|e| e.value().clone())
        .ok_or_else(|| QueryFail::new(404, format!("实例 {instance_name} 不存在")))?;
    if inst.status != InstStatus::Running {
        return Err(QueryFail::new(
            400,
            format!(
                "实例 {} 当前状态 {},仅运行中可查看函数",
                instance_name,
                inst.status.label()
            ),
        ));
    }
    let (container, _) = resolve_node(&inst, "master").map_err(QueryFail::new_400)?;
    let secret = manager()
        .ensure_query_secret(instance_name)
        .map_err(QueryFail::new_500)?;
    let _ = provision(&container, &secret, false)
        .await
        .map_err(|e| QueryFail::new(500, format!("查询账号准备失败: {}", redact(&e))))?;
    let (user, pass) = if allow_root_fallback() {
        ("root".to_string(), root_pass().to_string())
    } else {
        (ACCT_RO.to_string(), secret)
    };
    let sql = format!(
        "SELECT ROUTINE_NAME, ROUTINE_TYPE FROM information_schema.ROUTINES \
         WHERE ROUTINE_SCHEMA={} ORDER BY ROUTINE_NAME",
        sql_str(db)
    );
    let out = dk::query_table(&container, &user, &pass, &sql, timeout_secs().min(30), "")
        .await
        .map_err(|e| QueryFail::new(400, redact(&e).chars().take(400).collect::<String>()))?;
    let parsed = parse_table(&out, 500, 512 * 1024);
    let cols: Vec<String> = parsed.columns.iter().map(|s| s.to_lowercase()).collect();
    let (Some(i_n), Some(i_t)) = (
        col_index(&cols, "routine_name"),
        col_index(&cols, "routine_type"),
    ) else {
        return Ok(serde_json::json!({ "ok": true, "db": db, "routines": [] }));
    };
    let list: Vec<serde_json::Value> = cells(&parsed, &[i_n, i_t])
        .into_iter()
        .map(|r| serde_json::json!({ "name": r[0], "type": r[1] }))
        .collect();
    manager().store.audit(
        &auth::current_user(),
        instance_name,
        "routines_view",
        db,
        "ok",
        "",
    );
    Ok(serde_json::json!({ "ok": true, "db": db, "routines": list }))
}

pub struct QueryFail {
    pub status: u16,
    pub message: String,
}

impl QueryFail {
    fn new(status: u16, message: String) -> Self {
        QueryFail { status, message }
    }
    fn new_400(message: String) -> Self {
        QueryFail::new(400, message)
    }
    fn new_500(message: String) -> Self {
        QueryFail::new(500, message)
    }
}

fn truncate_utf8(s: &str, n: usize) -> String {
    if s.len() <= n {
        s.to_string()
    } else {
        s.chars().take(n).collect()
    }
}

/// 当前护栏默认值(供 GET /api/rds/query/caps 只读展示)
pub fn caps_view(instance_name: &str) -> Value {
    let nodes: Vec<String> = match manager().instances.get(instance_name) {
        Some(e) => {
            let i = e.value();
            let mut v = vec!["master".to_string()];
            if i.nodes
                .iter()
                .any(|n| n.role == crate::instance::Role::Read)
            {
                v.push("read".to_string());
            }
            if i.nodes.iter().any(|n| n.role.is_offline()) {
                v.push("offline".to_string());
            }
            v
        }
        None => Vec::new(),
    };
    json!({
        "timeout_secs": timeout_secs(),
        "max_sql": max_sql_len(),
        "max_rows": max_rows(),
        "max_bytes": max_bytes(),
        "max_concurrency": env_num("RDSCTL_QUERY_CONCURRENCY", 4, 1, 64),
        "allow_root_fallback": allow_root_fallback(),
        "dbs": query_dbs(),
        "nodes": nodes,
        "mask_rules": mask_rules(),
        "block_tables": block_prefixes(),
    })
}

// ─── 单元测试(纯逻辑,无 docker/MySQL) ───

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_reads_writes_and_denies() {
        assert!(classify_sql("SELECT * FROM t").unwrap().read_only);
        assert!(
            classify_sql("with x as (select 1) select * from x")
                .unwrap()
                .read_only
        );
        assert!(classify_sql("SHOW TABLES").unwrap().read_only);
        assert!(classify_sql("EXPLAIN SELECT 1").unwrap().read_only);
        assert!(classify_sql("DESCRIBE t").unwrap().read_only);
        assert!(!classify_sql("UPDATE t SET a=1").unwrap().read_only);
        assert!(!classify_sql("INSERT INTO t VALUES (1)").unwrap().read_only);
        assert!(!classify_sql("DELETE FROM t").unwrap().read_only);
        assert!(classify_sql("DROP TABLE t").is_err(), "DDL 应拒绝");
        assert!(classify_sql("SET autocommit=0").is_err(), "SET 应拒绝");
        assert!(classify_sql("SELECT 1; SELECT 2").is_err(), "多语句应拒绝");
        assert!(classify_sql("SELECT 1;").is_ok(), "末尾单分号容忍");
        assert!(
            classify_sql("/* c */ SELECT 1").unwrap().read_only,
            "注释头"
        );
        assert!(
            classify_sql("-- hi\nSELECT 1").unwrap().read_only,
            "行注释头"
        );
        assert!(classify_sql("DELETE FROM t WHERE a='x'").is_ok());
    }

    #[test]
    fn classify_forbidden_and_multi_via_strings() {
        // 字符串内的分号/关键词不触发
        assert!(classify_sql("SELECT 'a;b' FROM t").is_ok());
        assert!(
            classify_sql("SELECT 'SLEEP(1)'").is_ok(),
            "字符串内 SLEEP 不拒绝"
        );
        // 多语句在引号外的分号
        assert!(classify_sql("SELECT 1 FROM t WHERE a=';';SELECT 2").is_err());
        // OUTFILE 拒绝
        assert!(classify_sql("SELECT 1 INTO OUTFILE '/tmp/x'").is_err());
    }

    #[test]
    fn blocked_tables_prefix() {
        assert!(blocked_table("SELECT * FROM mysql.user").is_some());
        assert!(blocked_table("UPDATE mysql.user SET x=1").is_some());
        assert!(blocked_table(
            "SELECT * FROM performance_schema.events_statements_summary_by_digest"
        )
        .is_some());
        assert!(blocked_table("SELECT * FROM appdb.users").is_none());
    }

    #[test]
    fn parse_table_basic() {
        let out = "id\tname\temail\n1\ta\tb\n2\tNULL\tx\\ttab\n";
        let p = parse_table(out, 100, 1 << 20);
        assert_eq!(p.columns, vec!["id", "name", "email"]);
        assert_eq!(p.rows.len(), 2);
        assert_eq!(p.rows[1][1], Value::Null);
        assert_eq!(p.rows[1][2], Value::String("x\ttab".into()));
    }

    #[test]
    fn parse_table_caps() {
        let mut out = String::from("id\n");
        for i in 0..20 {
            out.push_str(&format!("{i}\n"));
        }
        let p = parse_table(&out, 10, 1 << 20);
        assert_eq!(p.rows.len(), 10);
        assert!(p.truncated);
    }

    #[test]
    fn mask_rules_match() {
        assert!(is_masked_col("user_password"));
        assert!(is_masked_col("accessToken"));
        assert!(!is_masked_col("name"));
        assert!(!is_masked_col("created_at"));
    }
}
