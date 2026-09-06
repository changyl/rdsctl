// rdsctl — 全局慢查询治理采集与聚合(slow-query-design §4/§6)
//
// 职责:
//   - 周期采集 performance_schema.events_statements_summary_by_digest(逐 running 实例,
//     master + offline,可选 read),与上轮基线做差分,增量快照落 slow_digest_snapshots;
//   - 只存规范化 digest_text(MySQL 归一化,无字面量);绝不存 sample_text/原始 SQL;
//   - 规则(全局窗口聚合 → 阈值)→ slow_governance 治理队列(open/ack/resolved);
//   - 保留清理(快照 RDSCTL_SLOW_RETENTION_DAYS 默认 30 天;治理已解决 90 天);
//   - 确定性聚合/分桶纯函数(供 HTTP 接口与单测复用)。
//
// 采集失败:静默降级(debug 日志),不影响实例状态/巡检/查询台;无运行实例时零写入。

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::{Mutex, OnceLock};

use serde_json::{json, Value};

use crate::docker as dk;
use crate::instance::{InstStatus, RdsManager, Role, ROOT_PASS};
use crate::manager;
use crate::query::parse_table;
use crate::store::{SlowBase, SlowSample};

const DIGEST_SQL: &str = "SELECT SCHEMA_NAME, DIGEST, DIGEST_TEXT, COUNT_STAR, \
     ROUND(SUM_TIMER_WAIT/1000000000) AS sum_ms, \
     ROUND(AVG_TIMER_WAIT/1000000000) AS avg_ms, \
     ROUND(MAX_TIMER_WAIT/1000000000) AS max_ms, \
     UNIX_TIMESTAMP(FIRST_SEEN) AS first_seen, UNIX_TIMESTAMP(LAST_SEEN) AS last_seen \
     FROM performance_schema.events_statements_summary_by_digest \
     WHERE DIGEST IS NOT NULL AND SCHEMA_NAME IS NOT NULL AND COUNT_STAR > 0 \
     ORDER BY SUM_TIMER_WAIT DESC LIMIT 500";

fn env_u64(key: &str, d: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(d)
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

// ─── 确定性纯函数(聚合/趋势/规则,供接口与测试) ───

/// 按 digest 聚合窗口采样(跨实例全局口径)。
/// rows: slow_window 的 JSON 行。返回排序后的 [{digest,digest_text,total_count,total_ms,
/// avg_ms,max_ms,instances,instance_count}](按 total_ms 降序),并套用 min_count 过滤。
pub fn aggregate_window(rows: &[Value], min_count: u64) -> Vec<Value> {
    #[derive(Default)]
    struct Acc {
        text: String,
        count: u64,
        sum: u64,
        max: u64,
        instances: Vec<String>,
    }
    let mut m: HashMap<String, Acc> = HashMap::new();
    for r in rows {
        let Some(digest) = r["digest"].as_str().map(String::from) else {
            continue;
        };
        let a = m.entry(digest).or_default();
        if a.text.is_empty() {
            a.text = r["digest_text"].as_str().unwrap_or("").to_string();
        }
        a.count += r["count_star"].as_u64().unwrap_or(0);
        a.sum += r["sum_ms"].as_u64().unwrap_or(0);
        a.max = a.max.max(r["max_ms"].as_u64().unwrap_or(0));
        if let Some(inst) = r["instance"].as_str() {
            if !a.instances.iter().any(|x| x == inst) {
                a.instances.push(inst.to_string());
            }
        }
    }
    let mut out: Vec<Value> = m
        .into_iter()
        .filter(|(_, a)| a.count >= min_count)
        .map(|(digest, a)| {
            let avg = if a.count > 0 { a.sum / a.count } else { 0 };
            let mut insts = a.instances;
            insts.sort();
            json!({
                "digest": digest,
                "digest_text": a.text,
                "total_count": a.count,
                "total_ms": a.sum,
                "avg_ms": avg,
                "max_ms": a.max,
                "instances": insts,
                "instance_count": insts.len(),
            })
        })
        .collect();
    out.sort_by(|a, b| {
        b["total_ms"]
            .as_u64()
            .unwrap_or(0)
            .cmp(&a["total_ms"].as_u64().unwrap_or(0))
    });
    out
}

/// 时间分桶趋势(bucket_secs 默认 3600):按 ts 桶累加 count/sum(调用方已按 digest 过滤)
pub fn trend_rows(rows: &[Value], bucket_secs: u64) -> Vec<Value> {
    let bucket = bucket_secs.max(60);
    let mut m: HashMap<u64, (u64, u64)> = HashMap::new();
    for r in rows {
        let ts = r["ts"].as_u64().unwrap_or(0);
        let b = ts / bucket * bucket;
        let e = m.entry(b).or_default();
        e.0 += r["count_star"].as_u64().unwrap_or(0);
        e.1 += r["sum_ms"].as_u64().unwrap_or(0);
    }
    let mut out: Vec<Value> = m
        .into_iter()
        .map(|(ts, (count, sum))| json!({ "ts": ts, "count": count, "sum_ms": sum }))
        .collect();
    out.sort_by_key(|v| v["ts"].as_u64().unwrap_or(0));
    out
}

/// 治理规则:总耗时/次数/均值是否命中(阈值 env;全部 0 关闭)
pub fn rule_hit(total_ms: u64, count: u64, avg_ms: u64) -> bool {
    let min_count = env_u64("RDSCTL_SLOW_MIN_COUNT", 10);
    if count < min_count {
        return false;
    }
    let avg_t = env_u64("RDSCTL_SLOW_AVG_MS", 1000);
    let max_t = env_u64("RDSCTL_SLOW_MAX_MS", 5000);
    avg_ms >= avg_t || total_ms >= max_t
}

// ─── 规则版建议(dba-ai-design §4.2;纯函数,可单测,不进 LLM)───

/// WHERE/ORDER BY 终止词(收集谓词列时截断用)
const CLAUSE_BOUNDS: &[&str] = &[
    " order by ", " group by ", " limit ", " offset ", " having ", " union ",
    " returning ", " fetch ", " for ", " window ",
];

/// 保留词/非列 token(启发式过滤;仅作建议提示,非完备 SQL 解析)
const NON_COLS: &[&str] = &[
    "select", "from", "where", "order", "group", "by", "limit", "offset", "having", "union",
    "and", "or", "not", "in", "like", "between", "is", "null", "asc", "desc", "true", "false",
    "then", "when", "else", "end", "case", "join", "left", "right", "inner", "outer", "on", "as",
    "distinct", "if", "coalesce", "count", "sum", "avg", "min", "max", "date", "interval",
    "exists", "values", "set", "update", "delete", "insert", "into", "for", "wait",
];

/// 从规范化 digest_text 提取 WHERE/ORDER BY 谓词列候选(dba-ai-design §4.2 R2 启发版)。
/// 保守启发:取 where / order by 之后至终止词前的标识符,去重、去关键字/表限定,≤4 个;
/// 结果仅供"索引候选"提示,最终需 EXPLAIN/schema 复核(不做 SQL 完备解析)。
pub fn candidate_columns(text: &str) -> Vec<String> {
    let lower = text.to_lowercase();
    let mut segs: Vec<&str> = Vec::new();
    if let Some(i) = lower.find("where") {
        segs.push(cut_at_bound(&lower[i + 5..]));
    }
    if let Some(i) = lower.find("order by") {
        segs.push(cut_at_bound(&lower[i + 8..]));
    }
    let mut out: Vec<String> = Vec::new();
    for seg in segs {
        let mut tok = String::new();
        let flush = |tok: &mut String, out: &mut Vec<String>| {
            if tok.is_empty() {
                return;
            }
            let t = tok.replace('`', "");
            let col = t.split('.').last().unwrap_or("").to_string();
            let t = col.trim().to_string();
            tok.clear();
            if t.is_empty()
                || t.chars().next().map_or(true, |c| !(c.is_ascii_alphabetic() || c == '_'))
                || t.chars().any(|c| !(c.is_ascii_alphanumeric() || c == '_'))
                || NON_COLS.contains(&t.as_str())
                || out.iter().any(|x| x == &t)
            {
                return;
            }
            out.push(t);
        };
        for c in seg.chars() {
            if c.is_ascii_alphanumeric() || c == '_' || c == '`' || c == '.' {
                tok.push(c);
            } else {
                flush(&mut tok, &mut out);
            }
        }
        flush(&mut tok, &mut out);
    }
    out.truncate(4);
    out
}

fn cut_at_bound(seg: &str) -> &str {
    let mut end = seg.len();
    for b in CLAUSE_BOUNDS {
        if let Some(i) = seg.find(b) {
            end = end.min(i);
        }
    }
    &seg[..end]
}

/// 组装规则版建议(dba-ai-design §4.2;命中 avg/max/rising/列候选 → 常量文本目录)。
/// 阈值显式传入(调用点读 env),保持纯函数可单测。rising_pct 为环比增长百分比(无则 None)。
pub fn build_advice(
    digest_text: &str,
    total_ms: u64,
    count: u64,
    avg_ms: u64,
    max_ms: u64,
    avg_t: u64,
    max_t: u64,
    rising_pct: Option<u64>,
) -> Value {
    let mut reasons: Vec<String> = Vec::new();
    let mut suggestions: Vec<Value> = Vec::new();
    if avg_t > 0 && avg_ms >= avg_t {
        reasons.push(format!("平均耗时 {avg_ms}ms ≥ {avg_t}ms"));
        suggestions.push(json!({"type": "verify", "detail": "平均耗时超阈值:用 EXPLAIN 复核执行计划(表行数与索引覆盖优先)"}));
    }
    if max_t > 0 && max_ms >= max_t {
        reasons.push(format!("单次峰值 {max_ms}ms ≥ {max_t}ms"));
    }
    if let Some(p) = rising_pct {
        if p > 0 {
            reasons.push(format!("窗口总耗时环比上升 {p}%"));
            suggestions.push(json!({"type": "verify", "detail": "耗时上升:核查窗口内变更(发布/新流量/参数/索引失效)后再处置"}));
        }
    }
    if total_ms > 0 && count > 0 {
        let cols = candidate_columns(digest_text);
        if !cols.is_empty() {
            suggestions.push(json!({
                "type": "index",
                "detail": format!("WHERE/ORDER BY 引用列 [{}]:请与实例现有索引比对(以 EXPLAIN 复核为准)", cols.join(", "))
            }));
        }
    }
    suggestions.truncate(3);
    if suggestions.is_empty() {
        suggestions.push(json!({"type": "verify", "detail": "进入治理队列:建议在查询台 EXPLAIN 复核并跟踪窗口趋势"}));
    }
    json!({
        "rising_pct": rising_pct,
        "reasons": reasons,
        "suggestions": suggestions,
    })
}

/// 前序窗口(ts < cut_ts)各 digest 总耗时(上升异常判定输入;纯函数)
pub fn prev_total_ms(rows_prev: &[Value], cut_ts: u64) -> HashMap<String, u64> {
    let mut m: HashMap<String, u64> = HashMap::new();
    for r in rows_prev {
        let ts = r["ts"].as_u64().unwrap_or(0);
        if ts >= cut_ts {
            continue;
        }
        let Some(digest) = r["digest"].as_str() else {
            continue;
        };
        *m.entry(digest.to_string()).or_insert(0) += r["sum_ms"].as_u64().unwrap_or(0);
    }
    m
}

fn window_since(window: &str, n: u64) -> Option<u64> {
    match window {
        "" | "24h" => Some(n.saturating_sub(24 * 3600)),
        "7d" => Some(n.saturating_sub(7 * 86400)),
        _ => None,
    }
}

// ─── 采集 ───

/// digest 可用性自检:确认 performance_schema 语句摘要为何“无数据/采集失败”，
/// 兼容 MySQL 5.6/5.7/8.0(该表非 8.0 专属,5.7 默认可用)。
async fn digest_selfcheck(container: &str) -> String {
    // 1) 表可用性(不存在/权限 → 版本或 PS 未开启)
    let cnt = dk::exec_mysql_local(
        container,
        "root",
        ROOT_PASS,
        "SELECT COUNT(*) FROM performance_schema.events_statements_summary_by_digest",
    )
    .await;
    match cnt {
        Err(e) => {
            let el = e.to_lowercase();
            return if el.contains("doesn't exist")
                || el.contains("1146")
                || el.contains("unknown table")
                || el.contains("access denied")
                || el.contains("denied")
            {
                format!(
                    "节点 digest 表不可用: {} (常见:MySQL 5.5 及更早 / performance_schema 未开启或未初始化 / 权限不足)",
                    redact(&e)
                )
            } else {
                format!("节点查询失败: {}", redact(&e))
            };
        }
        Ok(o) => {
            if o.trim().parse::<u64>().unwrap_or(0) > 0 {
                return String::new(); // 有数据:正常
            }
        }
    }
    // 2) 表存在但为空 → 逐项定位
    let val_of = |out: &str| -> String {
        out.lines()
            .next()
            .unwrap_or("")
            .split('\t')
            .last()
            .unwrap_or("")
            .trim()
            .to_string()
    };
    if let Ok(o) = dk::exec_mysql_local(container, "root", ROOT_PASS, "SHOW VARIABLES LIKE 'performance_schema'").await
    {
        if val_of(&o).eq_ignore_ascii_case("OFF") {
            return "performance_schema=OFF(digest 汇总不采集;需开启并重启 MySQL)".to_string();
        }
    }
    if let Ok(o) = dk::exec_mysql_local(
        container,
        "root",
        ROOT_PASS,
        "SHOW VARIABLES LIKE 'performance_schema_digests_size'",
    )
    .await
    {
        if val_of(&o) == "0" {
            return "performance_schema_digests_size=0(digest 不记录)".to_string();
        }
    }
    if let Ok(o) = dk::exec_mysql_local(
        container,
        "root",
        ROOT_PASS,
        "SELECT COUNT(*) FROM performance_schema.setup_instruments \
         WHERE NAME LIKE 'statement/sql/%' AND ENABLED='NO'",
    )
    .await
    {
        if o.trim().parse::<u64>().unwrap_or(0) > 0 {
            return "语句级 instrument(statement/sql/*)被禁用(digest 不累计)".to_string();
        }
    }
    "digest 表当前为空:实例上尚未实际执行过 SQL,或表被 TRUNCATE/重置(产生真实语句后会自动累计)".to_string()
}

/// 限频诊断日志(每实例节点 10 分钟内至多一条,避免周期采集刷屏)
static SLOW_DIAG: OnceLock<Mutex<HashMap<String, u64>>> = OnceLock::new();
fn diag_rate_ok(key: &str) -> bool {
    let now_t = now();
    let g = SLOW_DIAG.get_or_init(|| Mutex::new(HashMap::new()));
    let mut map = g.lock().unwrap();
    let last = *map.get(key).unwrap_or(&0);
    if now_t.saturating_sub(last) >= 600 {
        map.insert(key.to_string(), now_t);
        true
    } else {
        false
    }
}

/// 采集异常/空数据时给出“明确原因”(不静默):查询失败原样告警;表空给出可用性诊断
async fn diagnose_node(inst: &str, container: &str, err: Option<String>) {
    let key = format!("{inst}|{container}");
    if !diag_rate_ok(&key) {
        return;
    }
    let why = digest_selfcheck(container).await;
    let msg = match err {
        Some(e) => format!("慢查采集失败: {e}{}", if why.is_empty() { String::new() } else { format!("; 诊断:{why}") }),
        None if !why.is_empty() => format!("慢查 digest 无数据诊断: {why}"),
        None => return,
    };
    tracing::warn!("instance={inst} node={container} {msg}");
}

fn val_str(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        _ => v.as_str().map(String::from).unwrap_or_default(),
    }
}

fn redact(s: &str) -> String {
    s.replace(ROOT_PASS, "***").chars().take(300).collect()
}

fn val_u64(v: &Value) -> u64 {
    if let Some(s) = v.as_str() {
        s.trim().parse().unwrap_or(0)
    } else {
        v.as_u64().unwrap_or(0)
    }
}

/// 采集一轮 running 实例(master+offline;RDSCTL_SLOW_NODES=all 追加 read)
async fn collect_once() {
    let m = manager();
    let scan: Vec<crate::instance::RdsInstance> = m
        .instances
        .iter()
        .filter(|e| e.value().status == InstStatus::Running)
        .map(|e| e.value().clone())
        .collect();
    if scan.is_empty() {
        return;
    }
    let baselines: HashMap<(String, String, String), (u64, u64)> = m
        .store
        .slow_baselines_load()
        .into_iter()
        .map(|b: SlowBase| {
            (
                (b.instance, b.node, b.digest),
                (b.count_star, b.sum_ms),
            )
        })
        .collect();
    let all_nodes = std::env::var("RDSCTL_SLOW_NODES").as_deref() == Ok("all");
    let timeout = env_u64("RDSCTL_QUERY_TIMEOUT_SECS", 20).max(10);

    for inst in &scan {
        let mut targets: Vec<(String, String)> = Vec::new(); // (container, role标签)
        if let Some(mc) = inst.master() {
            targets.push((mc.container.clone(), "master".into()));
        }
        for n in &inst.nodes {
            if n.role.is_offline() {
                targets.push((n.container.clone(), "offline".into()));
            } else if all_nodes && n.role == Role::Read {
                targets.push((n.container.clone(), "read".into()));
            }
        }
        for (container, role) in targets {
            let out = match dk::query_table(&container, "root", ROOT_PASS, DIGEST_SQL, timeout, "").await
            {
                Ok(o) => o,
                Err(e) => {
                    // 不再静默:给出可操作的失败原因(容器不可达/表不可用/权限等)
                    diagnose_node(&inst.name, &container, Some(redact(&e))).await;
                    continue;
                }
            };
            let parsed = parse_table(&out, 5000, 64 * 1024 * 1024);
            if parsed.rows.is_empty() {
                // 表存在但零行(如 5.7 摘要尚未累计):限频给出可用性诊断,避免“无数据”无人知晓
                diagnose_node(&inst.name, &container, None).await;
            }
            let cols: Vec<String> = parsed.columns.iter().map(|s| s.to_lowercase()).collect();
            let idx = |name: &str| cols.iter().position(|c| c == name);
            let (Some(i_schema), Some(i_digest), Some(i_text), Some(i_count), Some(i_sum), Some(i_max), Some(i_first), Some(i_last)) =
                (idx("schema_name"), idx("digest"), idx("digest_text"), idx("count_star"), idx("sum_ms"), idx("max_ms"), idx("first_seen"), idx("last_seen"))
            else {
                continue;
            };
            let mut samples: Vec<SlowSample> = Vec::new();
            let mut new_base: Vec<SlowBase> = Vec::new();
            for row in &parsed.rows {
                let digest = val_str(&row[i_digest]);
                if digest.is_empty() {
                    continue;
                }
                let cur_count = val_u64(&row[i_count]);
                let cur_sum = val_u64(&row[i_sum]);
                let (base_count, base_sum) = baselines
                    .get(&(inst.name.clone(), role.clone(), digest.clone()))
                    .copied()
                    .unwrap_or((0, 0));
                if cur_count < base_count || cur_sum < base_sum {
                    // 服务/表重置(重启/TRUNCATE):本轮丢弃,基线重建为当前值
                    new_base.push(SlowBase {
                        instance: inst.name.clone(),
                        node: role.clone(),
                        digest: digest.clone(),
                        count_star: cur_count,
                        sum_ms: cur_sum,
                        seen_at: now(),
                    });
                    continue;
                }
                let delta_count = cur_count - base_count;
                let delta_sum = cur_sum - base_sum;
                if delta_count > 0 {
                    samples.push(SlowSample {
                        instance: inst.name.clone(),
                        node: role.clone(),
                        schema_name: val_str(&row[i_schema]),
                        digest: digest.clone(),
                        digest_text: val_str(&row[i_text]),
                        count_star: delta_count,
                        sum_ms: delta_sum,
                        avg_ms: if delta_count > 0 { delta_sum / delta_count } else { 0 },
                        max_ms: val_u64(&row[i_max]),
                        first_seen: val_u64(&row[i_first]),
                        last_seen: val_u64(&row[i_last]),
                    });
                }
                new_base.push(SlowBase {
                    instance: inst.name.clone(),
                    node: role.clone(),
                    digest: digest.clone(),
                    count_star: cur_count,
                    sum_ms: cur_sum,
                    seen_at: now(),
                });
            }
            m.store.slow_snapshots_insert(&samples);
            m.store.slow_baselines_set(&inst.name, &role, &new_base);
        }
    }
    // 治理规则:最近 24h 全局聚合 → 候选入队;规则版建议(advice_json)随行写入
    // (dba-ai-design §4.2 R1/R3/R4:avg/max 阈值 + GROW_PCT 上升规则启用)
    let n = now();
    let since = n - 24 * 3600;
    let rows = m.store.slow_window(since, None, None, 200_000);
    let prev = prev_total_ms(&m.store.slow_window(n - 48 * 3600, None, None, 200_000), since);
    let min_count = env_u64("RDSCTL_SLOW_MIN_COUNT", 10);
    let avg_t = env_u64("RDSCTL_SLOW_AVG_MS", 1000);
    let max_t = env_u64("RDSCTL_SLOW_MAX_MS", 5000);
    let grow_pct = env_u64("RDSCTL_SLOW_GROW_PCT", 200); // 0 = 关闭上升规则
    let hits = aggregate_window(&rows, min_count);
    for h in hits {
        let total_ms = h["total_ms"].as_u64().unwrap_or(0);
        let count = h["total_count"].as_u64().unwrap_or(0);
        let avg_ms = h["avg_ms"].as_u64().unwrap_or(0);
        let max_ms = h["max_ms"].as_u64().unwrap_or(0);
        let digest = h["digest"].as_str().unwrap_or("");
        let text = h["digest_text"].as_str().unwrap_or("");
        let rising_pct = prev.get(digest).and_then(|p| {
            if *p > 0 && total_ms >= *p {
                Some(((total_ms - *p) * 100) / *p)
            } else {
                None
            }
        });
        let rising = grow_pct > 0 && count >= min_count
            && rising_pct.map_or(false, |v| v >= grow_pct);
        if !rule_hit(total_ms, count, avg_ms) && !rising {
            continue;
        }
        let opened = m.store.slow_gov_ensure(digest, text);
        if opened {
            let why = if rising { "上升" } else { "avg/max" };
            m.store.audit(
                "system",
                "",
                "slow_gov_open",
                &format!(
                    "{} ({}ms/{}次,{why})",
                    digest.chars().take(40).collect::<String>(),
                    total_ms,
                    count
                ),
                "ok",
                "",
            );
        }
        // 规则版建议:新开行必写;既有行仅 rising 变化时刷新(防周期刷写)
        if opened || rising {
            let advice =
                build_advice(text, total_ms, count, avg_ms, max_ms, avg_t, max_t, rising_pct);
            let hit = m.store.slow_gov_advise(digest, &advice.to_string());
            if !hit {
                tracing::debug!("slow_gov_advise 未命中(digest={digest} 治理行可能已 closed)");
            }
        }
    }
}

/// 保留清理(每小时;同时清理治理已解决旧项)
fn prune_once() {
    let retention = env_u64("RDSCTL_SLOW_RETENTION_DAYS", 30).max(1);
    let n = now();
    let (s, g) = manager().store.slow_prune(
        n.saturating_sub(retention * 86400),
        n.saturating_sub(90 * 86400),
    );
    if s + g > 0 {
        tracing::info!("慢查保留清理:快照 {s} 行、治理 {g} 行");
    }
}

/// 启动慢查后台任务(采集 + 保留清理;随 manager() 启动)
/// 手动立即采集一轮(供前端“立即采集”/排障;权限在路由层)
pub async fn collect_now() {
    collect_once().await;
}

pub fn start(mgr: &Arc<RdsManager>) {
    let period = env_u64("RDSCTL_SLOW_SECS", 60).max(10);
    tracing::info!("慢查采集启动:每 {period}s 一次");
    let m1 = Arc::clone(mgr);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(period)).await;
            // 采集用全局 manager(与 http 层同源;m1 仅保持句柄一致)
            let _ = &m1;
            collect_once().await;
        }
    });
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            prune_once();
        }
    });
}

// ─── HTTP 视图组装(api.rs 调用) ───

/// 全局 Top(instances.view;digest_text 需 instances.query,否则占位)
pub fn top_view(window: &str, instance: Option<&str>, min_count: u64, top: usize) -> (u16, Value) {
    let n = now();
    let Some(since) = window_since(window, n) else {
        return (400, json!({ "ok": false, "error": "未知 window(支持 24h/7d)" }));
    };
    let rows = manager()
        .store
        .slow_window(since, instance, None, 200_000);
    let mut items = aggregate_window(&rows, min_count);
    items.truncate(top.max(1).min(500));
    let masked = !crate::auth::has_perm("instances.query");
    if masked {
        for it in &mut items {
            it["digest_text"] = json!("(需 instances.query 权限查看 SQL 摘要)");
        }
    }
    (
        200,
        json!({ "ok": true, "items": items, "window_since": since, "now": n }),
    )
}

/// 单实例明细(同上文本规则)
pub fn instance_view(
    inst: &str,
    window: &str,
    limit: usize,
) -> (u16, Value) {
    let n = now();
    let Some(since) = window_since(window, n) else {
        return (400, json!({ "ok": false, "error": "未知 window(支持 24h/7d)" }));
    };
    let rows = manager().store.slow_window(since, Some(inst), None, 200_000);
    let mut items = aggregate_window(&rows, 0);
    items.sort_by(|a, b| {
        b["total_ms"]
            .as_u64()
            .unwrap_or(0)
            .cmp(&a["total_ms"].as_u64().unwrap_or(0))
    });
    items.truncate(limit.max(1).min(500));
    (
        200,
        json!({ "ok": true, "instance": inst, "items": items, "window_since": since }),
    )
}

/// 某 digest 全局趋势(instances.query 可见文本之外的时序)
pub fn trend_view(digest: &str, window: &str) -> (u16, Value) {
    let n = now();
    let Some(since) = window_since(window, n) else {
        return (400, json!({ "ok": false, "error": "未知 window(支持 24h/7d)" }));
    };
    let rows = manager()
        .store
        .slow_window(since, None, Some(digest), 200_000);
    (
        200,
        json!({ "ok": true, "digest": digest, "points": trend_rows(&rows, 3600) }),
    )
}

/// digest 建议视图(dba-ai-design §4.2/§10.1 规则版;instances.view):
/// 最近窗口聚合 × 治理状态 × 落库规则建议(advice_json);digest_text 按 instances.query 投影。
pub fn advice_view(digest: &str, window: &str, top: usize) -> (u16, Value) {
    let n = now();
    let Some(since) = window_since(window, n) else {
        return (400, json!({ "ok": false, "error": "未知 window(支持 24h/7d)" }));
    };
    let rows = manager().store.slow_window(since, None, None, 200_000);
    let items = aggregate_window(&rows, 0);
    let gov = manager().store.slow_gov_list(2000, None);
    let gov_of: HashMap<&str, &Value> = gov
        .iter()
        .filter_map(|g| g["digest"].as_str().map(|d| (d, g)))
        .collect();
    let masked = !crate::auth::has_perm("instances.query");
    let mut out: Vec<Value> = Vec::new();
    for it in items {
        let d = it["digest"].as_str().unwrap_or("");
        if !digest.is_empty() && d != digest {
            continue;
        }
        let mut o = it.clone();
        if let Some(g) = gov_of.get(d) {
            o["status"] = g["status"].clone();
            o["assignee"] = g["assignee"].clone();
            o["advice"] = g["advice"].clone();
        }
        if masked {
            o["digest_text"] = json!("(需 instances.query 权限查看 SQL 摘要)");
        }
        out.push(o);
        if digest.is_empty() && out.len() >= top.max(1).min(500) {
            break;
        }
    }
    if !digest.is_empty() && out.is_empty() {
        return (
            404,
            json!({ "ok": false, "error": "该 digest 窗口内无数据或未命中治理规则" }),
        );
    }
    (200, json!({ "ok": true, "items": out, "window_since": since, "now": n }))
}

// ─── 单元测试(纯聚合/趋势/规则;无 docker/MySQL) ───

#[cfg(test)]
mod tests {
    use super::*;

    fn row(ts: u64, inst: &str, digest: &str, text: &str, count: u64, sum: u64, max: u64) -> Value {
        json!({
            "ts": ts, "instance": inst, "node": "master", "schema_name": "appdb",
            "digest": digest, "digest_text": text, "count_star": count,
            "sum_ms": sum, "avg_ms": if count > 0 { sum / count } else { 0 },
            "max_ms": max, "first_seen": ts, "last_seen": ts,
        })
    }

    #[test]
    fn aggregate_across_instances() {
        let rows = vec![
            row(1, "a", "d1", "select * from t where a=?", 10, 5000, 800),
            row(1, "b", "d1", "select * from t where a=?", 20, 9000, 900),
            row(1, "a", "d2", "update t set x=? where id=?", 3, 300, 120),
        ];
        let items = aggregate_window(&rows, 0);
        assert_eq!(items.len(), 2);
        let d1 = items.iter().find(|i| i["digest"] == "d1").unwrap();
        assert_eq!(d1["total_count"], 30);
        assert_eq!(d1["total_ms"], 14000);
        assert_eq!(d1["instance_count"], 2);
        assert_eq!(d1["max_ms"], 900);
        // min_count 过滤
        let items = aggregate_window(&rows, 5);
        assert_eq!(items.len(), 1);
    }

    #[test]
    fn trend_buckets() {
        let rows = vec![
            row(3600, "a", "d1", "t", 1, 100, 100),
            row(7200, "b", "d1", "t", 2, 400, 300),
            row(7200, "a", "d1", "t", 1, 100, 100),
        ];
        let pts = trend_rows(&rows, 3600);
        assert_eq!(pts.len(), 2);
        assert_eq!(pts[0]["ts"], 3600);
        assert_eq!(pts[1]["count"], 3);
        assert_eq!(pts[1]["sum_ms"], 500);
    }

    #[test]
    fn rule_thresholds() {
        assert!(!rule_hit(500, 3, 100)); // 次数不足
        assert!(rule_hit(10_000, 20, 400)); // 总耗时超
        assert!(rule_hit(5_000, 20, 1_100)); // 均值超
        assert!(!rule_hit(4_000, 20, 900)); // 都不超
    }

    #[test]
    fn candidate_columns_heuristic() {
        let cols = candidate_columns(
            "select * from `appdb`.`orders` where status = ? and created_at > ? order by id desc limit ?",
        );
        assert_eq!(cols, vec!["status", "created_at", "id"]);
        // 无 where/order by → 空;保留词/表名/字面量不进候选
        assert!(candidate_columns("select 1").is_empty());
        let upd = candidate_columns("update t set x = ? where tenant_id = ? and name like ?");
        assert!(upd.contains(&"tenant_id".to_string()));
        assert!(upd.contains(&"name".to_string())); // 非保留词即候选(列名提示用)
        assert!(!upd.iter().any(|c| c == "like" || c == "update"));
        // 去重 + 上限
        assert_eq!(candidate_columns("select * from t where a = ? and a = ? and b = ? and c = ? and d = ? and e = ?").len(), 4);
    }

    #[test]
    fn advice_reasons_and_suggestions() {
        // 未超阈值(阈值显式传入)→ 无原因,仅兜底 verify
        let a = build_advice("select count(*) from t", 5_000, 20, 200, 900, 1_000, 5_000, None);
        assert!(a["reasons"].as_array().unwrap().is_empty());
        let sug = a["suggestions"].as_array().unwrap();
        assert_eq!(sug[0]["type"], "verify");
        // avg + rising 命中 → 原因/建议/rising_pct
        let a2 = build_advice(
            "select * from t where a = ?", 20_000, 20, 1_000, 3_000,
            1_000, 5_000, Some(300),
        );
        let txt: String = a2["reasons"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r.as_str().unwrap_or(""))
            .collect::<Vec<_>>()
            .join("|");
        assert!(txt.contains("平均耗时"));
        assert!(txt.contains("环比上升 300%"));
        assert_eq!(a2["rising_pct"], json!(300));
        let sug2 = a2["suggestions"].as_array().unwrap();
        assert!(sug2.iter().any(|s| s["type"] == "index"));
        assert!(sug2.iter().any(|s| s["type"] == "verify"));
        // 峰值命中
        let a3 = build_advice("select ...", 1_000, 20, 50, 9_000, 1_000, 5_000, None);
        assert!(a3["reasons"].as_array().unwrap()[0].as_str().unwrap().contains("峰值"));
    }

    #[test]
    fn prev_window_totals_cut() {
        let rows = vec![
            row(100, "a", "d1", "t", 5, 500, 100),   // 前序窗(ts < cut)
            row(100, "a", "d2", "t", 1, 40, 40),     // 前序窗
            row(1_000, "a", "d1", "t", 5, 1_000, 200), // 当前窗:不计
        ];
        let m = prev_total_ms(&rows, 500);
        assert_eq!(m.get("d1"), Some(&500));
        assert_eq!(m.get("d2"), Some(&40));
        assert!(m.get("nope").is_none());
    }
}
