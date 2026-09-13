// rdsctl — 容量与成本(规则版,dba-ai-design §6)
//
// 职责(L1 磁盘采样 + 规则外推 + 水位/回收候选,零 LLM、零新依赖):
//   - 采样 ticker:遍历 running 实例仅取 master() 节点,容器内 `df -P /var/lib/mysql`
//     取磁盘 used/total(字节 = 1024-blocks × 1024),连同 parse_gib(data_size) 批量落
//     capacity_samples(env RDSCTL_CAP_SECS,默认 0 = 关闭;>0 启动,周期最小 10s);
//   - 保留清理:每小时按 RDSCTL_CAP_RETENTION_DAYS(默认 90)删除过期采样;
//   - forecast_series:同一 (instance,node) 采样 ≥7 条且跨度 ≥72h 才做最小二乘外推,
//     growth_bytes_per_day = 每秒增速×86400(四舍五入 i64);growth>0 时给出
//     days_to_90pct = ceil((0.9×total−last_used)/growth/day) 与 forecast_at,否则 null;
//   - overview():组全局视图 —— warnings(master 节点 days_to_90pct ≤ RDSCTL_CAP_WARN_DAYS
//     且 pct ≥ 50)/waterline(按 region/shard 实例数外推 days_to_limit,
//     RDSCTL_CAP_SHARD_LIMIT)/recycle(Failed 或 disabled ≥30 天的回收候选,不执行动作);
//   - 采集/解析失败静默降级(debug 日志),不审计不告警;无 running 实例零写入;
//     禁止 panic。
//
// HTTP 接线:overview()/forecast_view() 由 api.rs(/api/rds/capacity*) 调用;
// 采样循环随 manager() 注册(main.rs,slow::start 同款模式)。

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use serde_json::{json, Value};

use crate::docker as dk;
use crate::instance::{InstStatus, RdsInstance, RdsManager};
use crate::manager;
use crate::store::CapSample;

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

// ─── 确定性纯函数(采样解析 / 规格化 / 外推;供接口与单测,无 IO) ───

/// 解析 `df -P <dir>` 输出:跳过头行(表头),取首个数据行。
/// 数据行按空白分隔:col1=1024-blocks(total)、col2=Used、col3=Available;
/// 字节 = 值 × 1024。返回 Some((used_bytes, total_bytes));解析失败返回 None(静默跳过)。
fn parse_df(out: &str) -> Option<(u64, u64)> {
    for line in out.lines().skip(1) {
        let f: Vec<&str> = line.split_whitespace().collect();
        if f.len() < 4 {
            continue;
        }
        let blocks = f[1].parse::<u64>().ok()?;
        let used = f[2].parse::<u64>().ok()?;
        if blocks == 0 {
            return None;
        }
        // df 列值(1024-blocks)远小于 u64 上限,checked 防御
        return Some((
            used.checked_mul(1024).unwrap_or(u64::MAX),
            blocks.checked_mul(1024).unwrap_or(u64::MAX),
        ));
    }
    None
}

/// 规格化容量字符串 → GiB(1024 进制换算)。
/// 规则:
///   - 无单位 → 数值本身即 GiB("1024" → 1024.0);
///   - 二进制单位(iB 后缀)或单字母 K/M/G/T/P → 按 1024 进制(1.5T → 1536.0 GiB);
///   - 十进制字节单位(B/KB/MB/GB/TB/PB)→ 字节数/1024³ 换算 GiB("256GB" ≈ 238.4 GiB);
///   - 大小写不敏感、允许数字与单位间空白;非法输入返回 None。
pub fn parse_gib(s: &str) -> Option<f64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let cut = s
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(s.len());
    let num = &s[..cut];
    if num.is_empty() || !num.chars().any(|c| c.is_ascii_digit()) {
        return None;
    }
    let n: f64 = num.parse().ok()?;
    if !n.is_finite() || n < 0.0 {
        return None;
    }
    let unit: String = s[cut..].trim().to_ascii_lowercase();
    let factor: f64 = match unit.as_str() {
        "" => 1.0, // 无单位:数值即 GiB
        // 二进制(1024 进制)与单字母:K/M/G/T/P == KiB/MiB/GiB/TiB/PiB
        "k" | "kib" => 1.0 / (1024.0 * 1024.0),
        "m" | "mib" => 1.0 / 1024.0,
        "g" | "gib" => 1.0,
        "t" | "tib" => 1024.0,
        "p" | "pib" => 1024.0 * 1024.0,
        // 十进制字节单位(B 后缀)→ GiB
        "b" => 1.0 / (1024.0 * 1024.0 * 1024.0),
        "kb" => 1000.0 / (1024.0 * 1024.0 * 1024.0),
        "mb" => 1_000_000.0 / (1024.0 * 1024.0 * 1024.0),
        "gb" => 1_000_000_000.0 / (1024.0 * 1024.0 * 1024.0),
        "tb" => 1_000_000_000_000.0 / (1024.0 * 1024.0 * 1024.0),
        "pb" => 1_000_000_000_000_000.0 / (1024.0 * 1024.0 * 1024.0),
        _ => return None,
    };
    Some(n * factor)
}

/// 线性最小二乘斜率(disk_used_bytes ~ ts,单位:字节/秒;去均值中心化求数值稳定)。
/// 数据不足两点或退化解(零方差/非有限)→ None。
fn ls_slope_per_sec(rows: &[Value]) -> Option<f64> {
    let n = rows.len();
    if n < 2 {
        return None;
    }
    let mut xs: Vec<f64> = Vec::with_capacity(n);
    let mut ys: Vec<f64> = Vec::with_capacity(n);
    for r in rows {
        xs.push(r["ts"].as_u64()? as f64);
        ys.push(r["disk_used_bytes"].as_u64()? as f64);
    }
    let xm = xs.iter().sum::<f64>() / n as f64;
    let ym = ys.iter().sum::<f64>() / n as f64;
    let mut num = 0.0f64;
    let mut den = 0.0f64;
    for i in 0..n {
        let xc = xs[i] - xm;
        let yc = ys[i] - ym;
        num += xc * yc;
        den += xc * xc;
    }
    if den <= 0.0 || !num.is_finite() {
        return None;
    }
    let slope = num / den;
    if !slope.is_finite() {
        return None;
    }
    Some(slope)
}

fn insufficient_json(n: usize, span_secs: u64) -> Value {
    json!({ "insufficient": true, "n": n, "span_secs": span_secs })
}

fn pct1(x: f64) -> f64 {
    (x * 10.0).round() / 10.0
}

/// 同一 (instance,node) 采样(已按 ts 升序,含 {ts,disk_used_bytes,disk_total_bytes,...})
/// 的规则外推(dba-ai-design §6.2 线性版;纯函数)。
///   - 样本 <7 或时间跨度 <72h → {"insufficient":true,"n","span_secs"};
///   - total 取末行 disk_total_bytes(≤0 → insufficient true);
///   - growth_bytes_per_day = 每秒增速×86400 四舍五入 i64;≤0(负增长/零增长)→ 不预警,
///     days_to_90pct/forecast_at 为 null;
///   - pct = last_used/total×100(1 位小数);days_to_90pct=ceil((0.9×total−last_used)/growth/day)
///     仅当 growth>0(已 ≥90% 时按 0 天处理);forecast_at=last_ts+days×86400。
pub fn forecast_series(rows: &[Value]) -> Value {
    let n = rows.len();
    let ts0 = rows.first().and_then(|r| r["ts"].as_u64()).unwrap_or(0);
    let ts1 = rows.last().and_then(|r| r["ts"].as_u64()).unwrap_or(0);
    let span = ts1.saturating_sub(ts0);
    let node = rows
        .first()
        .and_then(|r| r["node"].as_str())
        .unwrap_or("")
        .to_string();
    if n < 7 || span < 72 * 3600 {
        return insufficient_json(n, span);
    }
    let total = rows
        .last()
        .and_then(|r| r["disk_total_bytes"].as_u64())
        .unwrap_or(0);
    let last_used = rows
        .last()
        .and_then(|r| r["disk_used_bytes"].as_u64())
        .unwrap_or(0);
    if total == 0 {
        return insufficient_json(n, span);
    }
    let Some(slope) = ls_slope_per_sec(rows) else {
        return insufficient_json(n, span);
    };
    let growth_day = slope * 86400.0; // 字节/天(f64,未取整,用于天数推算)
    let growth_i = growth_day.round() as i64; // 四舍五入
    let pct = pct1(last_used as f64 * 100.0 / total as f64);
    let (days, forecast_at): (Option<i64>, Option<u64>) = if growth_day > 0.0 {
        let need = 0.9 * total as f64 - last_used as f64; // 距 90% 尚需字节
        if need <= 0.0 {
            // 已 ≥90%:按 0 天处理(立即预警)
            (Some(0), Some(ts1))
        } else {
            let d = (need / growth_day).ceil() as i64;
            let at = ts1.saturating_add((d as u64).saturating_mul(86400));
            (Some(d), Some(at))
        }
    } else {
        // 负增长/零增长 = 不预警
        (None, None)
    };
    json!({
        "insufficient": false,
        "n": n,
        "span_secs": span,
        "growth_bytes_per_day": growth_i,
        "total_bytes": total,
        "last_used_bytes": last_used,
        "pct": pct,
        "days_to_90pct": days,
        "forecast_at": forecast_at,
        "node": node,
    })
}

/// 降采样:rows 已按 ts 升序;len ≤ max 原样返回,否则每 stride 取 1(stride=ceil(n/max)),
/// 从末位对齐取点(保证最后一个样本总在序列内),点数 ≤ max。
fn downsample(rows: &[Value], max: usize) -> Vec<Value> {
    let n = rows.len();
    let max = max.max(1);
    if n <= max {
        return rows.to_vec();
    }
    let stride = (n + max - 1) / max;
    let offset = (n - 1) % stride;
    let mut out = Vec::with_capacity(max);
    let mut i = offset;
    while i < n {
        out.push(rows[i].clone());
        i += stride;
    }
    out
}

// ─── 采样 ticker(dba-ai-design §6.1 L1;随 manager() 注册,slow::start 同款) ───

/// 采样一轮 running 实例的 master 节点(`df -P /var/lib/mysql`);失败静默 debug。
async fn collect_once() {
    let m = manager();
    let scan: Vec<RdsInstance> = m
        .instances
        .iter()
        .filter(|e| e.value().status == InstStatus::Running)
        .map(|e| e.value().clone())
        .collect();
    if scan.is_empty() {
        return; // 无 running 实例 → 零写入
    }
    let mut samples: Vec<CapSample> = Vec::new();
    for inst in &scan {
        let Some(mc) = inst.master() else {
            continue;
        };
        let args = [
            String::from("df"),
            String::from("-P"),
            String::from("/var/lib/mysql"),
        ];
        let out = match dk::exec_in(&mc.container, &args).await {
            Ok(o) => o,
            Err(e) => {
                tracing::debug!(
                    "容量采样失败 instance={} container={}: {}",
                    inst.name,
                    mc.container,
                    e
                );
                continue;
            }
        };
        let Some((used, total)) = parse_df(&out) else {
            tracing::debug!(
                "容量采样 df 输出解析失败 instance={} container={}",
                inst.name,
                mc.container
            );
            continue;
        };
        samples.push(CapSample {
            instance: inst.name.clone(),
            node: "master".to_string(),
            disk_used_bytes: used,
            disk_total_bytes: total,
            data_gib: parse_gib(&inst.data_size).unwrap_or(0.0),
        });
    }
    if samples.is_empty() {
        return;
    }
    m.store.capacity_insert(&samples);
}

/// 保留清理(每小时):按 RDSCTL_CAP_RETENTION_DAYS(默认 90)删除过期采样
fn prune_once() {
    let retention = env_u64("RDSCTL_CAP_RETENTION_DAYS", 90).max(1);
    let n = manager()
        .store
        .capacity_prune(now().saturating_sub(retention * 86400));
    if n > 0 {
        tracing::info!("容量保留清理:删除 {n} 行采样(保留 {retention} 天)");
    }
}

/// 启动容量后台任务(RDSCTL_CAP_SECS 默认 0 = 关闭;>0 时启动采样循环,周期最小 10s)
/// 与每小时保留清理。采样/清理用全局 manager()(与 http 层同源;mgr 仅保持句柄)。
pub fn start(mgr: &Arc<RdsManager>) {
    let secs = env_u64("RDSCTL_CAP_SECS", 0);
    if secs == 0 {
        tracing::info!("容量采样关闭(RDSCTL_CAP_SECS=0)");
        return;
    }
    let period = secs.max(10);
    tracing::info!("容量采样启动:每 {period}s 一次(规则版 dba-ai-design §6)");
    let m1 = Arc::clone(mgr);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(period)).await;
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

// ─── HTTP 视图(供容量页与周报;api.rs/http.rs 接线) ───

/// 组全局视图(dba-ai-design §6;容量卡组 + 周报数据源):
///   - warnings:running 实例近 30 天采样(master 节点)外推命中
///     days_to_90pct ≤ RDSCTL_CAP_WARN_DAYS(默认 14)且 pct ≥ 50;
///     按 days_to_90pct 升序,截断 20;
///   - waterline:按 (region,shard) 实例数 + 最早 created_at 外推
///     days_to_limit(RDSCTL_CAP_SHARD_LIMIT 默认 10000),按 count 降序;
///   - recycle:Failed 或 disabled ≥30 天的回收候选(仅提示,不执行动作;≤20,按 age 降序)。
pub fn overview() -> Value {
    let m = manager();
    let now_t = now();
    let warn_days = env_u64("RDSCTL_CAP_WARN_DAYS", 14);
    let shard_limit = env_u64("RDSCTL_CAP_SHARD_LIMIT", 10_000);
    let since = now_t.saturating_sub(30 * 86400);

    // ── warnings ──
    let running: Vec<RdsInstance> = m
        .instances
        .iter()
        .filter(|e| e.value().status == InstStatus::Running)
        .map(|e| e.value().clone())
        .collect();
    let mut warns: Vec<Value> = Vec::new();
    for inst in &running {
        let rows = m.store.capacity_since(Some(&inst.name), since, 100_000);
        let master_rows: Vec<Value> = rows
            .into_iter()
            .filter(|r| r["node"].as_str() == Some("master"))
            .collect();
        if master_rows.is_empty() {
            continue;
        }
        let fc = forecast_series(&master_rows);
        if fc["insufficient"].as_bool().unwrap_or(true) {
            continue;
        }
        let Some(days) = fc["days_to_90pct"].as_i64() else {
            continue; // 负增长/零增长:不预警
        };
        if days < 0 {
            continue;
        }
        let pct = fc["pct"].as_f64().unwrap_or(0.0);
        if pct >= 50.0 && days as u64 <= warn_days {
            let data_gib = master_rows
                .last()
                .and_then(|r| r["data_gib"].as_f64())
                .unwrap_or(0.0);
            warns.push(json!({
                "instance": inst.name,
                "node": "master",
                "pct": fc["pct"],
                "days_to_90pct": fc["days_to_90pct"],
                "growth_bytes_per_day": fc["growth_bytes_per_day"],
                "data_gib": data_gib,
            }));
        }
    }
    warns.sort_by(|a, b| {
        a["days_to_90pct"]
            .as_i64()
            .cmp(&b["days_to_90pct"].as_i64())
            .then_with(|| a["instance"].as_str().cmp(&b["instance"].as_str()))
    });
    warns.truncate(20);

    // ── waterline(实例数水位;按 region/shard) ──
    #[derive(Clone, Copy)]
    struct Grp {
        count: u64,
        created: u64,
    }
    let mut groups: HashMap<(String, String), Grp> = HashMap::new();
    for e in m.instances.iter() {
        let i = e.value();
        let g = groups
            .entry((i.region.clone(), i.shard.clone()))
            .or_insert(Grp {
                count: 0,
                created: now_t,
            });
        g.count += 1;
        g.created = g.created.min(i.created_at);
    }
    let mut waterline: Vec<Value> = groups
        .into_iter()
        .map(|((region, shard), g)| {
            let span_days = (now_t.saturating_sub(g.created) / 86400).max(1);
            // growth_per_day ≈ count/span_days
            let growth = g.count as f64 / span_days as f64;
            let days = if g.count >= shard_limit {
                0
            } else {
                ((shard_limit - g.count) as f64 / growth).ceil() as u64
            };
            json!({ "region": region, "shard": shard, "count": g.count, "days_to_limit": days })
        })
        .collect();
    waterline.sort_by(|a, b| {
        b["count"]
            .as_u64()
            .cmp(&a["count"].as_u64())
            .then_with(|| a["region"].as_str().cmp(&b["region"].as_str()))
            .then_with(|| a["shard"].as_str().cmp(&b["shard"].as_str()))
    });

    // ── recycle(回收候选提示;不执行动作) ──
    let mut recycle: Vec<Value> = Vec::new();
    for e in m.instances.iter() {
        let i = e.value();
        let age_days = now_t.saturating_sub(i.created_at) / 86400;
        let hit = i.status == InstStatus::Failed || (!i.enabled && age_days >= 30);
        if hit {
            recycle.push(json!({
                "name": i.name,
                "status": i.status,
                "age_days": age_days,
                "created_at": i.created_at,
            }));
        }
    }
    recycle.sort_by(|a, b| {
        b["age_days"]
            .as_u64()
            .cmp(&a["age_days"].as_u64())
            .then_with(|| a["name"].as_str().cmp(&b["name"].as_str()))
    });
    recycle.truncate(20);

    json!({
        "ok": true,
        "generated_at": now_t,
        "warnings": warns,
        "waterline": waterline,
        "recycle": recycle,
    })
}

/// 单实例各节点外推 + 原始点(供容量 /capacity/forecast 接口)。
/// 实例不存在 → (404, {ok:false,error});存在但无采样 → (200, 空 series)。
/// 采样窗口近 30 天(与 overview 一致);每节点:forecast_series 全量外推,
/// points = [{ts,disk_used_bytes}] 升序降采样 ≤200。
pub fn forecast_view(instance: &str) -> (u16, Value) {
    let m = manager();
    if !m.instances.contains_key(instance) {
        return (
            404,
            json!({ "ok": false, "error": format!("实例不存在: {instance}") }),
        );
    }
    let since = now().saturating_sub(30 * 86400);
    let rows = m.store.capacity_since(Some(instance), since, 100_000);
    let mut by_node: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    for r in rows {
        let node = r["node"].as_str().unwrap_or("").to_string();
        if node.is_empty() {
            continue;
        }
        by_node.entry(node).or_default().push(r);
    }
    let series: Vec<Value> = by_node
        .into_iter()
        .map(|(node, node_rows)| {
            let points: Vec<Value> = downsample(&node_rows, 200)
                .into_iter()
                .map(|r| {
                    json!({
                        "ts": r["ts"],
                        "disk_used_bytes": r["disk_used_bytes"],
                    })
                })
                .collect();
            json!({
                "node": node,
                "forecast": forecast_series(&node_rows),
                "points": points,
            })
        })
        .collect();
    (
        200,
        json!({ "ok": true, "instance": instance, "series": series }),
    )
}

// ─── 单元测试(纯函数/解析;无 docker/MySQL,同 slow.rs 风格) ───

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1024 * 1024 * 1024;

    /// 构造一条采样行(节点级;disk_total 恒定,disk_used 递增)
    fn cap_row(ts: u64, used: u64, total: u64) -> Value {
        json!({
            "ts": ts,
            "instance": "demo",
            "node": "master",
            "disk_used_bytes": used,
            "disk_total_bytes": total,
            "data_gib": 64.0,
        })
    }

    #[test]
    fn parse_gib_valid_and_invalid() {
        // 二进制后缀/单字母
        assert_eq!(parse_gib("128GiB"), Some(128.0));
        assert_eq!(parse_gib("8G"), Some(8.0));
        assert_eq!(parse_gib("1.5T"), Some(1536.0));
        assert_eq!(parse_gib("1.5tib"), Some(1536.0));
        assert_eq!(parse_gib("512M"), Some(0.5));
        assert_eq!(parse_gib("2KiB"), Some(2.0 / 1024.0 / 1024.0));
        // 十进制字节单位 → GiB(1024 进制换算):256GB ≈ 238.4 GiB
        let gb = parse_gib("256GB").unwrap();
        assert!((gb - 238.41858).abs() < 0.01, "256GB -> {gb}");
        let mb = parse_gib("1024MB").unwrap();
        assert!((mb - 0.9536743).abs() < 1e-5, "1024MB -> {mb}");
        let b = parse_gib("1073741824B").unwrap();
        assert!((b - 1.0).abs() < 1e-9);
        // 无单位 = GiB
        assert_eq!(parse_gib("1024"), Some(1024.0));
        // 大小写/空白不敏感
        assert_eq!(parse_gib(" 64 gIb "), Some(64.0));
        // 非法输入 → None
        assert_eq!(parse_gib(""), None);
        assert_eq!(parse_gib("abc"), None);
        assert_eq!(parse_gib("GiB"), None);
        assert_eq!(parse_gib("12.5XB"), None);
        assert_eq!(parse_gib("1.2.3GiB"), None);
        assert_eq!(parse_gib("-128GiB"), None);
    }

    #[test]
    fn parse_df_ok_and_fail() {
        // 标准 df -P 输出:首行表头,次行数据(值×1024 → 字节)
        let out = "Filesystem     1024-blocks      Used Available Capacity Mounted on\n\
                   /dev/sda1       104857600  52428800  52428800    50% /var/lib/mysql\n";
        let (used, total) = parse_df(out).unwrap();
        assert_eq!(used, 52_428_800 * 1024); // 50GiB
        assert_eq!(total, 104_857_600 * 1024); // 100GiB
                                               // 无数据行(仅表头)/垃圾 → None
        assert_eq!(
            parse_df("Filesystem     1024-blocks      Used Available Capacity Mounted on\n"),
            None
        );
        assert_eq!(parse_df("garbage\n"), None);
        assert_eq!(parse_df(""), None);
        // 数据行非数字 → None
        let bad = "Filesystem 1024-blocks Used Available Capacity Mounted on\n/dev/sda1  x y 50% /var/lib/mysql\n";
        assert_eq!(parse_df(bad), None);
    }

    #[test]
    fn forecast_insufficient_few_samples() {
        // 6 样本(即使跨度足够)→ insufficient
        let rows: Vec<Value> = (0..6)
            .map(|i| cap_row(1_700_000_000 + i * 86_400, 10 * GIB, 64 * GIB))
            .collect();
        let v = forecast_series(&rows);
        assert_eq!(v["insufficient"], true);
        assert_eq!(v["n"], 6);
        assert_eq!(v["span_secs"], 5 * 86_400);
    }

    #[test]
    fn forecast_insufficient_short_span() {
        // 8 样本但跨度 42h < 72h → insufficient
        let rows: Vec<Value> = (0..8)
            .map(|i| {
                cap_row(
                    1_700_000_000 + i * 21_600,
                    10 * GIB + i * (GIB / 4),
                    64 * GIB,
                )
            })
            .collect();
        let v = forecast_series(&rows);
        assert_eq!(v["insufficient"], true);
        assert_eq!(v["n"], 8);
        assert_eq!(v["span_secs"], 7 * 21_600);
    }

    #[test]
    fn forecast_growth_and_days() {
        // 8 样本跨 4 天(每 12h 一条),每天 disk_used +1GiB(严格线性)
        let base = 1_700_000_000u64;
        let rows: Vec<Value> = (0..8)
            .map(|i| cap_row(base + i * 43_200, 10 * GIB + i * (GIB / 2), 64 * GIB))
            .collect();
        let v = forecast_series(&rows);
        assert_eq!(v["insufficient"], false);
        assert_eq!(v["n"], 8);
        assert_eq!(v["span_secs"], 7 * 43_200);
        // growth ≈ 1GiB/天(1024^3 字节/天)
        let g = v["growth_bytes_per_day"].as_i64().unwrap();
        assert!((g - (GIB as i64)).abs() <= 1, "growth={g}");
        // total/last_used
        assert_eq!(v["total_bytes"], 64 * GIB);
        assert_eq!(v["last_used_bytes"], 10 * GIB + 7 * (GIB / 2));
        // pct = 13.5GiB / 64GiB ≈ 21.1
        let pct = v["pct"].as_f64().unwrap();
        assert!((pct - 21.1).abs() < 0.05, "pct={pct}");
        // days_to_90pct = ceil((0.9×64 − 13.5)/1) = ceil(44.1) = 45
        let days = v["days_to_90pct"].as_i64().unwrap();
        assert_eq!(days, 45);
        // forecast_at = 末样本 ts + 45 天
        let last_ts = base + 7 * 43_200;
        assert_eq!(v["forecast_at"], last_ts + 45 * 86_400);
        assert_eq!(v["node"], "master");
    }

    #[test]
    fn forecast_negative_growth_no_warn() {
        // 递减趋势(每天 −1GiB)→ 负增长:不预警,days_to_90pct/forecast_at 为 null
        let base = 1_700_000_000u64;
        let rows: Vec<Value> = (0..8)
            .map(|i| cap_row(base + i * 43_200, 20 * GIB - i * (GIB / 2), 64 * GIB))
            .collect();
        let v = forecast_series(&rows);
        assert_eq!(v["insufficient"], false);
        let g = v["growth_bytes_per_day"].as_i64().unwrap();
        assert!(g <= 0, "growth={g}");
        assert!(v["days_to_90pct"].is_null());
        assert!(v["forecast_at"].is_null());
    }

    #[test]
    fn forecast_zero_total_insufficient() {
        // 样本足够但 total=0 → insufficient
        let base = 1_700_000_000u64;
        let rows: Vec<Value> = (0..8)
            .map(|i| cap_row(base + i * 43_200, 10 * GIB + i * (GIB / 2), 0))
            .collect();
        let v = forecast_series(&rows);
        assert_eq!(v["insufficient"], true);
    }

    #[test]
    fn downsample_keeps_order_and_last() {
        let rows: Vec<Value> = (0..10u64).map(|i| json!({ "ts": i, "v": i })).collect();
        let small = downsample(&rows, 5);
        assert!(small.len() <= 5);
        // 顺序保持且最后一个点(ts=9)始终在序列内
        let ts: Vec<u64> = small.iter().map(|r| r["ts"].as_u64().unwrap()).collect();
        assert_eq!(ts.last(), Some(&9));
        let mut sorted = ts.clone();
        sorted.sort();
        assert_eq!(ts, sorted);
        // 未超上限原样返回
        assert_eq!(downsample(&rows, 10).len(), 10);
        assert_eq!(downsample(&rows[..3], 5).len(), 3);
    }
}
