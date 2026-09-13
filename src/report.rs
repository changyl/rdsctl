// rdsctl — 巡检报告自动生成(升级)(docs/dba-ai-design.md §8)
//
// 职责:
//   - `start`:仅 RDSCTL_REPORT_ENABLED=1 时启动单个 60s 周期 loop —— 以 UTC 时钟换算的
//     HHMM(hh*100+mm)与 RDSCTL_REPORT_HHMM(默认 "0005" → 5)相等时产日报(一次性标志防
//     同分钟重复,记上次触发 UTC 日 secs/86400);当日为周一(UTC)额外产周报(同样一次性
//     标志);每小时一次 reports_prune 保留清理(RDSCTL_REPORT_RETENTION_DAYS 默认 90 天)。
//   - `generate(rtype)`:daily|weekly 手动生成(测试/补跑;不受 ENABLED 限制,include 仍按
//     各自 env 值)—— 基础段(insights::compose_report:审计+实例)+ 扩展段(慢查 Top3 /
//     容量预警 / open 告警群,各 RDSCTL_REPORT_INCLUDE_* 默认 1),最终 text/counts 落
//     reports 表(store::report_insert)并返回。
//
// 红线(自动化产物无用户上下文):
//   - 慢查段只输出 digest 前 12 字符与统计,**绝不输出 digest_text/SQL 原文**;
//   - 文本不含口令;失败静默降级(debug/warn 日志),零 panic;不触碰巡检/采集热路径。
//
// 时间口径:UTC(unix secs;日界 = secs/86400 取整)。周判定锚点(以 `date -u` 命令核验):
//   1970-01-01(epoch 0)为周四 → days%7==0 为周四,故周一 ⇔ days ≡ 4 (mod 7),
//   即 (days+3)%7 == 0;实测 1970-01-05(345600)、2025-09-01(1756684800)、
//   2026-09-07(1788739200)均为周一,相邻日(±1 天)非周一。
//
// 接线(由主控完成,本文件不触碰 main.rs):main.rs 增 `mod report;`,manager() 内
// `crate::report::start(&m);`(仿 capacity/slow)。

use std::sync::Arc;

use serde_json::{json, Value};

use crate::instance::RdsManager;

const DAY: u64 = 86400;

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn env_u64(key: &str, d: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(d)
}

/// 扩展段开关(默认开):仅显式 "0" 视为关,其余(未设置/非 0)视为开
fn flag_on(key: &str) -> bool {
    !matches!(std::env::var(key).ok().as_deref(), Some("0"))
}

// ─── 纯函数(时间/文本组合;单测覆盖,无 IO) ───

/// rtype → (报告 period 值, 数据窗口秒)。daily=日报(today)、weekly=周报(week,7 天)。
pub(crate) fn period_of(rtype: &str) -> Option<(&'static str, u64)> {
    match rtype {
        "daily" => Some(("today", 86400)),
        "weekly" => Some(("week", 7 * 86400)),
        _ => None,
    }
}

/// UTC 时刻的 HHMM 值(hh*100+mm 口径,与 RDSCTL_REPORT_HHMM 一致:"0005"→5)。
/// 00:05:00~00:05:59 均返回 5(分钟粒度)。
pub(crate) fn hhmm_of(secs: u64) -> u64 {
    let hh = (secs % DAY) / 3600;
    let mm = (secs % 3600) / 60;
    hh * 100 + mm
}

/// 解析 RDSCTL_REPORT_HHMM:字符串按 hh*100+mm 数值解析("0005"→5,"2359"→2359);
/// 非法/越界(>2359)回退默认 5(00:05)。纯函数便于单测。
pub(crate) fn parse_hhmm(raw: &str) -> u64 {
    match raw.trim().parse::<u64>() {
        Ok(n) if n <= 2359 => n,
        _ => 5,
    }
}

/// 是否为周一(UTC)。锚点见文件头注释:1970-01-01 为周四 ⇒ days%7==4 ⇔ 周一。
pub(crate) fn is_monday(secs: u64) -> bool {
    (secs / DAY + 3) % 7 == 0
}

/// 组合最终报告文本与计数(纯函数):
///   text   = base_text + 各非空段("\n" 分隔;段 header 非空时独占一行,随后 body);
///   counts = base_counts(非对象时兜底 {}) 合并 add_counts。
pub(crate) fn render(
    base_text: &str,
    base_counts: Value,
    sections: &[(&str, &str)],
    add_counts: &[(&str, Value)],
) -> (String, Value) {
    let mut text = base_text.to_string();
    for (header, body) in sections {
        if body.is_empty() {
            continue;
        }
        text.push('\n');
        if !header.is_empty() {
            text.push_str(header);
            text.push('\n');
        }
        text.push_str(body);
    }
    let mut counts = if base_counts.is_object() {
        base_counts
    } else {
        json!({})
    };
    for (k, v) in add_counts {
        counts[k] = v.clone();
    }
    (text, counts)
}

// ─── 扩展段行渲染 ───

/// 慢查行:只含 digest 前 12 字符 + 统计,绝不含 digest_text/SQL(红线)
fn slow_line(h: &Value) -> String {
    let digest = h["digest"].as_str().unwrap_or("");
    let prefix: String = digest.chars().take(12).collect();
    format!(
        "  - {prefix}… {}ms均/{}次",
        h["avg_ms"].as_u64().unwrap_or(0),
        h["total_count"].as_u64().unwrap_or(0)
    )
}

/// 容量预警行(格式:磁盘 {pct}%,预计 {days} 天后达 90%)
fn cap_line(w: &Value) -> String {
    let inst = w["instance"].as_str().unwrap_or("");
    let pct = w["pct"].as_f64().unwrap_or(0.0);
    let days = w["days_to_90pct"].as_i64().unwrap_or(0);
    format!("  - {inst}: 磁盘 {pct:.0}%,预计 {days} 天后达 90%")
}

/// 结构水位(最满一组合并行;可选段)
fn waterline_line(wl: &Value) -> String {
    format!(
        "  - 结构水位 {}/{}: {} 台,预计 {} 天后达上限",
        wl["region"].as_str().unwrap_or(""),
        wl["shard"].as_str().unwrap_or(""),
        wl["count"].as_u64().unwrap_or(0),
        wl["days_to_limit"].as_u64().unwrap_or(0)
    )
}

/// open 告警群行:label ×count(severity): message(截 80 字符)
fn alert_line(g: &Value) -> String {
    let label = g["label"].as_str().unwrap_or("");
    let sev = g["severity"].as_str().unwrap_or("");
    let msg: String = g["message"]
        .as_str()
        .unwrap_or("")
        .chars()
        .take(80)
        .collect();
    format!(
        "  - {label} ×{}({sev}): {msg}",
        g["count"].as_u64().unwrap_or(0)
    )
}

// ─── 生成 ───

/// 生成一次报告(daily|weekly):采集 → 组装 → 落库 → 返回 (code, {ok, report})。
/// 手动触发不受 RDSCTL_REPORT_ENABLED 限制;扩展段按各自 RDSCTL_REPORT_INCLUDE_* 开关。
pub fn generate(rtype: &str) -> (u16, Value) {
    let Some((period, _span)) = period_of(rtype) else {
        return (
            400,
            json!({ "ok": false, "error": "rtype 需为 daily|weekly" }),
        );
    };
    let m = crate::manager();
    let now = now_secs();
    // 时段起点:与既有 /api/rds/report 语义一致(insights::period_since;UTC 口径:
    // today=今日 00:00 起 ≈ 上一自然日 24h,week=7 天前)。base.text 头部展示同一 since。
    let Some(since) = crate::insights::period_since(period, now) else {
        return (500, json!({ "ok": false, "error": "报告时段计算失败" }));
    };

    // 基础段:审计 + 实例快照 → 规则版报告(主干 text/counts)
    let audit = m.store.audit_since(since, 10_000);
    let insts: Vec<crate::instance::RdsInstance> =
        m.instances.iter().map(|e| e.value().clone()).collect();
    let Some(base) = crate::insights::compose_report(period, now, &audit, &insts) else {
        return (500, json!({ "ok": false, "error": "基础报告生成失败" }));
    };
    let base_text = base["text"].as_str().unwrap_or("").to_string();
    let base_counts = base.get("counts").cloned().unwrap_or_else(|| json!({}));

    // 扩展段:内容仅当非空入文;计数在开关开启时恒登记(0 亦登记,便于消费端判别)
    let mut blocks: Vec<(String, String)> = Vec::new(); // (header, body)
    let mut add_counts: Vec<(&'static str, Value)> = Vec::new();

    // a) 慢查 Top3(digest 前缀 + 统计;无 SQL 原文)
    if flag_on("RDSCTL_REPORT_INCLUDE_SLOW") {
        let min_count = env_u64("RDSCTL_SLOW_MIN_COUNT", 10);
        let rows = m.store.slow_window(since, None, None, 200_000);
        let agg = crate::slow::aggregate_window(&rows, min_count);
        let lines: Vec<String> = agg.iter().take(3).map(slow_line).collect();
        add_counts.push(("slow_top", json!(lines.len())));
        if !lines.is_empty() {
            blocks.push(("慢查询 Top3:".to_string(), lines.join("\n")));
        }
    }

    // b) 容量预警(≤5 行 warnings;水位最满 1 条可选)
    if flag_on("RDSCTL_REPORT_INCLUDE_CAP") {
        let w = crate::capacity::overview();
        let warn_lines: Vec<String> = w["warnings"]
            .as_array()
            .map(|a| a.iter().take(5).map(cap_line).collect())
            .unwrap_or_default();
        add_counts.push(("cap_warnings", json!(warn_lines.len())));
        let mut body = warn_lines.join("\n");
        if let Some(wl) = w["waterline"].as_array().and_then(|a| a.first()) {
            if !body.is_empty() {
                body.push('\n');
            }
            body.push_str(&waterline_line(wl));
        }
        if !body.is_empty() {
            blocks.push(("容量预警:".to_string(), body));
        }
    }

    // c) open 告警群摘要(≤5 群)
    if flag_on("RDSCTL_REPORT_INCLUDE_ALERTS") {
        let open = m.store.alert_list(2000, None, Some("open"), None);
        add_counts.push(("open_alerts", json!(open.len())));
        let groups = crate::insights::group_alerts(&open);
        let lines: Vec<String> = groups.iter().take(5).map(alert_line).collect();
        if !lines.is_empty() {
            blocks.push(("未处理告警:".to_string(), lines.join("\n")));
        }
    }

    // 组 final text/counts
    let sec_refs: Vec<(&str, &str)> = blocks
        .iter()
        .map(|(h, b)| (h.as_str(), b.as_str()))
        .collect();
    let (text, counts) = render(&base_text, base_counts, &sec_refs, &add_counts);

    // 归档(store 内自取 ts;generated_at 以本函数 now 为准)
    m.store
        .report_insert(period, rtype, &text, &counts.to_string());
    (
        200,
        json!({
            "ok": true,
            "report": {
                "rtype": rtype,
                "period": period,
                "generated_at": now,
                "text": text,
                "counts": counts,
            }
        }),
    )
}

/// 定时路径包装:仅记日志(成功 info / 失败 warn),不抛错
fn run_generate(rtype: &str) {
    let (code, out) = generate(rtype);
    if code == 200 {
        tracing::info!(
            "自动报告({rtype})生成并归档:{}",
            out["report"]["counts"].to_string()
        );
    } else {
        tracing::warn!(
            "自动报告({rtype})生成失败:{}",
            out["error"].as_str().unwrap_or("未知错误")
        );
    }
}

/// 保留清理(每小时;仿 slow::prune_once —— 仅删除数 >0 时 info 日志)
fn prune_once() {
    let retention = env_u64("RDSCTL_REPORT_RETENTION_DAYS", 90).max(1);
    let n = crate::manager()
        .store
        .reports_prune(now_secs().saturating_sub(retention * DAY));
    if n > 0 {
        tracing::info!("报告保留清理:删除 {n} 行(保留 {retention} 天)");
    }
}

/// 启动自动报告后台任务:ENABLED≠"1" 时零行为直接返回(不 spawn);
/// 否则单个 60s 周期 loop —— UTC HHMM 命中产日报、周一加产周报、每小时保留清理。
pub fn start(mgr: &Arc<RdsManager>) {
    if std::env::var("RDSCTL_REPORT_ENABLED").as_deref() != Ok("1") {
        tracing::info!("自动报告关闭(RDSCTL_REPORT_ENABLED≠1),不做定时生成");
        return;
    }
    let target = parse_hhmm(&std::env::var("RDSCTL_REPORT_HHMM").unwrap_or_default());
    let retention = env_u64("RDSCTL_REPORT_RETENTION_DAYS", 90).max(1);
    tracing::info!(
        "自动报告启动:UTC 每日 HHMM={target:04} 产日报(周一加产周报),reports 保留 {retention} 天"
    );
    let keep = Arc::clone(mgr); // 仅保持句柄(与 slow/capacity start 同风格)
    tokio::spawn(async move {
        // 一次性标志:记上次触发 UTC 日(secs/86400),防同分钟/同日重复生成;
        // 进程重启后同日可能补生成一次(定时为尽力而为,设计如此)
        let mut daily_day: Option<u64> = None;
        let mut weekly_day: Option<u64> = None;
        let mut prune_hour: Option<u64> = None; // 每小时一次保留清理
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            let _ = &keep;
            let now = now_secs();
            let day = now / DAY;
            if hhmm_of(now) == target {
                if daily_day != Some(day) {
                    daily_day = Some(day);
                    run_generate("daily");
                }
                // 周一(UTC)额外产周报(同一触发分钟,同样一次性标志)
                if is_monday(now) && weekly_day != Some(day) {
                    weekly_day = Some(day);
                    run_generate("weekly");
                }
            }
            let hour = now / 3600;
            if prune_hour != Some(hour) {
                prune_hour = Some(hour);
                prune_once();
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn period_of_maps_rtype() {
        assert_eq!(period_of("daily"), Some(("today", 86400)));
        assert_eq!(period_of("weekly"), Some(("week", 7 * 86400)));
        assert_eq!(period_of("month"), None);
        assert_eq!(period_of(""), None);
    }

    #[test]
    fn hhmm_value_utc_minute() {
        assert_eq!(hhmm_of(0), 0); // 1970-01-01 00:00
        assert_eq!(hhmm_of(5 * 60), 5); // 00:05:00
        assert_eq!(hhmm_of(5 * 60 + 59), 5); // 00:05:59 同一分钟
        assert_eq!(hhmm_of(23 * 3600 + 59 * 60), 2359); // 23:59:00
        assert_eq!(hhmm_of(86400 - 1), 2359); // 23:59:59
        assert_eq!(hhmm_of(86400), 0); // 次日 00:00 回绕
        assert_eq!(hhmm_of(86400 + 5 * 60), 5);
    }

    #[test]
    fn parse_hhmm_env_value() {
        assert_eq!(parse_hhmm("0005"), 5); // 默认值 00:05
        assert_eq!(parse_hhmm("2359"), 2359);
        assert_eq!(parse_hhmm("0000"), 0);
        assert_eq!(parse_hhmm("2400"), 5); // 越界 → 回退默认
        assert_eq!(parse_hhmm("abc"), 5); // 非法 → 回退默认
        assert_eq!(parse_hhmm(" 1205 "), 1205); // 容忍空白
        assert_eq!(parse_hhmm(""), 5);
    }

    #[test]
    fn is_monday_anchors_verified_with_date() {
        // 锚点均以 `date -u` 命令核验(见文件头注释):
        // 1970-01-01(epoch 0)= 周四;1970-01-05(345600)= 周一;
        // 2025-09-01(1756684800)= 周一;2026-09-07(1788739200)= 周一,
        // 2026-09-08(1788825600)= 周二,2026-09-09(1788912000)= 周三。
        assert!(!is_monday(0));
        assert!(is_monday(345_600));
        assert!(is_monday(1_756_684_800));
        assert!(is_monday(1_788_739_200));
        assert!(!is_monday(1_788_825_600));
        assert!(!is_monday(1_788_912_000));
        // 周一相邻日互斥
        assert!(is_monday(345_600));
        assert!(!is_monday(345_600 - 86_400));
        assert!(!is_monday(345_600 + 86_400));
    }

    #[test]
    fn render_joins_blocks_and_merges_counts() {
        let base_counts = json!({ "total": 2, "anomalies": 1 });
        let sections = [
            ("慢查询 Top3:", "  - ab12cd34ef56… 320ms均/88次"),
            ("容量预警:", "  - inst-a: 磁盘 80%,预计 6 天后达 90%"),
            ("未处理告警:", ""), // 空 body 应跳过
            ("", ""),
        ];
        let adds = [("slow_top", json!(1)), ("open_alerts", json!(0))];
        let (text, counts) = render("= 运维日报 =", base_counts, &sections, &adds);
        let want = "\
= 运维日报 =
慢查询 Top3:
  - ab12cd34ef56… 320ms均/88次
容量预警:
  - inst-a: 磁盘 80%,预计 6 天后达 90%";
        assert_eq!(text, want);
        // 基础计数保留 + 扩展计数合并
        assert_eq!(counts["total"], 2);
        assert_eq!(counts["anomalies"], 1);
        assert_eq!(counts["slow_top"], 1);
        assert_eq!(counts["open_alerts"], 0);
        // 无 body 的段头不落文
        assert!(!text.contains("未处理告警:"));
    }

    #[test]
    fn render_tolerates_non_object_base_counts_and_empty_base() {
        // base_counts 非对象(异常/缺省)时兜底为空对象且不 panic
        let (text, counts) = render(
            "base",
            json!(null),
            &[("H", "x"), ("", "")],
            &[("k", json!(1))],
        );
        assert_eq!(text, "base\nH\nx");
        assert_eq!(counts["k"], 1);
        assert_eq!(counts.as_object().unwrap().len(), 1);
        // 空 base + 全空段:不 panic,结果为空文本 + 空计数
        let (t2, c2) = render("", json!({}), &[], &[]);
        assert_eq!(t2, "");
        assert!(c2.is_object());
        // header 空、body 非空:直接续 body(供不需要段头的场景)
        let (t3, _) = render("a", json!({}), &[("", "b")], &[]);
        assert_eq!(t3, "a\nb");
    }
}
