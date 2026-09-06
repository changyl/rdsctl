// rdsctl — vtorc 式内嵌 orchestrator 的事实层(P1 骨架,见 docs/meta-authority.md)
//
// 本模块只负责“复制事实”的解析与决策纯函数,不含网络/容器副作用:
//   - 解析 `SHOW SLAVE STATUS`、半同步状态文本 → 结构化 Fact
//   - 按实例复制模式(async/semi-sync)给出 loss 预算与候选取舍
// 采集与巡检接线、UI 双源角标属后续增量(由巡检调用本模块解析)。

#![allow(dead_code)] // P1 骨架:解析/决策函数将由健康巡检接入后使用

/// 复制模式
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplMode {
    Async,
    SemiSync,
}

impl ReplMode {
    /// 实例 itype 映射:sync → 半同步;async/single 其余 → 异步
    pub fn of_itype(itype: &str) -> ReplMode {
        match itype.trim().to_ascii_lowercase().as_str() {
            "sync" => ReplMode::SemiSync,
            _ => ReplMode::Async,
        }
    }
    pub fn label(&self) -> &'static str {
        match self {
            ReplMode::Async => "async",
            ReplMode::SemiSync => "semi_sync",
        }
    }
}

/// 半同步状态(主/从两侧)
#[derive(Debug, Clone, PartialEq)]
pub struct SemiSync {
    pub master_enabled: bool, // 主侧 rpl_semi_sync_master_enabled
    pub master_ack: u64,      // 主侧 Rpl_semi_sync_master_clients 或收到 ack 数
    pub master_degraded: bool, // 主侧已退化(无 ack 保护仍继续写)
    pub slave_enabled: bool,  // 从侧 rpl_semi_sync_slave_enabled
}

/// 某副本的复制事实
#[derive(Debug, Clone, PartialEq)]
pub struct Fact {
    pub container: String,
    pub role: String, // master | slave(读从/离线语义由登记侧决定)
    pub mode: ReplMode,
    pub alive: bool,
    pub io_running: bool,
    pub sql_running: bool,
    pub lag_secs: Option<i64>,
    pub semisync: Option<SemiSync>,
}

impl Fact {
    /// 复制链路是否健康(从:IO/SQL 均运行;主:存活即可)
    pub fn repl_ok(&self) -> bool {
        if self.role == "master" {
            return self.alive;
        }
        self.alive && self.io_running && self.sql_running
    }
    /// 是否可作为近零丢候选:半同步实例要求 主侧 ack ≥1 且未退化、且该从半同步启用
    pub fn ack_safe(&self) -> bool {
        match &self.semisync {
            Some(s) => self.mode == ReplMode::SemiSync && s.slave_enabled,
            None => false,
        }
    }
}

/// `SHOW SLAVE STATUS`(文本,G 输出)解析出的最小字段
#[derive(Debug, Clone, PartialEq)]
pub struct SlaveStatus {
    pub io_running: bool,
    pub sql_running: bool,
    pub master_host: String,
    pub seconds_behind: Option<i64>,
}

fn boolish(v: &str) -> bool {
    let t = v.trim().to_ascii_lowercase();
    matches!(t.as_str(), "yes" | "on" | "1" | "true" | "running")
}

/// 解析 MySQL `SHOW SLAVE STATUS`(含 \G 的行式输出)
pub fn parse_slave_status(text: &str) -> Option<SlaveStatus> {
    let mut io = false;
    let mut sql = false;
    let mut host = String::new();
    let mut seen = 0usize;
    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() || !line.contains(':') {
            continue;
        }
        let mut it = line.splitn(2, ':');
        let k = it.next().unwrap_or("").trim().to_ascii_lowercase();
        let v = it.next().unwrap_or("").trim();
        match k.as_str() {
            "slave_io_running" => {
                io = boolish(v);
                seen += 1;
            }
            "slave_sql_running" => {
                sql = boolish(v);
                seen += 1;
            }
            "master_host" => {
                host = v.to_string();
                seen += 1;
            }
            _ => {}
        }
    }
    if seen == 0 {
        return None;
    }
    // NULL → None(未知);"0" → 追平
    let lag = text
        .lines()
        .map(|l| l.trim())
        .find_map(|l| {
            if l.to_ascii_lowercase().starts_with("seconds_behind_master:") {
                let v = l.splitn(2, ':').nth(1).unwrap_or("").trim();
                match v {
                    "NULL" => None,
                    _ => v.parse::<i64>().ok(),
                }
            } else {
                None
            }
        });
    Some(SlaveStatus {
        io_running: io,
        sql_running: sql,
        master_host: host,
        seconds_behind: lag,
    })
}

/// 解析半同步状态:输入形如 `rpl_semi_sync_master_enabled: ON` 的行集合
pub fn parse_semisync(text: &str) -> SemiSync {
    let mut out = SemiSync {
        master_enabled: false,
        master_ack: 0,
        master_degraded: false,
        slave_enabled: false,
    };
    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() || !line.contains(':') {
            continue;
        }
        let mut it = line.splitn(2, ':');
        let k = it.next().unwrap_or("").trim().to_ascii_lowercase();
        let v = it.next().unwrap_or("").trim();
        match k.as_str() {
            "rpl_semi_sync_master_enabled" => out.master_enabled = boolish(v),
            "rpl_semi_sync_slave_enabled" => out.slave_enabled = boolish(v),
            "rpl_semi_sync_master_clients" => out.master_ack = v.parse().unwrap_or(0),
            _ => {}
        }
    }
    out.master_degraded = out.master_enabled && out.master_ack == 0;
    out
}

/// 候选从(参与切换挑选的排序输入)
#[derive(Debug, Clone)]
pub struct Candidate {
    pub name: String,
    pub lag_secs: i64,
    pub ack_safe: bool,
}

/// 选择最优候选(索引):
///  - 近零丢(zero_loss,半同步模板):优先 ack_safe 候选,再按 lag 升序
///  - 允许丢(async 模板):直接按 lag 升序
/// 返回 None 表示无候选
pub fn pick_candidate(cands: &[Candidate], zero_loss: bool) -> Option<usize> {
    let mut idx: Vec<usize> = (0..cands.len()).collect();
    if zero_loss {
        idx.retain(|&i| cands[i].ack_safe);
    }
    if idx.is_empty() {
        return None;
    }
    idx.sort_by(|&a, &b| cands[a].lag_secs.cmp(&cands[b].lag_secs).then(a.cmp(&b)));
    Some(idx[0])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mode_mapping_and_labels() {
        assert_eq!(ReplMode::of_itype("sync"), ReplMode::SemiSync);
        assert_eq!(ReplMode::of_itype("async"), ReplMode::Async);
        assert_eq!(ReplMode::of_itype("SINGLE"), ReplMode::Async);
        assert_eq!(ReplMode::SemiSync.label(), "semi_sync");
    }

    #[test]
    fn parse_slave_status_yes() {
        let txt = "             Slave_IO_Running: Yes\n            Slave_SQL_Running: Yes\n                    Master_Host: rds-x-master\n       Seconds_Behind_Master: 2";
        let s = parse_slave_status(txt).expect("应解析成功");
        assert!(s.io_running && s.sql_running);
        assert_eq!(s.master_host, "rds-x-master");
        assert_eq!(s.seconds_behind, Some(2));
    }

    #[test]
    fn parse_slave_status_no_and_null() {
        let txt = "Slave_IO_Running: Connecting\nSlave_SQL_Running: No\nSeconds_Behind_Master: NULL";
        let s = parse_slave_status(txt).unwrap();
        assert!(!s.io_running);
        assert!(!s.sql_running);
        assert_eq!(s.seconds_behind, None);
    }

    #[test]
    fn semisync_parse_and_degraded() {
        let txt = "rpl_semi_sync_master_enabled: ON\nrpl_semi_sync_slave_enabled: ON\nrpl_semi_sync_master_clients: 1";
        let s = parse_semisync(txt);
        assert!(s.master_enabled && s.slave_enabled);
        assert_eq!(s.master_ack, 1);
        assert!(!s.master_degraded);
        // 主侧启用但 ack=0 → 退化(无保护继续写)
        let d = parse_semisync("rpl_semi_sync_master_enabled: ON\nrpl_semi_sync_master_clients: 0");
        assert!(d.master_degraded);
    }

    #[test]
    fn fact_health_and_ack_safe() {
        let semisync = SemiSync { master_enabled: true, master_ack: 1, master_degraded: false, slave_enabled: true };
        let f = Fact {
            container: "rds-x-s1".into(),
            role: "slave".into(),
            mode: ReplMode::SemiSync,
            alive: true,
            io_running: true,
            sql_running: true,
            lag_secs: Some(0),
            semisync: Some(semisync),
        };
        assert!(f.repl_ok());
        assert!(f.ack_safe());
        let m = Fact { role: "master".into(), alive: true, ..f.clone() };
        assert!(m.repl_ok());
        let dead = Fact { alive: false, ..f.clone() };
        assert!(!dead.repl_ok());
    }

    #[test]
    fn candidate_pick_zero_loss_vs_lossy() {
        let cands = vec![
            Candidate { name: "s-a".into(), lag_secs: 0, ack_safe: false },
            Candidate { name: "s-b".into(), lag_secs: 5, ack_safe: true },
        ];
        // 半同步(近零丢):只从 ack_safe 中选
        assert_eq!(pick_candidate(&cands, true), Some(1));
        // 异步:允许丢,取 lag 最小
        assert_eq!(pick_candidate(&cands, false), Some(0));
        // 近零丢但无 ack 候选 → None(提示需降级策略)
        assert_eq!(pick_candidate(&[cands[0].clone()], true), None);
    }
}
