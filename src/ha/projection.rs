// rdsctl HA — sink 投影(设计 §12)
//
// 定位:sink(元数据库)只是**只读投影/读模型**,不是权威;权威是共识日志与状态机。
// 本模块把日志里已提交的**决策**投影到 sink 的审计表,并保证:
//   - **幂等**:每条投影带标记 `proj:<shard>:<index>`,重复投影(含全量重建)不产生重复行;
//   - **可重建**:游标丢失/重置后从日志重放即可恢复投影(设计 I18);
//   - **不阻塞**:投影失败只积压游标,绝不影响共识与租约(设计 C9/I17)。
//
// 游标:<DATA_DIR>/shard-<n>/projection.cursor(记录已投影到的日志 index)。
// 只投影决策类 op(租约、步骤账本、显式审计),避免把高频派生事实灌进 sink。

use std::path::PathBuf;

use serde_json::Value;

use super::log::Log;
use super::state::Op;
use super::HaResult;

/// 投影标记:写入 sink 的 params 前置,用于幂等判定与重建
pub fn marker(shard: u16, index: u64) -> String {
    format!("proj:{shard}:{index}")
}

/// 一条待投影的审计行(来自日志 op)
#[derive(Debug, Clone, PartialEq)]
pub struct ProjectedAudit {
    pub shard: u16,
    pub index: u64,
    pub who: String,
    pub instance: String,
    pub action: String,
    pub params: String,
    pub result: String,
    pub task_id: String,
}

/// 决策类 op → 审计行(None = 不投影)
pub fn project_op(shard: u16, term: u64, index: u64, op: &Op) -> Option<ProjectedAudit> {
    let _ = term;
    let mk = marker(shard, index);
    let base = |instance: &str, action: &str, params: String, who: &str, task: &str| {
        ProjectedAudit {
            shard,
            index,
            who: who.to_string(),
            instance: instance.to_string(),
            action: action.to_string(),
            params: format!("{mk} {params}"),
            result: "ok".to_string(),
            task_id: task.to_string(),
        }
    };
    match op {
        Op::Noop | Op::Put { .. } | Op::Delete { .. } | Op::ConfigSet { .. } => None,
        // 过期租约回收是**内部收敛动作**,不是人的决策:不产生审计行(否则日志会被周期性
        // 的 GC 记录刷满,把真正的操作留痕淹没)
        Op::LeasePurge { .. } => None,
        Op::LeaseGrant {
            instance,
            holder,
            ttl_ms,
            at_ms,
        } => Some(base(
            instance,
            "lease_grant",
            format!("holder={holder} ttl_ms={ttl_ms} at_ms={at_ms}"),
            holder,
            "",
        )),
        Op::LeaseRenew {
            instance,
            holder,
            at_ms,
        } => Some(base(
            instance,
            "lease_renew",
            format!("holder={holder} at_ms={at_ms}"),
            holder,
            "",
        )),
        Op::LeaseRelease { instance, holder } => Some(base(
            instance,
            "lease_release",
            format!("holder={holder}"),
            holder,
            "",
        )),
        Op::StepBegin {
            task_id,
            node,
            step,
            idem_key,
        } => Some(base(
            "",
            "step_begin",
            format!("task={task_id} node={node} step={step} idem={idem_key}"),
            "controller",
            task_id,
        )),
        Op::StepDone {
            task_id,
            node,
            step,
            idem_key,
            result,
        } => Some(base(
            "",
            "step_done",
            format!(
                "task={task_id} node={node} step={step} idem={idem_key} result={}",
                truncate(result, 120)
            ),
            "controller",
            task_id,
        )),
        Op::AuditAppend {
            who,
            instance,
            action,
            params,
            result,
            task_id,
        } => Some(base(instance, action, params.clone(), who, task_id).with_result(result)),
    }
}

impl ProjectedAudit {
    fn with_result(mut self, result: &str) -> Self {
        self.result = result.to_string();
        self
    }
}

fn truncate(v: &Value, n: usize) -> String {
    let s = v.to_string();
    if s.chars().count() <= n {
        s
    } else {
        s.chars().take(n).collect::<String>() + "…"
    }
}

/// 投影游标
#[derive(Debug, Clone)]
pub struct Projector {
    cursor_path: PathBuf,
    cursor: u64,
}

impl Projector {
    pub fn open(dir: &std::path::Path, shard: u16) -> Self {
        let cursor_path = dir.join(format!("shard-{shard}")).join("projection.cursor");
        let cursor = std::fs::read_to_string(&cursor_path)
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(0);
        Self {
            cursor_path,
            cursor,
        }
    }

    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    /// 重置游标 → 下次投影即从日志头重放(全量重建投影;幂等标记保证不产生重复行)
    pub fn reset(&mut self, from: u64) -> HaResult<()> {
        self.cursor = from;
        self.save()
    }

    fn save(&self) -> HaResult<()> {
        if let Some(dir) = self.cursor_path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        std::fs::write(&self.cursor_path, format!("{}\n", self.cursor))?;
        Ok(())
    }

    /// 取出"待投影"的行(不推进游标;由调用方成功写入 sink 后调用 commit)
    pub fn pending(&self, log: &Log, applied_index: u64, limit: usize) -> Vec<ProjectedAudit> {
        let mut out = Vec::new();
        let from = self.cursor + 1;
        for rec in log.entries_from(from) {
            if rec.index > applied_index {
                break;
            }
            if out.len() >= limit {
                break;
            }
            match serde_json::from_slice::<Op>(&rec.payload) {
                Ok(op) => {
                    if let Some(row) = project_op(0, rec.term, rec.index, &op) {
                        out.push(row);
                    }
                }
                Err(e) => {
                    tracing::debug!("投影:index={} 的 op 解析失败({e}),跳过", rec.index);
                }
            }
        }
        out
    }

    /// 推进游标(仅在对应行已成功写入 sink 之后调用)
    pub fn commit(&mut self, upto: u64) -> HaResult<()> {
        if upto > self.cursor {
            self.cursor = upto;
            self.save()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ha::log::{Log, PendingEntry, KIND_ENTRY};

    fn tmp_dir(tag: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        let n = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        p.push(format!("rdsctl-ha-proj-{tag}-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn only_decision_ops_are_projected() {
        let shard = 2u16;
        assert!(project_op(shard, 1, 1, &Op::Noop).is_none());
        assert!(project_op(
            shard,
            1,
            2,
            &Op::Put {
                key: "k".into(),
                value: serde_json::json!(1),
                expect_ver: None
            }
        )
        .is_none());
        let lease = project_op(
            shard,
            1,
            3,
            &Op::LeaseGrant {
                instance: "db1".into(),
                holder: "n1".into(),
                ttl_ms: 30_000,
                at_ms: 100,
            },
        )
        .expect("租约必须投影");
        assert_eq!(lease.action, "lease_grant");
        assert_eq!(lease.instance, "db1");
        assert!(
            lease.params.starts_with("proj:2:3 "),
            "必须带幂等标记:{}",
            lease.params
        );
        let step = project_op(
            shard,
            1,
            4,
            &Op::StepDone {
                task_id: "t-1".into(),
                node: "master".into(),
                step: 0,
                idem_key: "docker_run:x".into(),
                result: serde_json::json!("容器已启动"),
            },
        )
        .expect("步骤终态必须投影");
        assert_eq!(step.action, "step_done");
        assert_eq!(step.task_id, "t-1");
    }

    #[test]
    fn cursor_replay_is_idempotent_by_marker() {
        let dir = tmp_dir("cursor");
        let mut log = Log::open(&dir, 0, [0u8; 32]).unwrap();
        let payload = |op: &Op| serde_json::to_vec(op).unwrap();
        log.append_batch(&[
            PendingEntry {
                kind: KIND_ENTRY,
                term: 1,
                index: 1,
                payload: payload(&Op::Noop),
            },
            PendingEntry {
                kind: KIND_ENTRY,
                term: 1,
                index: 2,
                payload: payload(&Op::LeaseGrant {
                    instance: "db1".into(),
                    holder: "n1".into(),
                    ttl_ms: 30_000,
                    at_ms: 1,
                }),
            },
            PendingEntry {
                kind: KIND_ENTRY,
                term: 1,
                index: 3,
                payload: payload(&Op::LeaseRenew {
                    instance: "db1".into(),
                    holder: "n1".into(),
                    at_ms: 2,
                }),
            },
        ])
        .unwrap();

        let mut p = Projector::open(&dir, 0);
        assert_eq!(p.cursor(), 0);
        let rows = p.pending(&log, 3, 100);
        assert_eq!(rows.len(), 2, "只有决策 op 进入投影(Noop 跳过)");
        assert_eq!(rows[0].index, 2);
        assert_eq!(rows[1].index, 3);
        p.commit(3).unwrap();

        // 重开:游标持久化,无新增
        let p2 = Projector::open(&dir, 0);
        assert_eq!(p2.cursor(), 3);
        assert!(p2.pending(&log, 3, 100).is_empty());

        // 重置后重放:同样的行(标记相同 → 上层据此去重),这就是"可重建"
        let mut p3 = Projector::open(&dir, 0);
        p3.reset(0).unwrap();
        let replay = p3.pending(&log, 3, 100);
        assert_eq!(replay.len(), 2);
        assert_eq!(replay[0].params, rows[0].params, "重放必须产生同一标记(幂等)");
    }
}
