// rdsctl HA — 状态机(设计 §6)
//
// 铁律:**apply 必须确定性**。因此:
//   - 所有时间都来自 op 载荷(`at_ms`),状态机内不读本地时钟(设计 §5.4);
//   - 所有被序列化的映射一律用 BTreeMap(保证快照字节序确定,否则各副本快照哈希不一致);
//   - 非法 op 不静默忽略,而是返回 Rejected(调用方可审计)。
//
// 内容:带版本 KV(CAS)、实例租约(单写者)、步骤账本(有效一次)、审计尾、配置世代。

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::Fence;

/// 状态机对外暴露的错误(非法 op)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reject {
    /// KV CAS 版本不符
    VersionConflict { key: String, expect: u64, actual: u64 },
    /// 键不存在却要求版本匹配
    NotFound { key: String },
    /// 租约被他人持有(冲突授予被拒)
    LeaseHeld { instance: String, holder: String },
    /// 冲突授予必须等到此毫秒(设计 §5.4:expire + max_skew)
    LeaseConflictNotYetSafe { instance: String, safe_from_ms: u64 },
    /// 续约/释放时并非持有者
    LeaseNotHeld { instance: String, holder: String },
    /// op 载荷本身非法
    Invalid(String),
}

impl std::fmt::Display for Reject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Reject::VersionConflict { key, expect, actual } => {
                write!(f, "版本冲突:key={key} expect={expect} actual={actual}")
            }
            Reject::NotFound { key } => write!(f, "键不存在:{key}"),
            Reject::LeaseHeld { instance, holder } => {
                write!(f, "实例 {instance} 的租约由 {holder} 持有")
            }
            Reject::LeaseConflictNotYetSafe { instance, safe_from_ms } => write!(
                f,
                "实例 {instance} 的旧租约尚未安全过期(需等到 {safe_from_ms}ms;expire + max_skew)"
            ),
            Reject::LeaseNotHeld { instance, holder } => {
                write!(f, "实例 {instance} 的租约不由 {holder} 持有")
            }
            Reject::Invalid(m) => write!(f, "op 非法:{m}"),
        }
    }
}

/// apply 结果:调用方据此决定响应(短路、拒绝、或已应用)
#[derive(Debug, Clone, PartialEq)]
pub enum Applied {
    /// 已应用到状态机
    Ok,
    /// 被状态机拒绝(不改变状态;确定性:所有副本都会同样拒绝)
    Rejected(Reject),
    /// 步骤账本命中:该步骤已完成,调用方必须短路(不再执行副作用)
    StepAlreadyDone { result: Option<Value> },
}

impl Applied {
    pub fn is_ok(&self) -> bool {
        matches!(self, Applied::Ok)
    }
    pub fn rejected(&self) -> Option<&Reject> {
        match self {
            Applied::Rejected(r) => Some(r),
            _ => None,
        }
    }
}

/// KV 条目(带版本,乐观并发)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KvEntry {
    pub value: Value,
    pub ver: u64,
    pub updated_index: u64,
}

/// 实例租约(单写者)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Lease {
    pub instance: String,
    pub holder: String,
    /// 授予时的 term(用于还原 fence)
    pub granted_term: u64,
    /// 授予 op 的日志 index(用于还原 fence)
    pub granted_index: u64,
    /// 由授予时刻 + ttl 计算(不读本地时钟)
    pub expire_at_ms: u64,
    pub ttl_ms: u64,
    /// 已续约次数(可观测)
    pub renewals: u64,
}

impl Lease {
    pub fn fence(&self, shard: u16) -> Fence {
        Fence::new(shard, self.granted_term, self.granted_index)
    }

    /// 本 holder 在 at_ms 时刻是否仍持有该租约
    pub fn held_by(&self, holder: &str, at_ms: u64) -> bool {
        self.holder == holder && at_ms < self.expire_at_ms
    }
}

/// 步骤账本状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StepState {
    Started,
    Done,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StepRecord {
    pub state: StepState,
    pub attempts: u32,
    pub result: Option<Value>,
    pub started_index: u64,
    pub done_index: Option<u64>,
}

/// 审计行(决策审计随 op 提交,见设计 C10)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuditRow {
    pub seq: u64,
    pub index: u64,
    pub who: String,
    pub instance: String,
    pub action: String,
    pub params: String,
    pub result: String,
    pub task_id: String,
}

/// 日志 op(设计 §6.2 的子集:M1a 正确性内核所需部分)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Op {
    /// 新 term 首条,推进 commit index
    Noop,
    /// 通用带版本写入
    Put {
        key: String,
        value: Value,
        expect_ver: Option<u64>,
    },
    /// 删除键
    Delete { key: String, expect_ver: Option<u64> },
    /// 配置世代与时钟前提(M1a 只需要 epoch 与 max_skew 进状态机,保证判定确定性)
    ConfigSet {
        config_epoch: u64,
        max_skew_ms: u64,
    },
    /// 租约授予(at_ms 由 leader 在追加前取自身时钟,随 op 落日志)
    LeaseGrant {
        instance: String,
        holder: String,
        ttl_ms: u64,
        at_ms: u64,
    },
    /// 租约续约
    LeaseRenew {
        instance: String,
        holder: String,
        at_ms: u64,
    },
    /// 租约释放(仅持有者;释放后他人可立即授予,无需等待)
    LeaseRelease {
        instance: String,
        holder: String,
    },
    /// 过期租约回收(确定性 GC;设计 §19 发现 15)
    ///
    /// `cutoff_ms` 由 **leader 在提议时**按
    /// `now - (max_skew_ms + 宽限)` 算好并随 op 落日志;
    /// apply 只做 `expire_at_ms <= cutoff_ms` 的比较,**绝不读本地时钟** ——
    /// 否则各副本 apply 结果不一致,确定性被破坏(与租约 `at_ms` 同一口径)。
    ///
    /// 安全性:任何后续 `LeaseGrant` 的 `at_ms` 都 ≥ 本条 op 的提议时刻,
    /// 而 `cutoff_ms + max_skew_ms <= 提议时刻`,故 `at_ms >= cutoff_ms + max_skew_ms
    /// >= expire_at_ms + max_skew_ms` —— 即"冲突检查本来就会放行"。删除它不削弱 §5.4。
    LeasePurge {
        cutoff_ms: u64,
    },
    /// 步骤开始(执行副作用前的登记)
    StepBegin {
        task_id: String,
        node: String,
        step: u32,
        idem_key: String,
    },
    /// 步骤完成(重放命中的短路依据)
    StepDone {
        task_id: String,
        node: String,
        step: u32,
        idem_key: String,
        result: Value,
    },
    /// 决策审计(与业务 op 同批提交;幂等键 = (task_id, action, seq))
    AuditAppend {
        who: String,
        instance: String,
        action: String,
        params: String,
        result: String,
        task_id: String,
    },
}

impl Op {
    /// 该 op 是否"变更权威状态"(用于断言/统计)
    pub fn mutates(&self) -> bool {
        !matches!(self, Op::Noop)
    }

    /// 为租约类 op 填充授予时刻(状态机不读时钟,时间必须来自 op 载荷)。
    /// 调用方(leader)在追加前用自身时钟调用;at_ms 已给出时不覆盖。
    pub fn with_now(mut self, now_ms: u64) -> Self {
        match &mut self {
            Op::LeaseGrant { at_ms, .. } | Op::LeaseRenew { at_ms, .. } if *at_ms == 0 => {
                *at_ms = now_ms;
            }
            _ => {}
        }
        self
    }
}

/// 步骤账本键(转义 '|' 保证无歧义)
fn step_key(task_id: &str, node: &str, step: u32, idem_key: &str) -> String {
    let esc = |s: &str| s.replace('|', "||");
    format!(
        "{}|{}|{}|{}",
        esc(task_id),
        esc(node),
        step,
        esc(idem_key)
    )
}

/// 状态机镜像(快照/恢复用;字段顺序固定以保证序列化确定)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StateImage {
    pub shard: u16,
    pub config_epoch: u64,
    pub max_skew_ms: u64,
    pub applied_index: u64,
    pub applied_term: u64,
    pub audit_seq: u64,
    pub kv: BTreeMap<String, KvEntry>,
    pub leases: BTreeMap<String, Lease>,
    pub steps: BTreeMap<String, StepRecord>,
    /// 审计尾部(完整审计走 sink 投影;此处只保留最近 N 条作为权威近端)
    pub audit_tail: Vec<AuditRow>,
}

/// 状态机
#[derive(Debug, Clone)]
pub struct StateMachine {
    shard: u16,
    config_epoch: u64,
    max_skew_ms: u64,
    applied_index: u64,
    applied_term: u64,
    audit_seq: u64,
    kv: BTreeMap<String, KvEntry>,
    leases: BTreeMap<String, Lease>,
    steps: BTreeMap<String, StepRecord>,
    audit_tail: Vec<AuditRow>,
    audit_tail_cap: usize,
}

impl StateMachine {
    pub fn new(shard: u16, max_skew_ms: u64) -> Self {
        Self {
            shard,
            config_epoch: 0,
            max_skew_ms,
            applied_index: 0,
            applied_term: 0,
            audit_seq: 0,
            kv: BTreeMap::new(),
            leases: BTreeMap::new(),
            steps: BTreeMap::new(),
            audit_tail: Vec::new(),
            audit_tail_cap: 2000,
        }
    }

    pub fn shard(&self) -> u16 {
        self.shard
    }
    pub fn applied_index(&self) -> u64 {
        self.applied_index
    }
    pub fn applied_term(&self) -> u64 {
        self.applied_term
    }
    pub fn config_epoch(&self) -> u64 {
        self.config_epoch
    }
    pub fn max_skew_ms(&self) -> u64 {
        self.max_skew_ms
    }
    pub fn kv_get(&self, key: &str) -> Option<&KvEntry> {
        self.kv.get(key)
    }

    /// 按前缀取键值(带版本)。用于保留键空间(`s/ u/ r/`)的整体读视图与引导灌入。
    ///
    /// 返回 `BTreeMap` 而不是 `Vec`:顺序稳定 ⇒ 同一份状态在任何副本上渲染出的列表逐字节一致
    /// (确定性要求,与 `apply_is_deterministic_and_survives_snapshot_roundtrip` 同源)。
    pub fn kv_prefix(&self, prefix: &str) -> BTreeMap<String, (Value, u64)> {
        self.kv
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), (v.value.clone(), v.ver)))
            .collect()
    }
    pub fn lease_of(&self, instance: &str) -> Option<&Lease> {
        self.leases.get(instance)
    }
    pub fn step_record(&self, task_id: &str, node: &str, step: u32, idem_key: &str) -> Option<&StepRecord> {
        self.steps
            .get(&step_key(task_id, node, step, idem_key))
    }
    /// 按 task_id 列出步骤账本(key 形如 "<task_id>|<node>|<step>|<idem>",task_id 已转义)
    pub fn steps_of(&self, task_id: &str) -> Vec<(String, StepRecord)> {
        let prefix = format!("{}|", task_id.replace('|', "||"));
        self.steps
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    pub fn audit_tail(&self) -> &[AuditRow] {
        &self.audit_tail
    }
    pub fn kv_len(&self) -> usize {
        self.kv.len()
    }
    pub fn lease_count(&self) -> usize {
        self.leases.len()
    }

    /// 实例租约台账(只读):谁持有哪个实例、fence 是多少、何时过期。
    ///
    /// 租约是**共识里的权威状态**(设计 D2/C4),不是本地缓存,因此这份台账在所有副本上
    /// 最终一致;`remaining_ms` 由调用方传入的 `now` 计算(不读本地时钟,便于测试与时钟偏移诊断)。
    pub fn leases_view(&self, now: u64) -> Vec<Value> {
        self.leases
            .values()
            .map(|l| {
                json!({
                    "instance": l.instance,
                    "holder": l.holder,
                    "fence_term": l.granted_term,
                    "fence_index": l.granted_index,
                    "expire_at_ms": l.expire_at_ms,
                    "ttl_ms": l.ttl_ms,
                    "renewals": l.renewals,
                    "remaining_ms": l.expire_at_ms.saturating_sub(now),
                    "expired": l.expire_at_ms <= now,
                })
            })
            .collect()
    }

    /// 应用一条已提交 op。`term`/`index` 为该 op 在日志中的位置(权威顺序)。
    pub fn apply(&mut self, term: u64, index: u64, op: &Op) -> Applied {
        let out = self.apply_inner(term, index, op);
        self.applied_index = index;
        self.applied_term = term;
        out
    }

    fn apply_inner(&mut self, term: u64, index: u64, op: &Op) -> Applied {
        match op {
            Op::Noop => Applied::Ok,

            Op::Put {
                key,
                value,
                expect_ver,
            } => {
                if key.trim().is_empty() {
                    return Applied::Rejected(Reject::Invalid("key 为空".into()));
                }
                match (self.kv.get(key), expect_ver) {
                    (None, Some(0)) | (None, None) => {}
                    (None, Some(expect)) => {
                        return Applied::Rejected(Reject::NotFound {
                            key: key.clone(),
                            // 期望匹配到具体版本,但键不存在
                        })
                        .tap_expect(*expect);
                    }
                    (Some(cur), Some(expect)) if cur.ver != *expect => {
                        return Applied::Rejected(Reject::VersionConflict {
                            key: key.clone(),
                            expect: *expect,
                            actual: cur.ver,
                        })
                    }
                    (Some(_), _) => {}
                }
                let ver = self.kv.get(key).map(|e| e.ver + 1).unwrap_or(1);
                self.kv.insert(
                    key.clone(),
                    KvEntry {
                        value: value.clone(),
                        ver,
                        updated_index: index,
                    },
                );
                Applied::Ok
            }

            Op::Delete { key, expect_ver } => match (self.kv.get(key), expect_ver) {
                (None, _) => Applied::Rejected(Reject::NotFound { key: key.clone() }),
                (Some(cur), Some(expect)) if cur.ver != *expect => {
                    Applied::Rejected(Reject::VersionConflict {
                        key: key.clone(),
                        expect: *expect,
                        actual: cur.ver,
                    })
                }
                _ => {
                    self.kv.remove(key);
                    Applied::Ok
                }
            },

            Op::ConfigSet {
                config_epoch,
                max_skew_ms,
            } => {
                if *config_epoch <= self.config_epoch {
                    return Applied::Rejected(Reject::Invalid(format!(
                        "config_epoch 必须递增:当前 {},收到 {}",
                        self.config_epoch, config_epoch
                    )));
                }
                if *max_skew_ms == 0 {
                    return Applied::Rejected(Reject::Invalid("max_skew_ms 不能为 0".into()));
                }
                self.config_epoch = *config_epoch;
                self.max_skew_ms = *max_skew_ms;
                Applied::Ok
            }

            Op::LeaseGrant {
                instance,
                holder,
                ttl_ms,
                at_ms,
            } => {
                if instance.trim().is_empty() || holder.trim().is_empty() {
                    return Applied::Rejected(Reject::Invalid("instance/holder 为空".into()));
                }
                if *ttl_ms == 0 {
                    return Applied::Rejected(Reject::Invalid("ttl_ms 不能为 0".into()));
                }
                if *ttl_ms < self.max_skew_ms.saturating_mul(10) {
                    return Applied::Rejected(Reject::Invalid(format!(
                        "ttl_ms={ttl_ms} 违反前提 A1:必须 ≥ 10×max_skew(={})",
                        self.max_skew_ms.saturating_mul(10)
                    )));
                }
                if let Some(old) = self.leases.get(instance) {
                    if old.holder != *holder {
                        // 冲突授予:必须等到"旧租约过期 + 一个时钟偏移上界"才安全(设计 §5.4)
                        let safe_from = old.expire_at_ms.saturating_add(self.max_skew_ms);
                        if *at_ms < safe_from {
                            return Applied::Rejected(Reject::LeaseConflictNotYetSafe {
                                instance: instance.clone(),
                                safe_from_ms: safe_from,
                            });
                        }
                    }
                }
                self.leases.insert(
                    instance.clone(),
                    Lease {
                        instance: instance.clone(),
                        holder: holder.clone(),
                        granted_term: term,
                        granted_index: index,
                        expire_at_ms: at_ms.saturating_add(*ttl_ms),
                        ttl_ms: *ttl_ms,
                        renewals: 0,
                    },
                );
                Applied::Ok
            }

            Op::LeaseRenew {
                instance,
                holder,
                at_ms,
            } => {
                let Some(cur) = self.leases.get(instance).cloned() else {
                    return Applied::Rejected(Reject::LeaseNotHeld {
                        instance: instance.clone(),
                        holder: holder.clone(),
                    });
                };
                if cur.holder != *holder {
                    return Applied::Rejected(Reject::LeaseNotHeld {
                        instance: instance.clone(),
                        holder: holder.clone(),
                    });
                }
                self.leases.insert(
                    instance.clone(),
                    Lease {
                        expire_at_ms: at_ms.saturating_add(cur.ttl_ms),
                        renewals: cur.renewals + 1,
                        // 续约不改变 fence 首授予身份,但 fence 取本条 op 位置(单调)
                        granted_term: term,
                        granted_index: index,
                        ..cur
                    },
                );
                Applied::Ok
            }

            Op::LeaseRelease { instance, holder } => {
                let Some(cur) = self.leases.get(instance) else {
                    return Applied::Rejected(Reject::LeaseNotHeld {
                        instance: instance.clone(),
                        holder: holder.clone(),
                    });
                };
                if cur.holder != *holder {
                    return Applied::Rejected(Reject::LeaseNotHeld {
                        instance: instance.clone(),
                        holder: holder.clone(),
                    });
                }
                self.leases.remove(instance);
                Applied::Ok
            }

            Op::LeasePurge { cutoff_ms } => {
                // 纯粹的确定性收敛:`expire_at_ms` 与 `cutoff_ms` 都来自日志,不读本地时钟。
                // 保留严格大于 cutoff 的条目,因此反复重放同一 op 幂等。
                self.leases.retain(|_, l| l.expire_at_ms > *cutoff_ms);
                Applied::Ok
            }

            Op::StepBegin {
                task_id,
                node,
                step,
                idem_key,
            } => {
                let k = step_key(task_id, node, *step, idem_key);
                match self.steps.get(&k) {
                    Some(r) if r.state == StepState::Done => Applied::StepAlreadyDone {
                        result: r.result.clone(),
                    },
                    Some(r) => {
                        let mut r = r.clone();
                        r.attempts += 1;
                        self.steps.insert(k, r);
                        Applied::Ok
                    }
                    None => {
                        self.steps.insert(
                            k,
                            StepRecord {
                                state: StepState::Started,
                                attempts: 1,
                                result: None,
                                started_index: index,
                                done_index: None,
                            },
                        );
                        Applied::Ok
                    }
                }
            }

            Op::StepDone {
                task_id,
                node,
                step,
                idem_key,
                result,
            } => {
                let k = step_key(task_id, node, *step, idem_key);
                match self.steps.get(&k) {
                    // 有效一次:已完成则保留**首次**结果,不被后续重放覆盖
                    Some(r) if r.state == StepState::Done => Applied::StepAlreadyDone {
                        result: r.result.clone(),
                    },
                    Some(r) => {
                        let mut r = r.clone();
                        r.state = StepState::Done;
                        r.result = Some(result.clone());
                        r.done_index = Some(index);
                        self.steps.insert(k, r);
                        Applied::Ok
                    }
                    None => {
                        // 未经 StepBegin 直接 Done:登记为已完成(容忍"重放时只看到 Done"的场景)
                        self.steps.insert(
                            k,
                            StepRecord {
                                state: StepState::Done,
                                attempts: 0,
                                result: Some(result.clone()),
                                started_index: index,
                                done_index: Some(index),
                            },
                        );
                        Applied::Ok
                    }
                }
            }

            Op::AuditAppend {
                who,
                instance,
                action,
                params,
                result,
                task_id,
            } => {
                self.audit_seq += 1;
                self.audit_tail.push(AuditRow {
                    seq: self.audit_seq,
                    index,
                    who: who.clone(),
                    instance: instance.clone(),
                    action: action.clone(),
                    params: params.clone(),
                    result: result.clone(),
                    task_id: task_id.clone(),
                });
                if self.audit_tail.len() > self.audit_tail_cap {
                    let drop_n = self.audit_tail.len() - self.audit_tail_cap;
                    self.audit_tail.drain(0..drop_n);
                }
                Applied::Ok
            }
        }
    }

    /// 导出镜像(供快照)
    pub fn to_image(&self) -> StateImage {
        StateImage {
            shard: self.shard,
            config_epoch: self.config_epoch,
            max_skew_ms: self.max_skew_ms,
            applied_index: self.applied_index,
            applied_term: self.applied_term,
            audit_seq: self.audit_seq,
            kv: self.kv.clone(),
            leases: self.leases.clone(),
            steps: self.steps.clone(),
            audit_tail: self.audit_tail.clone(),
        }
    }

    pub fn from_image(img: StateImage) -> Self {
        Self {
            shard: img.shard,
            config_epoch: img.config_epoch,
            max_skew_ms: img.max_skew_ms,
            applied_index: img.applied_index,
            applied_term: img.applied_term,
            audit_seq: img.audit_seq,
            kv: img.kv,
            leases: img.leases,
            steps: img.steps,
            audit_tail: img.audit_tail,
            audit_tail_cap: 2000,
        }
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(&self.to_image()).unwrap_or_else(|e| format!("{{\"error\":\"{e}\"}}"))
    }

    pub fn from_json(s: &str) -> Result<Self, String> {
        let img: StateImage = serde_json::from_str(s).map_err(|e| e.to_string())?;
        Ok(Self::from_image(img))
    }
}

/// 小工具:构造 NotFound 时可携带期望版本(便于日志定位)
trait TapExpect {
    fn tap_expect(self, expect: u64) -> Self;
}

impl TapExpect for Applied {
    fn tap_expect(self, expect: u64) -> Self {
        if let Applied::Rejected(Reject::NotFound { key }) = self {
            return Applied::Rejected(Reject::NotFound {
                key: format!("{key}(期望版本 {expect})"),
            });
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const SKEW: u64 = 1000;
    const TTL: u64 = 30_000;

    fn sm() -> StateMachine {
        let mut s = StateMachine::new(0, SKEW);
        assert!(s.apply(1, 1, &Op::ConfigSet { config_epoch: 1, max_skew_ms: SKEW }).is_ok());
        s
    }

    fn grant(instance: &str, holder: &str, at: u64) -> Op {
        Op::LeaseGrant {
            instance: instance.into(),
            holder: holder.into(),
            ttl_ms: TTL,
            at_ms: at,
        }
    }

    #[test]
    fn kv_cas_and_versions() {
        let mut s = sm();
        assert!(s
            .apply(1, 2, &Op::Put { key: "k".into(), value: json!({"a":1}), expect_ver: None })
            .is_ok());
        assert_eq!(s.kv_get("k").unwrap().ver, 1);
        // 重复写入(无 expect)覆盖并递增版本
        assert!(s
            .apply(1, 3, &Op::Put { key: "k".into(), value: json!({"a":2}), expect_ver: None })
            .is_ok());
        assert_eq!(s.kv_get("k").unwrap().ver, 2);
        // CAS 不符 → 拒绝且不改状态
        let r = s.apply(1, 4, &Op::Put { key: "k".into(), value: json!({"a":3}), expect_ver: Some(99) });
        assert!(matches!(r.rejected(), Some(Reject::VersionConflict { .. })));
        assert_eq!(s.kv_get("k").unwrap().ver, 2);
        assert_eq!(s.kv_get("k").unwrap().value, json!({"a":2}));
        // CAS 命中 → 通过
        assert!(s
            .apply(1, 5, &Op::Put { key: "k".into(), value: json!({"a":4}), expect_ver: Some(2) })
            .is_ok());
        assert_eq!(s.kv_get("k").unwrap().ver, 3);
        // 不存在的键 + 期望版本 → NotFound
        let r = s.apply(1, 6, &Op::Put { key: "nope".into(), value: json!(1), expect_ver: Some(1) });
        assert!(matches!(r.rejected(), Some(Reject::NotFound { .. })));
    }

    #[test]
    fn lease_grant_conflict_waits_for_expiry_plus_skew() {
        let mut s = sm();
        assert!(s.apply(1, 2, &grant("i1", "nodeA", 0)).is_ok());
        assert_eq!(s.lease_of("i1").unwrap().expire_at_ms, TTL);
        // 未到期:冲突授予被拒
        let r = s.apply(1, 3, &grant("i1", "nodeB", TTL - 1));
        assert!(matches!(
            r.rejected(),
            Some(Reject::LeaseConflictNotYetSafe { .. })
        ));
        // 刚过期但未过 max_skew:仍被拒(这就是设计 §5.4 修正后的规则)
        let r = s.apply(1, 4, &grant("i1", "nodeB", TTL));
        match r.rejected() {
            Some(Reject::LeaseConflictNotYetSafe { safe_from_ms, .. }) => {
                assert_eq!(*safe_from_ms, TTL + SKEW)
            }
            other => panic!("应拒绝,实际 {other:?}"),
        }
        // 过期 + skew:允许接管
        assert!(s.apply(2, 5, &grant("i1", "nodeB", TTL + SKEW)).is_ok());
        let l = s.lease_of("i1").unwrap();
        assert_eq!(l.holder, "nodeB");
        assert_eq!(l.fence(0), Fence::new(0, 2, 5), "fence 必须取自接管 op 的位置");
    }

    #[test]
    fn lease_ttl_must_respect_skew_premise() {
        let mut s = sm();
        let r = s.apply(
            1,
            2,
            &Op::LeaseGrant {
                instance: "i".into(),
                holder: "h".into(),
                ttl_ms: SKEW * 3,
                at_ms: 0,
            },
        );
        assert!(matches!(r.rejected(), Some(Reject::Invalid(_))), "ttl < 10×skew 必须拒绝");
    }

    #[test]
    fn lease_renew_only_by_holder_and_release_frees_immediately() {
        let mut s = sm();
        assert!(s.apply(1, 2, &grant("i1", "A", 0)).is_ok());
        // 非持有者续约 → 拒
        let r = s.apply(
            1,
            3,
            &Op::LeaseRenew { instance: "i1".into(), holder: "B".into(), at_ms: 100 },
        );
        assert!(matches!(r.rejected(), Some(Reject::LeaseNotHeld { .. })));
        // 持有者续约 → 通过且过期时间前移
        assert!(s
            .apply(1, 4, &Op::LeaseRenew { instance: "i1".into(), holder: "A".into(), at_ms: 10_000 })
            .is_ok());
        assert_eq!(s.lease_of("i1").unwrap().expire_at_ms, 10_000 + TTL);
        assert_eq!(s.lease_of("i1").unwrap().renewals, 1);
        // 释放后他人可立即授予(无需等待 skew)
        assert!(s
            .apply(1, 5, &Op::LeaseRelease { instance: "i1".into(), holder: "A".into() })
            .is_ok());
        assert!(s.lease_of("i1").is_none());
        assert!(s.apply(1, 6, &grant("i1", "B", 10_001)).is_ok());
        assert_eq!(s.lease_of("i1").unwrap().holder, "B");
    }

    #[test]
    fn step_ledger_is_effective_once_and_keeps_first_result() {
        let mut s = sm();
        assert!(s
            .apply(1, 2, &Op::StepBegin { task_id: "t-1".into(), node: "n1".into(), step: 0, idem_key: "docker-run:c1".into() })
            .is_ok());
        assert!(s
            .apply(1, 3, &Op::StepDone { task_id: "t-1".into(), node: "n1".into(), step: 0, idem_key: "docker-run:c1".into(), result: json!({"container":"c1"}) })
            .is_ok());
        // 重放 StepBegin → 短路 + 回放首次结果
        match s.apply(1, 4, &Op::StepBegin { task_id: "t-1".into(), node: "n1".into(), step: 0, idem_key: "docker-run:c1".into() }) {
            Applied::StepAlreadyDone { result } => {
                assert_eq!(result, Some(json!({"container":"c1"})));
            }
            other => panic!("应短路,实际 {other:?}"),
        }
        // 重放 StepDone(不同结果)→ 仍短路且保留首次结果(有效一次)
        match s.apply(2, 5, &Op::StepDone { task_id: "t-1".into(), node: "n1".into(), step: 0, idem_key: "docker-run:c1".into(), result: json!({"container":"c1-DUPLICATE"}) }) {
            Applied::StepAlreadyDone { result } => {
                assert_eq!(result, Some(json!({"container":"c1"})), "首次结果不得被覆盖");
            }
            other => panic!("应短路,实际 {other:?}"),
        }
        // 不同 idem_key → 独立步骤
        assert!(s
            .apply(1, 6, &Op::StepBegin { task_id: "t-1".into(), node: "n1".into(), step: 1, idem_key: "sql:x".into() })
            .is_ok());
    }

    #[test]
    fn step_key_is_unambiguous_under_crafted_names() {
        let mut s = sm();
        // 构造会让朴素 "a|b" 拼接冲突的两个 idem_key
        assert!(s.apply(1, 2, &Op::StepBegin { task_id: "t".into(), node: "n".into(), step: 0, idem_key: "k|1".into() }).is_ok());
        assert!(s.apply(1, 3, &Op::StepBegin { task_id: "t".into(), node: "n|0".into(), step: 0, idem_key: "k".into() }).is_ok());
        assert_eq!(s.steps.len(), 2, "转义后键必须互不冲突");
    }

    #[test]
    fn audit_seq_is_monotonic_and_bounded() {
        let mut s = StateMachine::new(0, SKEW);
        s.audit_tail_cap = 3;
        for i in 1..=5u64 {
            assert!(s
                .apply(1, i, &Op::AuditAppend {
                    who: "u".into(),
                    instance: "i1".into(),
                    action: format!("a{i}"),
                    params: "".into(),
                    result: "ok".into(),
                    task_id: "".into(),
                })
                .is_ok());
        }
        assert_eq!(s.audit_tail().len(), 3, "审计尾受上限约束");
        assert_eq!(s.audit_tail().last().unwrap().seq, 5);
        assert_eq!(s.to_image().audit_seq, 5, "总序号不回退(sink 可据此检测缺口)");
    }

    #[test]
    fn apply_is_deterministic_and_survives_snapshot_roundtrip() {
        let ops = vec![
            (1u64, 1u64, Op::ConfigSet { config_epoch: 1, max_skew_ms: SKEW }),
            (1, 2, Op::Put { key: "i/i1".into(), value: json!({"status":"running"}), expect_ver: None }),
            (1, 3, grant("i1", "A", 0)),
            (1, 4, Op::AuditAppend { who: "u".into(), instance: "i1".into(), action: "create".into(), params: "".into(), result: "ok".into(), task_id: "t-1".into() }),
            (2, 5, Op::LeaseRenew { instance: "i1".into(), holder: "A".into(), at_ms: 5_000 }),
        ];
        let mut a = StateMachine::new(0, SKEW);
        let mut b = StateMachine::new(0, SKEW);
        for (t, i, op) in &ops {
            a.apply(*t, *i, op);
            b.apply(*t, *i, op);
        }
        assert_eq!(a.to_json(), b.to_json(), "同一 op 序列必须得到同一状态字节");
        // 快照往返后状态一致,且可继续 apply
        let mut c = StateMachine::from_json(&a.to_json()).unwrap();
        assert_eq!(c.to_json(), a.to_json());
        assert_eq!(c.applied_index(), 5);
        assert!(c.apply(2, 6, &grant("i2", "A", 9_000)).is_ok());
    }

    #[test]
    fn rejected_op_does_not_advance_state_but_index_does() {
        let mut s = sm();
        let before = s.to_json();
        let r = s.apply(1, 2, &Op::Put { key: "k".into(), value: json!(1), expect_ver: Some(7) });
        assert!(r.rejected().is_some());
        // 内容不变,但 applied_index 前移(日志位置是权威的)
        let mut expect = s.to_image();
        expect.applied_index = 2;
        assert_eq!(s.to_image().applied_index, 2);
        let mut b = StateMachine::from_json(&before).unwrap();
        b.applied_index = 2;
        assert_eq!(s.to_json(), b.to_json(), "被拒 op 不得改变状态内容");
    }

    /// 发现 15:过期租约回收必须是**确定性**的,且与"冲突检查本来就会放行"**逐点等价**。
    ///
    /// 等价性的判据(设计 §19 发现 15 的证明在代码里落地):
    ///   purge 只删 `expire_at_ms <= cutoff_ms` 的条目,而 cutoff 满足
    ///   `cutoff_ms + max_skew_ms <= 提议时刻 <= 任何后续授予的 at_ms`,
    ///   因此后续授予在"无 purge"下也必然通过 `at_ms >= expire_at + max_skew` 检查。
    #[test]
    fn lease_purge_is_deterministic_and_equivalent_to_conflict_rule() {
        // ── ① 确定性:同一串 op 在两个独立状态机上 replay → 视图逐字相同 ──
        let ops: Vec<(u64, u64, Op)> = vec![
            (1, 2, grant("i1", "A", 1_000)),   // expire = 31_000
            (1, 3, grant("i2", "B", 100_000)), // expire = 130_000(不该被回收)
            (1, 4, Op::LeasePurge { cutoff_ms: 31_000 }),
            (1, 5, Op::LeasePurge { cutoff_ms: 31_000 }), // 幂等重放
        ];
        let mut a = sm();
        let mut b = sm();
        for (t, i, op) in &ops {
            assert!(a.apply(*t, *i, op).is_ok());
            assert!(b.apply(*t, *i, op).is_ok());
        }
        assert_eq!(a.to_json(), b.to_json(), "回收必须是确定性的");
        let view: Vec<String> = a
            .leases_view(0)
            .iter()
            .map(|l| l["instance"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(view, vec!["i2"], "只应回收 expire <= cutoff 的条目: {view:?}");
        assert_eq!(a.lease_count(), 1);

        // ── ② 等价性只在**物理可能**的时序上成立 ──
        // 不变量:`cutoff_ms + max_skew_ms <= 提议时刻 <= 任何后续授予的 at_ms`。
        // 因此 `cutoff + skew` 是"purge 之后最早可能出现的授予时刻"。
        let cutoff = 31_000u64;
        let earliest_grant_at = cutoff + SKEW; // = 32_000
        for at in [earliest_grant_at, 40_000, 999_999] {
            let mut with = sm();
            with.apply(1, 2, &grant("i1", "A", 1_000));
            with.apply(1, 3, &Op::LeasePurge { cutoff_ms: cutoff });
            let r_with = with.apply(1, 4, &grant("i1", "C", at));

            let mut without = sm();
            without.apply(1, 2, &grant("i1", "A", 1_000));
            let r_without = without.apply(1, 3, &grant("i1", "C", at));

            assert!(
                r_with.is_ok() && r_without.is_ok(),
                "at_ms={at}(>= cutoff+skew):两条路径都应放行(with={r_with:?}, without={r_without:?})"
            );
        }

        // ── ③ 看护"cutoff 过早"这种危险配置 ──
        // 如果 cutoff 早于 expire,什么都删不掉 —— 也就是说**回收能力**受 expire 约束;
        // 而"提前于 expire+skew 就放行"只能通过违反 ② 的不变量达成(物理上不可能),
        // 因此该不变量必须由 cutoff 的取值规则保证(`now - max_skew - 宽限`),
        // 而不是靠 apply 里的比较。runtime 侧有对应的算术断言与验收用例。
        let mut s = sm();
        s.apply(1, 2, &grant("i1", "A", 1_000));
        s.apply(1, 3, &Op::LeasePurge { cutoff_ms: 999 }); // 删不掉任何东西
        assert_eq!(s.lease_count(), 1, "cutoff 早于 expire 时不得回收");
        let r = s.apply(1, 4, &grant("i1", "C", 31_500)); // < expire + skew
        assert!(r.rejected().is_some(), "未过安全边界前不得授予: {r:?}");
    }

    /// 释放路径收口:正常释放后不留条目,残留只能来自"崩溃/未释放"
    #[test]
    fn lease_release_leaves_no_residue() {
        let mut s = sm();
        s.apply(1, 2, &grant("i1", "A", 1_000));
        s.apply(1, 3, &grant("i2", "A", 1_000));
        assert_eq!(s.lease_count(), 2);
        assert!(s
            .apply(
                1,
                4,
                &Op::LeaseRelease { instance: "i1".into(), holder: "A".into() }
            )
            .is_ok());
        assert_eq!(s.lease_count(), 1, "释放后应立刻少一条");
        // 非持有者释放 → 拒绝且不改状态(状态机的 holder 门禁)
        let r = s.apply(
            1,
            5,
            &Op::LeaseRelease { instance: "i2".into(), holder: "B".into() },
        );
        assert!(r.rejected().is_some(), "非持有者不得释放: {r:?}");
        assert_eq!(s.lease_count(), 1);
    }
}
