// rdsctl HA — 分片组共识核心(设计 §5)
//
// 与传输层解耦:本模块只做"状态 + 收消息 → 出消息"的纯决策,tick/handle 由调用方驱动。
// 这样同一份代码既能跑在真实 HTTP 传输上,也能在确定性仿真里重放(S1–S5,设计 §6)。
//
// 安全规则(标准 Raft,最小子集):
//   - 投票:term 更高、或同 term 未投票且候选日志不落后 → 授予;授予后持久化(hard_state);
//   - pre-vote:先探票不升 term,避免分区归来的旧 leader 打断健康集群(设计 §5.2);
//   - 提交:多数派 match_index ≥ N 且 entry.term == current_term 才提交(禁止直接提交旧 term 条目);
//   - AppendEntries:prev_log_index/term 校验失败则回退(带冲突提示);
//   - 重启:term/voted_for 必须持久化(否则会重复投票 → 可能选出两个 leader);
//   - 快照:压缩日志并把链锚点重锚到快照哈希;对落后 follower 走 InstallSnapshot。

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::clock::SharedClock;
use super::log::{self, Log, PendingEntry, KIND_ENTRY};
use super::snapshot::{self, SnapshotFile};
use super::state::{Applied, Op, StateMachine};
use super::{Fence, HaError, HaResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Follower => "follower",
            Role::Candidate => "candidate",
            Role::Leader => "leader",
        }
    }
}

/// 时钟偏移采样窗口大小(NTP 式最小延迟过滤:延迟恒 ≥ 0,故 `|offset|` 最小的样本最接近真值)
const CLOCK_SAMPLE_WINDOW: usize = 16;
/// 采样有效期(ms):过期观测不再参与 A1 判定。
/// 旧观测不能代表"当前"偏移;过期即视为"未验证"(而不是"超界")。
const CLOCK_SAMPLE_TTL_MS: u64 = 10_000;
/// 判定 A1 违规所需的持续超界样本数:单次延迟尖峰不足以判定
const CLOCK_VIOLATION_MIN_SAMPLES: usize = 2;
/// RTT/2 修正量的上限(ms)。
///
/// 修正量估计的是单程延迟,正常情况下不该有几千毫秒;设上限是为了防"一次被卡住的
/// 投递(接近 3s 投递超时)把真实偏移整段抹平"。取 2000:远大于任何合理的控制面
/// RTT(跨区也不过数百毫秒),又小于投递超时,慢消息不能完全抵消真偏移。
const CLOCK_RTT_CORRECTION_CAP_MS: u64 = 2_000;

/// 一次时钟偏移观测(带本地时刻;时间戳只用于"新鲜度"判定,不参与共识确定性)
#[derive(Debug, Clone, Copy)]
pub struct ClockSample {
    /// 偏移估计(sender 时钟 − 本地时钟),**已按 RTT/2 剔除单程延迟**;判定一律用这个值
    pub offset_ms: i64,
    /// 未修正的观测(含单程延迟;仅诊断展示)。远端报数时等于 `offset_ms`
    pub raw_ms: i64,
    /// 本机测得的 RTT(ms);0 = 没有本机 RTT 修正(老版本对端 / 远端报数)
    pub rtt_ms: u64,
    pub at_ms: u64,
}

#[derive(Debug, Clone)]
pub struct RaftConfig {
    pub node_id: String,
    pub shard: u16,
    /// 成员表(含自身);奇数 ≥3 由上层校验(前提 A3)
    pub voters: Vec<String>,
    /// 选举超时区间(ms)
    pub election_timeout_ms: (u64, u64),
    /// 心跳周期(ms)
    pub heartbeat_ms: u64,
    /// 单次 AppendEntries 最大条目数
    pub max_entries_per_append: usize,
    /// 日志条目数超此阈值触发快照 + 压实(0=关闭)
    pub snapshot_entry_threshold: usize,
    /// ttl 下限的时钟偏移上界(ms;前提 A1)。写入状态机以保证各副本判定一致
    pub max_skew_ms: u64,
}

impl RaftConfig {
    pub fn new(node_id: &str, shard: u16, voters: Vec<String>) -> Self {
        Self {
            node_id: node_id.to_string(),
            shard,
            voters,
            election_timeout_ms: (1500, 3000),
            heartbeat_ms: 300,
            max_entries_per_append: 256,
            snapshot_entry_threshold: 50_000,
            max_skew_ms: 1000,
        }
    }

    pub fn majority(&self) -> usize {
        self.voters.len() / 2 + 1
    }

    pub fn others(&self) -> Vec<String> {
        self.voters
            .iter()
            .filter(|v| **v != self.node_id)
            .cloned()
            .collect()
    }

    /// 前提 A3 校验:奇数且 ≥3、id 唯一、包含自身
    pub fn validate(&self) -> HaResult<()> {
        if self.voters.len() < 3 {
            return Err(HaError::Config(format!(
                "voter 数不足({}):至少 3 个才有多数派容错",
                self.voters.len()
            )));
        }
        if self.voters.len() % 2 == 0 {
            return Err(HaError::Config(format!(
                "voter 数为偶数({}):必须为奇数",
                self.voters.len()
            )));
        }
        let uniq: BTreeSet<&String> = self.voters.iter().collect();
        if uniq.len() != self.voters.len() {
            return Err(HaError::Config("成员表存在重复 id".into()));
        }
        if !self.voters.iter().any(|v| v == &self.node_id) {
            return Err(HaError::Config(format!(
                "自身 {} 不在成员表中",
                self.node_id
            )));
        }
        if self.max_skew_ms == 0 {
            return Err(HaError::Config("max_skew_ms 不能为 0(前提 A1)".into()));
        }
        // 时间参数必须"同档":选举超时是"允许丢几拍心跳"的容错预算。
        // 若心跳周期 ≥ 选举超时的一半,等于每拍都在赌网络 —— 同城勉强能跑,
        // 跨区/跨 AZ(WAN RTT + 抖动)下直接表现成选主风暴(设计 §20 跨区发现)。
        if self.heartbeat_ms == 0 {
            return Err(HaError::Config("心跳周期不能为 0".into()));
        }
        if self.election_timeout_ms.0 == 0
            || self.election_timeout_ms.1 < self.election_timeout_ms.0
        {
            return Err(HaError::Config(format!(
                "选举超时区间非法:{:?}(需 0 < 下限 ≤ 上限)",
                self.election_timeout_ms
            )));
        }
        if self.heartbeat_ms.saturating_mul(2) > self.election_timeout_ms.0 {
            return Err(HaError::Config(format!(
                "心跳周期 {}ms 不得 ≥ 选举超时下限 {}ms 的一半(时间参数必须同档):\
                 跨区/跨 AZ 部署请把两者一起放大,并保证选举超时 ≥ 4×实测 p99 RTT;\
                 见 docs/control-plane-ha-design.md §20 跨区部署",
                self.heartbeat_ms, self.election_timeout_ms.0
            )));
        }
        Ok(())
    }
}

/// 日志条目摘要(传输用;载荷为 Op 的 JSON)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EntryMeta {
    pub term: u64,
    pub index: u64,
    pub op_json: String,
}

/// 共识消息
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Message {
    RequestVote {
        term: u64,
        candidate: String,
        last_log_index: u64,
        last_log_term: u64,
        pre_vote: bool,
        /// 发送方本地时间(ms):接收方据此估算时钟偏移(前提 A1 的实测依据)
        #[serde(default)]
        sender_ms: u64,
    },
    RequestVoteResp {
        term: u64,
        voter: String,
        granted: bool,
        #[serde(default)]
        sender_ms: u64,
        /// 回显请求里的 `sender_ms`(= 发起方的 T1),供发起方算 RTT 并修正偏移(设计 §19)
        #[serde(default)]
        echo_ms: u64,
    },
    AppendEntries {
        term: u64,
        leader: String,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<EntryMeta>,
        leader_commit: u64,
        /// 发送方本地时间(ms):心跳天然是周期性探针 → 持续测量偏移
        #[serde(default)]
        sender_ms: u64,
        /// leader 对**本 follower** 的修正后偏移估计(ms;0 = 未知)。
        /// follower 无法自测 RTT,只能由 leader 回带(设计 §20 跨区发现)
        #[serde(default)]
        peer_skew_ms: u64,
    },
    AppendEntriesResp {
        term: u64,
        follower: String,
        success: bool,
        match_index: u64,
        /// 回退提示:follower 在该 index 上的 term(用于快速定位冲突)
        conflict_index: Option<u64>,
        #[serde(default)]
        sender_ms: u64,
        /// 回显请求里的 `sender_ms`(= leader 的 T1),供 leader 算 RTT 并修正偏移(设计 §19)
        #[serde(default)]
        echo_ms: u64,
    },
    InstallSnapshot {
        term: u64,
        leader: String,
        snapshot_json: String,
    },
    InstallSnapshotResp {
        term: u64,
        follower: String,
        success: bool,
        last_included_index: u64,
    },
    /// 领导权转移:leader 让某个已追平的 follower **立即**发起选举(设计 §5.2 优雅让位)
    TimeoutNow {
        term: u64,
        leader: String,
    },
}

impl Message {
    pub fn term(&self) -> u64 {
        match self {
            Message::RequestVote { term, .. }
            | Message::RequestVoteResp { term, .. }
            | Message::AppendEntries { term, .. }
            | Message::AppendEntriesResp { term, .. }
            | Message::InstallSnapshot { term, .. }
            | Message::InstallSnapshotResp { term, .. }
            | Message::TimeoutNow { term, .. } => *term,
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Message::RequestVote { .. } => "request_vote",
            Message::RequestVoteResp { .. } => "request_vote_resp",
            Message::AppendEntries { .. } => "append_entries",
            Message::AppendEntriesResp { .. } => "append_entries_resp",
            Message::InstallSnapshot { .. } => "install_snapshot",
            Message::InstallSnapshotResp { .. } => "install_snapshot_resp",
            Message::TimeoutNow { .. } => "timeout_now",
        }
    }
}

/// 出站消息
#[derive(Debug, Clone, PartialEq)]
pub struct Outbound {
    pub to: String,
    pub msg: Message,
    /// 是否在传输失败时计入"对端不可达"(用于 quorum_ok 判定)
    pub critical: bool,
}

/// 竞选阶段(必须显式跟踪:`voted_for.is_none()` 之类的推断会在"上个 term 投过票"时失真,
/// 使 pre-vote 无法升级为正式选举 —— 那是安全/可用性缺陷)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Campaign {
    None,
    PreVote,
    Vote,
}

/// 持久化的硬状态(Raft 安全必需)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct HardState {
    current_term: u64,
    voted_for: Option<String>,
}

/// 就绪/降级信息(/readyz 契约,见 deploy/README.md §7)
#[derive(Debug, Clone, PartialEq)]
pub struct Ready {
    pub role: &'static str,
    pub leader: Option<String>,
    pub term: u64,
    pub commit_index: u64,
    pub applied_index: u64,
    pub quorum_ok: bool,
    pub log_writable: bool,
    pub fsync_ok: bool,
    pub degraded_reason: Option<String>,
}

pub struct Node {
    pub cfg: RaftConfig,
    pub dir: PathBuf,
    clock: SharedClock,
    pub log: Log,
    pub sm: StateMachine,
    hard: HardState,
    role: Role,
    commit_index: u64,
    leader_id: Option<String>,
    votes: BTreeSet<String>,
    pre_votes: BTreeSet<String>,
    campaign: Campaign,
    election_deadline_ms: u64,
    last_heartbeat_ms: u64,
    next_index: BTreeMap<String, u64>,
    match_index: BTreeMap<String, u64>,
    /// 每个 peer 最近一次有效响应的时间(quorum_ok / leader-lease 判定)
    peer_ack_ms: BTreeMap<String, u64>,
    /// 状态机 apply 结果(按日志 index 索引,供提案方读取)
    results: BTreeMap<u64, Applied>,
    /// fsync 失败标记:一旦为 false,readyz 必须显式降级(设计 §7.2)
    fsync_ok: bool,
    /// 对每个 peer 的时钟偏移**采样窗口**(ms;正=peer 比我快)
    ///
    /// 为什么是窗口而不是"最近一次":单程延迟会被算进偏移(见 `observe_clock`),
    /// 只留最近一次等于把任意一次排队尖峰当成真实偏移;而且一旦该 peer 不再发消息,
    /// 那个尖峰就会被**永久锁死**(实测:n3 因一条被拖慢的选举消息把 1578ms 记成"时钟偏移",
    /// 此后该节点一直拒绝授予租约,而三副本其实同机、真实偏移只有 2–4ms)。
    peer_offset_samples: BTreeMap<String, Vec<ClockSample>>,
    rng: u64,
    /// 最后一次快照的 include index/term(用于判断是否需要 InstallSnapshot、校验 prev_term)
    snapshot_index: u64,
    snapshot_term: u64,
}

impl Node {
    /// 打开节点:快照 → 状态机 → 日志(以快照哈希为链锚点)→ 硬状态
    pub fn open(cfg: RaftConfig, dir: &Path, clock: SharedClock) -> HaResult<Self> {
        cfg.validate()?;
        std::fs::create_dir_all(dir)?;

        let snap = snapshot::load(dir, cfg.shard)?;
        let (sm, seed_hash, snap_index) = match &snap {
            Some(s) => {
                let sm = StateMachine::from_json(&s.state_json)
                    .map_err(|e| HaError::Corrupt(format!("快照状态机解析失败:{e}")))?;
                (sm, s.seed_bytes(), s.last_included_index)
            }
            None => (
                StateMachine::new(cfg.shard, cfg.max_skew_ms),
                [0u8; 32],
                0,
            ),
        };
        let log = Log::open(dir, cfg.shard, seed_hash)?;
        if log.truncated_tail_bytes() > 0 {
            tracing::warn!(
                "节点 {} 恢复时丢弃日志尾部 {} 字节(异常掉电)",
                cfg.node_id,
                log.truncated_tail_bytes()
            );
        }
        let hard = read_hard_state(dir)?;
        let snap_term = snap.as_ref().map(|s| s.last_included_term).unwrap_or(0);

        let now = clock.now_ms();
        let rng = seed_rng(&cfg.node_id, cfg.shard);
        let mut node = Node {
            cfg,
            dir: dir.to_path_buf(),
            clock,
            log,
            sm,
            hard,
            role: Role::Follower,
            commit_index: snap_index,
            leader_id: None,
            votes: BTreeSet::new(),
            pre_votes: BTreeSet::new(),
            campaign: Campaign::None,
            election_deadline_ms: 0,
            last_heartbeat_ms: 0,
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
            peer_ack_ms: BTreeMap::new(),
            peer_offset_samples: BTreeMap::new(),
            results: BTreeMap::new(),
            fsync_ok: true,
            rng,
            snapshot_index: snap_index,
            snapshot_term: snap_term,
        };
        node.reset_election_deadline(now);
        node.apply_committed();
        Ok(node)
    }

    pub fn role(&self) -> Role {
        self.role
    }
    pub fn term(&self) -> u64 {
        self.hard.current_term
    }
    pub fn commit_index(&self) -> u64 {
        self.commit_index
    }
    pub fn leader_id(&self) -> Option<&str> {
        self.leader_id.as_deref()
    }
    pub fn snapshot_index(&self) -> u64 {
        self.snapshot_index
    }
    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }

    /// 每个成员(含自己)的复制进度与最近响应时间 —— 只读,供管控集群页观测。
    ///
    /// `last_ack_age_ms = null` 表示**本进程从未收到过该 peer 的响应**(首次启动/长期不可达),
    /// 与"刚收到"(0ms)必须区分,否则界面会把"没联系过"显示成"刚刚联系过"。
    /// 非 leader 的 `match_index` 无意义(只有 leader 维护复制进度),此时返回 null。
    pub fn peers_view(&self, now: u64) -> Vec<Value> {
        let is_leader = self.is_leader();
        let last_index = self.log.last_index();
        self.cfg
            .voters
            .iter()
            .map(|v| {
                json!({
                    "id": v,
                    "is_self": v == &self.cfg.node_id,
                    "match_index": if is_leader {
                        json!(self.match_index.get(v).copied().unwrap_or(0))
                    } else {
                        Value::Null
                    },
                    "next_index": if is_leader {
                        json!(self.next_index.get(v).copied().unwrap_or(0))
                    } else {
                        Value::Null
                    },
                    "log_last_index": if is_leader { json!(last_index) } else { Value::Null },
                    "repl_lag": if is_leader {
                        json!(last_index.saturating_sub(self.match_index.get(v).copied().unwrap_or(0)))
                    } else {
                        Value::Null
                    },
                    "last_ack_age_ms": self.peer_ack_ms.get(v).map(|t| now.saturating_sub(*t)),
                    // 判定用值(已按最小延迟过滤)
                    "clock_offset_ms": self.peer_offset_filtered(v, now),
                    // 诊断用值:最新一次采样(含单程延迟);与上面的差 = 被过滤掉的延迟量级
                    "clock_offset_latest_ms": self.peer_offset_latest(v).map(|s| s.offset_ms),
                })
            })
            .collect()
    }

    fn reset_election_deadline(&mut self, now: u64) {
        let (lo, hi) = self.cfg.election_timeout_ms;
        let span = hi.saturating_sub(lo).max(1);
        let jitter = self.next_rand() % span;
        self.election_deadline_ms = now + lo + jitter;
    }

    fn next_rand(&mut self) -> u64 {
        // splitmix64:确定性(同一 seed 序列可复现),满足仿真可重放要求
        self.rng = self.rng.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.rng;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn persist_hard_state(&mut self) -> HaResult<()> {
        let path = self.dir.join("hard_state.json");
        let tmp = self.dir.join("hard_state.tmp");
        let body = serde_json::to_vec(&self.hard)
            .map_err(|e| HaError::Corrupt(format!("hard_state 序列化失败:{e}")))?;
        {
            use std::io::Write;
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(&body)?;
            // 投票与 term 必须落盘:否则重启会重复投票(可能选出两个 leader)
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &path)?;
        log::fsync_dir(&self.dir)?;
        Ok(())
    }

    fn become_follower(&mut self, term: u64, leader: Option<String>) -> HaResult<()> {
        if term > self.hard.current_term {
            self.hard.current_term = term;
            self.hard.voted_for = None;
            self.persist_hard_state()?;
        }
        if self.role != Role::Follower {
            tracing::info!(
                "节点 {} 退位为 follower(term={}, leader={:?})",
                self.cfg.node_id,
                self.hard.current_term,
                leader
            );
        }
        self.role = Role::Follower;
        self.campaign = Campaign::None;
        self.leader_id = leader;
        self.votes.clear();
        self.pre_votes.clear();
        Ok(())
    }

    /// 时钟驱动:tick 返回需要发送的消息
    pub fn tick(&mut self) -> Vec<Outbound> {
        let now = self.clock.now_ms();
        match self.role {
            Role::Leader => {
                if now.saturating_sub(self.last_heartbeat_ms) >= self.cfg.heartbeat_ms {
                    self.last_heartbeat_ms = now;
                    return self.broadcast_append();
                }
                Vec::new()
            }
            Role::Follower | Role::Candidate => {
                if now >= self.election_deadline_ms {
                    self.start_pre_vote();
                    if self.campaign == Campaign::PreVote {
                        let sender_ms = self.clock.now_ms();
                        return self.send_to_others(Message::RequestVote {
                            term: self.hard.current_term + 1,
                            candidate: self.cfg.node_id.clone(),
                            last_log_index: self.log.last_index(),
                            last_log_term: self.log.last_term(),
                            pre_vote: true,
                            sender_ms,
                        });
                    }
                }
                Vec::new()
            }
        }
    }

    fn start_pre_vote(&mut self) {
        let now = self.clock.now_ms();
        self.reset_election_deadline(now);
        if self.role == Role::Leader {
            return;
        }
        self.role = Role::Candidate;
        self.campaign = Campaign::PreVote;
        // 竞选期间**不再承认任何 leader**:留着陈旧的 leader_id 会误导上层(实测:副本
        // 恢复后持续把写转发给一个已经不是 leader 的节点,导致该副本 30s 内完全不可写)。
        self.leader_id = None;
        self.pre_votes.clear();
        self.pre_votes.insert(self.cfg.node_id.clone());
        self.votes.clear(); // 上一轮的票在新一轮竞选里无效
        tracing::debug!("节点 {} 发起 pre-vote(term 不变)", self.cfg.node_id);
    }

    fn start_election(&mut self) -> HaResult<Vec<Outbound>> {
        self.hard.current_term += 1;
        self.hard.voted_for = Some(self.cfg.node_id.clone());
        self.persist_hard_state()?;
        self.role = Role::Candidate;
        self.campaign = Campaign::Vote;
        self.leader_id = None;
        self.votes.clear();
        self.votes.insert(self.cfg.node_id.clone());
        self.pre_votes.clear();
        let now = self.clock.now_ms();
        self.reset_election_deadline(now);
        tracing::info!("节点 {} 开始选举(term={})", self.cfg.node_id, self.hard.current_term);
        let sender_ms = self.clock.now_ms();
        Ok(self.send_to_others(Message::RequestVote {
            term: self.hard.current_term,
            candidate: self.cfg.node_id.clone(),
            last_log_index: self.log.last_index(),
            last_log_term: self.log.last_term(),
            pre_vote: false,
            sender_ms,
        }))
    }

    fn send_to_others(&self, msg: Message) -> Vec<Outbound> {
        self.cfg
            .others()
            .into_iter()
            .map(|to| Outbound {
                to,
                msg: msg.clone(),
                critical: true,
            })
            .collect()
    }

    fn broadcast_append(&mut self) -> Vec<Outbound> {
        let others = self.cfg.others();
        let mut out = Vec::with_capacity(others.len());
        for to in others {
            let out_msgs = self.append_for(&to);
            out.extend(out_msgs);
        }
        out
    }

    fn append_for(&mut self, peer: &str) -> Vec<Outbound> {
        let next = *self
            .next_index
            .get(peer)
            .unwrap_or(&(self.log.last_index() + 1));
        // 需要的条目已被压实 → 用快照补齐
        if next <= self.snapshot_index && self.snapshot_index > 0 {
            if let Ok(Some(s)) = snapshot::load(&self.dir, self.cfg.shard) {
                if let Ok(json) = serde_json::to_string(&s) {
                    return vec![Outbound {
                        to: peer.to_string(),
                        msg: Message::InstallSnapshot {
                            term: self.hard.current_term,
                            leader: self.cfg.node_id.clone(),
                            snapshot_json: json,
                        },
                        critical: false,
                    }];
                }
            }
        }
        let prev_index = next.saturating_sub(1);
        let prev_term = if prev_index == 0 {
            0
        } else if prev_index == self.snapshot_index {
            self.snapshot_term
        } else {
            self.log.term_at(prev_index).unwrap_or(0)
        };
        let entries: Vec<EntryMeta> = self
            .log
            .entries_from(next)
            .into_iter()
            .take(self.cfg.max_entries_per_append)
            .map(|r| EntryMeta {
                term: r.term,
                index: r.index,
                op_json: String::from_utf8_lossy(&r.payload).to_string(),
            })
            .collect();
        let sender_ms = self.clock.now_ms();
        // 回带"我对该 peer 的修正后偏移估计":follower 测不到 RTT,只能靠 leader 告知
        // (设计 §20 跨区发现:否则 follower 会把单程延迟当成时钟偏移)。
        let peer_skew_ms = self.peer_skew_estimate_ms(peer).unwrap_or(0);
        vec![Outbound {
            to: peer.to_string(),
            msg: Message::AppendEntries {
                term: self.hard.current_term,
                leader: self.cfg.node_id.clone(),
                prev_log_index: prev_index,
                prev_log_term: prev_term,
                entries,
                leader_commit: self.commit_index,
                sender_ms,
                peer_skew_ms,
            },
            critical: true,
        }]
    }

    /// 处理一条入站消息
    pub fn handle(&mut self, from: &str, msg: Message) -> Vec<Outbound> {
        match self.handle_inner(from, msg) {
            Ok(out) => out,
            Err(e) => {
                tracing::error!("节点 {} 处理 {} 失败:{e}", self.cfg.node_id, e);
                // fsync/持久化失败:标记降级(readyz 会显式报告,不再"看起来就绪")
                self.fsync_ok = false;
                Vec::new()
            }
        }
    }

    fn handle_inner(&mut self, from: &str, msg: Message) -> HaResult<Vec<Outbound>> {
        let now = self.clock.now_ms();
        let msg_term = msg.term();

        // term 更高的消息:先降级(投票与 AppendEntries 通用规则)
        if msg_term > self.hard.current_term {
            match &msg {
                Message::RequestVote { pre_vote: true, .. } => {
                    // pre-vote 不升 term(否则被分区旧节点反复打断)
                }
                _ => {
                    tracing::warn!(
                        "收到更高 term 的消息({} term={} > 本地 {}):降级并重新选主",
                        msg.kind(),
                        msg_term,
                        self.hard.current_term
                    );
                    self.become_follower(msg_term, None)?;
                }
            }
        }

        match msg {
            Message::RequestVote {
                term,
                candidate,
                last_log_index,
                last_log_term,
                pre_vote,
                sender_ms,
            } => {
                self.peer_ack_ms.insert(from.to_string(), now);
                self.observe_clock(from, sender_ms, now);
                let my_ms = self.clock.now_ms();
                if pre_vote {
                    let up_to_date = self.log_is_up_to_date(last_log_index, last_log_term);
                    let leader_alive = self.leader_id.is_some()
                        && now.saturating_sub(self.last_heartbeat_ms) < self.cfg.election_timeout_ms.0;
                    let granted = term > self.hard.current_term && up_to_date && !leader_alive;
                tracing::debug!(
                    "pre-vote 来自 {}:term={} > {}? {}; 日志追平? {}; leader_alive? {} (距上次心跳 {}ms,选举超时 {}ms) ⇒ granted={}",
                    candidate,
                    term,
                    self.hard.current_term,
                    term > self.hard.current_term,
                    up_to_date,
                    leader_alive,
                    now.saturating_sub(self.last_heartbeat_ms),
                    self.cfg.election_timeout_ms.0,
                    granted
                );
                    return Ok(vec![Outbound {
                        to: candidate,
                        msg: Message::RequestVoteResp {
                            term: self.hard.current_term,
                            voter: self.cfg.node_id.clone(),
                            granted,
                            sender_ms: my_ms,
                            echo_ms: sender_ms,
                        },
                        critical: false,
                    }]);
                }
                if term < self.hard.current_term {
                    return Ok(vec![Outbound {
                        to: candidate,
                        msg: Message::RequestVoteResp {
                            term: self.hard.current_term,
                            voter: self.cfg.node_id.clone(),
                            granted: false,
                            sender_ms: my_ms,
                            echo_ms: sender_ms,
                        },
                        critical: false,
                    }]);
                }
                let already = self.hard.voted_for.as_deref() == Some(candidate.as_str());
                let can_vote = self.hard.voted_for.is_none() || already;
                let granted = term == self.hard.current_term
                    && can_vote
                    && self.log_is_up_to_date(last_log_index, last_log_term);
                if granted {
                    self.hard.voted_for = Some(candidate.clone());
                    self.persist_hard_state()?;
                    self.reset_election_deadline(now); // 授予投票相当于承认本轮选举
                    tracing::info!("节点 {} 投票给 {candidate}(term={term})", self.cfg.node_id);
                }
                Ok(vec![Outbound {
                    to: candidate,
                    msg: Message::RequestVoteResp {
                        term: self.hard.current_term,
                        voter: self.cfg.node_id.clone(),
                        granted,
                        sender_ms: self.clock.now_ms(),
                        echo_ms: sender_ms,
                    },
                    critical: false,
                }])
            }

            Message::RequestVoteResp {
                term,
                voter,
                granted,
                sender_ms,
                echo_ms,
            } => {
                self.peer_ack_ms.insert(voter.clone(), now);
                self.observe_clock_rtt(&voter, sender_ms, now, echo_ms);
                if term != self.hard.current_term || !granted {
                    return Ok(Vec::new());
                }
                if self.role != Role::Candidate {
                    return Ok(Vec::new());
                }
                match self.campaign {
                    // pre-vote:只收探票;达到多数派才发起正式选举(不在这里直接当 leader)
                    Campaign::PreVote => {
                        self.pre_votes.insert(voter.clone());
                        if self.pre_votes.len() >= self.cfg.majority() {
                            return self.start_election();
                        }
                    }
                    // 正式选举:收票;多数派 → 当选
                    Campaign::Vote => {
                        self.votes.insert(voter.clone());
                        if self.votes.len() >= self.cfg.majority() {
                            return Ok(self.become_leader()?);
                        }
                    }
                    Campaign::None => {}
                }
                Ok(Vec::new())
            }

            Message::AppendEntries {
                term,
                leader,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                sender_ms,
                peer_skew_ms,
            } => {
                self.peer_ack_ms.insert(leader.clone(), now);
                // 优先采信 leader 回带的**已修正**估计(follower 无 RTT 可测);
                // 缺失(老版本对端)才退回单向上界口径(设计 §20 跨区发现)。
                if peer_skew_ms > 0 {
                    self.observe_peer_skew(&leader, peer_skew_ms, now);
                } else {
                    self.observe_clock(&leader, sender_ms, now);
                }
                if term < self.hard.current_term {
                    return Ok(vec![self.append_resp(
                        &leader,
                        false,
                        0,
                        Some(self.log.last_index()),
                        sender_ms,
                    )]);
                }
                self.become_follower(term, Some(leader.clone()))?;
                self.last_heartbeat_ms = now;
                self.reset_election_deadline(now);

                // 一致性校验:prev 必须与本地日志一致;紧接快照处用快照 term 校验
                if prev_log_index > 0 {
                    let local_term = if prev_log_index == self.snapshot_index {
                        Some(self.snapshot_term)
                    } else {
                        self.log.term_at(prev_log_index)
                    };
                    if local_term != Some(prev_log_term) {
                        let conflict = self
                            .log
                            .first_index()
                            .saturating_sub(1)
                            .max(1)
                            .min(prev_log_index.max(1));
                        return Ok(vec![self.append_resp(&leader, false, 0, Some(conflict), sender_ms)]);
                    }
                }

                // 追加(处理冲突后缀)
                if !entries.is_empty() {
                    let first = entries[0].index;
                    if let Some(t) = self.log.term_at(first) {
                        let same = entries[0].term == t;
                        if !same {
                            self.log.truncate_suffix(first)?;
                        }
                    }
                    let mut pending = Vec::new();
                    for e in &entries {
                        if self.log.get(e.index).is_some() {
                            continue; // 已有相同条目(幂等)
                        }
                        pending.push(PendingEntry {
                            kind: KIND_ENTRY,
                            term: e.term,
                            index: e.index,
                            payload: e.op_json.as_bytes().to_vec(),
                        });
                    }
                    if !pending.is_empty() {
                        if let Err(e) = self.log.append_batch(&pending) {
                            self.fsync_ok = false;
                            return Err(e);
                        }
                    }
                }

                // 提交推进:min(leader_commit, last_index)
                let new_commit = leader_commit.min(self.log.last_index());
                if new_commit > self.commit_index {
                    self.commit_index = new_commit;
                    self.apply_committed();
                }
                Ok(vec![self.append_resp(
                    &leader,
                    true,
                    self.log.last_index(),
                    None,
                    sender_ms,
                )])
            }

            Message::AppendEntriesResp {
                term,
                follower,
                success,
                match_index,
                conflict_index,
                sender_ms,
                echo_ms,
            } => {
                self.peer_ack_ms.insert(follower.clone(), now);
                self.observe_clock_rtt(&follower, sender_ms, now, echo_ms);
                if term > self.hard.current_term {
                    self.become_follower(term, None)?;
                    return Ok(Vec::new());
                }
                if self.role != Role::Leader || term != self.hard.current_term {
                    return Ok(Vec::new());
                }
                if success {
                    self.match_index.insert(follower.clone(), match_index);
                    self.next_index.insert(follower.clone(), match_index + 1);
                    self.advance_commit();
                } else {
                    let hint = conflict_index.unwrap_or(1);
                    let cur = *self
                        .next_index
                        .get(&follower)
                        .unwrap_or(&(self.log.last_index() + 1));
                    let next = cur.min(hint).max(self.snapshot_index + 1).max(1);
                    self.next_index.insert(follower.clone(), next);
                    return Ok(self.append_for(&follower));
                }
                Ok(Vec::new())
            }

            Message::InstallSnapshot {
                term,
                leader,
                snapshot_json,
            } => {
                self.peer_ack_ms.insert(leader.clone(), now);
                if term < self.hard.current_term {
                    return Ok(Vec::new());
                }
                self.become_follower(term, Some(leader.clone()))?;
                self.last_heartbeat_ms = now;
                let snap: SnapshotFile = serde_json::from_str(&snapshot_json)
                    .map_err(|e| HaError::Corrupt(format!("InstallSnapshot 解析失败:{e}")))?;
                snap.verify()?;
                if snap.last_included_index <= self.snapshot_index {
                    return Ok(vec![Outbound {
                        to: leader,
                        msg: Message::InstallSnapshotResp {
                            term: self.hard.current_term,
                            follower: self.cfg.node_id.clone(),
                            success: true,
                            last_included_index: self.snapshot_index,
                        },
                        critical: false,
                    }]);
                }
                let sm = StateMachine::from_json(&snap.state_json)
                    .map_err(|e| HaError::Corrupt(format!("快照状态机解析失败:{e}")))?;
                snapshot::save(&self.dir, &snap)?;
                let seed = snap.seed_bytes();
                self.sm = sm;
                self.log.compact_upto(snap.last_included_index, seed)?;
                self.snapshot_index = snap.last_included_index;
                self.snapshot_term = snap.last_included_term;
                self.commit_index = self.commit_index.max(snap.last_included_index);
                tracing::info!(
                    "节点 {} 安装快照至 index={}",
                    self.cfg.node_id,
                    snap.last_included_index
                );
                Ok(vec![Outbound {
                    to: leader,
                    msg: Message::InstallSnapshotResp {
                        term: self.hard.current_term,
                        follower: self.cfg.node_id.clone(),
                        success: true,
                        last_included_index: snap.last_included_index,
                    },
                    critical: false,
                }])
            }

            Message::TimeoutNow { term, .. } => {
                // 收到让位指令:立即发起选举(不再等选举超时;pre-vote 亦跳过)
                if term < self.hard.current_term {
                    return Ok(Vec::new());
                }
                if !self.cfg.voters.iter().any(|v| v == &self.cfg.node_id) {
                    return Ok(Vec::new());
                }
                tracing::info!("节点 {} 收到 TimeoutNow,立即发起选举", self.cfg.node_id);
                self.reset_election_deadline(now);
                return self.start_election();
            }

            Message::InstallSnapshotResp {
                term,
                follower,
                success,
                last_included_index,
            } => {
                self.peer_ack_ms.insert(follower.clone(), now);
                if term > self.hard.current_term {
                    self.become_follower(term, None)?;
                    return Ok(Vec::new());
                }
                if self.role == Role::Leader && success {
                    self.match_index.insert(follower.clone(), last_included_index);
                    self.next_index.insert(follower.clone(), last_included_index + 1);
                }
                Ok(Vec::new())
            }
        }
    }

    fn append_resp(
        &self,
        to: &str,
        success: bool,
        match_index: u64,
        conflict_index: Option<u64>,
        echo_ms: u64,
    ) -> Outbound {
        Outbound {
            to: to.to_string(),
            msg: Message::AppendEntriesResp {
                term: self.hard.current_term,
                follower: self.cfg.node_id.clone(),
                success,
                match_index,
                conflict_index,
                sender_ms: self.clock.now_ms(),
                echo_ms,
            },
            critical: false,
        }
    }

    fn log_is_up_to_date(&self, cand_last_index: u64, cand_last_term: u64) -> bool {
        let my_term = self.log.last_term();
        let my_index = self.log.last_index();
        if cand_last_term != my_term {
            return cand_last_term > my_term;
        }
        cand_last_index >= my_index
    }

    fn become_leader(&mut self) -> HaResult<Vec<Outbound>> {
        self.role = Role::Leader;
        self.leader_id = Some(self.cfg.node_id.clone());
        let last = self.log.last_index();
        self.next_index.clear();
        self.match_index.clear();
        for v in self.cfg.voters.clone() {
            self.next_index.insert(v.clone(), last + 1);
            self.match_index.insert(v, 0);
        }
        let now = self.clock.now_ms();
        self.last_heartbeat_ms = now;
        self.peer_ack_ms.insert(self.cfg.node_id.clone(), now);
        tracing::info!(
            "节点 {} 当选 leader(term={}, last_index={})",
            self.cfg.node_id,
            self.hard.current_term,
            last
        );
        // 首个空 op:推进 commit index(标准做法)
        self.append_local(&Op::Noop)?;
        Ok(self.broadcast_append())
    }

    /// leader 追加本地日志并 fsync;返回 index
    fn append_local(&mut self, op: &Op) -> HaResult<u64> {
        if self.role != Role::Leader {
            return Err(HaError::NotLeader {
                leader: self.leader_id.clone(),
            });
        }
        let index = self.log.last_index() + 1;
        let payload = serde_json::to_vec(op)
            .map_err(|e| HaError::Corrupt(format!("op 序列化失败:{e}")))?;
        let batch = vec![PendingEntry {
            kind: KIND_ENTRY,
            term: self.hard.current_term,
            index,
            payload,
        }];
        if let Err(e) = self.log.append_batch(&batch) {
            self.fsync_ok = false;
            return Err(e);
        }
        self.match_index.insert(self.cfg.node_id.clone(), index);
        Ok(index)
    }

    /// 提案(仅 leader);返回日志 index,调用方随后 wait_applied(index) 等待提交与应用
    pub fn propose(&mut self, op: Op) -> HaResult<u64> {
        self.append_local(&op)
    }

    /// 主动推送(提案后立即复制)
    pub fn flush(&mut self) -> Vec<Outbound> {
        if self.role == Role::Leader {
            self.last_heartbeat_ms = self.clock.now_ms();
            self.broadcast_append()
        } else {
            Vec::new()
        }
    }

    fn advance_commit(&mut self) {
        let last = self.log.last_index();
        let maj = self.cfg.majority();
        let mut n = self.commit_index + 1;
        while n <= last {
            // 只提交当前 term 的条目(标准 Raft 规则)
            if self.log.term_at(n) != Some(self.hard.current_term) {
                n += 1;
                continue;
            }
            let count = self
                .match_index
                .values()
                .filter(|m| **m >= n)
                .count();
            if count >= maj {
                self.commit_index = n;
            }
            n += 1;
        }
        self.apply_committed();
    }

    fn apply_committed(&mut self) {
        while self.sm.applied_index() < self.commit_index {
            let idx = self.sm.applied_index() + 1;
            let Some(rec) = self.log.get(idx).cloned() else {
                // 条目已被压实(通常发生在安装快照之后):应用到快照位置即可
                break;
            };
            let op: Op = match serde_json::from_slice(&rec.payload) {
                Ok(op) => op,
                Err(e) => {
                    tracing::error!("日志 index={idx} 的 op 解析失败:{e}(跳过)");
                    self.sm.apply(rec.term, idx, &Op::Noop);
                    continue;
                }
            };
            let applied = self.sm.apply(rec.term, idx, &op);
            self.results.insert(idx, applied);
        }
        // results 只保留最近 4096 条
        while self.results.len() > 4096 {
            if let Some(k) = self.results.keys().next().copied() {
                self.results.remove(&k);
            }
        }
    }

    pub fn applied_index(&self) -> u64 {
        self.sm.applied_index()
    }

    /// 读取某 index 的 apply 结果(未提交/未应用则 None)
    pub fn result_of(&self, index: u64) -> Option<Applied> {
        self.results.get(&index).cloned()
    }

    /// 触发一次快照 + 日志压实(由上层在阈值/定时时调用)
    pub fn maybe_snapshot(&mut self) -> HaResult<bool> {
        let th = self.cfg.snapshot_entry_threshold;
        if th == 0 || self.log.len() <= th {
            return Ok(false);
        }
        let upto = self.sm.applied_index();
        if upto == 0 || upto <= self.snapshot_index {
            return Ok(false);
        }
        let term = self.log.term_at(upto).unwrap_or(self.sm.applied_term());
        let snap = SnapshotFile::new(
            self.cfg.shard,
            upto,
            term,
            self.sm.config_epoch(),
            self.sm.to_json(),
        );
        self.log.sync()?;
        snapshot::save(&self.dir, &snap)?;
        self.log.compact_upto(upto, snap.seed_bytes())?;
        self.snapshot_index = upto;
        self.snapshot_term = term;
        tracing::info!("节点 {} 完成快照:index={upto}", self.cfg.node_id);
        Ok(true)
    }

    /// quorum 可达性:多数派(含自身)在窗口内有过响应。
    ///
    /// 窗口取 max(4×心跳, 3×选举超时下限):过紧会导致"一两次心跳延迟就误判失去多数派",
    /// 而误判的代价是**拒绝一切写入**(fail-closed),连正在执行的步骤账本都写不进去。
    pub fn quorum_ok(&self, now: u64) -> bool {
        let window = self
            .cfg
            .heartbeat_ms
            .saturating_mul(4)
            .max(self.cfg.election_timeout_ms.0.saturating_mul(3));
        let mut alive = 1; // 自身
        for o in self.cfg.others() {
            if let Some(t) = self.peer_ack_ms.get(&o) {
                if now.saturating_sub(*t) <= window {
                    alive += 1;
                }
            }
        }
        alive >= self.cfg.majority()
    }

    /// leader-lease 读:多数派在选举超时窗口内确认过 → 可线性一致读
    pub fn can_serve_linearizable_read(&self, now: u64) -> bool {
        self.role == Role::Leader && self.quorum_ok(now)
    }

    pub fn ready(&self) -> Ready {
        let now = self.clock.now_ms();
        let quorum_ok = self.quorum_ok(now);
        let writable = self.log_writable();
        let mut reason = None;
        if !self.fsync_ok {
            reason = Some("fsync_failed".to_string());
        } else if !writable {
            reason = Some("log_unwritable".to_string());
        } else if !quorum_ok {
            reason = Some("quorum_unavailable".to_string());
        }
        if self.skew_exceeded() && reason.is_none() {
            reason = Some("skew_exceeded".to_string());
        }
        Ready {
            role: self.role.as_str(),
            leader: self.leader_id.clone(),
            term: self.hard.current_term,
            commit_index: self.commit_index,
            applied_index: self.sm.applied_index(),
            quorum_ok,
            log_writable: writable,
            fsync_ok: self.fsync_ok,
            degraded_reason: reason,
        }
    }

    fn log_writable(&self) -> bool {
        let probe = self.dir.join(".write-probe");
        std::fs::write(&probe, b"1").is_ok()
    }

    /// 记录一次对 peer 的时钟偏移观测(**无 RTT 修正**,单向口径)。
    ///
    /// 估算口径:`offset ≈ sender_ms - 本地接收时刻`(含单向延迟)。延迟恒 ≥ 0,所以这只是
    /// 偏移的**上界**;同城部署"通常 ≪ max_skew",跨区则单程 80~150ms 会变成常态偏差 ——
    /// 因此能拿到 RTT 的路径一律走 `observe_clock_rtt`(设计 §20 跨区发现)。
    fn observe_clock(&mut self, peer: &str, sender_ms: u64, local_ms: u64) {
        if sender_ms == 0 {
            return; // 老版本/未带时间戳:不参与测量
        }
        let raw = sender_ms as i64 - local_ms as i64;
        self.push_sample(peer, raw, raw, 0, local_ms);
    }

    /// 记录一次**带 RTT 修正**的观测(发起方视角)。
    ///
    /// 口径:本机发出请求时打 `T1 = echo_ms`,对端在 `T3 = sender_ms` 回包,本机在 `T4 = local_ms` 收到。
    /// 原始单向观测 `raw = T3 − T4` 含有单程延迟(d2);`RTT = T4 − T1 ≈ d1 + d2`,
    /// 故 `offset ≈ raw + RTT/2`(对称链路下即真实偏移)。
    ///
    /// 修正量按 `CLOCK_RTT_CORRECTION_CAP_MS` 封顶:只允许"把延迟减掉",不允许因一次被卡住的
    /// 投递把真实偏移抹平(设计 §20 跨区发现)。
    fn observe_clock_rtt(&mut self, peer: &str, sender_ms: u64, local_ms: u64, echo_ms: u64) {
        if sender_ms == 0 || echo_ms == 0 {
            // 对端未回显请求时间戳(老版本):退化为单向观测(上界)
            self.observe_clock(peer, sender_ms, local_ms);
            return;
        }
        let raw = sender_ms as i64 - local_ms as i64;
        let rtt = local_ms.saturating_sub(echo_ms);
        let corr = (rtt / 2).min(CLOCK_RTT_CORRECTION_CAP_MS) as i64;
        self.push_sample(peer, raw.saturating_add(corr), raw, rtt, local_ms);
    }

    /// 采信**对端报来**的偏移估计(该值已在发起方用 RTT 修正过)。
    ///
    /// 为什么需要:只有"发起方"才知道 T1/T4,因此 follower 无法自测;由 leader 在每个心跳里
    /// 带上"我对你的偏移估计",follower 即可得到与本人量测等价的值(A1 是双向对称的)。
    fn observe_peer_skew(&mut self, peer: &str, skew_ms: u64, local_ms: u64) {
        let v = skew_ms.min(i64::MAX as u64) as i64;
        self.push_sample(peer, v, v, 0, local_ms);
    }

    fn push_sample(&mut self, peer: &str, offset_ms: i64, raw_ms: i64, rtt_ms: u64, at_ms: u64) {
        let w = self
            .peer_offset_samples
            .entry(peer.to_string())
            .or_default();
        if w.len() >= CLOCK_SAMPLE_WINDOW {
            w.remove(0);
        }
        w.push(ClockSample {
            offset_ms,
            raw_ms,
            rtt_ms,
            at_ms,
        });
    }

    /// 某 peer 的**过滤后**偏移估计。
    ///
    /// 两条口径(按样本来源分流):
    /// - 有本机 RTT 修正的样本(`rtt_ms > 0`):取 **RTT 最小**的那个样本的 `offset_ms`。
    ///   这是 NTP clock-filter 的本意(延迟越小,估计越准)。**不能**在这里取 `|offset|` 最小:
    ///   修正后的样本会因过修正而**偏小**,取最小等于系统性低估偏移(不安全方向)。
    /// - 纯单向样本(老版本对端 / 远端回带):延迟只会让观测值变大,故取 `|offset|` 最小者。
    ///
    /// 这是设计 §20 跨区发现的原口径升级:同城 `≪ max_skew` 时单向上界够用,跨区必须去掉单程延迟。
    fn peer_offset_filtered(&self, peer: &str, now: u64) -> Option<i64> {
        let fresh: Vec<&ClockSample> = self
            .peer_offset_samples
            .get(peer)?
            .iter()
            .filter(|s| now.saturating_sub(s.at_ms) <= CLOCK_SAMPLE_TTL_MS)
            .collect();
        if fresh.is_empty() {
            return None;
        }
        if let Some(best) = fresh.iter().filter(|s| s.rtt_ms > 0).min_by_key(|s| s.rtt_ms) {
            return Some(best.offset_ms);
        }
        fresh
            .iter()
            .min_by_key(|s| s.offset_ms.unsigned_abs())
            .map(|s| s.offset_ms)
    }

    /// 某 peer 的**最新**采样(含单程延迟;只用于诊断展示,不参与判定)
    fn peer_offset_latest(&self, peer: &str) -> Option<ClockSample> {
        self.peer_offset_samples.get(peer)?.last().copied()
    }

    /// 某 peer 的修正后偏移估计(供 leader 在心跳里回带;见 `observe_peer_skew`)
    pub fn peer_skew_estimate_ms(&self, peer: &str) -> Option<u64> {
        let now = self.clock.now_ms();
        self.peer_offset_filtered(peer, now).map(|v| v.unsigned_abs())
    }

    /// 某 peer 在有效窗口内的 `(超界样本数, 有效样本数)`
    fn peer_offset_counts(&self, peer: &str, limit: u64, now: u64) -> (usize, usize) {
        let Some(w) = self.peer_offset_samples.get(peer) else {
            return (0, 0);
        };
        let mut over = 0usize;
        let mut fresh = 0usize;
        for s in w {
            if now.saturating_sub(s.at_ms) <= CLOCK_SAMPLE_TTL_MS {
                fresh += 1;
                if s.offset_ms.unsigned_abs() > limit {
                    over += 1;
                }
            }
        }
        (over, fresh)
    }

    /// 实测时钟偏移(ms,**已按最小延迟过滤**)。
    /// None = 没有**有效期内**的观测(未测量/观测过期,**不等于**偏移为 0)。
    pub fn skew_measured_ms(&self) -> Option<u64> {
        self.skew_diag().0
    }

    /// 诊断视图:`(过滤后上界, 最新采样上界, 有效样本数)`
    ///
    /// 两个值的差就是"被过滤掉的单程延迟"量级 —— 运维据此区分
    /// 「真 NTP 不同步」与「消息延迟尖峰」,不再被一句"请修 NTP"带偏。
    pub fn skew_diag(&self) -> (Option<u64>, Option<u64>, usize) {
        let now = self.clock.now_ms();
        let mut filt: Option<u64> = None;
        let mut latest: Option<u64> = None;
        let mut n = 0usize;
        for peer in self.peer_offset_samples.keys() {
            if let Some(v) = self.peer_offset_filtered(peer, now) {
                let a = v.unsigned_abs();
                filt = Some(filt.map_or(a, |b| b.max(a)));
            }
            if let Some(s) = self.peer_offset_latest(peer) {
                if now.saturating_sub(s.at_ms) <= CLOCK_SAMPLE_TTL_MS {
                    // 用**未修正**的原始观测做诊断值:它的口径与旧版一致(含单程延迟),
                    // 与 `filt` 的差就是"被 RTT 修正剔掉的单程延迟量级"。
                    let a = s.raw_ms.unsigned_abs();
                    latest = Some(latest.map_or(a, |b| b.max(a)));
                }
            }
            let (_, fresh) = self.peer_offset_counts(peer, self.cfg.max_skew_ms, now);
            n += fresh;
        }
        (filt, latest, n)
    }

    /// 测试钩子:注入对某 peer 的偏移观测(用于验证"持续超界才拒绝授予")。
    /// 注入 `CLOCK_VIOLATION_MIN_SAMPLES` 次,表示"这是一个持续存在的偏移"而非单次抖动。
    #[cfg(test)]
    pub fn inject_peer_offset(&mut self, peer: &str, offset_ms: i64) {
        let now = self.clock.now_ms();
        let sender = if offset_ms >= 0 {
            now.saturating_add(offset_ms as u64)
        } else {
            now.saturating_sub(offset_ms.unsigned_abs())
        };
        for _ in 0..CLOCK_VIOLATION_MIN_SAMPLES {
            self.observe_clock(peer, sender, now);
        }
    }

    /// 前提 A1 是否被测量证实(有观测且未超界)
    pub fn clock_verified(&self) -> bool {
        match self.skew_measured_ms() {
            Some(s) => s <= self.cfg.max_skew_ms,
            None => false,
        }
    }

    /// 是否已超出允许偏移(必须拒绝授予新租约,设计 §5.4/A1)
    pub fn skew_exceeded(&self) -> bool {
        let now = self.clock.now_ms();
        for peer in self.peer_offset_samples.keys() {
            // ① 判据必须与 `skew_measured_ms()` **同一个值**(过滤后的估计),否则会出现
            //    "上界只有 20ms 却判定超界"这种自相矛盾(第一版就是这么写的,被 s3/f7 用例抓住)。
            // ② 至少要有 2 个有效样本:单独一个样本本身也可能被延迟污染,
            //    不足以下"持续偏移"的结论(反例:n3 曾被一条 1578ms 的选举消息锁死)。
            let (_, fresh) = self.peer_offset_counts(peer, self.cfg.max_skew_ms, now);
            if fresh < CLOCK_VIOLATION_MIN_SAMPLES {
                continue;
            }
            if let Some(v) = self.peer_offset_filtered(peer, now) {
                if v.unsigned_abs() > self.cfg.max_skew_ms {
                    return true;
                }
            }
        }
        false // 未测量/样本不足 ≠ 超界;前者由"未验证"表述,不伪装成超界
    }

    /// 租约授予提案(leader 侧):at_ms 取本节点时钟并随 op 落日志(状态机不读时钟)
    pub fn propose_lease_grant(
        &mut self,
        instance: &str,
        holder: &str,
        ttl_ms: u64,
    ) -> HaResult<u64> {
        let at_ms = self.clock.now_ms();
        self.propose(Op::LeaseGrant {
            instance: instance.to_string(),
            holder: holder.to_string(),
            ttl_ms,
            at_ms,
        })
    }

    pub fn propose_lease_renew(
        &mut self,
        instance: &str,
        holder: &str,
    ) -> HaResult<u64> {
        let at_ms = self.clock.now_ms();
        self.propose(Op::LeaseRenew {
            instance: instance.to_string(),
            holder: holder.to_string(),
            at_ms,
        })
    }

    pub fn propose_lease_release(&mut self, instance: &str, holder: &str) -> HaResult<u64> {
        self.propose(Op::LeaseRelease {
            instance: instance.to_string(),
            holder: holder.to_string(),
        })
    }

    /// 优雅让位(滚动升级用,设计 §5.2/§14):
    ///   1) 先追加空 op 并投递,尽量把 commit 推到位;
    ///   2) 选中"日志最全"的 follower 发 TimeoutNow(让它立即选举,避免升级期写空窗);
    ///   3) 自己降为 follower 并重置选举计时(不再抢主)。
    ///
    /// 非 leader 调用是幂等的(返回空)。
    pub fn step_down(&mut self) -> HaResult<Vec<Outbound>> {
        if self.role != Role::Leader {
            return Ok(Vec::new());
        }
        // 1) 推进 commit(best-effort)
        let _ = self.append_local(&Op::Noop);
        let mut outs = self.broadcast_append();
        // 2) 选择最合适的继任者:日志追平优先,其次 match_index 最大
        let mut best: Option<(String, u64)> = None;
        for v in self.cfg.others() {
            let m = self.match_index.get(&v).copied().unwrap_or(0);
            if best.as_ref().map(|(_, bm)| m > *bm).unwrap_or(true) {
                best = Some((v, m));
            }
        }
        let target = best.map(|(id, _)| id);
        let term = self.hard.current_term;
        if let Some(t) = &target {
            // 只对"已追平"的节点发起转移;否则先复制(由 tick 继续推进)
            let caught_up = self.match_index.get(t).copied().unwrap_or(0) >= self.log.last_index();
            if caught_up {
                outs.push(Outbound {
                    to: t.clone(),
                    msg: Message::TimeoutNow {
                        term,
                        leader: self.cfg.node_id.clone(),
                    },
                    critical: false,
                });
            } else {
                tracing::warn!(
                    "节点 {} 让位:候选 {} 尚未追平(match={:?}, last={}),先复制不强制转移",
                    self.cfg.node_id,
                    t,
                    self.match_index.get(t),
                    self.log.last_index()
                );
            }
        }
        tracing::info!("节点 {} 让位为 follower(term={term})", self.cfg.node_id);
        self.become_follower(term, None)?;
        let now = self.clock.now_ms();
        self.reset_election_deadline(now);
        // 让位宽限期:本节点日志通常最新,若立刻参与竞选会把领导权"抢回来",
        // 使转移失败(滚动升级时表现为反复易主)。因此让位后推迟自身竞选,
        // 给继任者足够的选举窗口;若继任者未当选,宽限期过后本节点仍可参选(不会永久停摆)。
        let grace = self.cfg.election_timeout_ms.1.saturating_mul(2);
        self.election_deadline_ms = now + grace;
        tracing::info!("节点 {} 让位宽限期 {grace}ms", self.cfg.node_id);
        Ok(outs)
    }

    /// 关闭(用于测试/停机):确保日志落盘
    pub fn shutdown(&mut self) -> HaResult<()> {
        self.log.sync()
    }
}

fn seed_rng(node_id: &str, shard: u16) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for b in node_id.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100_0000_01b3);
    }
    h ^ (shard as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

fn read_hard_state(dir: &Path) -> HaResult<HardState> {
    let path = dir.join("hard_state.json");
    if !path.exists() {
        return Ok(HardState::default());
    }
    let raw = std::fs::read(&path)?;
    serde_json::from_slice(&raw)
        .map_err(|e| HaError::Corrupt(format!("hard_state 解析失败:{e}")))
}

/// fence 便捷:从状态机租约还原(执行面发给 agent 的令牌)
pub fn lease_fence(sm: &StateMachine, shard: u16, instance: &str) -> Option<Fence> {
    sm.lease_of(instance).map(|l| l.fence(shard))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ha::clock::{Clock, ManualClock};
    use serde_json::json;
    use std::collections::VecDeque;
    use std::sync::Arc;

    /// 确定性多节点仿真骨架(单线程事件驱动;S1–S5 的最小可用形态)
    struct Cluster {
        nodes: BTreeMap<String, Node>,
        clock: ManualClock,
        /// 每个节点各自的时钟(注入偏移用;同一时间轴时它们同步推进)
        clocks: BTreeMap<String, ManualClock>,
        /// 被切断的双向链路(有序对集合)
        cut: BTreeSet<(String, String)>,
        queue: VecDeque<(String, String, Message)>, // (from, to, msg)
        root: PathBuf,
        tick_ms: u64,
        last_delivered: Option<(String, String, Message)>,
    }

    impl Cluster {
        fn new(tag: &str, n: usize, tick_ms: u64) -> Self {
            let root = std::env::temp_dir().join(format!(
                "rdsctl-ha-raft-{tag}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            std::fs::create_dir_all(&root).unwrap();
            let clock = ManualClock::new(1_000_000);
            let voters: Vec<String> = (1..=n).map(|i| format!("n{i}")).collect();
            let mut nodes = BTreeMap::new();
            let mut clocks = BTreeMap::new();
            for v in &voters {
                let mut cfg = RaftConfig::new(v, 0, voters.clone());
                cfg.election_timeout_ms = (150, 300);
                cfg.heartbeat_ms = 40;
                // 每节点独立时钟:默认与基准同步推进,注入偏移时只动其中一个
                let nc = ManualClock::new(clock.now_ms());
                let shared: SharedClock = Arc::new(nc.clone());
                clocks.insert(v.clone(), nc);
                nodes.insert(v.clone(), Node::open(cfg, &root.join(v), shared).unwrap());
            }
            Self {
                nodes,
                clock,
                clocks,
                cut: BTreeSet::new(),
                queue: VecDeque::new(),
                root,
                tick_ms,
                last_delivered: None,
            }
        }

        fn partition(&mut self, a: &str, b: &str) {
            self.cut.insert((a.to_string(), b.to_string()));
            self.cut.insert((b.to_string(), a.to_string()));
        }
        fn heal(&mut self, a: &str, b: &str) {
            self.cut.remove(&(a.to_string(), b.to_string()));
            self.cut.remove(&(b.to_string(), a.to_string()));
        }

        /// 单向切断:from → to 的消息被丢弃(to → from 仍通)—— F3 用
        fn cut_one_way(&mut self, from: &str, to: &str) {
            self.cut.insert((from.to_string(), to.to_string()));
        }

        /// 断言:同一 term 至多一个 leader(脑裂检测)
        fn assert_at_most_one_leader_per_term(&self) {
            let mut by_term: BTreeMap<u64, Vec<String>> = BTreeMap::new();
            for (id, n) in self.nodes.iter() {
                if n.is_leader() {
                    by_term.entry(n.term()).or_default().push(id.clone());
                }
            }
            for (t, ls) in by_term {
                assert_eq!(ls.len(), 1, "INV-1 违反:term={t} 出现多个 leader {ls:?}");
            }
        }

        /// 推进一拍:先所有节点 tick,再投递队列中的消息(按 id 排序保证确定性)
        fn step(&mut self) {
            let ids: Vec<String> = self.nodes.keys().cloned().collect();
            for id in &ids {
                let out = self.nodes.get_mut(id).unwrap().tick();
                self.enqueue(id, out);
            }
            // 投递(每次 step 投递一轮,模拟单跳延迟)
            let batch: Vec<(String, String, Message)> = self.queue.drain(..).collect();
            let mut pending = VecDeque::new();
            for (from, to, msg) in batch {
                if self.cut.contains(&(from.clone(), to.clone())) {
                    continue; // 链路断开:丢弃
                }
                self.record_delivery(&from, &to, &msg);
                if let Some(node) = self.nodes.get_mut(&to) {
                    let out = node.handle(&from, msg);
                    for o in out {
                        if o.to != from {
                            pending.push_back((to.clone(), o.to.clone(), o.msg));
                        } else {
                            pending.push_back((to.clone(), from.clone(), o.msg));
                        }
                    }
                }
            }
            for item in pending {
                self.queue.push_back(item);
            }
            self.clock.advance_ms(self.tick_ms);
            for c in self.clocks.values() {
                c.advance_ms(self.tick_ms);
            }
        }

        /// 只推进某个节点的时钟(注入时钟偏移)
        fn skew(&self, id: &str, ms: u64) {
            if let Some(c) = self.clocks.get(id) {
                c.advance_ms(ms);
            }
        }

        fn clock_of(&self, id: &str) -> ManualClock {
            self.clocks.get(id).cloned().expect("节点时钟不存在")
        }

        fn enqueue(&mut self, from: &str, out: Vec<Outbound>) {
            for o in out {
                self.queue.push_back((from.to_string(), o.to, o.msg));
            }
        }

        /// 记录最近一次投递的消息(供 S5 重复投递/乱序注入)
        fn record_delivery(&mut self, from: &str, to: &str, msg: &Message) {
            self.last_delivered = Some((from.to_string(), to.to_string(), msg.clone()));
        }

        /// 重复投递最近一条消息 n 次(S5:重复投递必须幂等)
        fn redeliver_last(&mut self, n: usize) {
            if let Some((from, to, msg)) = self.last_delivered.clone() {
                for _ in 0..n {
                    self.queue.push_back((from.clone(), to.clone(), msg.clone()));
                }
            }
        }

        /// 投递一条"陈旧的"AppendEntries(用当前 leader 的日志前缀构造)→ 乱序/回退注入
        fn deliver_stale_append(&mut self, from: &str, to: &str) -> bool {
            let Some(leader) = self.nodes.get(from) else {
                return false;
            };
            if !leader.is_leader() || leader.log.last_index() == 0 {
                return false;
            }
            let prev = leader.log.last_index() - 1;
            let msg = Message::AppendEntries {
                term: leader.term(),
                leader: from.to_string(),
                prev_log_index: prev,
                prev_log_term: if prev == 0 { 0 } else { leader.log.term_at(prev).unwrap_or(0) },
                entries: Vec::new(),
                leader_commit: 0, // 陈旧的 commit(比 follower 当前 commit 小)
                sender_ms: leader.clock.now_ms(),
                peer_skew_ms: 0,
            };
            self.queue.push_back((from.to_string(), to.to_string(), msg));
            true
        }

        fn run_ticks(&mut self, n: usize) {
            for _ in 0..n {
                self.step();
            }
        }

        fn ids(&self) -> Vec<String> {
            self.nodes.keys().cloned().collect()
        }

        fn leaders(&self) -> Vec<String> {
            self.nodes
                .iter()
                .filter(|(_, n)| n.is_leader())
                .map(|(k, _)| k.clone())
                .collect()
        }

        fn run_until_leader(&mut self, max_ticks: usize) -> String {
            for _ in 0..max_ticks {
                self.step();
                let l = self.leaders();
                if l.len() == 1 {
                    return l[0].clone();
                }
            }
            panic!("未能在 {max_ticks} 拍内选出唯一 leader;当前 leaders={:?}", self.leaders());
        }

        fn node(&mut self, id: &str) -> &mut Node {
            self.nodes.get_mut(id).unwrap()
        }

        /// 提案并驱动到提交;返回提交后的 index
        fn propose_and_commit(&mut self, leader: &str, op: Op, max_ticks: usize) -> u64 {
            let idx = self.node(leader).propose(op).expect("leader 应可提案");
            let out = self.node(leader).flush();
            self.enqueue(leader, out);
            for _ in 0..max_ticks {
                self.step();
                if self.node(leader).commit_index() >= idx {
                    return idx;
                }
            }
            panic!("提案 index={idx} 未在 {max_ticks} 拍内提交");
        }
    }

    impl Drop for Cluster {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    fn put(key: &str, v: serde_json::Value) -> Op {
        Op::Put {
            key: key.to_string(),
            value: v,
            expect_ver: None,
        }
    }

    #[test]
    fn elects_single_leader_and_replicates_state() {
        let mut c = Cluster::new("elect", 3, 20);
        let leader = c.run_until_leader(200);
        assert_eq!(c.leaders(), vec![leader.clone()]);
        // 提案 → 多数派提交 → 各副本状态一致
        c.propose_and_commit(&leader, put("i/i1", json!({"status":"running"})), 50);
        c.run_ticks(30);
        let a = c.node(&leader).sm.to_json();
        for id in ["n1", "n2", "n3"] {
            assert_eq!(c.node(id).sm.to_json(), a, "节点 {id} 状态应与 leader 一致");
        }
        // 所有节点都认这个 leader
        for id in ["n1", "n2", "n3"] {
            let n = c.node(id);
            assert_eq!(n.leader_id(), Some(leader.as_str()), "节点 {id} 的 leader 视图");
        }
    }

    #[test]
    fn appends_are_committed_only_with_majority() {
        let mut c = Cluster::new("minority", 3, 20);
        let leader = c.run_until_leader(200);
        // 孤立原 leader:切断它与另外两台的链路
        let others: Vec<String> = c.nodes.keys().filter(|k| **k != leader).cloned().collect();
        for o in &others {
            c.partition(&leader, o);
        }
        let committed_before = c.node(&leader).commit_index();
        // 孤立期间提案:绝不能被提交
        let idx = c.node(&leader).propose(put("i/solo", json!(1))).unwrap();
        let out = c.node(&leader).flush();
        c.enqueue(&leader, out);
        c.run_ticks(60);
        assert!(
            c.node(&leader).commit_index() <= committed_before,
            "少数派不得提交任何条目(commit_index 不得前移)"
        );
        assert!(idx > committed_before);
        // 多数派侧应选出新 leader 并继续提交
        let mut new_leader = None;
        for _ in 0..400 {
            c.step();
            if let Some(l) = c
                .nodes
                .iter()
                .find(|(k, n)| n.is_leader() && **k != leader)
                .map(|(k, _)| k.clone())
            {
                new_leader = Some(l);
                break;
            }
        }
        let nl = new_leader.expect("多数派侧应选出新 leader");
        assert_ne!(nl, leader);
        assert!(c.node(&nl).term() > c.node(&leader).term());
    }

    #[test]
    fn restart_preserves_term_and_vote_no_double_vote() {
        let mut c = Cluster::new("restart", 3, 20);
        let vote = Message::RequestVote {
            term: 1,
            candidate: "n1".into(),
            last_log_index: 0,
            last_log_term: 0,
            pre_vote: false,
            sender_ms: 0,
        };
        let out = c.node("n3").handle("n1", vote);
        assert!(matches!(
            out[0].msg,
            Message::RequestVoteResp { granted: true, .. }
        ));
        // 重启 n3(同一目录)
        let dir = c.root.join("n3");
        let clock = Arc::new(c.clock_of("n3")) as SharedClock;
        c.node("n3").shutdown().unwrap();
        let mut cfg = RaftConfig::new("n3", 0, vec!["n1".into(), "n2".into(), "n3".into()]);
        cfg.election_timeout_ms = (150, 300);
        cfg.heartbeat_ms = 40;
        let restarted = Node::open(cfg, &dir, clock).unwrap();
        c.nodes.insert("n3".into(), restarted);
        assert_eq!(c.node("n3").term(), 1, "term 必须持久化");
        // 同 term 内另一个候选要票:必须拒绝(否则同 term 可能选出两个 leader)
        let vote2 = Message::RequestVote {
            term: 1,
            candidate: "n2".into(),
            last_log_index: 0,
            last_log_term: 0,
            pre_vote: false,
            sender_ms: 0,
        };
        let out = c.node("n3").handle("n2", vote2);
        match &out[0].msg {
            Message::RequestVoteResp { granted, .. } => {
                assert!(!granted, "重启后不得在同一 term 重复投票")
            }
            other => panic!("意外消息 {other:?}"),
        }
    }

    #[test]
    fn leader_overwrites_divergent_follower_suffix() {
        let mut c = Cluster::new("conflict", 3, 20);
        let leader = c.run_until_leader(200);
        // 人为给一个 follower 写入"旧 leader 的未提交条目"(term 号高于当前)
        let victim = c
            .nodes
            .keys()
            .find(|k| **k != leader)
            .cloned()
            .unwrap();
        let bogus_term = c.node(&leader).term() + 5;
        {
            let n = c.node(&victim);
            let start = n.log.last_index() + 1;
            let batch: Vec<PendingEntry> = (0..3)
                .map(|i| PendingEntry {
                    kind: KIND_ENTRY,
                    term: bogus_term,
                    index: start + i,
                    payload: br#"{"op":"put","key":"bogus","value":1,"expect_ver":null}"#.to_vec(),
                })
                .collect();
            n.log.append_batch(&batch).unwrap();
            assert_eq!(n.log.last_term(), bogus_term);
        }
        // 重新连上并让 leader 复制:分歧后缀必须被截断并覆盖
        c.propose_and_commit(&leader, put("i/i2", json!({"status":"running"})), 80);
        c.run_ticks(60);
        let lview = c.node(&leader).sm.to_json();
        let fview = c.node(&victim).sm.to_json();
        assert_eq!(fview, lview, "分歧后缀必须被 leader 的日志覆盖");
        assert!(c.node(&victim).log.term_at(1).unwrap() <= c.node(&leader).term());
        let n = c.node(&victim);
        assert!(
            n.log.entries_from(1).iter().all(|r| r.term <= n.term()),
            "follower 不得保留高于当前 term 的条目"
        );
    }

    #[test]
    fn snapshot_compaction_survives_restart() {
        let mut c = Cluster::new("snap", 3, 20);
        let leader = c.run_until_leader(200);
        c.node(&leader).cfg.snapshot_entry_threshold = 5;
        for i in 1..=8 {
            c.propose_and_commit(&leader, put(&format!("i/i{i}"), json!({"n": i})), 60);
        }
        c.run_ticks(40);
        // leader 触发快照 + 压实
        let did = c.node(&leader).maybe_snapshot().unwrap();
        assert!(did, "超过阈值应触发快照");
        assert!(c.node(&leader).log.len() < 8, "日志应被压实");
        let state_before = c.node(&leader).sm.to_json();
        let snap_idx = c.node(&leader).snapshot_index();
        assert!(snap_idx > 0);
        // 重启 leader:从快照恢复
        let dir = c.root.join(&leader);
        let clock = Arc::new(c.clock_of(&leader)) as SharedClock;
        c.node(&leader).shutdown().unwrap();
        let mut cfg = RaftConfig::new(&leader, 0, vec!["n1".into(), "n2".into(), "n3".into()]);
        cfg.election_timeout_ms = (150, 300);
        cfg.heartbeat_ms = 40;
        let restarted = Node::open(cfg, &dir, clock).unwrap();
        assert_eq!(restarted.sm.to_json(), state_before, "快照应完整恢复状态机");
        assert_eq!(restarted.snapshot_index(), snap_idx);
    }

    #[test]
    fn lease_takeover_across_leader_change_respects_skew_margin() {
        let mut c = Cluster::new("lease", 3, 10);
        let leader = c.run_until_leader(200);
        // leader 授予 h1 租约(ttl 30s,满足 ttl ≥ 10×skew)
        c.node(&leader).cfg.max_skew_ms = 1000;
        let idx = c
            .node(&leader)
            .propose_lease_grant("i1", "h1", 30_000)
            .unwrap();
        let out = c.node(&leader).flush();
        c.enqueue(&leader, out);
        for _ in 0..60 {
            c.step();
            if c.node(&leader).commit_index() >= idx {
                break;
            }
        }
        let l = c.node(&leader).sm.lease_of("i1").cloned().unwrap();
        assert_eq!(l.holder, "h1");
        let granted_at = c.clock.now_ms();
        let fence1 = l.fence(0);
        c.run_ticks(20);
        // 立即抢占:必须被拒(旧租约未过期)
        let idx2 = c
            .node(&leader)
            .propose_lease_grant("i1", "h2", 30_000)
            .unwrap();
        let out = c.node(&leader).flush();
        c.enqueue(&leader, out);
        for _ in 0..60 {
            c.step();
            if c.node(&leader).applied_index() >= idx2 {
                break;
            }
        }
        match c.node(&leader).result_of(idx2).unwrap() {
            Applied::Rejected(r) => assert!(
                matches!(r, crate::ha::state::Reject::LeaseConflictNotYetSafe { .. }),
                "同一人不得在被占期间抢占: {r}"
            ),
            other => panic!("应被拒,实际 {other:?}"),
        }
        assert_eq!(c.node(&leader).sm.lease_of("i1").unwrap().holder, "h1");
        assert!(granted_at > 0 && fence1.index == idx);
    }

    /// S1 安全扫描:随机分区/愈合 + 随机提案,逐步断言
    ///   INV-1 同一 term 至多一个 leader(脑裂检测);
    ///   INV-2 愈合后各副本状态机收敛一致(已提交条目未丢/未分叉)。
    #[test]
    fn s1_safety_sweep_random_partitions_never_two_leaders_in_one_term() {
        let mut c = Cluster::new("sweep", 3, 20);
        let ids = vec!["n1".to_string(), "n2".to_string(), "n3".to_string()];
        let mut rng: u64 = 0x5EED_1234_ABCD_0001;
        let mut next = move || {
            rng = rng.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = rng;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        };
        let mut proposals = 0usize;
        for round in 0..30u32 {
            let a = &ids[(next() % 3) as usize];
            let b = &ids[(next() % 3) as usize];
            if a != b {
                if next() % 2 == 0 {
                    c.partition(a, b);
                } else {
                    c.heal(a, b);
                }
            }
            if let Some(l) = c.leaders().first().cloned() {
                if next() % 3 == 0 {
                    let _ = c.node(&l).propose(put(&format!("k{round}"), json!(round)));
                    let out = c.node(&l).flush();
                    c.enqueue(&l, out);
                    proposals += 1;
                }
            }
            for _ in 0..25 {
                c.step();
                // INV-1:同一 term 不得出现两个 leader
                let mut by_term: BTreeMap<u64, Vec<String>> = BTreeMap::new();
                for (id, n) in c.nodes.iter() {
                    if n.is_leader() {
                        by_term.entry(n.term()).or_default().push(id.clone());
                    }
                }
                for (t, ls) in by_term {
                    assert_eq!(ls.len(), 1, "INV-1 违反:term={t} 有多个 leader {ls:?}");
                }
            }
        }
        assert!(proposals > 0, "扫描中应至少发生一次提案");
        // 全部愈合 → 收敛
        for a in &ids {
            for b in &ids {
                c.heal(a, b);
            }
        }
        c.run_ticks(400);
        let views: Vec<String> = ids.iter().map(|i| c.node(i).sm.to_json()).collect();
        assert!(
            views.iter().all(|v| v == &views[0]),
            "INV-2 违反:愈合后各副本状态机未收敛"
        );
    }

    /// F3(非对称分区):A→B 断、B→A 通,方向再翻转,最后愈合。
    /// 这是单向丢包最容易打乱共识的场景:断言全程无脑裂,愈合后收敛。
    #[test]
    fn f3_asymmetric_partition_never_yields_two_leaders() {
        let mut c = Cluster::new("f3", 3, 20);
        let leader = c.run_until_leader(200);
        let others: Vec<String> = c.ids().into_iter().filter(|i| i != &leader).collect();
        let (a, b) = (leader.clone(), others[0].clone());

        c.cut_one_way(&a, &b);
        for _ in 0..60 {
            c.step();
            c.assert_at_most_one_leader_per_term();
        }
        // 方向翻转(丢包方向变化)
        c.cut_one_way(&b, &a);
        for _ in 0..60 {
            c.step();
            c.assert_at_most_one_leader_per_term();
        }
        // 愈合 → 收敛一致,且仍有唯一 leader
        c.heal(&a, &b);
        for _ in 0..400 {
            c.step();
            c.assert_at_most_one_leader_per_term();
        }
        let ids = c.ids();
        let views: Vec<String> = ids.iter().map(|i| c.node(i).sm.to_json()).collect();
        assert!(
            views.iter().all(|v| v == &views[0]),
            "非对称分区愈合后各副本状态机必须收敛"
        );
        assert_eq!(c.leaders().len(), 1, "愈合后应回到唯一 leader");
    }

    /// S5:重复投递与乱序(陈旧 leader_commit)不得改变状态、不得让 index 回退。
    #[test]
    fn s5_duplicate_and_stale_delivery_is_idempotent() {
        let mut c = Cluster::new("s5", 3, 20);
        let leader = c.run_until_leader(200);
        for i in 1..=3 {
            c.propose_and_commit(&leader, put(&format!("k{i}"), json!(i)), 60);
        }
        c.run_ticks(40);
        let before: BTreeMap<String, (u64, u64, usize)> = c
            .ids()
            .into_iter()
            .map(|id| {
                let n = c.node(&id);
                (
                    id,
                    (n.commit_index(), n.applied_index(), n.log.len()),
                )
            })
            .collect();
        let sm_before: Vec<String> = c.ids().iter().map(|i| c.node(i).sm.to_json()).collect();

        // 重复投递最近一条消息 20 次 + 投递陈旧 AppendEntries 10 次
        c.redeliver_last(20);
        let follower = c.ids().into_iter().find(|i| i != &leader).unwrap();
        for _ in 0..10 {
            let ok = c.deliver_stale_append(&leader, &follower);
            if !ok {
                break;
            }
        }
        c.run_ticks(20);

        // index 单调不回退、状态机不变、日志不重复增长
        for id in c.ids() {
            let n = c.node(&id);
            let (c0, a0, l0) = before[&id];
            assert!(n.commit_index() >= c0, "节点 {id} commit 回退");
            assert!(n.applied_index() >= a0, "节点 {id} applied 回退");
            assert_eq!(n.log.len(), l0, "节点 {id} 日志长度因重复投递而变化(应幂等跳过)");
        }
        let sm_after: Vec<String> = c.ids().iter().map(|i| c.node(i).sm.to_json()).collect();
        assert_eq!(sm_before, sm_after, "重复/乱序投递不得改变状态机");
        c.assert_at_most_one_leader_per_term();
    }

    /// F10(内核层):leader 让位后由已追平的 follower 立即当选,全程无脑裂。
    #[test]
    fn step_down_transfers_leadership_without_two_leaders() {
        let mut c = Cluster::new("stepdown", 3, 20);
        let leader = c.run_until_leader(200);
        c.propose_and_commit(&leader, put("k-before", json!(1)), 60);
        c.run_ticks(40); // 让 follower 追平

        let outs = c.node(&leader).step_down().unwrap();
        c.enqueue(&leader, outs);
        assert!(!c.node(&leader).is_leader(), "让位后原 leader 必须已是 follower");

        let mut new_leader = None;
        for _ in 0..300 {
            c.step();
            c.assert_at_most_one_leader_per_term();
            let ls = c.leaders();
            if ls.len() == 1 && ls[0] != leader {
                new_leader = Some(ls[0].clone());
                break;
            }
        }
        let nl = new_leader.expect("让位后应迅速选出新 leader(由 TimeoutNow 触发)");
        // 新 leader 保有让位前已提交的状态
        assert_eq!(
            c.node(&nl).sm.kv_get("k-before").map(|e| e.value.clone()),
            Some(json!(1)),
            "新 leader 必须保有已提交状态"
        );
        // 原 leader 仍可正常服务读(且不再是 leader)
        assert_eq!(c.leaders(), vec![nl]);
    }

    /// S3/F7:注入超过上限的时钟偏移。
    ///   - 偏移在界内时 A1 视为已验证;
    ///   - 偏移超界时被**实测**发现(`skew_measured_ms` > max_skew),`ready()` 报 `skew_exceeded`;
    ///   - 期间不得出现脑裂(同 term 两个 leader);
    ///   - 偏移恢复后重新变为已验证(可自愈)。
    /// 发现 22 回归:把**单程延迟**误判成时钟偏移,并且一旦那个 peer 不再发消息就被永久锁死。
    ///
    /// 实测事故:三副本同机(真实偏移 2–4ms),n3 因一条被 CPU 抢占拖慢的选举消息把
    /// **1578ms** 记成"时钟偏移",此后该节点**一直拒绝授予实例租约**,报错还写着"请修 NTP"。
    #[test]
    fn delayed_message_is_not_mistaken_for_clock_skew() {
        let clock = ManualClock::new(1_000_000);
        let shared: SharedClock = Arc::new(clock.clone());
        let mut cfg = RaftConfig::new("n1", 0, vec!["n1".into(), "n2".into(), "n3".into()]);
        cfg.max_skew_ms = 1000;
        let dir = std::env::temp_dir().join(format!("rdsctl-skew-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let mut n = Node::open(cfg, &dir, shared).expect("open");

        let now = clock.now_ms();
        // ① 正常样本(真实偏移 3ms):A1 视为已验证
        n.observe_clock("n2", now + 3, now);
        assert_eq!(n.skew_measured_ms(), Some(3));
        assert!(!n.skew_exceeded(), "正常样本不得判超界");
        assert!(n.clock_verified());

        // ② 一条被拖慢 1.5s 的消息(延迟被算进观测值):**不得**据此拒发租约
        n.observe_clock("n2", now + 1503, now);
        assert_eq!(
            n.skew_measured_ms(),
            Some(3),
            "最小延迟过滤应把 1503ms 的延迟尖峰滤掉,仍报 3ms"
        );
        assert!(
            !n.skew_exceeded(),
            "单次延迟尖峰不得判 A1 超界(否则会被永久锁死)"
        );
        // 但诊断能看见"最新采样很大" ⇒ 运维可区分真偏移与延迟
        let (filt, latest, samples) = n.skew_diag();
        assert_eq!(filt, Some(3));
        assert_eq!(latest, Some(1503), "最新采样如实暴露延迟量级");
        assert_eq!(samples, 2);

        // ③ 真实持续偏移(每个样本都偏 1500ms)⇒ 必须判超界
        let clock2 = ManualClock::new(2_000_000);
        let shared2: SharedClock = Arc::new(clock2.clone());
        let mut cfg2 = RaftConfig::new("n1", 0, vec!["n1".into(), "n2".into(), "n3".into()]);
        cfg2.max_skew_ms = 1000;
        let mut n2 = Node::open(cfg2, &dir, shared2).expect("open");
        let t = clock2.now_ms();
        n2.observe_clock("n2", t + 1500, t);
        assert!(!n2.skew_exceeded(), "只有一个样本时不足以判定(可能是延迟)");
        n2.observe_clock("n2", t + 1500, t);
        assert_eq!(n2.skew_measured_ms(), Some(1500));
        assert!(n2.skew_exceeded(), "持续偏移必须判超界");
        assert!(!n2.clock_verified(), "超界时 A1 不得标记为已验证");

        // ④ 采样过期 ⇒ 回到"未验证",而不是继续"超界"(旧观测不能代表当前)
        let ttl = CLOCK_SAMPLE_TTL_MS + 1;
        let clock3 = ManualClock::new(3_000_000);
        let shared3: SharedClock = Arc::new(clock3.clone());
        let mut cfg3 = RaftConfig::new("n1", 0, vec!["n1".into(), "n2".into(), "n3".into()]);
        cfg3.max_skew_ms = 1000;
        let mut n3 = Node::open(cfg3, &dir, shared3).expect("open");
        let t3 = clock3.now_ms();
        n3.observe_clock("n2", t3 + 1500, t3);
        n3.observe_clock("n2", t3 + 1500, t3);
        assert!(n3.skew_exceeded());
        clock3.advance_ms(ttl);
        assert!(!n3.skew_exceeded(), "过期观测不得继续判超界");
        assert_eq!(n3.skew_measured_ms(), None, "过期后回到未测量");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 跨区回归:单程延迟**不得**被当成时钟偏移(设计 §20 跨区发现)。
    ///
    /// 场景:真实偏移 5ms、单程 150ms(RTT 300ms,跨区量级)。旧口径把单程延迟算进观测值
    /// ⇒ 报 145ms;在 `max_skew_ms = 50`(同城口径的"安全"配置)下会**持续**判超界
    /// ⇒ 拒绝授予任何实例租约(可用性事故,且报错会误导人"去修 NTP")。
    /// 新口径用四时间戳消掉单程延迟 ⇒ 报 5ms。
    #[test]
    fn one_way_delay_is_removed_by_rtt_correction() {
        let voters = vec!["n1".to_string(), "n2".to_string(), "n3".to_string()];
        let dir = std::env::temp_dir().join(format!(
            "rdsctl-rtt-skew-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let d = 150u64; // 单程延迟(WAN 量级)
        let theta = 5i64; // 真实偏移(对端时钟比本机快 5ms)

        // ① 旧口径(无 RTT 修正):单程延迟被算成偏移 —— 跨区误判的来源
        let c1 = ManualClock::new(1_000_000);
        let mut cfg1 = RaftConfig::new("n1", 0, voters.clone());
        cfg1.max_skew_ms = 50; // 收得很紧:A1 的"同城安全值"
        let mut raw_node = Node::open(cfg1, &dir.join("a"), Arc::new(c1.clone())).expect("open");
        let t1 = c1.now_ms(); // 本机发出请求(T1)
        let t3 = (t1 as i64 + d as i64 + theta) as u64; // 对端回包时刻(对端时钟)
        let t4 = t1 + 2 * d; // 本机收到时刻(T4)
        raw_node.observe_clock("n2", t3, t4);
        raw_node.observe_clock("n2", t3, t4); // 持续样本(超界判定需要 ≥2 个)
        assert_eq!(raw_node.skew_measured_ms(), Some(145), "旧口径把单程延迟算进偏移");
        assert!(
            raw_node.skew_exceeded(),
            "max_skew=50 时旧口径会持续判超界 ⇒ 拒绝授予租约(跨区可用性事故)"
        );

        // ② 新口径(回显 T1 算 RTT):修正后还原真实偏移,不再误判
        let c2 = ManualClock::new(1_000_000);
        let mut cfg2 = RaftConfig::new("n1", 0, voters.clone());
        cfg2.max_skew_ms = 50;
        let mut fix_node = Node::open(cfg2, &dir.join("b"), Arc::new(c2.clone())).expect("open");
        let t1 = c2.now_ms();
        let t3 = (t1 as i64 + d as i64 + theta) as u64;
        let t4 = t1 + 2 * d;
        fix_node.observe_clock_rtt("n2", t3, t4, t1);
        fix_node.observe_clock_rtt("n2", t3, t4, t1);
        assert_eq!(
            fix_node.skew_measured_ms(),
            Some(theta.unsigned_abs()),
            "RTT/2 修正后应还原真实偏移(5ms)"
        );
        assert!(!fix_node.skew_exceeded(), "修正后不得再判超界");
        assert!(fix_node.clock_verified(), "修正后 A1 应视为已验证");
        // 诊断值仍是**未修正**的原始观测 ⇒ 运维能看出"被剔掉的单程延迟量级"
        let (filt, latest, _) = fix_node.skew_diag();
        assert_eq!(filt, Some(5));
        assert_eq!(latest, Some(145), "原始观测(含单程延迟)如实暴露在诊断里");

        // ③ 修正量按上限封顶:一次被卡住的投递(接近投递超时)不得把真实偏移整段抹平
        //    raw = -5000ms,rtt = 10s ⇒ 修正量封顶为 CLOCK_RTT_CORRECTION_CAP_MS(2000)
        let c3 = ManualClock::new(5_000_000);
        let mut cfg3 = RaftConfig::new("n1", 0, voters.clone());
        cfg3.max_skew_ms = 50;
        let mut cap_node = Node::open(cfg3, &dir.join("c"), Arc::new(c3.clone())).expect("open");
        let now = c3.now_ms();
        cap_node.observe_clock_rtt("n2", now.saturating_sub(5_000), now, now.saturating_sub(10_000));
        assert_eq!(
            cap_node.skew_measured_ms(),
            Some(3_000),
            "欠修正(封顶 2000)= 偏保守,真偏移仍能被看见,不会被一次卡顿抹平"
        );

        // ④ 回带链路:leader 把修正后的估计随心跳下发,follower 据此得到同样的值
        let leader_est = fix_node.peer_skew_estimate_ms("n2");
        assert_eq!(leader_est, Some(5), "leader 侧修正后估计 = 真实偏移");
        let c4 = ManualClock::new(9_000_000);
        let mut cfg4 = RaftConfig::new("n3", 0, voters.clone());
        cfg4.max_skew_ms = 50;
        let mut follower = Node::open(cfg4, &dir.join("d"), Arc::new(c4.clone())).expect("open");
        let outs = follower.handle(
            "n1",
            Message::AppendEntries {
                term: 1,
                leader: "n1".into(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
                sender_ms: 0,
                peer_skew_ms: leader_est.unwrap(),
            },
        );
        assert!(!outs.is_empty(), "follower 必须回 AppendEntriesResp");
        assert_eq!(
            follower.skew_measured_ms(),
            Some(5),
            "follower 无 RTT 可测,只能采信 leader 回带的修正值"
        );
        assert!(!follower.skew_exceeded(), "回带值正确时 follower 不得误判超界");

        // ⑤ 回带字段缺失(老版本对端)⇒ 退回单向口径,不假装"已修正"
        let c5 = ManualClock::new(11_000_000);
        let mut cfg5 = RaftConfig::new("n3", 0, voters.clone());
        cfg5.max_skew_ms = 50;
        let mut old_peer = Node::open(cfg5, &dir.join("e"), Arc::new(c5.clone())).expect("open");
        let now = c5.now_ms();
        let _ = old_peer.handle(
            "n1",
            Message::AppendEntries {
                term: 1,
                leader: "n1".into(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
                sender_ms: now.saturating_sub(145),
                peer_skew_ms: 0,
            },
        );
        assert_eq!(
            old_peer.skew_measured_ms(),
            Some(145),
            "无回带值时退回单向上界口径(诚实,不伪装成已修正)"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn s3_f7_clock_skew_is_measured_and_degrades() {
        let mut c = Cluster::new("skew", 3, 20);
        let leader = c.run_until_leader(200);
        c.propose_and_commit(&leader, put("k1", json!(1)), 60);
        c.run_ticks(30); // 建立偏移观测(心跳)

        let max_skew = c.node(&leader).cfg.max_skew_ms;
        assert!(
            c.node(&leader).clock_verified(),
            "同一时间轴下偏移应在界内(实测 {:?})",
            c.node(&leader).skew_measured_ms()
        );
        assert!(!c.node(&leader).skew_exceeded());

        // 注入 5×max_skew 偏移
        let follower = c.ids().into_iter().find(|i| i != &leader).unwrap();
        c.skew(&follower, 5 * max_skew);
        let mut detected = None;
        for _ in 0..200 {
            c.step();
            c.assert_at_most_one_leader_per_term();
            if c.node(&leader).skew_exceeded() {
                detected = c.node(&leader).skew_measured_ms();
                break;
            }
        }
        let measured = detected.unwrap_or_else(|| {
            panic!(
                "应实测到超界偏移(上限 {max_skew}ms,实测 {:?})",
                c.node(&leader).skew_measured_ms()
            )
        });
        assert!(measured > max_skew, "实测偏移 {measured}ms 应超过上限 {max_skew}ms");
        assert_eq!(
            c.node(&leader).ready().degraded_reason.as_deref(),
            Some("skew_exceeded"),
            "偏移超界必须显式降级(不能看起来健康)"
        );
        assert!(!c.node(&leader).clock_verified(), "超界时 A1 不得标记为已验证");

        // 偏移恢复(把该节点时钟拉回同步):应重新变为已验证
        let back = c.clock_of(&follower);
        back.set(c.clock.now_ms());
        let mut recovered = false;
        for _ in 0..200 {
            c.step();
            c.assert_at_most_one_leader_per_term();
            if !c.node(&leader).skew_exceeded() {
                recovered = true;
                break;
            }
        }
        assert!(recovered, "偏移恢复后应重新在界内(可自愈)");
    }

    #[test]
    fn pre_vote_does_not_raise_term_of_healthy_cluster() {
        let mut c = Cluster::new("prevote", 3, 20);
        let leader = c.run_until_leader(200);
        let term_before = c.node(&leader).term();
        // 孤立 n3 很久(它会不断尝试 pre-vote,但不应升 term)
        let l = leader.clone();
        c.partition("n3", &l);
        c.partition("n3", if l == "n1" { "n2" } else { "n1" });
        for _ in 0..80 {
            c.step();
        }
        assert_eq!(
            c.node("n3").term(),
            term_before,
            "pre-vote 阶段不得提升 term(避免打断健康集群)"
        );
        // 恢复后集群仍是同一 term 的同一 leader
        c.heal("n3", &l);
        c.heal("n3", if l == "n1" { "n2" } else { "n1" });
        c.run_ticks(40);
        assert_eq!(c.node(&leader).term(), term_before, "健康集群的 term 不应被干扰");
    }
}
