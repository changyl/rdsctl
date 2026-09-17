// rdsctl HA — 集群运行时(设计 §4 角色模型、§7 恢复、§16 M1a 骨架)
//
// 职责:
//   - 持有 `Node`(共识核心)并在 tick 循环里驱动它;
//   - 内部 RPC(`/internal/raft`):节点间投递共识消息(手写 HTTP,与 agent 同风格);
//   - 对外探针:`/healthz`(进程存活)/ `/readyz`(是否可接流量,含降级原因);
//   - `RDSCTL_MODE=cluster` 的**进程内启动自检**:前提不达标 → 退出码 2(与
//     `deploy/bin/rdsctl-preflight.sh` 的约定一致,见 deploy/README.md §6)。
//
// 边界(M1a 骨架):本模块只提供"管控面集群自身"的能力 —— 共识、租约/fence、探针、
// 内部写入口。把实例生命周期(instance.rs)改造成**经由**共识租约与步骤账本,
// 是 M1a 的后续项;在此之前 cluster 模式的业务 API 一律 503(不假装可用)。

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use serde_json::{json, Value};

use super::clock::{SharedClock, SystemClock};
use super::raft::{Message, Node, Outbound, RaftConfig, Ready};
use super::state::{Applied, Op};
use super::{HaError, HaResult};

/// 单条内部 RPC 投递超时(ms):超过即视为该次投递失败,计入 transport_errors
const DELIVER_TIMEOUT_MS: u64 = 3000;

/// 提案转发(`/internal/propose`)的预算(ms)。
///
/// 必须**大于** leader 侧的"提案 + 等 apply"预算(`propose_with_flush` 最多 5s):
/// 否则 follower 会把一次"其实已提交但提交得慢"的提案判成转发失败,跑去试下一个副本
/// (重复提案虽然幂等,但会拖长客户端等待)。沿用改造前 `post_json` 的 10s 内部预算。
const RPC_PROPOSE_TIMEOUT_MS: u64 = 10_000;

/// `InstallSnapshot` 单条投递预算(ms)。
///
/// 为什么必须单独一档:心跳/日志条目是**小消息**,3s 足够;而整份快照是**一条**消息
/// (`Message::InstallSnapshot { snapshot_json }`),3s 预算下跨区(乃至同城大状态机)
/// 永远传不完 —— 表现为"落后副本永远追不上",而且串行投递时每轮都要赔上 3s,
/// 把同一轮其它 peer 的心跳也一起推迟(跨区域部署的硬缺陷,见设计 §19)。
/// 默认 300s,可用 `RDSCTL_SNAPSHOT_DELIVER_MS` 调整。
///
/// 仍未解决(后续项):快照未分块/不支持断点续传,超大快照会长时间占用一条连接。
fn snapshot_deliver_timeout_ms() -> u64 {
    static V: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("RDSCTL_SNAPSHOT_DELIVER_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(300_000)
    })
}

/// 单条消息的投递预算:快照档 vs 普通档
fn deliver_budget_ms(kind: &str) -> u64 {
    if kind == "install_snapshot" {
        snapshot_deliver_timeout_ms()
    } else {
        DELIVER_TIMEOUT_MS
    }
}

/// 管控集群页的成员探测超时(ms)。
///
/// 必须**短**:这是给人看的观测路径,一个卡住的 peer 不得把页面拖到超时。
/// 探测是并发的,因此整体延迟 ≈ 本常数(而非 × 成员数)。超过即该成员 `reachable=false`,
/// 界面据此显示"不可达",而不是等待或猜测。
const MEMBER_PROBE_TIMEOUT_MS: u64 = 700;

/// 桥接(阻塞 API → 异步共识)的总超时(ms)。
///
/// **不变量:必须严格大于内部各段超时之和**,否则"其实成功但很慢"的路径会被报成超时,
/// 而调用方无法区分(它既可能已拿到租约、也可能没有)。内部预算是:
///   `propose_op` 最多 5s(`propose_with_flush` 的 `wait_applied_async`)
/// + 本地追平 `LEASE_LOCAL_CATCHUP_MS`
/// + 转发/调度余量。
/// 实测教训:把本地追平从 2s 调到 5s 后,最坏 5s+5s=10s 超过了旧的 8s 桥接超时 →
/// `create` 偶发 400「共识租约请求超时(桥接等待超时)」(设计 §19 发现 20)。
const LEASE_BRIDGE_TIMEOUT_MS: u64 = 12_000;

/// 租约写入已提交后,等**本副本**追平到该 index 的上限(ms)。
///
/// 为什么需要这个等待:提案可能在 follower 上发起(`propose_op` 会自动转发),而
/// `propose_op` 返回的 `applied` 是 **leader 侧**结果 —— 只证明"已提交",不证明"本副本已 apply"。
/// 而 `RdsManager` 需要从**本地**状态机取出 fence 才能开始执行(设计 §5.4)。
///
/// 为什么是 5s:判据已改为**按 index**(确定性),正常追平是毫秒级 —— 5s 不是"预期耗时",
/// 而是"**已经提交的授予绝不因为一次追平抖动被丢掉**"。实测在 3 副本同机的高负载/连跑下,
/// 偶发出现"发起方 commit_index 落后 1 条 2s 以上"(原因未完全定位,已加投递耗时 WARN 观测),
/// 此时 2s 会把一次已提交的租约操作报成失败;5s 让它成功。
/// 真掉队时错误会带上 index/applied_index/commit_index/leader,一眼可辨,不会静默。
/// **注意**:本值与 `LEASE_BRIDGE_TIMEOUT_MS` 有预算关系(见该常量的说明),
/// 调大它必须同步调大桥接超时(5s + 5s < 12s)。
const LEASE_LOCAL_CATCHUP_MS: u64 = 5_000;

/// 会话/RBAC 写入后等本副本 apply 的上限(ms)。与租约同理:正常是毫秒级,
/// 这里只是"绝不因为一次抖动就丢掉一次**已提交**的会话写入"。
const AUTH_LOCAL_APPLY_MS: u64 = 3_000;

/// 进程级集群运行时句柄。
///
/// 为什么用全局:既有的 `RdsManager` 是 `OnceLock` 单例,而 authority(共识租约)必须在
/// 管理器构造时就可用。main 在 cluster 模式下先 `set_global`,再初始化管理器。
static GLOBAL: std::sync::OnceLock<Arc<ClusterRuntime>> = std::sync::OnceLock::new();

/// 注册本进程的集群运行时(cluster 模式启动时调用;重复注册返回 false)
pub fn set_global(rt: Arc<ClusterRuntime>) -> bool {
    GLOBAL.set(rt).is_ok()
}

/// 当前进程的集群运行时(未启动 cluster 模式则为 None)
pub fn global() -> Option<Arc<ClusterRuntime>> {
    GLOBAL.get().cloned()
}

/// 是否运行在 cluster 模式
pub fn cluster_mode() -> bool {
    GLOBAL.get().is_some() || std::env::var("RDSCTL_MODE").as_deref() == Ok("cluster")
}

/// 集群成员地址表(来自 `RDSCTL_CLUSTER=id@ip:port,...`)
#[derive(Debug, Clone)]
pub struct MemberTable {
    pub addrs: BTreeMap<String, (String, u16)>,
}

impl MemberTable {
    pub fn parse(spec: &str) -> HaResult<Self> {
        let mut addrs = BTreeMap::new();
        for item in spec.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()) {
            let (id, addr) = item.split_once('@').ok_or_else(|| {
                HaError::Config(format!("集群成员格式应为 id@ip:port,收到:{item}"))
            })?;
            let (ip, port) = addr.rsplit_once(':').ok_or_else(|| {
                HaError::Config(format!("集群成员缺少端口:{item}"))
            })?;
            let port: u16 = port
                .parse()
                .map_err(|_| HaError::Config(format!("端口非法:{item}")))?;
            if addrs.insert(id.to_string(), (ip.to_string(), port)).is_some() {
                return Err(HaError::Config(format!("集群成员 id 重复:{id}")));
            }
        }
        if addrs.is_empty() {
            return Err(HaError::Config("集群成员表为空".into()));
        }
        Ok(Self { addrs })
    }

    pub fn ids(&self) -> Vec<String> {
        self.addrs.keys().cloned().collect()
    }

    pub fn get(&self, id: &str) -> Option<&(String, u16)> {
        self.addrs.get(id)
    }
}

/// 租约获取/续约失败分类(映射到 API 与审计)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseError {
    /// 实例被其它 holder 持有(含"旧租约尚未安全过期")
    Held(String),
    /// 本节点不是 leader(附带已知 leader)
    NotLeader(Option<String>),
    /// 无多数派
    Quorum,
    /// 实测时钟偏移超界(前提 A1 被违反):拒绝授予新租约
    Skew(u64),
    /// 其它内部错误
    Internal(String),
}

impl std::fmt::Display for LeaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LeaseError::Held(m) => write!(f, "实例租约被占用:{m}"),
            LeaseError::NotLeader(l) => match l {
                Some(l) => write!(f, "本节点不是 leader(leader={l})"),
                None => write!(f, "本节点不是 leader 且当前无 leader"),
            },
            LeaseError::Quorum => write!(f, "失去多数派,无法获取实例租约"),
            LeaseError::Skew(ms) => write!(
                f,
                "实测时钟偏移 {ms}ms 持续超出上限:拒绝授予新租约(前提 A1)。\
                 该值已按最小延迟样本过滤并要求持续超界,因此单一网络/调度尖峰不会触发;\
                 持续如此请修 NTP(/readyz 的 skew_measured_ms 为过滤值,skew_latest_ms 为最新采样)"
            ),
            LeaseError::Internal(m) => write!(f, "租约操作内部错误:{m}"),
        }
    }
}

impl std::error::Error for LeaseError {}

/// 启动自检结果(前提 A1–A4 的可执行检查)
#[derive(Debug, Clone)]
pub struct SelfCheck {
    pub voters_ok: bool,
    pub data_dir_ok: bool,
    pub fsync_ok: bool,
    pub clock_verified: bool,
    pub agent_fence_ok: bool,
    /// A7:实测对端 RPC 往返与选举超时**同档**(设计 §20 跨区部署)
    pub network_ok: bool,
    pub notes: Vec<String>,
    /// 是否满足全部前提(不满足时必须退出 2)
    pub all_ok: bool,
}

impl SelfCheck {
    /// **阻断性**失败清单:已用 lab 开关显式放行的前提不再计入(但仍会在 `/readyz`
    /// 的 `premises_unverified` 中如实标注)。用于启动拒绝信息与日志,避免"已放行却仍报失败"的误导。
    pub fn blocking_failures(&self, allow_clock: bool, allow_agent: bool, allow_network: bool) -> Vec<String> {
        let mut v = Vec::new();
        if !self.voters_ok {
            v.push(
                "集群配置非法(voter 必须奇数 ≥3、id 唯一且含自身、心跳与选举超时同档;\
                 具体原因见自检输出)"
                    .into(),
            );
        }
        if !self.data_dir_ok {
            v.push("数据目录不可写".into());
        }
        if !self.fsync_ok {
            v.push("fsync 探测失败(前提 A2)".into());
        }
        if !self.clock_verified && !allow_clock {
            v.push("时钟同步状态不可验证(前提 A1;lab 可设 RDSCTL_PREFLIGHT_ALLOW_UNVERIFIED_CLOCK=1 显式放行)".into());
        }
        if !self.agent_fence_ok && !allow_agent {
            v.push("执行面 agent 未就绪或未强制 fence(前提 A4;lab 可设 RDSCTL_ALLOW_NO_AGENT=1 降级)".into());
        }
        if !self.network_ok && !allow_network {
            v.push(
                "网络往返与时间参数不同档(前提 A7:需 选举超时 ≥ 4×实测 RPC 往返);\
                 lab 可设 RDSCTL_ALLOW_SLOW_NETWORK=1 放行"
                    .into(),
            );
        }
        v
    }

    pub fn failures(&self) -> Vec<String> {
        let mut v = Vec::new();
        if !self.voters_ok {
            v.push(
                "集群配置非法(voter 必须奇数 ≥3、id 唯一且含自身、心跳与选举超时同档;\
                 具体原因见自检输出)"
                    .into(),
            );
        }
        if !self.data_dir_ok {
            v.push("数据目录不可写".into());
        }
        if !self.fsync_ok {
            v.push("fsync 探测失败(前提 A2)".into());
        }
        if !self.clock_verified {
            v.push("时钟同步状态不可验证(前提 A1;lab 可设 RDSCTL_PREFLIGHT_ALLOW_UNVERIFIED_CLOCK=1 显式放行)".into());
        }
        if !self.agent_fence_ok {
            v.push("执行面 agent 未就绪或未强制 fence(前提 A4;lab 可设 RDSCTL_ALLOW_NO_AGENT=1 降级)".into());
        }
        if !self.network_ok {
            v.push(
                "网络往返与时间参数不同档(前提 A7:需 选举超时 ≥ 4×实测 RPC 往返;\
                 见 /readyz 的 network_note)"
                    .into(),
            );
        }
        v
    }
}

pub struct ClusterRuntime {
    node: Mutex<Node>,
    table: MemberTable,
    clock: SharedClock,
    token: Option<String>,
    started_ms: u64,
    /// 是否处于"未验证时钟"的 lab 放行状态(A1 未验证,readyz 必须明示)
    clock_unverified: bool,
    /// 是否处于"无 agent 强制 fence"的降级状态(不满足 G1)
    agent_best_effort: bool,
    selfcheck: SelfCheck,
    delivered: std::sync::atomic::AtomicU64,
    transport_errors: std::sync::atomic::AtomicU64,
    /// 出站投递的**按 peer** 在途集合(防止慢 peer 拖住心跳节拍;见 `spawn_background`)
    ///
    /// 为什么不是全局布尔:跨区/跨 AZ 部署下,单个慢 peer 会把全局标记一直占住,
    /// 于是**同一轮对健康 peer 的心跳也被跳过** → 健康 follower 选举超时 → 选主风暴。
    /// 按 peer 抑制只跳过"仍在途的那个 peer",其余 peer 照常收发(设计 §20 跨区发现)。
    inflight: Mutex<BTreeSet<String>>,
}

impl ClusterRuntime {
    /// 执行启动自检(不启动任何后台任务,便于先判定退出码)
    pub fn self_check(
        cfg: &RaftConfig,
        dir: &std::path::Path,
        agent_url: Option<&str>,
        peers: Option<&MemberTable>,
        token: Option<&str>,
    ) -> SelfCheck {
        let mut notes = Vec::new();
        let voters_ok = cfg.validate().is_ok();
        if let Err(e) = cfg.validate() {
            notes.push(format!("{e}"));
        }
        let data_dir_ok = std::fs::create_dir_all(dir).is_ok() && {
            let probe = dir.join(".selfcheck");
            std::fs::write(&probe, b"1").is_ok()
        };
        // fsync 探测:写 + sync_all 必须成功(前提 A2)
        let fsync_ok = {
            let probe = dir.join(".fsync-probe");
            match std::fs::File::create(&probe) {
                Ok(mut f) => {
                    use std::io::Write;
                    let ok = f.write_all(b"probe").is_ok() && f.sync_all().is_ok();
                    let _ = std::fs::remove_file(&probe);
                    ok
                }
                Err(_) => false,
            }
        };
        // A1:进程内探测 NTP 同步状态(与 deploy/bin/rdsctl-preflight.sh 同一判据)。
        // 注意:放行开关只决定"未验证时是否允许启动",**不改变**"是否已验证"的事实 ——
        // 否则 lab 放行会被误读成 A1 已满足。
        let clock_verified = detect_clock_sync().unwrap_or(false);
        if !clock_verified {
            notes.push(
                "A1 未验证:未检测到已同步的 NTP/chrony(timedatectl/chronyc/ntpq 均不可用)".into(),
            );
        }
        // A4:执行面必须在线且 fence_capable
        let agent_fence_ok = match agent_url {
            Some(u) if !u.trim().is_empty() => {
                // 同步探测(启动期允许阻塞);失败即不达标
                probe_agent_fence(u).unwrap_or(false)
            }
            _ => false,
        };
        if !agent_fence_ok {
            notes.push("A4:未配置可达的 RDSCTL_AGENT_URL(或 agent 未声明 fence_capable)".into());
        }
        // A7:实测对端 RPC 往返必须与选举超时同档(设计 §20 跨区部署)。
        let (network_ok, net_notes) = measure_peer_network(cfg, peers, token);
        notes.extend(net_notes);
        let all_ok =
            voters_ok && data_dir_ok && fsync_ok && clock_verified && agent_fence_ok && network_ok;
        SelfCheck {
            voters_ok,
            data_dir_ok,
            fsync_ok,
            clock_verified,
            agent_fence_ok,
            network_ok,
            notes,
            all_ok,
        }
    }

    /// 启动:自检 → 打开节点 → 起 tick 循环
    pub fn start(
        cfg: RaftConfig,
        table: MemberTable,
        dir: &std::path::Path,
        agent_url: Option<&str>,
        token: Option<String>,
    ) -> HaResult<Arc<Self>> {
        let check = Self::self_check(&cfg, dir, agent_url, Some(&table), token.as_deref());
        if !check.all_ok && !lab_override_allows(&check) {
            return Err(HaError::Config(format!(
                "启动自检未通过,拒绝进入 cluster 模式:\n  - {}",
                check.failures().join("\n  - ")
            )));
        }
        let lab = lab_override_allows(&check) && !check.all_ok;
        if lab {
            tracing::warn!(
                "以 lab 降级模式启动(前提未全部满足):{}",
                check.failures().join("; ")
            );
        }
        let clock: SharedClock = Arc::new(SystemClock);
        let node = Node::open(cfg, dir, clock.clone())?;
        let rt = Arc::new(Self {
            node: Mutex::new(node),
            table,
            clock,
            token,
            started_ms: now_ms(),
            clock_unverified: !check.clock_verified,
            agent_best_effort: !check.agent_fence_ok,
            selfcheck: check,
            delivered: std::sync::atomic::AtomicU64::new(0),
            transport_errors: std::sync::atomic::AtomicU64::new(0),
            inflight: Mutex::new(BTreeSet::new()),
        });
        Ok(rt)
    }

    /// 启动 tick 循环(心跳/选举/压实)与出站投递
    pub fn spawn_background(self: &Arc<Self>, tick_ms: u64) {
        let me = Arc::clone(self);
        tokio::spawn(async move {
            let interval = Duration::from_millis(tick_ms.max(5));
            loop {
                tokio::time::sleep(interval).await;
                let outs = {
                    let mut node = me.node.lock();
                    let outs = node.tick();
                    if let Err(e) = node.maybe_snapshot() {
                        tracing::error!("快照失败:{e}");
                    }
                    outs
                };
                if outs.is_empty() {
                    continue;
                }
                // **心跳节拍不得被投递拖住**(设计 §19 发现 23):
                // 单条投递上限是 `DELIVER_TIMEOUT_MS`(3s);若在这里同步等它,一个慢/不可达的
                // peer 就会把下一拍推到 3s 之后 —— 心跳停发 ⇒ follower 选举超时(600ms~1.2s)
                // 必然触发 ⇒ 换主 ⇒ 新 leader 同样卡在投递上(选举风暴);
                // 或者反向:F 收不到新 commit index,实例操作被"未追平"拒掉。
                // 因此把本轮投递交给独立任务,并按 **peer** 抑制重复投递(上一轮该 peer 还没投完
                // 就跳过本轮对它的消息 —— Raft 对延迟/丢失是容错的,下一拍会重发)。
                let send = me.claim_outbound(outs);
                if send.is_empty() {
                    tracing::debug!("本轮出站全部在途,跳过(下一拍重发)");
                    continue;
                }
                let me2 = Arc::clone(&me);
                tokio::spawn(async move {
                    me2.deliver(send).await;
                });
            }
        });
    }

    /// 认领本轮的出站消息:对周期性的 `AppendEntries` 做**按 peer** 在途抑制。
    ///
    /// 只抑制 AppendEntries:它是每 `heartbeat_ms` 重发的周期性消息,晚一拍无害;
    /// 而 `RequestVote`/`TimeoutNow` 等一次性消息若被抑制会直接推迟选举(可用性损失),
    /// 因此一律放行。
    fn claim_outbound(&self, outs: Vec<Outbound>) -> Vec<Outbound> {
        let mut keep: Vec<Outbound> = Vec::with_capacity(outs.len());
        {
            let mut inflight = self.inflight.lock();
            for o in outs {
                if matches!(o.msg, Message::AppendEntries { .. }) {
                    if inflight.contains(&o.to) {
                        continue; // 该 peer 上一轮还没投完,本轮跳过(下一拍重发)
                    }
                    inflight.insert(o.to.clone());
                }
                keep.push(o);
            }
        }
        keep
    }

    /// 某条出站消息投递结束后清理在途标记(仅 AppendEntries 会登记)
    fn release_outbound(&self, to: &str, kind: &str) {
        if kind == "append_entries" {
            self.inflight.lock().remove(to);
        }
    }

    /// 投递出站消息(批内并发:延迟 = 最慢 peer,而不是各 peer 之和)
    ///
    /// 跨区/跨 AZ 部署下"各 peer 之和"是致命的:串行时一轮心跳 = Σ(2×RTT + 建连),
    /// 三区可到 ~1s,逼近选举超时下限 → 选主风暴(设计 §20 跨区发现)。
    async fn deliver(&self, outs: Vec<Outbound>) {
        // 兜底:本轮认领过的 AppendEntries peer 必须在**批结束时**一律放出。
        // 正常路径已在单条投递完成时释放(见 `deliver_inner` + `release_outbound`),
        // 这里是"任务异常/提前返回"时不让某个 peer 被**永久**抑制的保险 ——
        // 永久抑制的后果极重:follower 再也收不到心跳 ⇒ 必然选主 ⇒ leader 反复更替。
        let claimed: BTreeSet<String> = outs
            .iter()
            .filter(|o| matches!(o.msg, Message::AppendEntries { .. }))
            .map(|o| o.to.clone())
            .collect();
        let started = std::time::Instant::now();
        let kinds: Vec<String> = outs.iter().map(|o| o.msg.kind().to_string()).collect();
        let n_msgs = outs.len();
        self.deliver_inner(outs).await;
        if !claimed.is_empty() {
            let mut inflight = self.inflight.lock();
            for p in &claimed {
                inflight.remove(p);
            }
        }
        let el = started.elapsed();
        // 观测:一次投递超过一个心跳周期就已经在伤害可用性(心跳会晚发 → follower 选主)。
        // 阈值取 500ms(= 默认选举超时量级),超过就打 WARN 并带上消息种类,便于定位是谁慢。
        // 快照安装是**预期慢**的长消息,单独一档,不参与本告警以免淹没真信号。
        let slow_only_snapshot = kinds.iter().all(|k| k == "install_snapshot");
        if el.as_millis() >= 500 && !slow_only_snapshot {
            tracing::warn!(
                "投递耗时 {}ms({} 条:{:?})—— 已超过心跳周期,可能推迟下一次心跳",
                el.as_millis(),
                n_msgs,
                kinds
            );
        }
    }

    async fn deliver_inner(&self, outs: Vec<Outbound>) {
        let mut queue = outs;
        let mut budget = 4096usize;
        let from = self.node_id();
        while !queue.is_empty() {
            // 一批并发投递:入站消息处理端返回的消息(投票/心跳响应)也走这里,
            // 因此同一轮内的多个 peer 互不阻塞(设计 §20 跨区发现)。
            let batch = std::mem::take(&mut queue);
            let mut set = tokio::task::JoinSet::new();
            for o in batch {
                if budget == 0 {
                    tracing::warn!("出站投递预算耗尽,丢弃剩余消息(疑似消息风暴)");
                    break;
                }
                budget -= 1;
                let Some((ip, port)) = self.table.get(&o.to).cloned() else {
                    tracing::warn!("未知 peer {}:无法投递 {}", o.to, o.msg.kind());
                    self.transport_errors
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    self.release_outbound(&o.to, o.msg.kind());
                    continue;
                };
                let body = json!({ "from": from, "msg": o.msg });
                let token = self.token.clone();
                let kind = o.msg.kind().to_string();
                let to = o.to.clone();
                // 单条投递限时:卡住的 peer 不得拖死心跳/请求链(否则 leader 会误判失去多数派)。
                // 快照单独一档:整份快照是**一条**消息,3s 预算下跨区永远装不完。
                let budget_ms = deliver_budget_ms(&kind);
                set.spawn(async move {
                    let sent = tokio::time::timeout(
                        Duration::from_millis(budget_ms),
                        post_json(&ip, port, "/internal/raft", &body, token.as_deref(), budget_ms),
                    )
                    .await
                    .unwrap_or_else(|_| Err("投递超时".to_string()));
                    (to, kind, sent)
                });
            }
            while let Some(joined) = set.join_next().await {
                match joined {
                    Ok((to, kind, Ok(v))) => {
                        self.release_outbound(&to, &kind);
                        self.delivered
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        if let Some(list) = v.get("out").and_then(|x| x.as_array()) {
                            for item in list {
                                if let (Some(to), Ok(msg)) = (
                                    item.get("to").and_then(|x| x.as_str()),
                                    serde_json::from_value::<Message>(
                                        item.get("msg").cloned().unwrap_or(Value::Null),
                                    ),
                                ) {
                                    queue.push(Outbound {
                                        to: to.to_string(),
                                        msg,
                                        critical: false,
                                    });
                                }
                            }
                        }
                    }
                    Ok((to, kind, Err(e))) => {
                        self.release_outbound(&to, &kind);
                        self.transport_errors
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        tracing::debug!("投递 {kind} → {to} 失败:{e}");
                    }
                    Err(e) => {
                        self.transport_errors
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        tracing::debug!("投递任务异常退出:{e}");
                    }
                }
            }
        }
    }

    /// 本节点当前是否为该分片组的 leader(供续跑仲裁等串行化职责判定)
    pub fn is_leader(&self) -> bool {
        self.node.lock().is_leader()
    }

    pub fn node_id(&self) -> String {
        self.node.lock().cfg.node_id.clone()
    }

    /// 本节点的心跳周期(ms) —— 供启动期自检/告警核对"时间参数是否同档"
    pub fn heartbeat_ms(&self) -> u64 {
        self.node.lock().cfg.heartbeat_ms
    }

    /// 处理一条入站共识消息(返回需要回发的消息)
    pub fn handle_message(&self, from: &str, msg: Message) -> Vec<Outbound> {
        self.node.lock().handle(from, msg)
    }

    /// 就绪信息(含 A1/A4 降级标记)
    pub fn ready(&self) -> Value {
        let node = self.node.lock();
        let Ready {
            role,
            leader,
            term,
            commit_index,
            applied_index,
            quorum_ok,
            log_writable,
            fsync_ok,
            degraded_reason,
        } = node.ready();
        // 就绪(能不能接流量)= 多数派可达 + 日志可写 + fsync 可信;
        // 前提是否"已验证"单独上报(premises_ok / 未验证清单)——
        // lab 放行不应让节点永久不可用,但**必须**在输出里显式标注,不能"看起来就绪"。
        //
        // 多个降级原因可能同时存在(例如既失多数派又时钟超界):全部列出,
        // `degraded_reason` 取首要原因(便于单调判断),`degraded_reasons` 给全量。
        let mut reasons: Vec<&'static str> = Vec::new();
        if !fsync_ok {
            reasons.push("fsync_failed");
        }
        if !log_writable {
            reasons.push("log_unwritable");
        }
        if !quorum_ok {
            reasons.push("quorum_unavailable");
        }
        if node.skew_exceeded() {
            reasons.push("skew_exceeded");
        }
        let ready = reasons.is_empty();
        // 单次取锁完成全部读取:`ready()` 已持有 node guard,再取同锁会自死锁
        // 注意:一次取锁算完三件事(见发现 0 的死锁教训)
        let (skew_filtered, skew_latest, skew_n) = node.skew_diag();
        let skew_ms = skew_filtered
            .map(|v| serde_json::json!(v))
            .unwrap_or(Value::Null);
        let clock_now_ok = node.clock_verified();
        let mut premises_unverified = Vec::new();
        if !self.selfcheck.voters_ok {
            premises_unverified.push("A3_voters".to_string());
        }
        if !self.selfcheck.fsync_ok {
            premises_unverified.push("A2_fsync".to_string());
        }
        // A1 以**运行时实测**为准:启动时若用了放行开关但运行中测到偏移在界内,则视为已验证;
        // 反之(测到超界或从未测到)则持续标注未验证。
        if !clock_now_ok {
            premises_unverified.push("A1_clock".to_string());
        }
        if !self.selfcheck.agent_fence_ok {
            premises_unverified.push("A4_agent_fence".to_string());
        }
        // A7:网络往返与时间参数是否同档(启动期实测;未测到对端时不假装已验证)
        if !self.selfcheck.network_ok {
            premises_unverified.push("A7_network".to_string());
        }
        let premises_ok = premises_unverified.is_empty();
        let _ = degraded_reason; // Node 侧的单一原因由上面 reasons 全量重算,避免两处口径不一致
        let reason = reasons.first().map(|s| s.to_string());
        let lab_degraded = !premises_ok;
        json!({
            "ready": ready,
            "role": role,
            "leader": leader,
            "term": term,
            "node_id": node.cfg.node_id,
            "shard": node.cfg.shard,
            "commit_index": commit_index,
            "applied_index": applied_index,
            "quorum_ok": quorum_ok,
            "log_writable": log_writable,
            "fsync_ok": fsync_ok,
            "skew_measured_ms": skew_ms,
            // 诊断:最新采样(含单程延迟)与有效样本数。
            // `skew_measured_ms` 与 `skew_latest_ms` 的差 = 被过滤掉的延迟量级 ⇒
            // 一眼区分「真 NTP 不同步」与「消息延迟尖峰」(见 docs 发现 22)。
            "skew_latest_ms": skew_latest.map(|v| serde_json::json!(v)).unwrap_or(Value::Null),
            "skew_samples": skew_n,
            "premises_ok": premises_ok,
            "premises_unverified": premises_unverified,
            "lab_degraded": lab_degraded,
            "agent_fence_ok": !self.agent_best_effort,
            "selfcheck": {
                "voters_ok": self.selfcheck.voters_ok,
                "data_dir_ok": self.selfcheck.data_dir_ok,
                "fsync_ok": self.selfcheck.fsync_ok,
                "clock_verified": self.selfcheck.clock_verified,
                "agent_fence_ok": self.selfcheck.agent_fence_ok,
                "network_ok": self.selfcheck.network_ok,
                "notes": self.selfcheck.notes,
            },
            "degraded_reason": reason,
            "degraded_reasons": reasons,
            "started_ms": self.started_ms,
            "uptime_ms": self.clock.now_ms().saturating_sub(self.started_ms),
            "delivered": self.delivered.load(std::sync::atomic::Ordering::Relaxed),
            "transport_errors": self.transport_errors.load(std::sync::atomic::Ordering::Relaxed),
        })
    }

    /// 等待某 index 被应用(带超时)
    pub fn wait_applied(&self, index: u64, timeout_ms: u64) -> Option<Applied> {
        let deadline = now_ms() + timeout_ms;
        loop {
            {
                let node = self.node.lock();
                if node.applied_index() >= index {
                    return node.result_of(index);
                }
            }
            if now_ms() > deadline {
                return None;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    pub fn status(&self) -> Value {
        let node = self.node.lock();
        let mut voters = Vec::new();
        for v in &node.cfg.voters {
            voters.push(json!({
                "id": v,
                "addr": self.table.get(v).map(|(ip, p)| format!("{ip}:{p}")),
                "is_self": v == &node.cfg.node_id,
            }));
        }
        json!({
            "node_id": node.cfg.node_id,
            "shard": node.cfg.shard,
            "role": node.role().as_str(),
            "term": node.term(),
            "leader": node.leader_id(),
            "commit_index": node.commit_index(),
            "applied_index": node.applied_index(),
            "snapshot_index": node.snapshot_index(),
            "voters": voters,
            "kv_len": node.sm.kv_len(),
            "leases": node.sm.lease_count(),
            "config_epoch": node.sm.config_epoch(),
            "data_dir": node.dir.display().to_string(),
        })
    }

    // ─── 管控集群视图(运维面:只读观测 + 两个受控动作) ───

    /// 本副本视角下的成员复制进度(非 leader 的 match/next 为 null)
    pub fn peers_view(&self) -> Value {
        let now = self.clock.now_ms();
        json!(self.node.lock().peers_view(now))
    }

    /// 本副本状态机里的实例租约台账(共识权威,最终一致)
    pub fn leases_view(&self) -> Value {
        let now = self.clock.now_ms();
        json!(self.node.lock().sm.leases_view(now))
    }

    /// 本副本的完整成员视图 = status(拓扑/状态机规模) + ready(就绪/前提/角色) + peers + leases
    ///
    /// **取样说明**:三次取锁之间状态机可能前进(term 跃迁 / 提交推进),重叠字段
    /// (role/term/leader/commit/applied)一律以最后读取的 `ready()` 为准,保证同一份 JSON 内不自相矛盾;
    /// 非重叠字段(kv_len/leases/snapshot_index)是"某次取锁时刻"的真实值,单调或瞬时,不做平滑。
    fn local_member_view(&self) -> Value {
        let status = self.status();
        let ready = self.ready();
        let peers = self.peers_view();
        let leases = self.leases_view();
        member_view(
            ready["node_id"].as_str().unwrap_or("").to_string(),
            String::new(),
            true,
            &status,
            &ready,
            &peers,
            &leases,
        )
    }

    /// 集群全量视图:本副本 + 向其余成员并发拉取 `/internal/view`(带超时)。
    ///
    /// 任何成员探测失败都**如实标记 `reachable=false` + `error`**,不隐藏、不猜测、不用本副本视角
    /// 冒充对方的视角 —— 这正是"看不了"与"坏了"必须区分的地方。
    pub async fn cluster_view(self: &Arc<Self>) -> Value {
        let local = self.local_member_view();
        let me = self.node_id();
        let mut handles = Vec::new();
        for id in self.table.ids() {
            if id == me {
                continue;
            }
            let Some((ip, port)) = self.table.get(&id).cloned() else {
                continue;
            };
            let token = self.token.clone();
            handles.push(tokio::spawn(async move {
                let r = get_json(
                    &ip,
                    port,
                    "/internal/view",
                    token.as_deref(),
                    MEMBER_PROBE_TIMEOUT_MS,
                )
                .await;
                (id, format!("{ip}:{port}"), r)
            }));
        }
        let mut members: Vec<Value> = vec![local];
        for h in handles {
            let Ok((id, addr, res)) = h.await else {
                continue; // 任务 panic:跳过(已在下方以 voters/reachable 计数体现不一致)
            };
            match res {
                Ok(v) => {
                    let status = v.get("status").cloned().unwrap_or_else(|| json!({}));
                    let ready = v.get("ready").cloned().unwrap_or_else(|| json!({}));
                    let peers = v.get("peers").cloned().unwrap_or_else(|| json!([]));
                    let leases = v.get("leases").cloned().unwrap_or_else(|| json!([]));
                    members.push(member_view(id, addr, false, &status, &ready, &peers, &leases));
                }
                Err(e) => {
                    // 不可达成员:除 id/addr 外一律不给字段(留空),避免前端把 null 渲染成 0 当成事实
                    members.push(json!({
                        "id": id, "addr": addr, "is_self": false,
                        "reachable": false, "error": e,
                        "role": Value::Null, "term": Value::Null, "leader": Value::Null,
                        "ready": Value::Null, "peers": [], "leases": [],
                    }));
                }
            }
        }
        // 成员按 id 稳定排序(便于对比不同时刻的同一行)
        members.sort_by(|a, b| {
            a["id"]
                .as_str()
                .unwrap_or("")
                .cmp(b["id"].as_str().unwrap_or(""))
        });
        let voters = self.table.ids().len();
        let reachable = members
            .iter()
            .filter(|m| m["reachable"] == Value::Bool(true))
            .count();
        let leader = members
            .iter()
            .find(|m| m["is_self"] == Value::Bool(true))
            .and_then(|m| m["leader"].as_str())
            .map(|s| s.to_string());
        let leader_addr = leader
            .as_deref()
            .and_then(|l| members.iter().find(|m| m["id"].as_str() == Some(l)))
            .and_then(|m| m["addr"].as_str())
            .map(|s| s.to_string());
        let local_term = members
            .iter()
            .find(|m| m["is_self"] == Value::Bool(true))
            .and_then(|m| m["term"].as_u64());
        let local_quorum_ok = members
            .iter()
            .find(|m| m["is_self"] == Value::Bool(true))
            .and_then(|m| m["quorum_ok"].as_bool());
        let mut notes = vec![
            format!(
                "成员信息由各副本的 /internal/view 现场拉取(单个超时 {MEMBER_PROBE_TIMEOUT_MS}ms,并发);reachable=false 表示该副本本次探测无响应"
            ),
            "peers[].match_index / repl_lag 仅 leader 维护,非 leader 为 null;last_ack_age_ms=null 表示本进程从未收到过该成员响应"
                .to_string(),
            "leases 来自本副本状态机(共识权威、最终一致);remaining_ms 以本副本时钟计算".to_string(),
        ];
        if reachable < voters {
            notes.push(format!(
                "有 {} 个成员本次不可达:可能是网络分区或进程退出;本页只呈现探测结果,不代替多数派判定",
                voters - reachable
            ));
        }
        json!({
            "enabled": true,
            "mode": "cluster",
            "version": env!("CARGO_PKG_VERSION"),
            "node_id": me,
            "generated_at_ms": now_ms(),
            "probe_timeout_ms": MEMBER_PROBE_TIMEOUT_MS,
            "quorum": {
                "voters": voters,
                "reachable": reachable,
                "ok": local_quorum_ok,
                "leader": leader,
                "leader_addr": leader_addr,
                "term": local_term,
            },
            "members": members,
            "leases": self.leases_view(),
            "notes": notes,
        })
    }

    // ─── 会话与 RBAC(共识权威;设计 §11.4 / C8;单机模式下不走这里) ───

    /// 认证读屏障:本副本必须**已 apply 到当前已知的 commit**才允许做认证判定。
    ///
    /// 为什么必须 fail-closed:一个尚未 apply 到"撤销/冻结/改权"那条日志的副本,
    /// 如果照常服务,就会接受**已撤销**的会话。宁可 503 也不放行(与 `/readyz` 同一取向)。
    /// 允许 `wait_ms` 的短暂等待,用于吸收"commit 已推进、apply 还差一瞬"的正常窗口。
    pub async fn auth_barrier(&self, wait_ms: u64) -> Result<(), super::auth::AuthError> {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(wait_ms);
        loop {
            let (applied, commit) = {
                let n = self.node.lock();
                (n.applied_index(), n.commit_index())
            };
            if super::auth::auth_barrier_ok(applied, commit) {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(super::auth::AuthError::Lagged { applied, commit });
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    /// 线性一致读准备:向 leader 取一次当前 commit index,并等**本副本**追平到它。
    ///
    /// 登录必须走这一步:口令校验读的是本地状态机,若本副本还停在"旧口令"上,
    /// 就会出现"刚改完密码的旧口令仍能登录"的越权窗口(设计风险 13)。
    pub async fn read_index(&self, timeout_ms: u64) -> Result<u64, super::auth::AuthError> {
        let me = self.node_id();
        let leader = {
            let n = self.node.lock();
            n.leader_id().map(|s| s.to_string())
        };
        let target = match leader {
            Some(l) if l == me => {
                let n = self.node.lock();
                n.commit_index()
            }
            Some(l) => {
                let Some((ip, port)) = self.table.get(&l).cloned() else {
                    tracing::warn!("认证 read-index:leader {l} 不在成员表中");
                    return Err(super::auth::AuthError::Quorum);
                };
                let t = timeout_ms.clamp(200, 3_000);
                let r = tokio::time::timeout(
                    Duration::from_millis(t),
                    get_json(&ip, port, "/internal/commit-index", self.token.as_deref(), t),
                )
                .await;
                match r {
                    Ok(Ok(v)) => v["commit_index"].as_u64().unwrap_or(0),
                    Ok(Err(e)) => {
                        tracing::warn!("认证 read-index:读取 leader {l} 失败:{e}");
                        return Err(super::auth::AuthError::Quorum);
                    }
                    Err(_) => {
                        tracing::warn!("认证 read-index:读取 leader {l} 超时({t}ms)");
                        return Err(super::auth::AuthError::Quorum);
                    }
                }
            }
            // 无 leader = 无多数派(cluster 模式下**建立会话必须写共识**)。
            // 这条必须映射成"暂不可用(503)"而不是内部错误:它反映的是可用性,
            // 也绝不能退化成"从 sink 读个密码就算登录成功"——那会在无多数派时
            // 伪造出一个谁都撤销不掉的会话(设计 C8)。
            None => {
                tracing::warn!("认证 read-index:当前无 leader(无多数派),拒绝建立会话");
                return Err(super::auth::AuthError::Quorum);
            }
        };
        self.wait_applied_async(target, timeout_ms)
            .await
            .ok_or_else(|| {
                let (applied, commit) = {
                    let n = self.node.lock();
                    (n.applied_index(), n.commit_index())
                };
                super::auth::AuthError::Lagged { applied, commit }
            })?;
        Ok(target)
    }

    /// 读取本地认证键空间(单次取锁)
    fn auth_keys(&self, prefix: &str) -> BTreeMap<String, Value> {
        let n = self.node.lock();
        n.sm
            .kv_prefix(prefix)
            .into_iter()
            .map(|(k, (v, _))| (k, v))
            .collect()
    }

    fn auth_entry(&self, key: &str) -> Option<Value> {
        let n = self.node.lock();
        n.sm.kv_get(key).map(|e| e.value.clone())
    }

    fn auth_entry_ver(&self, key: &str) -> Option<(u64, Value)> {
        let n = self.node.lock();
        n.sm.kv_get(key).map(|e| (e.ver, e.value.clone()))
    }

    /// 角色 → 权限表
    fn role_perms_map(&self) -> BTreeMap<String, Vec<String>> {
        let mut m = BTreeMap::new();
        for (k, v) in self.auth_keys(super::auth::K_ROLE) {
            if let Some(role) = k.strip_prefix(super::auth::K_ROLE) {
                m.insert(role.to_string(), super::auth::entry_perms(&v));
            }
        }
        m
    }

    fn perms_of(&self, user: &Value, role_perms: &BTreeMap<String, Vec<String>>) -> Vec<String> {
        super::auth::expand_perms(
            &super::auth::entry_roles(user),
            role_perms,
            crate::store::PERMISSIONS,
        )
    }

    /// 用户列表(与 store 同形;排序稳定 ⇒ 跨副本逐字节一致)
    pub fn auth_users_list(&self) -> Value {
        let users = self.auth_keys(super::auth::K_USER);
        let rp = self.role_perms_map();
        json!(super::auth::users_list_from(
            &users,
            &rp,
            crate::store::PERMISSIONS
        ))
    }

    /// 角色列表(与 store 同形)
    pub fn auth_roles_list(&self) -> Value {
        let roles = self.auth_keys(super::auth::K_ROLE);
        json!(super::auth::roles_list_from(&roles))
    }

    /// 是否已有 RBAC 记录(引导灌入的判定条件)
    pub fn auth_has_users(&self) -> bool {
        !self.auth_keys(super::auth::K_USER).is_empty()
    }

    /// 是否已经灌入过(幂等标记 `a/hydrated`)
    pub fn auth_hydrated(&self) -> bool {
        self.auth_entry(super::auth::K_HYDRATED).is_some()
    }

    /// 本副本视角的认证判定:会话 → (user, perms)。调用方需先过 `auth_barrier`。
    ///
    /// 返回 `None` = 会话不可用(不存在/过期/被撤销/账号冻结/epoch 不匹配),**一律拒绝**,
    /// 且不对外区分原因(避免暴露"该 token 存在但已过期")。
    pub fn auth_session_lookup(&self, token_hash: &str) -> Option<(String, Vec<String>)> {
        let now = self.clock.now_ms();
        let sess = self.auth_entry(&super::auth::session_key(token_hash))?;
        let user_name = super::auth::entry_user(&sess)?.to_string();
        let user = self.auth_entry(&super::auth::user_key(&user_name))?;
        if !super::auth::session_usable(&sess, &user_name, &user, now) {
            return None;
        }
        let perms = self.perms_of(&user, &self.role_perms_map());
        Some((user_name, perms))
    }

    /// 列出全部会话(可观测/排查;只给哈希前缀,不暴露可用凭据)
    pub fn auth_sessions_view(&self) -> Value {
        let now = self.clock.now_ms();
        let mut out = Vec::new();
        for (k, v) in self.auth_keys(super::auth::K_SESSION) {
            let h = k.strip_prefix(super::auth::K_SESSION).unwrap_or("");
            out.push(json!({
                "token_hash_prefix": &h[..h.len().min(12)],
                "user": super::auth::entry_user(&v),
                "issued_ms": v.get("issued_ms").cloned().unwrap_or(Value::Null),
                "expire_at_ms": super::auth::entry_expire_ms(&v),
                "expired": super::auth::entry_expire_ms(&v) <= now,
            }));
        }
        json!(out)
    }

    /// 登录:read-index(线性一致读)+ 本地口令校验 + 提议 `Put s/<hash>`。
    ///
    /// 返回 (明文 token, perms)。**明文 token 只在此刻存在于内存**,状态机里只有哈希。
    pub async fn auth_login(
        self: &Arc<Self>,
        user: &str,
        pass: &str,
        ttl_ms: u64,
        read_index_ms: u64,
    ) -> Result<(String, Vec<String>), super::auth::AuthError> {
        self.read_index(read_index_ms).await?;
        // 引导灌入未完成时,库里没有任何用户 —— 此时必须说"还没就绪",
        // 而不是"用户名或口令错误"(后者是在撒谎:我们根本还没加载凭据)。
        let user_key = super::auth::user_key(user);
        let entry = match self.auth_entry(&user_key) {
            Some(e) => e,
            None => {
                if !self.auth_hydrated() && !self.auth_has_users() {
                    return Err(super::auth::AuthError::NotReady);
                }
                return Err(super::auth::AuthError::BadCredentials);
            }
        };
        if !super::auth::password_ok(&entry, pass) {
            return Err(super::auth::AuthError::BadCredentials);
        }
        if !super::auth::entry_enabled(&entry) {
            return Err(super::auth::AuthError::Disabled);
        }
        let token = new_session_token();
        let hash = super::auth::token_hash(&token);
        let now = self.clock.now_ms();
        let rec =
            super::auth::session_entry(user, super::auth::entry_epoch(&entry), now, ttl_ms);
        let idx = self
            .put_key(&super::auth::session_key(&hash), rec, None)
            .await?;
        // 等"全部可达副本"都已 apply 这条会话:否则浏览器紧接着的请求若落到另一副本,
        // 会因"还没学到这条 commit"而 401(表现为"刚登录就被踢回登录页")。
        let (okn, missing) = self.wait_visible_on_all(idx, 1_500).await;
        if !missing.is_empty() {
            tracing::warn!(
                "会话 {} 已写入 index={idx},但以下副本未在 1.5s 内确认可见:{:?}(其上的请求可能被要求重新登录)",
                user,
                missing
            );
        } else {
            tracing::debug!("会话已生效于全部 {okn} 个副本(index={idx})");
        }
        let perms = self.perms_of(&entry, &self.role_perms_map());
        Ok((token, perms))
    }

    /// 登出/撤销(幂等:不存在也算成功,避免泄露 token 是否有效)。
    ///
    /// 返回被删除记录的日志 index(**None** = 本来就没有该会话);调用方据此等待
    /// "全部可达副本都已生效",再告诉人"已登出"。
    pub async fn auth_session_revoke(
        self: &Arc<Self>,
        token_hash: &str,
    ) -> Result<Option<u64>, super::auth::AuthError> {
        let key = super::auth::session_key(token_hash);
        if self.auth_entry(&key).is_none() {
            return Ok(None);
        }
        self.delete_key_idx(&key).await.map(Some)
    }

    async fn delete_key_idx(
        self: &Arc<Self>,
        key: &str,
    ) -> Result<u64, super::auth::AuthError> {
        match self
            .propose_op(Op::Delete {
                key: key.to_string(),
                expect_ver: None,
            })
            .await
        {
            Ok((idx, applied)) if applied["status"] == Value::String("applied".into()) => {
                self.await_local(idx).await?;
                Ok(idx)
            }
            Ok((_, applied)) => Err(super::auth::AuthError::Internal(format!(
                "删除被状态机拒绝:{applied}"
            ))),
            Err(HaError::QuorumUnavailable) => Err(super::auth::AuthError::Quorum),
            Err(e) => Err(super::auth::AuthError::Internal(e.to_string())),
        }
    }

    /// 写入后**等本副本 apply 到该 index**才返回。
    ///
    /// 为什么必须等:提案可能在 follower 上发起(`propose_op` 会自动转发),返回值里的
    /// `applied` 是 **leader 侧**的结果 —— 只证明"已提交"。若不等于本地也生效,
    /// 紧接着的请求(例如刚登录就 `/api/auth/me`、刚登出就再请求)会在**同一个副本**上
    /// 看到旧状态:登录立刻 401、登出后仍然 200。这与设计 §19 发现 16(租约授予被误判为失败)
    /// 是同一类错误的两个面:**"leader 已提交"≠"本副本已生效"**。
    async fn await_local(self: &Arc<Self>, idx: u64) -> Result<(), super::auth::AuthError> {
        if self.wait_applied_async(idx, AUTH_LOCAL_APPLY_MS).await.is_some() {
            return Ok(());
        }
        let (applied, commit) = {
            let n = self.node.lock();
            (n.applied_index(), n.commit_index())
        };
        Err(super::auth::AuthError::Lagged { applied, commit })
    }

    async fn put_key(
        self: &Arc<Self>,
        key: &str,
        value: Value,
        expect_ver: Option<u64>,
    ) -> Result<u64, super::auth::AuthError> {
        match self
            .propose_op(Op::Put {
                key: key.to_string(),
                value,
                expect_ver,
            })
            .await
        {
            Ok((idx, applied)) if applied["status"] == Value::String("applied".into()) => {
                self.await_local(idx).await?;
                Ok(idx)
            }
            Ok((_, applied)) => Err(super::auth::AuthError::Internal(format!(
                "写入被状态机拒绝(CAS 版本不符或条件不满足):{applied}"
            ))),
            Err(HaError::QuorumUnavailable) => Err(super::auth::AuthError::Quorum),
            Err(e) => Err(super::auth::AuthError::Internal(e.to_string())),
        }
    }

    async fn delete_key(self: &Arc<Self>, key: &str) -> Result<(), super::auth::AuthError> {
        self.delete_key_idx(key).await.map(|_| ())
    }

    /// 读-改-写用户记录(CAS 重试)。
    ///
    /// 为什么用 CAS 而不是"最后写入者获胜":两个管理员同时改同一用户时,
    /// 后者必须看到前者的结果(否则权限会被静默回退)。`Put{expect_ver}` 恰好提供这个语义。
    /// `bump_epoch` 由闭包决定(口令或启用态变化必须 +1 → 旧会话立即失效)。
    async fn mutate_user<F>(self: &Arc<Self>, user: &str, mut f: F) -> Result<(), super::auth::AuthError>
    where
        F: FnMut(Option<Value>) -> (Value, bool),
    {
        let key = super::auth::user_key(user);
        for _ in 0..8 {
            let cur = self.auth_entry_ver(&key);
            let (ver, old) = match cur {
                Some((v, val)) => (Some(v), Some(val)),
                None => (None, None),
            };
            let (mut new, bump) = f(old.clone());
            let base = old.as_ref().map(super::auth::entry_epoch).unwrap_or(0);
            new["epoch"] = json!(if bump { base + 1 } else { base });
            match self.put_key(&key, new, ver).await {
                Ok(_) => return Ok(()),
                Err(super::auth::AuthError::Internal(m)) if m.contains("CAS") => continue,
                Err(e) => return Err(e),
            }
        }
        Err(super::auth::AuthError::Internal(format!(
            "用户 {user} 更新在 8 次 CAS 重试后仍未成功(并发写过多)"
        )))
    }

    /// 角色读-改-写(CAS 重试)
    async fn mutate_role<F>(self: &Arc<Self>, role: &str, mut f: F) -> Result<(), super::auth::AuthError>
    where
        F: FnMut(Option<Value>) -> Value,
    {
        let key = super::auth::role_key(role);
        for _ in 0..8 {
            let cur = self.auth_entry_ver(&key);
            let (ver, old) = match cur {
                Some((v, val)) => (Some(v), Some(val)),
                None => (None, None),
            };
            let new = f(old);
            match self.put_key(&key, new, ver).await {
                Ok(_) => return Ok(()),
                Err(super::auth::AuthError::Internal(m)) if m.contains("CAS") => continue,
                Err(e) => return Err(e),
            }
        }
        Err(super::auth::AuthError::Internal(format!(
            "角色 {role} 更新在 8 次 CAS 重试后仍未成功"
        )))
    }

    /// 新建/改密/改启用态/改角色(口令或启用态变化 ⇒ epoch +1 ⇒ 该用户所有会话失效)
    pub async fn auth_user_upsert(
        self: &Arc<Self>,
        user: &str,
        pass: Option<&str>,
        enabled: Option<bool>,
        roles: Option<Vec<String>>,
    ) -> Result<(), super::auth::AuthError> {
        if user.trim().is_empty() {
            return Err(super::auth::AuthError::Internal("用户名不能为空".into()));
        }
        let pass = pass.map(|s| s.to_string());
        self.mutate_user(user, move |old| {
            let get = |k: &str| {
                old.as_ref()
                    .and_then(|v| v.get(k))
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string()
            };
            let old_enabled = old
                .as_ref()
                .map(super::auth::entry_enabled)
                .unwrap_or(true);
            let old_roles = old
                .as_ref()
                .map(super::auth::entry_roles)
                .unwrap_or_default();
            let (salt, hash, pass_changed) = match pass.as_deref() {
                Some(p) if !p.is_empty() => {
                    let s = super::auth::new_salt();
                    let h = super::auth::hash_password(&s, p);
                    (s, h, true)
                }
                _ => (get("salt"), get("pass_hash"), false),
            };
            let en = enabled.unwrap_or(old_enabled);
            let rr = roles.clone().unwrap_or(old_roles);
            let bump = pass_changed || en != old_enabled;
            (super::auth::user_entry(&salt, &hash, en, &rr, 0), bump)
        })
        .await
    }

    /// 仅切换启用态(冻结/解冻)
    pub async fn auth_user_set_enabled(
        self: &Arc<Self>,
        user: &str,
        enabled: bool,
    ) -> Result<(), super::auth::AuthError> {
        self.auth_user_upsert(user, None, Some(enabled), None).await
    }

    /// 覆盖用户角色(不改 epoch:权限每请求现算,改角色即时生效且无需踢会话)
    pub async fn auth_user_roles_set(
        self: &Arc<Self>,
        user: &str,
        roles: Vec<String>,
    ) -> Result<(), super::auth::AuthError> {
        self.auth_user_upsert(user, None, None, Some(roles)).await
    }

    /// 新建/更新角色描述
    pub async fn auth_role_upsert(
        self: &Arc<Self>,
        role: &str,
        desc: &str,
    ) -> Result<(), super::auth::AuthError> {
        if role.trim().is_empty() {
            return Err(super::auth::AuthError::Internal("角色名不能为空".into()));
        }
        let d = desc.to_string();
        self.mutate_role(role, move |old| {
            let perms = old
                .as_ref()
                .map(super::auth::entry_perms)
                .unwrap_or_default();
            super::auth::role_entry(&d, &perms, 0)
        })
        .await
    }

    /// 覆盖角色权限
    pub async fn auth_role_perms_set(
        self: &Arc<Self>,
        role: &str,
        perms: Vec<String>,
    ) -> Result<(), super::auth::AuthError> {
        self.mutate_role(role, move |old| {
            let desc = old
                .as_ref()
                .and_then(|v| v.get("description"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            super::auth::role_entry(&desc, &perms, 0)
        })
        .await
    }

    /// 引导灌入:把 sink 里的既有 RBAC 复制进状态机(仅 leader、仅一次、幂等)。
    ///
    /// 为什么需要:升级到 cluster 模式时状态机是空的,而用户/角色早已存在于 MySQL(sink)。
    /// 不灌入 = 谁都登录不了。灌入**不含会话**(会话不迁移:切模式必须重新登录,设计 §15)。
    /// 参数必须是含 `salt/pass_hash` 的**原始记录**(列表接口不暴露口令摘要)。
    pub async fn auth_hydrate_from(
        self: &Arc<Self>,
        users: &[Value],
        roles: &[Value],
    ) -> Result<usize, super::auth::AuthError> {
        if self.auth_hydrated() {
            return Ok(0);
        }
        let mut n = 0usize;
        for r in roles {
            let Some(name) = r["name"].as_str() else {
                continue;
            };
            let desc = r["description"].as_str().unwrap_or("");
            let perms: Vec<String> = r["perms"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default();
            self.put_key(
                &super::auth::role_key(name),
                super::auth::role_entry(desc, &perms, 0),
                None,
            )
            .await?;
            n += 1;
        }
        for u in users {
            let Some(name) = u["user"].as_str() else {
                continue;
            };
            let salt = u["salt"].as_str().unwrap_or("");
            let hash = u["pass_hash"].as_str().unwrap_or("");
            if hash.is_empty() {
                tracing::warn!("引导灌入跳过用户 {name}:缺少口令摘要(sink 未提供)");
                continue;
            }
            let roles: Vec<String> = u["roles"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default();
            let enabled = u["enabled"].as_bool().unwrap_or(true);
            self.put_key(
                &super::auth::user_key(name),
                super::auth::user_entry(salt, hash, enabled, &roles, 1),
                None,
            )
            .await?;
            n += 1;
        }
        // 幂等标记**最后**写:中途失败则下次启动重试(逐条写入本身是覆盖,天然幂等)
        self.put_key(
            super::auth::K_HYDRATED,
            json!({ "at_ms": self.clock.now_ms() }),
            None,
        )
        .await?;
        Ok(n)
    }

    /// 等待该 index 在**全部可达副本**上都已 **apply**(各副本自报 `applied_index`)。
    ///
    /// 为什么要这么强:屏障只能挡住"已经知道 commit 但还没 apply"的窗口,挡不住
    /// "**还没从心跳里学到新 commit**"的副本 —— 那种副本既不会拒绝、也不知道该拒绝,
    /// 于是刚登录的 cookie 打到它身上就是 401(实测:100% 复现,不是理论问题)。
    /// 每次请求都做 read-index 太贵(多一个 RTT),所以在**写入方**等一次:
    /// 登录/登出这类"必须立刻在所有入口生效"的少数操作,把窗口压到零(对可达副本)。
    ///
    /// 返回 `(已确认副本数, 未确认成员)`;不可达/超时都**如实返回**,由调用方决定怎么汇报。
    pub async fn wait_visible_on_all(
        self: &Arc<Self>,
        index: u64,
        timeout_ms: u64,
    ) -> (usize, Vec<String>) {
        let me = self.node_id();
        let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
        let mut pending: Vec<String> = self.table.ids().into_iter().filter(|i| *i != me).collect();
        loop {
            let mut handles = Vec::new();
            for id in pending.iter() {
                let Some((ip, port)) = self.table.get(id).cloned() else {
                    continue;
                };
                let token = self.token.clone();
                let id2 = id.clone();
                handles.push(tokio::spawn(async move {
                    let r = get_json(&ip, port, "/internal/commit-index", token.as_deref(), 500).await;
                    (id2, r)
                }));
            }
            let mut still: Vec<String> = Vec::new();
            let mut ok = 1usize; // 自己:本地已 apply 才会走到这里
            for h in handles {
                match h.await {
                    Ok((id, Ok(v))) => {
                        if v["applied_index"].as_u64().unwrap_or(0) >= index {
                            ok += 1;
                        } else {
                            still.push(id);
                        }
                    }
                    Ok((id, Err(_))) => still.push(id),
                    Err(_) => {}
                }
            }
            if still.is_empty() || tokio::time::Instant::now() >= deadline {
                return (ok, still);
            }
            pending = still;
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// 主动让位:把 leader 交还给 raft(继任者由 raft 按"日志已追平优先"选择)。
    ///
    /// 本副本是 leader 就地让位;否则把请求**转发**给已知 leader 的 `/internal/stepdown`
    /// (运维从任意副本入口操作都成立)。当前无 leader 时明确报错,不假装成功。
    pub async fn step_down_leader(self: &Arc<Self>) -> Result<Value, LeaseError> {
        let me = self.node_id();
        let leader = {
            let n = self.node.lock();
            n.leader_id().map(|s| s.to_string())
        };
        let Some(l) = leader else {
            return Err(LeaseError::Internal(
                "当前无 leader(可能正在选主中),请稍后重试".into(),
            ));
        };
        if l == me {
            let res = { self.node.lock().step_down() };
            return match res {
                Ok(outs) => {
                    self.deliver(outs).await;
                    Ok(json!({ "stepped_down": true, "via": "local", "former_leader": me }))
                }
                Err(e) => Err(LeaseError::Internal(e.to_string())),
            };
        }
        let Some((ip, port)) = self.table.get(&l).cloned() else {
            return Err(LeaseError::Internal(format!("leader {l} 不在成员表中")));
        };
        let r = tokio::time::timeout(
            Duration::from_millis(DELIVER_TIMEOUT_MS),
            post_json(
                &ip,
                port,
                "/internal/stepdown",
                &json!({}),
                self.token.as_deref(),
                DELIVER_TIMEOUT_MS,
            ),
        )
        .await;
        match r {
            Ok(Ok(v)) => Ok(json!({
                "stepped_down": v["stepped_down"],
                "via": "forward",
                "leader": l,
                "former_leader": l,
            })),
            Ok(Err(e)) => Err(LeaseError::Internal(format!("转发给 leader {l} 失败:{e}"))),
            Err(_) => Err(LeaseError::Internal(format!(
                "转发给 leader {l} 超时({DELIVER_TIMEOUT_MS}ms)"
            ))),
        }
    }

    /// leader 侧的过期租约回收(确定性 GC;设计 §19 发现 15)。
    ///
    /// 只由 leader 提议(避免多副本同时写同一条 GC);且**先本地数一遍**,没有可回收项就
    /// 不提议 —— 否则每轮都会往日志里塞一条空 op,把真日志刷满。
    ///
    /// `cutoff_ms = now - (max_skew_ms + 宽限)`,其中宽限默认 60s
    /// (`RDSCTL_LEASE_REAP_GRACE_MS`)。安全性证明见 `Op::LeasePurge` 的注释:
    /// 任何后续授予的 `at_ms` 都满足 `>= cutoff_ms + max_skew_ms`,
    /// 因此被删掉的条目"冲突检查本来就会放行",回收不削弱 §5.4。
    ///
    /// 返回 `(本次回收前的候选数, cutoff_ms)`。
    pub async fn purge_expired_leases(self: &Arc<Self>) -> Result<(usize, u64), String> {
        if !self.is_leader() {
            return Err("not_leader".into());
        }
        let (now, skew) = {
            let n = self.node.lock();
            (self.clock.now_ms(), n.cfg.max_skew_ms)
        };
        let grace = std::env::var("RDSCTL_LEASE_REAP_GRACE_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(60_000);
        let cutoff = now.saturating_sub(skew.saturating_add(grace));
        // 安全不变量:`cutoff_ms + max_skew_ms <= 提议时刻`(证明见 `Op::LeasePurge`)。
        // 显式断言而不是"靠写法正确"——将来有人改 cutoff 公式会立刻在这里被打回。
        // 注意 `now` 早于真实提议时刻,所以该式成立则真实不变量更强成立。
        debug_assert!(
            cutoff.saturating_add(skew) <= now,
            "回收 cutoff 违反安全不变量:cutoff={cutoff} skew={skew} now={now}"
        );
        let cands = self
            .leases_view()
            .as_array()
            .map(|a| {
                a.iter()
                    .filter(|l| l["expire_at_ms"].as_u64().unwrap_or(u64::MAX) <= cutoff)
                    .count()
            })
            .unwrap_or(0);
        if cands == 0 {
            return Ok((0, cutoff));
        }
        match self.propose_op(super::state::Op::LeasePurge { cutoff_ms: cutoff }).await {
            Ok((_, applied)) => {
                if applied["status"] == Value::String("applied".into()) {
                    tracing::info!("回收过期租约 {cands} 条(cutoff_ms={cutoff})");
                    Ok((cands, cutoff))
                } else {
                    Err(format!("回收提案未生效:{applied}"))
                }
            }
            Err(e) => Err(e.to_string()),
        }
    }

    /// 重置 sink 投影游标(重建审计投影的起点)。
    ///
    /// 只在 **leader** 上有意义:投影循环只在 leader 跑(设计 §12),在 follower 上重置本地游标
    /// 既不会立刻生效、也不会被使用,因此**拒绝**并告知当前 leader,而不是返回一个假的"成功"。
    /// 动作本身不写库:只把本地游标置 0,leader 下一轮(≤5s)按幂等标记从日志重放补齐。
    pub fn resync_sink(&self) -> Result<Value, LeaseError> {
        let (dir, shard, leader) = {
            let n = self.node.lock();
            (
                n.dir.clone(),
                n.cfg.shard,
                n.leader_id().map(|s| s.to_string()),
            )
        };
        let me = self.node_id();
        if leader.as_deref() != Some(me.as_str()) {
            return Err(LeaseError::NotLeader(leader));
        }
        let mut proj = super::projection::Projector::open(&dir, shard);
        let before = proj.cursor();
        proj.reset(0)
            .map_err(|e| LeaseError::Internal(e.to_string()))?;
        Ok(json!({
            "before": before,
            "after": 0,
            "data_dir": dir.display().to_string(),
            "shard": shard,
            "note": "运行中的 leader 将在下一轮对账(≤5s)从日志重放并按幂等标记补齐;重复行不会产生",
        }))
    }

    // ─── 实例租约(共识权威;供 RdsManager 的 authority 使用) ───

    /// 获取实例租约。成功返回本次授予的 fence;失败原因区分"被他人持有"与"暂无多数派"。
    pub async fn acquire_lease(
        self: &Arc<Self>,
        instance: &str,
        holder: &str,
        ttl_ms: u64,
    ) -> Result<super::Fence, LeaseError> {
        // 注意:必须一次性取锁求值 —— `if self.node.lock()...{}` 里再取同锁会**自死锁**
        // (parking_lot::Mutex 不可重入,且 if 条件里的临时 guard 活到整个 if 语句结束)。
        if let Some(ms) = self.skew_violation() {
            return Err(LeaseError::Skew(ms));
        }
        let op = Op::LeaseGrant {
            instance: instance.to_string(),
            holder: holder.to_string(),
            ttl_ms,
            at_ms: 0,
        };
        match self.propose_op(op).await {
            Ok((idx, applied)) => match applied["status"].as_str() {
                Some("applied") => self
                    .await_local_lease_apply(instance, idx)
                    .await
                    .ok_or_else(|| self.local_lag_error(instance, idx)),
                Some("rejected") => Err(LeaseError::Held(
                    applied["reason"].as_str().unwrap_or("被其它持有者占用").to_string(),
                )),
                other => Err(LeaseError::Internal(format!("未知 apply 结果:{other:?}"))),
            },
            Err(HaError::NotLeader { leader }) => Err(LeaseError::NotLeader(leader)),
            Err(HaError::QuorumUnavailable) => Err(LeaseError::Quorum),
            Err(e) => Err(LeaseError::Internal(e.to_string())),
        }
    }

    /// 续约。失败必须由调用方视为"已失去租约"并停止执行副作用(设计 §5.4)。
    pub async fn renew_lease(self: &Arc<Self>, instance: &str, holder: &str) -> Result<super::Fence, LeaseError> {
        // 注意:必须一次性取锁求值 —— `if self.node.lock()...{}` 里再取同锁会**自死锁**
        // (parking_lot::Mutex 不可重入,且 if 条件里的临时 guard 活到整个 if 语句结束)。
        if let Some(ms) = self.skew_violation() {
            return Err(LeaseError::Skew(ms));
        }
        let op = Op::LeaseRenew {
            instance: instance.to_string(),
            holder: holder.to_string(),
            at_ms: 0,
        };
        match self.propose_op(op).await {
            Ok((idx, applied)) => match applied["status"].as_str() {
                Some("applied") => self
                    .await_local_lease_apply(instance, idx)
                    .await
                    .ok_or_else(|| self.local_lag_error(instance, idx)),
                Some("rejected") => Err(LeaseError::Held(
                    applied["reason"].as_str().unwrap_or("非持有者").to_string(),
                )),
                other => Err(LeaseError::Internal(format!("未知 apply 结果:{other:?}"))),
            },
            Err(HaError::NotLeader { leader }) => Err(LeaseError::NotLeader(leader)),
            Err(HaError::QuorumUnavailable) => Err(LeaseError::Quorum),
            Err(e) => Err(LeaseError::Internal(e.to_string())),
        }
    }

    /// 释放(仅持有者;失败不致命,租约到期会自然失效)
    pub async fn release_lease(self: &Arc<Self>, instance: &str, holder: &str) -> Result<(), LeaseError> {
        let op = Op::LeaseRelease {
            instance: instance.to_string(),
            holder: holder.to_string(),
        };
        match self.propose_op(op).await {
            Ok(_) => Ok(()),
            Err(HaError::QuorumUnavailable) => Err(LeaseError::Quorum),
            Err(HaError::NotLeader { leader }) => Err(LeaseError::NotLeader(leader)),
            Err(e) => Err(LeaseError::Internal(e.to_string())),
        }
    }

    /// 内部写入口(供 authority 与步骤账本使用):**任意节点可发起**。
    ///
    /// 若本节点不是 leader,则把提案转发给当前 leader(HTTP `/internal/propose`)——
    /// 否则"请求落到非 leader 的 API 实例"就会全部失败,集群里 2/3 的入口形同虚设。
    /// 转发只在**发起侧**发生一次:RPC 处理端用本地版(不转发),因此不会成环。
    pub async fn propose_op(self: &Arc<Self>, op: Op) -> Result<(u64, Value), HaError> {
        // 有界试探:领导权抖动期间"缓存的 leader"可能已过期,对端会回 not_leader;
        // 此时按其提示重试,提示缺失则依次试其它 voter(≤ 成员数-1 次,绝不成环)。
        let me = self.node_id();
        let mut tried: Vec<String> = vec![me.clone()];
        let mut last_leader: Option<String> = None;
        let budget = self.table.addrs.len().max(1);
        for _ in 0..budget {
            let (is_leader, leader) = {
                let n = self.node.lock();
                (n.is_leader(), n.leader_id().map(|s| s.to_string()))
            };
            if is_leader {
                return self.propose_with_flush(op.clone()).await;
            }
            // 候选目标:优先当前视图里的 leader,其次任一未试过的 voter
            let target = match leader {
                Some(l) if !tried.contains(&l) => Some(l),
                _ => self
                    .table
                    .addrs
                    .keys()
                    .find(|k| !tried.contains(k))
                    .cloned(),
            };
            let Some(l) = target else {
                return Err(HaError::NotLeader { leader: last_leader });
            };
            tried.push(l.clone());
            last_leader = Some(l.clone());
            let Some((ip, port)) = self.table.get(&l).cloned() else {
                continue;
            };
            let body = serde_json::to_value(&op)
                .map_err(|e| HaError::Transport(format!("op 序列化失败:{e}")))?;
            match post_json(
                &ip,
                port,
                "/internal/propose",
                &body,
                self.token.as_deref(),
                RPC_PROPOSE_TIMEOUT_MS,
            )
            .await
            {
                Ok(v) => {
                    if v["ok"] == Value::Bool(true) {
                        return Ok((v["index"].as_u64().unwrap_or(0), v["applied"].clone()));
                    }
                    match v["error"].as_str().unwrap_or("") {
                        "quorum_unavailable" => return Err(HaError::QuorumUnavailable),
                        "not_leader" => {
                            if let Some(n) = v["leader"].as_str() {
                                last_leader = Some(n.to_string());
                            }
                            continue; // 试下一个候选
                        }
                        other => return Err(HaError::Transport(format!("转发提案失败:{other}"))),
                    }
                }
                Err(e) => {
                    tracing::debug!("转发提案到 {l} 失败:{e};尝试其它副本");
                    continue;
                }
            }
        }
        Err(HaError::NotLeader { leader: last_leader })
    }

    /// 本地提案(仅 leader;不转发)—— RPC 处理端使用
    async fn propose_local_only(self: &Arc<Self>, op: Op) -> Result<(u64, Value), HaError> {
        self.propose_with_flush(op).await
    }

    /// 等待本地状态机追平到某租约可见(转发场景下 follower 需要一点时间追平)。
    ///
    /// 必须是 async:此前用 `std::thread::sleep` 会**阻塞桥接 runtime 的工作线程**,
    /// 而桥接 runtime 只有 2 个 worker —— 并发续约时会把 HTTP 请求一起饿死,
    /// 表现为"续约超时 → 误判失去租约 → 任务被中止"。
    /// 等待"本副本已 apply 到本次租约写入的 index",然后返回本地读到 fence。
    ///
    /// 为什么按 **index** 等而不是按"轮询租约是否出现"(旧实现):
    ///   · `propose_op` 返回的 `applied` 是 **leader 侧**的结果,只证明"已提交";
    ///     发起方若恰是 follower,自己的状态机可能还没跑到该 index;
    ///   · 按 index 等是确定性判据(applied_index >= index),不会因为"租约随后被释放/覆盖"
    ///     而误判为"没授予";
    ///   · 若最终读到 `None`,说明该 index 已被后续 op 覆盖(例如我们已被接管),这才是真的失去租约。
    async fn await_local_lease_apply(
        self: &Arc<Self>,
        instance: &str,
        index: u64,
    ) -> Option<super::Fence> {
        let t0 = std::time::Instant::now();
        let ok = self.wait_applied_async(index, LEASE_LOCAL_CATCHUP_MS).await;
        let el = t0.elapsed();
        if el.as_millis() >= 1_000 {
            // 偏慢就留痕:这类"已提交但本副本追不上"的可用性事故必须能从日志复盘
            let (applied, commit) = {
                let n = self.node.lock();
                (n.applied_index(), n.commit_index())
            };
            tracing::warn!(
                "本副本追平租约 index={index} 耗时 {}ms(applied={applied}, commit={commit}, 成功={})",
                el.as_millis(),
                ok.is_some()
            );
        }
        ok?;
        self.lease_fence(instance)
    }

    /// 本地追平超时的错误:带上 index 与本地水位,便于一眼看出"是延迟还是真掉队"
    fn local_lag_error(&self, instance: &str, index: u64) -> LeaseError {
        let (applied, commit, ready) = {
            let n = self.node.lock();
            (n.applied_index(), n.commit_index(), n.leader_id().map(|s| s.to_string()))
        };
        LeaseError::Internal(format!(
            "租约已提交(index={index})但本副本未在 {LEASE_LOCAL_CATCHUP_MS}ms 内追平\
             (applied_index={applied}, commit_index={commit}, leader={ready:?});\
             实例 {instance} 本次操作未开始,可安全重试"
        ))
    }

    /// 若实测偏移超界则返回偏移值(单次取锁,避免重入死锁)
    fn skew_violation(&self) -> Option<u64> {
        let n = self.node.lock();
        if n.skew_exceeded() {
            n.skew_measured_ms()
        } else {
            None
        }
    }

    /// 测试钩子:注入对某 peer 的时钟偏移观测(验证"超界即拒绝授予租约")
    #[cfg(test)]
    pub fn inject_peer_clock_offset(&self, peer: &str, offset_ms: i64) {
        self.node.lock().inject_peer_offset(peer, offset_ms);
    }

    /// 测试钩子:跳过前提门禁直接构造运行时(便于单测聚焦机制本身)
    #[cfg(test)]
    pub fn start_for_test(
        cfg: RaftConfig,
        table: MemberTable,
        dir: &std::path::Path,
    ) -> HaResult<Arc<Self>> {
        let clock: SharedClock = Arc::new(SystemClock);
        let node = Node::open(cfg, dir, clock.clone())?;
        Ok(Arc::new(Self {
            node: Mutex::new(node),
            table,
            clock,
            token: None,
            started_ms: now_ms(),
            clock_unverified: false,
            agent_best_effort: false,
            selfcheck: SelfCheck {
                voters_ok: true,
                data_dir_ok: true,
                fsync_ok: true,
                clock_verified: false,
                agent_fence_ok: false,
                network_ok: true,
                notes: vec![],
                all_ok: false,
            },
            inflight: Mutex::new(BTreeSet::new()),
            delivered: std::sync::atomic::AtomicU64::new(0),
            transport_errors: std::sync::atomic::AtomicU64::new(0),
        }))
    }

    /// 优雅让位(异步):滚动升级前调用,避免写空窗
    pub async fn step_down(&self) -> Result<bool, HaError> {
        let (was_leader, outs) = {
            let mut node = self.node.lock();
            let was = node.is_leader();
            let outs = node.step_down()?;
            (was, outs)
        };
        self.deliver(outs).await;
        Ok(was_leader)
    }

    // ─── 同步桥(临时措施,见设计 §19 待办) ───
    //
    // 既有生命周期入口(create/destroy/scaleout/replace/migrate/...)全是**同步**函数,
    // 直接改 async 会波及约 50 处调用点与大量测试。这里用"另起 runtime 的线程 + 通道"
    // 把同步调用桥接到共识提案上:阻塞的是该次生命周期操作(低频),不影响运行时其它 worker。
    // 待生命周期入口统一切到 async 后应移除此桥。

    pub fn acquire_lease_blocking(
        self: &Arc<Self>,
        instance: &str,
        holder: &str,
        ttl_ms: u64,
    ) -> Result<super::Fence, LeaseError> {
        let me = Arc::clone(self);
        let (i, h) = (instance.to_string(), holder.to_string());
        bridge_block(
            async move { me.acquire_lease(&i, &h, ttl_ms).await },
            Duration::from_millis(LEASE_BRIDGE_TIMEOUT_MS),
        )
    }

    pub fn renew_lease_blocking(
        self: &Arc<Self>,
        instance: &str,
        holder: &str,
    ) -> Result<super::Fence, LeaseError> {
        let me = Arc::clone(self);
        let (i, h) = (instance.to_string(), holder.to_string());
        bridge_block(
            async move { me.renew_lease(&i, &h).await },
            Duration::from_millis(LEASE_BRIDGE_TIMEOUT_MS),
        )
    }

    pub fn release_lease_blocking(self: &Arc<Self>, instance: &str, holder: &str) -> Result<(), LeaseError> {
        let me = Arc::clone(self);
        let (i, h) = (instance.to_string(), holder.to_string());
        bridge_block(
            async move { me.release_lease(&i, &h).await },
            Duration::from_millis(LEASE_BRIDGE_TIMEOUT_MS),
        )
    }

    /// 只读访问本地日志与应用进度(供 sink 投影使用)
    pub fn with_log<R>(&self, f: impl FnOnce(&super::log::Log, u64) -> R) -> R {
        let n = self.node.lock();
        f(&n.log, n.applied_index())
    }

    /// 当前租约的 fence(未持有则 None)
    pub fn lease_fence(&self, instance: &str) -> Option<super::Fence> {
        let node = self.node.lock();
        node.sm.lease_of(instance).map(|l| l.fence(node.cfg.shard))
    }

    /// 状态机查询(内部诊断/验收用)
    pub fn kv_view(&self, key: &str) -> Option<Value> {
        let node = self.node.lock();
        node.sm.kv_get(key).map(|e| {
            json!({ "value": e.value, "ver": e.ver, "updated_index": e.updated_index })
        })
    }

    pub fn lease_view(&self, instance: &str) -> Option<Value> {
        let node = self.node.lock();
        node.sm.lease_of(instance).map(|l| {
            json!({
                "instance": l.instance,
                "holder": l.holder,
                "expire_at_ms": l.expire_at_ms,
                "ttl_ms": l.ttl_ms,
                "renewals": l.renewals,
                "fence": l.fence(node.cfg.shard).wire(),
            })
        })
    }

    /// 列出某任务的步骤账本记录(诊断/验收用;key 前缀即 task_id,分隔符为 '|')
    pub fn steps_of_task(&self, task_id: &str) -> Vec<Value> {
        let node = self.node.lock();
        node.sm
            .steps_of(task_id)
            .into_iter()
            .map(|(key, r)| {
                json!({
                    "key": key,
                    "state": if r.state == super::state::StepState::Done { "done" } else { "started" },
                    "attempts": r.attempts,
                    "result": r.result,
                    "done_index": r.done_index,
                })
            })
            .collect()
    }

    pub fn step_view(&self, task_id: &str, node_name: &str, step: u32, idem: &str) -> Option<Value> {
        let node = self.node.lock();
        node.sm.step_record(task_id, node_name, step, idem).map(|r| {
            json!({
                "state": if r.state == super::state::StepState::Done { "done" } else { "started" },
                "attempts": r.attempts,
                "result": r.result,
                "started_index": r.started_index,
                "done_index": r.done_index,
            })
        })
    }

    /// 公开探针服务(cluster 模式的公开端口只提供 /healthz、/readyz;
    /// 其余路径一律 503 —— **不假装业务可用**)
    pub async fn serve_public(self: Arc<Self>, port: u16) -> std::io::Result<()> {
        let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
        tracing::info!("cluster 公开端口 listening on 0.0.0.0:{port}(仅探针)");
        loop {
            let (mut stream, _) = listener.accept().await?;
            let me = Arc::clone(&self);
            tokio::spawn(async move {
                if let Err(e) = me.public_conn(&mut stream).await {
                    tracing::debug!("public conn error: {e}");
                }
            });
        }
    }

    async fn public_conn(&self, stream: &mut tokio::net::TcpStream) -> std::io::Result<()> {
        use tokio::io::AsyncReadExt;
        let mut buf = vec![0u8; 8192];
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }
        let text = String::from_utf8_lossy(&buf[..n]).into_owned();
        let path = text
            .split_whitespace()
            .nth(1)
            .unwrap_or("/")
            .to_string();
        match path.as_str() {
            "/healthz" => {
                let body = json!({
                    "ok": true,
                    "mode": "cluster",
                    "node_id": self.node_id(),
                    "version": env!("CARGO_PKG_VERSION"),
                })
                .to_string();
                reply(stream, 200, &body).await
            }
            "/readyz" => {
                let r = self.ready();
                let code = if r["ready"] == Value::Bool(true) { 200 } else { 503 };
                reply(stream, code, &r.to_string()).await
            }
            p if p.starts_with("/internal/") => {
                reply(
                    stream,
                    403,
                    &json!({ "ok": false, "error": "内部端点不对外暴露(请走 --rpc-port)" })
                        .to_string(),
                )
                .await
            }
            _ => {
                reply(
                    stream,
                    503,
                    &json!({
                        "ok": false,
                        "error": "cluster 模式的业务 API 尚未接线(M1a 骨架);请用 single 模式",
                        "hint": "RDSCTL_MODE=single",
                    })
                    .to_string(),
                )
                .await
            }
        }
    }

    /// 内部 RPC 服务(手写 HTTP,与 agent 同风格)
    pub async fn serve_rpc(self: Arc<Self>, port: u16) -> std::io::Result<()> {
        let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
        tracing::info!("cluster RPC listening on 0.0.0.0:{port}");
        loop {
            let (mut stream, _) = listener.accept().await?;
            let me = Arc::clone(&self);
            tokio::spawn(async move {
                if let Err(e) = me.handle_conn(&mut stream).await {
                    tracing::debug!("rpc conn error: {e}");
                }
            });
        }
    }

    async fn handle_conn(self: &Arc<Self>, stream: &mut tokio::net::TcpStream) -> std::io::Result<()> {
        use tokio::io::AsyncReadExt;
        let mut buf = vec![0u8; 65536];
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }
        let text = String::from_utf8_lossy(&buf[..n]).into_owned();
        let head_end = text.find("\r\n\r\n").map(|p| p + 4).unwrap_or(0);
        let head = &text[..head_end];
        let mut lines = head.split("\r\n");
        let req = lines.next().unwrap_or("").to_string();
        let mut parts = req.split_whitespace();
        let method = parts.next().unwrap_or("").to_uppercase();
        // 注意:请求行里带 query,必须分离后再匹配路由(否则 /internal/lease?x=1 会 404)
        let target = parts.next().unwrap_or("/").to_string();
        let (path, query) = match target.split_once('?') {
            Some((p, q)) => (p.to_string(), q.to_string()),
            None => (target, String::new()),
        };
        let need = lines
            .clone()
            .find(|l| l.to_ascii_lowercase().starts_with("content-length:"))
            .and_then(|l| l.split(':').nth(1))
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(0);
        let mut body = text[head_end.min(n)..].to_string();
        while body.len() < need {
            let mut chunk = [0u8; 8192];
            let r = stream.read(&mut chunk).await?;
            if r == 0 {
                break;
            }
            body.push_str(&String::from_utf8_lossy(&chunk[..r]));
        }
        // 集群内鉴权(可选;未配置 token 视为 lab)
        if let Some(expect) = &self.token {
            let got = lines
                .find(|l| l.to_ascii_lowercase().starts_with("x-cluster-token"))
                .map(|l| l["x-cluster-token".len() + 1..].trim().to_string());
            if got.as_deref() != Some(expect.as_str()) {
                return reply(stream, 403, r#"{"ok":false,"error":"cluster token 不匹配"}"#).await;
            }
        }

        let (status, out) = match (method.as_str(), path.as_str()) {
            ("GET", "/healthz") => (
                200,
                json!({
                    "ok": true,
                    "mode": "cluster",
                    "node_id": self.node_id(),
                    "version": env!("CARGO_PKG_VERSION"),
                })
                .to_string(),
            ),
            ("GET", "/readyz") => {
                let r = self.ready();
                let code = if r["ready"] == Value::Bool(true) { 200 } else { 503 };
                (code, r.to_string())
            }
            ("GET", "/internal/status") => (200, self.status().to_string()),
            // read-index:认证路径需要"leader 视角的已提交水位"来保证线性一致读。
            // 注意:两个水位必须**一次取锁**取出 —— `json!` 里写两次 `self.node.lock()`
            // 会因 parking_lot 不可重入而自死锁(见设计 §19 发现 0 的同类教训)。
            ("GET", "/internal/commit-index") => {
                let (commit, applied) = {
                    let n = self.node.lock();
                    (n.commit_index(), n.applied_index())
                };
                (
                    200,
                    json!({
                        "ok": true,
                        "node_id": self.node_id(),
                        "commit_index": commit,
                        "applied_index": applied,
                    })
                    .to_string(),
                )
            }
            ("GET", "/internal/view") => (
                200,
                json!({
                    "ok": true,
                    "status": self.status(),
                    "ready": self.ready(),
                    "peers": self.peers_view(),
                    "leases": self.leases_view(),
                })
                .to_string(),
            ),
            ("GET", "/internal/state") => {
                let key = qparam(&query, "key").unwrap_or_default();
                match self.kv_view(&key) {
                    Some(v) => (200, json!({ "ok": true, "key": key, "entry": v }).to_string()),
                    None => (404, json!({ "ok": false, "error": "not found" }).to_string()),
                }
            }
            ("GET", "/internal/lease") => {
                let inst = qparam(&query, "instance").unwrap_or_default();
                match self.lease_view(&inst) {
                    Some(v) => (200, json!({ "ok": true, "lease": v }).to_string()),
                    None => (404, json!({ "ok": false, "error": "no lease" }).to_string()),
                }
            }
            ("GET", "/internal/steps") => {
                let t = qparam(&query, "task_id").unwrap_or_default();
                let list = self.steps_of_task(&t);
                (
                    200,
                    json!({ "ok": true, "task_id": t, "count": list.len(), "steps": list })
                        .to_string(),
                )
            }
            ("GET", "/internal/step") => {
                let t = qparam(&query, "task_id").unwrap_or_default();
                let nd = qparam(&query, "node").unwrap_or_default();
                let st: u32 = qparam(&query, "step").unwrap_or_default().parse().unwrap_or(0);
                let idem = qparam(&query, "idem").unwrap_or_default();
                match self.step_view(&t, &nd, st, &idem) {
                    Some(v) => (200, json!({ "ok": true, "step": v }).to_string()),
                    None => (404, json!({ "ok": false, "error": "no step record" }).to_string()),
                }
            }
            ("POST", "/internal/raft") => {
                let v: Value = serde_json::from_str(&body).unwrap_or(json!({}));
                let from = v["from"].as_str().unwrap_or("").to_string();
                match serde_json::from_value::<Message>(v["msg"].clone()) {
                    Ok(msg) => {
                        let outs = self.handle_message(&from, msg);
                        // 异步投递:处理端必须立刻回响应,否则"心跳延迟"会与对端处理耗时耦合
                        // (实测后果:leader 误判失去多数派 → 拒绝一切写入,连自己正在跑的
                        //  任务的步骤账本都写不进去 → 任务 fail-closed 失败)
                        if !outs.is_empty() {
                            let me = Arc::clone(self);
                            tokio::spawn(async move { me.deliver(outs).await });
                        }
                        (200, json!({ "ok": true }).to_string())
                    }
                    Err(e) => (
                        400,
                        json!({ "ok": false, "error": format!("消息解析失败:{e}") }).to_string(),
                    ),
                }
            }
            ("POST", "/internal/stepdown") => {
                // 注意:锁必须在 await 之前释放(guard 跨 await 会让 future 非 Send)
                let res = {
                    let mut node = self.node.lock();
                    let was_leader = node.is_leader();
                    node.step_down().map(|outs| (was_leader, outs))
                };
                match res {
                    Ok((was_leader, outs)) => {
                        self.deliver(outs).await;
                        (
                            200,
                            json!({ "ok": true, "stepped_down": was_leader }).to_string(),
                        )
                    }
                    Err(e) => (
                        500,
                        json!({ "ok": false, "error": e.to_string() }).to_string(),
                    ),
                }
            }
            ("POST", "/internal/propose") => {
                // body 直接是 Op(形如 {"op":"lease_grant","instance":...})
                match serde_json::from_str::<Op>(&body) {
                    Ok(op) => {
                        let r = self.propose_local_only(op).await;
                        match r {
                            Ok((idx, applied)) => (
                                200,
                                json!({ "ok": true, "index": idx, "applied": applied }).to_string(),
                            ),
                            Err(HaError::NotLeader { leader }) => (
                                409,
                                json!({ "ok": false, "error": "not_leader", "leader": leader })
                                    .to_string(),
                            ),
                            Err(HaError::QuorumUnavailable) => (
                                503,
                                json!({ "ok": false, "error": "quorum_unavailable" }).to_string(),
                            ),
                            Err(e) => (500, json!({ "ok": false, "error": e.to_string() }).to_string()),
                        }
                    }
                    Err(e) => (
                        400,
                        json!({ "ok": false, "error": format!("op 解析失败:{e}") }).to_string(),
                    ),
                }
            }
            _ => (404, json!({ "ok": false, "error": format!("未知路径 {path}") }).to_string()),
        };
        reply(stream, status, &out).await
    }

    /// 提案 → 立即投递 → 等待**提交并应用**(内部写入口的完整语义)
    ///
    /// 硬要求:没有多数派时**绝不允许返回成功** —— 否则"接受写入"就是假的可用性。
    /// 因此:①写入前检查 quorum;②超时内未被 apply(未提交)→ 返回 QuorumUnavailable。
    async fn propose_with_flush(self: &Arc<Self>, op: Op) -> HaResult<(u64, Value)> {
        let (idx, outs) = {
            let mut node = self.node.lock();
            if !node.is_leader() {
                return Err(HaError::NotLeader {
                    leader: node.leader_id().map(|s| s.to_string()),
                });
            }
            if !node.quorum_ok(now_ms()) {
                return Err(HaError::QuorumUnavailable);
            }
            let idx = node.propose(op.with_now(now_ms()))?;
            let outs = node.flush();
            (idx, outs)
        };
        // 先把消息投出去,再等待(否则永远等不到多数派)
        self.deliver_flush(outs).await;
        match self.wait_applied_async(idx, 5_000).await {
            Some(a) => Ok((idx, applied_json(a))),
            None => Err(HaError::QuorumUnavailable),
        }
    }

    async fn deliver_flush(self: &Arc<Self>, outs: Vec<Outbound>) {
        // 并发投递:提交延迟 = 最慢 peer 而不是各 peer 之和
        let mut handles = Vec::new();
        for o in outs {
            let Some((ip, port)) = self.table.get(&o.to).cloned() else {
                continue;
            };
            let me = Arc::clone(self);
            // flush 里也可能夹带 InstallSnapshot(peer 落后到快照点之后),预算按消息种类分档
            let budget_ms = deliver_budget_ms(o.msg.kind());
            handles.push(tokio::spawn(async move {
                let body = json!({ "from": me.node_id(), "msg": o.msg });
                let _ = tokio::time::timeout(
                    Duration::from_millis(budget_ms),
                    post_json(
                        &ip,
                        port,
                        "/internal/raft",
                        &body,
                        me.token.as_deref(),
                        budget_ms,
                    ),
                )
                .await;
            }));
        }
        for h in handles {
            let _ = h.await;
        }
    }

    async fn wait_applied_async(&self, index: u64, timeout_ms: u64) -> Option<Applied> {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
        loop {
            {
                let node = self.node.lock();
                if node.applied_index() >= index {
                    return node.result_of(index);
                }
            }
            if tokio::time::Instant::now() > deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

/// 专用桥接 runtime(与主 runtime 隔离,避免在 async 上下文中阻塞)
fn bridge_runtime() -> &'static tokio::runtime::Runtime {
    static RT: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("桥接 runtime 创建失败")
    })
}

/// 把"异步共识调用"适配回同步调用点(实例操作是同步 API)。
///
/// **关键约束:等待绝不能占住 tokio 的 worker 线程**(设计 §19 发现 23)。
/// 这个等待最长可达 `LEASE_BRIDGE_TIMEOUT_MS`(12s);如果它就那么 `recv_timeout`,
/// 那么这个进程的主运行时少一个 worker —— 而同一个进程还**必须**处理
/// leader 发来的 `/internal/raft`(AppendEntries,携带新的 commit index)。
/// worker 被占满时,本副本就"收不到 commit":实测表现为
/// `租约已提交(index=121)但本副本未在 5000ms 内追平(applied=120, commit=120)`,
/// 实例操作被拒(而且它其实已经提交了)。
/// 因此:在多线程运行时里用 `block_in_place`(tokio 会临时补充 worker,池子不被饿死);
/// 非运行时线程(例如直接调用该 API 的单测)照旧阻塞等待。
fn bridge_block<F, T>(fut: F, timeout: Duration) -> Result<T, LeaseError>
where
    F: std::future::Future<Output = Result<T, LeaseError>> + Send + 'static,
    T: Send + 'static,
{
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let out = bridge_runtime().block_on(fut);
        let _ = tx.send(out);
    });
    let wait = || rx.recv_timeout(timeout);
    let in_multi_thread = tokio::runtime::Handle::try_current()
        .map(|h| h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread)
        .unwrap_or(false);
    let res = if in_multi_thread {
        tokio::task::block_in_place(wait)
    } else {
        wait()
    };
    match res {
        Ok(r) => r,
        Err(_) => Err(LeaseError::Internal(
            "共识租约请求超时(桥接等待超时;等待期间不占 tokio worker)".into(),
        )),
    }
}

fn applied_json(a: Applied) -> Value {
    match a {
        Applied::Ok => json!({ "status": "applied" }),
        Applied::Rejected(r) => json!({ "status": "rejected", "reason": r.to_string() }),
        Applied::StepAlreadyDone { result } => {
            json!({ "status": "step_already_done", "result": result })
        }
    }
}

/// lab 放行:显式声明"前提未验证也允许启动",但 readyz 会一直标记未就绪
fn lab_override_allows(check: &SelfCheck) -> bool {
    let allow_clock = std::env::var("RDSCTL_PREFLIGHT_ALLOW_UNVERIFIED_CLOCK").as_deref() == Ok("1");
    let allow_agent = std::env::var("RDSCTL_ALLOW_NO_AGENT").as_deref() == Ok("1");
    let allow_network = std::env::var("RDSCTL_ALLOW_SLOW_NETWORK").as_deref() == Ok("1");
    // 结构性前提(配置/数据目录/fsync)永不放行
    check.voters_ok
        && check.data_dir_ok
        && check.fsync_ok
        && (!check.clock_verified && allow_clock || check.clock_verified)
        && (!check.agent_fence_ok && allow_agent || check.agent_fence_ok)
        && (!check.network_ok && allow_network || check.network_ok)
}

/// 实测到某 peer 的**单次 RPC 往返**(ms):TCP 连接 + HTTP 请求/响应,与真实共识投递同构
/// (含建连,因为共识路径当前**每条消息新建连接**)。失败返回 None。
fn probe_peer_rpc_rtt_ms(ip: &str, port: u16, token: Option<&str>, timeout_ms: u64) -> Option<u64> {
    use std::io::{Read, Write};
    use std::net::ToSocketAddrs;
    let sa = (ip, port).to_socket_addrs().ok()?.next()?;
    let t0 = std::time::Instant::now();
    let mut s = std::net::TcpStream::connect_timeout(&sa, Duration::from_millis(timeout_ms)).ok()?;
    let _ = s.set_read_timeout(Some(Duration::from_millis(timeout_ms)));
    let _ = s.set_write_timeout(Some(Duration::from_millis(timeout_ms)));
    let mut req = format!("GET /healthz HTTP/1.1\r\nHost: {ip}\r\nConnection: close\r\n");
    if let Some(t) = token {
        req.push_str(&format!("X-Cluster-Token: {t}\r\n"));
    }
    req.push_str("\r\n");
    s.write_all(req.as_bytes()).ok()?;
    let mut buf = [0u8; 256];
    if s.read(&mut buf).ok()? == 0 {
        return None;
    }
    Some(t0.elapsed().as_millis() as u64)
}

/// 前提 A7:实测对端 RPC 往返,校验 **选举超时下限 ≥ 4 × 实测往返**。
///
/// 为什么是这项判据:选举超时是"允许丢几拍心跳"的容错预算。跨区部署时单次往返可达数百毫秒,
/// 若时间参数不随之放大,就会表现为心跳间歇性迟到 ⇒ 频繁误选举 ⇒ 选主风暴(设计 §19)。
///
/// 只看**已响应**的 peer:一个都测不到时(首次引导/滚动升级期间对端未起)不阻断 ——
/// 那个场景由 preflight 第 4 项"多数派可达"负责,不在这里重复判失败。
fn measure_peer_network(
    cfg: &RaftConfig,
    peers: Option<&MemberTable>,
    token: Option<&str>,
) -> (bool, Vec<String>) {
    let Some(t) = peers else {
        return (true, vec!["A7 未测量:未提供成员表".to_string()]);
    };
    let budget = (cfg.election_timeout_ms.0 / 4).max(1);
    let mut notes = Vec::new();
    let mut ok = true;
    let mut measured = 0usize;
    for (id, (ip, port)) in t.addrs.iter() {
        if id == &cfg.node_id {
            continue;
        }
        let mut best: Option<u64> = None;
        let mut reached = 0usize;
        for _ in 0..3 {
            if let Some(ms) = probe_peer_rpc_rtt_ms(ip, *port, token, 3_000) {
                reached += 1;
                best = Some(best.map_or(ms, |b| b.min(ms)));
            }
        }
        if reached == 0 {
            continue; // 对端未起:不在此判失败(见函数注释)
        }
        measured += 1;
        let ms = best.unwrap_or(0);
        if ms > budget {
            ok = false;
            notes.push(format!(
                "A7:peer {id} 实测 RPC 往返 {ms}ms > 选举超时下限的四分之一({budget}ms);\
                 请把 RDSCTL_ELECTION_TIMEOUT_MS 放大到 ≥ {}ms(跨区建议 ≥ {}ms)",
                ms.saturating_mul(4).max(1_500),
                ms.saturating_mul(8).max(3_000)
            ));
        }
        if reached < 3 {
            notes.push(format!(
                "A7:peer {id} 三次探测仅 {reached} 次成功(WAN 丢包会放大成误选举,建议排查链路)"
            ));
        }
    }
    if measured == 0 {
        notes.push(
            "A7 未测量:无对端响应(首次引导/升级期间属预期;上线后必须复测——\
             跨区部署的时间参数必须按实测 RTT 定档)"
                .into(),
        );
    }
    (ok, notes)
}

/// 探测本机 NTP 同步状态(与 deploy 脚本同判据;无法判定返回 None)
fn detect_clock_sync() -> Option<bool> {
    use std::process::Command;
    if let Ok(out) = Command::new("chronyc").arg("tracking").output() {
        if out.status.success() {
            let text = String::from_utf8_lossy(&out.stdout).to_string();
            let leap = text
                .lines()
                .find(|l| l.starts_with("Leap status"))
                .and_then(|l| l.split(':').nth(1))
                .map(|s| s.trim().to_string());
            if let Some(l) = leap {
                return Some(l == "Normal");
            }
        }
    }
    if let Ok(out) = Command::new("timedatectl")
        .args(["show", "-p", "NTPSynchronized", "--value"])
        .output()
    {
        if out.status.success() {
            let v = String::from_utf8_lossy(&out.stdout).trim().to_string();
            if !v.is_empty() {
                return Some(v == "yes");
            }
        }
    }
    if let Ok(out) = Command::new("ntpq").arg("-p").output() {
        if out.status.success() {
            return Some(true); // 可达即视为已同步(与脚本同口径的弱判据)
        }
    }
    None
}

fn probe_agent_fence(url: &str) -> Option<bool> {
    let (host, port) = parse_http_base(url)?;
    // 必须带 token:agent 配置了 RDSCTL_AGENT_TOKEN 时,无 token 的 ping 返回 403,
    // 会被误判成"agent 不可达"→ A4 不满足 → 集群拒绝启动(实施期实测踩到)。
    let mut path = "/agent/ping".to_string();
    if let Ok(t) = std::env::var("RDSCTL_AGENT_TOKEN") {
        if !t.trim().is_empty() {
            path = format!("/agent/ping?token={}", t.trim());
        }
    }
    let body = ureq_get(&host, port, &path)?;
    let v: Value = serde_json::from_str(&body).ok()?;
    Some(v["ok"] == Value::Bool(true) && v["fence_capable"] == Value::Bool(true))
}

fn parse_http_base(base: &str) -> Option<(String, u16)> {
    let rest = base
        .strip_prefix("http://")
        .or_else(|| base.strip_prefix("https://"))?
        .split('/')
        .next()?;
    let (h, p) = rest.rsplit_once(':')?;
    Some((h.to_string(), p.parse().ok()?))
}

/// 同步 GET(仅启动自检使用:此时还没有 tokio 任务需要它)
fn ureq_get(host: &str, port: u16, path: &str) -> Option<String> {
    use std::io::{Read, Write};
    let mut s = std::net::TcpStream::connect((host, port)).ok()?;
    s.set_read_timeout(Some(Duration::from_secs(3))).ok()?;
    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n"
    );
    s.write_all(req.as_bytes()).ok()?;
    let mut buf = String::new();
    s.read_to_string(&mut buf).ok()?;
    buf.split_once("\r\n\r\n").map(|(_, b)| b.to_string())
}

/// 把一次成员探测的四段原始视图(status/ready/peers/leases)拼成统一的成员对象。
///
/// 重叠字段(role/term/leader/commit/applied/ready/前提)一律取 `ready`(聚合端最后一次取样),
/// 保证同一份 JSON 内不自相矛盾;`status` 只提供它独有的字段(拓扑规模/快照/数据目录)。
fn member_view(
    id: String,
    addr: String,
    is_self: bool,
    status: &Value,
    ready: &Value,
    peers: &Value,
    leases: &Value,
) -> Value {
    json!({
        "id": id,
        "addr": addr,
        "is_self": is_self,
        "reachable": true,
        "error": Value::Null,
        "shard": ready["shard"],
        "role": ready["role"],
        "term": ready["term"],
        "leader": ready["leader"],
        "commit_index": ready["commit_index"],
        "applied_index": ready["applied_index"],
        "snapshot_index": status["snapshot_index"],
        "kv_len": status["kv_len"],
        "config_epoch": status["config_epoch"],
        "data_dir": status["data_dir"],
        "voters": status["voters"],
        "ready": ready["ready"],
        "degraded_reason": ready["degraded_reason"],
        "degraded_reasons": ready["degraded_reasons"],
        "quorum_ok": ready["quorum_ok"],
        "log_writable": ready["log_writable"],
        "fsync_ok": ready["fsync_ok"],
        "premises_ok": ready["premises_ok"],
        "premises_unverified": ready["premises_unverified"],
        "lab_degraded": ready["lab_degraded"],
        "skew_measured_ms": ready["skew_measured_ms"],
        "uptime_ms": ready["uptime_ms"],
        "delivered": ready["delivered"],
        "transport_errors": ready["transport_errors"],
        "peers": peers,
        "lease_count": leases.as_array().map(|a| a.len()).unwrap_or(0),
        "leases": leases,
    })
}

/// 新会话 token(明文,只回给浏览器;状态机里只存哈希)。
/// 零新 crate:复用 SHA-256 做 128 位随机派生(时间 + 计数器 + 进程 + 栈地址熵)。
pub fn new_session_token() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let a = &N as *const _ as usize;
    let mut seed = format!("{t}:{n}:{a}:{}", std::process::id());
    // 叠加一个进程内随机源(避免同一纳秒内并发登录产生相同 token)
    for _ in 0..4 {
        seed.push_str(&format!(":{:?}", std::time::Instant::now()));
    }
    crate::sha256::to_hex(&crate::sha256::digest(seed.as_bytes()))
}

/// 手写 HTTP JSON GET(集群内部只读 RPC;供管控集群页的成员探测使用)
async fn get_json(
    host: &str,
    port: u16,
    path: &str,
    token: Option<&str>,
    timeout_ms: u64,
) -> Result<Value, String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut head = format!("GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n");
    if let Some(t) = token {
        head.push_str(&format!("X-Cluster-Token: {t}\r\n"));
    }
    head.push_str("\r\n");
    let fut = async {
        let mut s = tokio::net::TcpStream::connect((host, port)).await?;
        s.write_all(head.as_bytes()).await?;
        s.flush().await?;
        let mut buf = Vec::new();
        let mut chunk = [0u8; 8192];
        loop {
            let r = s.read(&mut chunk).await?;
            if r == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..r]);
        }
        Ok::<Vec<u8>, std::io::Error>(buf)
    };
    let raw = tokio::time::timeout(Duration::from_millis(timeout_ms), fut)
        .await
        .map_err(|_| format!("探测超时({timeout_ms}ms)"))?
        .map_err(|e| format!("连接失败: {e}"))?;
    let text = String::from_utf8_lossy(&raw).into_owned();
    let status: u16 = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let body_text = {
        let head_end = text.find("\r\n\r\n").map(|p| p + 4).unwrap_or(0);
        text[head_end.min(text.len())..].to_string()
    };
    if status == 403 {
        return Err("cluster token 不匹配".into());
    }
    if status != 200 {
        return Err(format!("HTTP {status}"));
    }
    serde_json::from_str::<Value>(&body_text).map_err(|e| format!("响应非 JSON: {e}"))
}

/// 手写 HTTP JSON POST(集群内部 RPC)
///
/// `timeout_ms` 必须由调用方给出:普通共识消息用 `DELIVER_TIMEOUT_MS`(3s),
/// 而整份快照的 InstallSnapshot 是**一条**长消息,3s 预算下跨区永远传不完
/// (设计 §20 跨区发现)。这里不做"内部默认值",避免调用点忘记给预算。
async fn post_json(
    host: &str,
    port: u16,
    path: &str,
    payload: &Value,
    token: Option<&str>,
    timeout_ms: u64,
) -> Result<Value, String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let body = payload.to_string();
    let mut head = format!(
        "POST {path} HTTP/1.1\r\nHost: {host}\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    if let Some(t) = token {
        head.push_str(&format!("X-Cluster-Token: {t}\r\n"));
    }
    head.push_str("\r\n");
    let fut = async {
        let mut s = tokio::net::TcpStream::connect((host, port)).await?;
        s.write_all(head.as_bytes()).await?;
        s.write_all(body.as_bytes()).await?;
        s.flush().await?;
        let mut buf = Vec::new();
        let mut chunk = [0u8; 8192];
        loop {
            let r = s.read(&mut chunk).await?;
            if r == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..r]);
        }
        Ok::<Vec<u8>, std::io::Error>(buf)
    };
    let raw = tokio::time::timeout(Duration::from_millis(timeout_ms), fut)
        .await
        .map_err(|_| format!("RPC 超时({timeout_ms}ms)"))?
        .map_err(|e| format!("RPC 连接失败: {e}"))?;
    let text = String::from_utf8_lossy(&raw).into_owned();
    let head_end = text.find("\r\n\r\n").map(|p| p + 4).unwrap_or(0);
    let status: u16 = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let body_text = text[head_end.min(text.len())..].to_string();
    if status == 403 {
        return Err("cluster token 不匹配".into());
    }
    serde_json::from_str(body_text.trim()).map_err(|e| format!("RPC 响应解析失败:{e}"))
}

async fn reply(
    stream: &mut tokio::net::TcpStream,
    status: u16,
    body: &str,
) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;
    let reason = match status {
        200 => "OK",
        403 => "Forbidden",
        404 => "Not Found",
        409 => "Conflict",
        503 => "Service Unavailable",
        _ => "Error",
    };
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
}

fn qparam(query: &str, key: &str) -> Option<String> {
    query.split('&').find_map(|kv| {
        let (k, v) = kv.split_once('=')?;
        (k == key).then(|| v.to_string())
    })
}

fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 发现 23 回归:桥接等待**不得占住 tokio worker**。
    ///
    /// 为什么这条重要:等待最长 12s,而同一个进程还必须处理 leader 发来的
    /// `/internal/raft`(AppendEntries,带新 commit index)。worker 被占住时本副本"收不到 commit",
    /// 于是已提交的租约操作被报成"未追平"(实测事故:实例操作被拒、可安全重试但用户看到失败)。
    ///
    /// 判据:在**单 worker** 的多线程运行时里,用一个"等另一个任务把标志置位"的 future 做桥接。
    /// 若等待占住 worker,那个任务永远跑不起来 → future 只能超时返回 0。
    #[test]
    fn bridge_wait_does_not_starve_the_runtime_worker() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1) // 关键:只有一个 worker,占住就是死
            .enable_all()
            .build()
            .expect("build rt");
        rt.block_on(async {
            let during = Arc::new(AtomicBool::new(false));
            let d2 = Arc::clone(&during);
            // 关键:桥接调用必须发生在**运行时任务里**(= 真实情况:HTTP handler 中同步调用),
            // 这样等待才会占住一个 worker。在 `block_on` 的调用线程上直接调则测不出问题。
            let waiter = tokio::spawn(async move {
                let r: Result<u32, LeaseError> = bridge_block(
                    async move {
                        for _ in 0..400 {
                            if during.load(Ordering::SeqCst) {
                                return Ok(1);
                            }
                            tokio::time::sleep(Duration::from_millis(5)).await;
                        }
                        Ok(0) // 等待期间拿不到 worker:标志始终没被置位
                    },
                    Duration::from_millis(5000),
                );
                r
            });
            // 模拟"必须被调度的 AppendEntries 处理":它跑不动 ⇒ 本副本永远学不到新 commit
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(60)).await;
                d2.store(true, Ordering::SeqCst);
            });
            let r = waiter.await.expect("join").expect("bridge_block 不应超时");
            assert_eq!(
                r, 1,
                "桥接等待期间其他任务必须能运行(否则本副本无法处理 AppendEntries,表现为\"已提交但未追平\")"
            );
        });
    }

    #[test]
    fn member_table_parses_and_rejects_bad_spec() {
        let t = MemberTable::parse("n1@127.0.0.1:9331,n2@10.0.0.2:9330").unwrap();
        assert_eq!(t.ids(), vec!["n1".to_string(), "n2".to_string()]);
        assert_eq!(t.get("n1"), Some(&("127.0.0.1".to_string(), 9331)));
        assert!(MemberTable::parse("n1-127.0.0.1:9331").is_err(), "缺 @ 必须报错");
        assert!(MemberTable::parse("n1@127.0.0.1").is_err(), "缺端口必须报错");
        assert!(MemberTable::parse("n1@127.0.0.1:abc").is_err(), "端口非法必须报错");
        assert!(MemberTable::parse("n1@1.1.1.1:1,n1@2.2.2.2:2").is_err(), "id 重复必须报错");
        assert!(MemberTable::parse("").is_err(), "空表必须报错");
    }

    /// F7/A1:实测偏移超界时,**拒绝授予新租约**(且该检查先于 leader 判定,
    /// 保证任何节点都不会在偏移不可信时发放租约)。
    #[tokio::test]
    async fn skewed_node_refuses_new_lease_grants() {
        let dir = std::env::temp_dir().join(format!(
            "rdsctl-ha-skew-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let mut cfg = RaftConfig::new(
            "n1",
            0,
            vec!["n1".into(), "n2".into(), "n3".into()],
        );
        cfg.max_skew_ms = 1000;
        let table = MemberTable::parse("n1@127.0.0.1:1,n2@127.0.0.1:2,n3@127.0.0.1:3").unwrap();
        let rt = ClusterRuntime::start_for_test(cfg, table, &dir).unwrap();

        // 观测到 5×max_skew 的偏移
        rt.inject_peer_clock_offset("n2", 5_000);
        assert_eq!(rt.node.lock().skew_measured_ms(), Some(5_000));
        assert!(rt.node.lock().skew_exceeded());

        let err = rt.acquire_lease("i1", "h1", 30_000).await.unwrap_err();
        assert!(
            matches!(err, LeaseError::Skew(5000)),
            "超界时必须拒绝授予租约,实际:{err:?}"
        );
        let err = rt.renew_lease("i1", "h1").await.unwrap_err();
        assert!(matches!(err, LeaseError::Skew(_)), "续约同样必须拒绝:{err:?}");

        // readyz 必须显式降级并标注 A1 未验证
        let r = rt.ready();
        assert_eq!(r["skew_measured_ms"], 5000);
        assert_eq!(r["ready"], false);
        let reasons: Vec<String> = r["degraded_reasons"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();
        assert!(
            reasons.contains(&"skew_exceeded".to_string()),
            "降级原因必须包含 skew_exceeded:{r}"
        );
        let unv: Vec<String> = r["premises_unverified"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();
        assert!(unv.contains(&"A1_clock".to_string()), "{r}");

        // 偏移回到界内 → 重新可授予(机制自愈)
        rt.inject_peer_clock_offset("n2", 100);
        assert!(!rt.node.lock().skew_exceeded());
        let err = rt.acquire_lease("i1", "h1", 30_000).await.unwrap_err();
        assert!(
            !matches!(err, LeaseError::Skew(_)),
            "偏移恢复后不应再因 skew 被拒:{err:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn self_check_rejects_even_voters_and_unverifiable_premises() {
        let dir = std::env::temp_dir().join(format!("rdsctl-ha-selfcheck-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        // 偶数 voter → 结构前提不达标(不可被 lab 放行)
        let cfg = RaftConfig::new("n1", 0, vec!["n1".into(), "n2".into()]);
        let c = ClusterRuntime::self_check(&cfg, &dir, None, None, None);
        assert!(!c.voters_ok);
        assert!(!c.all_ok);
        assert!(!lab_override_allows(&c), "结构性问题不得被 lab 放行");
        // 奇数 voter + 无 agent + 未放行时钟 → 不达标但可 lab 放行
        let cfg2 = RaftConfig::new("n1", 0, vec!["n1".into(), "n2".into(), "n3".into()]);
        let c2 = ClusterRuntime::self_check(&cfg2, &dir, None, None, None);
        assert!(c2.voters_ok && c2.data_dir_ok && c2.fsync_ok);
        assert!(!c2.clock_verified && !c2.agent_fence_ok);
        assert!(!c2.all_ok);
        assert!(c2.failures().iter().any(|f| f.contains("A1")));
        assert!(c2.failures().iter().any(|f| f.contains("A4")));
    }

    /// 跨区回归:心跳的"在途抑制"必须**按 peer**,不能是全局开关。
    ///
    /// 旧实现用一个全局 `delivering` 布尔:一个慢 peer(跨区 RTT 大,或正在传快照)会把标记
    /// 一直占住,于是**同一轮对健康 peer 的心跳也被跳过** → 健康 follower 选举超时 → 选主风暴。
    #[test]
    fn inflight_suppression_is_per_peer_and_never_delays_elections() {
        let dir = std::env::temp_dir().join(format!("rdsctl-inflight-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let table = MemberTable::parse("n1@127.0.0.1:1,n2@127.0.0.1:2,n3@127.0.0.1:3").unwrap();
        let cfg = RaftConfig::new("n1", 0, table.ids());
        let rt = ClusterRuntime::start_for_test(cfg, table, &dir).expect("start");

        let append = |to: &str| Outbound {
            to: to.to_string(),
            msg: Message::AppendEntries {
                term: 1,
                leader: "n1".into(),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
                sender_ms: 0,
                peer_skew_ms: 0,
            },
            critical: true,
        };

        // 首轮:两个 peer 的心跳都要发出去,并标记在途
        let keep = rt.claim_outbound(vec![append("n2"), append("n3")]);
        assert_eq!(keep.len(), 2, "不同 peer 的心跳互不抑制");

        // 第二轮:两个都还在途 ⇒ 均跳过(Raft 对丢失容错,下一拍重发)
        assert!(rt.claim_outbound(vec![append("n2"), append("n3")]).is_empty());

        // 关键:n2 投递完成、n3 仍卡住 ⇒ 只放开 n2,n3 继续被抑制
        rt.release_outbound("n2", "append_entries");
        let keep = rt.claim_outbound(vec![append("n2"), append("n3")]);
        assert_eq!(keep.len(), 1);
        assert_eq!(
            keep[0].to, "n2",
            "卡住的 peer 不得连带抑制健康 peer 的心跳(否则健康 follower 会选主)"
        );

        // 选举类消息**一律不抑制**:抑制它会直接推迟选举(可用性损失)
        let vote = Outbound {
            to: "n3".to_string(),
            msg: Message::RequestVote {
                term: 2,
                candidate: "n1".into(),
                last_log_index: 0,
                last_log_term: 0,
                pre_vote: true,
                sender_ms: 1,
            },
            critical: true,
        };
        let keep = rt.claim_outbound(vec![vote]);
        assert_eq!(keep.len(), 1, "RequestVote 不得被在途抑制挡住");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 跨区回归:前提 A7 —— 实测 RPC 往返超出"选举超时下限/4"时,启动自检必须不达标;
    /// 把选举超时按实测 RTT 放大后,同一条链路必须达标(否则部署方只会看到"莫名其妙拒绝启动")。
    #[test]
    fn slow_peer_network_fails_self_check_until_timeouts_are_widened() {
        use std::io::{Read, Write};
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        std::thread::spawn(move || {
            for s in l.incoming() {
                let mut s = match s {
                    Ok(s) => s,
                    Err(_) => break,
                };
                let mut b = [0u8; 1024];
                let _ = s.read(&mut b);
                std::thread::sleep(Duration::from_millis(200)); // 模拟跨区 RPC 往返
                let _ = s.write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                );
            }
        });
        let dir = std::env::temp_dir().join(format!("rdsctl-net-pre-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let table =
            MemberTable::parse(&format!("n1@127.0.0.1:1,n2@127.0.0.1:{port},n3@127.0.0.1:1"))
                .unwrap();

        // 选举下限 400ms ⇒ 预算 100ms < 实测 ~200ms ⇒ A7 不达标
        let mut cfg = RaftConfig::new("n1", 0, table.ids());
        cfg.election_timeout_ms = (400, 800);
        cfg.heartbeat_ms = 100;
        let c = ClusterRuntime::self_check(&cfg, &dir, None, Some(&table), None);
        assert!(!c.network_ok, "实测往返超预算时 A7 必须不达标");
        assert!(!c.all_ok);
        assert!(c.failures().iter().any(|f| f.contains("A7")));
        assert!(
            c.notes.iter().any(|n| n.starts_with("A7") && n.contains("peer n2")),
            "必须给出可执行的修法(实测值 + 建议的时间参数):{:?}",
            c.notes
        );

        // 按实测放大选举超时(4000ms ⇒ 预算 1000ms)⇒ 同一条链路达标
        let mut cfg2 = RaftConfig::new("n1", 0, table.ids());
        cfg2.election_timeout_ms = (4_000, 8_000);
        cfg2.heartbeat_ms = 300;
        let c2 = ClusterRuntime::self_check(&cfg2, &dir, None, Some(&table), None);
        assert!(c2.network_ok, "放大时间参数后必须达标:{:?}", c2.notes);

        // 未测量(无对端响应)不算不达标:首次引导期间对端本就还没起
        let cold = MemberTable::parse("n1@127.0.0.1:1,n2@127.0.0.1:1,n3@127.0.0.1:1").unwrap();
        let c3 = ClusterRuntime::self_check(&cfg, &dir, None, Some(&cold), None);
        assert!(c3.network_ok, "对端全未起时不得阻断(由 preflight 第 4 项负责)");
        assert!(c3.notes.iter().any(|n| n.contains("A7 未测量")));

        let _ = std::fs::remove_dir_all(&dir);
    }
}
