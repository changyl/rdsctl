// rdsctl — 管控面 HA 正确性内核(见 docs/control-plane-ha-design.md)
//
// 模块划分:
//   auth.rs     会话与 RBAC 的共识权威表示(键空间 + 记录 + 口令/权限展开;设计 §11.4)
//   clock.rs    时钟抽象(真实时钟 / 手工时钟;仿真注入偏移与停顿)
//   log.rs      本地持久日志(记录格式 + 哈希链 + fsync + 尾部截断容忍)
//   snapshot.rs 快照(原子写 + 版本 + 恢复)
//   state.rs    状态机(KV 带版本 CAS、租约、队列索引、步骤账本)
//   raft.rs     分片组共识核心(选举/提交/日志复制;与传输层解耦,可确定性仿真)
//
// 设计约束(与仓库既有风格一致):
//   - 零新 crate:SHA-256 复用 src/sha256.rs,持久化用 std::fs,网络走手写 HTTP;
//   - 与传输/时钟解耦,使 S1–S5 仿真可确定性重放(单线程事件驱动);
//   - 任何"看起来就绪"但前提不达标的路径都不允许存在(退出码 2 与 deploy 自检对齐)。

// M1a 实施期:内核按"先正确性、后接线"顺序落地,接线完成前有部分 API 尚未被调用。
// 该注解与 src/orch.rs:9 的骨架期做法一致;接线完成后移除。
#![allow(dead_code)]

pub mod auth;
pub mod clock;
pub mod fence;
pub mod log;
pub mod projection;
pub mod raft;
pub mod runtime;
pub mod snapshot;
pub mod state;

use std::fmt;

/// 内核错误。语义分类刻意保持少而明确,便于上层映射到 HTTP/退出码。
#[derive(Debug)]
pub enum HaError {
    /// 本地 I/O 或 fsync 失败(前提 A2 受损)
    Io(std::io::Error),
    /// 日志/快照损坏(哈希链断裂、magic/ver 不符):必须拒绝启动
    Corrupt(String),
    /// 配置非法(前提 A3:奇数 voter、成员表一致等)
    Config(String),
    /// 日志尾部不完整记录已被丢弃(告警级,非致命)
    TailTruncated { dropped_bytes: u64 },
    /// 本节点不是该分片组的 leader
    NotLeader { leader: Option<String> },
    /// 失去多数派(写必须 fail-closed)
    QuorumUnavailable,
    /// 集群内通信失败
    Transport(String),
}

impl fmt::Display for HaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HaError::Io(e) => write!(f, "本地 I/O 失败: {e}"),
            HaError::Corrupt(m) => write!(f, "日志/快照损坏: {m}"),
            HaError::Config(m) => write!(f, "集群配置非法: {m}"),
            HaError::TailTruncated { dropped_bytes } => {
                write!(f, "日志尾部不完整记录已丢弃({dropped_bytes} 字节)")
            }
            HaError::NotLeader { leader } => match leader {
                Some(l) => write!(f, "本节点不是 leader(当前 leader: {l})"),
                None => write!(f, "本节点不是 leader 且当前无 leader"),
            },
            HaError::QuorumUnavailable => write!(f, "失去多数派,写操作 fail-closed"),
            HaError::Transport(m) => write!(f, "集群内通信失败: {m}"),
        }
    }
}

impl std::error::Error for HaError {}

impl From<std::io::Error> for HaError {
    fn from(e: std::io::Error) -> Self {
        HaError::Io(e)
    }
}

pub type HaResult<T> = Result<T, HaError>;

/// fence token:单调的 (shard, term, index)。执行面据此拒绝过期持有者的命令(设计 §5.5)。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct Fence {
    pub shard: u16,
    pub term: u64,
    pub index: u64,
}

impl Fence {
    pub const fn new(shard: u16, term: u64, index: u64) -> Self {
        Self { shard, term, index }
    }

    /// 线格式 `<shard>:<term>:<index>`(agent 的 X-Rdsctl-Fence 头)
    pub fn wire(&self) -> String {
        format!("{}:{}:{}", self.shard, self.term, self.index)
    }

    /// 解析线格式;非法返回 None(调用方按 fence 缺失处理,不得当作"通过")
    pub fn parse(s: &str) -> Option<Self> {
        let mut it = s.trim().split(':');
        let shard = it.next()?.parse().ok()?;
        let term = it.next()?.parse().ok()?;
        let index = it.next()?.parse().ok()?;
        if it.next().is_some() {
            return None;
        }
        Some(Self { shard, term, index })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fence_ordering_is_lexicographic_by_term_then_index() {
        let a = Fence::new(0, 1, 9);
        let b = Fence::new(0, 1, 10);
        let c = Fence::new(0, 2, 1);
        assert!(a < b && b < c, "fence 必须按 (term, index) 单调");
    }

    #[test]
    fn fence_wire_roundtrip_and_reject_malformed() {
        let f = Fence::new(3, 7, 42);
        assert_eq!(f.wire(), "3:7:42");
        assert_eq!(Fence::parse("3:7:42"), Some(f));
        assert_eq!(Fence::parse(" 3:7:42 "), Some(f));
        assert_eq!(Fence::parse("3:7"), None);
        assert_eq!(Fence::parse("3:7:42:1"), None);
        assert_eq!(Fence::parse("a:7:42"), None);
        assert_eq!(Fence::parse(""), None);
    }
}
