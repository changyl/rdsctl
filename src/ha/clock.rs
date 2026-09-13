// rdsctl HA — 时钟抽象
//
// 为什么需要抽象:设计 §5.4 规定租约的时间比较**不依赖各副本本地时钟**,而仿真(S3)
// 又必须能注入 5×max_skew 的偏移与长时间停顿。因此内核内所有"现在几点"都经本 trait,
// 生产用 SystemClock,测试/仿真用 ManualClock。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// 毫秒级单调(非严格)时钟源
pub trait Clock: Send + Sync {
    /// 自 UNIX 纪元起的毫秒数(与 lease 过期比较口径一致)
    fn now_ms(&self) -> u64;

    /// 面向仿真的时钟推进能力;真实时钟为 no-op。
    fn advance_ms(&self, _ms: u64) {}

    /// 是否为确定性(仿真)时钟 —— 用于在日志/审计中标注
    fn deterministic(&self) -> bool {
        false
    }
}

/// 生产时钟
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
}

/// 手工时钟(仿真/测试):可任意设置与推进
#[derive(Clone)]
pub struct ManualClock {
    now: Arc<AtomicU64>,
}

impl ManualClock {
    pub fn new(start_ms: u64) -> Self {
        Self {
            now: Arc::new(AtomicU64::new(start_ms)),
        }
    }

    pub fn set(&self, ms: u64) {
        self.now.store(ms, Ordering::SeqCst);
    }

    /// 从共享句柄访问(仿真中各节点共享同一台时钟,以模拟"同一物理时间轴")
    pub fn handle(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.now)
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> u64 {
        self.now.load(Ordering::SeqCst)
    }

    fn advance_ms(&self, ms: u64) {
        self.now.fetch_add(ms, Ordering::SeqCst);
    }

    fn deterministic(&self) -> bool {
        true
    }
}

/// 便捷:共享时钟句柄
pub type SharedClock = Arc<dyn Clock>;

pub fn system_clock() -> SharedClock {
    Arc::new(SystemClock)
}

pub fn manual_clock(start_ms: u64) -> SharedClock {
    Arc::new(ManualClock::new(start_ms))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manual_clock_advances_and_is_shared() {
        let c = ManualClock::new(1_000);
        let shared: SharedClock = Arc::new(c.clone());
        assert_eq!(shared.now_ms(), 1_000);
        shared.advance_ms(250);
        assert_eq!(c.now_ms(), 1_250, "同一台时钟的多个句柄必须看到同样的时间");
        assert!(shared.deterministic());
    }

    #[test]
    fn system_clock_is_monotonic_enough_for_lease_math() {
        let c = SystemClock;
        let a = c.now_ms();
        let b = c.now_ms();
        assert!(b >= a);
        assert!(!c.deterministic());
    }
}
