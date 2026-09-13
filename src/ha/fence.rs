// rdsctl HA — 执行面 fence 强制点(设计 §5.5 / §9.2)
//
// 为什么必须在**执行面**强制:进程内自检挡不住僵尸进程(GC 停顿、分区恢复后的旧 holder、
// 被 SIGCONT 唤醒的旧副本)。只有"资源侧"—— 真正调 docker/SQL 的那一端 —— 拒绝过期 fence,
// 才能保证 C3(fence 单调且在资源侧强制)。
//
// 语义:
//   - `check_and_raise(key, incoming)`:incoming < seen → 拒绝(409 fence_stale);
//     incoming > seen → **先落盘并 fsync,再允许执行**;相同 → 幂等放行(重发安全);
//   - shard 不同的 fence 不可比较 → 一律拒绝(容器换了分片归属必须显式重建);
//   - 幂等缓存:`idem_get/idem_put`,重复请求直接回放首次结果(与步骤账本互补)。
//
// 键:调用方用容器名(agent 只认知容器)。键会被消毒,禁止路径穿越。

use std::io::Write;
use std::path::{Path, PathBuf};

use super::Fence;

/// 过期 fence 拒绝详情(agent 以 409 fence_stale 返回)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaleFence {
    pub key: String,
    pub seen: Option<Fence>,
    pub incoming: Fence,
    pub reason: &'static str,
}

impl std::fmt::Display for StaleFence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.seen {
            Some(s) => write!(
                f,
                "fence_stale[{}]:incoming={} 低于或不可比于已见 {} ({})",
                self.key,
                self.incoming.wire(),
                s.wire(),
                self.reason
            ),
            None => write!(f, "fence_stale[{}]:{}", self.key, self.reason),
        }
    }
}

impl std::error::Error for StaleFence {}

/// 执行面守卫:每个宿主机一个实例(键空间 = 该机容器)
#[derive(Debug, Clone)]
pub struct ExecutorGuard {
    root: PathBuf,
}

impl ExecutorGuard {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        Self { root }
    }

    /// 由环境推导数据目录:`RDSCTL_AGENT_DATA_DIR` → `$RDSCTL_DATA_DIR/agent` → `./logs/ha/agent`
    pub fn from_env() -> Self {
        let root = std::env::var("RDSCTL_AGENT_DATA_DIR")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .or_else(|| {
                std::env::var("RDSCTL_DATA_DIR")
                    .ok()
                    .filter(|s| !s.trim().is_empty())
                    .map(|d| format!("{}/agent", d.trim_end_matches('/')))
            })
            .unwrap_or_else(|| "./logs/ha/agent".to_string());
        Self::new(root)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// 消毒键:仅保留 [A-Za-z0-9._-],其余替换为 '_'(防路径穿越)
    fn safe_key(key: &str) -> String {
        let cleaned: String = key
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        // 纯点名(., ..)必须再兜一层
        if cleaned.is_empty() || cleaned.chars().all(|c| c == '.') {
            return format!("k{}", crate::sha256::to_hex(&crate::sha256::digest(key.as_bytes()))[..16].to_string());
        }
        cleaned
    }

    fn seen_path(&self, key: &str) -> PathBuf {
        self.root.join("fence_seen").join(Self::safe_key(key))
    }

    fn idem_path(&self, idem: &str) -> PathBuf {
        self.root.join("idem").join(Self::safe_key(idem))
    }

    /// 读取已见 fence(内存无关,始终以落盘为准 —— 进程重启后仍生效)
    pub fn seen(&self, key: &str) -> Option<Fence> {
        let path = self.seen_path(key);
        let raw = std::fs::read_to_string(&path).ok()?;
        Fence::parse(raw.trim())
    }

    /// 校验并抬高。**返回 Ok 之前已完成 fsync**(设计 §9.2 第 2 步)。
    pub fn check_and_raise(&self, key: &str, incoming: Fence) -> Result<(), StaleFence> {
        let seen = self.seen(key);
        if let Some(s) = seen {
            if s.shard != incoming.shard {
                return Err(StaleFence {
                    key: key.to_string(),
                    seen: Some(s),
                    incoming,
                    reason: "shard 不同, fence 不可比(容器分片归属变更需显式重建)",
                });
            }
            if incoming < s {
                return Err(StaleFence {
                    key: key.to_string(),
                    seen: Some(s),
                    incoming,
                    reason: "低于已见 fence",
                });
            }
            if incoming == s {
                return Ok(()); // 幂等重发:放行,不重复写盘
            }
        }
        self.write_seen(key, incoming).map_err(|e| StaleFence {
            key: key.to_string(),
            seen,
            incoming,
            reason: if e.kind() == std::io::ErrorKind::PermissionDenied {
                "fence_seen 落盘失败(权限)"
            } else {
                "fence_seen 落盘失败(fsync 不可信,拒绝执行)"
            },
        })
    }

    fn write_seen(&self, key: &str, fence: Fence) -> std::io::Result<()> {
        let path = self.seen_path(key);
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let tmp = path.with_extension("tmp");
        {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(fence.wire().as_bytes())?;
            f.write_all(b"\n")?;
            // 必须落盘:否则崩溃后旧 fence 可能被"忘记",导致重复执行
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &path)?;
        if let Some(dir) = path.parent() {
            super::log::fsync_dir(dir)?;
        }
        Ok(())
    }

    /// 幂等缓存读取
    pub fn idem_get(&self, idem: &str) -> Option<String> {
        std::fs::read_to_string(self.idem_path(idem)).ok()
    }

    /// 幂等缓存写入(落盘 fsync)
    pub fn idem_put(&self, idem: &str, body: &str) -> std::io::Result<()> {
        let path = self.idem_path(idem);
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let tmp = path.with_extension("tmp");
        {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(body.as_bytes())?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &path)?;
        Ok(())
    }

    /// 该宿主机已见的最大 fence(诊断/`/agent/ping` 上报)
    pub fn seen_max(&self) -> Option<Fence> {
        let dir = self.root.join("fence_seen");
        let mut max: Option<Fence> = None;
        for entry in std::fs::read_dir(dir).ok()? {
            let p = entry.ok()?.path();
            let raw = std::fs::read_to_string(&p).ok()?;
            if let Some(f) = Fence::parse(raw.trim()) {
                max = Some(match max {
                    Some(m) => m.max(f),
                    None => f,
                });
            }
        }
        max
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_dir(tag: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        let n = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        p.push(format!("rdsctl-ha-fence-{tag}-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn raises_monotonically_and_rejects_stale() {
        let dir = tmp_dir("raise");
        let g = ExecutorGuard::new(&dir);
        assert!(g.check_and_raise("c1", Fence::new(0, 1, 10)).is_ok());
        // 相同 fence:幂等放行(重发安全)
        assert!(g.check_and_raise("c1", Fence::new(0, 1, 10)).is_ok());
        // 更高:放行并抬高
        assert!(g.check_and_raise("c1", Fence::new(0, 2, 1)).is_ok());
        // 更低:必须拒绝
        let err = g.check_and_raise("c1", Fence::new(0, 1, 99)).unwrap_err();
        assert!(err.reason.contains("低于已见"));
        assert_eq!(g.seen("c1"), Some(Fence::new(0, 2, 1)));
        // index 更高但 term 更低:同样拒绝(按 (term,index) 字典序)
        assert!(g.check_and_raise("c1", Fence::new(0, 1, 1_000_000)).is_err());
    }

    #[test]
    fn shard_mismatch_is_rejected() {
        let dir = tmp_dir("shard");
        let g = ExecutorGuard::new(&dir);
        assert!(g.check_and_raise("c1", Fence::new(0, 5, 5)).is_ok());
        let err = g.check_and_raise("c1", Fence::new(1, 9, 9)).unwrap_err();
        assert!(err.reason.contains("shard 不同"), "跨分片 fence 不可比: {err}");
    }

    #[test]
    fn fence_survives_restart_and_isolated_per_key() {
        let dir = tmp_dir("persist");
        {
            let g = ExecutorGuard::new(&dir);
            assert!(g.check_and_raise("c1", Fence::new(0, 3, 30)).is_ok());
            assert!(g.check_and_raise("c2", Fence::new(0, 1, 1)).is_ok());
        }
        let g2 = ExecutorGuard::new(&dir);
        assert_eq!(g2.seen("c1"), Some(Fence::new(0, 3, 30)), "重启后仍生效");
        assert!(
            g2.check_and_raise("c1", Fence::new(0, 1, 1)).is_err(),
            "重启后旧 fence 必须继续被拒(僵尸副本兜底)"
        );
        // 键之间互不影响
        assert!(g2.check_and_raise("c2", Fence::new(0, 1, 2)).is_ok());
        assert_eq!(g2.seen_max(), Some(Fence::new(0, 3, 30)));
    }

    #[test]
    fn keys_are_sanitized_against_path_traversal() {
        let dir = tmp_dir("traverse");
        let g = ExecutorGuard::new(&dir);
        let evil = "../../../../etc/passwd";
        assert!(g.check_and_raise(evil, Fence::new(0, 1, 1)).is_ok());
        // 逃逸必须失败:文件只能落在 root 之内
        let escaped = dir.parent().unwrap().join("etc/passwd");
        assert!(!escaped.exists(), "不得写到 root 之外");
        let mut found = false;
        for e in std::fs::read_dir(dir.join("fence_seen")).unwrap() {
            let p = e.unwrap().path();
            assert!(p.starts_with(&dir), "文件必须位于 root 内:{}", p.display());
            found = true;
        }
        assert!(found, "应落在 root 内");
        // 纯点名兜底
        assert!(g.check_and_raise("..", Fence::new(0, 1, 1)).is_ok());
        assert!(dir.join("fence_seen").join("k").exists() || {
            std::fs::read_dir(dir.join("fence_seen")).unwrap().count() == 2
        });
    }

    #[test]
    fn idem_cache_roundtrip_and_persistence() {
        let dir = tmp_dir("idem");
        let g = ExecutorGuard::new(&dir);
        assert!(g.idem_get("k1").is_none());
        g.idem_put("k1", r#"{"ok":true,"out":"done"}"#).unwrap();
        assert_eq!(
            g.idem_get("k1").as_deref(),
            Some(r#"{"ok":true,"out":"done"}"#)
        );
        let g2 = ExecutorGuard::new(&dir);
        assert_eq!(
            g2.idem_get("k1").as_deref(),
            Some(r#"{"ok":true,"out":"done"}"#),
            "幂等缓存必须跨进程存活"
        );
    }
}
