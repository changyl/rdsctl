// rdsctl HA — 快照(设计 §7.3)
//
// 形态:JSON(可人工检视,复用 serde;见设计 §17.3 Q2 的推荐),自校验哈希 + 原子写。
//   snapshot.json      当前快照
//   snapshot.prev.json 上一份(写入过程中的回退点)
// 内容:last_included_index/term + config_epoch + format_ver + 状态机镜像 + hash。
// 恢复:优先当前,损坏则回退上一份;两份都不可用 → 返回 None(由上层从日志重放)。

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::log::fsync_dir;
use super::{HaError, HaResult};
use crate::sha256;

pub const SNAPSHOT_FORMAT_VER: u16 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotFile {
    pub format_ver: u16,
    pub shard: u16,
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub config_epoch: u64,
    /// 状态机镜像(serde_json 文本;由 state::StateMachine 提供)
    pub state_json: String,
    /// sha256(shard | index | term | epoch | state_json) 的 hex
    pub hash: String,
}

fn compute_hash(
    shard: u16,
    index: u64,
    term: u64,
    config_epoch: u64,
    state_json: &str,
) -> String {
    let mut buf = Vec::with_capacity(state_json.len() + 64);
    buf.extend_from_slice(&shard.to_be_bytes());
    buf.extend_from_slice(&index.to_be_bytes());
    buf.extend_from_slice(&term.to_be_bytes());
    buf.extend_from_slice(&config_epoch.to_be_bytes());
    buf.extend_from_slice(state_json.as_bytes());
    sha256::to_hex(&sha256::digest(&buf))
}

impl SnapshotFile {
    pub fn new(
        shard: u16,
        last_included_index: u64,
        last_included_term: u64,
        config_epoch: u64,
        state_json: String,
    ) -> Self {
        let hash = compute_hash(
            shard,
            last_included_index,
            last_included_term,
            config_epoch,
            &state_json,
        );
        Self {
            format_ver: SNAPSHOT_FORMAT_VER,
            shard,
            last_included_index,
            last_included_term,
            config_epoch,
            state_json,
            hash,
        }
    }

    pub fn verify(&self) -> HaResult<()> {
        if self.format_ver != SNAPSHOT_FORMAT_VER {
            return Err(HaError::Corrupt(format!(
                "快照格式版本不支持:{} (本程序 {})",
                self.format_ver, SNAPSHOT_FORMAT_VER
            )));
        }
        let expect = compute_hash(
            self.shard,
            self.last_included_index,
            self.last_included_term,
            self.config_epoch,
            &self.state_json,
        );
        if expect != self.hash {
            return Err(HaError::Corrupt(format!(
                "快照哈希失配(index={}):文件可能被截断或篡改",
                self.last_included_index
            )));
        }
        Ok(())
    }

    /// 该快照的哈希(作为日志压实后的链锚点)
    pub fn seed_bytes(&self) -> [u8; 32] {
        sha256::digest(self.hash.as_bytes())
    }
}

pub fn snapshot_path(dir: &Path, shard: u16) -> PathBuf {
    dir.join(format!("shard-{shard}")).join("snapshot.json")
}

fn prev_path(dir: &Path, shard: u16) -> PathBuf {
    dir.join(format!("shard-{shard}")).join("snapshot.prev.json")
}

/// 原子写:temp → fsync → (当前转 prev) → rename → 目录 fsync
pub fn save(dir: &Path, snap: &SnapshotFile) -> HaResult<()> {
    let sdir = dir.join(format!("shard-{}", snap.shard));
    std::fs::create_dir_all(&sdir)?;
    let cur = snapshot_path(dir, snap.shard);
    let prev = prev_path(dir, snap.shard);
    let tmp = sdir.join("snapshot.tmp");

    let body = serde_json::to_vec(snap)
        .map_err(|e| HaError::Corrupt(format!("快照序列化失败: {e}")))?;
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&body)?;
        f.sync_all()?;
    }
    if cur.exists() {
        // 保留上一份作为回退点
        std::fs::rename(&cur, &prev)?;
    }
    std::fs::rename(&tmp, &cur)?;
    fsync_dir(&sdir)?;
    Ok(())
}

/// 加载:优先当前,损坏/缺失回退上一份;都不可用返回 None。
pub fn load(dir: &Path, shard: u16) -> HaResult<Option<SnapshotFile>> {
    let cur = snapshot_path(dir, shard);
    match read_one(&cur) {
        Ok(Some(s)) => return Ok(Some(s)),
        Ok(None) => {}
        Err(e) => {
            tracing::warn!("当前快照不可用({}):{e};尝试回退上一份", cur.display());
        }
    }
    let prev = prev_path(dir, shard);
    match read_one(&prev) {
        Ok(Some(s)) => {
            tracing::warn!("已回退使用上一份快照:{}", prev.display());
            Ok(Some(s))
        }
        Ok(None) => Ok(None),
        Err(e) => Err(e),
    }
}

fn read_one(path: &Path) -> HaResult<Option<SnapshotFile>> {
    if !path.exists() {
        return Ok(None);
    }
    let raw = std::fs::read(path)?;
    let snap: SnapshotFile = serde_json::from_slice(&raw)
        .map_err(|e| HaError::Corrupt(format!("快照解析失败({}):{e}", path.display())))?;
    snap.verify()?;
    Ok(Some(snap))
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
        p.push(format!("rdsctl-ha-snap-{tag}-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn save_load_roundtrip() {
        let dir = tmp_dir("rt");
        let s = SnapshotFile::new(0, 42, 3, 1, "{\"kv\":{}}".into());
        save(&dir, &s).unwrap();
        let got = load(&dir, 0).unwrap().expect("应能加载");
        assert_eq!(got.last_included_index, 42);
        assert_eq!(got.last_included_term, 3);
        assert_eq!(got.state_json, "{\"kv\":{}}");
    }

    #[test]
    fn tampered_snapshot_is_detected_and_falls_back_to_prev() {
        let dir = tmp_dir("tamper");
        save(&dir, &SnapshotFile::new(0, 1, 1, 1, "{\"v\":1}".into())).unwrap();
        save(&dir, &SnapshotFile::new(0, 2, 1, 1, "{\"v\":2}".into())).unwrap();
        // 篡改当前快照
        let cur = snapshot_path(&dir, 0);
        let mut raw = std::fs::read(&cur).unwrap();
        let n = raw.len();
        raw[n - 3] ^= 0xFF;
        std::fs::write(&cur, &raw).unwrap();
        let got = load(&dir, 0).unwrap().expect("应回退到上一份");
        assert_eq!(got.last_included_index, 1, "必须回退到上一份快照");
    }

    #[test]
    fn both_snapshots_broken_returns_error() {
        let dir = tmp_dir("broken");
        save(&dir, &SnapshotFile::new(0, 1, 1, 1, "{}".into())).unwrap();
        save(&dir, &SnapshotFile::new(0, 2, 1, 1, "{}".into())).unwrap();
        for p in [snapshot_path(&dir, 0), prev_path(&dir, 0)] {
            std::fs::write(&p, b"not json").unwrap();
        }
        assert!(load(&dir, 0).is_err(), "两份都损坏必须报错而非静默当空");
    }

    #[test]
    fn missing_snapshot_is_none() {
        let dir = tmp_dir("none");
        assert!(load(&dir, 0).unwrap().is_none());
    }
}
