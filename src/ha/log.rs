// rdsctl HA — 本地持久日志(设计 §7.1/§7.2/§7.4)
//
// 记录格式(定长头 + 变长载荷 + 自校验哈希):
//   [magic u32][ver u16][kind u8][flags u8][shard u16]
//   [term u64][index u64][prev_hash 32B][payload_len u32][payload][hash 32B]
//   hash = SHA-256(payload ‖ header)  —— 与设计文档口径一致
//   prev_hash 形成哈希链:捕获截断/错位/静默损坏。
//
// 恢复语义(设计 §7.4):
//   - 尾部不完整记录(异常掉电)→ 丢弃并告警(TailTruncated);
//   - 中间损坏(magic/hash 失配且其后仍有合法记录)→ **拒绝启动**(Corrupt)。
//
// 说明(v1 取舍):压实与后缀截断用"重写整文件 + 原子 rename"实现;日志规模由快照阈值
// 约束(默认 50000 条 / 128MB),重写属低频操作,且按分片错峰执行(设计 §13.3)。

use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::{HaError, HaResult};
use crate::sha256;

pub const MAGIC: u32 = 0x5244_5343; // "RDSC"
pub const LOG_VER: u16 = 1;
pub const HEADER_LEN: usize = 62;
pub const HASH_LEN: usize = 32;

/// 记录类型
pub const KIND_ENTRY: u8 = 1; // 普通 op(载荷 = state::Op 的 JSON)
pub const KIND_CONFIG: u8 = 2; // 配置世代(config_epoch / 成员表)

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    pub ver: u16,
    pub kind: u8,
    pub flags: u8,
    pub shard: u16,
    pub term: u64,
    pub index: u64,
    pub prev_hash: [u8; 32],
    pub payload: Vec<u8>,
    pub hash: [u8; 32],
}

fn header_bytes(
    ver: u16,
    kind: u8,
    flags: u8,
    shard: u16,
    term: u64,
    index: u64,
    prev_hash: &[u8; 32],
    payload_len: u32,
) -> [u8; HEADER_LEN] {
    let mut h = [0u8; HEADER_LEN];
    h[0..4].copy_from_slice(&MAGIC.to_be_bytes());
    h[4..6].copy_from_slice(&ver.to_be_bytes());
    h[6] = kind;
    h[7] = flags;
    h[8..10].copy_from_slice(&shard.to_be_bytes());
    h[10..18].copy_from_slice(&term.to_be_bytes());
    h[18..26].copy_from_slice(&index.to_be_bytes());
    h[26..58].copy_from_slice(prev_hash);
    h[58..62].copy_from_slice(&payload_len.to_be_bytes());
    h
}

/// 记录哈希:SHA-256(payload ‖ header)(顺序与设计文档一致)
fn compute_hash(header: &[u8; HEADER_LEN], payload: &[u8]) -> [u8; 32] {
    let mut buf = Vec::with_capacity(payload.len() + HEADER_LEN);
    buf.extend_from_slice(payload);
    buf.extend_from_slice(header);
    sha256::digest(&buf)
}

impl Record {
    pub fn payload_len(&self) -> u32 {
        self.payload.len() as u32
    }

    pub fn header(&self) -> [u8; HEADER_LEN] {
        header_bytes(
            self.ver,
            self.kind,
            self.flags,
            self.shard,
            self.term,
            self.index,
            &self.prev_hash,
            self.payload_len(),
        )
    }

    pub fn encode(&self) -> Vec<u8> {
        let h = self.header();
        let mut out = Vec::with_capacity(HEADER_LEN + self.payload.len() + HASH_LEN);
        out.extend_from_slice(&h);
        out.extend_from_slice(&self.payload);
        out.extend_from_slice(&self.hash);
        out
    }

    /// 记录总字节数
    pub fn wire_len(&self) -> u64 {
        (HEADER_LEN + self.payload.len() + HASH_LEN) as u64
    }

    fn verify_hash(&self) -> bool {
        compute_hash(&self.header(), &self.payload) == self.hash
    }
}

/// 待追加条目(term/index 由 raft 指派,log 只负责持久化与校验)
#[derive(Debug, Clone)]
pub struct PendingEntry {
    pub kind: u8,
    pub term: u64,
    pub index: u64,
    pub payload: Vec<u8>,
}

/// 追加结果(便于上层判断是否为本批次最后一条)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendOutcome {
    pub last_index: u64,
    pub synced: bool,
}

#[derive(Debug)]
pub struct Log {
    dir: PathBuf,
    path: PathBuf,
    shard: u16,
    file: File,
    entries: Vec<Record>,
    seed_hash: [u8; 32],
    truncated_tail_bytes: u64,
    /// 测试注入:为 true 时 append_batch 在写入前即失败(模拟 fsync/磁盘不可信,A2/F8)
    #[cfg(test)]
    fsync_fail: bool,
}

impl Log {
    /// 打开(或创建)分片日志。`seed_hash` = 快照哈希(无快照传全零),
    /// 用于校验首条记录的 prev_hash 与快照衔接。
    pub fn open(dir: &Path, shard: u16, seed_hash: [u8; 32]) -> HaResult<Self> {
        let sdir = dir.join(format!("shard-{shard}"));
        std::fs::create_dir_all(&sdir)?;
        let path = sdir.join("log");
        if !path.exists() {
            File::create(&path)?.sync_all()?;
            fsync_dir(&sdir)?;
        }
        let mut log = Log {
            dir: sdir.clone(),
            path: path.clone(),
            shard,
            file: OpenOptions::new().read(true).append(true).open(&path)?,
            entries: Vec::new(),
            seed_hash,
            truncated_tail_bytes: 0,
            #[cfg(test)]
            fsync_fail: false,
        };
        log.replay()?;
        Ok(log)
    }

    fn replay(&mut self) -> HaResult<()> {
        let mut f = File::open(&self.path)?;
        let total = f.metadata()?.len();
        let mut reader = BufReader::new(&mut f);
        let mut offset: u64 = 0;
        let mut prev = self.seed_hash;
        let mut entries: Vec<Record> = Vec::new();

        loop {
            let mut header = [0u8; HEADER_LEN];
            let got = read_full(&mut reader, &mut header)?;
            if got == 0 {
                break; // 干净结束
            }
            if got < HEADER_LEN {
                return self.drop_tail(offset, total, "头部不完整");
            }
            let magic = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
            let ver = u16::from_be_bytes([header[4], header[5]]);
            let kind = header[6];
            let flags = header[7];
            let shard = u16::from_be_bytes([header[8], header[9]]);
            let term = u64::from_be_bytes(header[10..18].try_into().unwrap());
            let index = u64::from_be_bytes(header[18..26].try_into().unwrap());
            let mut prev_hash = [0u8; 32];
            prev_hash.copy_from_slice(&header[26..58]);
            let payload_len = u32::from_be_bytes([header[58], header[59], header[60], header[61]]) as usize;

            if magic != MAGIC {
                return self.drop_tail(offset, total, "magic 失配");
            }
            if ver != LOG_VER {
                return Err(HaError::Corrupt(format!(
                    "记录格式版本不支持:文件 ver={ver},本程序 ver={LOG_VER}(需按运维手册升级)"
                )));
            }
            if shard != self.shard {
                return Err(HaError::Corrupt(format!(
                    "分片号不符:文件 shard={shard},期望 {}",
                    self.shard
                )));
            }

            let mut payload = vec![0u8; payload_len];
            if read_full(&mut reader, &mut payload)? < payload_len {
                return self.drop_tail(offset, total, "载荷不完整");
            }
            let mut hash = [0u8; HASH_LEN];
            if read_full(&mut reader, &mut hash)? < HASH_LEN {
                return self.drop_tail(offset, total, "哈希不完整");
            }

            let rec = Record {
                ver,
                kind,
                flags,
                shard,
                term,
                index,
                prev_hash,
                payload,
                hash,
            };
            if !rec.verify_hash() {
                return self.drop_tail(offset, total, "记录哈希失配");
            }
            if rec.prev_hash != prev {
                return Err(HaError::Corrupt(format!(
                    "哈希链断裂:index={} 的 prev_hash 与上一条不符(中间损坏,拒绝启动)",
                    rec.index
                )));
            }
            prev = rec.hash;
            offset += rec.wire_len();
            entries.push(rec);
        }

        // 索引连续性校验(必须由 1 开始连续;若有快照则从快照 index+1 开始)
        for w in entries.windows(2) {
            if w[1].index != w[0].index + 1 {
                return Err(HaError::Corrupt(format!(
                    "日志索引不连续:{} → {}",
                    w[0].index, w[1].index
                )));
            }
        }
        if let Some(first) = entries.first() {
            if self.seed_hash == [0u8; 32] && first.index != 1 {
                return Err(HaError::Corrupt(format!(
                    "无快照时首条记录必须为 index=1,实际 {}",
                    first.index
                )));
            }
        }

        self.entries = entries;
        Ok(())
    }

    /// 尾部不完整/损坏的处理:其后若仍有合法记录 → 中间损坏(致命);否则截断尾部。
    fn drop_tail(&mut self, offset: u64, total: u64, why: &str) -> HaResult<()> {
        let mut tail = File::open(&self.path)?;
        tail.seek(SeekFrom::Start(offset))?;
        let mut rest = Vec::new();
        tail.read_to_end(&mut rest)?;
        // 注意:必须跳过当前失败记录自身的 magic,否则"头部完整但载荷不完整"会被误判为中间损坏。
        let scan_from = 4.min(rest.len());
        let has_later_record = rest[scan_from..]
            .windows(4)
            .any(|w| u32::from_be_bytes([w[0], w[1], w[2], w[3]]) == MAGIC);
        if has_later_record {
            return Err(HaError::Corrupt(format!(
                "{why} 且其后仍存在合法记录(offset={offset}):中间损坏,拒绝启动"
            )));
        }
        let dropped = total.saturating_sub(offset);
        tracing::warn!(
            "日志尾部丢弃 {dropped} 字节({why};offset={offset});其后无合法记录,按尾部截断处理"
        );
        self.truncated_tail_bytes = dropped;
        self.file.set_len(offset)?;
        self.file.sync_all()?;
        // 截断后重放:此时前缀必为已校验通过的完整记录,一次即可收敛。
        // (不能直接返回 Ok —— 那样本轮的 entries 尚未写回 self。)
        self.replay()
    }

    /// 测试注入:模拟 fsync/磁盘失败(必须导致"拒绝提交且不自降级为已持久化")
    #[cfg(test)]
    pub fn inject_fsync_failure(&mut self, on: bool) {
        self.fsync_fail = on;
    }

    /// 追加一批记录(组提交:整批一次 fsync)。索引必须自 last_index+1 起连续。
    pub fn append_batch(&mut self, batch: &[PendingEntry]) -> HaResult<AppendOutcome> {
        #[cfg(test)]
        if self.fsync_fail {
            // 关键语义:**在写入之前失败**,即内存索引也不推进 —— 绝不允许"应答了但没落盘"
            return Err(HaError::Io(std::io::Error::other(
                "注入的 fsync 失败(测试)",
            )));
        }
        if batch.is_empty() {
            return Ok(AppendOutcome {
                last_index: self.last_index(),
                synced: false,
            });
        }
        let mut expect = self.last_index() + 1;
        for e in batch {
            if e.index != expect {
                return Err(HaError::Corrupt(format!(
                    "追加索引不连续:期望 {expect},实际 {}(raft 指派错误)",
                    e.index
                )));
            }
            expect += 1;
        }

        let mut prev = self.prev_hash_for_next();
        let mut buf: Vec<u8> = Vec::new();
        let mut staged: Vec<Record> = Vec::with_capacity(batch.len());
        for e in batch {
            let header = header_bytes(
                LOG_VER,
                e.kind,
                0,
                self.shard,
                e.term,
                e.index,
                &prev,
                e.payload.len() as u32,
            );
            let hash = compute_hash(&header, &e.payload);
            let rec = Record {
                ver: LOG_VER,
                kind: e.kind,
                flags: 0,
                shard: self.shard,
                term: e.term,
                index: e.index,
                prev_hash: prev,
                payload: e.payload.clone(),
                hash,
            };
            buf.extend_from_slice(&header);
            buf.extend_from_slice(&e.payload);
            buf.extend_from_slice(&hash);
            prev = hash;
            staged.push(rec);
        }

        self.file.write_all(&buf)?;
        // fsync 失败 = 前提 A2 受损:必须上抛,由上层自降级(绝不"未持久化即应答")
        self.file.sync_all()?;
        let last = staged.last().map(|r| r.index).unwrap_or(0);
        self.entries.extend(staged);
        Ok(AppendOutcome {
            last_index: last,
            synced: true,
        })
    }

    pub fn last_index(&self) -> u64 {
        self.entries.last().map(|r| r.index).unwrap_or(0)
    }

    pub fn last_term(&self) -> u64 {
        self.entries.last().map(|r| r.term).unwrap_or(0)
    }

    /// 最早可用的内存索引(空则返回 last_index+1,表示"下一条写入位置")
    pub fn first_index(&self) -> u64 {
        self.entries
            .first()
            .map(|r| r.index)
            .unwrap_or_else(|| self.last_index() + 1)
    }

    pub fn seed_hash(&self) -> [u8; 32] {
        self.seed_hash
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn truncated_tail_bytes(&self) -> u64 {
        self.truncated_tail_bytes
    }

    pub fn get(&self, index: u64) -> Option<&Record> {
        let first = self.entries.first()?.index;
        if index < first {
            return None;
        }
        self.entries.get((index - first) as usize)
    }

    pub fn term_at(&self, index: u64) -> Option<u64> {
        self.get(index).map(|r| r.term)
    }

    /// 供 follower 复制:[from, last] 的克隆
    pub fn entries_from(&self, from: u64) -> Vec<Record> {
        let first = match self.entries.first() {
            Some(r) => r.index,
            None => return Vec::new(),
        };
        if from < first {
            return self.entries.clone();
        }
        let skip = (from - first) as usize;
        self.entries.iter().skip(skip).cloned().collect()
    }

    fn prev_hash_for_next(&self) -> [u8; 32] {
        self.entries
            .last()
            .map(|r| r.hash)
            .unwrap_or(self.seed_hash)
    }

    /// 丢弃 index >= from 的后缀(leader 覆盖冲突条目)
    pub fn truncate_suffix(&mut self, from: u64) -> HaResult<()> {
        if from > self.last_index() {
            return Ok(());
        }
        let keep: Vec<Record> = self
            .entries
            .iter()
            .filter(|r| r.index < from)
            .cloned()
            .collect();
        // 截断"后缀":保留的是**前缀**,故链锚点不变(仍是 seed_hash);
        // 被保留条目的哈希会在 rewrite 中按其真实 prev 重新计算。
        let seed = self.seed_hash;
        self.rewrite(keep, seed)
    }

    /// 压实:丢弃 index <= upto 的条目,并把哈希链重新锚定到快照哈希
    pub fn compact_upto(&mut self, upto: u64, snap_hash: [u8; 32]) -> HaResult<()> {
        let keep: Vec<Record> = self
            .entries
            .iter()
            .filter(|r| r.index > upto)
            .cloned()
            .collect();
        self.rewrite(keep, snap_hash)
    }

    fn rewrite(&mut self, keep: Vec<Record>, seed: [u8; 32]) -> HaResult<()> {
        let tmp = self.dir.join("log.tmp");
        let mut prev = seed;
        let mut buf: Vec<u8> = Vec::new();
        let mut staged: Vec<Record> = Vec::with_capacity(keep.len());
        for mut r in keep {
            r.ver = LOG_VER;
            r.prev_hash = prev;
            let header = header_bytes(
                r.ver,
                r.kind,
                r.flags,
                self.shard,
                r.term,
                r.index,
                &r.prev_hash,
                r.payload.len() as u32,
            );
            r.hash = compute_hash(&header, &r.payload);
            prev = r.hash;
            buf.extend_from_slice(&header);
            buf.extend_from_slice(&r.payload);
            buf.extend_from_slice(&r.hash);
            staged.push(r);
        }
        {
            let mut f = File::create(&tmp)?;
            f.write_all(&buf)?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &self.path)?;
        fsync_dir(&self.dir)?;
        self.file = OpenOptions::new().read(true).append(true).open(&self.path)?;
        self.entries = staged;
        self.seed_hash = seed;
        Ok(())
    }

    /// 显式 fsync(用于快照前的强制落盘)
    pub fn sync(&mut self) -> HaResult<()> {
        self.file.sync_all()?;
        Ok(())
    }
}

/// 读满 buf;返回实际读取字节数(EOF 时可能小于 buf.len())
fn read_full<R: Read>(r: &mut R, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        match r.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(filled)
}

/// 目录 fsync:保证 rename/create 的目录项变更落盘
pub fn fsync_dir(dir: &Path) -> std::io::Result<()> {
    match File::open(dir) {
        Ok(f) => f.sync_all(),
        // 某些平台/文件系统不支持对目录 fsync:降级为告警而非失败
        Err(e) => {
            tracing::warn!("目录 fsync 不可用({}):{e}", dir.display());
            Ok(())
        }
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
        p.push(format!("rdsctl-ha-log-{tag}-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn entry(index: u64, term: u64, body: &str) -> PendingEntry {
        PendingEntry {
            kind: KIND_ENTRY,
            term,
            index,
            payload: body.as_bytes().to_vec(),
        }
    }

    #[test]
    fn append_reopen_and_replay() {
        let dir = tmp_dir("replay");
        {
            let mut log = Log::open(&dir, 0, [0u8; 32]).unwrap();
            log.append_batch(&[entry(1, 1, "a"), entry(2, 1, "b")]).unwrap();
            log.append_batch(&[entry(3, 2, "c")]).unwrap();
            assert_eq!(log.last_index(), 3);
            assert_eq!(log.last_term(), 2);
            assert_eq!(log.first_index(), 1);
        }
        let log = Log::open(&dir, 0, [0u8; 32]).unwrap();
        assert_eq!(log.len(), 3);
        assert_eq!(log.get(2).unwrap().payload, b"b");
        assert_eq!(log.term_at(3), Some(2));
        assert_eq!(log.truncated_tail_bytes(), 0);
        assert_eq!(log.entries_from(2).len(), 2);
    }

    #[test]
    fn append_rejects_non_contiguous_index() {
        let dir = tmp_dir("contig");
        let mut log = Log::open(&dir, 0, [0u8; 32]).unwrap();
        let err = log.append_batch(&[entry(2, 1, "x")]).unwrap_err();
        assert!(matches!(err, HaError::Corrupt(_)), "索引不连续必须拒绝");
        assert_eq!(log.last_index(), 0);
    }

    /// F8/S4:fsync 失败必须"拒绝提交":内存索引不推进、文件不留半条记录、
    /// 重开日志也看不到这条记录(否则就是"未持久化即应答")
    #[test]
    fn injected_fsync_failure_rejects_append_without_side_effects() {
        let dir = tmp_dir("fsyncfail");
        {
            let mut log = Log::open(&dir, 0, [0u8; 32]).unwrap();
            log.append_batch(&[entry(1, 1, "a")]).unwrap();
            log.inject_fsync_failure(true);
            let err = log.append_batch(&[entry(2, 1, "b")]).unwrap_err();
            assert!(matches!(err, HaError::Io(_)), "应返回 IO 错误:{err}");
            assert_eq!(log.last_index(), 1, "失败后索引不得推进");
            assert_eq!(log.len(), 1);
            // 恢复后可继续追加(且从 index=2 开始)
            log.inject_fsync_failure(false);
            log.append_batch(&[entry(2, 1, "b")]).unwrap();
            assert_eq!(log.last_index(), 2);
        }
        // 重开:文件里只有两条完整记录,没有半条 fsync 失败的残留
        let log = Log::open(&dir, 0, [0u8; 32]).unwrap();
        assert_eq!(log.len(), 2);
        assert_eq!(log.last_index(), 2);
        assert_eq!(log.truncated_tail_bytes(), 0);
    }

    #[test]
    fn tail_truncation_is_tolerated() {
        let dir = tmp_dir("tail");
        {
            let mut log = Log::open(&dir, 0, [0u8; 32]).unwrap();
            log.append_batch(&[entry(1, 1, "a"), entry(2, 1, "b")]).unwrap();
        }
        // 追加半条记录(模拟掉电)
        let path = dir.join("shard-0").join("log");
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            let header = header_bytes(LOG_VER, KIND_ENTRY, 0, 0, 1, 3, &[7u8; 32], 100);
            f.write_all(&header).unwrap();
            f.write_all(b"partial").unwrap();
        }
        let log = Log::open(&dir, 0, [0u8; 32]).unwrap();
        assert_eq!(log.last_index(), 2, "尾部不完整记录应被丢弃");
        assert_eq!(log.len(), 2);
        assert!(log.truncated_tail_bytes() > 0);
        let on_disk = std::fs::metadata(&path).unwrap().len();
        let expected: u64 = log.entries.iter().map(|r| r.wire_len()).sum();
        assert_eq!(on_disk, expected, "文件应被截断到最后一个完整记录");
    }

    #[test]
    fn middle_corruption_is_fatal() {
        let dir = tmp_dir("middle");
        {
            let mut log = Log::open(&dir, 0, [0u8; 32]).unwrap();
            log.append_batch(&[
                entry(1, 1, "aaaa"),
                entry(2, 1, "bbbb"),
                entry(3, 1, "cccc"),
            ])
            .unwrap();
        }
        let path = dir.join("shard-0").join("log");
        let mut bytes = std::fs::read(&path).unwrap();
        // 破坏第 2 条记录的载荷(第 1 条之后),其后仍有合法记录 → 必须致命
        let first_len = (HEADER_LEN + 4 + HASH_LEN) as usize;
        bytes[first_len + HEADER_LEN] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();
        let err = Log::open(&dir, 0, [0u8; 32]).unwrap_err();
        assert!(matches!(err, HaError::Corrupt(_)), "中间损坏必须拒绝启动: {err}");
    }

    #[test]
    fn hash_chain_break_is_fatal() {
        let dir = tmp_dir("chain");
        {
            let mut log = Log::open(&dir, 0, [0u8; 32]).unwrap();
            log.append_batch(&[entry(1, 1, "a"), entry(2, 1, "b")]).unwrap();
        }
        let path = dir.join("shard-0").join("log");
        let mut bytes = std::fs::read(&path).unwrap();
        // 把第 2 条的 prev_hash 改掉并重算其 hash(模拟"文件被拼接/错位")
        let first_len = (HEADER_LEN + 1 + HASH_LEN) as usize;
        let p = first_len + 26;
        bytes[p] ^= 0xFF;
        let payload_start = first_len + HEADER_LEN;
        let mut header = [0u8; HEADER_LEN];
        header.copy_from_slice(&bytes[first_len..first_len + HEADER_LEN]);
        let payload = &bytes[payload_start..payload_start + 1];
        let h = compute_hash(&header, payload);
        let hash_start = payload_start + 1;
        bytes[hash_start..hash_start + HASH_LEN].copy_from_slice(&h);
        std::fs::write(&path, &bytes).unwrap();
        let err = Log::open(&dir, 0, [0u8; 32]).unwrap_err();
        assert!(matches!(err, HaError::Corrupt(_)), "哈希链断裂必须拒绝: {err}");
    }

    #[test]
    fn truncate_suffix_keeps_prefix_and_reanchors() {
        let dir = tmp_dir("trunc");
        let mut log = Log::open(&dir, 0, [0u8; 32]).unwrap();
        log.append_batch(&[entry(1, 1, "a"), entry(2, 1, "b"), entry(3, 1, "c")])
            .unwrap();
        log.truncate_suffix(2).unwrap();
        assert_eq!(log.last_index(), 1);
        assert_eq!(log.len(), 1);
        // 截断后可继续追加,且哈希链正确(重开验证)
        log.append_batch(&[entry(2, 2, "b2")]).unwrap();
        assert_eq!(log.term_at(2), Some(2));
        drop(log);
        let log = Log::open(&dir, 0, [0u8; 32]).unwrap();
        assert_eq!(log.last_index(), 2);
        assert_eq!(log.get(2).unwrap().payload, b"b2");
    }

    #[test]
    fn compact_upto_reseeds_chain_to_snapshot_hash() {
        let dir = tmp_dir("compact");
        let snap_hash = [9u8; 32];
        {
            let mut log = Log::open(&dir, 0, [0u8; 32]).unwrap();
            log.append_batch(&[entry(1, 1, "a"), entry(2, 1, "b"), entry(3, 1, "c")])
                .unwrap();
            log.compact_upto(2, snap_hash).unwrap();
            assert_eq!(log.len(), 1);
            assert_eq!(log.first_index(), 3);
            assert_eq!(log.seed_hash(), snap_hash);
            assert_eq!(log.get(3).unwrap().prev_hash, snap_hash, "压实后哈希链锚定快照");
        }
        // 以同一快照哈希重开:衔接校验必须通过
        let log = Log::open(&dir, 0, snap_hash).unwrap();
        assert_eq!(log.len(), 1);
        assert_eq!(log.get(3).unwrap().payload, b"c");
        assert_eq!(log.get(2), None, "已压实条目不再可用(需从快照恢复)");
    }

    #[test]
    fn opening_with_wrong_seed_hash_is_fatal() {
        let dir = tmp_dir("seed");
        {
            let mut log = Log::open(&dir, 0, [1u8; 32]).unwrap();
            log.append_batch(&[entry(1, 1, "a"), entry(2, 1, "b")]).unwrap();
        }
        // 用不同的快照哈希重开:首条记录 prev_hash 与期望不符 → 必须拒绝
        let err = Log::open(&dir, 0, [2u8; 32]).unwrap_err();
        assert!(matches!(err, HaError::Corrupt(_)), "seed 与日志不符必须拒绝: {err}");
        // 正确 seed 可正常打开
        assert_eq!(Log::open(&dir, 0, [1u8; 32]).unwrap().last_index(), 2);
    }

    #[test]
    fn shard_mismatch_is_fatal() {
        let dir = tmp_dir("shard");
        {
            let mut log = Log::open(&dir, 1, [0u8; 32]).unwrap();
            log.append_batch(&[entry(1, 1, "a")]).unwrap();
        }
        // 把 shard-1 的日志文件错放到 shard-2 目录(模拟分片号被改/目录错配)
        std::fs::create_dir_all(dir.join("shard-2")).unwrap();
        std::fs::copy(
            dir.join("shard-1").join("log"),
            dir.join("shard-2").join("log"),
        )
        .unwrap();
        let err = Log::open(&dir, 2, [0u8; 32]).unwrap_err();
        assert!(matches!(err, HaError::Corrupt(_)), "分片号不符必须拒绝: {err}");
    }
}
