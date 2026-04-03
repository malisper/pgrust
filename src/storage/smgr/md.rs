//! Magnetic disk storage manager — mirrors PostgreSQL's `md.c`.
//!
//! This is the sole concrete implementation of the `StorageManager` trait.
//! It translates the abstract API into filesystem operations via `std::fs`
//! and `std::io`.

use super::*;

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

#[cfg(unix)]
use std::os::unix::io::AsRawFd;

#[cfg(target_os = "linux")]
extern crate libc;

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

/// Handle to an open segment file.
///
/// Mirrors the `MdfdVec` struct in `md.c`.
struct OpenSeg {
    file: File,
    #[allow(dead_code)]
    segno: u32,
}

/// Key for the open-file cache: relation + fork + segment number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct SegKey {
    rel: RelFileLocator,
    fork: ForkNumber,
    segno: u32,
}

// ---------------------------------------------------------------------------
// MdStorageManager
// ---------------------------------------------------------------------------

/// The "magnetic disk" storage manager.
///
/// This is the Rust equivalent of PostgreSQL's `md.c`. It translates the
/// abstract `StorageManager` API into filesystem operations.
pub struct MdStorageManager {
    base_dir: PathBuf,
    open_segs: HashMap<SegKey, OpenSeg>,
    pub in_recovery: bool,
}

impl MdStorageManager {
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        MdStorageManager {
            base_dir: base_dir.into(),
            open_segs: HashMap::new(),
            in_recovery: false,
        }
    }

    pub fn new_in_recovery(base_dir: impl Into<PathBuf>) -> Self {
        MdStorageManager {
            base_dir: base_dir.into(),
            open_segs: HashMap::new(),
            in_recovery: true,
        }
    }

    /// Close all open file handles without destroying any relation state.
    ///
    /// Rust equivalent of `PROCSIGNAL_BARRIER_SMGRRELEASE`.
    pub fn release_all(&mut self) {
        self.open_segs.clear();
    }

    fn seg_path(&self, rel: RelFileLocator, fork: ForkNumber, segno: u32) -> PathBuf {
        segment_path(&self.base_dir, rel, fork, segno)
    }

    fn db_dir(&self, rel: RelFileLocator) -> PathBuf {
        self.base_dir.join(rel.db_oid.to_string())
    }

    /// Open (or retrieve from cache) a specific segment file.
    /// Rust analogue of `_mdfd_getseg()`.
    fn get_seg(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        segno: u32,
    ) -> Result<&mut OpenSeg, SmgrError> {
        let key = SegKey { rel, fork, segno };

        if !self.open_segs.contains_key(&key) {
            let path = self.seg_path(rel, fork, segno);
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .map_err(|e| {
                    if e.kind() == io::ErrorKind::NotFound {
                        SmgrError::RelationNotFound { rel, fork }
                    } else {
                        SmgrError::Io(e)
                    }
                })?;
            self.open_segs.insert(key, OpenSeg { file, segno });
        }

        Ok(self.open_segs.get_mut(&key).unwrap())
    }

    /// Open (or retrieve from cache) a segment, creating it if needed.
    fn get_or_create_seg(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        segno: u32,
    ) -> Result<&mut OpenSeg, SmgrError> {
        let key = SegKey { rel, fork, segno };

        if !self.open_segs.contains_key(&key) {
            let path = self.seg_path(rel, fork, segno);
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)?;
            self.open_segs.insert(key, OpenSeg { file, segno });
        }

        Ok(self.open_segs.get_mut(&key).unwrap())
    }

    /// Count total blocks across all segment files for a relation fork.
    fn count_blocks(
        &self,
        rel: RelFileLocator,
        fork: ForkNumber,
    ) -> Result<BlockNumber, SmgrError> {
        let mut total: BlockNumber = 0;

        for segno in 0.. {
            let path = self.seg_path(rel, fork, segno);
            match fs::metadata(&path) {
                Ok(meta) => {
                    let block_count = (meta.len() / BLCKSZ as u64) as u32;
                    total += block_count;
                    if block_count < RELSEG_SIZE {
                        break;
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => break,
                Err(e) => return Err(SmgrError::Io(e)),
            }
        }

        Ok(total)
    }

    /// Deactivate segments beyond `start_segno` by truncating to 0 bytes.
    /// Mirrors Postgres md.c's behavior for mdtruncate.
    fn deactivate_segments_from(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        start_segno: u32,
    ) {
        for segno in start_segno.. {
            let path = self.seg_path(rel, fork, segno);
            if !path.exists() {
                break;
            }

            let key = SegKey { rel, fork, segno };
            self.open_segs.remove(&key);

            match OpenOptions::new().write(true).open(&path) {
                Ok(f) => {
                    if let Err(e) = f.set_len(0) {
                        eprintln!("WARNING: could not deactivate {:?}: {}", path, e);
                    }
                }
                Err(e) => {
                    eprintln!("WARNING: could not open for deactivation {:?}: {}", path, e);
                }
            }
        }
    }

    /// Remove all segment files for one fork starting from `start_segno`.
    fn remove_segments_from(&mut self, rel: RelFileLocator, fork: ForkNumber, start_segno: u32) {
        for segno in start_segno.. {
            let key = SegKey { rel, fork, segno };
            self.open_segs.remove(&key);

            let path = self.seg_path(rel, fork, segno);
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => break,
                Err(e) => {
                    eprintln!("WARNING: could not remove {:?}: {}", path, e);
                    break;
                }
            }
        }
    }
}

impl StorageManager for MdStorageManager {
    fn open(&mut self, rel: RelFileLocator) -> Result<(), SmgrError> {
        let dir = self.db_dir(rel);
        fs::create_dir_all(&dir)?;
        Ok(())
    }

    fn close(&mut self, rel: RelFileLocator, fork: ForkNumber) -> Result<(), SmgrError> {
        self.open_segs
            .retain(|key, _| !(key.rel == rel && key.fork == fork));
        Ok(())
    }

    fn create(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        is_redo: bool,
    ) -> Result<(), SmgrError> {
        let dir = self.db_dir(rel);
        fs::create_dir_all(&dir)?;

        let path = self.seg_path(rel, fork, 0);

        let result = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path);

        let file = match result {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                if is_redo {
                    OpenOptions::new().read(true).write(true).open(&path)?
                } else {
                    return Err(SmgrError::AlreadyExists { rel, fork });
                }
            }
            Err(e) => return Err(SmgrError::Io(e)),
        };

        let key = SegKey {
            rel,
            fork,
            segno: 0,
        };
        self.open_segs.insert(key, OpenSeg { file, segno: 0 });

        Ok(())
    }

    fn exists(&mut self, rel: RelFileLocator, fork: ForkNumber) -> bool {
        if !self.in_recovery {
            let _ = self.close(rel, fork);
        }
        self.seg_path(rel, fork, 0).exists()
    }

    fn unlink(&mut self, rel: RelFileLocator, fork: Option<ForkNumber>, _is_redo: bool) {
        let forks: Vec<ForkNumber> = match fork {
            Some(f) => vec![f],
            None => vec![
                ForkNumber::Main,
                ForkNumber::Fsm,
                ForkNumber::VisibilityMap,
                ForkNumber::Init,
            ],
        };

        for f in forks {
            self.remove_segments_from(rel, f, 0);
        }
    }

    fn read_block(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        block: BlockNumber,
        buf: &mut [u8],
    ) -> Result<(), SmgrError> {
        if buf.len() != BLCKSZ {
            return Err(SmgrError::BadBufferSize { size: buf.len() });
        }

        let (segno, seg_offset) = seg_for_block(block);
        let byte_offset = seg_offset as u64 * BLCKSZ as u64;

        let seg = self.get_seg(rel, fork, segno)?;
        seg.file.seek(SeekFrom::Start(byte_offset))?;

        let n = seg.file.read(buf)?;
        if n != BLCKSZ {
            return Err(SmgrError::BlockOutOfRange { rel, fork, block });
        }

        Ok(())
    }

    fn write_block(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        block: BlockNumber,
        data: &[u8],
        _skip_fsync: bool,
    ) -> Result<(), SmgrError> {
        if data.len() != BLCKSZ {
            return Err(SmgrError::BadBufferSize { size: data.len() });
        }

        let (segno, seg_offset) = seg_for_block(block);
        let byte_offset = seg_offset as u64 * BLCKSZ as u64;

        let seg = self.get_seg(rel, fork, segno)?;
        seg.file.seek(SeekFrom::Start(byte_offset))?;

        let n = seg.file.write(data)?;
        if n != BLCKSZ {
            return Err(SmgrError::ShortIo {
                expected: BLCKSZ,
                actual: n,
            });
        }

        Ok(())
    }

    fn writeback(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        block: BlockNumber,
        nblocks: u32,
    ) -> Result<(), SmgrError> {
        if nblocks == 0 {
            return Ok(());
        }

        let first_seg = block / RELSEG_SIZE;
        let last_block = block.saturating_add(nblocks - 1);
        let last_seg = last_block / RELSEG_SIZE;

        for segno in first_seg..=last_seg {
            let key = SegKey { rel, fork, segno };
            if let Some(seg) = self.open_segs.get_mut(&key) {
                seg.file.sync_data()?;
            }
        }

        Ok(())
    }

    fn prefetch(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        block: BlockNumber,
        nblocks: u32,
    ) -> Result<(), SmgrError> {
        #[cfg(target_os = "linux")]
        {
            if nblocks == 0 {
                return Ok(());
            }
            let first_seg = block / RELSEG_SIZE;
            let last_seg = block.saturating_add(nblocks - 1) / RELSEG_SIZE;

            for segno in first_seg..=last_seg {
                let seg_start_block = segno * RELSEG_SIZE;
                let local_first = block.saturating_sub(seg_start_block);
                let local_last =
                    (block + nblocks - 1).min(seg_start_block + RELSEG_SIZE - 1) - seg_start_block;
                let offset = local_first as i64 * BLCKSZ as i64;
                let len = ((local_last - local_first + 1) as i64) * BLCKSZ as i64;

                let seg = match self.get_seg(rel, fork, segno) {
                    Ok(s) => s,
                    Err(_) => break,
                };

                let fd = seg.file.as_raw_fd();
                unsafe {
                    libc::posix_fadvise(fd, offset, len, libc::POSIX_FADV_WILLNEED);
                }
            }
        }
        let _ = (rel, fork, block, nblocks);
        Ok(())
    }

    fn max_combine(&self, _rel: RelFileLocator, _fork: ForkNumber, block: BlockNumber) -> u32 {
        let remaining_in_seg = RELSEG_SIZE - (block % RELSEG_SIZE);
        remaining_in_seg.min(MAX_IO_COMBINE_LIMIT)
    }

    #[cfg(unix)]
    fn fd(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        block: BlockNumber,
    ) -> Result<(i32, u64), SmgrError> {
        let (segno, seg_offset) = seg_for_block(block);
        let byte_offset = seg_offset as u64 * BLCKSZ as u64;
        let seg = self.get_seg(rel, fork, segno)?;
        Ok((seg.file.as_raw_fd(), byte_offset))
    }

    fn extend(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        block: BlockNumber,
        data: &[u8],
        _skip_fsync: bool,
    ) -> Result<(), SmgrError> {
        if data.len() != BLCKSZ {
            return Err(SmgrError::BadBufferSize { size: data.len() });
        }

        let (segno, seg_offset) = seg_for_block(block);
        let byte_offset = seg_offset as u64 * BLCKSZ as u64;

        let seg = self.get_or_create_seg(rel, fork, segno)?;
        seg.file.seek(SeekFrom::Start(byte_offset))?;
        let n = seg.file.write(data)?;
        if n != BLCKSZ {
            return Err(SmgrError::ShortIo {
                expected: BLCKSZ,
                actual: n,
            });
        }

        Ok(())
    }

    fn zero_extend(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        block: BlockNumber,
        nblocks: u32,
        skip_fsync: bool,
    ) -> Result<(), SmgrError> {
        let zero_page = vec![0u8; BLCKSZ];
        for i in 0..nblocks {
            self.extend(rel, fork, block + i, &zero_page, skip_fsync)?;
        }
        Ok(())
    }

    fn nblocks(&mut self, rel: RelFileLocator, fork: ForkNumber) -> Result<BlockNumber, SmgrError> {
        self.count_blocks(rel, fork)
    }

    fn truncate(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        nblocks: BlockNumber,
    ) -> Result<(), SmgrError> {
        let (target_seg, target_byte_len) = if nblocks == 0 {
            (0u32, 0u64)
        } else {
            let last_block = nblocks - 1;
            let seg = last_block / RELSEG_SIZE;
            let blocks_in_seg = (last_block % RELSEG_SIZE) + 1;
            (seg, blocks_in_seg as u64 * BLCKSZ as u64)
        };

        let key = SegKey {
            rel,
            fork,
            segno: target_seg,
        };
        self.open_segs.remove(&key);

        let path = self.seg_path(rel, fork, target_seg);
        if path.exists() {
            let file = OpenOptions::new().read(true).write(true).open(&path)?;
            file.set_len(target_byte_len)?;
            self.open_segs.insert(
                key,
                OpenSeg {
                    file,
                    segno: target_seg,
                },
            );
        }

        if self.in_recovery {
            self.remove_segments_from(rel, fork, target_seg + 1);
        } else {
            self.deactivate_segments_from(rel, fork, target_seg + 1);
        }

        Ok(())
    }

    fn immedsync(&mut self, rel: RelFileLocator, fork: ForkNumber) -> Result<(), SmgrError> {
        for segno in 0.. {
            let path = self.seg_path(rel, fork, segno);
            if !path.exists() {
                break;
            }

            let key = SegKey { rel, fork, segno };
            if !self.open_segs.contains_key(&key) {
                let file = OpenOptions::new().read(true).write(true).open(&path)?;
                self.open_segs.insert(key, OpenSeg { file, segno });
            }

            let seg = self.open_segs.get_mut(&key).unwrap();
            seg.file.sync_all()?;
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    fn temp_smgr(label: &str) -> (MdStorageManager, PathBuf) {
        let base = env::temp_dir().join(format!("pgrust_smgr_test_{}", label));
        let _ = fs::remove_dir_all(&base);
        fs::create_dir_all(&base).unwrap();
        (MdStorageManager::new(&base), base)
    }

    fn test_rel(n: u32) -> RelFileLocator {
        RelFileLocator {
            spc_oid: 0,
            db_oid: 1,
            rel_number: n,
        }
    }

    fn page_pattern(block: u32) -> Vec<u8> {
        (0..BLCKSZ)
            .map(|i| ((block as usize * 7 + i) % 251) as u8)
            .collect()
    }

    #[test]
    fn test_create_and_exists() {
        let (mut smgr, _base) = temp_smgr("create_exists");
        let rel = test_rel(1000);

        smgr.open(rel).unwrap();
        assert!(!smgr.exists(rel, ForkNumber::Main));

        smgr.create(rel, ForkNumber::Main, false).unwrap();
        assert!(smgr.exists(rel, ForkNumber::Main));
        assert_eq!(smgr.nblocks(rel, ForkNumber::Main).unwrap(), 0);
    }

    #[test]
    fn test_create_duplicate_fails() {
        let (mut smgr, _base) = temp_smgr("create_duplicate");
        let rel = test_rel(1001);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();

        let err = smgr.create(rel, ForkNumber::Main, false);
        assert!(matches!(err, Err(SmgrError::AlreadyExists { .. })));
    }

    #[test]
    fn test_create_redo_idempotent() {
        let (mut smgr, _base) = temp_smgr("create_redo");
        let rel = test_rel(1002);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();
        smgr.create(rel, ForkNumber::Main, true).unwrap();
        assert!(smgr.exists(rel, ForkNumber::Main));
    }

    #[test]
    fn test_extend_nblocks() {
        let (mut smgr, _base) = temp_smgr("extend_nblocks");
        let rel = test_rel(2000);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();

        for i in 0..5u32 {
            let data = page_pattern(i);
            smgr.extend(rel, ForkNumber::Main, i, &data, true).unwrap();
        }

        assert_eq!(smgr.nblocks(rel, ForkNumber::Main).unwrap(), 5);
    }

    #[test]
    fn test_zero_extend() {
        let (mut smgr, _base) = temp_smgr("zero_extend");
        let rel = test_rel(2001);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();
        smgr.zero_extend(rel, ForkNumber::Main, 0, 10, true)
            .unwrap();

        assert_eq!(smgr.nblocks(rel, ForkNumber::Main).unwrap(), 10);

        let mut buf = vec![0u8; BLCKSZ];
        for i in 0..10u32 {
            smgr.read_block(rel, ForkNumber::Main, i, &mut buf).unwrap();
            assert!(buf.iter().all(|&b| b == 0), "block {i} not zero");
        }
    }

    #[test]
    fn test_write_read_roundtrip() {
        let (mut smgr, _base) = temp_smgr("write_read");
        let rel = test_rel(3000);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();

        for i in 0..3u32 {
            let data = page_pattern(i);
            smgr.extend(rel, ForkNumber::Main, i, &data, true).unwrap();
        }

        let mut buf = vec![0u8; BLCKSZ];
        for i in 0..3u32 {
            smgr.read_block(rel, ForkNumber::Main, i, &mut buf).unwrap();
            assert_eq!(buf, page_pattern(i), "block {i} data mismatch after read");
        }
    }

    #[test]
    fn test_overwrite_block() {
        let (mut smgr, _base) = temp_smgr("overwrite");
        let rel = test_rel(3001);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();

        let original = page_pattern(1);
        smgr.extend(rel, ForkNumber::Main, 0, &original, true)
            .unwrap();

        let new_data = page_pattern(42);
        smgr.write_block(rel, ForkNumber::Main, 0, &new_data, true)
            .unwrap();

        let mut buf = vec![0u8; BLCKSZ];
        smgr.read_block(rel, ForkNumber::Main, 0, &mut buf).unwrap();
        assert_eq!(buf, new_data);
    }

    #[test]
    fn test_read_missing_block() {
        let (mut smgr, _base) = temp_smgr("read_missing");
        let rel = test_rel(3002);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();

        let mut buf = vec![0u8; BLCKSZ];
        let err = smgr.read_block(rel, ForkNumber::Main, 0, &mut buf);
        assert!(matches!(err, Err(SmgrError::BlockOutOfRange { .. })));
    }

    #[test]
    fn test_multiple_forks_independent() {
        let (mut smgr, _base) = temp_smgr("multi_fork");
        let rel = test_rel(4000);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();
        smgr.create(rel, ForkNumber::Fsm, false).unwrap();

        let main_data = page_pattern(10);
        let fsm_data = page_pattern(20);

        smgr.extend(rel, ForkNumber::Main, 0, &main_data, true)
            .unwrap();
        smgr.extend(rel, ForkNumber::Fsm, 0, &fsm_data, true)
            .unwrap();

        let mut buf = vec![0u8; BLCKSZ];

        smgr.read_block(rel, ForkNumber::Main, 0, &mut buf).unwrap();
        assert_eq!(buf, main_data, "main fork block mismatch");

        smgr.read_block(rel, ForkNumber::Fsm, 0, &mut buf).unwrap();
        assert_eq!(buf, fsm_data, "FSM fork block mismatch");
    }

    #[test]
    fn test_truncate() {
        let (mut smgr, _base) = temp_smgr("truncate");
        let rel = test_rel(5000);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();

        for i in 0..10u32 {
            smgr.extend(rel, ForkNumber::Main, i, &page_pattern(i), true)
                .unwrap();
        }
        assert_eq!(smgr.nblocks(rel, ForkNumber::Main).unwrap(), 10);

        smgr.truncate(rel, ForkNumber::Main, 4).unwrap();
        assert_eq!(smgr.nblocks(rel, ForkNumber::Main).unwrap(), 4);

        let mut buf = vec![0u8; BLCKSZ];
        for i in 0..4u32 {
            smgr.read_block(rel, ForkNumber::Main, i, &mut buf).unwrap();
            assert_eq!(buf, page_pattern(i), "block {i} wrong after truncate");
        }

        let err = smgr.read_block(rel, ForkNumber::Main, 4, &mut buf);
        assert!(matches!(err, Err(SmgrError::BlockOutOfRange { .. })));
    }

    #[test]
    fn test_truncate_to_zero() {
        let (mut smgr, _base) = temp_smgr("truncate_zero");
        let rel = test_rel(5001);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();
        smgr.zero_extend(rel, ForkNumber::Main, 0, 5, true).unwrap();

        smgr.truncate(rel, ForkNumber::Main, 0).unwrap();
        assert_eq!(smgr.nblocks(rel, ForkNumber::Main).unwrap(), 0);
    }

    #[test]
    fn test_segment_arithmetic() {
        assert_eq!(seg_for_block(0), (0, 0));
        assert_eq!(seg_for_block(RELSEG_SIZE - 1), (0, RELSEG_SIZE - 1));
        assert_eq!(seg_for_block(RELSEG_SIZE), (1, 0));
        assert_eq!(seg_for_block(2 * RELSEG_SIZE), (2, 0));
        assert_eq!(seg_for_block(2 * RELSEG_SIZE + 5), (2, 5));
    }

    #[test]
    fn test_unlink() {
        let (mut smgr, _base) = temp_smgr("unlink");
        let rel = test_rel(6000);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();
        smgr.zero_extend(rel, ForkNumber::Main, 0, 3, true).unwrap();
        assert!(smgr.exists(rel, ForkNumber::Main));

        smgr.unlink(rel, Some(ForkNumber::Main), false);
        assert!(!smgr.exists(rel, ForkNumber::Main));
    }

    #[test]
    fn test_unlink_all_forks() {
        let (mut smgr, _base) = temp_smgr("unlink_all");
        let rel = test_rel(6001);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();
        smgr.create(rel, ForkNumber::Fsm, false).unwrap();

        smgr.unlink(rel, None, false);

        assert!(!smgr.exists(rel, ForkNumber::Main));
        assert!(!smgr.exists(rel, ForkNumber::Fsm));
    }

    #[test]
    fn test_close_and_reopen() {
        let (mut smgr, _base) = temp_smgr("close_reopen");
        let rel = test_rel(7000);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();
        smgr.extend(rel, ForkNumber::Main, 0, &page_pattern(99), true)
            .unwrap();

        smgr.close(rel, ForkNumber::Main).unwrap();

        let mut buf = vec![0u8; BLCKSZ];
        smgr.read_block(rel, ForkNumber::Main, 0, &mut buf).unwrap();
        assert_eq!(buf, page_pattern(99));
    }

    #[test]
    fn test_path_construction() {
        let base = PathBuf::from("/pgdata/base");
        let rel = RelFileLocator {
            spc_oid: 0,
            db_oid: 5,
            rel_number: 16384,
        };

        assert_eq!(
            segment_path(&base, rel, ForkNumber::Main, 0),
            PathBuf::from("/pgdata/base/5/16384")
        );
        assert_eq!(
            segment_path(&base, rel, ForkNumber::Main, 2),
            PathBuf::from("/pgdata/base/5/16384.2")
        );
        assert_eq!(
            segment_path(&base, rel, ForkNumber::Fsm, 0),
            PathBuf::from("/pgdata/base/5/16384_fsm")
        );
        assert_eq!(
            segment_path(&base, rel, ForkNumber::VisibilityMap, 1),
            PathBuf::from("/pgdata/base/5/16384_vm.1")
        );
        assert_eq!(
            segment_path(&base, rel, ForkNumber::Init, 0),
            PathBuf::from("/pgdata/base/5/16384_init")
        );
    }

    #[test]
    fn test_immedsync() {
        let (mut smgr, _base) = temp_smgr("immedsync");
        let rel = test_rel(8000);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();
        smgr.zero_extend(rel, ForkNumber::Main, 0, 3, true).unwrap();

        smgr.immedsync(rel, ForkNumber::Main).unwrap();
    }

    #[test]
    fn test_bad_buffer_size() {
        let (mut smgr, _base) = temp_smgr("bad_buf");
        let rel = test_rel(9000);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();

        let mut small_buf = vec![0u8; 100];
        let err = smgr.read_block(rel, ForkNumber::Main, 0, &mut small_buf);
        assert!(matches!(err, Err(SmgrError::BadBufferSize { .. })));

        let small_data = vec![0u8; 100];
        let err = smgr.extend(rel, ForkNumber::Main, 0, &small_data, true);
        assert!(matches!(err, Err(SmgrError::BadBufferSize { .. })));
    }

    #[test]
    fn test_max_combine() {
        let smgr = MdStorageManager::new("/tmp");
        let rel = test_rel(0);

        assert_eq!(
            smgr.max_combine(rel, ForkNumber::Main, 0),
            MAX_IO_COMBINE_LIMIT
        );
        assert_eq!(smgr.max_combine(rel, ForkNumber::Main, RELSEG_SIZE - 1), 1);
        assert_eq!(smgr.max_combine(rel, ForkNumber::Main, RELSEG_SIZE - 2), 2);
        assert_eq!(
            smgr.max_combine(rel, ForkNumber::Main, RELSEG_SIZE),
            MAX_IO_COMBINE_LIMIT
        );
    }

    #[test]
    fn test_prefetch_no_error() {
        let (mut smgr, _base) = temp_smgr("prefetch");
        let rel = test_rel(10000);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();
        smgr.zero_extend(rel, ForkNumber::Main, 0, 4, true).unwrap();

        smgr.prefetch(rel, ForkNumber::Main, 0, 4).unwrap();
    }

    #[test]
    #[cfg(unix)]
    fn test_fd() {
        let (mut smgr, _base) = temp_smgr("fd");
        let rel = test_rel(11000);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();
        smgr.zero_extend(rel, ForkNumber::Main, 0, 3, true).unwrap();

        let (fd0, off0) = smgr.fd(rel, ForkNumber::Main, 0).unwrap();
        assert!(fd0 >= 0, "expected valid fd for block 0");
        assert_eq!(off0, 0);

        let (fd2, off2) = smgr.fd(rel, ForkNumber::Main, 2).unwrap();
        assert!(fd2 >= 0, "expected valid fd for block 2");
        assert_eq!(off2, 2 * BLCKSZ as u64);

        assert_eq!(fd0, fd2);
    }

    #[test]
    fn test_recovery_mode_exists_skips_close() {
        let base_dir = {
            let b = std::env::temp_dir().join("pgrust_smgr_test_recovery_exists");
            let _ = fs::remove_dir_all(&b);
            fs::create_dir_all(&b).unwrap();
            b
        };
        let mut smgr = MdStorageManager::new_in_recovery(&base_dir);
        let rel = test_rel(12000);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();
        assert!(smgr.exists(rel, ForkNumber::Main));
    }

    #[test]
    fn test_non_recovery_exists_closes_first() {
        let (mut smgr, _base) = temp_smgr("exists_closes");
        let rel = test_rel(12001);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();
        assert!(smgr.exists(rel, ForkNumber::Main));

        smgr.unlink(rel, Some(ForkNumber::Main), false);
        assert!(!smgr.exists(rel, ForkNumber::Main));
    }

    #[test]
    fn test_truncate_leaves_inactive_segments() {
        let (mut smgr, base) = temp_smgr("inactive_segs");

        let rel = test_rel(13000);
        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();

        let seg1_path = base.join("1").join("13000.1");
        fs::create_dir_all(base.join("1")).unwrap();
        let mut f = fs::File::create(&seg1_path).unwrap();
        f.write_all(&vec![0u8; BLCKSZ * 3]).unwrap();
        drop(f);

        smgr.truncate(rel, ForkNumber::Main, 0).unwrap();

        let meta = fs::metadata(&seg1_path).unwrap();
        assert_eq!(
            meta.len(),
            0,
            "truncated segment should be 0 bytes (inactive), not removed"
        );
    }

    #[test]
    fn test_truncate_recovery_removes_segments() {
        let base = std::env::temp_dir().join("pgrust_smgr_test_truncate_recovery");
        let _ = fs::remove_dir_all(&base);
        fs::create_dir_all(&base).unwrap();

        let mut smgr = MdStorageManager::new_in_recovery(&base);
        let rel = test_rel(13001);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();

        let seg1_path = base.join("1").join("13001.1");
        fs::create_dir_all(base.join("1")).unwrap();
        let mut f = fs::File::create(&seg1_path).unwrap();
        f.write_all(&vec![0u8; BLCKSZ]).unwrap();
        drop(f);

        smgr.truncate(rel, ForkNumber::Main, 0).unwrap();
        assert!(
            !seg1_path.exists(),
            "recovery truncate should remove excess segments"
        );
    }

    #[test]
    fn test_release_all() {
        let (mut smgr, _base) = temp_smgr("release_all");
        let rel = test_rel(14000);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();
        smgr.extend(rel, ForkNumber::Main, 0, &page_pattern(1), true)
            .unwrap();

        let mut buf = vec![0u8; BLCKSZ];
        smgr.read_block(rel, ForkNumber::Main, 0, &mut buf).unwrap();
        assert!(!smgr.open_segs.is_empty(), "should have cached handles");

        smgr.release_all();
        assert!(
            smgr.open_segs.is_empty(),
            "release_all should clear all handles"
        );

        smgr.read_block(rel, ForkNumber::Main, 0, &mut buf).unwrap();
        assert_eq!(
            buf,
            page_pattern(1),
            "data should be intact after release_all"
        );
    }

    // -----------------------------------------------------------------------
    // Crash simulation tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_crash_after_write() {
        let (mut smgr, base) = temp_smgr("crash_after_write");
        let rel = test_rel(20000);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();
        for i in 0..5u32 {
            smgr.extend(rel, ForkNumber::Main, i, &page_pattern(i), true)
                .unwrap();
        }

        drop(smgr);

        let mut smgr2 = MdStorageManager::new(&base);

        assert_eq!(
            smgr2.nblocks(rel, ForkNumber::Main).unwrap(),
            5,
            "nblocks should survive crash"
        );

        let mut buf = vec![0u8; BLCKSZ];
        for i in 0..5u32 {
            smgr2
                .read_block(rel, ForkNumber::Main, i, &mut buf)
                .unwrap();
            assert_eq!(buf, page_pattern(i), "block {} data wrong after crash", i);
        }
    }

    #[test]
    fn test_crash_torn_write() {
        let (mut smgr, base) = temp_smgr("crash_torn_write");
        let rel = test_rel(20001);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();
        for i in 0..3u32 {
            smgr.extend(rel, ForkNumber::Main, i, &page_pattern(i), true)
                .unwrap();
        }
        drop(smgr);

        let seg_path = base.join("1").join("20001");
        let partial_size = 3 * BLCKSZ as u64 + 100;
        {
            let f = fs::OpenOptions::new().write(true).open(&seg_path).unwrap();
            f.set_len(partial_size).unwrap();
        }
        assert_eq!(fs::metadata(&seg_path).unwrap().len(), partial_size);

        let mut smgr2 = MdStorageManager::new(&base);

        assert_eq!(
            smgr2.nblocks(rel, ForkNumber::Main).unwrap(),
            3,
            "nblocks should floor to complete blocks after torn write"
        );

        let mut buf = vec![0u8; BLCKSZ];
        for i in 0..3u32 {
            smgr2
                .read_block(rel, ForkNumber::Main, i, &mut buf)
                .unwrap();
            assert_eq!(buf, page_pattern(i), "block {} wrong after torn write", i);
        }

        let err = smgr2.read_block(rel, ForkNumber::Main, 3, &mut buf);
        assert!(
            matches!(err, Err(SmgrError::BlockOutOfRange { .. })),
            "reading partial block should return BlockOutOfRange"
        );
    }

    #[test]
    fn test_crash_after_truncate_inactive_segments() {
        let (mut smgr, base) = temp_smgr("crash_after_truncate");
        let rel = test_rel(20002);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();
        for i in 0..5u32 {
            smgr.extend(rel, ForkNumber::Main, i, &page_pattern(i), true)
                .unwrap();
        }

        let seg1_path = base.join("1").join("20002.1");
        {
            let mut f = fs::File::create(&seg1_path).unwrap();
            f.write_all(&page_pattern(99).repeat(3)).unwrap();
        }
        assert_eq!(fs::metadata(&seg1_path).unwrap().len(), 3 * BLCKSZ as u64,);

        smgr.truncate(rel, ForkNumber::Main, 3).unwrap();
        assert_eq!(fs::metadata(&seg1_path).unwrap().len(), 0);

        drop(smgr);

        let mut smgr2 = MdStorageManager::new(&base);

        assert!(seg1_path.exists());
        assert_eq!(fs::metadata(&seg1_path).unwrap().len(), 0);

        assert_eq!(
            smgr2.nblocks(rel, ForkNumber::Main).unwrap(),
            3,
            "inactive zero-length segment should not contribute to nblocks"
        );

        let mut buf = vec![0u8; BLCKSZ];
        for i in 0..3u32 {
            smgr2
                .read_block(rel, ForkNumber::Main, i, &mut buf)
                .unwrap();
            assert_eq!(
                buf,
                page_pattern(i),
                "block {} wrong after crash+truncate",
                i
            );
        }
    }
}
