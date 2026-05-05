//! Magnetic disk storage manager — mirrors PostgreSQL's `md.c`.
//!
//! This is the sole concrete implementation of the `StorageManager` trait.
//! It translates the abstract API into filesystem operations via `std::fs`
//! and `std::io`.

use super::*;

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io;
#[cfg(not(unix))]
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;

use crate::sync::SyncQueue;
use pgrust_core::GLOBAL_TABLESPACE_OID;

#[cfg(unix)]
use std::os::unix::fs::FileExt;
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

type RelForkKey = (RelFileLocator, ForkNumber);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LruEntry {
    key: SegKey,
    generation: u64,
}

// ---------------------------------------------------------------------------
// MdStorageManager
// ---------------------------------------------------------------------------

/// The "magnetic disk" storage manager.
///
/// This is the Rust equivalent of PostgreSQL's `md.c`. It translates the
/// abstract `StorageManager` API into filesystem operations.
/// Default maximum number of open file descriptors for segment files.
/// Matches PG's `max_files_per_process` default (1000). Like PG, this
/// limits how many OS FDs we keep open simultaneously. Files beyond
/// this limit are closed (LRU eviction) and transparently reopened on
/// next access.
const DEFAULT_MAX_OPEN_FDS: usize = 1000;

pub struct MdStorageManager {
    base_dir: PathBuf,
    open_segs: HashMap<SegKey, OpenSeg>,
    open_forks: HashMap<RelForkKey, HashSet<SegKey>>,
    /// LRU order tracker: front = least recently used, back = most recently used.
    /// Like PG's VFD doubly-linked ring, but simpler (VecDeque instead of
    /// intrusive list). Used to evict the LRU file when we hit max_open_fds.
    lru_order: VecDeque<LruEntry>,
    lru_generations: HashMap<SegKey, u64>,
    next_lru_generation: u64,
    /// Maximum total open file descriptors (segment files + external).
    /// Matches PG's `max_files_per_process`.
    max_open_fds: usize,
    /// Number of externally-held file descriptors (sockets, WAL files, etc.)
    /// not managed by the VFD layer but counted against the limit.
    /// Like PG's `numExternalFDs`.
    external_fds: usize,
    pub in_recovery: bool,
    /// Cache of opened relations — avoids mkdir/create_dir_all per insert.
    opened_rels: HashSet<RelFileLocator>,
    /// Cache of created forks — avoids create_new syscall per insert.
    created_forks: HashSet<(RelFileLocator, ForkNumber)>,
    /// Cache of block counts — avoids stat() per insert. Updated on extend.
    nblocks_cache: HashMap<(RelFileLocator, ForkNumber), BlockNumber>,
    sync_queue: Option<Arc<SyncQueue>>,
}

impl MdStorageManager {
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self::new_internal(base_dir.into(), false, None)
    }

    pub fn new_with_sync_queue(base_dir: impl Into<PathBuf>, sync_queue: Arc<SyncQueue>) -> Self {
        Self::new_internal(base_dir.into(), false, Some(sync_queue))
    }

    pub fn new_in_recovery(base_dir: impl Into<PathBuf>) -> Self {
        Self::new_internal(base_dir.into(), true, None)
    }

    pub fn new_in_recovery_with_sync_queue(
        base_dir: impl Into<PathBuf>,
        sync_queue: Arc<SyncQueue>,
    ) -> Self {
        Self::new_internal(base_dir.into(), true, Some(sync_queue))
    }

    fn new_internal(
        base_dir: PathBuf,
        in_recovery: bool,
        sync_queue: Option<Arc<SyncQueue>>,
    ) -> Self {
        MdStorageManager {
            base_dir,
            open_segs: HashMap::new(),
            open_forks: HashMap::new(),
            lru_order: VecDeque::new(),
            lru_generations: HashMap::new(),
            next_lru_generation: 0,
            max_open_fds: DEFAULT_MAX_OPEN_FDS,
            external_fds: 0,
            in_recovery,
            opened_rels: HashSet::new(),
            created_forks: HashSet::new(),
            nblocks_cache: HashMap::new(),
            sync_queue,
        }
    }

    /// Close all open file handles without destroying any relation state.
    ///
    /// Rust equivalent of `PROCSIGNAL_BARRIER_SMGRRELEASE`.
    pub fn release_all(&mut self) {
        self.open_segs.clear();
        self.open_forks.clear();
        self.lru_order.clear();
        self.lru_generations.clear();
    }

    /// Evict the least-recently-used file descriptor to stay under
    /// `max_open_fds`. Like PG's `ReleaseLruFiles()`, considers both
    /// segment files and external FDs (sockets, WAL, etc.) against the limit.
    fn evict_lru(&mut self) {
        while self.open_segs.len() + self.external_fds >= self.max_open_fds {
            if let Some(entry) = self.lru_order.pop_front() {
                if self.lru_generations.get(&entry.key) == Some(&entry.generation) {
                    self.remove_open_seg(&entry.key);
                }
            } else {
                break;
            }
        }
    }

    /// Move a segment key to the MRU (most recently used) position.
    fn touch_lru(&mut self, key: &SegKey) {
        self.next_lru_generation = self.next_lru_generation.wrapping_add(1);
        let generation = self.next_lru_generation;
        self.lru_generations.insert(*key, generation);
        self.lru_order.push_back(LruEntry {
            key: *key,
            generation,
        });
    }

    fn insert_open_seg(&mut self, key: SegKey, seg: OpenSeg) {
        self.open_segs.insert(key, seg);
        self.open_forks
            .entry((key.rel, key.fork))
            .or_default()
            .insert(key);
        self.touch_lru(&key);
    }

    fn remove_open_seg(&mut self, key: &SegKey) -> Option<OpenSeg> {
        let removed = self.open_segs.remove(key);
        if removed.is_some() {
            let fork_key = (key.rel, key.fork);
            let remove_fork = if let Some(keys) = self.open_forks.get_mut(&fork_key) {
                keys.remove(key);
                keys.is_empty()
            } else {
                false
            };
            if remove_fork {
                self.open_forks.remove(&fork_key);
            }
            self.lru_generations.remove(key);
        }
        removed
    }

    /// Register an external file descriptor (socket, WAL file, etc.) that
    /// counts against the open FD limit but is not managed by the VFD layer.
    /// Like PG's `AcquireExternalFD()`.
    pub fn acquire_external_fd(&mut self) {
        self.external_fds += 1;
    }

    /// Release a previously registered external file descriptor.
    /// Like PG's `ReleaseExternalFD()`.
    pub fn release_external_fd(&mut self) {
        self.external_fds = self.external_fds.saturating_sub(1);
    }

    fn seg_path(&self, rel: RelFileLocator, fork: ForkNumber, segno: u32) -> PathBuf {
        segment_path(&self.base_dir, rel, fork, segno)
    }

    fn db_dir(&self, rel: RelFileLocator) -> PathBuf {
        if rel.db_oid == 0 && rel.spc_oid == GLOBAL_TABLESPACE_OID {
            self.base_dir.join("global")
        } else if rel.spc_oid != 0 {
            self.base_dir
                .join("pg_tblspc")
                .join(rel.spc_oid.to_string())
                .join(TABLESPACE_VERSION_DIRECTORY)
                .join(rel.db_oid.to_string())
        } else {
            self.base_dir.join("base").join(rel.db_oid.to_string())
        }
    }

    /// Open (or retrieve from cache) a specific segment file.
    /// Rust analogue of `_mdfd_getseg()`. Like PG's VFD `FileAccess()`,
    /// evicts the LRU file if we're at the FD limit, and moves the
    /// accessed file to the MRU position.
    fn get_seg(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        segno: u32,
    ) -> Result<&mut OpenSeg, SmgrError> {
        let key = SegKey { rel, fork, segno };

        if !self.open_segs.contains_key(&key) {
            self.evict_lru();
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
            self.insert_open_seg(key, OpenSeg { file, segno });
        } else {
            self.touch_lru(&key);
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
            self.evict_lru();
            let path = self.seg_path(rel, fork, segno);
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)?;
            self.insert_open_seg(key, OpenSeg { file, segno });
        } else {
            self.touch_lru(&key);
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
            self.remove_open_seg(&key);

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
            self.remove_open_seg(&key);

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

    fn fork_has_segment_after_zero(&self, rel: RelFileLocator, fork: ForkNumber) -> bool {
        self.seg_path(rel, fork, 1).exists()
    }

    pub fn replace_relation_main_fork_from_shadow(
        &mut self,
        shadow: RelFileLocator,
        target: RelFileLocator,
    ) -> Result<(), SmgrError> {
        if shadow == target {
            return Err(SmgrError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "shadow and target relfiles must differ",
            )));
        }
        if self.fork_has_segment_after_zero(shadow, ForkNumber::Main)
            || self.fork_has_segment_after_zero(target, ForkNumber::Main)
        {
            return Err(SmgrError::Io(io::Error::new(
                io::ErrorKind::Other,
                "atomic relfile replacement only supports single-segment main forks",
            )));
        }

        self.close(shadow, ForkNumber::Main)?;
        self.close(target, ForkNumber::Main)?;

        let shadow_path = self.seg_path(shadow, ForkNumber::Main, 0);
        let target_path = self.seg_path(target, ForkNumber::Main, 0);
        if !shadow_path.exists() {
            return Err(SmgrError::RelationNotFound {
                rel: shadow,
                fork: ForkNumber::Main,
            });
        }

        let shadow_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&shadow_path)?;
        crate::fsync_file(&shadow_file)?;
        drop(shadow_file);

        let parent = target_path.parent().ok_or_else(|| {
            SmgrError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "target relfile has no parent directory",
            ))
        })?;
        crate::fsync_dir(parent)?;
        fs::rename(&shadow_path, &target_path)?;
        crate::fsync_dir(parent)?;

        for fork in [
            ForkNumber::Main,
            ForkNumber::Fsm,
            ForkNumber::VisibilityMap,
            ForkNumber::Init,
        ] {
            self.nblocks_cache.remove(&(shadow, fork));
            self.nblocks_cache.remove(&(target, fork));
            self.created_forks.remove(&(shadow, fork));
        }
        self.opened_rels.remove(&shadow);
        self.created_forks.insert((target, ForkNumber::Main));
        self.opened_rels.insert(target);

        Ok(())
    }
}

#[cfg(unix)]
fn file_read_at(file: &mut File, buf: &mut [u8], byte_offset: u64) -> io::Result<usize> {
    file.read_at(buf, byte_offset)
}

#[cfg(not(unix))]
fn file_read_at(file: &mut File, buf: &mut [u8], byte_offset: u64) -> io::Result<usize> {
    use std::io::Read as _;
    file.seek(SeekFrom::Start(byte_offset))?;
    file.read(buf)
}

#[cfg(unix)]
fn file_write_all_at(file: &mut File, mut data: &[u8], mut byte_offset: u64) -> io::Result<()> {
    while !data.is_empty() {
        let written = match file.write_at(data, byte_offset) {
            Ok(written) => written,
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(err),
        };
        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "failed to write a complete page",
            ));
        }
        data = &data[written..];
        byte_offset += written as u64;
    }
    Ok(())
}

#[cfg(not(unix))]
fn file_write_all_at(file: &mut File, mut data: &[u8], mut byte_offset: u64) -> io::Result<()> {
    file.seek(SeekFrom::Start(byte_offset))?;
    while !data.is_empty() {
        let written = match file.write(data) {
            Ok(written) => written,
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(err),
        };
        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "failed to write a complete page",
            ));
        }
        data = &data[written..];
        byte_offset += written as u64;
        file.seek(SeekFrom::Start(byte_offset))?;
    }
    Ok(())
}

impl StorageManager for MdStorageManager {
    fn open(&mut self, rel: RelFileLocator) -> Result<(), SmgrError> {
        if self.opened_rels.contains(&rel) {
            return Ok(());
        }
        let dir = self.db_dir(rel);
        fs::create_dir_all(&dir)?;
        self.opened_rels.insert(rel);
        Ok(())
    }

    fn close(&mut self, rel: RelFileLocator, fork: ForkNumber) -> Result<(), SmgrError> {
        if let Some(keys) = self.open_forks.remove(&(rel, fork)) {
            for key in keys {
                self.remove_open_seg(&key);
            }
        }
        self.nblocks_cache.remove(&(rel, fork));
        Ok(())
    }

    fn create(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        is_redo: bool,
    ) -> Result<(), SmgrError> {
        if !is_redo && self.created_forks.contains(&(rel, fork)) {
            return Err(SmgrError::AlreadyExists { rel, fork });
        }

        if !self.opened_rels.contains(&rel) {
            let dir = self.db_dir(rel);
            fs::create_dir_all(&dir)?;
            self.opened_rels.insert(rel);
        }

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
        self.evict_lru();
        self.insert_open_seg(key, OpenSeg { file, segno: 0 });
        self.created_forks.insert((rel, fork));

        Ok(())
    }

    fn exists(&mut self, rel: RelFileLocator, fork: ForkNumber) -> bool {
        if !self.in_recovery {
            let _ = self.close(rel, fork);
        }
        self.seg_path(rel, fork, 0).exists()
    }

    fn unlink(&mut self, rel: RelFileLocator, fork: Option<ForkNumber>, _is_redo: bool) {
        if let Some(sync_queue) = self.sync_queue.as_ref() {
            sync_queue.cancel_relation(rel, fork);
        }
        let forks: Vec<ForkNumber> = match fork {
            Some(f) => vec![f],
            None => vec![
                ForkNumber::Main,
                ForkNumber::Fsm,
                ForkNumber::VisibilityMap,
                ForkNumber::Init,
            ],
        };

        for f in &forks {
            self.nblocks_cache.remove(&(rel, *f));
            self.created_forks.remove(&(rel, *f));
        }
        self.opened_rels.remove(&rel);
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
        let n = file_read_at(&mut seg.file, buf, byte_offset)?;
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
        skip_fsync: bool,
    ) -> Result<(), SmgrError> {
        if data.len() != BLCKSZ {
            return Err(SmgrError::BadBufferSize { size: data.len() });
        }

        let (segno, seg_offset) = seg_for_block(block);
        let byte_offset = seg_offset as u64 * BLCKSZ as u64;

        let seg = self.get_seg(rel, fork, segno)?;
        file_write_all_at(&mut seg.file, data, byte_offset)?;

        if !skip_fsync {
            crate::fsync_file(&seg.file)?;
        } else if let Some(sync_queue) = self.sync_queue.as_ref() {
            sync_queue.register(rel, fork);
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
                crate::fsync_file(&seg.file)?;
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
        skip_fsync: bool,
    ) -> Result<(), SmgrError> {
        if data.len() != BLCKSZ {
            return Err(SmgrError::BadBufferSize { size: data.len() });
        }

        let (segno, seg_offset) = seg_for_block(block);
        let byte_offset = seg_offset as u64 * BLCKSZ as u64;

        let seg = self.get_or_create_seg(rel, fork, segno)?;
        file_write_all_at(&mut seg.file, data, byte_offset)?;

        if !skip_fsync {
            crate::fsync_file(&seg.file)?;
        } else if let Some(sync_queue) = self.sync_queue.as_ref() {
            sync_queue.register(rel, fork);
        }

        // Update nblocks cache.
        let entry = self.nblocks_cache.entry((rel, fork)).or_insert(0);
        if block + 1 > *entry {
            *entry = block + 1;
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

    fn reserve_block(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        block: BlockNumber,
        skip_fsync: bool,
    ) -> Result<(), SmgrError> {
        let nblocks = self.nblocks(rel, fork)?;
        if block != nblocks {
            return Err(SmgrError::BlockOutOfRange { rel, fork, block });
        }

        let (segno, seg_offset) = seg_for_block(block);
        let len = (seg_offset as u64 + 1) * BLCKSZ as u64;
        let seg = self.get_or_create_seg(rel, fork, segno)?;
        seg.file.set_len(len)?;

        if !skip_fsync {
            crate::fsync_file(&seg.file)?;
        } else if let Some(sync_queue) = self.sync_queue.as_ref() {
            sync_queue.register(rel, fork);
        }

        self.nblocks_cache.insert((rel, fork), block + 1);
        Ok(())
    }

    fn nblocks(&mut self, rel: RelFileLocator, fork: ForkNumber) -> Result<BlockNumber, SmgrError> {
        if let Some(&cached) = self.nblocks_cache.get(&(rel, fork)) {
            return Ok(cached);
        }
        let n = self.count_blocks(rel, fork)?;
        self.nblocks_cache.insert((rel, fork), n);
        Ok(n)
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
        self.remove_open_seg(&key);

        let path = self.seg_path(rel, fork, target_seg);
        if path.exists() {
            let file = OpenOptions::new().read(true).write(true).open(&path)?;
            file.set_len(target_byte_len)?;
            self.insert_open_seg(
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

        // Update nblocks cache.
        self.nblocks_cache.insert((rel, fork), nblocks);

        if let Some(sync_queue) = self.sync_queue.as_ref() {
            sync_queue.register_truncated_relation(rel, fork);
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
                self.evict_lru();
                self.insert_open_seg(key, OpenSeg { file, segno });
            } else {
                self.touch_lru(&key);
            }

            let seg = self.open_segs.get_mut(&key).unwrap();
            crate::fsync_file(&seg.file)?;
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
    use std::io::Write;

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
    fn test_replace_relation_main_fork_from_shadow() {
        let (mut smgr, base) = temp_smgr("replace_shadow");
        let target = test_rel(6100);
        let shadow = test_rel(6101);

        smgr.open(target).unwrap();
        smgr.create(target, ForkNumber::Main, false).unwrap();
        smgr.extend(target, ForkNumber::Main, 0, &page_pattern(1), true)
            .unwrap();

        smgr.open(shadow).unwrap();
        smgr.create(shadow, ForkNumber::Main, false).unwrap();
        smgr.extend(shadow, ForkNumber::Main, 0, &page_pattern(2), true)
            .unwrap();

        smgr.replace_relation_main_fork_from_shadow(shadow, target)
            .unwrap();

        let target_path = segment_path(&base, target, ForkNumber::Main, 0);
        let shadow_path = segment_path(&base, shadow, ForkNumber::Main, 0);
        assert!(target_path.exists(), "target relfile should remain");
        assert!(!shadow_path.exists(), "shadow relfile should be consumed");

        let mut buf = vec![0u8; BLCKSZ];
        smgr.read_block(target, ForkNumber::Main, 0, &mut buf)
            .unwrap();
        assert_eq!(buf, page_pattern(2));
    }

    #[test]
    fn test_replace_relation_main_fork_rejects_multisegment() {
        let (mut smgr, base) = temp_smgr("replace_shadow_multiseg");
        let target = test_rel(6110);
        let shadow = test_rel(6111);

        smgr.open(target).unwrap();
        smgr.create(target, ForkNumber::Main, false).unwrap();
        smgr.extend(target, ForkNumber::Main, 0, &page_pattern(1), true)
            .unwrap();

        smgr.open(shadow).unwrap();
        smgr.create(shadow, ForkNumber::Main, false).unwrap();
        smgr.extend(shadow, ForkNumber::Main, 0, &page_pattern(2), true)
            .unwrap();

        let shadow_seg1 = segment_path(&base, shadow, ForkNumber::Main, 1);
        fs::write(&shadow_seg1, page_pattern(3)).unwrap();
        let err = smgr
            .replace_relation_main_fork_from_shadow(shadow, target)
            .unwrap_err();
        assert!(matches!(err, SmgrError::Io(_)));

        let mut buf = vec![0u8; BLCKSZ];
        smgr.read_block(target, ForkNumber::Main, 0, &mut buf)
            .unwrap();
        assert_eq!(buf, page_pattern(1));
        assert!(
            segment_path(&base, shadow, ForkNumber::Main, 0).exists(),
            "shadow segment 0 should remain after rejected replacement"
        );

        fs::remove_file(&shadow_seg1).unwrap();
        let target_seg1 = segment_path(&base, target, ForkNumber::Main, 1);
        fs::write(&target_seg1, page_pattern(4)).unwrap();
        let err = smgr
            .replace_relation_main_fork_from_shadow(shadow, target)
            .unwrap_err();
        assert!(matches!(err, SmgrError::Io(_)));
        smgr.read_block(target, ForkNumber::Main, 0, &mut buf)
            .unwrap();
        assert_eq!(buf, page_pattern(1));
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
        let base = PathBuf::from("/pgdata");
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

        let seg1_path = segment_path(&base, rel, ForkNumber::Main, 1);
        fs::create_dir_all(seg1_path.parent().unwrap()).unwrap();
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

        let seg1_path = segment_path(&base, rel, ForkNumber::Main, 1);
        fs::create_dir_all(seg1_path.parent().unwrap()).unwrap();
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

    #[test]
    fn test_exists_closes_only_target_fork_handles() {
        let (mut smgr, _base) = temp_smgr("exists_closes_target");
        let rel1 = test_rel(14100);
        let rel2 = test_rel(14101);

        smgr.open(rel1).unwrap();
        smgr.open(rel2).unwrap();
        smgr.create(rel1, ForkNumber::Main, false).unwrap();
        smgr.create(rel2, ForkNumber::Main, false).unwrap();

        assert!(
            smgr.open_forks.contains_key(&(rel1, ForkNumber::Main)),
            "rel1 should have an open main fork handle"
        );
        assert!(
            smgr.open_forks.contains_key(&(rel2, ForkNumber::Main)),
            "rel2 should have an open main fork handle"
        );

        assert!(smgr.exists(rel1, ForkNumber::Main));
        assert!(
            !smgr.open_forks.contains_key(&(rel1, ForkNumber::Main)),
            "exists should close only the target fork"
        );
        assert!(
            smgr.open_forks.contains_key(&(rel2, ForkNumber::Main)),
            "exists should leave unrelated fork handles cached"
        );
    }

    #[test]
    fn test_close_preserves_other_forks_for_same_relation() {
        let (mut smgr, _base) = temp_smgr("close_preserves_other_forks");
        let rel = test_rel(14102);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();
        smgr.create(rel, ForkNumber::Fsm, false).unwrap();

        smgr.close(rel, ForkNumber::Main).unwrap();
        assert!(!smgr.open_forks.contains_key(&(rel, ForkNumber::Main)));
        assert!(smgr.open_forks.contains_key(&(rel, ForkNumber::Fsm)));
    }

    // -----------------------------------------------------------------------
    // Crash simulation tests
    // -----------------------------------------------------------------------

    /// write_block with skip_fsync=false must call sync_data and not error.
    /// write_block with skip_fsync=true must succeed without syncing.
    #[test]
    fn test_write_block_fsync() {
        let (mut smgr, base) = temp_smgr("write_block_fsync");
        let rel = test_rel(19000);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();

        // extend with skip_fsync=true (no sync — used during batch load)
        smgr.extend(rel, ForkNumber::Main, 0, &page_pattern(1), true)
            .unwrap();

        // write_block with skip_fsync=false: must sync and not error
        smgr.write_block(rel, ForkNumber::Main, 0, &page_pattern(2), false)
            .unwrap();

        // Reopen: data written with fsync=true must be present
        drop(smgr);
        let mut smgr2 = MdStorageManager::new(&base);
        let mut buf = vec![0u8; BLCKSZ];
        smgr2
            .read_block(rel, ForkNumber::Main, 0, &mut buf)
            .unwrap();
        assert_eq!(buf, page_pattern(2), "fsynced write must survive reopen");
    }

    /// extend with skip_fsync=false must sync the new block to disk.
    #[test]
    fn test_extend_fsync() {
        let (mut smgr, base) = temp_smgr("extend_fsync");
        let rel = test_rel(19001);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();

        // extend with skip_fsync=false: must sync each block
        for i in 0..3u32 {
            smgr.extend(rel, ForkNumber::Main, i, &page_pattern(i), false)
                .unwrap();
        }

        drop(smgr);
        let mut smgr2 = MdStorageManager::new(&base);
        assert_eq!(smgr2.nblocks(rel, ForkNumber::Main).unwrap(), 3);
        let mut buf = vec![0u8; BLCKSZ];
        for i in 0..3u32 {
            smgr2
                .read_block(rel, ForkNumber::Main, i, &mut buf)
                .unwrap();
            assert_eq!(
                buf,
                page_pattern(i),
                "block {i} must survive reopen after fsynced extend"
            );
        }
    }

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

        let seg_path = segment_path(&base, rel, ForkNumber::Main, 0);
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

        let seg1_path = segment_path(&base, rel, ForkNumber::Main, 1);
        {
            fs::create_dir_all(seg1_path.parent().unwrap()).unwrap();
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
