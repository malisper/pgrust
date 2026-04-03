//! Storage Manager (smgr) — Rust port of PostgreSQL's smgr/md.c
//!
//! # What this is
//!
//! In PostgreSQL, the storage manager (smgr) is the layer between the buffer
//! manager and the OS filesystem. It is responsible for:
//!
//!   - Opening and closing relation file handles
//!   - Creating and unlinking relation files on disk
//!   - Reading and writing 8-KiB pages (blocks) to/from those files
//!   - Extending a relation to add new blocks
//!   - Truncating a relation to remove trailing blocks
//!   - Syncing dirty data to disk (fsync / fdatasync)
//!   - Reporting how many blocks a relation currently has
//!
//! PostgreSQL's actual smgr has a vtable dispatch layer (`smgr.c`) that sits
//! in front of concrete implementations. Historically there were multiple
//! storage managers, but today only one remains: the "magnetic disk" manager
//! (`md.c`). This module mirrors that structure with a `StorageManager` trait
//! (the vtable) and `MdStorageManager` as the sole implementation.
//!
//! # Key concepts
//!
//! **Relation file locator** — every physical relation is identified by a
//! triple: (tablespace OID, database OID, relation number). This is
//! `RelFileLocator` in Postgres and in this crate.
//!
//! **Fork** — a relation can have multiple on-disk files ("forks") for
//! different purposes:
//!   - `Main` (0): the actual heap or index data
//!   - `Fsm` (1): the Free Space Map
//!   - `VisibilityMap` (2): the Visibility Map
//!   - `Init` (3): unlogged-relation init fork
//!
//! **Block / page** — the unit of I/O is always `BLCKSZ` bytes (8 KiB).
//! A `BlockNumber` is a 0-based index within a fork.
//!
//! **Segment** — because some OSes historically cap file sizes, Postgres
//! splits large relations into segment files. Each segment holds at most
//! `RELSEG_SIZE` blocks (131,072 by default = 1 GiB). Segment 0 is the base
//! file; segment N is named `<base>.<N>`. This module implements the same
//! splitting.
//!
//! # File paths
//!
//! Given a `base_dir`, the file for (spc=0, db=5, rel=16384), main fork,
//! segment 0 is:
//!
//!   `<base_dir>/5/16384`
//!
//! Segment 1 of the same relation:
//!
//!   `<base_dir>/5/16384.1`
//!
//! FSM fork, segment 0:
//!
//!   `<base_dir>/5/16384_fsm`
//!
//! This mirrors PostgreSQL's `relpath()` output (simplified: we always treat
//! spc_oid 0 as the default tablespace and map to `base_dir/db_oid/rel`).
//!
//! # What is intentionally left out
//!
//! The following PostgreSQL features are not yet implemented. See
//! `pgrust/plans/smgr-deferred-features.md` for details.
//!
//!   - **Async I/O** (`smgr_startreadv`, `PgAioHandle`): all reads and
//!     writes are synchronous. Needs a decision on async runtime first.
//!   - **Deferred fsync** (`smgr_registersync`): we use `immedsync` only;
//!     the sync-queue / checkpoint-flush path is not implemented.
//!   - **Non-default tablespace paths**: we use a single `base_dir` and
//!     ignore `spc_oid`. See `pgrust/plans/smgr-deferred-features.md`.
//!   - **The SMgrRelation hash table / pin system** in `smgr.c`: we use a
//!     simple HashMap of file handles instead.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::io::AsRawFd;

#[cfg(target_os = "linux")]
extern crate libc;

// ---------------------------------------------------------------------------
// Public constants
// ---------------------------------------------------------------------------

/// The size of a single database page, in bytes.
/// Matches PostgreSQL's `BLCKSZ` (the default build value).
pub const BLCKSZ: usize = 8192;

/// Number of blocks per segment file.
///
/// PostgreSQL's `RELSEG_SIZE` defaults to 131072 blocks = 1 GiB. We use the
/// same value so that path-construction and segment-arithmetic tests match the
/// real system's behavior.
pub const RELSEG_SIZE: u32 = 131_072;

/// Sentinel value for "no valid block number".
/// Mirrors `InvalidBlockNumber` in `storage/block.h`.
pub const INVALID_BLOCK_NUMBER: u32 = u32::MAX;

/// Maximum number of blocks that can be coalesced into a single vectored I/O.
///
/// Mirrors `MAX_IO_COMBINE_LIMIT` in PostgreSQL's AIO subsystem. Used by
/// `max_combine()` to cap the hint it returns to callers.
pub const MAX_IO_COMBINE_LIMIT: u32 = 64;

// ---------------------------------------------------------------------------
// Core types  (also shared with the buffer-manager module in lib.rs, but
// redefined here so smgr.rs is self-contained and can be used independently)
// ---------------------------------------------------------------------------

/// 0-based index of a page within a relation fork.
/// Mirrors PostgreSQL's `BlockNumber` typedef.
pub type BlockNumber = u32;

/// Identifies the physical on-disk location of a relation.
///
/// In PostgreSQL this is `RelFileLocator` (struct with spcOid, dbOid,
/// relNumber). We store the same three fields.
///
/// - `spc_oid`: tablespace OID (0 = default tablespace, mapped to `base/`)
/// - `db_oid`: database OID
/// - `rel_number`: the relation's file number (not its catalog OID)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RelFileLocator {
    pub spc_oid: u32,
    pub db_oid: u32,
    pub rel_number: u32,
}

/// Which on-disk fork of a relation we are addressing.
///
/// Mirrors PostgreSQL's `ForkNumber` enum in `common/relpath.h`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ForkNumber {
    /// The main data fork (heap pages, index pages, etc.). Fork 0.
    Main = 0,
    /// Free Space Map fork. Fork 1.
    Fsm = 1,
    /// Visibility Map fork. Fork 2.
    VisibilityMap = 2,
    /// Initialization fork for unlogged relations. Fork 3.
    Init = 3,
}

impl ForkNumber {
    /// Returns the suffix appended to the base relation path for this fork.
    ///
    /// Main fork has no suffix (segment 0 is just the bare relation number).
    /// Other forks append `_fsm`, `_vm`, or `_init`.
    fn suffix(self) -> &'static str {
        match self {
            ForkNumber::Main => "",
            ForkNumber::Fsm => "_fsm",
            ForkNumber::VisibilityMap => "_vm",
            ForkNumber::Init => "_init",
        }
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors that can be returned by storage manager operations.
///
/// PostgreSQL uses `ereport(ERROR, ...)` for most smgr failures, which
/// unwinds via longjmp. Rust uses `Result` instead.
#[derive(Debug)]
pub enum SmgrError {
    /// The requested relation fork does not exist.
    RelationNotFound { rel: RelFileLocator, fork: ForkNumber },
    /// A block number is out of the valid range for this relation fork.
    BlockOutOfRange {
        rel: RelFileLocator,
        fork: ForkNumber,
        block: BlockNumber,
    },
    /// A relation already exists when `create` was called without `is_redo`.
    AlreadyExists { rel: RelFileLocator, fork: ForkNumber },
    /// A write or read produced fewer bytes than expected.
    ShortIo {
        expected: usize,
        actual: usize,
    },
    /// The buffer supplied to a read or write is not exactly BLCKSZ bytes.
    BadBufferSize { size: usize },
    /// An underlying OS I/O error.
    Io(io::Error),
}

impl std::fmt::Display for SmgrError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SmgrError::RelationNotFound { rel, fork } =>
                write!(f, "relation {}/{}/{} fork {:?} not found",
                    rel.spc_oid, rel.db_oid, rel.rel_number, fork),
            SmgrError::BlockOutOfRange { rel, fork, block } =>
                write!(f, "block {} out of range for {}/{}/{} fork {:?}",
                    block, rel.spc_oid, rel.db_oid, rel.rel_number, fork),
            SmgrError::AlreadyExists { rel, fork } =>
                write!(f, "relation {}/{}/{} fork {:?} already exists",
                    rel.spc_oid, rel.db_oid, rel.rel_number, fork),
            SmgrError::ShortIo { expected, actual } =>
                write!(f, "short I/O: expected {expected} bytes, got {actual}"),
            SmgrError::BadBufferSize { size } =>
                write!(f, "buffer must be exactly {BLCKSZ} bytes, got {size}"),
            SmgrError::Io(e) =>
                write!(f, "I/O error: {e}"),
        }
    }
}

impl std::error::Error for SmgrError {}

impl From<io::Error> for SmgrError {
    fn from(e: io::Error) -> Self {
        SmgrError::Io(e)
    }
}

// ---------------------------------------------------------------------------
// StorageManager trait  (mirrors the f_smgr vtable in smgr.c)
// ---------------------------------------------------------------------------

/// The storage manager interface.
///
/// This is a Rust translation of the `f_smgr` function-pointer struct in
/// PostgreSQL's `smgr.c`. Every method corresponds to one entry in that struct.
///
/// Methods that Postgres names `smgr_foo` are named `foo` here; the `smgr_`
/// prefix is implied by the trait name.
///
/// ## Omitted methods
///
/// The following `f_smgr` slots are not in this trait (see module-level docs):
///   - `smgr_startreadv`   (async read — needs runtime decision first)
///   - `smgr_registersync` (deferred fsync queue — not yet implemented)
pub trait StorageManager {
    // --- Lifecycle ---

    /// Open the physical files for this relation (but don't read anything yet).
    ///
    /// Corresponds to `smgr_open` / `mdopen`. In PostgreSQL this allocates the
    /// `MdfdVec` arrays inside the `SMgrRelation`; here it pre-creates the
    /// per-db directory so subsequent creates/writes have somewhere to land.
    fn open(&mut self, rel: RelFileLocator) -> Result<(), SmgrError>;

    /// Close file descriptors for a specific fork, releasing OS resources.
    ///
    /// Corresponds to `smgr_close` / `mdclose`. The relation entry itself is
    /// still valid; only OS-level file handles are released.
    fn close(&mut self, rel: RelFileLocator, fork: ForkNumber) -> Result<(), SmgrError>;

    // --- File-level operations ---

    /// Create the first segment file for a relation fork.
    ///
    /// Corresponds to `smgr_create` / `mdcreate`. If `is_redo` is true (WAL
    /// replay), it is not an error if the file already exists.
    fn create(&mut self, rel: RelFileLocator, fork: ForkNumber, is_redo: bool) -> Result<(), SmgrError>;

    /// Return true if the relation fork has at least one segment file on disk.
    ///
    /// In non-recovery mode, the implementation first closes the fork's cached
    /// file handles so that a recently-unlinked file is not falsely reported as
    /// existing. In recovery mode the close is skipped (mirrors `mdexists`).
    ///
    /// Corresponds to `smgr_exists` / `mdexists`.
    fn exists(&mut self, rel: RelFileLocator, fork: ForkNumber) -> bool;

    /// Delete all segment files for a relation fork (or all forks if
    /// `fork` is `None`).
    ///
    /// Corresponds to `smgr_unlink` / `mdunlink`. Errors are logged as
    /// warnings rather than panics because this is called from post-commit
    /// cleanup where we cannot raise a hard error.
    fn unlink(&mut self, rel: RelFileLocator, fork: Option<ForkNumber>, is_redo: bool);

    // --- Block-level I/O ---

    /// Read a single block from a relation fork into `buf`.
    ///
    /// `buf` must be exactly `BLCKSZ` bytes. Returns `SmgrError::BlockOutOfRange`
    /// if the block does not exist.
    ///
    /// Corresponds to `smgr_readv` / `mdreadv` with nblocks=1.
    fn read_block(&mut self, rel: RelFileLocator, fork: ForkNumber, block: BlockNumber, buf: &mut [u8]) -> Result<(), SmgrError>;

    /// Write a single block to a relation fork from `data`.
    ///
    /// `data` must be exactly `BLCKSZ` bytes. The block must already exist
    /// (use `extend` to add new blocks).
    ///
    /// Corresponds to `smgr_writev` / `mdwritev` with nblocks=1.
    fn write_block(&mut self, rel: RelFileLocator, fork: ForkNumber, block: BlockNumber, data: &[u8], skip_fsync: bool) -> Result<(), SmgrError>;

    /// Issue a writeback hint to the OS for a range of blocks.
    ///
    /// This is an advisory operation — it asks the OS to flush dirty pages
    /// from its page cache to disk, but does not guarantee durability (that
    /// requires `immedsync`). Corresponds to `smgr_writeback` / `mdwriteback`.
    fn writeback(&mut self, rel: RelFileLocator, fork: ForkNumber, block: BlockNumber, nblocks: u32) -> Result<(), SmgrError>;

    /// Hint to the OS to prefetch `nblocks` blocks starting at `block`.
    ///
    /// This is purely advisory — the OS may ignore it. On Linux this calls
    /// `posix_fadvise(POSIX_FADV_WILLNEED)`; on other platforms it is a no-op.
    /// Corresponds to `smgr_prefetch` / `mdprefetch`.
    fn prefetch(&mut self, rel: RelFileLocator, fork: ForkNumber, block: BlockNumber, nblocks: u32) -> Result<(), SmgrError>;

    /// Return the maximum number of consecutive blocks that can be submitted
    /// as a single vectored I/O starting at `block`.
    ///
    /// The result is capped at `MAX_IO_COMBINE_LIMIT` and at the number of
    /// blocks remaining in the current segment file (since a single I/O cannot
    /// cross a segment boundary). Corresponds to `smgr_maxcombine` /
    /// `mdmaxcombine`.
    fn max_combine(&self, rel: RelFileLocator, fork: ForkNumber, block: BlockNumber) -> u32;

    /// Return the OS file descriptor and the byte offset within the file for
    /// a given block. Intended for use by AIO backends that issue I/O directly
    /// against the fd rather than going through `read_block` / `write_block`.
    ///
    /// Corresponds to `smgr_fd` / `mdfd`. Only available on Unix targets.
    ///
    /// Returns `(raw_fd, byte_offset_in_file)`.
    #[cfg(unix)]
    fn fd(&mut self, rel: RelFileLocator, fork: ForkNumber, block: BlockNumber) -> Result<(i32, u64), SmgrError>;

    // --- Relation size management ---

    /// Append a new block to a relation fork, writing `data` as its content.
    ///
    /// `block` must equal the current `nblocks()` of the fork (i.e., you can
    /// only extend one block at a time in sequence). Corresponds to
    /// `smgr_extend` / `mdextend`.
    fn extend(&mut self, rel: RelFileLocator, fork: ForkNumber, block: BlockNumber, data: &[u8], skip_fsync: bool) -> Result<(), SmgrError>;

    /// Append `nblocks` zero-filled blocks starting at `block`.
    ///
    /// More efficient than calling `extend` in a loop when you want empty
    /// pages. Corresponds to `smgr_zeroextend` / `mdzeroextend`.
    fn zero_extend(&mut self, rel: RelFileLocator, fork: ForkNumber, block: BlockNumber, nblocks: u32, skip_fsync: bool) -> Result<(), SmgrError>;

    /// Return the total number of blocks currently in a relation fork.
    ///
    /// Counts across all segment files. Corresponds to `smgr_nblocks` /
    /// `mdnblocks`.
    fn nblocks(&mut self, rel: RelFileLocator, fork: ForkNumber) -> Result<BlockNumber, SmgrError>;

    /// Truncate a relation fork to `nblocks` blocks.
    ///
    /// Blocks from `nblocks` onward are discarded. Segment files that become
    /// completely empty are removed (truncated to 0 bytes and left as inactive
    /// segments, mirroring `md.c`'s behavior). Corresponds to `smgr_truncate`
    /// / `mdtruncate`.
    fn truncate(&mut self, rel: RelFileLocator, fork: ForkNumber, nblocks: BlockNumber) -> Result<(), SmgrError>;

    // --- Sync ---

    /// Immediately fsync all segment files for a relation fork to durable storage.
    ///
    /// This is the "hard sync" path used during checkpoints and relation
    /// creation. Corresponds to `smgr_immedsync` / `mdimmedsync`.
    fn immedsync(&mut self, rel: RelFileLocator, fork: ForkNumber) -> Result<(), SmgrError>;
}

// ---------------------------------------------------------------------------
// Path construction helpers
// ---------------------------------------------------------------------------

/// Build the filesystem path for a specific segment of a relation fork.
///
/// Maps onto the same naming convention as PostgreSQL's `relpath()` output,
/// simplified for the single-tablespace case:
///
///   `<base_dir>/<db_oid>/<rel_number>[<fork_suffix>][.<segno>]`
///
/// Segment 0 of the main fork has no suffix at all:
///
///   `base_dir/5/16384`
///
/// Segment 2 of the FSM fork:
///
///   `base_dir/5/16384_fsm.2`
fn segment_path(base_dir: &Path, rel: RelFileLocator, fork: ForkNumber, segno: u32) -> PathBuf {
    let db_dir = base_dir.join(rel.db_oid.to_string());
    let fork_suffix = fork.suffix();

    // Segment 0 has no segment-number suffix; segment N appends ".<N>".
    let filename = if segno == 0 {
        format!("{}{}", rel.rel_number, fork_suffix)
    } else {
        format!("{}{}.{}", rel.rel_number, fork_suffix, segno)
    };

    db_dir.join(filename)
}

/// Return the segment number and the block offset within that segment for a
/// given absolute block number.
///
/// For example, with RELSEG_SIZE = 131072:
///   block 0       → segment 0, offset 0
///   block 131071  → segment 0, offset 131071
///   block 131072  → segment 1, offset 0
#[inline]
fn seg_for_block(block: BlockNumber) -> (u32, u32) {
    (block / RELSEG_SIZE, block % RELSEG_SIZE)
}

// ---------------------------------------------------------------------------
// MdStorageManager — the "magnetic disk" implementation
// ---------------------------------------------------------------------------

/// Handle to an open segment file.
///
/// Mirrors the `MdfdVec` struct in `md.c`. We cache the open `File` handle so
/// repeated reads/writes to the same segment don't pay `open(2)` every time.
struct OpenSeg {
    file: File,
    /// Which segment number this is (0 = first segment).
    /// Stored for diagnostics and future use (e.g., logging, AIO target info).
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

/// The "magnetic disk" storage manager.
///
/// This is the Rust equivalent of PostgreSQL's `md.c`. It translates the
/// abstract `StorageManager` API into filesystem operations via `std::fs` and
/// `std::io`.
///
/// ## Internal state
///
/// `open_segs`: a cache of open `File` handles, keyed by (relation, fork,
/// segment). This avoids repeatedly calling `open(2)` for the same segment
/// during a sequence of reads or writes. The cache is eagerly populated on
/// first access and can be invalidated by calling `close`.
///
/// `base_dir`: the root directory that contains all relation files. Subdirs
/// are named after database OIDs; within each subdir, files are named after
/// relation numbers with optional fork/segment suffixes.
pub struct MdStorageManager {
    /// Root directory for all relation files.
    /// Maps to `PGDATA/base` for the default tablespace.
    base_dir: PathBuf,

    /// Cache of open file handles.
    /// Populated lazily on first access; cleared by `close()`.
    open_segs: HashMap<SegKey, OpenSeg>,

    /// Whether the storage manager is operating in WAL recovery mode.
    ///
    /// When `true`, `exists()` skips the close-before-check step (mirrors
    /// Postgres's `InRecovery` check in `mdexists`), and `unlink()` removes
    /// files immediately instead of deferring.
    pub in_recovery: bool,
}

impl MdStorageManager {
    /// Create a new storage manager rooted at `base_dir`.
    ///
    /// `base_dir` must already exist (or will be created on the first `open`
    /// call). Within it, per-database subdirectories are created automatically.
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        MdStorageManager {
            base_dir: base_dir.into(),
            open_segs: HashMap::new(),
            in_recovery: false,
        }
    }

    /// Create a storage manager that operates in WAL recovery mode.
    ///
    /// In recovery mode, `exists()` skips the close-before-check and
    /// `unlink()` removes files immediately (no deferred-unlink protection).
    pub fn new_in_recovery(base_dir: impl Into<PathBuf>) -> Self {
        MdStorageManager {
            base_dir: base_dir.into(),
            open_segs: HashMap::new(),
            in_recovery: true,
        }
    }

    /// Close all open file handles without destroying any relation state.
    ///
    /// This is the Rust equivalent of `PROCSIGNAL_BARRIER_SMGRRELEASE` in
    /// PostgreSQL: when a relation is being dropped, the dropping backend
    /// signals all other backends to close their file handles so the OS can
    /// finalize the unlink. File handles are re-opened lazily on next access.
    pub fn release_all(&mut self) {
        self.open_segs.clear();
    }

    /// Return the path for a given segment of a relation fork.
    fn seg_path(&self, rel: RelFileLocator, fork: ForkNumber, segno: u32) -> PathBuf {
        segment_path(&self.base_dir, rel, fork, segno)
    }

    /// Return the directory that holds all files for a given database.
    fn db_dir(&self, rel: RelFileLocator) -> PathBuf {
        self.base_dir.join(rel.db_oid.to_string())
    }

    /// Open (or retrieve from cache) a specific segment file for reading and
    /// writing. Returns an error if the segment does not exist.
    ///
    /// This is the Rust analogue of `_mdfd_getseg()` in `md.c`.
    fn get_seg(&mut self, rel: RelFileLocator, fork: ForkNumber, segno: u32) -> Result<&mut OpenSeg, SmgrError> {
        let key = SegKey { rel, fork, segno };

        // If not already open, open it now.
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

    /// Open (or retrieve from cache) a segment, creating it if it doesn't
    /// exist. Used during `extend` when we need to move into a new segment.
    fn get_or_create_seg(&mut self, rel: RelFileLocator, fork: ForkNumber, segno: u32) -> Result<&mut OpenSeg, SmgrError> {
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

    /// Count the number of complete-and-partial-segment files that exist for
    /// a relation fork, and compute the total block count.
    ///
    /// Mirrors `mdnblocks()` in `md.c`: we scan segment files starting from 0
    /// and stop when we find one that does not exist (inactive segments are
    /// skipped by the assumption that there's at most one partial segment
    /// before the first absent one).
    fn count_blocks(&self, rel: RelFileLocator, fork: ForkNumber) -> Result<BlockNumber, SmgrError> {
        let mut total: BlockNumber = 0;

        for segno in 0.. {
            let path = self.seg_path(rel, fork, segno);
            match fs::metadata(&path) {
                Ok(meta) => {
                    // How many complete blocks are in this segment?
                    let byte_len = meta.len();
                    let block_count = (byte_len / BLCKSZ as u64) as u32;
                    total += block_count;

                    // If this segment is not full, it's the last active one.
                    // (A partial segment means we've reached the end.)
                    if block_count < RELSEG_SIZE {
                        break;
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    // No more segments.
                    break;
                }
                Err(e) => return Err(SmgrError::Io(e)),
            }
        }

        Ok(total)
    }

    /// Deactivate segments beyond `start_segno` by truncating them to 0 bytes
    /// and evicting their cached handles, but leaving the files on disk.
    ///
    /// This mirrors Postgres `md.c`'s behavior for `mdtruncate`: excess
    /// segments are left as zero-length "inactive" files rather than being
    /// removed. This prevents their relation file number from being reused
    /// before the next checkpoint in the event of a crash.
    ///
    /// `count_blocks()` naturally stops at the first segment with fewer than
    /// RELSEG_SIZE blocks, so zero-length inactive segments are invisible to
    /// callers of `nblocks()`.
    fn deactivate_segments_from(&mut self, rel: RelFileLocator, fork: ForkNumber, start_segno: u32) {
        for segno in start_segno.. {
            let path = self.seg_path(rel, fork, segno);
            if !path.exists() {
                break; // no more segments
            }

            // Evict cached handle so the truncation is clean.
            let key = SegKey { rel, fork, segno };
            self.open_segs.remove(&key);

            // Truncate to 0 bytes (deactivate) rather than remove.
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

    /// Remove all segment files for one fork of a relation, starting from
    /// segment `start_segno`. Used by `unlink` and (in recovery) `truncate`.
    fn remove_segments_from(&mut self, rel: RelFileLocator, fork: ForkNumber, start_segno: u32) {
        for segno in start_segno.. {
            let key = SegKey { rel, fork, segno };
            // Remove from cache first (closes the File handle).
            self.open_segs.remove(&key);

            let path = self.seg_path(rel, fork, segno);
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => break, // no more segments
                Err(e) => {
                    // Postgres logs these as WARNING rather than ERROR because
                    // unlink is called from post-commit cleanup. We do the same.
                    eprintln!("WARNING: could not remove {:?}: {}", path, e);
                    break;
                }
            }
        }
    }
}

impl StorageManager for MdStorageManager {
    // -----------------------------------------------------------------------
    // open / close
    // -----------------------------------------------------------------------

    fn open(&mut self, rel: RelFileLocator) -> Result<(), SmgrError> {
        // Create the per-database directory if it doesn't already exist.
        // In Postgres this is done by TablespaceCreateDbspace() called from
        // mdcreate(); we do it here so the directory always exists before any
        // file operations.
        let dir = self.db_dir(rel);
        fs::create_dir_all(&dir)?;
        Ok(())
    }

    fn close(&mut self, rel: RelFileLocator, fork: ForkNumber) -> Result<(), SmgrError> {
        // Evict all cached open segments for this (relation, fork) pair.
        // The File handles are dropped here, which closes the OS fd.
        self.open_segs.retain(|key, _| !(key.rel == rel && key.fork == fork));
        Ok(())
    }

    // -----------------------------------------------------------------------
    // create / exists / unlink
    // -----------------------------------------------------------------------

    fn create(&mut self, rel: RelFileLocator, fork: ForkNumber, is_redo: bool) -> Result<(), SmgrError> {
        // Ensure the db directory exists (mirrors TablespaceCreateDbspace in mdcreate).
        let dir = self.db_dir(rel);
        fs::create_dir_all(&dir)?;

        let path = self.seg_path(rel, fork, 0);

        // In Postgres, mdcreate uses O_CREAT | O_EXCL unless is_redo, in
        // which case it falls back to a plain open if the exclusive create
        // fails. We mirror that logic here.
        let result = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true) // equivalent to O_CREAT | O_EXCL
            .open(&path);

        let file = match result {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                if is_redo {
                    // During WAL replay it's fine for the file to already exist.
                    OpenOptions::new().read(true).write(true).open(&path)?
                } else {
                    return Err(SmgrError::AlreadyExists { rel, fork });
                }
            }
            Err(e) => return Err(SmgrError::Io(e)),
        };

        // Cache the open handle for segment 0.
        let key = SegKey { rel, fork, segno: 0 };
        self.open_segs.insert(key, OpenSeg { file, segno: 0 });

        Ok(())
    }

    fn exists(&mut self, rel: RelFileLocator, fork: ForkNumber) -> bool {
        // Mirrors mdexists(): in non-recovery mode, close the fork's cached
        // handles first so that a recently-unlinked file doesn't appear to
        // exist because we hold an open fd to it. In recovery mode, relations
        // are already closed when dropped so the close is unnecessary.
        if !self.in_recovery {
            let _ = self.close(rel, fork);
        }
        self.seg_path(rel, fork, 0).exists()
    }

    fn unlink(&mut self, rel: RelFileLocator, fork: Option<ForkNumber>, _is_redo: bool) {
        // If fork is None, remove all forks. Otherwise remove the specified one.
        let forks: &[ForkNumber] = match fork {
            Some(f) => match f {
                ForkNumber::Main => &[ForkNumber::Main],
                ForkNumber::Fsm => &[ForkNumber::Fsm],
                ForkNumber::VisibilityMap => &[ForkNumber::VisibilityMap],
                ForkNumber::Init => &[ForkNumber::Init],
            },
            None => &[
                ForkNumber::Main,
                ForkNumber::Fsm,
                ForkNumber::VisibilityMap,
                ForkNumber::Init,
            ],
        };

        for &f in forks {
            self.remove_segments_from(rel, f, 0);
        }
    }

    // -----------------------------------------------------------------------
    // Block I/O: read_block / write_block / writeback
    // -----------------------------------------------------------------------

    fn read_block(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        block: BlockNumber,
        buf: &mut [u8],
    ) -> Result<(), SmgrError> {
        // Validate buffer size first so callers get a clear error.
        if buf.len() != BLCKSZ {
            return Err(SmgrError::BadBufferSize { size: buf.len() });
        }

        // Determine which segment file this block lives in, and the byte
        // offset within that segment.
        //
        // Example (RELSEG_SIZE = 131072):
        //   block 200000 → segment 1, offset within segment = 200000 - 131072 = 68928
        //   byte offset in segment file = 68928 * 8192 = 564,953,088
        let (segno, seg_offset) = seg_for_block(block);
        let byte_offset = seg_offset as u64 * BLCKSZ as u64;

        let seg = self.get_seg(rel, fork, segno)?;

        // Seek to the block's byte offset within the segment file.
        seg.file.seek(SeekFrom::Start(byte_offset))?;

        // Read exactly BLCKSZ bytes. A short read means the block is beyond
        // the end of the file, which is a block-out-of-range condition.
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
            return Err(SmgrError::ShortIo { expected: BLCKSZ, actual: n });
        }

        // skip_fsync=false would normally register a deferred fsync. We omit
        // deferred sync (see module docs) — sync only happens via immedsync().
        Ok(())
    }

    fn writeback(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        block: BlockNumber,
        nblocks: u32,
    ) -> Result<(), SmgrError> {
        // On Linux, Postgres calls sync_file_range() here. On macOS/other, it
        // falls back to a no-op or fdatasync. We use sync_all() (fdatasync
        // equivalent) on the affected segments as a safe portable substitute.
        //
        // This is advisory — callers that need guaranteed durability should use
        // immedsync() instead.
        if nblocks == 0 {
            return Ok(());
        }

        let first_seg = block / RELSEG_SIZE;
        let last_block = block.saturating_add(nblocks - 1);
        let last_seg = last_block / RELSEG_SIZE;

        for segno in first_seg..=last_seg {
            // Only sync segments that are actually open; don't force-open them.
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
        // On Linux, issue posix_fadvise(POSIX_FADV_WILLNEED) for each segment
        // span covered by [block, block+nblocks). On other platforms this is a
        // deliberate no-op — the OS page cache handles readahead well enough.
        #[cfg(target_os = "linux")]
        {
            if nblocks == 0 {
                return Ok(());
            }
            let first_seg = block / RELSEG_SIZE;
            let last_seg = block.saturating_add(nblocks - 1) / RELSEG_SIZE;

            for segno in first_seg..=last_seg {
                // Determine the byte range within this segment.
                let seg_start_block = segno * RELSEG_SIZE;
                let local_first = block.saturating_sub(seg_start_block);
                let local_last = (block + nblocks - 1).min(seg_start_block + RELSEG_SIZE - 1)
                    - seg_start_block;
                let offset = local_first as i64 * BLCKSZ as i64;
                let len = ((local_last - local_first + 1) as i64) * BLCKSZ as i64;

                // Ensure the segment is open so we have an fd.
                let seg = match self.get_seg(rel, fork, segno) {
                    Ok(s) => s,
                    Err(_) => break, // segment doesn't exist, stop prefetching
                };

                let fd = seg.file.as_raw_fd();
                // SAFETY: fd is valid and owned by the open File handle above.
                unsafe {
                    libc::posix_fadvise(fd, offset, len, libc::POSIX_FADV_WILLNEED);
                }
            }
        }
        // Suppress unused-variable warnings on non-Linux.
        let _ = (rel, fork, block, nblocks);
        Ok(())
    }

    fn max_combine(&self, _rel: RelFileLocator, _fork: ForkNumber, block: BlockNumber) -> u32 {
        // The maximum coalesced I/O cannot cross a segment boundary, so it is
        // capped at the number of blocks remaining in the current segment.
        // It is also capped at MAX_IO_COMBINE_LIMIT (the AIO subsystem limit).
        //
        // Example: block = 131070, RELSEG_SIZE = 131072
        //   remaining_in_seg = 131072 - (131070 % 131072) = 131072 - 131070 = 2
        //   max_combine = min(64, 2) = 2
        let remaining_in_seg = RELSEG_SIZE - (block % RELSEG_SIZE);
        remaining_in_seg.min(MAX_IO_COMBINE_LIMIT)
    }

    #[cfg(unix)]
    fn fd(&mut self, rel: RelFileLocator, fork: ForkNumber, block: BlockNumber) -> Result<(i32, u64), SmgrError> {
        // Open the segment (lazily cached) and return:
        //   - its raw OS file descriptor
        //   - the byte offset of `block` within that segment file
        //
        // The caller is responsible for ensuring the fd remains valid for the
        // duration of the I/O — i.e., they must not call close() or
        // release_all() while an AIO operation using this fd is in flight.
        let (segno, seg_offset) = seg_for_block(block);
        let byte_offset = seg_offset as u64 * BLCKSZ as u64;
        let seg = self.get_seg(rel, fork, segno)?;
        Ok((seg.file.as_raw_fd(), byte_offset))
    }

    // -----------------------------------------------------------------------
    // Relation size: extend / zero_extend / nblocks / truncate
    // -----------------------------------------------------------------------

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

        // Compute which segment this block lands in.
        let (segno, seg_offset) = seg_for_block(block);
        let byte_offset = seg_offset as u64 * BLCKSZ as u64;

        // get_or_create_seg opens the segment file, creating it if needed.
        // This handles the cross-segment boundary case: when block % RELSEG_SIZE == 0
        // and segno > 0, we're starting a new segment file.
        let seg = self.get_or_create_seg(rel, fork, segno)?;

        // Write at the expected offset. The file must already be at exactly
        // byte_offset length for this to be a true extension (not overwrite).
        // We seek to the target position and write; if the file is shorter,
        // the OS will fill the gap with zeros (sparse file behavior).
        seg.file.seek(SeekFrom::Start(byte_offset))?;
        let n = seg.file.write(data)?;
        if n != BLCKSZ {
            return Err(SmgrError::ShortIo { expected: BLCKSZ, actual: n });
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
        // Write nblocks zero-filled pages starting at `block`.
        // More efficient than calling extend() in a loop because we reuse the
        // same zero buffer and avoid repeated seek overhead.
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
        // --- Step 1: figure out the target segment and offset within it ---
        //
        // If nblocks == 0, we want segment 0 to be empty (0 bytes).
        // If nblocks > 0, the last valid block is nblocks-1.
        //   last_seg  = (nblocks - 1) / RELSEG_SIZE
        //   blocks_in_last_seg = ((nblocks - 1) % RELSEG_SIZE) + 1
        //   byte length of last segment = blocks_in_last_seg * BLCKSZ

        let (target_seg, target_byte_len) = if nblocks == 0 {
            (0u32, 0u64)
        } else {
            let last_block = nblocks - 1;
            let seg = last_block / RELSEG_SIZE;
            let blocks_in_seg = (last_block % RELSEG_SIZE) + 1;
            (seg, blocks_in_seg as u64 * BLCKSZ as u64)
        };

        // --- Step 2: truncate the target segment to the right byte length ---

        // Close the cached handle first so we can reopen it in write mode.
        let key = SegKey { rel, fork, segno: target_seg };
        self.open_segs.remove(&key);

        let path = self.seg_path(rel, fork, target_seg);
        if path.exists() {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)?;
            file.set_len(target_byte_len)?;
            // Re-cache the handle.
            self.open_segs.insert(key, OpenSeg { file, segno: target_seg });
        }

        // --- Step 3: handle segments beyond the target ---
        //
        // In non-recovery mode: deactivate (truncate to 0 bytes, leave on disk)
        // so the relation file number cannot be reused before the next checkpoint
        // if we crash. This matches Postgres md.c's mdtruncate() behavior.
        //
        // In recovery mode: remove immediately, since there is no reuse hazard
        // (WAL replay will recreate any needed relations).
        if self.in_recovery {
            self.remove_segments_from(rel, fork, target_seg + 1);
        } else {
            self.deactivate_segments_from(rel, fork, target_seg + 1);
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Sync
    // -----------------------------------------------------------------------

    fn immedsync(&mut self, rel: RelFileLocator, fork: ForkNumber) -> Result<(), SmgrError> {
        // Sync all currently-open segments for this relation fork to durable
        // storage. This is the "hard" sync path (checkpoint, relation creation).
        //
        // For segments that aren't open yet we do a stat-and-open pass to
        // find them all, mirroring mdimmedsync()'s behavior.
        for segno in 0.. {
            let path = self.seg_path(rel, fork, segno);
            if !path.exists() {
                break; // no more segments
            }

            // Open if not already cached.
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

    /// Create a temporary directory for test files and return a storage manager
    /// rooted there. The directory is inside `std::env::temp_dir()` and is NOT
    /// automatically cleaned up so failures can be inspected. Tests use unique
    /// names via a monotonic counter.
    fn temp_smgr(label: &str) -> (MdStorageManager, PathBuf) {
        let base = env::temp_dir().join(format!("pgrust_smgr_test_{}", label));
        // Start fresh each run.
        let _ = fs::remove_dir_all(&base);
        fs::create_dir_all(&base).unwrap();
        (MdStorageManager::new(&base), base)
    }

    /// Construct a simple test relation locator.
    fn test_rel(n: u32) -> RelFileLocator {
        RelFileLocator { spc_oid: 0, db_oid: 1, rel_number: n }
    }

    /// Fill a BLCKSZ buffer with a recognizable pattern: every byte is
    /// `(block * 7 + byte_index) % 251`. Used to distinguish blocks in tests.
    fn page_pattern(block: u32) -> Vec<u8> {
        (0..BLCKSZ).map(|i| ((block as usize * 7 + i) % 251) as u8).collect()
    }

    // --- Test: create and existence ---

    /// Creating a fork produces an existing empty relation.
    #[test]
    fn test_create_and_exists() {
        let (mut smgr, _base) = temp_smgr("create_exists");
        let rel = test_rel(1000);

        smgr.open(rel).unwrap();
        // Before creation, the fork must not exist.
        assert!(!smgr.exists(rel, ForkNumber::Main));

        smgr.create(rel, ForkNumber::Main, false).unwrap();

        // After creation, exists() returns true.
        assert!(smgr.exists(rel, ForkNumber::Main));

        // An empty relation has 0 blocks.
        assert_eq!(smgr.nblocks(rel, ForkNumber::Main).unwrap(), 0);
    }

    /// Creating the same fork twice without is_redo is an error.
    #[test]
    fn test_create_duplicate_fails() {
        let (mut smgr, _base) = temp_smgr("create_duplicate");
        let rel = test_rel(1001);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();

        // Second create (not redo) must fail.
        let err = smgr.create(rel, ForkNumber::Main, false);
        assert!(matches!(err, Err(SmgrError::AlreadyExists { .. })));
    }

    /// Creating the same fork twice with is_redo=true is idempotent.
    #[test]
    fn test_create_redo_idempotent() {
        let (mut smgr, _base) = temp_smgr("create_redo");
        let rel = test_rel(1002);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();

        // Second create with is_redo=true must succeed.
        smgr.create(rel, ForkNumber::Main, true).unwrap();
        assert!(smgr.exists(rel, ForkNumber::Main));
    }

    // --- Test: extend and nblocks ---

    /// Extending a relation adds blocks and nblocks() reflects the new size.
    #[test]
    fn test_extend_nblocks() {
        let (mut smgr, _base) = temp_smgr("extend_nblocks");
        let rel = test_rel(2000);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();

        // Extend by writing 5 sequential blocks.
        for i in 0..5u32 {
            let data = page_pattern(i);
            smgr.extend(rel, ForkNumber::Main, i, &data, true).unwrap();
        }

        assert_eq!(smgr.nblocks(rel, ForkNumber::Main).unwrap(), 5);
    }

    /// zero_extend writes multiple zero-filled pages in one call.
    #[test]
    fn test_zero_extend() {
        let (mut smgr, _base) = temp_smgr("zero_extend");
        let rel = test_rel(2001);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();
        smgr.zero_extend(rel, ForkNumber::Main, 0, 10, true).unwrap();

        assert_eq!(smgr.nblocks(rel, ForkNumber::Main).unwrap(), 10);

        // All pages must be zero.
        let mut buf = vec![0u8; BLCKSZ];
        for i in 0..10u32 {
            smgr.read_block(rel, ForkNumber::Main, i, &mut buf).unwrap();
            assert!(buf.iter().all(|&b| b == 0), "block {i} not zero");
        }
    }

    // --- Test: read and write ---

    /// Writing a block and reading it back returns the same data.
    #[test]
    fn test_write_read_roundtrip() {
        let (mut smgr, _base) = temp_smgr("write_read");
        let rel = test_rel(3000);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();

        // Write 3 blocks with distinct patterns.
        for i in 0..3u32 {
            let data = page_pattern(i);
            smgr.extend(rel, ForkNumber::Main, i, &data, true).unwrap();
        }

        // Read them back and verify each one.
        let mut buf = vec![0u8; BLCKSZ];
        for i in 0..3u32 {
            smgr.read_block(rel, ForkNumber::Main, i, &mut buf).unwrap();
            assert_eq!(buf, page_pattern(i), "block {i} data mismatch after read");
        }
    }

    /// Overwriting a block with write_block replaces the data.
    #[test]
    fn test_overwrite_block() {
        let (mut smgr, _base) = temp_smgr("overwrite");
        let rel = test_rel(3001);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();

        let original = page_pattern(1);
        smgr.extend(rel, ForkNumber::Main, 0, &original, true).unwrap();

        // Overwrite block 0 with a different pattern.
        let new_data = page_pattern(42);
        smgr.write_block(rel, ForkNumber::Main, 0, &new_data, true).unwrap();

        // Read back and verify the new data.
        let mut buf = vec![0u8; BLCKSZ];
        smgr.read_block(rel, ForkNumber::Main, 0, &mut buf).unwrap();
        assert_eq!(buf, new_data);
    }

    /// Reading a block that doesn't exist returns an error.
    #[test]
    fn test_read_missing_block() {
        let (mut smgr, _base) = temp_smgr("read_missing");
        let rel = test_rel(3002);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();

        // No blocks written yet — reading block 0 must fail.
        let mut buf = vec![0u8; BLCKSZ];
        let err = smgr.read_block(rel, ForkNumber::Main, 0, &mut buf);
        assert!(matches!(err, Err(SmgrError::BlockOutOfRange { .. })));
    }

    // --- Test: multiple forks ---

    /// Each fork is independent: writing to Main does not affect FSM.
    #[test]
    fn test_multiple_forks_independent() {
        let (mut smgr, _base) = temp_smgr("multi_fork");
        let rel = test_rel(4000);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();
        smgr.create(rel, ForkNumber::Fsm, false).unwrap();

        let main_data = page_pattern(10);
        let fsm_data = page_pattern(20);

        smgr.extend(rel, ForkNumber::Main, 0, &main_data, true).unwrap();
        smgr.extend(rel, ForkNumber::Fsm, 0, &fsm_data, true).unwrap();

        let mut buf = vec![0u8; BLCKSZ];

        smgr.read_block(rel, ForkNumber::Main, 0, &mut buf).unwrap();
        assert_eq!(buf, main_data, "main fork block mismatch");

        smgr.read_block(rel, ForkNumber::Fsm, 0, &mut buf).unwrap();
        assert_eq!(buf, fsm_data, "FSM fork block mismatch");
    }

    // --- Test: truncate ---

    /// Truncating a relation removes trailing blocks.
    #[test]
    fn test_truncate() {
        let (mut smgr, _base) = temp_smgr("truncate");
        let rel = test_rel(5000);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();

        // Write 10 blocks.
        for i in 0..10u32 {
            smgr.extend(rel, ForkNumber::Main, i, &page_pattern(i), true).unwrap();
        }
        assert_eq!(smgr.nblocks(rel, ForkNumber::Main).unwrap(), 10);

        // Truncate to 4 blocks.
        smgr.truncate(rel, ForkNumber::Main, 4).unwrap();
        assert_eq!(smgr.nblocks(rel, ForkNumber::Main).unwrap(), 4);

        // The first 4 blocks must still be readable and correct.
        let mut buf = vec![0u8; BLCKSZ];
        for i in 0..4u32 {
            smgr.read_block(rel, ForkNumber::Main, i, &mut buf).unwrap();
            assert_eq!(buf, page_pattern(i), "block {i} wrong after truncate");
        }

        // Block 4 must no longer exist.
        let err = smgr.read_block(rel, ForkNumber::Main, 4, &mut buf);
        assert!(matches!(err, Err(SmgrError::BlockOutOfRange { .. })));
    }

    /// Truncating to 0 blocks leaves the relation empty.
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

    // --- Test: segment boundaries ---

    /// Blocks that span segment boundaries read and write correctly.
    ///
    /// We use a very small RELSEG_SIZE for testing would require recompiling;
    /// instead we test arithmetic around the boundary directly by writing
    /// to block numbers that land in segment 0 and segment 1.
    ///
    /// This test writes at block 0 and RELSEG_SIZE (the first block of segment 1)
    /// by extending through the entire first segment — but that would be 131072
    /// blocks which is slow. Instead we use a separate small-RELSEG variant
    /// via seg_for_block arithmetic verification.
    #[test]
    fn test_segment_arithmetic() {
        // Block 0 → segment 0, offset 0
        assert_eq!(seg_for_block(0), (0, 0));
        // Block RELSEG_SIZE - 1 → segment 0, last block
        assert_eq!(seg_for_block(RELSEG_SIZE - 1), (0, RELSEG_SIZE - 1));
        // Block RELSEG_SIZE → segment 1, first block
        assert_eq!(seg_for_block(RELSEG_SIZE), (1, 0));
        // Block 2 * RELSEG_SIZE → segment 2, first block
        assert_eq!(seg_for_block(2 * RELSEG_SIZE), (2, 0));
        // Block 2 * RELSEG_SIZE + 5 → segment 2, offset 5
        assert_eq!(seg_for_block(2 * RELSEG_SIZE + 5), (2, 5));
    }

    // --- Test: unlink ---

    /// Unlinking a relation removes its files.
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

    /// Unlinking all forks removes each fork's files.
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

    // --- Test: close releases handles, re-open works ---

    /// Closing a fork and re-accessing it succeeds (re-opens the file).
    #[test]
    fn test_close_and_reopen() {
        let (mut smgr, _base) = temp_smgr("close_reopen");
        let rel = test_rel(7000);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();
        smgr.extend(rel, ForkNumber::Main, 0, &page_pattern(99), true).unwrap();

        // Close the fork (drops cached file handles).
        smgr.close(rel, ForkNumber::Main).unwrap();

        // Reading after close must still work (re-opens the file lazily).
        let mut buf = vec![0u8; BLCKSZ];
        smgr.read_block(rel, ForkNumber::Main, 0, &mut buf).unwrap();
        assert_eq!(buf, page_pattern(99));
    }

    // --- Test: path construction ---

    /// Verify the file path convention matches what Postgres would produce.
    #[test]
    fn test_path_construction() {
        let base = PathBuf::from("/pgdata/base");
        let rel = RelFileLocator { spc_oid: 0, db_oid: 5, rel_number: 16384 };

        // Main fork, segment 0: no suffix, no segment number.
        assert_eq!(
            segment_path(&base, rel, ForkNumber::Main, 0),
            PathBuf::from("/pgdata/base/5/16384")
        );
        // Main fork, segment 2.
        assert_eq!(
            segment_path(&base, rel, ForkNumber::Main, 2),
            PathBuf::from("/pgdata/base/5/16384.2")
        );
        // FSM fork, segment 0.
        assert_eq!(
            segment_path(&base, rel, ForkNumber::Fsm, 0),
            PathBuf::from("/pgdata/base/5/16384_fsm")
        );
        // Visibility map fork, segment 1.
        assert_eq!(
            segment_path(&base, rel, ForkNumber::VisibilityMap, 1),
            PathBuf::from("/pgdata/base/5/16384_vm.1")
        );
        // Init fork, segment 0.
        assert_eq!(
            segment_path(&base, rel, ForkNumber::Init, 0),
            PathBuf::from("/pgdata/base/5/16384_init")
        );
    }

    // --- Test: immedsync ---

    /// immedsync completes without error on a populated relation.
    #[test]
    fn test_immedsync() {
        let (mut smgr, _base) = temp_smgr("immedsync");
        let rel = test_rel(8000);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();
        smgr.zero_extend(rel, ForkNumber::Main, 0, 3, true).unwrap();

        // immedsync should succeed without error.
        smgr.immedsync(rel, ForkNumber::Main).unwrap();
    }

    // --- Test: bad buffer size returns clear error ---

    /// Passing a wrong-sized buffer returns SmgrError::BadBufferSize.
    #[test]
    fn test_bad_buffer_size() {
        let (mut smgr, _base) = temp_smgr("bad_buf");
        let rel = test_rel(9000);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();

        let mut small_buf = vec![0u8; 100]; // wrong size
        let err = smgr.read_block(rel, ForkNumber::Main, 0, &mut small_buf);
        assert!(matches!(err, Err(SmgrError::BadBufferSize { .. })));

        let small_data = vec![0u8; 100];
        let err = smgr.extend(rel, ForkNumber::Main, 0, &small_data, true);
        assert!(matches!(err, Err(SmgrError::BadBufferSize { .. })));
    }

    // --- Test: max_combine ---

    /// max_combine respects the segment boundary.
    #[test]
    fn test_max_combine() {
        let smgr = MdStorageManager::new("/tmp");
        let rel = test_rel(0);

        // Block 0: a full RELSEG_SIZE of room, capped at MAX_IO_COMBINE_LIMIT.
        assert_eq!(smgr.max_combine(rel, ForkNumber::Main, 0), MAX_IO_COMBINE_LIMIT);

        // One block before the segment boundary: only 1 block available.
        assert_eq!(smgr.max_combine(rel, ForkNumber::Main, RELSEG_SIZE - 1), 1);

        // Two blocks before: 2 available, still under the limit.
        assert_eq!(smgr.max_combine(rel, ForkNumber::Main, RELSEG_SIZE - 2), 2);

        // First block of segment 1: again a full RELSEG_SIZE of room.
        assert_eq!(smgr.max_combine(rel, ForkNumber::Main, RELSEG_SIZE), MAX_IO_COMBINE_LIMIT);
    }

    // --- Test: prefetch (no-op on non-Linux, smoke test everywhere) ---

    /// prefetch does not error even if the segment doesn't exist.
    #[test]
    fn test_prefetch_no_error() {
        let (mut smgr, _base) = temp_smgr("prefetch");
        let rel = test_rel(10000);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();
        smgr.zero_extend(rel, ForkNumber::Main, 0, 4, true).unwrap();

        // prefetch on existing blocks succeeds (no-op on non-Linux, syscall on Linux).
        smgr.prefetch(rel, ForkNumber::Main, 0, 4).unwrap();
    }

    // --- Test: fd ---

    /// fd() returns a valid descriptor and the correct byte offset.
    #[test]
    #[cfg(unix)]
    fn test_fd() {
        let (mut smgr, _base) = temp_smgr("fd");
        let rel = test_rel(11000);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();
        smgr.zero_extend(rel, ForkNumber::Main, 0, 3, true).unwrap();

        // Block 0: offset 0
        let (fd0, off0) = smgr.fd(rel, ForkNumber::Main, 0).unwrap();
        assert!(fd0 >= 0, "expected valid fd for block 0");
        assert_eq!(off0, 0);

        // Block 2: offset 2 * BLCKSZ
        let (fd2, off2) = smgr.fd(rel, ForkNumber::Main, 2).unwrap();
        assert!(fd2 >= 0, "expected valid fd for block 2");
        assert_eq!(off2, 2 * BLCKSZ as u64);

        // Both blocks are in segment 0, so the fds should be the same.
        assert_eq!(fd0, fd2);
    }

    // --- Test: in_recovery mode ---

    /// In recovery mode, exists() does not close file handles first.
    #[test]
    fn test_recovery_mode_exists_skips_close() {
        let (base_dir, _tmp) = {
            let b = std::env::temp_dir().join("pgrust_smgr_test_recovery_exists");
            let _ = fs::remove_dir_all(&b);
            fs::create_dir_all(&b).unwrap();
            (b.clone(), b)
        };
        let mut smgr = MdStorageManager::new_in_recovery(&base_dir);
        let rel = test_rel(12000);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();

        // In recovery mode exists() should still report true.
        assert!(smgr.exists(rel, ForkNumber::Main));
    }

    /// In non-recovery mode, exists() closes handles before checking.
    #[test]
    fn test_non_recovery_exists_closes_first() {
        let (mut smgr, _base) = temp_smgr("exists_closes");
        let rel = test_rel(12001);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();

        // Exists should be true.
        assert!(smgr.exists(rel, ForkNumber::Main));

        // After unlink, exists should be false (close-before-check ensures this).
        smgr.unlink(rel, Some(ForkNumber::Main), false);
        assert!(!smgr.exists(rel, ForkNumber::Main));
    }

    // --- Test: truncate leaves inactive segments on disk (non-recovery) ---

    /// In non-recovery mode, truncate leaves excess segments as zero-length files.
    #[test]
    fn test_truncate_leaves_inactive_segments() {
        let (mut smgr, base) = temp_smgr("inactive_segs");

        // Manually create two segment files for relation 13000.
        let rel = test_rel(13000);
        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();

        // Write a full first segment and start a second.
        // (We can't actually write 131072 blocks in a unit test — too slow.)
        // Instead, directly create segment 1 on disk to simulate having it.
        let seg1_path = base.join("1").join("13000.1");
        fs::create_dir_all(base.join("1")).unwrap();
        // Write a few blocks into segment 1 directly.
        let mut f = fs::File::create(&seg1_path).unwrap();
        f.write_all(&vec![0u8; BLCKSZ * 3]).unwrap();
        drop(f);

        // Now truncate to 0 blocks.
        smgr.truncate(rel, ForkNumber::Main, 0).unwrap();

        // Segment 1 should still exist but be zero-length (inactive).
        let meta = fs::metadata(&seg1_path).unwrap();
        assert_eq!(meta.len(), 0, "truncated segment should be 0 bytes (inactive), not removed");
    }

    /// In recovery mode, truncate removes excess segments immediately.
    #[test]
    fn test_truncate_recovery_removes_segments() {
        let base = std::env::temp_dir().join("pgrust_smgr_test_truncate_recovery");
        let _ = fs::remove_dir_all(&base);
        fs::create_dir_all(&base).unwrap();

        let mut smgr = MdStorageManager::new_in_recovery(&base);
        let rel = test_rel(13001);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();

        // Create segment 1 directly.
        let seg1_path = base.join("1").join("13001.1");
        fs::create_dir_all(base.join("1")).unwrap();
        let mut f = fs::File::create(&seg1_path).unwrap();
        f.write_all(&vec![0u8; BLCKSZ]).unwrap();
        drop(f);

        // In recovery mode, truncate should remove segment 1 entirely.
        smgr.truncate(rel, ForkNumber::Main, 0).unwrap();
        assert!(!seg1_path.exists(), "recovery truncate should remove excess segments");
    }

    // --- Test: release_all ---

    /// release_all closes all handles; subsequent access re-opens lazily.
    #[test]
    fn test_release_all() {
        let (mut smgr, _base) = temp_smgr("release_all");
        let rel = test_rel(14000);

        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();
        smgr.extend(rel, ForkNumber::Main, 0, &page_pattern(1), true).unwrap();

        // Force the handle into the cache.
        let mut buf = vec![0u8; BLCKSZ];
        smgr.read_block(rel, ForkNumber::Main, 0, &mut buf).unwrap();
        assert!(!smgr.open_segs.is_empty(), "should have cached handles");

        // release_all clears the cache.
        smgr.release_all();
        assert!(smgr.open_segs.is_empty(), "release_all should clear all handles");

        // Accessing the relation again re-opens lazily.
        smgr.read_block(rel, ForkNumber::Main, 0, &mut buf).unwrap();
        assert_eq!(buf, page_pattern(1), "data should be intact after release_all");
    }

    // -----------------------------------------------------------------------
    // Crash simulation tests
    //
    // A "crash" is modelled by dropping the MdStorageManager (which closes all
    // OS file handles) without calling immedsync first, then creating a fresh
    // MdStorageManager pointed at the same directory. This simulates a process
    // dying mid-operation: all in-memory state is lost but whatever the OS had
    // flushed to disk is still there.
    //
    // Three scenarios are tested:
    //
    //   1. Crash-after-write: data written before the crash is recoverable.
    //   2. Torn write (crash mid-extend): the file is left at a
    //      non-block-aligned size. nblocks() floors to complete blocks and
    //      reading the partial block fails.
    //   3. Crash after truncate: inactive (zero-length) segment files left
    //      on disk are correctly skipped by nblocks() after reopen.
    // -----------------------------------------------------------------------

    /// Crash after writing blocks but before calling immedsync.
    ///
    /// The OS write-back cache typically preserves data written before a crash,
    /// so we expect all blocks to be readable after reopen. This test confirms
    /// that smgr does not rely on any in-memory state to reconstruct the
    /// relation — everything it needs is on disk.
    #[test]
    fn test_crash_after_write() {
        let (mut smgr, base) = temp_smgr("crash_after_write");
        let rel = test_rel(20000);

        // Write 5 blocks with known patterns, no fsync.
        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();
        for i in 0..5u32 {
            smgr.extend(rel, ForkNumber::Main, i, &page_pattern(i), true).unwrap();
        }

        // Simulate crash: drop smgr without syncing, losing all in-memory state.
        drop(smgr);

        // Reopen with a brand-new storage manager — no memory of the previous one.
        let mut smgr2 = MdStorageManager::new(&base);

        // All 5 blocks must still be present and correct.
        assert_eq!(smgr2.nblocks(rel, ForkNumber::Main).unwrap(), 5,
            "nblocks should survive crash");

        let mut buf = vec![0u8; BLCKSZ];
        for i in 0..5u32 {
            smgr2.read_block(rel, ForkNumber::Main, i, &mut buf).unwrap();
            assert_eq!(buf, page_pattern(i), "block {} data wrong after crash", i);
        }
    }

    /// Crash mid-extend: the segment file is left at a non-block-aligned size,
    /// as if the process died partway through writing a new block.
    ///
    /// nblocks() should floor to the number of complete blocks, and attempting
    /// to read the partial block should fail gracefully with BlockOutOfRange.
    #[test]
    fn test_crash_torn_write() {
        let (mut smgr, base) = temp_smgr("crash_torn_write");
        let rel = test_rel(20001);

        // Write 3 complete blocks.
        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();
        for i in 0..3u32 {
            smgr.extend(rel, ForkNumber::Main, i, &page_pattern(i), true).unwrap();
        }
        drop(smgr);

        // Simulate a torn write: truncate the segment file to a size that is
        // not a multiple of BLCKSZ — as if the process wrote a partial block
        // and then crashed. We leave 3 complete blocks plus 100 stray bytes.
        let seg_path = base.join("1").join("20001");
        let partial_size = 3 * BLCKSZ as u64 + 100;
        {
            let f = fs::OpenOptions::new().write(true).open(&seg_path).unwrap();
            f.set_len(partial_size).unwrap();
        }
        assert_eq!(fs::metadata(&seg_path).unwrap().len(), partial_size);

        // Reopen with a fresh storage manager.
        let mut smgr2 = MdStorageManager::new(&base);

        // nblocks() must floor to 3 (ignoring the 100-byte tail).
        assert_eq!(smgr2.nblocks(rel, ForkNumber::Main).unwrap(), 3,
            "nblocks should floor to complete blocks after torn write");

        // The 3 complete blocks must be intact.
        let mut buf = vec![0u8; BLCKSZ];
        for i in 0..3u32 {
            smgr2.read_block(rel, ForkNumber::Main, i, &mut buf).unwrap();
            assert_eq!(buf, page_pattern(i), "block {} wrong after torn write", i);
        }

        // Reading block 3 (which only partially exists) must fail.
        let err = smgr2.read_block(rel, ForkNumber::Main, 3, &mut buf);
        assert!(matches!(err, Err(SmgrError::BlockOutOfRange { .. })),
            "reading partial block should return BlockOutOfRange");
    }

    /// Crash after truncate: the storage manager deactivated excess segments
    /// (truncated them to 0 bytes) but then crashed before any checkpoint
    /// could clean them up. After reopen, the zero-length inactive segments
    /// must be invisible to nblocks() and the relation must appear at its
    /// post-truncate size.
    #[test]
    fn test_crash_after_truncate_inactive_segments() {
        let (mut smgr, base) = temp_smgr("crash_after_truncate");
        let rel = test_rel(20002);

        // Write 5 blocks.
        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();
        for i in 0..5u32 {
            smgr.extend(rel, ForkNumber::Main, i, &page_pattern(i), true).unwrap();
        }

        // Manually place a fake segment 1 on disk (simulates a relation that
        // once spanned two segments). We can't write 131072 real blocks in a
        // unit test, so we craft the file directly.
        let seg1_path = base.join("1").join("20002.1");
        {
            let mut f = fs::File::create(&seg1_path).unwrap();
            // A few blocks worth of data so it looks like a real segment.
            f.write_all(&page_pattern(99).repeat(3)).unwrap();
        }
        assert_eq!(
            fs::metadata(&seg1_path).unwrap().len(),
            3 * BLCKSZ as u64,
            "segment 1 should have 3 blocks before truncate"
        );

        // Truncate to 3 blocks (non-recovery mode: segment 1 deactivated, not removed).
        smgr.truncate(rel, ForkNumber::Main, 3).unwrap();
        assert_eq!(fs::metadata(&seg1_path).unwrap().len(), 0,
            "segment 1 should be zero-length (inactive) after truncate");

        // Crash: drop without syncing.
        drop(smgr);

        // Reopen with a fresh storage manager.
        let mut smgr2 = MdStorageManager::new(&base);

        // The inactive segment 1 is still on disk (zero bytes).
        assert!(seg1_path.exists(), "inactive segment should still exist after crash");
        assert_eq!(fs::metadata(&seg1_path).unwrap().len(), 0);

        // nblocks() must report 3, not be confused by the zero-length segment 1.
        assert_eq!(smgr2.nblocks(rel, ForkNumber::Main).unwrap(), 3,
            "inactive zero-length segment should not contribute to nblocks");

        // The first 3 blocks must be intact.
        let mut buf = vec![0u8; BLCKSZ];
        for i in 0..3u32 {
            smgr2.read_block(rel, ForkNumber::Main, i, &mut buf).unwrap();
            assert_eq!(buf, page_pattern(i), "block {} wrong after crash+truncate", i);
        }
    }
}
