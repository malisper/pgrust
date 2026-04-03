//! Storage manager dispatch layer — mirrors PostgreSQL's `smgr.c`.
//!
//! This module owns the `StorageManager` trait (the vtable) and the shared
//! types used across the smgr boundary. The concrete implementation lives in
//! `md.rs`, mirroring `md.c`.
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
//! file; segment N is named `<base>.<N>`.
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
//! # What is intentionally left out
//!
//! See `pgrust/plans/smgr-deferred-features.md` for details on async I/O,
//! deferred fsync, non-default tablespace paths, and the SMgrRelation hash
//! table / pin system.

pub mod md;
pub use md::MdStorageManager;

use std::io;
use std::path::{Path, PathBuf};

// ---------------------------------------------------------------------------
// Public constants
// ---------------------------------------------------------------------------

/// The size of a single database page, in bytes.
/// Matches PostgreSQL's `BLCKSZ` (the default build value).
pub const BLCKSZ: usize = 8192;

/// Number of blocks per segment file.
///
/// PostgreSQL's `RELSEG_SIZE` defaults to 131072 blocks = 1 GiB.
pub const RELSEG_SIZE: u32 = 131_072;

/// Sentinel value for "no valid block number".
/// Mirrors `InvalidBlockNumber` in `storage/block.h`.
pub const INVALID_BLOCK_NUMBER: u32 = u32::MAX;

/// Maximum number of blocks that can be coalesced into a single vectored I/O.
///
/// Mirrors `MAX_IO_COMBINE_LIMIT` in PostgreSQL's AIO subsystem.
pub const MAX_IO_COMBINE_LIMIT: u32 = 64;

// ---------------------------------------------------------------------------
// Core types
// ---------------------------------------------------------------------------

/// 0-based index of a page within a relation fork.
pub type BlockNumber = u32;

/// Identifies the physical on-disk location of a relation.
///
/// In PostgreSQL this is `RelFileLocator` (struct with spcOid, dbOid,
/// relNumber).
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
    Main,
    /// Free Space Map fork. Fork 1.
    Fsm,
    /// Visibility Map fork. Fork 2.
    VisibilityMap,
    /// Initialization fork for unlogged relations. Fork 3.
    Init,
    /// Extension point for future or custom fork numbers.
    Other(u8),
}

impl ForkNumber {
    /// Returns the canonical integer fork number used by PostgreSQL.
    pub fn as_u8(self) -> u8 {
        match self {
            ForkNumber::Main => 0,
            ForkNumber::Fsm => 1,
            ForkNumber::VisibilityMap => 2,
            ForkNumber::Init => 3,
            ForkNumber::Other(n) => n,
        }
    }

    /// Returns the filename suffix for this fork.
    fn suffix(self) -> String {
        match self {
            ForkNumber::Main => String::new(),
            ForkNumber::Fsm => "_fsm".to_string(),
            ForkNumber::VisibilityMap => "_vm".to_string(),
            ForkNumber::Init => "_init".to_string(),
            ForkNumber::Other(n) => format!("_fork{}", n),
        }
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors that can be returned by storage manager operations.
#[derive(Debug)]
pub enum SmgrError {
    RelationNotFound {
        rel: RelFileLocator,
        fork: ForkNumber,
    },
    BlockOutOfRange {
        rel: RelFileLocator,
        fork: ForkNumber,
        block: BlockNumber,
    },
    AlreadyExists {
        rel: RelFileLocator,
        fork: ForkNumber,
    },
    ShortIo {
        expected: usize,
        actual: usize,
    },
    BadBufferSize {
        size: usize,
    },
    Io(io::Error),
}

impl std::fmt::Display for SmgrError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SmgrError::RelationNotFound { rel, fork } => write!(
                f,
                "relation {}/{}/{} fork {:?} not found",
                rel.spc_oid, rel.db_oid, rel.rel_number, fork
            ),
            SmgrError::BlockOutOfRange { rel, fork, block } => write!(
                f,
                "block {} out of range for {}/{}/{} fork {:?}",
                block, rel.spc_oid, rel.db_oid, rel.rel_number, fork
            ),
            SmgrError::AlreadyExists { rel, fork } => write!(
                f,
                "relation {}/{}/{} fork {:?} already exists",
                rel.spc_oid, rel.db_oid, rel.rel_number, fork
            ),
            SmgrError::ShortIo { expected, actual } => {
                write!(f, "short I/O: expected {expected} bytes, got {actual}")
            }
            SmgrError::BadBufferSize { size } => {
                write!(f, "buffer must be exactly {BLCKSZ} bytes, got {size}")
            }
            SmgrError::Io(e) => write!(f, "I/O error: {e}"),
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
pub trait StorageManager {
    fn open(&mut self, rel: RelFileLocator) -> Result<(), SmgrError>;
    fn close(&mut self, rel: RelFileLocator, fork: ForkNumber) -> Result<(), SmgrError>;
    fn create(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        is_redo: bool,
    ) -> Result<(), SmgrError>;
    fn exists(&mut self, rel: RelFileLocator, fork: ForkNumber) -> bool;
    fn unlink(&mut self, rel: RelFileLocator, fork: Option<ForkNumber>, is_redo: bool);
    fn read_block(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        block: BlockNumber,
        buf: &mut [u8],
    ) -> Result<(), SmgrError>;
    fn write_block(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        block: BlockNumber,
        data: &[u8],
        skip_fsync: bool,
    ) -> Result<(), SmgrError>;
    fn writeback(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        block: BlockNumber,
        nblocks: u32,
    ) -> Result<(), SmgrError>;
    fn prefetch(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        block: BlockNumber,
        nblocks: u32,
    ) -> Result<(), SmgrError>;
    fn max_combine(&self, rel: RelFileLocator, fork: ForkNumber, block: BlockNumber) -> u32;
    #[cfg(unix)]
    fn fd(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        block: BlockNumber,
    ) -> Result<(i32, u64), SmgrError>;
    fn extend(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        block: BlockNumber,
        data: &[u8],
        skip_fsync: bool,
    ) -> Result<(), SmgrError>;
    fn zero_extend(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        block: BlockNumber,
        nblocks: u32,
        skip_fsync: bool,
    ) -> Result<(), SmgrError>;
    fn nblocks(&mut self, rel: RelFileLocator, fork: ForkNumber) -> Result<BlockNumber, SmgrError>;
    fn truncate(
        &mut self,
        rel: RelFileLocator,
        fork: ForkNumber,
        nblocks: BlockNumber,
    ) -> Result<(), SmgrError>;
    fn immedsync(&mut self, rel: RelFileLocator, fork: ForkNumber) -> Result<(), SmgrError>;
}

// ---------------------------------------------------------------------------
// Path construction helpers
// ---------------------------------------------------------------------------

/// Build the filesystem path for a specific segment of a relation fork.
///
/// Maps onto the same naming convention as PostgreSQL's `relpath()`:
///   `<base_dir>/<db_oid>/<rel_number>[<fork_suffix>][.<segno>]`
pub(crate) fn segment_path(
    base_dir: &Path,
    rel: RelFileLocator,
    fork: ForkNumber,
    segno: u32,
) -> PathBuf {
    let db_dir = base_dir.join(rel.db_oid.to_string());
    let fork_suffix = fork.suffix();

    let filename = if segno == 0 {
        format!("{}{}", rel.rel_number, fork_suffix)
    } else {
        format!("{}{}.{}", rel.rel_number, fork_suffix, segno)
    };

    db_dir.join(filename)
}

/// Return the segment number and the block offset within that segment for a
/// given absolute block number.
#[inline]
pub(crate) fn seg_for_block(block: BlockNumber) -> (u32, u32) {
    (block / RELSEG_SIZE, block % RELSEG_SIZE)
}
