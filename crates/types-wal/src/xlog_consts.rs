//! WAL-engine configuration vocabulary and segment/control constants shared by
//! the xlog units (`access/xlog.h`, `access/xlogdefs.h`, `access/xlog_internal.h`,
//! `catalog/pg_control.h`, `storage/proc.h`). Trimmed to what the xlog crate
//! and its callers consume.

use types_core::{TimeLineID, XLogRecPtr};

// ===========================================================================
// Segment / page geometry (access/xlog_internal.h, pg_config.h).
// ===========================================================================

/// `DEFAULT_XLOG_SEG_SIZE` (pg_config.h) — 16 MB.
pub const DEFAULT_XLOG_SEG_SIZE: i32 = 16 * 1024 * 1024;
/// `XLOG_BLCKSZ` (pg_config.h) — WAL page size, 8 KB.
pub const XLOG_BLCKSZ: usize = 8192;
/// `WalSegMinSize` (access/xlog_internal.h) — 1 MB.
pub const WAL_SEG_MIN_SIZE: i32 = 1024 * 1024;
/// `WalSegMaxSize` (access/xlog_internal.h) — 1 GB.
pub const WAL_SEG_MAX_SIZE: i32 = 1024 * 1024 * 1024;
/// `XLOG_FNAME_LEN` (access/xlog_internal.h) — 24 hex chars.
pub const XLOG_FNAME_LEN: usize = 24;
/// `XLOGDIR` (access/xlog_internal.h).
pub const XLOGDIR: &str = "pg_wal";
/// `SizeOfXLogShortPHD` (access/xlog_internal.h) — `MAXALIGN(sizeof(XLogPageHeaderData))`.
pub const SIZE_OF_XLOG_SHORT_PHD: usize = 24;
/// `SizeOfXLogLongPHD` (access/xlog_internal.h) — `MAXALIGN(sizeof(XLogLongPageHeaderData))`.
pub const SIZE_OF_XLOG_LONG_PHD: usize = 40;

// ===========================================================================
// wal_sync_method (access/xlogdefs.h, WalSyncMethod enum).
// ===========================================================================

/// `WalSyncMethod` (access/xlogdefs.h). `repr(i32)` matching the C enum order.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
#[repr(i32)]
pub enum WalSyncMethod {
    Fsync = 0,
    Fdatasync = 1,
    Open = 2,
    FsyncWritethrough = 3,
    OpenDsync = 4,
}

// ===========================================================================
// archive_mode (access/xlog.h, ArchiveMode enum).
// ===========================================================================

/// `ArchiveMode` (access/xlog.h).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
#[repr(i32)]
pub enum ArchiveMode {
    Off = 0,
    On = 1,
    Always = 2,
}

// ===========================================================================
// wal_level (access/xlog.h, WalLevel enum).
// ===========================================================================

/// `WalLevel` (access/xlog.h). The `>=` comparisons (`XLogIsNeeded`,
/// `XLogStandbyInfoActive`, `XLogLogicalInfoActive`) are over this order.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
#[repr(i32)]
pub enum WalLevel {
    Minimal = 0,
    Replica = 1,
    Logical = 2,
}

/// `WAL_LEVEL_MINIMAL` — name alias for the `wal_level` enum value, for
/// callers that compare an owner-returned `WalLevel` against the named levels.
pub const WAL_LEVEL_MINIMAL: WalLevel = WalLevel::Minimal;
/// `WAL_LEVEL_REPLICA`.
pub const WAL_LEVEL_REPLICA: WalLevel = WalLevel::Replica;
/// `WAL_LEVEL_LOGICAL`.
pub const WAL_LEVEL_LOGICAL: WalLevel = WalLevel::Logical;

// ===========================================================================
// wal_compression (access/xlog.h, WalCompression enum).
// ===========================================================================

/// `WalCompression` (access/xlog.h).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
#[repr(i32)]
pub enum WalCompression {
    None = 0,
    Pglz = 1,
    Lz4 = 2,
    Zstd = 3,
}

// ===========================================================================
// RecoveryState (access/xlog.h, RecoveryState enum).
// ===========================================================================

/// `RecoveryState` (access/xlog.h) — the result of `GetRecoveryState()`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum RecoveryState {
    Crash = 0,
    Archive = 1,
    Done = 2,
}

// ===========================================================================
// WALAvailability (access/xlog.h, WALAvailability enum).
// ===========================================================================

/// `WALAvailability` (access/xlog.h) — `GetWALAvailability()`'s classification
/// of a WAL position's retention state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum WALAvailability {
    /// `WALAVAIL_INVALID_LSN` — parameter error.
    InvalidLsn = 0,
    /// `WALAVAIL_RESERVED` — within `max_wal_size`.
    Reserved = 1,
    /// `WALAVAIL_EXTENDED` — reserved by a slot / `wal_keep_size`.
    Extended = 2,
    /// `WALAVAIL_UNRESERVED` — no longer reserved, not removed yet.
    Unreserved = 3,
    /// `WALAVAIL_REMOVED` — segment has been removed.
    Removed = 4,
}

// ===========================================================================
// CHECKPOINT_* flag bits (access/xlog.h).
// ===========================================================================

pub const CHECKPOINT_IS_SHUTDOWN: i32 = 0x0001;
pub const CHECKPOINT_END_OF_RECOVERY: i32 = 0x0002;
pub const CHECKPOINT_IMMEDIATE: i32 = 0x0004;
pub const CHECKPOINT_FORCE: i32 = 0x0008;
pub const CHECKPOINT_FLUSH_ALL: i32 = 0x0010;
pub const CHECKPOINT_WAIT: i32 = 0x0020;
pub const CHECKPOINT_REQUESTED: i32 = 0x0040;
pub const CHECKPOINT_CAUSE_XLOG: i32 = 0x0080;
pub const CHECKPOINT_CAUSE_TIME: i32 = 0x0100;

// ===========================================================================
// DELAY_CHKPT_* flag bits (storage/proc.h) — the checkpoint-delay phases a
// backend in a commit critical section sets in `MyProc->delayChkpt`.
// ===========================================================================

/// `DELAY_CHKPT_START` (storage/proc.h) — prevent phase-1->phase-2 transition.
pub const DELAY_CHKPT_START: i32 = 1 << 0;
/// `DELAY_CHKPT_COMPLETE` (storage/proc.h) — prevent phase-2->phase-3 transition.
pub const DELAY_CHKPT_COMPLETE: i32 = 1 << 1;

// ===========================================================================
// Invalid sentinels (access/xlogdefs.h).
// ===========================================================================

/// `InvalidXLogRecPtr` (access/xlogdefs.h).
pub const INVALID_XLOG_REC_PTR: XLogRecPtr = 0;
/// `BootstrapTimeLineID` (access/transam/xlog.c).
pub const BOOTSTRAP_TIME_LINE_ID: TimeLineID = 1;
