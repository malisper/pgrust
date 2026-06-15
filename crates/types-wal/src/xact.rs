//! Transaction WAL-record vocabulary (`access/xact.h`): the RM_XACT_ID record
//! opcodes, xinfo flags, and the parsed commit/abort record shapes shared by
//! the xact engine, the rmgr-desc unit, and the 2PC machinery.

use alloc::vec::Vec;

use types_core::primitive::{Oid, RepOriginId, TimestampTz, TransactionId, XLogRecPtr};
use types_storage::{RelFileLocator, SharedInvalidationMessage};

// --- record opcodes stored in xl_info, masked by XLOG_XACT_OPMASK ---
pub const XLOG_XACT_COMMIT: u8 = 0x00;
pub const XLOG_XACT_PREPARE: u8 = 0x10;
pub const XLOG_XACT_ABORT: u8 = 0x20;
pub const XLOG_XACT_COMMIT_PREPARED: u8 = 0x30;
pub const XLOG_XACT_ABORT_PREPARED: u8 = 0x40;
pub const XLOG_XACT_ASSIGNMENT: u8 = 0x50;
pub const XLOG_XACT_INVALIDATIONS: u8 = 0x60;
/// mask for filtering opcodes out of `xl_info`
pub const XLOG_XACT_OPMASK: u8 = 0x70;
/// does this record have an `xinfo` field or not
pub const XLOG_XACT_HAS_INFO: u8 = 0x80;

// --- xinfo flags ---
pub const XACT_XINFO_HAS_DBINFO: u32 = 1 << 0;
pub const XACT_XINFO_HAS_SUBXACTS: u32 = 1 << 1;
pub const XACT_XINFO_HAS_RELFILELOCATORS: u32 = 1 << 2;
pub const XACT_XINFO_HAS_INVALS: u32 = 1 << 3;
pub const XACT_XINFO_HAS_TWOPHASE: u32 = 1 << 4;
pub const XACT_XINFO_HAS_ORIGIN: u32 = 1 << 5;
pub const XACT_XINFO_HAS_AE_LOCKS: u32 = 1 << 6;
pub const XACT_XINFO_HAS_GID: u32 = 1 << 7;
pub const XACT_XINFO_HAS_DROPPED_STATS: u32 = 1 << 8;

// --- "completion" flags stored in the high bits of xinfo ---
pub const XACT_COMPLETION_APPLY_FEEDBACK: u32 = 1 << 29;
pub const XACT_COMPLETION_UPDATE_RELCACHE_FILE: u32 = 1 << 30;
pub const XACT_COMPLETION_FORCE_SYNC_COMMIT: u32 = 1 << 31;

/// `XactCompletionRelcacheInitFileInval(xinfo)`
pub const fn xact_completion_relcache_init_file_inval(xinfo: u32) -> bool {
    xinfo & XACT_COMPLETION_UPDATE_RELCACHE_FILE != 0
}
/// `XactCompletionForceSyncCommit(xinfo)`
pub const fn xact_completion_force_sync_commit(xinfo: u32) -> bool {
    xinfo & XACT_COMPLETION_FORCE_SYNC_COMMIT != 0
}
/// `XactCompletionApplyFeedback(xinfo)`
pub const fn xact_completion_apply_feedback(xinfo: u32) -> bool {
    xinfo & XACT_COMPLETION_APPLY_FEEDBACK != 0
}

/// One dropped pg_stat item, matching C's `xl_xact_stats_item`
/// (`{ int kind; Oid dboid; uint32 objid_lo; uint32 objid_hi; }`, 16 bytes;
/// the 64-bit objid is reassembled from its two halves). Canonically defined in
/// `types_core` (shared with the xact-system scalar vocabulary).
pub use types_core::xact::XlXactStatsItem;

/// `xl_xact_parsed_commit` (`access/xact.h`), the decoded form of a commit
/// record.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ParsedCommit {
    pub xact_time: TimestampTz,
    pub xinfo: u32,
    pub db_id: Oid,
    pub ts_id: Oid,
    pub subxacts: Vec<TransactionId>,
    pub xlocators: Vec<RelFileLocator>,
    pub stats: Vec<XlXactStatsItem>,
    /// `msgs`/`nmsgs` — the record's `SharedInvalidationMessage` array.
    pub msgs: Vec<SharedInvalidationMessage>,
    pub twophase_xid: TransactionId,
    pub twophase_gid: Vec<u8>,
    pub origin_lsn: XLogRecPtr,
    pub origin_timestamp: TimestampTz,
}

/// `xl_xact_parsed_abort` (`access/xact.h`).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ParsedAbort {
    pub xact_time: TimestampTz,
    pub xinfo: u32,
    pub db_id: Oid,
    pub ts_id: Oid,
    pub subxacts: Vec<TransactionId>,
    pub xlocators: Vec<RelFileLocator>,
    pub stats: Vec<XlXactStatsItem>,
    pub twophase_xid: TransactionId,
    pub twophase_gid: Vec<u8>,
    pub origin_lsn: XLogRecPtr,
    pub origin_timestamp: TimestampTz,
}

/// `RepOriginId` re-exported for parsed-record consumers.
pub type XactRepOriginId = RepOriginId;
