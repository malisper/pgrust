use core::ffi::{c_char, c_int};

use crate::wal::RelFileLocator;
use crate::{Oid, RepOriginId, TimestampTz, TransactionId, XLogRecPtr};

pub type LocalTransactionId = u32;
pub type SubTransactionId = u32;
pub type CommandId = u32;
pub type TransState = u32;
pub type TBlockState = u32;
pub type XidStatus = c_int;

pub const InvalidTransactionId: TransactionId = 0;
pub const BootstrapTransactionId: TransactionId = 1;
pub const FrozenTransactionId: TransactionId = 2;
pub const FirstNormalTransactionId: TransactionId = 3;
/// `MaxTransactionId` (access/transam.h) -- the largest 32-bit `TransactionId`.
pub const MaxTransactionId: TransactionId = 0xFFFF_FFFF;

// --- OID assignment thresholds (access/transam.h) ---
/// `FirstGenbkiObjectId` -- first OID assignable by genbki.pl.
pub const FirstGenbkiObjectId: Oid = 10000;
/// `FirstUnpinnedObjectId` -- first OID that is not pinned.
pub const FirstUnpinnedObjectId: Oid = 12000;
/// `FirstNormalObjectId` -- first OID assignable to user objects.
pub const FirstNormalObjectId: Oid = 16384;
pub const InvalidLocalTransactionId: LocalTransactionId = 0;
pub const InvalidSubTransactionId: SubTransactionId = 0;
pub const TopSubTransactionId: SubTransactionId = 1;
pub const FirstCommandId: CommandId = 0;
pub const InvalidCommandId: CommandId = !0;
pub const InvalidXLogRecPtr: XLogRecPtr = 0;

pub const TRANSACTION_STATUS_IN_PROGRESS: XidStatus = 0;
pub const TRANSACTION_STATUS_COMMITTED: XidStatus = 0x01;
pub const TRANSACTION_STATUS_ABORTED: XidStatus = 0x02;
pub const TRANSACTION_STATUS_SUB_COMMITTED: XidStatus = 0x03;

pub const XACT_READ_UNCOMMITTED: c_int = 0;
pub const XACT_READ_COMMITTED: c_int = 1;
pub const XACT_REPEATABLE_READ: c_int = 2;
pub const XACT_SERIALIZABLE: c_int = 3;

pub const SYNCHRONOUS_COMMIT_OFF: c_int = 0;
pub const SYNCHRONOUS_COMMIT_LOCAL_FLUSH: c_int = 1;
pub const SYNCHRONOUS_COMMIT_REMOTE_WRITE: c_int = 2;
pub const SYNCHRONOUS_COMMIT_REMOTE_FLUSH: c_int = 3;
pub const SYNCHRONOUS_COMMIT_REMOTE_APPLY: c_int = 4;
pub const SYNCHRONOUS_COMMIT_ON: c_int = SYNCHRONOUS_COMMIT_REMOTE_FLUSH;

pub const XACT_FLAGS_ACCESSEDTEMPNAMESPACE: c_int = 1 << 0;
pub const XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK: c_int = 1 << 1;
pub const XACT_FLAGS_NEEDIMMEDIATECOMMIT: c_int = 1 << 2;
pub const XACT_FLAGS_PIPELINING: c_int = 1 << 3;

// --- WAL record opcodes for RM_XACT_ID (access/xact.h). These are stored in
// the upper bits of XLogRecord->xl_info, masked by XLOG_XACT_OPMASK. ---
pub const XLOG_XACT_COMMIT: u8 = 0x00;
pub const XLOG_XACT_PREPARE: u8 = 0x10;
pub const XLOG_XACT_ABORT: u8 = 0x20;
pub const XLOG_XACT_COMMIT_PREPARED: u8 = 0x30;
pub const XLOG_XACT_ABORT_PREPARED: u8 = 0x40;
pub const XLOG_XACT_ASSIGNMENT: u8 = 0x50;
pub const XLOG_XACT_INVALIDATIONS: u8 = 0x60;
/// mask for filtering opcodes out of `xl_info`
pub const XLOG_XACT_OPMASK: u8 = 0x70;
/// does this record have a 'xinfo' field or not
pub const XLOG_XACT_HAS_INFO: u8 = 0x80;

// --- xinfo flags (access/xact.h) ---
pub const XACT_XINFO_HAS_DBINFO: u32 = 1 << 0;
pub const XACT_XINFO_HAS_SUBXACTS: u32 = 1 << 1;
pub const XACT_XINFO_HAS_RELFILELOCATORS: u32 = 1 << 2;
pub const XACT_XINFO_HAS_INVALS: u32 = 1 << 3;
pub const XACT_XINFO_HAS_TWOPHASE: u32 = 1 << 4;
pub const XACT_XINFO_HAS_ORIGIN: u32 = 1 << 5;
pub const XACT_XINFO_HAS_AE_LOCKS: u32 = 1 << 6;
pub const XACT_XINFO_HAS_GID: u32 = 1 << 7;
pub const XACT_XINFO_HAS_DROPPED_STATS: u32 = 1 << 8;

// --- "completion" flags stored in the high bits of xinfo (access/xact.h) ---
pub const XACT_COMPLETION_APPLY_FEEDBACK: u32 = 1 << 29;
pub const XACT_COMPLETION_UPDATE_RELCACHE_FILE: u32 = 1 << 30;
pub const XACT_COMPLETION_FORCE_SYNC_COMMIT: u32 = 1 << 31;

pub const TRANS_DEFAULT: TransState = 0;
pub const TRANS_START: TransState = 1;
pub const TRANS_INPROGRESS: TransState = 2;
pub const TRANS_COMMIT: TransState = 3;
pub const TRANS_ABORT: TransState = 4;
pub const TRANS_PREPARE: TransState = 5;

pub const TBLOCK_DEFAULT: TBlockState = 0;
pub const TBLOCK_STARTED: TBlockState = 1;
pub const TBLOCK_BEGIN: TBlockState = 2;
pub const TBLOCK_INPROGRESS: TBlockState = 3;
pub const TBLOCK_IMPLICIT_INPROGRESS: TBlockState = 4;
pub const TBLOCK_PARALLEL_INPROGRESS: TBlockState = 5;
pub const TBLOCK_END: TBlockState = 6;
pub const TBLOCK_ABORT: TBlockState = 7;
pub const TBLOCK_ABORT_END: TBlockState = 8;
pub const TBLOCK_ABORT_PENDING: TBlockState = 9;
pub const TBLOCK_PREPARE: TBlockState = 10;
pub const TBLOCK_SUBBEGIN: TBlockState = 11;
pub const TBLOCK_SUBINPROGRESS: TBlockState = 12;
pub const TBLOCK_SUBRELEASE: TBlockState = 13;
pub const TBLOCK_SUBCOMMIT: TBlockState = 14;
pub const TBLOCK_SUBABORT: TBlockState = 15;
pub const TBLOCK_SUBABORT_END: TBlockState = 16;
pub const TBLOCK_SUBABORT_PENDING: TBlockState = 17;
pub const TBLOCK_SUBRESTART: TBlockState = 18;
pub const TBLOCK_SUBABORT_RESTART: TBlockState = 19;

pub const XACT_EVENT_COMMIT: u32 = 0;
pub const XACT_EVENT_PARALLEL_COMMIT: u32 = 1;
pub const XACT_EVENT_ABORT: u32 = 2;
pub const XACT_EVENT_PARALLEL_ABORT: u32 = 3;
pub const XACT_EVENT_PREPARE: u32 = 4;
pub const XACT_EVENT_PRE_COMMIT: u32 = 5;
pub const XACT_EVENT_PARALLEL_PRE_COMMIT: u32 = 6;
pub const XACT_EVENT_PRE_PREPARE: u32 = 7;

pub const SUBXACT_EVENT_START_SUB: u32 = 0;
pub const SUBXACT_EVENT_COMMIT_SUB: u32 = 1;
pub const SUBXACT_EVENT_ABORT_SUB: u32 = 2;
pub const SUBXACT_EVENT_PRE_COMMIT_SUB: u32 = 3;

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FullTransactionId {
    pub value: u64,
}

impl FullTransactionId {
    pub const fn from_epoch_and_xid(epoch: u32, xid: TransactionId) -> Self {
        Self {
            value: ((epoch as u64) << 32) | xid as u64,
        }
    }

    pub const fn xid(self) -> TransactionId {
        self.value as TransactionId
    }

    pub const fn is_valid(self) -> bool {
        self.xid() != InvalidTransactionId
    }
}

/// `FirstNormalFullTransactionId` (`access/transam.h`) -- the smallest normal
/// `FullTransactionId` (epoch 0, xid = `FirstNormalTransactionId`).
pub const FirstNormalFullTransactionId: FullTransactionId =
    FullTransactionId::from_epoch_and_xid(0, FirstNormalTransactionId);

/// `TransamVariablesData` (`access/transam.h`) -- the shared variable-cache
/// state for transaction/OID assignment. Field order and types mirror the C
/// struct exactly (each field's lock group is preserved as a comment). The
/// lock-protected access discipline lives in the backend; this is just the
/// in-memory layout shared across the transam crates.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TransamVariablesData {
    // Protected by OidGenLock.
    /// next OID to assign
    pub nextOid: Oid,
    /// OIDs available before must do XLOG work
    pub oidCount: u32,

    // Protected by XidGenLock.
    /// next XID to assign
    pub nextXid: FullTransactionId,
    /// cluster-wide minimum datfrozenxid
    pub oldestXid: TransactionId,
    /// start forcing autovacuums here
    pub xidVacLimit: TransactionId,
    /// start complaining here
    pub xidWarnLimit: TransactionId,
    /// refuse to advance nextXid beyond here
    pub xidStopLimit: TransactionId,
    /// where the world ends
    pub xidWrapLimit: TransactionId,
    /// database with minimum datfrozenxid
    pub oldestXidDB: Oid,

    // Protected by CommitTsLock.
    pub oldestCommitTsXid: TransactionId,
    pub newestCommitTsXid: TransactionId,

    // Protected by ProcArrayLock.
    /// newest full XID that has committed or aborted
    pub latestCompletedXid: FullTransactionId,

    /// Number of top-level transactions with xids that completed since the
    /// start of the server. Always above 1.
    pub xactCompletionCount: u64,

    // Protected by XactTruncationLock.
    /// oldest it's safe to look up in clog
    pub oldestClogXid: TransactionId,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SavedTransactionCharacteristics {
    pub save_XactIsoLevel: c_int,
    pub save_XactReadOnly: bool,
    pub save_XactDeferrable: bool,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SerializedTransactionStateHeader {
    pub xactIsoLevel: c_int,
    pub xactDeferrable: bool,
    pub topFullTransactionId: FullTransactionId,
    pub currentFullTransactionId: FullTransactionId,
    pub currentCommandId: CommandId,
    pub nParallelCurrentXids: c_int,
}

/// One dropped pg_stat item, matching C's `xl_xact_stats_item`
/// (`{ int kind; Oid dboid; uint64 objid; }`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct XlXactStatsItem {
    pub kind: c_int,
    pub dboid: Oid,
    pub objid: u64,
}

/// Replication-origin metadata carried on a commit/abort record, matching
/// C's `xl_xact_origin` (`{ XLogRecPtr origin_lsn; TimestampTz origin_timestamp; }`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct XlXactOrigin {
    pub origin_lsn: XLogRecPtr,
    pub origin_timestamp: TimestampTz,
}

/// Full argument list for `XactLogCommitRecord`, mirroring the C signature.
/// The crate uses these to derive `xinfo` + opcode and assemble the record body.
#[derive(Clone, Copy, Debug)]
pub struct XactLogCommitRecordArgs<'a> {
    pub commit_time: TimestampTz,
    pub subxacts: &'a [TransactionId],
    pub rels: &'a [RelFileLocator],
    pub dropped_stats: &'a [XlXactStatsItem],
    pub msgs: &'a [u8],
    pub nmsgs: c_int,
    pub relcache_inval: bool,
    pub xactflags: c_int,
    pub twophase_xid: TransactionId,
    pub twophase_gid: Option<&'a core::ffi::CStr>,
    /// Snapshot of backend globals the format depends on.
    pub force_sync_commit: bool,
    pub synchronous_commit: c_int,
    pub xlog_logical_info_active: bool,
    pub my_database_id: Oid,
    pub my_database_table_space: Oid,
    pub replorigin_session_origin: RepOriginId,
    pub origin: Option<XlXactOrigin>,
}

/// Full argument list for `XactLogAbortRecord`, mirroring the C signature.
#[derive(Clone, Copy, Debug)]
pub struct XactLogAbortRecordArgs<'a> {
    pub abort_time: TimestampTz,
    pub subxacts: &'a [TransactionId],
    pub rels: &'a [RelFileLocator],
    pub dropped_stats: &'a [XlXactStatsItem],
    pub xactflags: c_int,
    pub twophase_xid: TransactionId,
    pub twophase_gid: Option<&'a core::ffi::CStr>,
    pub xlog_logical_info_active: bool,
    pub my_database_id: Oid,
    pub my_database_table_space: Oid,
    pub replorigin_session_origin: RepOriginId,
    pub origin: Option<XlXactOrigin>,
}

pub type XactCallback = fn(event: u32, arg: *mut core::ffi::c_void);
pub type SubXactCallback = fn(
    event: u32,
    my_subid: SubTransactionId,
    parent_subid: SubTransactionId,
    arg: *mut core::ffi::c_void,
);

pub type TransactionIdSlice<'a> = &'a [TransactionId];

pub type XactRedoRecord = *mut core::ffi::c_void;
pub type PrepareGid = *const c_char;
pub type WalRecordPointer = XLogRecPtr;
pub type PrevUser = Oid;

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    #[test]
    fn transaction_abi_layouts_match_postgres() {
        assert_eq!(size_of::<FullTransactionId>(), 8);
        assert_eq!(align_of::<FullTransactionId>(), 8);
        assert_eq!(offset_of!(FullTransactionId, value), 0);

        assert_eq!(size_of::<SavedTransactionCharacteristics>(), 8);
        assert_eq!(align_of::<SavedTransactionCharacteristics>(), 4);
        assert_eq!(
            offset_of!(SavedTransactionCharacteristics, save_XactIsoLevel),
            0
        );
        assert_eq!(
            offset_of!(SavedTransactionCharacteristics, save_XactReadOnly),
            4
        );
        assert_eq!(
            offset_of!(SavedTransactionCharacteristics, save_XactDeferrable),
            5
        );

        assert_eq!(size_of::<SerializedTransactionStateHeader>(), 32);
        assert_eq!(align_of::<SerializedTransactionStateHeader>(), 8);
        assert_eq!(
            offset_of!(SerializedTransactionStateHeader, xactIsoLevel),
            0
        );
        assert_eq!(
            offset_of!(SerializedTransactionStateHeader, xactDeferrable),
            4
        );
        assert_eq!(
            offset_of!(SerializedTransactionStateHeader, topFullTransactionId),
            8
        );
        assert_eq!(
            offset_of!(SerializedTransactionStateHeader, currentFullTransactionId),
            16
        );
        assert_eq!(
            offset_of!(SerializedTransactionStateHeader, currentCommandId),
            24
        );
        assert_eq!(
            offset_of!(SerializedTransactionStateHeader, nParallelCurrentXids),
            28
        );
    }
}
