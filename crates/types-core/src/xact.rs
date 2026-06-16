//! Transaction-system scalar vocabulary (`c.h`, `access/transam.h`,
//! `access/xact.h`, `storage/lock.h`, `replication/origin.h`).
//!
//! Populated incrementally from ../pgrust/src-idiomatic/crates/types/src/xact.rs
//! as ports need items; only the items currently consumed are present.

use crate::primitive::{LocalTransactionId, RepOriginId, TransactionId, XLogRecPtr};

/// `CommandId` (`c.h`) — a `uint32`.
pub type CommandId = u32;

// `LocalTransactionId` (`c.h`) lives in `primitive`.

/// `SubTransactionId` (`c.h`) — a `uint32`.
pub type SubTransactionId = u32;

/// `XidStatus` (`access/clog.h`) — transaction status in pg_xact, an `int`.
pub type XidStatus = i32;

/// `TransState` (xact.c) — low-level transaction state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum TransState {
    TRANS_DEFAULT,    /* idle */
    TRANS_START,      /* transaction starting */
    TRANS_INPROGRESS, /* inside a valid transaction */
    TRANS_COMMIT,     /* commit in progress */
    TRANS_ABORT,      /* abort in progress */
    TRANS_PREPARE,    /* prepare in progress */
}
pub use TransState::*;

/// `TBlockState` (xact.c) — transaction-block state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum TBlockState {
    /* not-in-transaction-block states */
    TBLOCK_DEFAULT, /* idle */
    TBLOCK_STARTED, /* running single-query transaction */

    /* transaction block states */
    TBLOCK_BEGIN,               /* starting transaction block */
    TBLOCK_INPROGRESS,          /* live transaction */
    TBLOCK_IMPLICIT_INPROGRESS, /* live transaction after implicit BEGIN */
    TBLOCK_PARALLEL_INPROGRESS, /* live transaction inside parallel worker */
    TBLOCK_END,                 /* COMMIT received */
    TBLOCK_ABORT,               /* failed xact, awaiting ROLLBACK */
    TBLOCK_ABORT_END,           /* failed xact, ROLLBACK received */
    TBLOCK_ABORT_PENDING,       /* live xact, ROLLBACK received */
    TBLOCK_PREPARE,             /* live xact, PREPARE received */

    /* subtransaction states */
    TBLOCK_SUBBEGIN,         /* starting a subtransaction */
    TBLOCK_SUBINPROGRESS,    /* live subtransaction */
    TBLOCK_SUBRELEASE,       /* RELEASE received */
    TBLOCK_SUBCOMMIT,        /* COMMIT received while TBLOCK_SUBINPROGRESS */
    TBLOCK_SUBABORT,         /* failed subxact, awaiting ROLLBACK */
    TBLOCK_SUBABORT_END,     /* failed subxact, ROLLBACK received */
    TBLOCK_SUBABORT_PENDING, /* live subxact, ROLLBACK received */
    TBLOCK_SUBRESTART,       /* live subxact, ROLLBACK TO received */
    TBLOCK_SUBABORT_RESTART, /* failed subxact, ROLLBACK TO received */
}
pub use TBlockState::*;

/// `InvalidTransactionId` (`access/transam.h`).
pub const InvalidTransactionId: TransactionId = 0;
pub const BootstrapTransactionId: TransactionId = 1;
pub const FrozenTransactionId: TransactionId = 2;
pub const FirstNormalTransactionId: TransactionId = 3;
/// `MaxTransactionId` (access/transam.h) — the largest 32-bit `TransactionId`.
pub const MaxTransactionId: TransactionId = 0xFFFF_FFFF;

pub const InvalidLocalTransactionId: LocalTransactionId = 0;
/// `InvalidSubTransactionId` (`c.h`).
pub const InvalidSubTransactionId: SubTransactionId = 0;
pub const TopSubTransactionId: SubTransactionId = 1;
pub const FirstCommandId: CommandId = 0;
pub const InvalidCommandId: CommandId = !0;

/// `InvalidXLogRecPtr` (`access/xlogdefs.h`).
pub const InvalidXLogRecPtr: XLogRecPtr = 0;

/// `GIDSIZE` (`access/xact.h`): maximum size of a global transaction id
/// (including the trailing NUL).
pub const GIDSIZE: usize = 200;

pub const TRANSACTION_STATUS_IN_PROGRESS: XidStatus = 0x00;
pub const TRANSACTION_STATUS_COMMITTED: XidStatus = 0x01;
pub const TRANSACTION_STATUS_ABORTED: XidStatus = 0x02;
pub const TRANSACTION_STATUS_SUB_COMMITTED: XidStatus = 0x03;

// `InvalidRepOriginId` lives in `primitive` (`origin.h`).
/// `DoNotReplicateId` — session marker meaning "do not replicate".
pub const DoNotReplicateId: RepOriginId = u16::MAX;

// --- isolation levels (`access/xact.h`) ---
pub const XACT_READ_UNCOMMITTED: i32 = 0;
pub const XACT_READ_COMMITTED: i32 = 1;
pub const XACT_REPEATABLE_READ: i32 = 2;
pub const XACT_SERIALIZABLE: i32 = 3;

// --- synchronous_commit GUC values (`access/xact.h`) ---
pub const SYNCHRONOUS_COMMIT_OFF: i32 = 0;
pub const SYNCHRONOUS_COMMIT_LOCAL_FLUSH: i32 = 1;
pub const SYNCHRONOUS_COMMIT_REMOTE_WRITE: i32 = 2;
pub const SYNCHRONOUS_COMMIT_REMOTE_FLUSH: i32 = 3;
pub const SYNCHRONOUS_COMMIT_REMOTE_APPLY: i32 = 4;
pub const SYNCHRONOUS_COMMIT_ON: i32 = SYNCHRONOUS_COMMIT_REMOTE_FLUSH;

// --- MyXactFlags bits (`access/xact.h`) ---
pub const XACT_FLAGS_ACCESSEDTEMPNAMESPACE: i32 = 1 << 0;
pub const XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK: i32 = 1 << 1;
pub const XACT_FLAGS_NEEDIMMEDIATECOMMIT: i32 = 1 << 2;
pub const XACT_FLAGS_PIPELINING: i32 = 1 << 3;

/// `TransactionIdIsValid(xid)` (`access/transam.h`).
#[inline]
pub const fn TransactionIdIsValid(xid: TransactionId) -> bool {
    xid != InvalidTransactionId
}

/// `TransactionIdIsNormal(xid)` (access/transam.h).
#[inline]
pub const fn TransactionIdIsNormal(xid: TransactionId) -> bool {
    xid >= FirstNormalTransactionId
}

/// One created/dropped pgstat item carried on commit/abort/prepare WAL
/// records, matching C's `xl_xact_stats_item` (`access/xact.h`:
/// `{ int kind; Oid dboid; uint32 objid_lo; uint32 objid_hi; }`). The split
/// `objid_lo`/`objid_hi` words (alignment-friendly WAL layout) are carried as
/// the single `u64` they encode.
///
/// **Not the WAL wire layout.** C deliberately keeps `objid` as two 4-byte
/// words so `xl_xact_stats_item` stays 4-byte-aligned in WAL records; this
/// struct's size/alignment differ from the on-disk record member. WAL
/// (de)serialization must re-apply the lo/hi split
/// (`objid_lo = objid as u32`, `objid_hi = (objid >> 32) as u32`;
/// recombine with `((objid_hi as u64) << 32) | objid_lo as u64`) — never
/// treat this struct's bytes as the record image.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct XlXactStatsItem {
    pub kind: i32,
    pub dboid: crate::primitive::Oid,
    pub objid: u64,
}

/// `XactEvent` (`access/xact.h`) — events delivered to xact callbacks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum XactEvent {
    XACT_EVENT_COMMIT,
    XACT_EVENT_PARALLEL_COMMIT,
    XACT_EVENT_ABORT,
    XACT_EVENT_PARALLEL_ABORT,
    XACT_EVENT_PREPARE,
    XACT_EVENT_PRE_COMMIT,
    XACT_EVENT_PARALLEL_PRE_COMMIT,
    XACT_EVENT_PRE_PREPARE,
}
pub use XactEvent::*;

/// `SubXactEvent` (`access/xact.h`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum SubXactEvent {
    SUBXACT_EVENT_START_SUB,
    SUBXACT_EVENT_COMMIT_SUB,
    SUBXACT_EVENT_ABORT_SUB,
    SUBXACT_EVENT_PRE_COMMIT_SUB,
}
pub use SubXactEvent::*;

/// `FullTransactionId` (`access/transam.h`) — a 64-bit transaction id
/// (epoch in the high 32 bits, xid in the low 32).
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FullTransactionId {
    pub value: u64,
}

impl FullTransactionId {
    pub const fn from_u64(value: u64) -> Self {
        Self { value }
    }

    /// `FullTransactionIdFromEpochAndXid` (access/transam.h).
    pub const fn from_epoch_and_xid(epoch: u32, xid: TransactionId) -> Self {
        Self {
            value: ((epoch as u64) << 32) | xid as u64,
        }
    }

    /// `EpochFromFullTransactionId(x)` — `(uint32) ((x).value >> 32)`.
    pub const fn epoch(self) -> u32 {
        (self.value >> 32) as u32
    }

    /// `XidFromFullTransactionId(x)` — `(uint32) (x).value`.
    pub const fn xid(self) -> TransactionId {
        self.value as TransactionId
    }

    /// `FullTransactionIdIsValid`
    pub const fn is_valid(self) -> bool {
        self.xid() != InvalidTransactionId
    }
}

/// `InvalidFullTransactionId` (`access/transam.h`).
pub const InvalidFullTransactionId: FullTransactionId = FullTransactionId { value: 0 };

/// `FirstNormalFullTransactionId` (`access/transam.h`) — the smallest normal
/// `FullTransactionId` (epoch 0, xid = `FirstNormalTransactionId`).
pub const FirstNormalFullTransactionId: FullTransactionId =
    FullTransactionId::from_epoch_and_xid(0, FirstNormalTransactionId);

/// `TransamVariablesData` (`access/transam.h`) — the cluster-wide variable
/// cache for transaction/OID assignment, resident in shared memory in C
/// (carved by `VarsupShmemInit` via `ShmemInitStruct("TransamVariables", ...)`).
/// Field order, types, and lock-group comments mirror the C struct exactly.
/// Each field group is protected by the noted LWLock; the lock-protected
/// access discipline lives in the varsup crate.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TransamVariablesData {
    // These fields are protected by OidGenLock.
    /// next OID to assign
    pub nextOid: crate::primitive::Oid,
    /// OIDs available before must do XLOG work
    pub oidCount: u32,

    // These fields are protected by XidGenLock.
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
    pub oldestXidDB: crate::primitive::Oid,

    // These fields are protected by CommitTsLock.
    pub oldestCommitTsXid: TransactionId,
    pub newestCommitTsXid: TransactionId,

    // These fields are protected by ProcArrayLock.
    /// newest full XID that has committed or aborted
    pub latestCompletedXid: FullTransactionId,

    /// Number of top-level transactions with xids that completed in some form
    /// since the start of the server. Always above 1.
    pub xactCompletionCount: u64,

    // This field is protected by XactTruncationLock.
    /// oldest it's safe to look up in clog
    pub oldestClogXid: TransactionId,
}

/// `VirtualTransactionId` (`storage/lock.h`) — `{ ProcNumber procNumber;
/// LocalTransactionId localTransactionId; }`.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct VirtualTransactionId {
    pub procNumber: crate::primitive::ProcNumber,
    pub localTransactionId: LocalTransactionId,
}

impl VirtualTransactionId {
    /// `SetInvalidVirtualTransactionId(vxid)`.
    pub const fn invalid() -> Self {
        Self {
            procNumber: crate::primitive::INVALID_PROC_NUMBER,
            localTransactionId: 0,
        }
    }

    /// `VirtualTransactionIdIsValid(vxid)` —
    /// `LocalTransactionIdIsValid((vxid).localTransactionId)`.
    pub const fn is_valid(self) -> bool {
        self.localTransactionId != 0
    }
}

/// `SavedTransactionCharacteristics` (`access/xact.h`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SavedTransactionCharacteristics {
    pub save_XactIsoLevel: i32,
    pub save_XactReadOnly: bool,
    pub save_XactDeferrable: bool,
}

/// `TimeoutId` (`utils/timeout.h`) — identifiers for timeout reasons.
/// Multiple simultaneous timeouts are serviced in this enum's order.
/// `USER_TIMEOUT` is the first user-definable reason
/// (`MAX_TIMEOUTS = USER_TIMEOUT + 10`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum TimeoutId {
    STARTUP_PACKET_TIMEOUT,
    DEADLOCK_TIMEOUT,
    LOCK_TIMEOUT,
    STATEMENT_TIMEOUT,
    STANDBY_DEADLOCK_TIMEOUT,
    STANDBY_TIMEOUT,
    STANDBY_LOCK_TIMEOUT,
    IDLE_IN_TRANSACTION_SESSION_TIMEOUT,
    TRANSACTION_TIMEOUT,
    IDLE_SESSION_TIMEOUT,
    IDLE_STATS_UPDATE_TIMEOUT,
    CLIENT_CONNECTION_CHECK_TIMEOUT,
    STARTUP_PROGRESS_TIMEOUT,
    USER_TIMEOUT,
}
pub use TimeoutId::*;

/// `MAX_TIMEOUTS` (`utils/timeout.h`).
pub const MAX_TIMEOUTS: i32 = USER_TIMEOUT as i32 + 10;
