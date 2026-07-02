use crate::primitive::{LocalTransactionId, RepOriginId, TransactionId, XLogRecPtr};

pub type CommandId = u32;

pub type SubTransactionId = u32;

pub type XidStatus = i32;

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum TBlockState {
    TBLOCK_DEFAULT, /* idle */
    TBLOCK_STARTED, /* running single-query transaction */

    TBLOCK_BEGIN,               /* starting transaction block */
    TBLOCK_INPROGRESS,          /* live transaction */
    TBLOCK_IMPLICIT_INPROGRESS, /* live transaction after implicit BEGIN */
    TBLOCK_PARALLEL_INPROGRESS, /* live transaction inside parallel worker */
    TBLOCK_END,                 /* COMMIT received */
    TBLOCK_ABORT,               /* failed xact, awaiting ROLLBACK */
    TBLOCK_ABORT_END,           /* failed xact, ROLLBACK received */
    TBLOCK_ABORT_PENDING,       /* live xact, ROLLBACK received */
    TBLOCK_PREPARE,             /* live xact, PREPARE received */

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

pub const InvalidTransactionId: TransactionId = 0;
pub const BootstrapTransactionId: TransactionId = 1;
pub const FrozenTransactionId: TransactionId = 2;
pub const FirstNormalTransactionId: TransactionId = 3;
pub const MaxTransactionId: TransactionId = 0xFFFF_FFFF;

pub const InvalidLocalTransactionId: LocalTransactionId = 0;
pub const InvalidSubTransactionId: SubTransactionId = 0;
pub const TopSubTransactionId: SubTransactionId = 1;
pub const FirstCommandId: CommandId = 0;
pub const InvalidCommandId: CommandId = !0;

pub const InvalidXLogRecPtr: XLogRecPtr = 0;

pub const GIDSIZE: usize = 200;

pub const TRANSACTION_STATUS_IN_PROGRESS: XidStatus = 0x00;
pub const TRANSACTION_STATUS_COMMITTED: XidStatus = 0x01;
pub const TRANSACTION_STATUS_ABORTED: XidStatus = 0x02;
pub const TRANSACTION_STATUS_SUB_COMMITTED: XidStatus = 0x03;

pub const DoNotReplicateId: RepOriginId = u16::MAX;

pub const XACT_READ_UNCOMMITTED: i32 = 0;
pub const XACT_READ_COMMITTED: i32 = 1;
pub const XACT_REPEATABLE_READ: i32 = 2;
pub const XACT_SERIALIZABLE: i32 = 3;

pub const SYNCHRONOUS_COMMIT_OFF: i32 = 0;
pub const SYNCHRONOUS_COMMIT_LOCAL_FLUSH: i32 = 1;
pub const SYNCHRONOUS_COMMIT_REMOTE_WRITE: i32 = 2;
pub const SYNCHRONOUS_COMMIT_REMOTE_FLUSH: i32 = 3;
pub const SYNCHRONOUS_COMMIT_REMOTE_APPLY: i32 = 4;
pub const SYNCHRONOUS_COMMIT_ON: i32 = SYNCHRONOUS_COMMIT_REMOTE_FLUSH;

pub const XACT_FLAGS_ACCESSEDTEMPNAMESPACE: i32 = 1 << 0;
pub const XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK: i32 = 1 << 1;
pub const XACT_FLAGS_NEEDIMMEDIATECOMMIT: i32 = 1 << 2;
pub const XACT_FLAGS_PIPELINING: i32 = 1 << 3;

#[inline]
pub const fn TransactionIdIsValid(xid: TransactionId) -> bool {
    xid != InvalidTransactionId
}

#[inline]
pub const fn TransactionIdIsNormal(xid: TransactionId) -> bool {
    xid >= FirstNormalTransactionId
}

#[inline]
pub const fn TransactionIdEquals(id1: TransactionId, id2: TransactionId) -> bool {
    id1 == id2
}

// Bootstrap/Frozen xids sort before all normal xids; modular compare after.
#[inline]
pub fn TransactionIdPrecedes(id1: TransactionId, id2: TransactionId) -> bool {
    if !TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2) {
        return id1 < id2;
    }
    (id1.wrapping_sub(id2) as i32) < 0
}

#[inline]
pub fn TransactionIdPrecedesOrEquals(id1: TransactionId, id2: TransactionId) -> bool {
    if !TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2) {
        return id1 <= id2;
    }
    (id1.wrapping_sub(id2) as i32) <= 0
}

#[inline]
pub fn TransactionIdFollows(id1: TransactionId, id2: TransactionId) -> bool {
    if !TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2) {
        return id1 > id2;
    }
    (id1.wrapping_sub(id2) as i32) > 0
}

#[inline]
pub fn TransactionIdFollowsOrEquals(id1: TransactionId, id2: TransactionId) -> bool {
    if !TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2) {
        return id1 >= id2;
    }
    (id1.wrapping_sub(id2) as i32) >= 0
}

// Multixacts have no Bootstrap/Frozen values: always the plain modular compare.
#[inline]
pub fn MultiXactIdPrecedes(
    multi1: crate::primitive::MultiXactId,
    multi2: crate::primitive::MultiXactId,
) -> bool {
    (multi1.wrapping_sub(multi2) as i32) < 0
}

#[inline]
pub fn MultiXactIdPrecedesOrEquals(
    multi1: crate::primitive::MultiXactId,
    multi2: crate::primitive::MultiXactId,
) -> bool {
    (multi1.wrapping_sub(multi2) as i32) <= 0
}

// C `xl_xact_stats_item` with the objid_lo/objid_hi words recombined as u64.
// NOT the WAL wire layout: (de)serialization must re-apply the lo/hi split.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct XlXactStatsItem {
    pub kind: i32,
    pub dboid: crate::primitive::Oid,
    pub objid: u64,
}

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum SubXactEvent {
    SUBXACT_EVENT_START_SUB,
    SUBXACT_EVENT_COMMIT_SUB,
    SUBXACT_EVENT_ABORT_SUB,
    SUBXACT_EVENT_PRE_COMMIT_SUB,
}
pub use SubXactEvent::*;

// Epoch in the high 32 bits, xid in the low 32.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FullTransactionId {
    pub value: u64,
}

impl FullTransactionId {
    pub const fn from_u64(value: u64) -> Self {
        Self { value }
    }

    pub const fn from_epoch_and_xid(epoch: u32, xid: TransactionId) -> Self {
        Self {
            value: ((epoch as u64) << 32) | xid as u64,
        }
    }

    pub const fn epoch(self) -> u32 {
        (self.value >> 32) as u32
    }

    pub const fn xid(self) -> TransactionId {
        self.value as TransactionId
    }

    pub const fn is_valid(self) -> bool {
        self.xid() != InvalidTransactionId
    }

    pub const fn to_u64(self) -> u64 {
        self.value
    }

    pub const fn is_normal(self) -> bool {
        TransactionIdIsNormal(self.xid())
    }

    pub const fn precedes(self, other: FullTransactionId) -> bool {
        self.value < other.value
    }

    pub const fn precedes_or_equals(self, other: FullTransactionId) -> bool {
        self.value <= other.value
    }
}

pub const InvalidFullTransactionId: FullTransactionId = FullTransactionId { value: 0 };

pub const FirstNormalFullTransactionId: FullTransactionId =
    FullTransactionId::from_epoch_and_xid(0, FirstNormalTransactionId);

// Each field group is protected by the noted LWLock (discipline in varsup).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TransamVariablesData {
    // Protected by OidGenLock:
    pub nextOid: crate::primitive::Oid,
    pub oidCount: u32,

    // Protected by XidGenLock:
    pub nextXid: FullTransactionId,
    pub oldestXid: TransactionId,
    pub xidVacLimit: TransactionId,
    pub xidWarnLimit: TransactionId,
    pub xidStopLimit: TransactionId,
    pub xidWrapLimit: TransactionId,
    pub oldestXidDB: crate::primitive::Oid,

    // Protected by CommitTsLock:
    pub oldestCommitTsXid: TransactionId,
    pub newestCommitTsXid: TransactionId,

    // Protected by ProcArrayLock:
    pub latestCompletedXid: FullTransactionId,
    pub xactCompletionCount: u64,

    // Protected by XactTruncationLock:
    pub oldestClogXid: TransactionId,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct VirtualTransactionId {
    pub procNumber: crate::primitive::ProcNumber,
    pub localTransactionId: LocalTransactionId,
}

impl VirtualTransactionId {
    pub const fn invalid() -> Self {
        Self {
            procNumber: crate::primitive::INVALID_PROC_NUMBER,
            localTransactionId: 0,
        }
    }

    pub const fn is_valid(self) -> bool {
        self.localTransactionId != 0
    }

    // A recovered prepared xact carries no real proc; the LXID is a real XID.
    pub const fn is_recovered_prepared_xact(self) -> bool {
        self.procNumber == crate::primitive::INVALID_PROC_NUMBER
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SavedTransactionCharacteristics {
    pub save_XactIsoLevel: i32,
    pub save_XactReadOnly: bool,
    pub save_XactDeferrable: bool,
}

// Simultaneous timeouts are serviced in this enum's order.
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

pub const MAX_TIMEOUTS: i32 = USER_TIMEOUT as i32 + 10;
