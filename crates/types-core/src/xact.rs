//! Transaction-system scalar vocabulary (`c.h`, `access/transam.h`,
//! `access/xact.h`, `storage/lock.h`, `replication/origin.h`).
//!
//! Populated incrementally from ../pgrust/src-idiomatic/crates/types/src/xact.rs
//! as ports need items; only the items currently consumed are present.

use crate::primitive::{RepOriginId, TransactionId};

/// `CommandId` (`c.h`) — a `uint32`.
pub type CommandId = u32;

/// `LocalTransactionId` (`c.h`).
pub type LocalTransactionId = u32;
/// `SubTransactionId` (`c.h`).
pub type SubTransactionId = u32;
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

pub const InvalidTransactionId: TransactionId = 0;
pub const BootstrapTransactionId: TransactionId = 1;
pub const FrozenTransactionId: TransactionId = 2;
pub const FirstNormalTransactionId: TransactionId = 3;
/// `MaxTransactionId` (access/transam.h).
pub const MaxTransactionId: TransactionId = 0xFFFF_FFFF;

pub const InvalidLocalTransactionId: LocalTransactionId = 0;
pub const InvalidSubTransactionId: SubTransactionId = 0;
pub const TopSubTransactionId: SubTransactionId = 1;
pub const FirstCommandId: CommandId = 0;
pub const InvalidCommandId: CommandId = !0;

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

/// `FullTransactionId` (`access/transam.h`) — 64-bit epoch-qualified XID.
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

    /// `XidFromFullTransactionId`
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

/// `VirtualTransactionId` (`storage/lock.h`) — `{ ProcNumber procNumber;
/// LocalTransactionId localTransactionId; }`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VirtualTransactionId {
    pub proc_number: crate::primitive::ProcNumber,
    pub local_transaction_id: LocalTransactionId,
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
