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
pub type TransState = u32;
/// `TBlockState` (xact.c) — transaction-block state.
pub type TBlockState = u32;

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

// --- TransState (xact.c) ---
pub const TRANS_DEFAULT: TransState = 0;
pub const TRANS_START: TransState = 1;
pub const TRANS_INPROGRESS: TransState = 2;
pub const TRANS_COMMIT: TransState = 3;
pub const TRANS_ABORT: TransState = 4;
pub const TRANS_PREPARE: TransState = 5;

// --- TBlockState (xact.c) ---
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

// --- XactEvent / SubXactEvent (`access/xact.h`) ---
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

// --- TimeoutId values consumed so far (`utils/timeout.h`) ---
/// `TRANSACTION_TIMEOUT` member of `TimeoutId` (9th enumerator).
pub const TRANSACTION_TIMEOUT: i32 = 8;
