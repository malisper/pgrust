//! Trimmed copy of the src-idiomatic `types::storage` module: the LWLock
//! handle and its supporting pieces.

use types_core::{uint8, uint16, uint32, Oid, ProcNumber, RelFileNumber, TransactionId, INVALID_PROC_NUMBER};

extern crate alloc;

/// `LWLockMode` (`storage/lwlock.h`).
pub type LWLockMode = u32;
pub const LW_EXCLUSIVE: LWLockMode = 0;
pub const LW_SHARED: LWLockMode = 1;
pub const LW_WAIT_UNTIL_FREE: LWLockMode = 2;

/// `pg_atomic_uint32` (`port/atomics.h`) — a shmem-resident atomic word.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct pg_atomic_uint32 {
    pub value: uint32,
}

/// `proclist_head` (`storage/proclist_types.h`) — head/tail pgprocno indexes of
/// a doubly-linked PGPROC list; `INVALID_PROC_NUMBER` at the ends.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct proclist_head {
    pub head: ProcNumber,
    pub tail: ProcNumber,
}

impl Default for proclist_head {
    fn default() -> Self {
        Self {
            head: INVALID_PROC_NUMBER,
            tail: INVALID_PROC_NUMBER,
        }
    }
}

/// `LWLock` (`storage/lwlock.h`): tranche id, atomic lock state, and the list
/// of waiting PGPROCs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LWLock {
    pub tranche: uint16,
    pub state: pg_atomic_uint32,
    pub waiters: proclist_head,
}

/// `NUM_INDIVIDUAL_LWLOCKS` — generated from `lwlocklist.h`.
pub const NUM_INDIVIDUAL_LWLOCKS: i32 = 54;

// `BuiltinTrancheIds` (`storage/lwlock.h`) — the chain from
// `LWTRANCHE_XACT_BUFFER = NUM_INDIVIDUAL_LWLOCKS` down to the tranche the
// pgstat ports consume (`LWTRANCHE_PGSTATS_DATA`).
pub const LWTRANCHE_XACT_BUFFER: i32 = NUM_INDIVIDUAL_LWLOCKS;
pub const LWTRANCHE_COMMITTS_BUFFER: i32 = LWTRANCHE_XACT_BUFFER + 1;
pub const LWTRANCHE_SUBTRANS_BUFFER: i32 = LWTRANCHE_COMMITTS_BUFFER + 1;
pub const LWTRANCHE_MULTIXACTOFFSET_BUFFER: i32 = LWTRANCHE_SUBTRANS_BUFFER + 1;
pub const LWTRANCHE_MULTIXACTMEMBER_BUFFER: i32 = LWTRANCHE_MULTIXACTOFFSET_BUFFER + 1;
pub const LWTRANCHE_NOTIFY_BUFFER: i32 = LWTRANCHE_MULTIXACTMEMBER_BUFFER + 1;
pub const LWTRANCHE_SERIAL_BUFFER: i32 = LWTRANCHE_NOTIFY_BUFFER + 1;
pub const LWTRANCHE_WAL_INSERT: i32 = LWTRANCHE_SERIAL_BUFFER + 1;
pub const LWTRANCHE_BUFFER_CONTENT: i32 = LWTRANCHE_WAL_INSERT + 1;
pub const LWTRANCHE_REPLICATION_ORIGIN_STATE: i32 = LWTRANCHE_BUFFER_CONTENT + 1;
pub const LWTRANCHE_REPLICATION_SLOT_IO: i32 = LWTRANCHE_REPLICATION_ORIGIN_STATE + 1;
pub const LWTRANCHE_LOCK_FASTPATH: i32 = LWTRANCHE_REPLICATION_SLOT_IO + 1;
pub const LWTRANCHE_BUFFER_MAPPING: i32 = LWTRANCHE_LOCK_FASTPATH + 1;
pub const LWTRANCHE_LOCK_MANAGER: i32 = LWTRANCHE_BUFFER_MAPPING + 1;
pub const LWTRANCHE_PREDICATE_LOCK_MANAGER: i32 = LWTRANCHE_LOCK_MANAGER + 1;
pub const LWTRANCHE_PARALLEL_HASH_JOIN: i32 = LWTRANCHE_PREDICATE_LOCK_MANAGER + 1;
pub const LWTRANCHE_PARALLEL_BTREE_SCAN: i32 = LWTRANCHE_PARALLEL_HASH_JOIN + 1;
pub const LWTRANCHE_PARALLEL_QUERY_DSA: i32 = LWTRANCHE_PARALLEL_BTREE_SCAN + 1;
pub const LWTRANCHE_PER_SESSION_DSA: i32 = LWTRANCHE_PARALLEL_QUERY_DSA + 1;
pub const LWTRANCHE_PER_SESSION_RECORD_TYPE: i32 = LWTRANCHE_PER_SESSION_DSA + 1;
pub const LWTRANCHE_PER_SESSION_RECORD_TYPMOD: i32 = LWTRANCHE_PER_SESSION_RECORD_TYPE + 1;
pub const LWTRANCHE_SHARED_TUPLESTORE: i32 = LWTRANCHE_PER_SESSION_RECORD_TYPMOD + 1;
pub const LWTRANCHE_SHARED_TIDBITMAP: i32 = LWTRANCHE_SHARED_TUPLESTORE + 1;
pub const LWTRANCHE_PARALLEL_APPEND: i32 = LWTRANCHE_SHARED_TIDBITMAP + 1;
pub const LWTRANCHE_PER_XACT_PREDICATE_LIST: i32 = LWTRANCHE_PARALLEL_APPEND + 1;
pub const LWTRANCHE_PGSTATS_DSA: i32 = LWTRANCHE_PER_XACT_PREDICATE_LIST + 1;
pub const LWTRANCHE_PGSTATS_HASH: i32 = LWTRANCHE_PGSTATS_DSA + 1;
pub const LWTRANCHE_PGSTATS_DATA: i32 = LWTRANCHE_PGSTATS_HASH + 1;

// ---------------------------------------------------------------------------
// Lock-manager vocabulary (`storage/lockdefs.h`, `storage/lock.h`), trimmed.
// ---------------------------------------------------------------------------

/// `LOCKMODE` (`storage/lockdefs.h`) — was C `int`.
pub type LOCKMODE = i32;
pub const NoLock: LOCKMODE = 0;
pub const AccessShareLock: LOCKMODE = 1;
pub const RowShareLock: LOCKMODE = 2;
pub const RowExclusiveLock: LOCKMODE = 3;
pub const ShareUpdateExclusiveLock: LOCKMODE = 4;
pub const ShareLock: LOCKMODE = 5;
pub const ShareRowExclusiveLock: LOCKMODE = 6;
pub const ExclusiveLock: LOCKMODE = 7;
pub const AccessExclusiveLock: LOCKMODE = 8;

/// `LOCKMETHODID` (`storage/lock.h`).
pub type LOCKMETHODID = uint16;
pub const DEFAULT_LOCKMETHOD: LOCKMETHODID = 1;
pub const USER_LOCKMETHOD: LOCKMETHODID = 2;

/// `LockTagType` (`storage/lock.h`) — trimmed to the tags ports consume.
pub type LockTagType = uint8;
pub const LOCKTAG_RELATION: LockTagType = 0;

/// `LOCKTAG` (`storage/lock.h`): the 16-byte key identifying any lockable
/// object.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LOCKTAG {
    pub locktag_field1: uint32,
    pub locktag_field2: uint32,
    pub locktag_field3: uint32,
    pub locktag_field4: uint16,
    pub locktag_type: uint8,
    pub locktag_lockmethodid: uint8,
}

/// `LockAcquireResult` (`storage/lock.h`).
pub type LockAcquireResult = u32;
pub const LOCKACQUIRE_NOT_AVAIL: LockAcquireResult = 0;
pub const LOCKACQUIRE_OK: LockAcquireResult = 1;
pub const LOCKACQUIRE_ALREADY_HELD: LockAcquireResult = 2;
pub const LOCKACQUIRE_ALREADY_CLEAR: LockAcquireResult = 3;

/// `VirtualTransactionId` (`storage/lock.h`).
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct VirtualTransactionId {
    pub procNumber: ProcNumber,
    pub localTransactionId: types_core::LocalTransactionId,
}

impl VirtualTransactionId {
    /// `SetInvalidVirtualTransactionId(vxid)`.
    pub const fn invalid() -> Self {
        Self {
            procNumber: INVALID_PROC_NUMBER,
            localTransactionId: 0,
        }
    }

    /// `VirtualTransactionIdIsValid(vxid)` —
    /// `LocalTransactionIdIsValid((vxid).localTransactionId)`.
    pub const fn is_valid(self) -> bool {
        self.localTransactionId != 0
    }
}

// ---------------------------------------------------------------------------
// Individual (named) LWLock indexes from `storage/lwlocklist.h`, trimmed to
// the ones ports release by id. `&MainLWLockArray[n].lock` in C.
// ---------------------------------------------------------------------------

/// `XidGenLock` — `PG_LWLOCK(3, XidGen)`.
pub const LWLOCK_XID_GEN: i32 = 3;
/// `ProcArrayLock` — `PG_LWLOCK(4, ProcArray)`.
pub const LWLOCK_PROC_ARRAY: i32 = 4;

// ---------------------------------------------------------------------------
// `storage/procsignal.h`, trimmed.
// ---------------------------------------------------------------------------

/// `ProcSignalReason` (`storage/procsignal.h`).
#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcSignalReason {
    PROCSIG_CATCHUP_INTERRUPT = 0,
    PROCSIG_NOTIFY_INTERRUPT = 1,
    PROCSIG_PARALLEL_MESSAGE = 2,
    PROCSIG_WALSND_INIT_STOPPING = 3,
    PROCSIG_BARRIER = 4,
    PROCSIG_LOG_MEMORY_CONTEXT = 5,
    PROCSIG_PARALLEL_APPLY_MESSAGE = 6,
    PROCSIG_RECOVERY_CONFLICT_DATABASE = 7,
    PROCSIG_RECOVERY_CONFLICT_TABLESPACE = 8,
    PROCSIG_RECOVERY_CONFLICT_LOCK = 9,
    PROCSIG_RECOVERY_CONFLICT_SNAPSHOT = 10,
    PROCSIG_RECOVERY_CONFLICT_LOGICALSLOT = 11,
    PROCSIG_RECOVERY_CONFLICT_BUFFERPIN = 12,
    PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK = 13,
}

pub const PROCSIG_RECOVERY_CONFLICT_FIRST: ProcSignalReason =
    ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_DATABASE;
pub const PROCSIG_RECOVERY_CONFLICT_LAST: ProcSignalReason =
    ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK;
pub const NUM_PROCSIGNALS: usize =
    ProcSignalReason::PROCSIG_RECOVERY_CONFLICT_STARTUP_DEADLOCK as usize + 1;

// ---------------------------------------------------------------------------
// `storage/standby.h` / `storage/procarray.h` running-xacts vocabulary.
// ---------------------------------------------------------------------------

/// `subxids_array_status` (`storage/standby.h`).
pub type subxids_array_status = u32;
pub const SUBXIDS_IN_ARRAY: subxids_array_status = 0;
pub const SUBXIDS_MISSING: subxids_array_status = 1;
pub const SUBXIDS_IN_SUBTRANS: subxids_array_status = 2;

/// `RunningTransactionsData` (`storage/standby.h`). The C `xids` pointer
/// (length `xcnt + subxcnt`) is an owned `Vec` here.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunningTransactionsData {
    pub xcnt: i32,
    pub subxcnt: i32,
    pub subxid_status: subxids_array_status,
    pub nextXid: TransactionId,
    pub oldestRunningXid: TransactionId,
    pub oldestDatabaseRunningXid: TransactionId,
    pub latestCompletedXid: TransactionId,
    pub xids: alloc::vec::Vec<TransactionId>,
}

/// `xl_standby_lock` (`storage/standbydefs.h`): one logged
/// AccessExclusiveLock — 12 bytes, no padding.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct xl_standby_lock {
    /// xid of the holding transaction.
    pub xid: TransactionId,
    /// `InvalidOid` when locking a shared relation.
    pub dbOid: Oid,
    pub relOid: Oid,
}

// ---------------------------------------------------------------------------
// `storage/sinval.h`, trimmed: the message union as an opaque payload.
// ---------------------------------------------------------------------------

/// `sizeof(SharedInvalidationMessage)` — the C union is 16 bytes.
pub const SHARED_INVALIDATION_MESSAGE_SIZE: usize = 16;

/// `SharedInvalidationMessage` (`storage/sinval.h`), carried as the raw
/// 16-byte union payload. Units that interpret the variants (inval.c,
/// sinvaladt.c) own the decoded view.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct SharedInvalidationMessage {
    pub raw: [u8; SHARED_INVALIDATION_MESSAGE_SIZE],
}

// ---------------------------------------------------------------------------
// `storage/relfilelocator.h`.
// ---------------------------------------------------------------------------

/// `RelFileLocator` (`storage/relfilelocator.h`).
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RelFileLocator {
    pub spcOid: Oid,
    pub dbOid: Oid,
    pub relNumber: RelFileNumber,
}
