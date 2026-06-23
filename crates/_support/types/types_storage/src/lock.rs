//! Heavyweight-lock vocabulary (`storage/lock.h`, `storage/lockdefs.h`),
//! trimmed to the items ports consume so far.

use ::types_core::int64;
use ::types_core::uint16;
use ::types_core::uint32;
use ::types_core::uint8;
use ::types_core::Oid;

use crate::ilist::{dclist_head, dlist_head, dlist_node};

/// `LOCKMODE` (`storage/lockdefs.h`) — was C `int`.
pub type LOCKMODE = i32;

/// `LOCKMASK` (`storage/lock.h`) — a bitmask of lock modes (`typedef int`).
pub type LOCKMASK = i32;

/// `LOCKBIT_ON(lockmode)` (`storage/lock.h`).
pub const fn LOCKBIT_ON(lockmode: LOCKMODE) -> LOCKMASK {
    1 << lockmode
}

/// `LOCKBIT_OFF(lockmode)` (`storage/lock.h`).
pub const fn LOCKBIT_OFF(lockmode: LOCKMODE) -> LOCKMASK {
    !(1 << lockmode)
}

/// `LOCKMETHODID` (`storage/lock.h`) — index of a lock method (`typedef uint16`).
pub type LOCKMETHODID = uint16;

/// `MAX_LOCKMODES` (`storage/lock.h`) — max number of lock modes; cannot be
/// larger than the number of bits in `LOCKMASK`.
pub const MAX_LOCKMODES: usize = 10;

pub const NoLock: LOCKMODE = 0;
pub const AccessShareLock: LOCKMODE = 1;
pub const RowShareLock: LOCKMODE = 2;
pub const RowExclusiveLock: LOCKMODE = 3;
pub const ShareUpdateExclusiveLock: LOCKMODE = 4;
pub const ShareLock: LOCKMODE = 5;
pub const ShareRowExclusiveLock: LOCKMODE = 6;
pub const ExclusiveLock: LOCKMODE = 7;
pub const AccessExclusiveLock: LOCKMODE = 8;

/// `InplaceUpdateTupleLock` (`storage/lockdefs.h`) — the lock taken on a
/// tuple before writing an inplace-updated catalog row.
pub const InplaceUpdateTupleLock: LOCKMODE = ExclusiveLock;

/// `DEFAULT_LOCKMETHOD` (`storage/lock.h`).
pub const DEFAULT_LOCKMETHOD: uint8 = 1;
/// `USER_LOCKMETHOD` (`storage/lock.h`) — advisory user locks.
pub const USER_LOCKMETHOD: uint8 = 2;

/// `enum LockTagType` (`storage/lock.h`), as the `locktag_type` byte.
pub const LOCKTAG_RELATION: uint8 = 0;
pub const LOCKTAG_RELATION_EXTEND: uint8 = 1;
pub const LOCKTAG_DATABASE_FROZEN_IDS: uint8 = 2;
pub const LOCKTAG_PAGE: uint8 = 3;
pub const LOCKTAG_TUPLE: uint8 = 4;
pub const LOCKTAG_TRANSACTION: uint8 = 5;
pub const LOCKTAG_VIRTUALTRANSACTION: uint8 = 6;
pub const LOCKTAG_SPECULATIVE_TOKEN: uint8 = 7;
pub const LOCKTAG_OBJECT: uint8 = 8;
pub const LOCKTAG_USERLOCK: uint8 = 9;
pub const LOCKTAG_ADVISORY: uint8 = 10;
/// transaction being applied on a logical-replication subscriber
pub const LOCKTAG_APPLY_TRANSACTION: uint8 = 11;
/// `LOCKTAG_LAST_TYPE` (`storage/lock.h`) — the highest `LockTagType` value.
pub const LOCKTAG_LAST_TYPE: uint8 = LOCKTAG_APPLY_TRANSACTION;

/// `LockRelId` (`utils/rel.h`) — the (relation, database) pair a relcache entry
/// carries in `rd_lockInfo.lockRelId`, identifying a relation to the lock
/// manager. `dbId` is `InvalidOid` (0) for a shared/global relation.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct LockRelId {
    /// `Oid relId` — a relation identifier.
    pub relId: Oid,
    /// `Oid dbId` — a database identifier (`InvalidOid` for shared relations).
    pub dbId: Oid,
}

/// `enum XLTW_Oper` (`storage/lmgr.h`) — the operation that needs to wait on
/// another transaction, used by `XactLockTableWait`'s error-context callback.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum XLTW_Oper {
    None = 0,
    Update = 1,
    Delete = 2,
    Lock = 3,
    LockUpdated = 4,
    InsertIndex = 5,
    InsertIndexUnique = 6,
    FetchUpdated = 7,
    RecheckExclusionConstr = 8,
}

/// `LOCKTAG` (`storage/lock.h`) — the key identifying a lockable object.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct LOCKTAG {
    pub locktag_field1: uint32,
    pub locktag_field2: uint32,
    pub locktag_field3: uint32,
    pub locktag_field4: uint16,
    /// see the `LOCKTAG_*` LockTagType constants
    pub locktag_type: uint8,
    pub locktag_lockmethodid: uint8,
}

impl LOCKTAG {
    /// `SET_LOCKTAG_ADVISORY(locktag, id1, id2, id3, id4)` (`storage/lock.h`):
    /// build the advisory-lock tag. `id1` is `MyDatabaseId`; for an int8 key,
    /// `id2`/`id3` are the high/low halves and `id4 == 1`; for two int4 keys,
    /// `id2`/`id3` are the keys and `id4 == 2`.
    pub fn advisory(id1: uint32, id2: uint32, id3: uint32, id4: uint16) -> Self {
        LOCKTAG {
            locktag_field1: id1,
            locktag_field2: id2,
            locktag_field3: id3,
            locktag_field4: id4,
            locktag_type: LOCKTAG_ADVISORY,
            locktag_lockmethodid: USER_LOCKMETHOD,
        }
    }

    /// `SET_LOCKTAG_TRANSACTION(locktag, xid)` (`storage/lock.h`): the tag for
    /// a transaction-completion lock. `field1` is the xid; the rest are zero.
    pub fn transaction(xid: uint32) -> Self {
        LOCKTAG {
            locktag_field1: xid,
            locktag_field2: 0,
            locktag_field3: 0,
            locktag_field4: 0,
            locktag_type: LOCKTAG_TRANSACTION,
            locktag_lockmethodid: DEFAULT_LOCKMETHOD,
        }
    }

    /// `SET_LOCKTAG_VIRTUALTRANSACTION(locktag, vxid)` (`storage/lock.h`): the
    /// tag for a virtual-transaction lock. `field1` is the vxid's procNumber and
    /// `field2` its localTransactionId.
    pub fn virtualtransaction(proc_number: uint32, local_transaction_id: uint32) -> Self {
        LOCKTAG {
            locktag_field1: proc_number,
            locktag_field2: local_transaction_id,
            locktag_field3: 0,
            locktag_field4: 0,
            locktag_type: LOCKTAG_VIRTUALTRANSACTION,
            locktag_lockmethodid: DEFAULT_LOCKMETHOD,
        }
    }
}

/// `LockInstanceData` (`storage/lock.h`) — one PROCLOCK's worth of state, as
/// passed from lmgr internals to the lock-listing user functions (lockfuncs.c).
#[derive(Clone, Copy, Debug)]
pub struct LockInstanceData {
    /// `LOCKTAG locktag` — tag for the locked object.
    pub locktag: LOCKTAG,
    /// `LOCKMASK holdMask` — locks held by this PGPROC.
    pub holdMask: LOCKMASK,
    /// `LOCKMODE waitLockMode` — lock awaited by this PGPROC, if any.
    pub waitLockMode: LOCKMODE,
    /// `VirtualTransactionId vxid` — virtual transaction ID of this PGPROC.
    pub vxid: crate::storage::VirtualTransactionId,
    /// `TimestampTz waitStart` — when this PGPROC started waiting for the lock.
    pub waitStart: ::types_core::TimestampTz,
    /// `int pid` — pid of this PGPROC.
    pub pid: i32,
    /// `int leaderPid` — pid of the group leader; `= pid` if no group.
    pub leaderPid: i32,
    /// `bool fastpath` — taken via fastpath?
    pub fastpath: bool,
}

/// One already-decoded SIREAD predicate-lock row for `pg_lock_status`'s
/// predicate leg (lockfuncs.c). The target-tag decode + the
/// `SERIALIZABLEXACT` holder fields are predicate.c-internal; this carries the
/// scalar projection (the columns lockfuncs.c emits) across the predicate seam
/// so the column-layout logic stays in lockfuncs.c's owner.
#[derive(Clone, Debug)]
pub struct PredLockStatusRow {
    /// `PredicateLockTagTypeNames[lockType]` — the `locktype` text.
    pub locktypename: alloc::string::String,
    /// `GET_PREDICATELOCKTARGETTAG_DB` — the `database` OID.
    pub database: u32,
    /// `GET_PREDICATELOCKTARGETTAG_RELATION` — the `relation` OID.
    pub relation: u32,
    /// True for TUPLE or PAGE target types (the `page` column is non-NULL).
    pub has_page: bool,
    /// `GET_PREDICATELOCKTARGETTAG_PAGE` — the `page` block number.
    pub page: u32,
    /// True for the TUPLE target type (the `tuple` column is non-NULL).
    pub has_tuple: bool,
    /// `GET_PREDICATELOCKTARGETTAG_OFFSET` — the `tuple` offset.
    pub tuple: u16,
    /// `xact->vxid.procNumber` — the holder's proc number.
    pub proc_number: i32,
    /// `xact->vxid.localTransactionId` — the holder's local xid.
    pub local_xid: u32,
    /// `xact->pid` — the holder's pid (0 ⇒ NULL `pid` column).
    pub pid: i32,
}

/// The outcome of lock.c's `VirtualXactLock` examination of a target backend's
/// `MyProc->fpInfoLock`-guarded fast-path VXID state (the cross-proc critical
/// section owned by proc.c). lock.c uses this to decide its next step.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VirtualXactExamineOutcome {
    /// The target's `vxid.procNumber` / `fpLocalTransactionId` no longer match
    /// the awaited vxid: the VXID has ended. The caller falls through to
    /// `XactLockForVirtualXact(vxid, InvalidTransactionId, wait)`.
    Ended,
    /// `wait == false` and the VXID is still running: return `false` directly,
    /// no lock-table entry set up.
    StillRunningNoWait,
    /// The VXID is still running and `wait == true`: any fast-path VXID lock has
    /// been transferred to the main lock table; `xid` is the target proc's `xid`
    /// (possibly `InvalidTransactionId`). The caller sleeps on the VXID lock.
    Proceed { xid: ::types_core::TransactionId },
}

/// `enum LockAcquireResult` (`storage/lock.h`).
pub type LockAcquireResult = i32;
/// `LOCKACQUIRE_NOT_AVAIL` — lock not available, and `dontWait == true`.
pub const LOCKACQUIRE_NOT_AVAIL: LockAcquireResult = 0;
/// `LOCKACQUIRE_OK` — lock successfully acquired.
pub const LOCKACQUIRE_OK: LockAcquireResult = 1;
/// `LOCKACQUIRE_ALREADY_HELD` — incremented count for a lock already held.
pub const LOCKACQUIRE_ALREADY_HELD: LockAcquireResult = 2;
/// `LOCKACQUIRE_ALREADY_CLEAR` — incremented count for a lock already clear.
pub const LOCKACQUIRE_ALREADY_CLEAR: LockAcquireResult = 3;

/// `DeadLockState` (`storage/lock.h`) — the deadlock states identified by
/// `DeadLockCheck()`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum DeadLockState {
    /// `DS_NOT_YET_CHECKED` — no deadlock check has run yet.
    NotYetChecked = 0,
    /// `DS_NO_DEADLOCK` — no deadlock detected.
    NoDeadLock = 1,
    /// `DS_SOFT_DEADLOCK` — deadlock avoided by queue rearrangement.
    SoftDeadLock = 2,
    /// `DS_HARD_DEADLOCK` — deadlock, no way out but ERROR.
    HardDeadLock = 3,
    /// `DS_BLOCKED_BY_AUTOVACUUM` — no deadlock; queue blocked by autovacuum
    /// worker.
    BlockedByAutoVacuum = 4,
}

/// `LockMethodData` (`storage/lock.h`) — the per-lock-method descriptor: how
/// many modes it has, the mode-vs-mode conflict table, the mode names, and an
/// optional trace flag. In C the conflict table / names / trace flag are
/// `const` pointers into static tables owned by `lock.c`; here they are owned
/// vectors built by `lock.c` when it lands.
#[derive(Clone, Debug)]
pub struct LockMethodData {
    /// `int numLockModes`.
    pub numLockModes: i32,
    /// `const LOCKMASK *conflictTab` — `numLockModes + 1` entries.
    pub conflictTab: alloc::vec::Vec<LOCKMASK>,
    /// `const char *const *lockModeNames` — `numLockModes + 1` entries.
    pub lockModeNames: alloc::vec::Vec<alloc::string::String>,
    /// `const bool *trace_flag`.
    pub trace_flag: bool,
}

/// `LockMethod` (`storage/lock.h`, `typedef const LockMethodData *LockMethod`)
/// — a pointer to a (static, `lock.c`-owned) `LockMethodData`. Modeled as an
/// owned boxed descriptor.
pub type LockMethod = alloc::boxed::Box<LockMethodData>;

/// `LOCK` (`storage/lock.h`) — the shared hash-table entry for one lockable
/// object: its tag, the granted/awaited masks, the lists of associated
/// `PROCLOCK`s and waiting `PGPROC`s, and the per-mode request/grant counts.
/// Shmem-resident, owned by `lock.c`.
#[derive(Debug)]
pub struct LOCK {
    /// `LOCKTAG tag` — hash key, unique identifier of the lockable object.
    pub tag: LOCKTAG,
    /// `LOCKMASK grantMask` — bitmask for lock types already granted.
    pub grantMask: LOCKMASK,
    /// `LOCKMASK waitMask` — bitmask for lock types awaited.
    pub waitMask: LOCKMASK,
    /// `dlist_head procLocks` — list of PROCLOCK objects assoc. with lock.
    pub procLocks: dlist_head,
    /// `dclist_head waitProcs` — list of PGPROC objects waiting on lock.
    pub waitProcs: dclist_head,
    /// `int requested[MAX_LOCKMODES]` — counts of requested locks.
    pub requested: [i32; MAX_LOCKMODES],
    /// `int nRequested` — total of `requested[]`.
    pub nRequested: i32,
    /// `int granted[MAX_LOCKMODES]` — counts of granted locks.
    pub granted: [i32; MAX_LOCKMODES],
    /// `int nGranted` — total of `granted[]`.
    pub nGranted: i32,
}

impl Default for LOCK {
    fn default() -> Self {
        LOCK {
            tag: LOCKTAG::default(),
            grantMask: 0,
            waitMask: 0,
            procLocks: dlist_head::default(),
            waitProcs: dclist_head::default(),
            requested: [0; MAX_LOCKMODES],
            nRequested: 0,
            granted: [0; MAX_LOCKMODES],
            nGranted: 0,
        }
    }
}

/// `PROCLOCKTAG` (`storage/lock.h`) — hash key of a `PROCLOCK`: the lock and
/// the owning backend. The C struct holds raw `LOCK *` / `PGPROC *`; here the
/// linked structures are reached by owning box.
#[derive(Debug)]
pub struct PROCLOCKTAG {
    /// `LOCK *myLock` — link to per-lockable-object information.
    pub myLock: Option<alloc::boxed::Box<LOCK>>,
    /// `PGPROC *myProc` — link to PGPROC of owning backend.
    pub myProc: Option<alloc::boxed::Box<crate::storage::PGPROC>>,
}

/// `PROCLOCK` (`storage/lock.h`) — the shared hash-table entry recording one
/// backend's relationship to one `LOCK`. Shmem-resident, owned by `lock.c`.
#[derive(Debug)]
pub struct PROCLOCK {
    /// `PROCLOCKTAG tag` — unique identifier of proclock object.
    pub tag: PROCLOCKTAG,
    /// `PGPROC *groupLeader` — proc's lock group leader, or proc itself.
    pub groupLeader: Option<alloc::boxed::Box<crate::storage::PGPROC>>,
    /// `LOCKMASK holdMask` — bitmask for lock types currently held.
    pub holdMask: LOCKMASK,
    /// `LOCKMASK releaseMask` — bitmask for lock types to be released.
    pub releaseMask: LOCKMASK,
    /// `dlist_node lockLink` — list link in LOCK's list of proclocks.
    pub lockLink: dlist_node,
    /// `dlist_node procLink` — list link in PGPROC's list of proclocks.
    pub procLink: dlist_node,
}

/// `LOCALLOCKTAG` (`storage/lock.h`) — key of a backend-local lock-table entry.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct LOCALLOCKTAG {
    /// `LOCKTAG lock` — identifies the lockable object.
    pub lock: LOCKTAG,
    /// `LOCKMODE mode` — lock mode for this table entry.
    pub mode: LOCKMODE,
}

/// Identity of a `ResourceOwnerData *` owned by the resowner unit
/// (`utils/resowner/resowner.c`). This is the one canonical
/// [`types_resowner::ResourceOwner`] handle, re-exported here so lock.c's
/// LOCALLOCKOWNER keeps naming it `ResourceOwnerHandle`; `lock.c` only stores
/// and compares it, threading it to/from the resowner subsystem.
pub type ResourceOwnerHandle = types_resowner::ResourceOwner;

/// `LOCALLOCKOWNER` (`storage/lock.h`) — one resource owner that holds a
/// backend-local lock, with the count of times it holds it. `owner == None`
/// means the lock is held on behalf of the session.
#[derive(Clone, Debug)]
pub struct LOCALLOCKOWNER {
    /// `struct ResourceOwnerData *owner` — owning resource owner, or `None`
    /// for a session-level hold. The `ResourceOwnerData` body is owned by the
    /// resowner unit; reached here by handle.
    pub owner: Option<ResourceOwnerHandle>,
    /// `int64 nLocks` — # of times held by this owner.
    pub nLocks: int64,
}

/// `LOCALLOCK` (`storage/lock.h`) — a backend-local lock-table entry caching a
/// held heavyweight lock. Backend-private, owned by `lock.c`.
#[derive(Debug)]
pub struct LOCALLOCK {
    /// `LOCALLOCKTAG tag` — unique identifier of locallock entry.
    pub tag: LOCALLOCKTAG,
    /// `uint32 hashcode` — copy of LOCKTAG's hash value.
    pub hashcode: uint32,
    /// `LOCK *lock` — associated LOCK object, if any.
    pub lock: Option<alloc::boxed::Box<LOCK>>,
    /// `PROCLOCK *proclock` — associated PROCLOCK object, if any.
    pub proclock: Option<alloc::boxed::Box<PROCLOCK>>,
    /// `int64 nLocks` — total number of times lock is held.
    pub nLocks: int64,
    /// `int numLockOwners` — # of relevant ResourceOwners.
    pub numLockOwners: i32,
    /// `int maxLockOwners` — allocated size of array.
    pub maxLockOwners: i32,
    /// `LOCALLOCKOWNER *lockOwners` — dynamically resizable array.
    pub lockOwners: alloc::vec::Vec<LOCALLOCKOWNER>,
    /// `bool holdsStrongLockCount` — bumped FastPathStrongRelationLocks.
    pub holdsStrongLockCount: bool,
    /// `bool lockCleared` — we read all sinval msgs for lock.
    pub lockCleared: bool,
}
