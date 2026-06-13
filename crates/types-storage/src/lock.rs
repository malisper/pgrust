//! Heavyweight-lock vocabulary (`storage/lock.h`, `storage/lockdefs.h`),
//! trimmed to the items ports consume so far.

use types_core::int64;
use types_core::uint16;
use types_core::uint32;
use types_core::uint8;

use crate::ilist::{dclist_head, dlist_head, dlist_node};

/// `LOCKMODE` (`storage/lockdefs.h`) — was C `int`.
pub type LOCKMODE = i32;

/// `LOCKMASK` (`storage/lock.h`) — a bitmask of lock modes (`typedef int`).
pub type LOCKMASK = i32;

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

/// `enum LockTagType` (`storage/lock.h`), as the `locktag_type` byte.
pub const LOCKTAG_RELATION: uint8 = 0;
pub const LOCKTAG_RELATION_EXTEND: uint8 = 1;
pub const LOCKTAG_DATABASE_FROZEN_IDS: uint8 = 2;
pub const LOCKTAG_PAGE: uint8 = 3;
pub const LOCKTAG_TUPLE: uint8 = 4;
pub const LOCKTAG_TRANSACTION: uint8 = 5;
pub const LOCKTAG_VIRTUALTRANSACTION: uint8 = 6;
pub const LOCKTAG_SPECULATIVE_TOKEN: uint8 = 7;

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
/// (`utils/resowner/resowner.c`). The `ResourceOwnerData` body is backend-local
/// and owned by that (unported) unit; `lock.c` only stores the pointer, so it
/// is modeled by handle here (same pattern as [`crate::latch::LatchHandle`]).
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ResourceOwnerHandle(pub usize);

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
