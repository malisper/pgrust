//! Heavyweight-lock vocabulary (`storage/lock.h`, `storage/lockdefs.h`),
//! trimmed to the items ports consume so far.

use types_core::uint16;
use types_core::uint32;
use types_core::uint8;

/// `LOCKMODE` (`storage/lockdefs.h`) — was C `int`.
pub type LOCKMODE = i32;

/// `LOCKMASK` (`storage/lockdefs.h`) — a bitmask of lock modes.
pub type LOCKMASK = i32;

/// `LOCKBIT_ON(lockmode)` (`storage/lock.h`).
pub const fn LOCKBIT_ON(lockmode: LOCKMODE) -> LOCKMASK {
    1 << lockmode
}

/// `LOCKBIT_OFF(lockmode)` (`storage/lock.h`).
pub const fn LOCKBIT_OFF(lockmode: LOCKMODE) -> LOCKMASK {
    !(1 << lockmode)
}

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

/// `MAX_LOCKMODES` (`storage/lock.h`) — cannot exceed the # of bits in
/// LOCKMASK.
pub const MAX_LOCKMODES: usize = 10;

/// `DEFAULT_LOCKMETHOD` (`storage/lock.h`).
pub const DEFAULT_LOCKMETHOD: uint8 = 1;
/// `USER_LOCKMETHOD` (`storage/lock.h`) — the lock method used by advisory
/// (user) locks.
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
pub const LOCKTAG_APPLY_TRANSACTION: uint8 = 11;

/// `LOCKTAG_LAST_TYPE` (`storage/lock.h`).
pub const LOCKTAG_LAST_TYPE: uint8 = LOCKTAG_APPLY_TRANSACTION;

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
    pub waitStart: types_core::TimestampTz,
    /// `int pid` — pid of this PGPROC.
    pub pid: i32,
    /// `int leaderPid` — pid of the group leader; `= pid` if no group.
    pub leaderPid: i32,
    /// `bool fastpath` — taken via fastpath?
    pub fastpath: bool,
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
