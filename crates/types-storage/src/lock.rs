//! Heavyweight-lock vocabulary (`storage/lock.h`, `storage/lockdefs.h`),
//! trimmed to the items ports consume so far.

use types_core::uint16;
use types_core::uint32;
use types_core::uint8;
use types_core::Oid;

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
