//! Heavyweight-lock vocabulary (`storage/lock.h`, `storage/lockdefs.h`),
//! trimmed to the items ports consume so far.

use types_core::uint16;
use types_core::uint32;
use types_core::uint8;

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
