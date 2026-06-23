//! The static lock-method tables of `storage/lmgr/lock.c`: the lock-mode
//! conflict matrix (`LockConflicts`), the mode display names
//! (`lock_mode_names`), the two `LockMethodData` descriptors, and the
//! `LockMethods[]` lookup. These are read-only `const` data in C; here they
//! are `const`/`static` functions producing the same values.

use types_storage::lock::{
    LOCKMASK, LOCKMETHODID, LOCKMODE, LOCKBIT_ON, MAX_LOCKMODES,
    AccessExclusiveLock, AccessShareLock, ExclusiveLock, RowExclusiveLock, RowShareLock,
    ShareLock, ShareRowExclusiveLock, ShareUpdateExclusiveLock,
    DEFAULT_LOCKMETHOD, USER_LOCKMETHOD,
};

/// `MaxLockMode` (`storage/lockdefs.h`) — the highest lock mode
/// (`AccessExclusiveLock`); the number of usable lock modes.
pub const MaxLockMode: LOCKMODE = AccessExclusiveLock;

/// `static const LOCKMASK LockConflicts[]` (lock.c) — the conflict table for
/// the standard lock methods. `LockConflicts[mode]` is the bitmask of modes
/// that conflict with `mode`. Index 0 (`NoLock`) is `0`.
///
/// This is `numLockModes + 1` entries (`MAX_LOCKMODES`-sized to match the C
/// `conflictTab[MAX_LOCKMODES]` access pattern).
pub fn lock_conflicts() -> [LOCKMASK; MAX_LOCKMODES] {
    let mut t = [0 as LOCKMASK; MAX_LOCKMODES];

    // index 0 (NoLock) = 0

    // AccessShareLock
    t[AccessShareLock as usize] = LOCKBIT_ON(AccessExclusiveLock);

    // RowShareLock
    t[RowShareLock as usize] =
        LOCKBIT_ON(ExclusiveLock) | LOCKBIT_ON(AccessExclusiveLock);

    // RowExclusiveLock
    t[RowExclusiveLock as usize] = LOCKBIT_ON(ShareLock)
        | LOCKBIT_ON(ShareRowExclusiveLock)
        | LOCKBIT_ON(ExclusiveLock)
        | LOCKBIT_ON(AccessExclusiveLock);

    // ShareUpdateExclusiveLock
    t[ShareUpdateExclusiveLock as usize] = LOCKBIT_ON(ShareUpdateExclusiveLock)
        | LOCKBIT_ON(ShareLock)
        | LOCKBIT_ON(ShareRowExclusiveLock)
        | LOCKBIT_ON(ExclusiveLock)
        | LOCKBIT_ON(AccessExclusiveLock);

    // ShareLock
    t[ShareLock as usize] = LOCKBIT_ON(RowExclusiveLock)
        | LOCKBIT_ON(ShareUpdateExclusiveLock)
        | LOCKBIT_ON(ShareRowExclusiveLock)
        | LOCKBIT_ON(ExclusiveLock)
        | LOCKBIT_ON(AccessExclusiveLock);

    // ShareRowExclusiveLock
    t[ShareRowExclusiveLock as usize] = LOCKBIT_ON(RowExclusiveLock)
        | LOCKBIT_ON(ShareUpdateExclusiveLock)
        | LOCKBIT_ON(ShareLock)
        | LOCKBIT_ON(ShareRowExclusiveLock)
        | LOCKBIT_ON(ExclusiveLock)
        | LOCKBIT_ON(AccessExclusiveLock);

    // ExclusiveLock
    t[ExclusiveLock as usize] = LOCKBIT_ON(RowShareLock)
        | LOCKBIT_ON(RowExclusiveLock)
        | LOCKBIT_ON(ShareUpdateExclusiveLock)
        | LOCKBIT_ON(ShareLock)
        | LOCKBIT_ON(ShareRowExclusiveLock)
        | LOCKBIT_ON(ExclusiveLock)
        | LOCKBIT_ON(AccessExclusiveLock);

    // AccessExclusiveLock
    t[AccessExclusiveLock as usize] = LOCKBIT_ON(AccessShareLock)
        | LOCKBIT_ON(RowShareLock)
        | LOCKBIT_ON(RowExclusiveLock)
        | LOCKBIT_ON(ShareUpdateExclusiveLock)
        | LOCKBIT_ON(ShareLock)
        | LOCKBIT_ON(ShareRowExclusiveLock)
        | LOCKBIT_ON(ExclusiveLock)
        | LOCKBIT_ON(AccessExclusiveLock);

    t
}

/// `static const char *const lock_mode_names[]` (lock.c).
pub const LOCK_MODE_NAMES: [&str; 9] = [
    "INVALID",
    "AccessShareLock",
    "RowShareLock",
    "RowExclusiveLock",
    "ShareUpdateExclusiveLock",
    "ShareLock",
    "ShareRowExclusiveLock",
    "ExclusiveLock",
    "AccessExclusiveLock",
];

/// `conflictTab[mode]` for the standard lock methods (both `default_lockmethod`
/// and `user_lockmethod` point at the same `LockConflicts`).
pub fn conflict_tab_for(_lockmethodid: LOCKMETHODID, mode: LOCKMODE) -> LOCKMASK {
    lock_conflicts()[mode as usize]
}

/// `LockMethods[lockmethodid]->numLockModes` — `MaxLockMode` for both standard
/// methods.
pub fn num_lock_modes(_lockmethodid: LOCKMETHODID) -> i32 {
    MaxLockMode
}

/// `GetLockmodeName(lockmethodid, mode)` (lock.c) — the display name of a lock
/// mode. Both standard methods share `lock_mode_names`.
pub fn get_lockmode_name(_lockmethodid: LOCKMETHODID, mode: LOCKMODE) -> &'static str {
    LOCK_MODE_NAMES[mode as usize]
}

/// Whether `lockmethodid` is one of the recognized standard methods
/// (`DEFAULT_LOCKMETHOD` / `USER_LOCKMETHOD`); mirrors the C
/// `0 < lockmethodid < lengthof(LockMethods)` guard. Consumed by F1
/// LockAcquire/LockRelease's lock-method validation.
#[allow(dead_code)]
pub fn is_valid_lockmethodid(lockmethodid: LOCKMETHODID) -> bool {
    lockmethodid as u8 == DEFAULT_LOCKMETHOD || lockmethodid as u8 == USER_LOCKMETHOD
}
