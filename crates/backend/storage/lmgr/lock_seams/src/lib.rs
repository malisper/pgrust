use types_core::ProcNumber;
use types_error::PgResult;

seam_core::seam!(
    pub fn abort_strong_lock_acquire()
);

// GetAwaitedLock() marshaled to the awaited LOCALLOCK's hashcode; None = NULL.
seam_core::seam!(
    pub fn get_awaited_lock_hashcode() -> Option<u32>
);

seam_core::seam!(
    pub fn grant_awaited_lock()
);

seam_core::seam!(
    pub fn reset_awaited_lock()
);

seam_core::seam!(
    pub fn remove_from_wait_queue(procno: ProcNumber, hashcode: u32)
);

seam_core::seam!(
    pub fn lock_release_all(lockmethodid: u8, all_locks: bool) -> PgResult<()>
);

use types_storage::lock::{LockAcquireResult, LOCKMODE, LOCKTAG};

// C's `LOCALLOCK **locallockp` marshaled away; mark_lock_clear re-finds it by local-hash key.
seam_core::seam!(
    pub fn lock_acquire_extended(
        locktag: LOCKTAG,
        lockmode: LOCKMODE,
        session_lock: bool,
        dont_wait: bool,
        report_memory_error: bool,
        log_lock_failure: bool
    ) -> PgResult<LockAcquireResult>
);

seam_core::seam!(
    pub fn lock_release(locktag: LOCKTAG, lockmode: LOCKMODE, session_lock: bool) -> PgResult<bool>
);

seam_core::seam!(
    pub fn mark_lock_clear(locktag: LOCKTAG, lockmode: LOCKMODE)
);

seam_core::seam!(
    pub fn lock_held_by_me(locktag: LOCKTAG, lockmode: LOCKMODE, orstronger: bool) -> bool
);

seam_core::seam!(
    pub fn lock_has_waiters(locktag: LOCKTAG, lockmode: LOCKMODE) -> PgResult<bool>
);

seam_core::seam!(
    // DoLockModesConflict (lock.c); pure conflict-table probe.
    pub fn do_lock_modes_conflict(mode1: LOCKMODE, mode2: LOCKMODE) -> bool
);

// C's `LOCALLOCK **locallocks, int nlocks`; None = NULL (overflowed resowner
// cache = walk the whole LOCALLOCK table).
seam_core::seam!(
    pub fn lock_reassign_current_owner<'a>(
        locallocks: Option<&'a [types_storage::lock::LOCALLOCKTAG]>,
    ) -> PgResult<()>
);

seam_core::seam!(
    pub fn lock_release_current_owner<'a>(
        locallocks: Option<&'a [types_storage::lock::LOCALLOCKTAG]>,
    ) -> PgResult<()>
);



