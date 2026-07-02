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
    pub fn virtual_xact_lock_table_insert(vxid: types_core::VirtualTransactionId) -> types_error::PgResult<()>
);

seam_core::seam!(
    pub fn at_prepare_locks() -> types_error::PgResult<()>
);

seam_core::seam!(
    pub fn post_prepare_locks(xid: types_core::TransactionId) -> types_error::PgResult<()>
);
