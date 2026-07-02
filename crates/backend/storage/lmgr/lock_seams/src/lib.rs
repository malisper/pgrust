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
