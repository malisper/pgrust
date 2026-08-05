seam_core::seam!(
    // GetBackgroundWorkerPid's status half, marshalled as the handle's
    // (slot, generation): true = BGWH_STOPPED or BGWH_POSTMASTER_DIED
    // (shm_mq's counterparty-gone checks; direct dep would cycle through
    // bgworker -> postgres -> tcop_dest -> tqueue -> shm_mq).
    pub fn background_worker_stopped(slot: i32, generation: u64) -> bool
);

seam_core::seam!(
    // GetBackgroundWorkerTypeByPid (bgworker.c); None is C's NULL (direct dep
    // would cycle through bgworker -> postgres -> fmgr_core -> pgstatfuncs).
    pub fn get_background_worker_type_by_pid(pid: i32) -> Option<String>
);
