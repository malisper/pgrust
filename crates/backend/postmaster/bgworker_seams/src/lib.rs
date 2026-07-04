seam_core::seam!(
    // GetBackgroundWorkerPid's status half, marshalled as the handle's
    // (slot, generation): true = BGWH_STOPPED or BGWH_POSTMASTER_DIED
    // (shm_mq's counterparty-gone checks; direct dep would cycle through
    // bgworker -> postgres -> tcop_dest -> tqueue -> shm_mq).
    pub fn background_worker_stopped(slot: i32, generation: u64) -> bool
);
