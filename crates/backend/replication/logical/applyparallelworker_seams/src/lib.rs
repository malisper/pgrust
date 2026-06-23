//! Seam declarations for the `backend-replication-logical-applyparallelworker`
//! unit (`replication/logical/applyparallelworker.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

seam_core::seam!(
    /// `HandleParallelApplyMessageInterrupt()` (applyparallelworker.c) — the
    /// PROCSIG_PARALLEL_APPLY_MESSAGE arm of `procsignal_sigusr1_handler`.
    /// Signal-handler-safe flag flipping; infallible.
    pub fn handle_parallel_apply_message_interrupt()
);

seam_core::seam!(
    /// `pa_detach_all_error_mq()` (applyparallelworker.c): detach the leader
    /// from the error message queue of every parallel apply worker. Can
    /// `ereport` from `shm_mq_detach`, carried on `Err`.
    pub fn pa_detach_all_error_mq() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// Read `winfo->shared->{logicalrep_worker_generation,
    /// logicalrep_worker_slot_no}` under `SpinLockAcquire(&winfo->shared->mutex)`
    /// — applyparallelworker owns the spinlock, so the read is seamed. Returns
    /// `(generation, slot_no)`.
    pub fn pa_read_winfo_slot(
        winfo: &replication_applyparallel::ParallelApplyWorkerInfo,
    ) -> types_error::PgResult<(u16, i32)>
);

seam_core::seam!(
    /// `winfo->error_mq_handle != NULL`: does the leader still hold this
    /// parallel worker's error queue?
    pub fn pa_winfo_has_error_mq(
        winfo: &replication_applyparallel::ParallelApplyWorkerInfo,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `shm_mq_detach(winfo->error_mq_handle); winfo->error_mq_handle = NULL;`
    /// — detach the leader from this parallel worker's error queue. Can
    /// `ereport` from `shm_mq_detach`, carried on `Err`.
    pub fn pa_winfo_detach_error_mq(
        winfo: &mut replication_applyparallel::ParallelApplyWorkerInfo,
    ) -> types_error::PgResult<()>
);
