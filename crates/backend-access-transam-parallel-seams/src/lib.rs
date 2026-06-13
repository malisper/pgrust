//! Seam declarations for the `backend-access-transam-parallel` unit
//! (`access/transam/parallel.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `IsParallelWorker()` (`access/parallel.h`):
    /// `(ParallelWorkerNumber >= 0)`; `ParallelWorkerNumber` is owned by
    /// `parallel.c`.
    pub fn is_parallel_worker() -> bool
);

seam_core::seam!(
    /// `HandleParallelMessageInterrupt()` (parallel.c) — the
    /// PROCSIG_PARALLEL_MESSAGE arm of `procsignal_sigusr1_handler`.
    /// Signal-handler-safe flag flipping; infallible.
    pub fn handle_parallel_message_interrupt()
);

seam_core::seam!(
    /// `AtEOXact_Parallel(isCommit)` — clean up unfinished parallel workers
    /// at top-level transaction end (warning about leaks on commit).
    pub fn at_eoxact_parallel(is_commit: bool) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `AtEOSubXact_Parallel(isCommit, mySubId)`.
    pub fn at_eosubxact_parallel(
        is_commit: bool,
        my_sub_id: types_core::SubTransactionId,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ParallelWorkerReportLastRecEnd(XactLastRecEnd)` — tell the leader
    /// about WAL this worker wrote.
    pub fn parallel_worker_report_last_rec_end(
        last_rec_end: types_core::XLogRecPtr,
    ) -> types_error::PgResult<()>
);
