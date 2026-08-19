seam_core::seam!(
    pub fn at_eoxact_logical_rep_workers(is_commit: bool)
);

seam_core::seam!(
    // ApplyWorkerMain (worker.c), reached via the launcher's dynamic bgworker.
    // main_arg = the LogicalRepCtx worker-slot index. Not installed until the
    // apply worker lands: the launcher's stub main exits cleanly instead.
    pub fn apply_worker_main(main_arg: u64) -> types_error::PgResult<()>
);

seam_core::seam!(
    // ParallelApplyWorkerMain (applyparallelworker.c), reached via the
    // leader's pa_launch_parallel_worker. main_arg = worker-slot index; the
    // shared-state handle rides bgw_extra (C's dsm_handle).
    pub fn parallel_apply_worker_main(main_arg: u64) -> types_error::PgResult<()>
);

seam_core::seam!(
    // HandleParallelApplyMessageInterrupt (applyparallelworker.c): runs on
    // the leader thread from the PROCSIG_PARALLEL_APPLY_MESSAGE arm.
    pub fn handle_parallel_apply_message_interrupt()
);

seam_core::seam!(
    // ProcessParallelApplyMessages (applyparallelworker.c), called from
    // ProcessInterrupts when parallel_apply_message_pending() is set.
    pub fn process_parallel_apply_messages() -> types_error::PgResult<()>
);

// ParallelApplyMessagePending (applyparallelworker.c file-static): per-thread
// (only the leader apply worker ever has it set). Lives here so ProcessInterrupts
// (tcop) can read it without depending on the worker crate.
pub mod parallel_apply_message {
    use std::cell::Cell;
    thread_local! {
        static PENDING: Cell<bool> = const { Cell::new(false) };
    }
    pub fn set(v: bool) {
        PENDING.with(|c| c.set(v));
    }
    pub fn pending() -> bool {
        PENDING.with(|c| c.get())
    }
}
