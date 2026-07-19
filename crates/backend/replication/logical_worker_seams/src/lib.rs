seam_core::seam!(
    pub fn at_eoxact_logical_rep_workers(is_commit: bool)
);

seam_core::seam!(
    // ApplyWorkerMain (worker.c), reached via the launcher's dynamic bgworker.
    // main_arg = the LogicalRepCtx worker-slot index. Not installed until the
    // apply worker lands: the launcher's stub main exits cleanly instead.
    pub fn apply_worker_main(main_arg: u64) -> types_error::PgResult<()>
);
