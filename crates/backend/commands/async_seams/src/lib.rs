use types_error::PgResult;

seam_core::seam!(
    pub fn pre_commit_notify() -> PgResult<()>
);

seam_core::seam!(
    pub fn at_commit_notify() -> PgResult<()>
);

seam_core::seam!(
    pub fn at_abort_notify()
);

seam_core::seam!(
    pub fn at_subcommit_notify() -> PgResult<()>
);

seam_core::seam!(
    pub fn at_subabort_notify()
);

seam_core::seam!(
    pub fn at_prepare_notify() -> PgResult<()>
);

seam_core::seam!(
    pub fn handle_notify_interrupt()
);
