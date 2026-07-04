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

seam_core::seam!(
    // AsyncNotifyFreezeXids (async.c): freeze old xids in the notify queue
    // before CLOG truncation (vac_truncate_clog).
    pub fn async_notify_freeze_xids(new_frozen_xid: types_core::TransactionId) -> PgResult<()>
);
