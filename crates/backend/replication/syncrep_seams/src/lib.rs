seam_core::seam!(
    pub fn sync_rep_cleanup_at_proc_exit()
);

seam_core::seam!(
    pub fn sync_rep_wait_for_lsn(lsn: types_core::XLogRecPtr, commit: bool) -> types_error::PgResult<()>
);
