seam_core::seam!(
    pub fn sync_rep_cleanup_at_proc_exit()
);

seam_core::seam!(
    pub fn sync_rep_wait_for_lsn(lsn: types_core::XLogRecPtr, commit: bool) -> types_error::PgResult<()>
);

seam_core::seam!(
    // SyncRepUpdateSyncStandbysDefined() (syncrep.c); checkpointer-only caller.
    pub fn sync_rep_update_sync_standbys_defined()
);

seam_core::seam!(
    // SyncRepInitConfig() (syncrep.c); walsender-only caller (StartReplication
    // and the WalSndLoop SIGHUP arm).
    pub fn sync_rep_init_config() -> types_error::PgResult<()>
);

seam_core::seam!(
    // SyncRepReleaseWaiters() (syncrep.c); walsender reply-message caller.
    pub fn sync_rep_release_waiters() -> types_error::PgResult<()>
);

seam_core::seam!(
    // SyncRepGetCandidateStandbys (syncrep.c) reduced to what
    // pg_stat_get_wal_senders needs: (walsnd_index, pid) of each candidate.
    pub fn sync_rep_candidate_indexes() -> types_error::PgResult<Vec<(i32, i32)>>
);

seam_core::seam!(
    // SyncRepConfig->syncrep_method == SYNC_REP_PRIORITY (pg_stat sync_state).
    pub fn sync_rep_method_is_priority() -> bool
);
