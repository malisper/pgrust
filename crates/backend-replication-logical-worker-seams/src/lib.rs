//! Seam declarations for the `backend-replication-logical-worker` unit
//! (`replication/logical/worker.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

seam_core::seam!(
    /// `AtEOXact_LogicalRepWorkers(isCommit)`.
    pub fn at_eoxact_logical_rep_workers(is_commit: bool)
);

seam_core::seam!(
    /// `am_leader_apply_worker()` (worker.c): is this backend a leader apply
    /// worker (`MyLogicalRepWorker->type == WORKERTYPE_APPLY &&
    /// !isParallelApplyWorker(MyLogicalRepWorker)`)? Modeled fallible to mirror
    /// the failure surface of the inline accessors it uses.
    pub fn am_leader_apply_worker() -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `LogRepWorkerWalRcvConn != NULL` (worker.c global): does this worker
    /// currently hold a walreceiver connection to the remote side?
    pub fn have_walrcv_conn() -> bool
);

seam_core::seam!(
    /// `walrcv_disconnect(LogRepWorkerWalRcvConn)` (walreceiver dispatch via the
    /// worker's connection global): gracefully disconnect from the remote side.
    /// Can `ereport(ERROR)` on a protocol/libpq failure, carried on `Err`.
    pub fn walrcv_disconnect() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `MyLogicalRepWorker->stream_fileset != NULL` (worker.c): is a streaming
    /// transaction fileset currently allocated for this worker?
    pub fn have_stream_fileset() -> bool
);

seam_core::seam!(
    /// `FileSetDeleteAll(MyLogicalRepWorker->stream_fileset)`: delete the
    /// streaming-transaction fileset and all its buffiles. Can `ereport` on a
    /// filesystem error, carried on `Err`.
    pub fn fileset_delete_all() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `InitializingApplyWorker` (worker.c global): true while an apply worker
    /// is still initializing; gates the session-level `LockReleaseAll` on exit.
    pub fn initializing_apply_worker() -> bool
);
