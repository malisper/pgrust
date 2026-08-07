seam_core::seam!(
    pub fn wake_autovacuum_launcher()
);

// AutoVacuumWorkItemType (autovacuum.h): AVW_BRINSummarizeRange is the only
// work-item type C defines.
pub const AVW_BRIN_SUMMARIZE_RANGE: i32 = 0;

seam_core::seam!(
    // AutoVacuumRequestWork (autovacuum.c): register a work item for the next
    // autovacuum worker on this database; returns false when the shmem
    // work-item array is full (callers treat requests as best-effort).
    pub fn auto_vacuum_request_work(
        av_type: i32,
        relation_id: types_core::Oid,
        blkno: types_core::BlockNumber
    ) -> bool
);

seam_core::seam!(
    // autovac_init (autovacuum.c): startup-time sanity check of autovacuum GUCs.
    pub fn autovac_init()
);

seam_core::seam!(
    // AutoVacuumingActive() (autovacuum.c): autovacuum_start_daemon && track_counts.
    pub fn autovacuuming_active() -> bool
);

seam_core::seam!(
    // VacuumUpdateCosts (autovacuum.c): resolve vacuum_cost_delay/limit for the
    // current process (autovacuum worker vs manual) and manage VacuumCostActive.
    pub fn vacuum_update_costs() -> types_error::PgResult<()>
);

seam_core::seam!(
    // AutoVacuumUpdateCostLimit (autovacuum.c).
    pub fn auto_vacuum_update_cost_limit() -> types_error::PgResult<()>
);

seam_core::seam!(
    // AutoVacWorkerFailed (autovacuum.c): postmaster failed to start a worker.
    pub fn autovac_worker_failed()
);
