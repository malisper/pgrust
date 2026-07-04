seam_core::seam!(
    pub fn wake_autovacuum_launcher()
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
