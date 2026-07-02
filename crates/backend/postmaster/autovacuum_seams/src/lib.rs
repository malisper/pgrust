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
