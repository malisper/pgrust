//! Seam declarations for the `backend-postmaster-autovacuum` unit
//! (`src/backend/postmaster/autovacuum.c`). The owning unit installs these
//! from its `init_seams()`; until then a call panics loudly.

seam_core::seam!(
    /// `AutoVacLauncherMain(startup_data, startup_data_len)`: autovacuum
    /// launcher entry point invoked by `postmaster_child_launch`; never returns.
    pub fn auto_vac_launcher_main(startup_data: &types_startup::StartupData) -> !
);

seam_core::seam!(
    /// `AutoVacWorkerMain(startup_data, startup_data_len)`: autovacuum worker
    /// entry point invoked by `postmaster_child_launch`; never returns.
    pub fn auto_vac_worker_main(startup_data: &types_startup::StartupData) -> !
);
