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

seam_core::seam!(
    /// `AutoVacuumingActive(void)` (autovacuum.c): whether autovacuum is enabled
    /// — true iff `autovacuum` (start daemon) and `track_counts` GUCs are both
    /// on. Read by `index_update_stats` (catalog/index.c) to decide whether a
    /// CREATE INDEX may update the parent's `relpages`/`reltuples` relstats.
    pub fn auto_vacuuming_active() -> bool
);

// --- backend-utils-init-postinit consumers (autovacuum.c) ---

seam_core::seam!(
    /// `AmAutoVacuumLauncherProcess()` (autovacuum.c / miscadmin.h): is this the
    /// autovacuum launcher?
    pub fn am_autovacuum_launcher_process() -> bool
);

seam_core::seam!(
    /// `AmAutoVacuumWorkerProcess()`: is this an autovacuum worker?
    pub fn am_autovacuum_worker_process() -> bool
);

seam_core::seam!(
    /// `autovacuum_worker_slots` (autovacuum.c GUC).
    pub fn autovacuum_worker_slots() -> i32
);

seam_core::seam!(
    /// `AutoVacuumShmemSize()` (ipci.c `CalculateShmemSize` accumulator) —
    /// shared-memory bytes this subsystem needs. Infallible in C (a fixed
    /// `add_size`/`mul_size` sum that cannot overflow at these magnitudes), so
    /// the seam returns a bare `Size`.
    pub fn auto_vacuum_shmem_size() -> types_core::Size
);

seam_core::seam!(
    /// `AutoVacuumShmemInit()` (ipci.c `CreateOrAttachShmemStructs`) — allocate-or-attach
    /// this subsystem's shared-memory structures. `Err` carries the C
    /// out-of-shared-memory `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn auto_vacuum_shmem_init() -> types_error::PgResult<()>
);
