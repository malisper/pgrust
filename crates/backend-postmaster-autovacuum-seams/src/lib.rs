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
    /// `AutoVacuumShmemSize()` (ipci.c `CalculateShmemSize` accumulator) — shared-memory
    /// bytes this subsystem needs. `Err` carries the `add_size`/`mul_size`
    /// overflow `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn auto_vacuum_shmem_size() -> types_error::PgResult<types_core::Size>
);

seam_core::seam!(
    /// `AutoVacuumShmemInit()` (ipci.c `CreateOrAttachShmemStructs`) — allocate-or-attach
    /// this subsystem's shared-memory structures. `Err` carries the C
    /// out-of-shared-memory `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn auto_vacuum_shmem_init() -> types_error::PgResult<()>
);
