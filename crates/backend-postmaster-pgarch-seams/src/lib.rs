//! Seam declarations for the `backend-postmaster-pgarch` unit
//! (`src/backend/postmaster/pgarch.c`). The owning unit installs these from its `init_seams()`;
//! until then a call panics loudly.

seam_core::seam!(
    /// `PgArchiverMain(startup_data, startup_data_len)` (`src/backend/postmaster/pgarch.c`): child entry
    /// point invoked by `postmaster_child_launch`; never returns.
    pub fn pg_archiver_main(startup_data: &types_startup::StartupData) -> !
);

seam_core::seam!(
    /// `PgArchShmemSize()` (ipci.c `CalculateShmemSize` accumulator) — shared-memory
    /// bytes this subsystem needs. `Err` carries the `add_size`/`mul_size`
    /// overflow `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn pg_arch_shmem_size() -> types_error::PgResult<types_core::Size>
);

seam_core::seam!(
    /// `PgArchShmemInit()` (ipci.c `CreateOrAttachShmemStructs`) — allocate-or-attach
    /// this subsystem's shared-memory structures. `Err` carries the C
    /// out-of-shared-memory `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn pg_arch_shmem_init() -> types_error::PgResult<()>
);
