//! Seam declarations for the `backend-postmaster-pgarch` unit
//! (`src/backend/postmaster/pgarch.c`). The owning unit installs these from its `init_seams()`;
//! until then a call panics loudly.

seam_core::seam!(
    /// `PgArchiverMain(startup_data, startup_data_len)` (`src/backend/postmaster/pgarch.c`): child entry
    /// point invoked by `postmaster_child_launch`; never returns.
    pub fn pg_archiver_main(startup_data: &types_startup::StartupData) -> !
);

seam_core::seam!(
    /// `PgArchShmemSize()` (ipci.c `CalculateShmemSize` accumulator) —
    /// shared-memory bytes this subsystem needs. Infallible in C, so the seam
    /// returns a bare `Size`.
    pub fn pg_arch_shmem_size() -> types_core::Size
);

seam_core::seam!(
    /// `PgArchShmemInit()` (ipci.c `CreateOrAttachShmemStructs`) — allocate the
    /// `PgArch` shared-memory control block. Infallible in C (`void`); the owner
    /// body returns `()`.
    pub fn pg_arch_shmem_init()
);
