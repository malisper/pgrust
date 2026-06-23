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

seam_core::seam!(
    /// `PgArchWakeup()` (`src/backend/postmaster/pgarch.c`) — set the archiver's
    /// latch so it notices a freshly-created `.ready` status file. Infallible in
    /// C (`void`); touches shared memory.
    pub fn pg_arch_wakeup()
);

seam_core::seam!(
    /// `PgArchForceDirScan()` (`src/backend/postmaster/pgarch.c`) — force the
    /// archiver to rescan `archive_status/` on its next pass (used for
    /// high-priority timeline-history files). Infallible in C (`void`); touches
    /// shared memory.
    pub fn pg_arch_force_dir_scan()
);
