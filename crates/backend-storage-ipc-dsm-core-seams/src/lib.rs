//! Seam declarations for the `backend-storage-ipc-dsm-core` unit
//! (`src/backend/storage/ipc/dsm.c`). The owning unit installs these from its
//! `init_seams()`; until then a call panics loudly.

seam_core::seam!(
    /// `dsm_detach_all()` (`dsm.c`): detach every dynamic shared memory
    /// segment, including the control segment.
    pub fn dsm_detach_all()
);

seam_core::seam!(
    /// `dsm_estimate_size()` (`dsm.c`) — shared-memory bytes for the DSM
    /// control segment; summed by ipci.c `CalculateShmemSize`. `Err` carries
    /// the `add_size`/`mul_size` overflow `ereport`. Scaffolded slot.
    pub fn dsm_estimate_size() -> types_error::PgResult<types_core::Size>
);

seam_core::seam!(
    /// `dsm_shmem_init()` (`dsm.c`) — initialize the DSM state in main shared
    /// memory (called from `CreateOrAttachShmemStructs`). `Err` carries the
    /// out-of-shmem `ereport(ERROR)`. Scaffolded slot.
    pub fn dsm_shmem_init() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `dsm_postmaster_startup(PGShmemHeader *shim)` (`dsm.c`) — set up the DSM
    /// control segment at postmaster startup. The `shim` header is genuinely
    /// shared memory (raw pointer, opacity inherited). `Err` carries the
    /// `ereport(ERROR)`. Scaffolded slot.
    pub fn dsm_postmaster_startup(shim: *mut types_storage::PGShmemHeader) -> types_error::PgResult<()>
);
