//! Seam declarations for the `backend-storage-aio-core` unit
//! (`storage/aio/aio.c` et al.). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

seam_core::seam!(
    /// `AtEOXact_Aio(isCommit)` — error out about leaked AIO handles at
    /// commit (Assert-side checks).
    pub fn at_eoxact_aio(is_commit: bool)
);

seam_core::seam!(
    /// `pgaio_error_cleanup()` — release AIO state on the abort path.
    pub fn pgaio_error_cleanup()
);

// --- backend-utils-init-postinit consumer (aio_init.c) ---

seam_core::seam!(
    /// `pgaio_init_backend()` (aio_init.c): initialize this backend's AIO
    /// subsystem. `Err` carries its `ereport` surface.
    pub fn pgaio_init_backend() -> types_error::PgResult<()>
);
