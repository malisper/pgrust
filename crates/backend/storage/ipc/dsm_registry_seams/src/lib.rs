//! Seam declarations for the dsm_registry owner
//! (`src/backend/storage/ipc/dsm_registry.c`). ipci.c sizes
//! (`DSMRegistryShmemSize`) and initializes (`DSMRegistryShmemInit`) the named
//! DSM segment registry. The owning unit installs these from its
//! `init_seams()`; until then a call panics loudly.

seam_core::seam!(
    /// `DSMRegistryShmemSize()` — shared-memory bytes for the DSM registry
    /// control block; summed by ipci.c `CalculateShmemSize`. `Err` carries the
    /// `add_size`/`mul_size` overflow `ereport`. Scaffolded slot.
    pub fn dsm_registry_shmem_size() -> types_error::PgResult<types_core::Size>
);

seam_core::seam!(
    /// `DSMRegistryShmemInit()` — allocate-or-attach the DSM registry control
    /// block. `Err` carries the out-of-shmem `ereport(ERROR)`. Scaffolded slot.
    pub fn dsm_registry_shmem_init() -> types_error::PgResult<()>
);
