//! Seam declarations for the injection-point owner
//! (`src/backend/utils/misc/injection_point.c`). ipci.c sizes
//! (`InjectionPointShmemSize`) and initializes (`InjectionPointShmemInit`) its
//! shared state. The owning unit installs these from its `init_seams()`;
//! until then a call panics loudly.

seam_core::seam!(
    /// `InjectionPointShmemSize()` — shared-memory bytes for the injection
    /// point table; summed by ipci.c `CalculateShmemSize`. `Err` carries the
    /// `add_size`/`mul_size` overflow `ereport`. Scaffolded slot.
    pub fn injection_point_shmem_size() -> types_error::PgResult<types_core::Size>
);

seam_core::seam!(
    /// `InjectionPointShmemInit()` — allocate-or-attach the injection point
    /// shared table. `Err` carries the out-of-shmem `ereport(ERROR)`.
    /// Scaffolded slot.
    pub fn injection_point_shmem_init() -> types_error::PgResult<()>
);
